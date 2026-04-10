import os
import json
import re
import html
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, Tuple, List, Optional

import requests
import yfinance as yf
from supabase import create_client, Client
from openai import OpenAI

# =========================
# 환경변수
# =========================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

WP_URL = os.getenv("WP_URL", os.getenv("WP_SITE_URL", "")).strip()
WP_USERNAME = os.getenv("WP_USERNAME", "").strip()
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "").strip()

FORCE_STAGE = os.getenv("FORCE_STAGE", "").strip().lower()

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY가 필요합니다.")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL / SUPABASE_KEY가 필요합니다.")
if not FORCE_STAGE:
    raise RuntimeError("FORCE_STAGE가 필요합니다. pre_open / intraday / close 중 하나를 넣어주세요.")

client = OpenAI(api_key=OPENAI_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KST = ZoneInfo("Asia/Seoul")

STAGE_TO_MARKET_TYPE = {
    "pre_open": "kr_stock_preopen",
    "intraday": "kr_stock_morning",
    "close": "kr_stock_close",
}

if FORCE_STAGE not in STAGE_TO_MARKET_TYPE:
    raise RuntimeError("FORCE_STAGE는 pre_open / intraday / close 중 하나여야 합니다.")

MARKET_TYPE = STAGE_TO_MARKET_TYPE[FORCE_STAGE]

# =========================
# 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)

def today_str() -> str:
    return now_kst().strftime("%Y-%m-%d")

def safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        val = float(v)
        if val != val:  # NaN
            return default
        return val
    except Exception:
        return default

def fmt_pct(v: Any) -> str:
    if v is None:
        return "N/A"
    return f"{v:+.2f}%"

def fmt_price(v: Any, digits: int = 2) -> str:
    if v is None:
        return "N/A"
    return f"{v:,.{digits}f}"

def sanitize_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# =========================
# 시장 데이터
# =========================
def get_ticker_snapshot(ticker: str, name: str) -> Dict[str, Any]:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d", interval="1d", auto_adjust=False)

        if hist is None or hist.empty:
            return {
                "name": name,
                "ticker": ticker,
                "error": "no_data",
            }

        last = hist.iloc[-1]
        close = safe_float(last.get("Close"), None)
        prev_close = safe_float(hist.iloc[-2].get("Close"), None) if len(hist) >= 2 else None

        if close is None or prev_close is None:
            return {
                "name": name,
                "ticker": ticker,
                "close": close,
                "prev_close": prev_close,
                "change": None,
                "pct": None,
            }

        change = close - prev_close
        pct = (change / prev_close * 100.0) if prev_close else None

        return {
            "name": name,
            "ticker": ticker,
            "close": round(close, 4),
            "prev_close": round(prev_close, 4),
            "change": round(change, 4),
            "pct": round(pct, 2) if pct is not None else None,
        }
    except Exception as e:
        return {
            "name": name,
            "ticker": ticker,
            "error": str(e),
        }

def collect_market_data() -> Dict[str, Any]:
    return {
        "date_kst": today_str(),
        "collected_at_kst": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "us_markets": {
            "dow": get_ticker_snapshot("^DJI", "다우"),
            "sp500": get_ticker_snapshot("^GSPC", "S&P500"),
            "nasdaq": get_ticker_snapshot("^IXIC", "나스닥"),
            "sox": get_ticker_snapshot("^SOX", "SOX"),
        },
        "kr_markets": {
            "kospi": get_ticker_snapshot("^KS11", "코스피"),
            "kosdaq": get_ticker_snapshot("^KQ11", "코스닥"),
        },
        "fx": {
            "usdkrw": get_ticker_snapshot("KRW=X", "USD/KRW"),
        },
    }

# =========================
# 뉴스 수집
# =========================
def parse_google_news_rss(query: str, limit: int = 5) -> List[Dict[str, str]]:
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    try:
        res = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        res.raise_for_status()

        root = ET.fromstring(res.text)
        items: List[Dict[str, str]] = []

        for item in root.findall(".//item"):
            title = sanitize_text(item.findtext("title", default=""))
            link = sanitize_text(item.findtext("link", default=""))
            pub_date = sanitize_text(item.findtext("pubDate", default=""))

            if title and link:
                items.append({
                    "title": title,
                    "link": link,
                    "pub_date": pub_date,
                })

            if len(items) >= limit:
                break

        return items
    except Exception as e:
        print(f"[parse_google_news_rss] query={query}, error={e}")
        return []

def collect_market_news_top3() -> List[Dict[str, str]]:
    queries = [
        "미국 증시 OR 나스닥 OR S&P500 OR 다우 OR SOX",
        "미국 금리 OR 연준 OR 국채금리 OR CPI OR PPI",
        "원달러 OR 환율 OR 유가 OR 반도체 OR 엔비디아",
    ]

    collected: List[Dict[str, str]] = []
    seen_titles = set()

    for q in queries:
        for item in parse_google_news_rss(q, limit=4):
            title_key = item["title"].strip().lower()
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            collected.append(item)
            if len(collected) >= 3:
                return collected

    return collected[:3]

def build_news_texts(news_items: List[Dict[str, str]]) -> Dict[str, str]:
    news_titles = [sanitize_text(x.get("title", "")) for x in news_items if x.get("title")]
    while len(news_titles) < 3:
        news_titles.append("관련 주요 뉴스 없음")

    return {
        "news_1": news_titles[0],
        "news_2": news_titles[1],
        "news_3": news_titles[2],
    }

# =========================
# 프롬프트
# =========================
def build_system_prompt(stage: str) -> str:
    common = """
너는 한국 주식시장 시황 전문 에디터다.
반드시 JSON만 출력해야 한다.
설명 문장, 코드블록, 마크다운 없이 JSON 객체만 출력한다.

반드시 아래 키를 모두 포함하라:
- one_line
- kospi_text
- kosdaq_text
- strong_sectors
- weak_sectors
- tomorrow_points
- full_text
- post_title

문체 규칙:
- 지나치게 과장하지 말 것
- 실제 증권사 데일리 브리핑처럼 간결하고 읽기 쉽게 작성
- 한글로 작성
- 이모지 금지
- 투자 권유 문구 금지
- 제공된 데이터만 근거로 사용할 것
- 데이터가 없으면 없다고 솔직히 쓸 것
"""

    if stage == "pre_open":
        return common + """
이번 글은 "개장 전 브리핑"이다.

핵심 규칙:
- 미국 3대 지수와 SOX는 반드시 전일 마감 기준으로 해석하라.
- 다우, S&P500, 나스닥, SOX, 원/달러 흐름을 오늘 한국장과 연결해라.
- 특히 SOX와 나스닥 흐름이 한국 반도체/AI 관련주에 미칠 영향을 해석하라.
- 외국인 수급, 성장주/수출주/내수주에 어떤 영향이 있을지 해석하라.
- 함께 제공된 밤사이 핵심 뉴스 3개를 오늘 한국장 관점에서 연결해서 해석하라.
- strong_sectors는 오늘 관심 업종, weak_sectors는 오늘 주의 업종으로 작성하라.
- full_text는 아래 4개를 반드시 자연스럽게 포함해야 한다.
  1) 미국장 요약
  2) SOX/반도체 해석
  3) 환율/수급 해석
  4) 밤사이 핵심 뉴스 3개가 한국장에 미칠 영향
- tomorrow_points는 장 시작 전 체크해야 할 포인트를 구체적으로 작성하라.

절대 금지:
- 존재하지 않는 뉴스나 이벤트를 지어내지 말 것
- 오전 장중 결과처럼 쓰지 말 것
- 장 마감 총평처럼 쓰지 말 것
"""

    if stage == "intraday":
        return common + """
이번 글은 "오전 시황"이다.

핵심 규칙:
- 오전 실제 흐름 중심으로 작성
- 코스피/코스닥의 오전 움직임, 강세 업종, 약세 업종, 수급 분위기 중심
- 개장 전 브리핑처럼 미국장 중심으로 길게 쓰지 말 것
- 마감 총평처럼 하루 전체 결론으로 쓰지 말 것
- tomorrow_points에는 오후장 체크 포인트를 작성
- full_text는 오전 장세 해설이어야 함
"""

    if stage == "close":
        return common + """
이번 글은 "장 마감 시황"이다.

핵심 규칙:
- 하루 전체를 정리하는 총평 형식으로 작성
- 코스피/코스닥 마감 흐름, 강세 업종, 약세 업종, 하루 해석을 담을 것
- tomorrow_points에는 내일 체크 포인트를 작성
- full_text는 오늘 시장을 한 번에 정리하는 마감 브리핑이어야 함
- post_title은 워드프레스 발행용 제목으로 자연스럽고 클릭 가능한 제목으로 작성
"""

    raise ValueError(f"Unknown stage: {stage}")

def build_preopen_context_lines(market_data: Dict[str, Any], news_texts: Dict[str, str]) -> str:
    us = market_data.get("us_markets", {})
    fx = market_data.get("fx", {})

    dow = us.get("dow", {})
    sp500 = us.get("sp500", {})
    nasdaq = us.get("nasdaq", {})
    sox = us.get("sox", {})
    usdkrw = fx.get("usdkrw", {})

    return f"""
미국지수 요약:
- 다우: close={dow.get('close')} / pct={dow.get('pct')}
- S&P500: close={sp500.get('close')} / pct={sp500.get('pct')}
- 나스닥: close={nasdaq.get('close')} / pct={nasdaq.get('pct')}
- SOX: close={sox.get('close')} / pct={sox.get('pct')}
- 원/달러: close={usdkrw.get('close')} / pct={usdkrw.get('pct')}

밤사이 핵심 뉴스 3개:
1. {news_texts.get('news_1', '')}
2. {news_texts.get('news_2', '')}
3. {news_texts.get('news_3', '')}
""".strip()

def build_user_prompt(stage: str, market_data: Dict[str, Any], news_texts: Dict[str, str]) -> str:
    extra_context = ""
    if stage == "pre_open":
        extra_context = build_preopen_context_lines(market_data, news_texts)

    return f"""
오늘 날짜(KST): {today_str()}
현재 stage: {stage}

시장 데이터(JSON):
{json.dumps(market_data, ensure_ascii=False, indent=2)}

추가 참고 정보:
{extra_context}

위 데이터를 바탕으로 해당 stage에 맞는 시황을 JSON으로 작성하라.
반드시 아래 형식의 JSON 객체만 출력하라.

{{
  "one_line": "...",
  "kospi_text": "...",
  "kosdaq_text": "...",
  "strong_sectors": "...",
  "weak_sectors": "...",
  "tomorrow_points": "...",
  "full_text": "...",
  "post_title": "..."
}}
"""

# =========================
# OpenAI 호출
# =========================
def extract_json_text(content: Any) -> str:
    if content is None:
        return ""

    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "output_text" and item.get("text"):
                    parts.append(item["text"])
                elif item.get("text"):
                    parts.append(str(item["text"]))
            else:
                parts.append(str(item))
        return "".join(parts).strip()

    return str(content).strip()

def generate_summary(stage: str, market_data: Dict[str, Any], news_texts: Dict[str, str]) -> Dict[str, Any]:
    system_prompt = build_system_prompt(stage)
    user_prompt = build_user_prompt(stage, market_data, news_texts)

    resp = client.chat.completions.create(
        model="gpt-5",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    raw_content = resp.choices[0].message.content
    content = extract_json_text(raw_content)

    try:
        result = json.loads(content)
    except Exception:
        print("[generate_summary] JSON parse failed")
        print("[generate_summary] raw content:", content)
        raise RuntimeError("모델 응답이 JSON이 아닙니다.")

    required_keys = [
        "one_line",
        "kospi_text",
        "kosdaq_text",
        "strong_sectors",
        "weak_sectors",
        "tomorrow_points",
        "full_text",
        "post_title",
    ]
    for k in required_keys:
        if k not in result:
            raise RuntimeError(f"응답 JSON에 {k} 키가 없습니다.")

    return result

# =========================
# Supabase 저장
# =========================
def upsert_summary(row: Dict[str, Any]) -> None:
    existing = (
        supabase.table("market_summaries")
        .select("id")
        .eq("summary_date", row["summary_date"])
        .eq("market_type", row["market_type"])
        .limit(1)
        .execute()
    )
    existing_rows = existing.data or []

    if existing_rows:
        row_id = existing_rows[0]["id"]
        (
            supabase.table("market_summaries")
            .update(row)
            .eq("id", row_id)
            .execute()
        )
        print(f"[upsert_summary] updated id={row_id}")
    else:
        (
            supabase.table("market_summaries")
            .insert(row)
            .execute()
        )
        print("[upsert_summary] inserted new row")

# =========================
# WordPress 발행
# =========================
def build_wp_html(summary: Dict[str, Any], market_data: Dict[str, Any], news_texts: Dict[str, str], stage: str) -> str:
    full_text_html = str(summary.get("full_text", "")).replace("\n", "<br>")

    us = market_data.get("us_markets", {})
    fx = market_data.get("fx", {})

    dow = us.get("dow", {})
    sp500 = us.get("sp500", {})
    nasdaq = us.get("nasdaq", {})
    sox = us.get("sox", {})
    usdkrw = fx.get("usdkrw", {})

    news_block = ""
    if stage == "pre_open":
        news_block = f"""
<h3>밤사이 미국 지수</h3>
<ul>
  <li>다우: {fmt_price(dow.get('close'))} ({fmt_pct(dow.get('pct'))})</li>
  <li>S&amp;P500: {fmt_price(sp500.get('close'))} ({fmt_pct(sp500.get('pct'))})</li>
  <li>나스닥: {fmt_price(nasdaq.get('close'))} ({fmt_pct(nasdaq.get('pct'))})</li>
  <li>SOX: {fmt_price(sox.get('close'))} ({fmt_pct(sox.get('pct'))})</li>
  <li>원/달러: {fmt_price(usdkrw.get('close'))} ({fmt_pct(usdkrw.get('pct'))})</li>
</ul>

<h3>밤사이 핵심 뉴스 3개</h3>
<ol>
  <li>{news_texts.get('news_1', '')}</li>
  <li>{news_texts.get('news_2', '')}</li>
  <li>{news_texts.get('news_3', '')}</li>
</ol>
""".strip()

    return f"""
<h2>{summary.get('one_line', '')}</h2>
<p>{full_text_html}</p>

{news_block}

<h3>코스피</h3>
<p>{summary.get('kospi_text', '')}</p>

<h3>코스닥</h3>
<p>{summary.get('kosdaq_text', '')}</p>

<h3>강한 업종</h3>
<p>{summary.get('strong_sectors', '')}</p>

<h3>약한 업종</h3>
<p>{summary.get('weak_sectors', '')}</p>

<h3>체크 포인트</h3>
<p>{summary.get('tomorrow_points', '').replace(chr(10), '<br>')}</p>
""".strip()

def publish_wordpress(summary: Dict[str, Any], market_data: Dict[str, Any], news_texts: Dict[str, str], stage: str) -> Tuple[str, str]:
    if not WP_URL:
        raise RuntimeError("WP_URL/WP_SITE_URL이 비어 있습니다.")
    if not WP_USERNAME:
        raise RuntimeError("WP_USERNAME이 비어 있습니다.")
    if not WP_APP_PASSWORD:
        raise RuntimeError("WP_APP_PASSWORD가 비어 있습니다.")

    endpoint = f"{WP_URL.rstrip('/')}/wp-json/wp/v2/posts"
    auth = (WP_USERNAME, WP_APP_PASSWORD)

    payload = {
        "title": summary["post_title"],
        "content": build_wp_html(summary, market_data, news_texts, stage),
        "status": "publish",
    }

    print("[publish_wordpress] endpoint:", endpoint)
    print("[publish_wordpress] title:", summary["post_title"])

    res = requests.post(endpoint, auth=auth, json=payload, timeout=30)

    print("[publish_wordpress] status:", res.status_code)
    print("[publish_wordpress] response text:", res.text[:1000])

    res.raise_for_status()
    data = res.json()

    wp_title = (
        data.get("title", {}).get("rendered")
        if isinstance(data.get("title"), dict)
        else summary["post_title"]
    )
    wp_link = data.get("link") or ""

    if not wp_link:
        raise RuntimeError("워드프레스 응답에 link가 없습니다.")

    print("[publish_wordpress] success link:", wp_link)
    return wp_title or summary["post_title"], wp_link

# =========================
# 메인
# =========================
def main() -> None:
    print(f"[START] FORCE_STAGE={FORCE_STAGE}, MARKET_TYPE={MARKET_TYPE}, date={today_str()}")

    market_data = collect_market_data()
    print("[market_data]")
    print(json.dumps(market_data, ensure_ascii=False, indent=2))

    news_items = collect_market_news_top3() if FORCE_STAGE == "pre_open" else []
    news_texts = build_news_texts(news_items)

    print("[news_texts]")
    print(json.dumps(news_texts, ensure_ascii=False, indent=2))

    summary = generate_summary(FORCE_STAGE, market_data, news_texts)
    print("[summary]")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    us = market_data.get("us_markets", {})
    fx = market_data.get("fx", {})

    dow = us.get("dow", {})
    sp500 = us.get("sp500", {})
    nasdaq = us.get("nasdaq", {})
    sox = us.get("sox", {})
    usdkrw = fx.get("usdkrw", {})

    dow_text = f"{fmt_price(dow.get('close'))} ({fmt_pct(dow.get('pct'))})"
    sp500_text = f"{fmt_price(sp500.get('close'))} ({fmt_pct(sp500.get('pct'))})"
    nasdaq_text = f"{fmt_price(nasdaq.get('close'))} ({fmt_pct(nasdaq.get('pct'))})"
    sox_text = f"{fmt_price(sox.get('close'))} ({fmt_pct(sox.get('pct'))})"
    usdkrw_text = f"{fmt_price(usdkrw.get('close'))}원 ({fmt_pct(usdkrw.get('pct'))})"

    post_title = summary.get("post_title", "")
    post_url = ""

    # 개장 전 / 마감만 WordPress 발행
    if FORCE_STAGE in ("pre_open", "close"):
        wp_title, wp_link = publish_wordpress(summary, market_data, news_texts, FORCE_STAGE)
        post_title = wp_title
        post_url = wp_link

    row = {
        "summary_date": today_str(),
        "market_type": MARKET_TYPE,
        "one_line": summary.get("one_line", ""),
        "kospi_text": summary.get("kospi_text", ""),
        "kosdaq_text": summary.get("kosdaq_text", ""),
        "strong_sectors": summary.get("strong_sectors", ""),
        "weak_sectors": summary.get("weak_sectors", ""),
        "tomorrow_points": summary.get("tomorrow_points", ""),
        "full_text": summary.get("full_text", ""),
        "post_title": post_title,
        "post_url": post_url,
        "updated_at": now_kst().isoformat(),

        # 개장 전 카드용 미국지수/환율/뉴스 저장
        "dow_text": dow_text,
        "sp500_text": sp500_text,
        "nasdaq_text": nasdaq_text,
        "sox_text": sox_text,
        "usdkrw_text": usdkrw_text,
        "news_1": news_texts.get("news_1", ""),
        "news_2": news_texts.get("news_2", ""),
        "news_3": news_texts.get("news_3", ""),
    }

    upsert_summary(row)
    print("[DONE] summary saved successfully")

if __name__ == "__main__":
    main()
