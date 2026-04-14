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

INTERNAL_LINK_1_TITLE = os.getenv("INTERNAL_LINK_1_TITLE", "").strip()
INTERNAL_LINK_1_URL = os.getenv("INTERNAL_LINK_1_URL", "").strip()
INTERNAL_LINK_2_TITLE = os.getenv("INTERNAL_LINK_2_TITLE", "").strip()
INTERNAL_LINK_2_URL = os.getenv("INTERNAL_LINK_2_URL", "").strip()
INTERNAL_LINK_3_TITLE = os.getenv("INTERNAL_LINK_3_TITLE", "").strip()
INTERNAL_LINK_3_URL = os.getenv("INTERNAL_LINK_3_URL", "").strip()

DEFAULT_SEO_IMAGE_URL = os.getenv("DEFAULT_SEO_IMAGE_URL", "").strip()

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
AD_SHORTCODE = '[adinserter block="1"]'

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
        if val != val:
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

def nl2br(text: str) -> str:
    return str(text or "").replace("\n", "<br>")

def esc_html(text: str) -> str:
    return html.escape(str(text or ""), quote=True)

def strip_to_plain_text(text: str) -> str:
    return sanitize_text(text)

def trim_chars(text: str, max_len: int) -> str:
    text = str(text or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"

def slugify_ko(text: str, max_len: int = 48) -> str:
    text = sanitize_text(text).lower()
    text = text.replace("%", "")
    text = re.sub(r"[^\w가-힣\s\-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:max_len].strip("-")

# =========================
# SEO 자산 생성
# =========================
def get_focus_keyword(stage: str) -> str:
    if stage == "pre_open":
        return "개장 전 브리핑"
    if stage == "close":
        return "장 마감 시황"
    return "오전 시황"

def build_seo_assets(stage: str, market_data: Dict[str, Any], summary: Dict[str, Any]) -> Dict[str, str]:
    us = market_data.get("us_markets", {})
    fx = market_data.get("fx", {})

    sox_pct = us.get("sox", {}).get("pct")
    usdkrw_close = fx.get("usdkrw", {}).get("close")

    focus_keyword = get_focus_keyword(stage)

    if stage == "pre_open":
        title = f"{focus_keyword} | 미국 증시·SOX·원달러로 보는 오늘 주식시장 전망 5가지"
        slug = slugify_ko("개장-전-브리핑-미국증시-sox-원달러")
        meta_description = trim_chars(
            f"{focus_keyword}입니다. 미국 증시와 SOX, 원달러 흐름을 바탕으로 오늘 주식시장 전망을 정리했습니다. "
            f"SOX {fmt_pct(sox_pct)}, 원달러 {fmt_price(usdkrw_close)}원 흐름과 반도체 영향까지 빠르게 확인하세요.",
            155
        )
    elif stage == "close":
        title = f"{focus_keyword} | 코스피·코스닥 흐름과 내일 체크포인트 5가지"
        slug = slugify_ko("장-마감-시황-코스피-코스닥-체크포인트")
        meta_description = trim_chars(
            f"{focus_keyword}입니다. 오늘 장 마감 요약과 코스피·코스닥 흐름, 강했던 업종과 약했던 업종, "
            f"내일 체크포인트를 한 번에 정리했습니다.",
            155
        )
    else:
        title = f"{focus_keyword} | 코스피·코스닥 오전 흐름 핵심 정리"
        slug = slugify_ko("오전-시황-코스피-코스닥-핵심")
        meta_description = trim_chars(
            f"{focus_keyword}입니다. 코스피와 코스닥 오전 흐름, 강세 업종과 약세 업종, 오후장 체크포인트를 빠르게 확인하세요.",
            155
        )

    return {
        "focus_keyword": focus_keyword,
        "seo_title": title,
        "slug": slug,
        "meta_description": meta_description,
    }

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
            key = item["title"].strip().lower()
            if key in seen_titles:
                continue
            seen_titles.add(key)
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
- tomorrow_points는 장 시작 전 체크해야 할 포인트를 구체적으로 작성하라.
- full_text는 900~1300자 수준으로 충분히 작성하라.
- 본문 초반에 "개장 전 브리핑" 문구가 자연스럽게 들어가게 작성하라.
- 포커스 키워드 밀도가 너무 낮지 않게 자연스럽게 반복하라.
"""

    if stage == "intraday":
        return common + """
이번 글은 "오전 시황"이다.

핵심 규칙:
- 오전 실제 흐름 중심으로 작성
- 코스피/코스닥의 오전 움직임, 강세 업종, 약세 업종, 수급 분위기 중심
- full_text는 700자 이상으로 작성하라.
"""

    if stage == "close":
        return common + """
이번 글은 "장 마감 시황"이다.

핵심 규칙:
- 하루 전체를 정리하는 총평 형식으로 작성
- 코스피/코스닥 마감 흐름, 강세 업종, 약세 업종, 하루 해석을 담을 것
- tomorrow_points에는 내일 체크 포인트를 작성
- full_text는 900~1300자 수준으로 충분히 작성하라.
- 본문 초반에 "장 마감 시황" 문구가 자연스럽게 들어가게 작성하라.
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
# 내부 링크 / 외부 링크 / FAQ schema
# =========================
def build_internal_links_html() -> str:
    links = [
        (INTERNAL_LINK_1_TITLE, INTERNAL_LINK_1_URL),
        (INTERNAL_LINK_2_TITLE, INTERNAL_LINK_2_URL),
        (INTERNAL_LINK_3_TITLE, INTERNAL_LINK_3_URL),
    ]

    valid_links = [(t, u) for t, u in links if t and u]
    if not valid_links:
        if WP_URL:
            return f"""
<h2 id="internal-links">개장 전 브리핑 관련 내부 링크</h2>
<ul>
  <li><a href="{esc_html(WP_URL.rstrip('/'))}">주식시장 전체 글 모아보기</a></li>
</ul>
""".strip()
        return ""

    items = "\n".join(
        [f'<li><a href="{esc_html(url)}">{esc_html(title)}</a></li>' for title, url in valid_links]
    )

    return f"""
<h2 id="internal-links">개장 전 브리핑 관련 내부 링크</h2>
<ul>
{items}
</ul>
""".strip()

def build_external_links_html(stage: str) -> str:
    if stage == "pre_open":
        return """
<h2 id="external-links">외부 참고 자료</h2>
<ul>
  <li><a href="https://www.nasdaq.com/market-activity/index/comp" target="_blank">Nasdaq Composite 공식 페이지</a></li>
  <li><a href="https://www.krx.co.kr/" target="_blank">KRX 한국거래소 공식 사이트</a></li>
  <li><a href="https://www.federalreserve.gov/" target="_blank">미 연준 공식 사이트</a></li>
</ul>
""".strip()

    return """
<h2 id="external-links">외부 참고 자료</h2>
<ul>
  <li><a href="https://www.krx.co.kr/" target="_blank">KRX 한국거래소 공식 사이트</a></li>
  <li><a href="https://www.federalreserve.gov/" target="_blank">미 연준 공식 사이트</a></li>
</ul>
""".strip()

def build_faq_schema_html(faqs: List[Tuple[str, str]]) -> str:
    if not faqs:
        return ""

    schema = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": q,
                "acceptedAnswer": {
                    "@type": "Answer",
                    "text": a
                }
            }
            for q, a in faqs
        ]
    }

    return f'<script type="application/ld+json">{json.dumps(schema, ensure_ascii=False)}</script>'

def build_toc_html(stage: str, focus_keyword: str) -> str:
    if stage == "pre_open":
        return f"""
<div class="seo-toc">
  <h2 id="toc">{focus_keyword} 목차</h2>
  <ul>
    <li><a href="#summary">{focus_keyword} 핵심 요약</a></li>
    <li><a href="#us-market">{focus_keyword} 미국 증시 정리</a></li>
    <li><a href="#sox-impact">{focus_keyword} SOX와 반도체 영향</a></li>
    <li><a href="#news-top3">{focus_keyword} 밤사이 핵심 뉴스 3개</a></li>
    <li><a href="#check-points">{focus_keyword} 장 시작 전 체크 포인트</a></li>
    <li><a href="#internal-links">{focus_keyword} 관련 내부 링크</a></li>
  </ul>
</div>
""".strip()

    return f"""
<div class="seo-toc">
  <h2 id="toc">{focus_keyword} 목차</h2>
  <ul>
    <li><a href="#summary">{focus_keyword} 요약</a></li>
    <li><a href="#kospi-close">{focus_keyword} 코스피 해설</a></li>
    <li><a href="#kosdaq-close">{focus_keyword} 코스닥 해설</a></li>
    <li><a href="#check-points">{focus_keyword} 내일 체크 포인트</a></li>
    <li><a href="#internal-links">{focus_keyword} 관련 내부 링크</a></li>
  </ul>
</div>
""".strip()

def build_image_html(focus_keyword: str) -> str:
    if not DEFAULT_SEO_IMAGE_URL:
        return ""
    return f'''
<p><img src="{esc_html(DEFAULT_SEO_IMAGE_URL)}" alt="{esc_html(focus_keyword)}" style="max-width:100%;height:auto;" /></p>
'''.strip()

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
# WordPress 발행 HTML
# =========================
def build_preopen_html(summary: Dict[str, Any], market_data: Dict[str, Any], news_texts: Dict[str, str], seo: Dict[str, str]) -> str:
    us = market_data.get("us_markets", {})
    fx = market_data.get("fx", {})

    dow = us.get("dow", {})
    sp500 = us.get("sp500", {})
    nasdaq = us.get("nasdaq", {})
    sox = us.get("sox", {})
    usdkrw = fx.get("usdkrw", {})

    focus_keyword = seo["focus_keyword"]
    full_text_html = nl2br(summary.get("full_text", ""))
    tomorrow_html = nl2br(summary.get("tomorrow_points", ""))

    faqs = [
        (
            "오늘 한국 증시는 어떤 흐름으로 출발할 가능성이 있나요?",
            "오늘 한국 증시는 미국 증시와 SOX, 원달러 환율 흐름의 영향을 받을 가능성이 큽니다. 특히 나스닥과 SOX 움직임은 반도체와 AI 관련주 방향에 직접적인 힌트를 줄 수 있습니다."
        ),
        (
            "원달러 환율은 왜 중요한가요?",
            "원달러 환율은 외국인 수급과 성장주 심리에 영향을 줄 수 있습니다. 환율이 안정되면 위험자산 선호가 개선될 수 있지만 다시 급등하면 변동성이 커질 수 있습니다."
        ),
        (
            "개장 전에 가장 먼저 체크해야 할 것은 무엇인가요?",
            "개장 전에는 미국 증시 마감 흐름, SOX, 원달러 환율, 밤사이 핵심 뉴스 3개, 그리고 장 시작 직후 외국인 수급 변화를 함께 보는 것이 좋습니다."
        ),
    ]

    faq_schema = build_faq_schema_html(faqs)
    internal_links_html = build_internal_links_html()
    external_links_html = build_external_links_html("pre_open")
    toc_html = build_toc_html("pre_open", focus_keyword)
    image_html = build_image_html(focus_keyword)

    intro = (
        f"{focus_keyword}입니다. 미국 증시와 SOX, 원달러 환율, 밤사이 핵심 뉴스 3개를 바탕으로 "
        f"오늘 주식시장 전망을 정리했습니다. 이번 {focus_keyword}에서는 반도체와 AI 관련주, "
        f"외국인 수급, 환율 민감 업종까지 함께 살펴봅니다."
    )

    return f"""
<h1>{esc_html(seo['seo_title'])}</h1>

<p><strong>{esc_html(intro)}</strong></p>

{image_html}

<p>{AD_SHORTCODE}</p>

{toc_html}

<h2 id="summary">{esc_html(focus_keyword)} 핵심 요약</h2>
<p>{full_text_html}</p>

<h2 id="us-market">{esc_html(focus_keyword)} 미국 증시 정리</h2>
<ul>
  <li>다우: {fmt_price(dow.get('close'))} ({fmt_pct(dow.get('pct'))})</li>
  <li>S&amp;P500: {fmt_price(sp500.get('close'))} ({fmt_pct(sp500.get('pct'))})</li>
  <li>나스닥: {fmt_price(nasdaq.get('close'))} ({fmt_pct(nasdaq.get('pct'))})</li>
  <li>SOX: {fmt_price(sox.get('close'))} ({fmt_pct(sox.get('pct'))})</li>
  <li>원/달러: {fmt_price(usdkrw.get('close'))} ({fmt_pct(usdkrw.get('pct'))})</li>
</ul>

<h2 id="sox-impact">{esc_html(focus_keyword)} SOX와 반도체 영향</h2>
<p>{esc_html(summary.get('strong_sectors', ''))}</p>
<p>{esc_html(focus_keyword)} 관점에서 보면 SOX와 나스닥 흐름은 국내 반도체와 AI 관련주의 방향을 읽는 데 중요합니다. 이번 {esc_html(focus_keyword)}에서도 이 부분을 핵심으로 봅니다.</p>

<h2 id="news-top3">{esc_html(focus_keyword)} 밤사이 핵심 뉴스 3개</h2>
<ol>
  <li>{esc_html(news_texts.get('news_1', ''))}</li>
  <li>{esc_html(news_texts.get('news_2', ''))}</li>
  <li>{esc_html(news_texts.get('news_3', ''))}</li>
</ol>

<p>{AD_SHORTCODE}</p>

<h2>{esc_html(focus_keyword)} 오늘 관심 업종</h2>
<p>{esc_html(summary.get('strong_sectors', ''))}</p>

<h2>{esc_html(focus_keyword)} 오늘 주의 업종</h2>
<p>{esc_html(summary.get('weak_sectors', ''))}</p>

<h2 id="check-points">{esc_html(focus_keyword)} 장 시작 전 체크 포인트</h2>
<p>{tomorrow_html}</p>

<h2>{esc_html(focus_keyword)} 코스피 전망</h2>
<p>{esc_html(summary.get('kospi_text', ''))}</p>

<h2>{esc_html(focus_keyword)} 코스닥 전망</h2>
<p>{esc_html(summary.get('kosdaq_text', ''))}</p>

<h2>자주 묻는 질문</h2>
<h3>{esc_html(faqs[0][0])}</h3>
<p>{esc_html(faqs[0][1])}</p>

<h3>{esc_html(faqs[1][0])}</h3>
<p>{esc_html(faqs[1][1])}</p>

<h3>{esc_html(faqs[2][0])}</h3>
<p>{esc_html(faqs[2][1])}</p>

{internal_links_html}

{external_links_html}

{faq_schema}
""".strip()

def build_close_html(summary: Dict[str, Any], seo: Dict[str, str]) -> str:
    focus_keyword = seo["focus_keyword"]
    full_text_html = nl2br(summary.get("full_text", ""))
    tomorrow_html = nl2br(summary.get("tomorrow_points", ""))

    faqs = [
        (
            "오늘 장 마감에서 가장 중요했던 포인트는 무엇인가요?",
            "오늘 장 마감에서는 코스피와 코스닥 흐름, 강했던 업종과 약했던 업종, 그리고 내일로 이어질 수 있는 수급 변화가 핵심 포인트입니다."
        ),
        (
            "내일 시장을 볼 때 가장 먼저 확인해야 할 것은 무엇인가요?",
            "내일 시장은 미국 증시, 환율, 외국인 수급, 업종별 강도 변화를 먼저 확인하는 것이 좋습니다."
        ),
    ]

    faq_schema = build_faq_schema_html(faqs)
    internal_links_html = build_internal_links_html()
    external_links_html = build_external_links_html("close")
    toc_html = build_toc_html("close", focus_keyword)
    image_html = build_image_html(focus_keyword)

    intro = (
        f"{focus_keyword}입니다. 오늘 장 마감 흐름과 코스피, 코스닥, 강했던 업종과 약했던 업종, "
        f"그리고 내일 체크포인트까지 한 번에 정리했습니다."
    )

    return f"""
<h1>{esc_html(seo['seo_title'])}</h1>

<p><strong>{esc_html(intro)}</strong></p>

{image_html}

<p>{AD_SHORTCODE}</p>

{toc_html}

<h2 id="summary">{esc_html(focus_keyword)} 요약</h2>
<p>{full_text_html}</p>

<h2 id="kospi-close">{esc_html(focus_keyword)} 코스피 해설</h2>
<p>{esc_html(summary.get('kospi_text', ''))}</p>

<h2 id="kosdaq-close">{esc_html(focus_keyword)} 코스닥 해설</h2>
<p>{esc_html(summary.get('kosdaq_text', ''))}</p>

<h2>{esc_html(focus_keyword)} 강했던 업종</h2>
<p>{esc_html(summary.get('strong_sectors', ''))}</p>

<h2>{esc_html(focus_keyword)} 약했던 업종</h2>
<p>{esc_html(summary.get('weak_sectors', ''))}</p>

<p>{AD_SHORTCODE}</p>

<h2 id="check-points">{esc_html(focus_keyword)} 내일 체크 포인트</h2>
<p>{tomorrow_html}</p>

<h2>자주 묻는 질문</h2>
<h3>{esc_html(faqs[0][0])}</h3>
<p>{esc_html(faqs[0][1])}</p>

<h3>{esc_html(faqs[1][0])}</h3>
<p>{esc_html(faqs[1][1])}</p>

{internal_links_html}

{external_links_html}

{faq_schema}
""".strip()

def build_wp_html(summary: Dict[str, Any], market_data: Dict[str, Any], news_texts: Dict[str, str], stage: str, seo: Dict[str, str]) -> str:
    if stage == "pre_open":
        return build_preopen_html(summary, market_data, news_texts, seo)
    return build_close_html(summary, seo)

# =========================
# WordPress 발행
# =========================
def publish_wordpress(summary: Dict[str, Any], market_data: Dict[str, Any], news_texts: Dict[str, str], stage: str, seo: Dict[str, str]) -> Tuple[str, str]:
    if not WP_URL:
        raise RuntimeError("WP_URL/WP_SITE_URL이 비어 있습니다.")
    if not WP_USERNAME:
        raise RuntimeError("WP_USERNAME이 비어 있습니다.")
    if not WP_APP_PASSWORD:
        raise RuntimeError("WP_APP_PASSWORD가 비어 있습니다.")

    endpoint = f"{WP_URL.rstrip('/')}/wp-json/wp/v2/posts"
    auth = (WP_USERNAME, WP_APP_PASSWORD)

    form_data = [
        ("title", seo["seo_title"]),
        ("content", build_wp_html(summary, market_data, news_texts, stage, seo)),
        ("status", "publish"),
        ("slug", seo["slug"]),
        ("excerpt", seo["meta_description"]),
        ("meta[rank_math_description]", seo["meta_description"]),
        ("meta[rank_math_focus_keyword]", seo["focus_keyword"]),
        ("meta[_yoast_wpseo_metadesc]", seo["meta_description"]),
        ("meta[_aioseo_description]", seo["meta_description"]),
    ]

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    print("[publish_wordpress] endpoint:", endpoint)
    print("[publish_wordpress] title:", seo["seo_title"])
    print("[publish_wordpress] slug:", seo["slug"])
    print("[publish_wordpress] meta_description:", seo["meta_description"])
    print("[publish_wordpress] focus_keyword:", seo["focus_keyword"])

    res = requests.post(
        endpoint,
        auth=auth,
        data=form_data,
        headers=headers,
        timeout=30,
    )

    print("[publish_wordpress] status:", res.status_code)
    print("[publish_wordpress] response text:", res.text[:1000])

    res.raise_for_status()
    data = res.json()

    wp_title = (
        data.get("title", {}).get("rendered")
        if isinstance(data.get("title"), dict)
        else seo["seo_title"]
    )
    wp_link = data.get("link") or ""

    if not wp_link:
        raise RuntimeError("워드프레스 응답에 link가 없습니다.")

    print("[publish_wordpress] success link:", wp_link)
    return wp_title or seo["seo_title"], wp_link

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

    seo = build_seo_assets(FORCE_STAGE, market_data, summary)
    print("[seo]")
    print(json.dumps(seo, ensure_ascii=False, indent=2))

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

    if FORCE_STAGE in ("pre_open", "close"):
        wp_title, wp_link = publish_wordpress(summary, market_data, news_texts, FORCE_STAGE, seo)
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
