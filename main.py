import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, Tuple

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

WP_URL = os.getenv("WP_URL", "").strip()
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

# =========================
# stage 매핑
# =========================
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

def safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default

def get_ticker_snapshot(ticker: str, name: str) -> Dict[str, Any]:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d", interval="1d", auto_adjust=False)

        if hist is None or hist.empty:
            return {
                "name": name,
                "ticker": ticker,
                "error": "no_data"
            }

        last = hist.iloc[-1]
        close = safe_float(last.get("Close"))

        if len(hist) >= 2:
            prev_close = safe_float(hist.iloc[-2].get("Close"), close)
        else:
            prev_close = close

        change = close - prev_close
        pct = (change / prev_close * 100.0) if prev_close else 0.0

        return {
            "name": name,
            "ticker": ticker,
            "close": round(close, 4),
            "prev_close": round(prev_close, 4),
            "change": round(change, 4),
            "pct": round(pct, 2),
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
- 실제 뉴스레터/증권사 브리핑처럼 간결하고 읽기 쉽게 작성
- 한글로 작성
- 이모지 금지
- 투자 권유 문구 금지
- 숫자를 언급할 때는 가능하면 제공된 데이터 기반으로 작성
"""

    if stage == "pre_open":
        return common + """
이번 글은 "개장 전 브리핑"이다.

역할 규칙:
- 반드시 밤사이 미국장 흐름을 중심으로 작성
- 나스닥, S&P500, 다우, 환율, 위험자산 심리, 밤사이 변수 위주로 정리
- 아직 한국장이 시작하지 않았다는 점이 문장에 자연스럽게 드러나야 함
- 코스피/코스닥 텍스트도 "오늘 한국장에 미칠 가능성" 관점으로 작성
- strong_sectors / weak_sectors는 "관심 업종" 또는 "주의 업종" 느낌으로 작성
- tomorrow_points에는 "오늘 장에서 체크할 변수"를 작성
- full_text는 반드시 "밤사이 미국 증시와 환율 흐름이 오늘 한국장에 어떤 영향을 줄지" 중심으로 작성
"""

    if stage == "intraday":
        return common + """
이번 글은 "오전 시황"이다.

역할 규칙:
- 오전 실제 흐름 중심으로 작성
- 코스피/코스닥의 오전 움직임, 강세 업종, 약세 업종, 수급 분위기 중심
- 개장 전 브리핑처럼 미국장 중심으로 쓰지 말 것
- 마감 총평처럼 하루 전체 결론으로 쓰지 말 것
- tomorrow_points에는 "오후장 체크 포인트"를 작성
- full_text는 반드시 "오전 장세 해설"이어야 함
"""

    if stage == "close":
        return common + """
이번 글은 "장 마감 시황"이다.

역할 규칙:
- 하루 전체를 정리하는 총평 형식으로 작성
- 코스피/코스닥 마감 흐름, 강세 업종, 약세 업종, 하루 해석을 담을 것
- tomorrow_points에는 "내일 체크 포인트"를 작성
- full_text는 "오늘 시장을 한 번에 정리하는 마감 브리핑"이어야 함
- post_title은 워드프레스 발행용 제목이므로 자연스럽고 클릭 가능한 제목으로 작성
"""

    raise ValueError(f"Unknown stage: {stage}")

def build_user_prompt(stage: str, market_data: Dict[str, Any]) -> str:
    return f"""
오늘 날짜(KST): {today_str()}
현재 stage: {stage}

시장 데이터(JSON):
{json.dumps(market_data, ensure_ascii=False, indent=2)}

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
def extract_json_text(content) -> str:
    if content is None:
        return ""

    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts = []
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

def generate_summary(stage: str, market_data: Dict[str, Any]) -> Dict[str, Any]:
    system_prompt = build_system_prompt(stage)
    user_prompt = build_user_prompt(stage, market_data)

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
    try:
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

    except Exception as e:
        raise RuntimeError(f"Supabase 저장 실패: {e}")

# =========================
# WordPress 발행
# =========================
def build_wp_html(summary: Dict[str, Any]) -> str:
    full_text_html = str(summary.get("full_text", "")).replace("\n", "<br>")

    return f"""
<h2>{summary.get('one_line', '')}</h2>
<p>{full_text_html}</p>

<h3>코스피</h3>
<p>{summary.get('kospi_text', '')}</p>

<h3>코스닥</h3>
<p>{summary.get('kosdaq_text', '')}</p>

<h3>강한 업종</h3>
<p>{summary.get('strong_sectors', '')}</p>

<h3>약한 업종</h3>
<p>{summary.get('weak_sectors', '')}</p>

<h3>내일 체크 포인트</h3>
<p>{summary.get('tomorrow_points', '')}</p>
""".strip()

def publish_wordpress(summary: Dict[str, Any]) -> Tuple[str, str]:
    if not WP_URL:
        raise RuntimeError("WP_URL이 비어 있습니다.")
    if not WP_USERNAME:
        raise RuntimeError("WP_USERNAME이 비어 있습니다.")
    if not WP_APP_PASSWORD:
        raise RuntimeError("WP_APP_PASSWORD가 비어 있습니다.")

    endpoint = f"{WP_URL.rstrip('/')}/wp-json/wp/v2/posts"
    auth = (WP_USERNAME, WP_APP_PASSWORD)

    payload = {
        "title": summary["post_title"],
        "content": build_wp_html(summary),
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
def main():
    print(f"[START] FORCE_STAGE={FORCE_STAGE}, MARKET_TYPE={MARKET_TYPE}, date={today_str()}")

    market_data = collect_market_data()
    print("[market_data]")
    print(json.dumps(market_data, ensure_ascii=False, indent=2))

    summary = generate_summary(FORCE_STAGE, market_data)
    print("[summary]")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    post_title = summary.get("post_title", "")
    post_url = ""

    if FORCE_STAGE == "close":
        wp_title, wp_link = publish_wordpress(summary)
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
    }

    upsert_summary(row)
    print("[DONE] summary saved successfully")

if __name__ == "__main__":
    main()
