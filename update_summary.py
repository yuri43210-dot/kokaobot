import os
import json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from collections import Counter

import requests
import FinanceDataReader as fdr
import pandas as pd
from pykrx import stock

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

TABLE_NAME = "market_summaries"
KST = ZoneInfo("Asia/Seoul")

# 종목명 기반 간단 섹터 키워드 매핑
SECTOR_KEYWORDS = {
    "반도체": ["삼성전자", "SK하이닉스", "한미반도체", "DB하이텍", "리노공업", "원익IPS"],
    "2차전지": ["에코프로", "에코프로비엠", "포스코퓨처엠", "엘앤에프", "금양", "LG에너지솔루션"],
    "AI": ["네이버", "카카오", "솔트룩스", "폴라리스오피스", "이스트소프트", "플리토"],
    "자동차": ["현대차", "기아", "현대모비스", "HL만도"],
    "바이오": ["삼성바이오로직스", "셀트리온", "알테오젠", "HLB", "유한양행"],
    "게임": ["크래프톤", "엔씨소프트", "넷마블", "펄어비스"],
    "조선": ["HD한국조선해양", "한화오션", "삼성중공업", "HD현대중공업"],
}

DEFAULT_STRONG = ["반도체", "AI 관련주"]
DEFAULT_WEAK = ["2차전지"]


def now_kst() -> datetime:
    return datetime.now(KST)


def market_stage(now: datetime) -> str:
    hhmm = now.hour * 100 + now.minute
    if hhmm < 900:
        return "pre_open"
    if 900 <= hhmm < 1530:
        return "intraday"
    return "close"


def safe_direction(change_pct: float) -> str:
    if change_pct > 0:
        return "상승"
    if change_pct < 0:
        return "하락"
    return "보합"


def get_last_two_rows(symbol: str) -> pd.DataFrame:
    start = date.today() - timedelta(days=10)
    df = fdr.DataReader(symbol, start.strftime("%Y-%m-%d"))

    if df is None or df.empty:
        raise ValueError(f"{symbol} 데이터가 비어 있습니다.")

    df = df.dropna().copy()
    if len(df) < 2:
        raise ValueError(f"{symbol} 데이터가 충분하지 않습니다.")

    return df.tail(2)


def get_index_change(symbol: str, name: str) -> dict:
    try:
        df = get_last_two_rows(symbol)
        prev = df.iloc[0]
        last = df.iloc[1]

        close = float(last["Close"])
        prev_close = float(prev["Close"])
        change_pct = round(((close - prev_close) / prev_close) * 100, 2)

        return {
            "name": name,
            "close": round(close, 2),
            "change_pct": change_pct,
            "direction": safe_direction(change_pct),
        }
    except Exception:
        # 실패 시 기본값
        return {
            "name": name,
            "close": 0.0,
            "change_pct": 0.0,
            "direction": "보합",
        }


def get_fx_change() -> dict | None:
    try:
        df = get_last_two_rows("USD/KRW")
        prev = df.iloc[0]
        last = df.iloc[1]

        close = float(last["Close"])
        prev_close = float(prev["Close"])
        change_pct = round(((close - prev_close) / prev_close) * 100, 2)

        return {
            "close": round(close, 2),
            "change_pct": change_pct,
            "direction": safe_direction(change_pct),
        }
    except Exception:
        return None


def get_recent_business_day_str(days_back: int = 14) -> str | None:
    today = date.today()

    for i in range(days_back):
        candidate = today - timedelta(days=i)
        ymd = candidate.strftime("%Y%m%d")

        try:
            df = stock.get_market_price_change(ymd, ymd, market="KOSPI")
            if df is not None and not df.empty:
                return ymd
        except Exception:
            continue

    return None


def get_top_movers(market: str = "KOSPI", n: int = 20) -> pd.DataFrame:
    bizday = get_recent_business_day_str()

    if not bizday:
        return pd.DataFrame()

    try:
        df = stock.get_market_price_change(bizday, bizday, market=market)
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()

    # 컬럼명 방어 처리
    rename_map = {}
    if "티커" in df.columns:
        rename_map["티커"] = "ticker"
    if "종목명" in df.columns:
        rename_map["종목명"] = "name"
    df = df.rename(columns=rename_map)

    if "name" not in df.columns or "등락률" not in df.columns:
        return pd.DataFrame()

    df = df.sort_values("등락률", ascending=False).head(n)
    return df[[col for col in ["ticker", "name", "등락률"] if col in df.columns]]


def infer_sectors_from_movers() -> tuple[list[str], list[str]]:
    try:
        kospi_movers = get_top_movers("KOSPI", 15)
        kosdaq_movers = get_top_movers("KOSDAQ", 15)

        frames = []
        if not kospi_movers.empty:
            frames.append(kospi_movers)
        if not kosdaq_movers.empty:
            frames.append(kosdaq_movers)

        if not frames:
            return DEFAULT_STRONG, DEFAULT_WEAK

        movers = pd.concat(frames, ignore_index=True)
        strong_counter = Counter()

        for _, row in movers.iterrows():
            name = str(row.get("name", ""))
            for sector, keywords in SECTOR_KEYWORDS.items():
                if any(keyword in name for keyword in keywords):
                    strong_counter[sector] += 1

        strong = [sector for sector, _ in strong_counter.most_common(3)]
        if not strong:
            strong = DEFAULT_STRONG

        weak_candidates = ["2차전지", "바이오", "게임"]
        weak = [w for w in weak_candidates if w not in strong][:2]
        if not weak:
            weak = DEFAULT_WEAK

        return strong, weak

    except Exception:
        return DEFAULT_STRONG, DEFAULT_WEAK


def build_raw_summary() -> dict:
    now = now_kst()
    stage = market_stage(now)

    kospi = get_index_change("KS11", "코스피")
    kosdaq = get_index_change("KQ11", "코스닥")
    fx = get_fx_change()
    strong_sectors, weak_sectors = infer_sectors_from_movers()

    if stage == "pre_open":
        title = "오늘의 개장 전 체크"
        one_line = "개장 전 주요 변수와 최근 시장 흐름을 기준으로 장을 준비하는 시간입니다."
        check_label = "오늘 체크"
    elif stage == "intraday":
        title = "오늘의 장중 요약"
        one_line = f"장중 기준 코스피는 {kospi['direction']}, 코스닥은 {kosdaq['direction']} 흐름입니다."
        check_label = "남은 장 체크"
    else:
        title = "오늘의 장마감 요약"
        one_line = f"오늘 시장은 코스피 {kospi['direction']}, 코스닥 {kosdaq['direction']} 흐름을 보였습니다."
        check_label = "내일 체크"

    fx_line = ""
    if fx:
        fx_line = f"\n- 원달러 환율: {fx['close']} ({fx['direction']}, {fx['change_pct']}%)"

    full_text = f"""🌳 [{title}]

📌 오늘의 한줄
{one_line}

📊 지수 흐름
- 코스피: {kospi['close']} ({kospi['change_pct']}%)
- 코스닥: {kosdaq['close']} ({kosdaq['change_pct']}%){fx_line}

🔥 강한 가지
- {' / '.join(strong_sectors)}

🍂 약한 가지
- {' / '.join(weak_sectors)}

📅 {check_label}
- 미국 증시 흐름
- 환율
- 외국인 수급
"""

    return {
        "summary_date": str(date.today()),
        "market_type": "kr_stock",
        "one_line": one_line,
        "kospi_text": f"코스피는 {kospi['close']}에서 {kospi['change_pct']}% {kospi['direction']} 흐름입니다.",
        "kosdaq_text": f"코스닥은 {kosdaq['close']}에서 {kosdaq['change_pct']}% {kosdaq['direction']} 흐름입니다.",
        "strong_sectors": ", ".join(strong_sectors),
        "weak_sectors": ", ".join(weak_sectors),
        "tomorrow_points": "미국 증시 흐름, 환율, 외국인 수급",
        "full_text": full_text,
        "structured": {
            "title": title,
            "stage": stage,
            "one_line": one_line,
            "kospi": kospi,
            "kosdaq": kosdaq,
            "fx": fx,
            "strong_sectors": strong_sectors,
            "weak_sectors": weak_sectors,
            "check_label": check_label,
        },
    }


def polish_with_gpt(raw: dict) -> str:
    if not OPENAI_API_KEY:
        return raw["full_text"]

    try:
        from openai import OpenAI

        client = OpenAI(api_key=OPENAI_API_KEY)

        prompt = f"""
너는 한국 주식시장 요약 에디터다.
아래 JSON 데이터를 바탕으로 카카오 챗봇용 한국어 시장 요약문을 작성해라.

조건:
- 초보자도 이해 가능
- 과장 금지
- 투자 권유 금지
- 8~12줄 정도
- 형식 유지:
  🌳 [제목]
  📌 오늘의 한줄
  📊 지수 흐름
  🔥 강한 가지
  🍂 약한 가지
  📅 체크포인트

입력 데이터:
{json.dumps(raw["structured"], ensure_ascii=False)}
"""

        response = client.responses.create(
            model="gpt-5.4-mini",
            input=prompt,
        )
        return response.output_text.strip()

    except Exception:
        return raw["full_text"]


def upsert_summary():
    raw = build_raw_summary()
    final_text = polish_with_gpt(raw)

    row = {
        "summary_date": raw["summary_date"],
        "market_type": raw["market_type"],
        "one_line": raw["one_line"],
        "kospi_text": raw["kospi_text"],
        "kosdaq_text": raw["kosdaq_text"],
        "strong_sectors": raw["strong_sectors"],
        "weak_sectors": raw["weak_sectors"],
        "tomorrow_points": raw["tomorrow_points"],
        "full_text": final_text,
    }

    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?on_conflict=summary_date,market_type"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    response = requests.post(url, headers=headers, json=row, timeout=30)

    print("status:", response.status_code)
    print("response:", response.text)

    if response.status_code not in [200, 201]:
        raise Exception(f"Supabase update failed: {response.status_code} {response.text}")


if __name__ == "__main__":
    upsert_summary()
