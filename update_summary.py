import os
import json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import requests
import FinanceDataReader as fdr
import pandas as pd

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

TABLE_NAME = "market_summaries"
KST = ZoneInfo("Asia/Seoul")

# 섹터별 대표 종목
SECTOR_STOCKS = {
    "반도체": ["005930", "000660", "042700", "000990", "240810"],
    "2차전지": ["373220", "247540", "003670", "066970", "003490"],
    "AI": ["035420", "035720", "304100", "318000", "047560"],
    "자동차": ["005380", "000270", "012330", "204320"],
    "바이오": ["207940", "068270", "196170", "028300", "000100"],
    "게임": ["259960", "036570", "251270", "263750"],
    "조선": ["009540", "042660", "010140", "329180"],
}

DEFAULT_STRONG = ["데이터 확인 중"]
DEFAULT_WEAK = ["데이터 확인 중"]


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


def get_stock_change_pct(ticker: str) -> float | None:
    try:
        start = date.today() - timedelta(days=10)
        df = fdr.DataReader(ticker, start.strftime("%Y-%m-%d"))

        if df is None or df.empty:
            return None

        df = df.dropna().copy()
        if len(df) < 2:
            return None

        prev = float(df.iloc[-2]["Close"])
        last = float(df.iloc[-1]["Close"])

        if prev == 0:
            return None

        return round(((last - prev) / prev) * 100, 2)
    except Exception:
        return None


def infer_sectors_from_representatives() -> tuple[list[str], list[str], dict]:
    sector_scores = {}

    for sector, tickers in SECTOR_STOCKS.items():
        changes = []

        for ticker in tickers:
            pct = get_stock_change_pct(ticker)
            if pct is not None:
                changes.append(pct)

        if changes:
            sector_scores[sector] = round(sum(changes) / len(changes), 2)

    if not sector_scores:
        return DEFAULT_STRONG, DEFAULT_WEAK, {}

    sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)

    strong = [name for name, score in sorted_sectors[:2]]
    weak = [name for name, score in sorted_sectors[-2:]]

    return strong, weak, sector_scores


def build_raw_summary() -> dict:
    now = now_kst()
    stage = market_stage(now)

    kospi = get_index_change("KS11", "코스피")
    kosdaq = get_index_change("KQ11", "코스닥")
    fx = get_fx_change()

    strong_sectors, weak_sectors, sector_scores = infer_sectors_from_representatives()

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

    strong_lines = []
    for sector in strong_sectors:
        score = sector_scores.get(sector)
        if score is not None:
            strong_lines.append(f"- {sector} ({score}%)")
        else:
            strong_lines.append(f"- {sector}")

    weak_lines = []
    for sector in weak_sectors:
        score = sector_scores.get(sector)
        if score is not None:
            weak_lines.append(f"- {sector} ({score}%)")
        else:
            weak_lines.append(f"- {sector}")

    full_text = f"""🌳 [{title}]

📌 오늘의 한줄
{one_line}

📊 지수 흐름
- 코스피: {kospi['close']} ({kospi['change_pct']}%)
- 코스닥: {kosdaq['close']} ({kosdaq['change_pct']}%){fx_line}

🔥 강한 섹터
{chr(10).join(strong_lines)}

🍂 약한 섹터
{chr(10).join(weak_lines)}

📅 {check_label}
- 미국 증시 흐름
- 환율
- 외국인 수급

👇 아래에서 시장 풀분석도 확인하세요
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
            "sector_scores": sector_scores,
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
  🔥 강한 섹터
  🍂 약한 섹터
  📅 체크포인트
- strong_sectors와 weak_sectors는 입력 데이터 기준으로 반영
- 문장은 자연스럽게 다듬되 숫자는 유지

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
