import os
import requests
from datetime import date, timedelta

import FinanceDataReader as fdr
import pandas as pd

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TABLE_NAME = "market_summaries"


def get_latest_change(symbol: str, name: str):
    start = date.today() - timedelta(days=10)
    df = fdr.DataReader(symbol, start.strftime("%Y-%m-%d"))

    if df is None or df.empty or len(df) < 2:
        raise ValueError(f"{name} 데이터를 충분히 가져오지 못했습니다.")

    df = df.dropna().copy()
    last = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(last["Close"])
    prev_close = float(prev["Close"])
    change_pct = ((close - prev_close) / prev_close) * 100

    direction = "상승" if change_pct > 0 else "하락" if change_pct < 0 else "보합"
    return {
        "name": name,
        "close": round(close, 2),
        "change_pct": round(change_pct, 2),
        "direction": direction,
    }


def get_fx_change():
    start = date.today() - timedelta(days=10)
    df = fdr.DataReader("USD/KRW", start.strftime("%Y-%m-%d"))

    if df is None or df.empty or len(df) < 2:
        return None

    df = df.dropna().copy()
    last = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(last["Close"])
    prev_close = float(prev["Close"])
    change_pct = ((close - prev_close) / prev_close) * 100

    direction = "상승" if change_pct > 0 else "하락" if change_pct < 0 else "보합"
    return {
        "close": round(close, 2),
        "change_pct": round(change_pct, 2),
        "direction": direction,
    }


def build_summary_text() -> dict:
    today = str(date.today())

    kospi = get_latest_change("KS11", "코스피")
    kosdaq = get_latest_change("KQ11", "코스닥")
    fx = get_fx_change()

    one_line = (
        f"오늘 시장은 코스피 {kospi['direction']}, 코스닥 {kosdaq['direction']} 흐름을 보였습니다."
    )

    kospi_text = (
        f"코스피는 {kospi['close']}에 마감했고 전일 대비 {kospi['change_pct']}% "
        f"{'올랐습니다' if kospi['change_pct'] > 0 else '내렸습니다' if kospi['change_pct'] < 0 else '보합이었습니다'}."
    )

    kosdaq_text = (
        f"코스닥은 {kosdaq['close']}에 마감했고 전일 대비 {kosdaq['change_pct']}% "
        f"{'올랐습니다' if kosdaq['change_pct'] > 0 else '내렸습니다' if kosdaq['change_pct'] < 0 else '보합이었습니다'}."
    )

    strong_sectors = "반도체, AI 관련주"
    weak_sectors = "2차전지"
    tomorrow_points = "미국 증시 흐름, 환율, 외국인 수급"

    fx_line = ""
    if fx:
        fx_line = f"\n- 원달러 환율: {fx['close']} ({fx['direction']}, {fx['change_pct']}%)"

    full_text = f"""🌳 [오늘의 장마감 요약]

📌 오늘의 한줄
{one_line}

📊 지수 흐름
- 코스피: {kospi['close']} ({kospi['change_pct']}%)
- 코스닥: {kosdaq['close']} ({kosdaq['change_pct']}%){fx_line}

🔥 강한 가지
- {strong_sectors}

🍂 약한 가지
- {weak_sectors}

📅 내일 체크
- {tomorrow_points}
"""

    return {
        "summary_date": today,
        "market_type": "kr_stock",
        "one_line": one_line,
        "kospi_text": kospi_text,
        "kosdaq_text": kosdaq_text,
        "strong_sectors": strong_sectors,
        "weak_sectors": weak_sectors,
        "tomorrow_points": tomorrow_points,
        "full_text": full_text,
    }


def upsert_summary():
    row = build_summary_text()
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?on_conflict=summary_date,market_type"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    response = requests.post(url, headers=headers, json=row)
    print("status:", response.status_code)
    print("response:", response.text)

    if response.status_code not in [200, 201]:
        raise Exception(f"Supabase update failed: {response.status_code} {response.text}")


if __name__ == "__main__":
    upsert_summary()
