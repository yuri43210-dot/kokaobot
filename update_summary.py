import requests
from datetime import date

SUPABASE_URL = "https://wqqfqinhiytntextpjey.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndxcWZxaW5oaXl0bnRleHRwamV5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUwMzIyNjksImV4cCI6MjA5MDYwODI2OX0.Cm8FtLiuzbv3sU0_TarpK_mowHqAbUB3xZFW0d2foNQ"

TABLE_NAME = "market_summaries"


def build_summary_text() -> dict:
    today = str(date.today())

    one_line = "오늘 시장은 반도체 중심으로 강한 흐름을 보였습니다."
    kospi_text = "코스피는 외국인 매수와 대형주 강세로 상승 마감했습니다."
    kosdaq_text = "코스닥은 보합권에서 혼조 흐름을 보였습니다."
    strong_sectors = "반도체, AI 관련주, 자동차"
    weak_sectors = "2차전지"
    tomorrow_points = "미국 증시 흐름, 환율, 외국인 수급 지속 여부"

    full_text = f"""🌳 [오늘의 장마감 요약]

📌 오늘의 한줄
{one_line}

📊 지수 흐름
- 코스피: 상승 마감
- 코스닥: 보합권 마감

🔥 강한 가지
- 반도체
- AI 관련주
- 자동차

📅 내일 체크
- 미국 증시 흐름
- 환율
- 외국인 수급 지속 여부"""

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
        "Prefer": "resolution=merge-duplicates"
    }

    response = requests.post(url, headers=headers, json=row)

    print("status:", response.status_code)
    print("response:", response.text)


if __name__ == "__main__":
    upsert_summary()