from fastapi import FastAPI
import requests

app = FastAPI()

SUPABASE_URL = "https://wqqfqinhiytntextpjey.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndxcWZxaW5oaXl0bnRleHRwamV5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUwMzIyNjksImV4cCI6MjA5MDYwODI2OX0.Cm8FtLiuzbv3sU0_TarpK_mowHqAbUB3xZFW0d2foNQ"

@app.post("/kakao/market-summary")
async def market_summary():
    url = f"{SUPABASE_URL}/rest/v1/market_summaries?select=*&order=summary_date.desc&limit=1"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    response = requests.get(url, headers=headers)
    data = response.json()

    latest = data[0]
    text = latest["full_text"]

    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": text
                    }
                },
                {
                    "basicCard": {
                        "title": "📊 경제지표 자세히 보기",
                        "description": "환율, 금리, 글로벌 흐름을 블로그에서 확인하세요.",
                        "buttons": [
                            {
                                "action": "webLink",
                                "label": "경제지표 바로가기",
                                "webLinkUrl": "https://moneycalc.wikitreee.com/2026/03/12/%ec%8b%a4%ec%8b%9c%ea%b0%84-%ea%b8%88-%ec%9d%80-%ec%9b%90%ec%9c%a0-%eb%8b%ac%eb%9f%ac-%ec%8b%9c%ec%84%b8-%ea%b8%80%eb%a1%9c%eb%b2%8c-%ea%b2%bd%ec%a0%9c%ec%a7%80%ec%88%98-%eb%8c%80%ec%8b%9c%eb%b3%b4/"
                            }
                        ]
                    }
                }
            ]
        }
    }
