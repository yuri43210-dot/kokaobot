import os
from fastapi import FastAPI
import requests

app = FastAPI()

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()
DEFAULT_BLOG_URL = os.environ.get("DEFAULT_BLOG_URL", "https://moneycalc.wikitreee.com").strip()

@app.get("/")
async def root():
    return {"message": "kakaobot server running"}

@app.post("/kakao/market-summary")
async def market_summary():
    url = f"{SUPABASE_URL}/rest/v1/market_summaries?select=*&order=summary_date.desc&limit=1"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    response = requests.get(url, headers=headers, timeout=20)
    data = response.json()

    latest = data[0]
    text = latest["full_text"]
    post_url = latest.get("post_url") or DEFAULT_BLOG_URL

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
                        "title": "📊 오늘 시장 풀분석 보기",
                        "description": "방금 발행된 최신 시장 분석 글로 이동합니다.",
                        "buttons": [
                            {
                                "action": "webLink",
                                "label": "최신 분석글 바로가기",
                                "webLinkUrl": post_url
                            }
                        ]
                    }
                }
            ]
        }
    }
