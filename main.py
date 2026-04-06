import os
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()
DEFAULT_BLOG_URL = os.environ.get("DEFAULT_BLOG_URL", "https://moneycalc.wikitreee.com").strip()


@app.get("/")
async def root():
    return {"message": "root ok"}


@app.get("/ping")
async def ping():
    return {"message": "pong"}


@app.get("/webhook")
async def webhook_check():
    return {"message": "webhook route exists"}


@app.post("/kakao/market-summary")
async def market_summary():
    try:
        url = f"{SUPABASE_URL}/rest/v1/market_summaries?select=*&order=summary_date.desc&limit=1"

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }

        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()

        if not data or len(data) == 0:
            return JSONResponse({
                "version": "2.0",
                "template": {
                    "outputs": [
                        {
                            "simpleText": {
                                "text": "아직 등록된 시장 요약 데이터가 없습니다."
                            }
                        }
                    ]
                }
            })

        latest = data[0]
        text = (latest.get("full_text") or "오늘 시장 요약 데이터가 비어 있습니다.").strip()
        post_url = (latest.get("post_url") or DEFAULT_BLOG_URL).strip()

        # 카카오 응답 길이 안정성 확보용
        if len(text) > 950:
            text = text[:950] + "\n\n...아래 버튼에서 전체 분석을 확인하세요."

        return JSONResponse({
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
        })

    except Exception as e:
        print("market_summary error:", str(e))

        return JSONResponse({
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "시장 요약을 불러오는 중 문제가 발생했습니다. 잠시 후 다시 시도해주세요."
                        }
                    },
                    {
                        "basicCard": {
                            "title": "📊 최신 분석글 보기",
                            "description": "시장 분석 페이지로 바로 이동할 수 있습니다.",
                            "buttons": [
                                {
                                    "action": "webLink",
                                    "label": "분석글 바로가기",
                                    "webLinkUrl": DEFAULT_BLOG_URL
                                }
                            ]
                        }
                    }
                ]
            }
        })
