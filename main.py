from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/")
def home():
    return {"message": "kakaobot server running"}

@app.post("/webhook")
async def webhook():
    return JSONResponse({
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "웹훅 응답 ✅"
                    }
                }
            ]
        }
    })

@app.post("/kakao/market-summary")
async def market_summary():
    return JSONResponse({
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "오늘 시장 요약입니다 📈"
                    }
                }
            ]
        }
    })
