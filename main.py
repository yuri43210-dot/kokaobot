from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/")
def home():
    return {"message": "kakaobot server running"}

@app.get("/webhook")
def webhook_check():
    return {"message": "webhook route exists"}

@app.post("/webhook")
async def webhook(request: Request):
    body = await request.json()
    print("kakao request:", body)

    return JSONResponse({
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "정상 연결됨 ✅"
                    }
                }
            ]
        }
    })
