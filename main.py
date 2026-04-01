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
                }
            ]
        }
    }