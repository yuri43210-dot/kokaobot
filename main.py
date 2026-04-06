import os
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()
DEFAULT_BLOG_URL = os.environ.get("DEFAULT_BLOG_URL", "https://moneycalc.wikitreee.com").strip()

TABLE_NAME = "market_summaries"


def build_summary_text(latest: dict) -> str:
    full_text = (latest.get("full_text") or "").strip()
    if full_text:
        return full_text

    one_line = (latest.get("one_line") or "").strip()
    kospi_text = (latest.get("kospi_text") or "").strip()
    kosdaq_text = (latest.get("kosdaq_text") or "").strip()
    strong_sectors = (latest.get("strong_sectors") or "").strip()
    weak_sectors = (latest.get("weak_sectors") or "").strip()
    tomorrow_points = (latest.get("tomorrow_points") or "").strip()

    market_type = (latest.get("market_type") or "").strip()
    title_map = {
        "kr_stock_preopen": "📌 [오늘의 개장 전 체크]",
        "kr_stock_morning": "📌 [오늘의 오전 시황]",
        "kr_stock_close": "📌 [오늘의 장마감 요약]",
    }
    title = title_map.get(market_type, "📌 [오늘의 시장 체크]")

    parts = [title]

    if one_line:
        parts.append(f"\n☀ 오늘의 한줄\n{one_line}")

    if kospi_text or kosdaq_text:
        section = ["\n📊 지수 흐름"]
        if kospi_text:
            section.append(f"- {kospi_text}")
        if kosdaq_text:
            section.append(f"- {kosdaq_text}")
        parts.append("\n".join(section))

    if strong_sectors:
        strong_lines = [x.strip() for x in strong_sectors.split(",") if x.strip()]
        if strong_lines:
            section = ["\n🔥 강한 섹터"]
            for item in strong_lines:
                section.append(f"- {item}")
            parts.append("\n".join(section))

    if weak_sectors:
        weak_lines = [x.strip() for x in weak_sectors.split(",") if x.strip()]
        if weak_lines:
            section = ["\n🍂 약한 섹터"]
            for item in weak_lines:
                section.append(f"- {item}")
            parts.append("\n".join(section))

    if tomorrow_points:
        parts.append(f"\n🎯 체크 포인트\n{tomorrow_points}")

    result = "\n".join(parts).strip()
    return result or "오늘 시장 요약 데이터가 비어 있습니다."


def fetch_latest_summary(market_type: str) -> dict | None:
    url = (
        f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
        f"?select=*"
        f"&market_type=eq.{market_type}"
        f"&order=summary_date.desc,created_at.desc"
        f"&limit=1"
    )

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers, timeout=20)

    print("REQUEST URL:", url)
    print("STATUS:", response.status_code)
    print("RESPONSE TEXT:", response.text[:1000])

    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise Exception(f"Supabase response is not a list: {data}")

    if len(data) == 0:
        return None

    return data[0]


def kakao_response_from_row(latest: dict | None, empty_text: str) -> JSONResponse:
    if not latest:
        return JSONResponse({
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": empty_text
                        }
                    }
                ]
            }
        })

    text = build_summary_text(latest)
    post_url = (latest.get("post_url") or DEFAULT_BLOG_URL).strip()
    post_title = (latest.get("post_title") or "📊 오늘 시장 풀분석 보기").strip()

    if len(text) > 950:
        text = text[:950].rstrip() + "\n\n👉 아래 버튼에서 전체 분석을 확인하세요."

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
                        "title": post_title,
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


@app.get("/")
async def root():
    return {"message": "root ok"}


@app.get("/ping")
async def ping():
    return {"message": "pong"}


@app.get("/webhook")
async def webhook_check():
    return {"message": "webhook route exists"}


@app.post("/webhook")
async def webhook_post():
    return JSONResponse({
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "웹훅 응답 정상입니다."
                    }
                }
            ]
        }
    })


@app.post("/kakao/market-preopen")
async def market_preopen():
    try:
        latest = fetch_latest_summary("kr_stock_preopen")
        return kakao_response_from_row(latest, "개장 전 요약 데이터가 아직 없습니다.")
    except Exception as e:
        print("market_preopen error:", repr(e))
        return JSONResponse({
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "개장 전 요약을 불러오는 중 문제가 발생했습니다."
                        }
                    }
                ]
            }
        })


@app.post("/kakao/market-morning")
async def market_morning():
    try:
        latest = fetch_latest_summary("kr_stock_morning")
        return kakao_response_from_row(latest, "오전 시황 데이터가 아직 없습니다.")
    except Exception as e:
        print("market_morning error:", repr(e))
        return JSONResponse({
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "오전 시황을 불러오는 중 문제가 발생했습니다."
                        }
                    }
                ]
            }
        })


@app.post("/kakao/market-close")
async def market_close():
    try:
        latest = fetch_latest_summary("kr_stock_close")
        return kakao_response_from_row(latest, "장마감 요약 데이터가 아직 없습니다.")
    except Exception as e:
        print("market_close error:", repr(e))
        return JSONResponse({
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "장마감 요약을 불러오는 중 문제가 발생했습니다."
                        }
                    }
                ]
            }
        })


@app.post("/kakao/market-summary")
async def market_summary():
    try:
        latest = fetch_latest_summary("kr_stock_close")
        if not latest:
            latest = fetch_latest_summary("kr_stock_morning")
        if not latest:
            latest = fetch_latest_summary("kr_stock_preopen")

        return kakao_response_from_row(latest, "시장 요약 데이터가 아직 없습니다.")
    except Exception as e:
        print("market_summary error:", repr(e))
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
