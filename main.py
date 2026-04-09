import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()
DEFAULT_BLOG_URL = os.environ.get("DEFAULT_BLOG_URL", "https://moneycalc.wikitreee.com").strip()

TABLE_NAME = "market_summaries"
KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(KST)


def current_hhmm() -> int:
    now = now_kst()
    return now.hour * 100 + now.minute


def get_common_quick_replies() -> list:
    return [
        {
            "label": "🔒 개장 전",
            "action": "message",
            "messageText": "개장 전"
        },
        {
            "label": "📈 오전 시황",
            "action": "message",
            "messageText": "오전 시황"
        },
        {
            "label": "🌙 장 마감",
            "action": "message",
            "messageText": "마감"
        },
        {
            "label": "🌍 글로벌",
            "action": "message",
            "messageText": "글로벌"
        }
    ]


def build_kakao_response(outputs: list, quick_replies: list | None = None) -> JSONResponse:
    payload = {
        "version": "2.0",
        "template": {
            "outputs": outputs,
            "quickReplies": quick_replies or get_common_quick_replies()
        }
    }
    return JSONResponse(payload)


def get_market_ui_meta(market_type: str) -> dict:
    mapping = {
        "kr_stock_preopen": {
            "header_title": "📌 [오늘의 개장 전 체크]",
            "button_label": "오늘 장마감 분석 보기",
            "fallback_empty": "개장 전 시황은 장 시작 전 정리됩니다. 밤사이 글로벌 변수와 미국장 흐름을 반영해 업데이트됩니다.",
        },
        "kr_stock_morning": {
            "header_title": "📌 [오늘의 오전 시황]",
            "button_label": "오늘 장마감 분석 보기",
            "fallback_empty": "오전 시황은 13시에 정리됩니다. 장 초반 흐름이 더 확인된 뒤 업데이트됩니다.",
        },
        "kr_stock_close": {
            "header_title": "📌 [오늘의 장마감 요약]",
            "button_label": "오늘 장마감 분석 보기",
            "fallback_empty": "아직 한국장이 마감되지 않았습니다. 장 마감 후 마감 시황이 정리됩니다.",
        },
    }
    return mapping.get(
        market_type,
        {
            "header_title": "📌 [오늘의 시장 체크]",
            "button_label": "최신 분석글 바로가기",
            "fallback_empty": "시장 요약 데이터가 아직 없습니다.",
        },
    )


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

    ui_meta = get_market_ui_meta(market_type)
    parts = [ui_meta["header_title"]]

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
        point_lines = [x.strip() for x in tomorrow_points.split("/") if x.strip()]
        if point_lines:
            section = ["\n🎯 체크 포인트"]
            for item in point_lines:
                section.append(f"- {item}")
            parts.append("\n".join(section))

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


def build_outputs_from_row(latest: dict) -> list:
    text = build_summary_text(latest)
    market_type = (latest.get("market_type") or "").strip()
    ui_meta = get_market_ui_meta(market_type)

    if len(text) > 950:
        text = text[:950].rstrip() + "\n\n👉 아래 버튼에서 전체 분석을 확인하세요."

    outputs = [
        {
            "simpleText": {
                "text": text
            }
        }
    ]

    post_url = (latest.get("post_url") or "").strip()
    post_title = (latest.get("post_title") or "").strip()

    # 개장 전/오전은 카드 숨김
    if market_type in ["kr_stock_preopen", "kr_stock_morning"]:
        return outputs

    if not post_url:
        return outputs

    card_title = post_title or ui_meta["header_title"].replace("📌 ", "")
    button_label = ui_meta["button_label"]

    outputs.append({
        "basicCard": {
            "title": card_title,
            "description": "시장 흐름을 더 자세히 확인할 수 있습니다.",
            "buttons": [
                {
                    "action": "webLink",
                    "label": button_label,
                    "webLinkUrl": post_url
                }
            ]
        }
    })

    return outputs


def should_block_market_view(market_type: str) -> tuple[bool, str]:
    hhmm = current_hhmm()

    if market_type == "kr_stock_morning" and hhmm < 1300:
        return True, "오전 시황은 13시에 정리됩니다. 장 초반 흐름이 더 확인된 뒤 업데이트됩니다."

    if market_type == "kr_stock_close" and hhmm < 1530:
        return True, "아직 한국장이 마감되지 않았습니다. 장 마감 후 마감 시황이 정리됩니다."

    return False, ""


def kakao_response_from_row(latest: dict | None, market_type: str) -> JSONResponse:
    ui_meta = get_market_ui_meta(market_type)

    blocked, block_message = should_block_market_view(market_type)
    if blocked:
        return build_kakao_response([
            {
                "simpleText": {
                    "text": block_message
                }
            }
        ])

    if not latest:
        return build_kakao_response([
            {
                "simpleText": {
                    "text": ui_meta["fallback_empty"]
                }
            }
        ])

    outputs = build_outputs_from_row(latest)
    return build_kakao_response(outputs)


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
    return build_kakao_response([
        {
            "simpleText": {
                "text": "웹훅 응답 정상입니다."
            }
        }
    ])


@app.post("/kakao/market-preopen")
async def market_preopen():
    try:
        latest = fetch_latest_summary("kr_stock_preopen")
        return kakao_response_from_row(latest, "kr_stock_preopen")
    except Exception as e:
        print("market_preopen error:", repr(e))
        return build_kakao_response([
            {
                "simpleText": {
                    "text": "개장 전 시황을 불러오는 중 문제가 발생했습니다."
                }
            }
        ])


@app.post("/kakao/market-morning")
async def market_morning():
    try:
        latest = fetch_latest_summary("kr_stock_morning")
        return kakao_response_from_row(latest, "kr_stock_morning")
    except Exception as e:
        print("market_morning error:", repr(e))
        return build_kakao_response([
            {
                "simpleText": {
                    "text": "오전 시황을 불러오는 중 문제가 발생했습니다."
                }
            }
        ])


@app.post("/kakao/market-close")
async def market_close():
    try:
        latest = fetch_latest_summary("kr_stock_close")
        return kakao_response_from_row(latest, "kr_stock_close")
    except Exception as e:
        print("market_close error:", repr(e))
        return build_kakao_response([
            {
                "simpleText": {
                    "text": "장마감 시황을 불러오는 중 문제가 발생했습니다."
                }
            }
        ])


@app.post("/kakao/market-summary")
async def market_summary():
    try:
        hhmm = current_hhmm()

        if hhmm < 1300:
            latest = fetch_latest_summary("kr_stock_preopen")
            return kakao_response_from_row(latest, "kr_stock_preopen")

        if hhmm < 1530:
            latest = fetch_latest_summary("kr_stock_morning")
            return kakao_response_from_row(latest, "kr_stock_morning")

        latest = fetch_latest_summary("kr_stock_close")
        return kakao_response_from_row(latest, "kr_stock_close")

    except Exception as e:
        print("market_summary error:", repr(e))

        fallback_outputs = [
            {
                "simpleText": {
                    "text": "시장 요약을 불러오는 중 문제가 발생했습니다. 잠시 후 다시 시도해주세요."
                }
            }
        ]

        if DEFAULT_BLOG_URL:
            fallback_outputs.append({
                "basicCard": {
                    "title": "📊 최신 분석글 보기",
                    "description": "시장 분석 페이지로 바로 이동할 수 있습니다.",
                    "buttons": [
                        {
                            "action": "webLink",
                            "label": "최신 분석글 바로가기",
                            "webLinkUrl": DEFAULT_BLOG_URL
                        }
                    ]
                }
            })

        return build_kakao_response(fallback_outputs)
