import os
import traceback
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from supabase import create_client, Client

# =========================
# 환경변수
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
BLOG_HOME_URL = os.getenv("BLOG_HOME_URL", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL / SUPABASE_KEY 환경변수가 필요합니다.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()
KST = ZoneInfo("Asia/Seoul")

# =========================
# Quick Replies
# =========================
QUICK_REPLIES = [
    {"label": "🔥 오늘 시장 방향", "action": "message", "messageText": "개장 전"},
    {"label": "📊 지금 시장 흐름", "action": "message", "messageText": "오전 시황"},
    {"label": "📉 오늘 결과 정리", "action": "message", "messageText": "장 마감"},
    {"label": "🌍 미국장 영향", "action": "message", "messageText": "글로벌"},
]

# =========================
# 안내 문구
# =========================
MORNING_BLOCK_TEXT = (
    "📊 오전 시황은 13시에 정리됩니다.\n\n"
    "장 초반 흐름이 더 확인된 뒤 업데이트됩니다."
)

CLOSE_BLOCK_TEXT = (
    "📉 아직 한국장이 마감되지 않았습니다.\n\n"
    "장 마감 후 마감 시황이 정리됩니다."
)

PREOPEN_EMPTY_TEXT = (
    "🔥 오늘 시장 방향 브리핑이 아직 준비되지 않았습니다.\n\n"
    "잠시 후 다시 확인해 주세요."
)

MORNING_EMPTY_TEXT = (
    "📊 오전 시황 데이터가 아직 준비되지 않았습니다.\n\n"
    "잠시 후 다시 확인해 주세요."
)

CLOSE_EMPTY_TEXT = (
    "📉 장 마감 시황 데이터가 아직 준비되지 않았습니다.\n\n"
    "잠시 후 다시 확인해 주세요."
)

GLOBAL_TEXT = (
    "🌍 글로벌 브리핑은 별도 기능으로 연결 예정입니다.\n\n"
    "현재는 개장 전 / 오전 시황 / 장 마감 중심으로 운영 중입니다."
)

# =========================
# 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)

def today_kst() -> date:
    return now_kst().date()

def format_date_kr(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def is_after_13(now_dt: datetime) -> bool:
    return (now_dt.hour, now_dt.minute) >= (13, 0)

def is_after_1530(now_dt: datetime) -> bool:
    return (now_dt.hour, now_dt.minute) >= (15, 30)

def detect_user_command(user_text: str) -> str:
    text = (user_text or "").strip()

    if "개장 전" in text or "개장전" in text:
        return "preopen"
    if "오전" in text:
        return "morning"
    if "마감" in text or "장 마감" in text or "장마감" in text:
        return "close"
    if "글로벌" in text or "미국장" in text:
        return "global"

    return "preopen"

def get_market_type(stage: str) -> Optional[str]:
    return {
        "preopen": "kr_stock_preopen",
        "morning": "kr_stock_morning",
        "close": "kr_stock_close",
    }.get(stage)

def fetch_summary(summary_date: str, market_type: str) -> Optional[Dict[str, Any]]:
    try:
        result = (
            supabase.table("market_summaries")
            .select("*")
            .eq("summary_date", summary_date)
            .eq("market_type", market_type)
            .order("id", desc=True)
            .limit(1)
            .execute()
        )
        rows = result.data or []
        if not rows:
            return None
        return rows[0]
    except Exception as e:
        print(f"[fetch_summary] error: {e}")
        traceback.print_exc()
        return None

def safe_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""

def trim_text(text: str, max_len: int) -> str:
    text = safe_text(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"

def compact_line(label: str, value: Any) -> str:
    text = safe_text(value)
    if not text:
        return ""
    return f"{label} {text}"

def compact_market_line(name: str, value: Any) -> str:
    text = safe_text(value)
    if not text:
        return ""
    return f"{name}: {text}"

# =========================
# 카카오 응답 빌더
# =========================
def make_simple_text_response(text: str) -> Dict[str, Any]:
    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": text
                    }
                }
            ],
            "quickReplies": QUICK_REPLIES
        }
    }

def make_basic_card_response(
    title: str,
    description: str,
    button_label: Optional[str] = None,
    button_url: Optional[str] = None,
) -> Dict[str, Any]:
    card: Dict[str, Any] = {
        "title": trim_text(title, 40),
        "description": trim_text(description, 330),
    }

    if button_label and button_url:
        card["buttons"] = [
            {
                "action": "webLink",
                "label": trim_text(button_label, 14),
                "webLinkUrl": button_url,
            }
        ]

    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "basicCard": card
                }
            ],
            "quickReplies": QUICK_REPLIES
        }
    }

# =========================
# 카드 내용 구성
# =========================
def build_preopen_card(row: Dict[str, Any]) -> Dict[str, str]:
    title = "🔥 개장 전 | 미국장·환율 체크"

    description = "\n".join(filter(None, [
        compact_line("• 핵심:", row.get("one_line")),
        compact_market_line("• 다우", row.get("dow_text")),
        compact_market_line("• S&P500", row.get("sp500_text")),
        compact_market_line("• 나스닥", row.get("nasdaq_text")),
        compact_market_line("• 원/달러", row.get("usdkrw_text")),
        compact_line("• 관심:", row.get("strong_sectors")),
        compact_line("• 주의:", row.get("weak_sectors")),
        compact_line("• 체크:", row.get("tomorrow_points")),
    ]))

    return {"title": title, "description": description}

def build_morning_card(row: Dict[str, Any]) -> Dict[str, str]:
    title = "📊 오전 시황 | 지금 시장 흐름"
    description = "\n".join(filter(None, [
        compact_line("• 핵심:", row.get("one_line")),
        compact_line("• 코스피:", row.get("kospi_text")),
        compact_line("• 코스닥:", row.get("kosdaq_text")),
        compact_line("• 강세:", row.get("strong_sectors")),
        compact_line("• 약세:", row.get("weak_sectors")),
    ]))
    return {"title": title, "description": description}

def build_close_card(row: Dict[str, Any]) -> Dict[str, str]:
    title = "📉 장 마감 | 오늘 시장 결과 정리"
    description = "\n".join(filter(None, [
        compact_line("• 핵심:", row.get("one_line")),
        compact_line("• 코스피:", row.get("kospi_text")),
        compact_line("• 코스닥:", row.get("kosdaq_text")),
        compact_line("• 강세:", row.get("strong_sectors")),
        compact_line("• 약세:", row.get("weak_sectors")),
        compact_line("• 내일:", row.get("tomorrow_points")),
    ]))
    return {"title": title, "description": description}

# =========================
# stage별 응답
# =========================
def build_stage_response(stage: str) -> Dict[str, Any]:
    now_dt = now_kst()
    today_str = format_date_kr(today_kst())

    if stage == "global":
        return make_simple_text_response(GLOBAL_TEXT)

    if stage == "morning" and not is_after_13(now_dt):
        return make_simple_text_response(MORNING_BLOCK_TEXT)

    if stage == "close" and not is_after_1530(now_dt):
        return make_simple_text_response(CLOSE_BLOCK_TEXT)

    market_type = get_market_type(stage)
    if not market_type:
        return make_simple_text_response("요청을 이해하지 못했습니다.")

    row = fetch_summary(today_str, market_type)

    if not row:
        if stage == "preopen":
            return make_simple_text_response(PREOPEN_EMPTY_TEXT)
        if stage == "morning":
            return make_simple_text_response(MORNING_EMPTY_TEXT)
        if stage == "close":
            return make_simple_text_response(CLOSE_EMPTY_TEXT)

    if stage == "preopen":
        card = build_preopen_card(row)
        return make_basic_card_response(
            title=card["title"],
            description=card["description"]
        )

    if stage == "morning":
        card = build_morning_card(row)
        return make_basic_card_response(
            title=card["title"],
            description=card["description"]
        )

    if stage == "close":
        card = build_close_card(row)
        post_url = safe_text(row.get("post_url")) or BLOG_HOME_URL

        # 버튼은 BLOG_HOME_URL만 있어도 항상 보이게
        if post_url:
            return make_basic_card_response(
                title=card["title"],
                description=card["description"],
                button_label="상세 분석 보기",
                button_url=post_url
            )

        return make_basic_card_response(
            title=card["title"],
            description=card["description"]
        )

    return make_simple_text_response("요청을 처리하지 못했습니다.")

# =========================
# 헬스체크 / keep-alive
# =========================
@app.get("/")
def root():
    return {
        "ok": True,
        "message": "Kakao market skill server is running",
        "time_kst": now_kst().isoformat()
    }

@app.head("/")
def root_head():
    return Response(status_code=200)

@app.get("/health")
def health():
    return {
        "ok": True,
        "time_kst": now_kst().isoformat()
    }

@app.head("/health")
def health_head():
    return Response(status_code=200)

@app.get("/test")
def test(stage: str = "preopen"):
    return build_stage_response(stage)

# =========================
# 공통 카카오 처리
# =========================
async def handle_kakao_request(request: Request, forced_stage: Optional[str] = None):
    try:
        body = await request.json()
        print("[kakao] request body:", body)

        user_request = body.get("userRequest", {}) or {}
        utterance = user_request.get("utterance", "") or ""

        action = body.get("action", {}) or {}
        params = action.get("params", {}) or {}

        candidate_text = utterance
        if not candidate_text:
            candidate_text = " ".join([str(v) for v in params.values() if v])

        stage = forced_stage or detect_user_command(candidate_text)
        response = build_stage_response(stage)

        print("[kakao] detected stage:", stage)
        print("[kakao] response ready")

        return JSONResponse(content=response)

    except Exception as e:
        print("[kakao] error:", e)
        traceback.print_exc()
        return JSONResponse(
            content=make_simple_text_response(
                "일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."
            )
        )

# =========================
# 카카오 엔드포인트
# =========================
@app.post("/kakao/skill")
async def kakao_skill(request: Request):
    return await handle_kakao_request(request)

@app.post("/kakao/market-preopen")
async def kakao_market_preopen(request: Request):
    return await handle_kakao_request(request, forced_stage="preopen")

@app.post("/kakao/market-morning")
async def kakao_market_morning(request: Request):
    return await handle_kakao_request(request, forced_stage="morning")

@app.post("/kakao/market-close")
async def kakao_market_close(request: Request):
    return await handle_kakao_request(request, forced_stage="close")
