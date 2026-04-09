import os
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client, Client

# =========================
# 환경변수
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL / SUPABASE_KEY 환경변수가 필요합니다.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()
KST = ZoneInfo("Asia/Seoul")

# =========================
# 공통 quick replies
# =========================
QUICK_REPLIES = [
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
        "messageText": "장 마감"
    },
    {
        "label": "🌍 글로벌",
        "action": "message",
        "messageText": "글로벌"
    },
]

# =========================
# 텍스트 정책
# =========================
MORNING_BLOCK_TEXT = "오전 시황은 13시에 정리됩니다. 장 초반 흐름이 더 확인된 뒤 업데이트됩니다."
CLOSE_BLOCK_TEXT = "아직 한국장이 마감되지 않았습니다. 장 마감 후 마감 시황이 정리됩니다."

PREOPEN_EMPTY_TEXT = "오늘 개장 전 시황 데이터가 아직 준비되지 않았습니다. 잠시 후 다시 확인해 주세요."
MORNING_EMPTY_TEXT = "오늘 오전 시황 데이터가 아직 준비되지 않았습니다. 잠시 후 다시 확인해 주세요."
CLOSE_EMPTY_TEXT = "오늘 마감 시황 데이터가 아직 준비되지 않았습니다. 잠시 후 다시 확인해 주세요."
GLOBAL_TEXT = "글로벌 브리핑은 별도 기능으로 연결 예정입니다."

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

    # 기본값
    return "preopen"

def get_market_type(stage: str) -> Optional[str]:
    mapping = {
        "preopen": "kr_stock_preopen",
        "morning": "kr_stock_morning",
        "close": "kr_stock_close",
    }
    return mapping.get(stage)

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
        return None

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

def join_nonempty(lines: List[str]) -> str:
    return "\n".join([x for x in lines if x and str(x).strip()])

def build_preopen_text(row: Dict[str, Any]) -> str:
    lines = [
        "🔒 오늘의 개장 전 브리핑",
        "",
        row.get("one_line", ""),
        "",
        "📌 밤사이 체크 포인트",
        row.get("full_text", ""),
        "",
        f"💵 환율/해외 변수: {row.get('tomorrow_points', '')}".strip(),
    ]
    return join_nonempty(lines)

def build_morning_text(row: Dict[str, Any]) -> str:
    lines = [
        "📈 오전 시황",
        "",
        row.get("one_line", ""),
        "",
        f"코스피: {row.get('kospi_text', '')}",
        f"코스닥: {row.get('kosdaq_text', '')}",
        "",
        f"강한 업종: {row.get('strong_sectors', '')}",
        f"약한 업종: {row.get('weak_sectors', '')}",
        "",
        row.get("full_text", ""),
    ]
    return join_nonempty(lines)

def build_close_text(row: Dict[str, Any]) -> str:
    lines = [
        "🌙 장 마감 시황",
        "",
        row.get("one_line", ""),
        "",
        f"코스피: {row.get('kospi_text', '')}",
        f"코스닥: {row.get('kosdaq_text', '')}",
        "",
        f"강한 업종: {row.get('strong_sectors', '')}",
        f"약한 업종: {row.get('weak_sectors', '')}",
        "",
        "📝 내일 체크 포인트",
        row.get("tomorrow_points", ""),
        "",
        row.get("full_text", ""),
    ]
    if row.get("post_url"):
        lines += ["", f"🔗 자세히 보기: {row.get('post_url')}"]
    return join_nonempty(lines)

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
        return make_simple_text_response(build_preopen_text(row))
    if stage == "morning":
        return make_simple_text_response(build_morning_text(row))
    if stage == "close":
        return make_simple_text_response(build_close_text(row))

    return make_simple_text_response("요청을 처리하지 못했습니다.")

# =========================
# 헬스체크
# =========================
@app.get("/")
def root():
    return {
        "ok": True,
        "message": "Kakao market skill server is running",
        "time_kst": now_kst().isoformat()
    }

@app.get("/health")
def health():
    return {
        "ok": True,
        "time_kst": now_kst().isoformat()
    }

# =========================
# 카카오 스킬
# =========================
@app.post("/kakao/skill")
async def kakao_skill(request: Request):
    try:
        body = await request.json()
        print("[kakao_skill] request body:", body)

        user_text = (
            body.get("userRequest", {})
            .get("utterance", "")
        )

        stage = detect_user_command(user_text)
        response = build_stage_response(stage)
        return JSONResponse(content=response)

    except Exception as e:
        print(f"[kakao_skill] error: {e}")
        return JSONResponse(
            content=make_simple_text_response(
                "일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."
            )
        )

# =========================
# 테스트용 직접 호출
# =========================
@app.get("/test")
def test(stage: str = "preopen"):
    return build_stage_response(stage)
