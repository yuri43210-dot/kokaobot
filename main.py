import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client, Client

# =========================
# 환경변수
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL / SUPABASE_KEY가 필요합니다.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI()

KST = ZoneInfo("Asia/Seoul")

# =========================
# 공통 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)

def get_today_str() -> str:
    return now_kst().strftime("%Y-%m-%d")

def quick_replies() -> List[Dict[str, Any]]:
    return [
        {"label": "🔥 개장 전", "action": "message", "messageText": "개장 전"},
        {"label": "📈 오전 시황", "action": "message", "messageText": "오전 시황"},
        {"label": "📝 장 마감", "action": "message", "messageText": "장 마감"},
        {"label": "🌎 글로벌", "action": "message", "messageText": "글로벌"},
    ]

def kakao_response(outputs: List[Dict[str, Any]]) -> JSONResponse:
    return JSONResponse(
        {
            "version": "2.0",
            "template": {
                "outputs": outputs,
                "quickReplies": quick_replies(),
            },
        }
    )

def normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = str(text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def shorten(text: str, max_len: int) -> str:
    text = normalize_text(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"

def split_for_kakao(text: str, max_len: int = 300) -> List[str]:
    text = normalize_text(text)
    if not text:
        return []

    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    chunks: List[str] = []
    current = ""

    def flush():
        nonlocal current
        if current.strip():
            chunks.append(current.strip())
        current = ""

    for p in paragraphs:
        if len(p) <= max_len:
            candidate = f"{current}\n{p}".strip() if current else p
            if len(candidate) <= max_len:
                current = candidate
            else:
                flush()
                current = p
            continue

        sentences = re.split(r"(?<=[.!?다요])\s+", p)
        for s in sentences:
            s = s.strip()
            if not s:
                continue
            if len(s) <= max_len:
                candidate = f"{current}\n{s}".strip() if current else s
                if len(candidate) <= max_len:
                    current = candidate
                else:
                    flush()
                    current = s
            else:
                if current:
                    flush()
                start = 0
                while start < len(s):
                    part = s[start:start + max_len].strip()
                    if part:
                        chunks.append(part)
                    start += max_len

    flush()
    return chunks

def make_simple_text(text: str) -> Dict[str, Any]:
    return {"simpleText": {"text": text}}

def make_basic_card(
    title: str,
    description: str,
    button_label: Optional[str] = None,
    button_url: Optional[str] = None,
) -> Dict[str, Any]:
    card: Dict[str, Any] = {
        "basicCard": {
            "title": title,
            "description": description,
        }
    }

    if button_label and button_url:
        card["basicCard"]["buttons"] = [
            {
                "action": "webLink",
                "label": button_label,
                "webLinkUrl": button_url,
            }
        ]
    return card

# =========================
# DB 조회
# =========================
def fetch_latest_summary(market_type: str, summary_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    query = (
        supabase.table("market_summaries")
        .select("*")
        .eq("market_type", market_type)
        .order("updated_at", desc=True)
        .limit(1)
    )
    if summary_date:
        query = query.eq("summary_date", summary_date)

    result = query.execute()
    rows = result.data or []
    return rows[0] if rows else None

def fetch_latest_any_of_types(types: List[str], summary_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    for t in types:
        row = fetch_latest_summary(t, summary_date=summary_date)
        if row:
            return row
    return None

# =========================
# 단계 판별
# =========================
def detect_stage(payload: Dict[str, Any]) -> str:
    try:
        user_request = payload.get("userRequest", {}) or {}
        utterance = str(user_request.get("utterance", "")).strip()
        utterance_lower = utterance.lower()

        action = payload.get("action", {}) or {}
        action_name = str(action.get("name", "")).strip()

        if "글로벌" in utterance or "미국장" in utterance or action_name == "글로벌":
            return "global"
        if "장 마감" in utterance or "마감" in utterance or action_name == "장 마감":
            return "close"
        if "오전 시황" in utterance or "오전" in utterance or action_name == "오전 시황":
            return "morning"
        if "개장 전" in utterance or "개장전" in utterance or action_name == "개장 전":
            return "preopen"

        if "global" in utterance_lower:
            return "global"

    except Exception:
        pass

    return "preopen"

# =========================
# 포맷터
# =========================
def build_preopen_outputs(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    one_line = normalize_text(row.get("one_line", ""))
    full_text = normalize_text(row.get("full_text", ""))
    post_url = (row.get("post_url") or "").strip()

    title = "🔥 개장 전 | 오늘 시장 방향"
    desc = shorten(one_line or full_text, 220)

    if post_url:
        return [
            make_basic_card(
                title=title,
                description=desc,
                button_label="개장 전 상세 보기",
                button_url=post_url,
            )
        ]

    text = f"{title}\n• 핵심: {one_line}\n\n{shorten(full_text, 600)}"
    return [make_simple_text(text)]

def build_morning_outputs(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    오전 시황은 상세 페이지가 없으므로
    카카오에서 읽기 좋게 구조화해서 여러 말풍선으로 출력.
    """
    one_line = normalize_text(row.get("one_line", ""))
    kospi_text = normalize_text(row.get("kospi_text", ""))
    kosdaq_text = normalize_text(row.get("kosdaq_text", ""))
    strong_sectors = normalize_text(row.get("strong_sectors", ""))
    weak_sectors = normalize_text(row.get("weak_sectors", ""))
    checkpoints = normalize_text(row.get("tomorrow_points", ""))

    outputs: List[Dict[str, Any]] = []

    # 1. 핵심
    headline = "📈 오전 시황 | 지금 시장 흐름"
    core_text = f"{headline}\n• 핵심: {one_line}" if one_line else headline
    outputs.append(make_simple_text(core_text))

    # 2. 코스피
    if kospi_text:
        outputs.append(
            make_simple_text(
                "📌 코스피\n" + shorten(kospi_text, 300)
            )
        )

    # 3. 코스닥
    if kosdaq_text:
        outputs.append(
            make_simple_text(
                "📌 코스닥\n" + shorten(kosdaq_text, 300)
            )
        )

    # 4. 체크포인트
    check_lines: List[str] = []
    if checkpoints:
        check_lines.append(f"• 체크포인트: {checkpoints}")
    if strong_sectors:
        check_lines.append(f"• 강세 업종: {strong_sectors}")
    if weak_sectors:
        check_lines.append(f"• 약세 업종: {weak_sectors}")

    if check_lines:
        merged = "\n".join(check_lines)
        for idx, chunk in enumerate(split_for_kakao(merged, max_len=300), start=1):
            title = "🔎 체크포인트" if idx == 1 else "🔎 체크포인트(계속)"
            outputs.append(make_simple_text(f"{title}\n{chunk}"))

    if not outputs:
        outputs.append(make_simple_text("📈 오전 시황 데이터가 아직 준비되지 않았습니다."))

    return outputs

def build_close_outputs(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    one_line = normalize_text(row.get("one_line", ""))
    full_text = normalize_text(row.get("full_text", ""))
    post_url = (row.get("post_url") or "").strip()

    title = "📝 장 마감 | 오늘 결과 정리"
    desc = shorten(one_line or full_text, 220)

    if post_url:
        return [
            make_basic_card(
                title=title,
                description=desc,
                button_label="장 마감 상세 보기",
                button_url=post_url,
            )
        ]

    text = f"{title}\n• 핵심: {one_line}\n\n{shorten(full_text, 600)}"
    return [make_simple_text(text)]

def build_global_outputs() -> List[Dict[str, Any]]:
    today = get_today_str()
    row = fetch_latest_any_of_types(["kr_stock_preopen", "kr_stock_close"], summary_date=today)
    if not row:
        return [
            make_simple_text(
                "🌎 글로벌 경제 지수\n미국장 관련 데이터가 아직 준비되지 않았습니다.\n잠시 후 다시 확인해 주세요."
            )
        ]

    dow = row.get("dow_text", "") or "N/A"
    sp500 = row.get("sp500_text", "") or "N/A"
    nasdaq = row.get("nasdaq_text", "") or "N/A"
    sox = row.get("sox_text", "") or "N/A"
    usdkrw = row.get("usdkrw_text", "") or "N/A"

    text = (
        "🌎 글로벌 경제 지수\n"
        f"• 다우: {dow}\n"
        f"• S&P500: {sp500}\n"
        f"• 나스닥: {nasdaq}\n"
        f"• SOX: {sox}\n"
        f"• 원/달러: {usdkrw}"
    )
    return [make_simple_text(text)]

# =========================
# 라우트
# =========================
@app.get("/health")
async def health():
    return {"ok": True, "time_kst": now_kst().isoformat()}

@app.get("/test")
async def test(stage: str = "preopen"):
    today = get_today_str()

    if stage == "preopen":
        row = fetch_latest_summary("kr_stock_preopen", summary_date=today)
        outputs = build_preopen_outputs(row) if row else [make_simple_text("개장 전 데이터가 없습니다.")]
        return {"stage": stage, "outputs": outputs}

    if stage == "morning":
        row = fetch_latest_summary("kr_stock_morning", summary_date=today)
        outputs = build_morning_outputs(row) if row else [make_simple_text("오전 시황 데이터가 없습니다.")]
        return {"stage": stage, "outputs": outputs}

    if stage == "close":
        row = fetch_latest_summary("kr_stock_close", summary_date=today)
        outputs = build_close_outputs(row) if row else [make_simple_text("장 마감 데이터가 없습니다.")]
        return {"stage": stage, "outputs": outputs}

    return {"stage": "global", "outputs": build_global_outputs()}

@app.post("/kakao/skill")
async def kakao_skill(request: Request):
    payload = await request.json()
    stage = detect_stage(payload)
    current = now_kst()
    today = current.strftime("%Y-%m-%d")

    print(f"[kakao] detected stage: {stage}")

    # 개장 전
    if stage == "preopen":
        row = fetch_latest_summary("kr_stock_preopen", summary_date=today)
        if not row:
            return kakao_response([
                make_simple_text("🔥 개장 전 데이터가 아직 준비되지 않았습니다.\n잠시 후 다시 확인해 주세요.")
            ])
        return kakao_response(build_preopen_outputs(row))

    # 오전 시황
    if stage == "morning":
        if current.hour < 13:
            return kakao_response([
                make_simple_text(
                    "📈 오전 시황은 13시에 정리됩니다.\n장 초반 흐름이 더 확인된 뒤 업데이트됩니다."
                )
            ])

        row = fetch_latest_summary("kr_stock_morning", summary_date=today)
        if not row:
            return kakao_response([
                make_simple_text("📈 오전 시황 데이터가 아직 준비되지 않았습니다.\n잠시 후 다시 확인해 주세요.")
            ])
        return kakao_response(build_morning_outputs(row))

    # 장 마감
    if stage == "close":
        current_minutes = current.hour * 60 + current.minute
        if current_minutes < (15 * 60 + 30):
            return kakao_response([
                make_simple_text(
                    "📝 아직 한국장이 마감되지 않았습니다.\n장 마감 후 마감 시황이 정리됩니다."
                )
            ])

        row = fetch_latest_summary("kr_stock_close", summary_date=today)
        if not row:
            return kakao_response([
                make_simple_text("📝 장 마감 데이터가 아직 준비되지 않았습니다.\n잠시 후 다시 확인해 주세요.")
            ])
        return kakao_response(build_close_outputs(row))

    # 글로벌
    return kakao_response(build_global_outputs())
