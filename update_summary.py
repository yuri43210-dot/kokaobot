import os
import json
import base64
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import requests
import FinanceDataReader as fdr
import pandas as pd

SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

WP_SITE_URL = os.environ["WP_SITE_URL"].strip().rstrip("/")
WP_USERNAME = os.environ["WP_USERNAME"].strip()
WP_APP_PASSWORD = os.environ["WP_APP_PASSWORD"].strip()

TABLE_NAME = "market_summaries"
KST = ZoneInfo("Asia/Seoul")

WP_CATEGORY_ID = 12
ADINSERTER_SHORTCODE = '[adinserter block="1"]'

SECTOR_CONFIG = {
    "반도체": [
        {"ticker": "005930", "name": "삼성전자", "weight": 0.45},
        {"ticker": "000660", "name": "SK하이닉스", "weight": 0.35},
        {"ticker": "042700", "name": "한미반도체", "weight": 0.08},
        {"ticker": "000990", "name": "DB하이텍", "weight": 0.06},
        {"ticker": "240810", "name": "원익IPS", "weight": 0.06},
    ],
    "2차전지": [
        {"ticker": "373220", "name": "LG에너지솔루션", "weight": 0.38},
        {"ticker": "247540", "name": "에코프로비엠", "weight": 0.22},
        {"ticker": "003670", "name": "포스코퓨처엠", "weight": 0.16},
        {"ticker": "066970", "name": "엘앤에프", "weight": 0.12},
        {"ticker": "006400", "name": "삼성SDI", "weight": 0.12},
    ],
    "AI/인터넷": [
        {"ticker": "035420", "name": "NAVER", "weight": 0.40},
        {"ticker": "035720", "name": "카카오", "weight": 0.28},
        {"ticker": "304100", "name": "솔트룩스", "weight": 0.12},
        {"ticker": "047560", "name": "이스트소프트", "weight": 0.10},
        {"ticker": "377300", "name": "카카오페이", "weight": 0.10},
    ],
    "자동차": [
        {"ticker": "005380", "name": "현대차", "weight": 0.50},
        {"ticker": "000270", "name": "기아", "weight": 0.35},
        {"ticker": "012330", "name": "현대모비스", "weight": 0.10},
        {"ticker": "204320", "name": "HL만도", "weight": 0.05},
    ],
    "바이오": [
        {"ticker": "207940", "name": "삼성바이오로직스", "weight": 0.34},
        {"ticker": "068270", "name": "셀트리온", "weight": 0.30},
        {"ticker": "196170", "name": "알테오젠", "weight": 0.16},
        {"ticker": "028300", "name": "HLB", "weight": 0.10},
        {"ticker": "000100", "name": "유한양행", "weight": 0.10},
    ],
    "게임": [
        {"ticker": "259960", "name": "크래프톤", "weight": 0.36},
        {"ticker": "036570", "name": "엔씨소프트", "weight": 0.20},
        {"ticker": "251270", "name": "넷마블", "weight": 0.18},
        {"ticker": "263750", "name": "펄어비스", "weight": 0.14},
        {"ticker": "112040", "name": "위메이드", "weight": 0.12},
    ],
    "조선": [
        {"ticker": "009540", "name": "HD한국조선해양", "weight": 0.36},
        {"ticker": "042660", "name": "한화오션", "weight": 0.26},
        {"ticker": "010140", "name": "삼성중공업", "weight": 0.20},
        {"ticker": "329180", "name": "HD현대중공업", "weight": 0.18},
    ],
    "방산": [
        {"ticker": "012450", "name": "한화에어로스페이스", "weight": 0.42},
        {"ticker": "079550", "name": "LIG넥스원", "weight": 0.24},
        {"ticker": "047810", "name": "한국항공우주", "weight": 0.20},
        {"ticker": "272210", "name": "한화시스템", "weight": 0.14},
    ],
    "은행": [
        {"ticker": "105560", "name": "KB금융", "weight": 0.34},
        {"ticker": "055550", "name": "신한지주", "weight": 0.28},
        {"ticker": "086790", "name": "하나금융지주", "weight": 0.22},
        {"ticker": "316140", "name": "우리금융지주", "weight": 0.16},
    ],
    "증권": [
        {"ticker": "006800", "name": "미래에셋증권", "weight": 0.30},
        {"ticker": "005940", "name": "NH투자증권", "weight": 0.24},
        {"ticker": "039490", "name": "키움증권", "weight": 0.24},
        {"ticker": "016360", "name": "삼성증권", "weight": 0.22},
    ],
    "통신": [
        {"ticker": "017670", "name": "SK텔레콤", "weight": 0.40},
        {"ticker": "030200", "name": "KT", "weight": 0.34},
        {"ticker": "032640", "name": "LG유플러스", "weight": 0.26},
    ],
    "화장품/소비재": [
        {"ticker": "090430", "name": "아모레퍼시픽", "weight": 0.32},
        {"ticker": "161890", "name": "한국콜마", "weight": 0.22},
        {"ticker": "214450", "name": "파마리서치", "weight": 0.16},
        {"ticker": "051900", "name": "LG생활건강", "weight": 0.18},
        {"ticker": "007310", "name": "오뚜기", "weight": 0.12},
    ],
    "철강": [
        {"ticker": "005490", "name": "POSCO홀딩스", "weight": 0.48},
        {"ticker": "004020", "name": "현대제철", "weight": 0.28},
        {"ticker": "001430", "name": "세아베스틸지주", "weight": 0.14},
        {"ticker": "460860", "name": "동국제강", "weight": 0.10},
    ],
    "건설": [
        {"ticker": "000720", "name": "현대건설", "weight": 0.34},
        {"ticker": "028260", "name": "삼성물산", "weight": 0.28},
        {"ticker": "047040", "name": "대우건설", "weight": 0.20},
        {"ticker": "006360", "name": "GS건설", "weight": 0.18},
    ],
    "에너지": [
        {"ticker": "267250", "name": "HD현대", "weight": 0.30},
        {"ticker": "096770", "name": "SK이노베이션", "weight": 0.28},
        {"ticker": "010950", "name": "S-Oil", "weight": 0.24},
        {"ticker": "078930", "name": "GS", "weight": 0.18},
    ],
    "엔터/미디어": [
        {"ticker": "352820", "name": "하이브", "weight": 0.42},
        {"ticker": "041510", "name": "에스엠", "weight": 0.22},
        {"ticker": "122870", "name": "와이지엔터테인먼트", "weight": 0.18},
        {"ticker": "035900", "name": "JYP Ent.", "weight": 0.18},
    ],
}

DEFAULT_STRONG = ["데이터 확인 중"]
DEFAULT_WEAK = ["데이터 확인 중"]


def now_kst() -> datetime:
    return datetime.now(KST)


def market_stage(now: datetime) -> str:
    forced_stage = os.environ.get("FORCE_STAGE", "").strip()
    if forced_stage in ["pre_open", "intraday", "close"]:
        print("FORCE_STAGE applied:", forced_stage)
        return forced_stage

    hhmm = now.hour * 100 + now.minute
    if hhmm < 900:
        return "pre_open"
    if 900 <= hhmm < 1530:
        return "intraday"
    return "close"


def stage_meta(stage: str) -> dict:
    if stage == "pre_open":
        return {
            "market_type": "kr_stock_preopen",
            "title": "오늘의 개장 전 체크",
            "check_label": "오늘 체크",
            "wp_keyword": "개장 전 시황",
        }
    if stage == "intraday":
        return {
            "market_type": "kr_stock_morning",
            "title": "오늘의 오전 시황",
            "check_label": "남은 장 체크",
            "wp_keyword": "오전 시황",
        }
    return {
        "market_type": "kr_stock_close",
        "title": "오늘의 장마감 요약",
        "check_label": "내일 체크",
        "wp_keyword": "장마감 시황",
    }


def safe_direction(change_pct: float) -> str:
    if change_pct > 0:
        return "상승"
    if change_pct < 0:
        return "하락"
    return "보합"


def get_last_two_rows(symbol: str) -> pd.DataFrame:
    start = date.today() - timedelta(days=10)
    df = fdr.DataReader(symbol, start.strftime("%Y-%m-%d"))

    if df is None or df.empty:
        raise ValueError(f"{symbol} 데이터가 비어 있습니다.")

    df = df.dropna().copy()
    if len(df) < 2:
        raise ValueError(f"{symbol} 데이터가 충분하지 않습니다.")

    return df.tail(2)


def get_index_change(symbol: str, name: str) -> dict:
    try:
        df = get_last_two_rows(symbol)
        prev = df.iloc[0]
        last = df.iloc[1]

        close = float(last["Close"])
        prev_close = float(prev["Close"])
        change_pct = round(((close - prev_close) / prev_close) * 100, 2)

        return {
            "name": name,
            "close": round(close, 2),
            "change_pct": change_pct,
            "direction": safe_direction(change_pct),
        }
    except Exception as e:
        print(f"get_index_change error [{symbol}]:", repr(e))
        return {
            "name": name,
            "close": 0.0,
            "change_pct": 0.0,
            "direction": "보합",
        }


def get_fx_change() -> dict | None:
    try:
        df = get_last_two_rows("USD/KRW")
        prev = df.iloc[0]
        last = df.iloc[1]

        close = float(last["Close"])
        prev_close = float(prev["Close"])
        change_pct = round(((close - prev_close) / prev_close) * 100, 2)

        return {
            "close": round(close, 2),
            "change_pct": change_pct,
            "direction": safe_direction(change_pct),
        }
    except Exception as e:
        print("get_fx_change error:", repr(e))
        return None


def infer_sectors_from_representatives() -> tuple[list[str], list[str], dict]:
    sector_scores = {}

    for sector, items in SECTOR_CONFIG.items():
        valid_items = []

        for item in items:
            pct = get_stock_change_pct(item["ticker"])
            if pct is not None:
                valid_items.append({
                    "ticker": item["ticker"],
                    "name": item["name"],
                    "weight": item["weight"],
                    "pct": pct,
                })

        if not valid_items:
            continue

        weight_sum = sum(x["weight"] for x in valid_items)
        if weight_sum == 0:
            continue

        weighted_score = 0.0
        for x in valid_items:
            normalized_weight = x["weight"] / weight_sum
            weighted_score += x["pct"] * normalized_weight

        sector_scores[sector] = round(weighted_score, 2)

    if not sector_scores:
        return DEFAULT_STRONG, DEFAULT_WEAK, {}

    sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
    strong = [name for name, _ in sorted_sectors[:3]]
    weak = [name for name, _ in sorted_sectors[-3:]]

    return strong, weak, sector_scores


def get_stock_change_pct(ticker: str) -> float | None:
    try:
        start = date.today() - timedelta(days=10)
        df = fdr.DataReader(ticker, start.strftime("%Y-%m-%d"))

        if df is None or df.empty:
            return None

        df = df.dropna().copy()
        if len(df) < 2:
            return None

        prev = float(df.iloc[-2]["Close"])
        last = float(df.iloc[-1]["Close"])

        if prev == 0:
            return None

        return round(((last - prev) / prev) * 100, 2)
    except Exception as e:
        print(f"get_stock_change_pct error [{ticker}]:", repr(e))
        return None


def get_market_temperature(kospi_pct: float, kosdaq_pct: float) -> str:
    avg = (kospi_pct + kosdaq_pct) / 2

    if avg >= 1.0:
        return "🟢 강세"
    if 0.2 <= avg < 1.0:
        return "🟡 강보합"
    if -0.2 < avg < 0.2:
        return "⚪ 중립"
    if -1.0 < avg <= -0.2:
        return "🟠 약세"
    return "🔴 급락/리스크"


def get_flow_summary(kospi_pct: float, kosdaq_pct: float, fx: dict | None) -> str:
    if kospi_pct < 0 and kosdaq_pct < 0:
        if fx and fx["direction"] == "상승":
            return "외국인 이탈 가능성, 방어적 흐름이 나타났습니다."
        return "전반적인 매도 우위와 관망 심리가 이어졌습니다."
    if kospi_pct > 0 and kosdaq_pct > 0:
        return "위험선호 심리가 살아나며 매수 우위 흐름이 나타났습니다."
    return "혼조 흐름으로, 외국인·기관 수급 방향성을 추가 확인할 필요가 있습니다."


def build_trade_points_fallback(raw: dict) -> list[str]:
    strong_sectors = raw["strong_sectors"]
    weak_sectors = raw["weak_sectors"]
    sector_scores = raw["sector_scores"]
    kospi = raw["kospi"]
    kosdaq = raw["kosdaq"]
    fx = raw["fx"]

    points = []

    market_up = kospi["change_pct"] > 0 and kosdaq["change_pct"] > 0
    market_down = kospi["change_pct"] < 0 and kosdaq["change_pct"] < 0

    if strong_sectors:
        s = strong_sectors[0]
        score = sector_scores.get(s, 0)
        if market_up:
            points.append(f"{s}는 시장 상승 속에서 {score}% 강세를 보이며 주도 업종 가능성이 높습니다.")
        elif market_down:
            points.append(f"{s}는 시장 약세에도 {score}% 상승하며 방어 성격을 보이고 있습니다.")
        else:
            points.append(f"{s}는 혼조 장세 속에서 상대적으로 강한 흐름({score}%)을 유지하고 있습니다.")

    if weak_sectors:
        w = weak_sectors[0]
        score = sector_scores.get(w, 0)
        if market_up:
            points.append(f"{w}는 시장 상승에도 불구하고 {score}% 약세를 보여 상대적 소외가 나타나고 있습니다.")
        elif market_down:
            points.append(f"{w}는 시장 하락과 함께 {score}% 약세를 보이며 동반 부진 흐름입니다.")
        else:
            points.append(f"{w}는 방향성 없는 장세에서 {score}% 약세를 보여 힘이 부족한 구간입니다.")

    if fx:
        if fx["direction"] == "상승":
            points.append("환율 상승은 외국인 수급 부담 요인이 될 수 있어 개별 업종별 대응이 중요합니다.")
        else:
            points.append("환율 하락은 외국인 수급 부담을 낮춰 대형주 심리에 우호적일 수 있습니다.")

    if raw["market_type"] == "kr_stock_preopen":
        points.append("개장 직후에는 지수 방향보다 반도체·대형주 수급이 먼저 붙는지 확인하는 것이 중요합니다.")
    elif raw["market_type"] == "kr_stock_morning":
        points.append("오전 장세는 추격보다 주도 업종의 거래대금 유지 여부를 확인하는 대응이 유효합니다.")
    else:
        points.append("마감 기준 강한 업종은 다음 거래일 초반에도 연속성이 나오는지 점검할 필요가 있습니다.")

    return points[:4]


def openai_client():
    if not OPENAI_API_KEY:
        return None
    try:
        from openai import OpenAI
        return OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        print("openai_client error:", repr(e))
        return None


def get_us_market_snapshot() -> dict:
    # FDR에서 자주 쓰이는 미국 지수 심볼 기준
    indices = {
        "다우": "DJI",
        "S&P500": "US500",
        "나스닥": "IXIC",
    }

    result = {}
    for name, symbol in indices.items():
        result[name] = get_index_change(symbol, name)
    return result


def generate_news_items(raw: dict) -> list[str]:
    client = openai_client()
    prompt = f"""
당신은 한국 주식시장 뉴스 에디터입니다.

아래 시장 데이터 기반으로, 오늘 시간대에 맞는 뉴스 제목 2개를 작성하세요.

조건:
- 한국어
- 실제 뉴스 제목처럼 자연스럽게
- 자극적 과장 금지
- 시장 상황과 강한 섹터/약한 섹터를 반영
- 입력에 없는 사실은 만들지 말 것
- 2개만 작성
- 반드시 JSON 배열 형식으로 출력

입력 데이터:
{json.dumps(raw, ensure_ascii=False)}
"""
    if client:
        try:
            response = client.responses.create(
                model="gpt-5.4-mini",
                input=prompt,
            )
            text = response.output_text.strip()
            items = json.loads(text)
            if isinstance(items, list) and len(items) >= 2:
                return items[:2]
        except Exception as e:
            print("generate_news_items error:", repr(e))

    return [
        "코스피·코스닥 흐름 엇갈려… 환율과 수급이 핵심 변수",
        "강한 섹터 중심 매기 집중… 약한 업종은 추가 변동성 주의",
    ]


def generate_trade_points_with_gpt(raw: dict) -> list[str]:
    client = openai_client()
    stage_hint = {
        "kr_stock_preopen": "개장 전 투자 준비 관점",
        "kr_stock_morning": "오전 실제 흐름 점검 관점",
        "kr_stock_close": "마감 기준 내일 준비 관점",
    }.get(raw["market_type"], "시장 점검 관점")

    prompt = f"""
당신은 한국 주식시장 데일리 시황 애널리스트입니다.

아래 입력 데이터만 바탕으로, 오늘의 매매 포인트 4개를 작성하세요.

관점:
- {stage_hint}

규칙:
- 한국어
- 실제 투자 리포트처럼 자연스럽게
- strong_sectors / weak_sectors / 지수흐름 / 환율 방향을 반드시 반영
- 입력에 없는 뉴스, 숫자, 사실을 상상해서 쓰지 말 것
- 과장 금지
- 실질적으로 도움이 되도록 작성
- 각 문장은 서로 다른 관점이어야 함
- 반드시 JSON 배열 형식으로 출력
- 문장 길이는 한 항목당 45자~95자 정도

입력 데이터:
{json.dumps(raw, ensure_ascii=False)}
"""
    if client:
        try:
            response = client.responses.create(
                model="gpt-5.4-mini",
                input=prompt,
            )
            text = response.output_text.strip()
            items = json.loads(text)
            if isinstance(items, list) and len(items) >= 3:
                return items[:4]
        except Exception as e:
            print("generate_trade_points_with_gpt error:", repr(e))

    return build_trade_points_fallback(raw)


def build_preopen_fallback_text(raw: dict) -> str:
    us = raw.get("us_markets", {})
    fx = raw["fx"]

    us_lines = []
    for key in ["다우", "S&P500", "나스닥"]:
        item = us.get(key)
        if item:
            us_lines.append(f"- {key}: {item['close']} ({item['change_pct']}% {item['direction']})")

    fx_line = ""
    if fx:
        fx_line = f"- 원달러 환율: {fx['close']} ({fx['change_pct']}% {fx['direction']})"

    strong_lines = [f"- {sector}" for sector in raw["strong_sectors"]]
    point_lines = [f"- {p}" for p in raw["trade_points"][:4]]

    return f"""🌳 [{raw['title']}]

📌 오늘의 한줄
{raw['one_line']}

🌍 밤사이 글로벌 변수
{chr(10).join(us_lines) if us_lines else "- 미국장 주요 흐름 확인 필요"}
{fx_line if fx_line else ""}

📊 오늘 한국장 영향
- 미국장과 환율 흐름이 개장 초반 투자심리에 영향을 줄 수 있습니다.
- 지수보다 업종별 강약이 먼저 나타나는지 확인이 필요합니다.

🔥 오늘 주목 섹터
{chr(10).join(strong_lines)}

🎯 개장 직후 체크 포인트
{chr(10).join(point_lines)}

👇 아래에서 시장 풀분석도 확인하세요
"""


def build_morning_fallback_text(raw: dict) -> str:
    fx = raw["fx"]
    fx_line = ""
    if fx:
        fx_line = f"\n- 원달러 환율: {fx['close']} ({fx['change_pct']}% {fx['direction']})"

    strong_lines = [f"- {sector}" for sector in raw["strong_sectors"]]
    weak_lines = [f"- {sector}" for sector in raw["weak_sectors"]]
    point_lines = [f"- {p}" for p in raw["trade_points"][:4]]

    return f"""🌳 [{raw['title']}]

📌 오전 한줄 요약
{raw['one_line']}

📊 오전 지수 흐름
- 코스피: {raw['kospi']['close']} ({raw['kospi']['change_pct']}% {raw['kospi']['direction']})
- 코스닥: {raw['kosdaq']['close']} ({raw['kosdaq']['change_pct']}% {raw['kosdaq']['direction']}){fx_line}

💰 오전 수급 흐름
- {raw['flow_summary']}

🔥 오전 강한 섹터
{chr(10).join(strong_lines)}

🍂 오전 약한 섹터
{chr(10).join(weak_lines)}

🎯 남은 장 체크
{chr(10).join(point_lines)}

👇 아래에서 시장 풀분석도 확인하세요
"""


def build_close_fallback_text(raw: dict) -> str:
    fx = raw["fx"]
    fx_line = ""
    if fx:
        fx_line = f"\n- 원달러 환율: {fx['close']} ({fx['change_pct']}% {fx['direction']})"

    strong_lines = [f"- {sector}" for sector in raw["strong_sectors"]]
    weak_lines = [f"- {sector}" for sector in raw["weak_sectors"]]
    point_lines = [f"- {p}" for p in raw["trade_points"][:4]]

    return f"""🌳 [{raw['title']}]

📌 오늘의 한줄
{raw['one_line']}

🌡 시장 온도
- {raw['market_temperature']}

📊 마감 지수 흐름
- 코스피: {raw['kospi']['close']} ({raw['kospi']['change_pct']}% {raw['kospi']['direction']})
- 코스닥: {raw['kosdaq']['close']} ({raw['kosdaq']['change_pct']}% {raw['kosdaq']['direction']}){fx_line}

💰 마감 수급 흐름
- {raw['flow_summary']}

🔥 강한 섹터
{chr(10).join(strong_lines)}

🍂 약한 섹터
{chr(10).join(weak_lines)}

🎯 내일 체크 포인트
{chr(10).join(point_lines)}

👇 아래에서 시장 풀분석도 확인하세요
"""


def generate_chat_text_with_gpt(raw: dict) -> str:
    client = openai_client()

    stage_prompt_map = {
        "kr_stock_preopen": """
당신은 한국 주식시장 개장 전 시황 에디터입니다.

반드시 반영할 것:
- 밤사이 미국장 흐름
- 환율/달러 움직임
- 오늘 한국장 개장에 줄 수 있는 영향
- 오늘 주목 업종
- 개장 직후 무엇을 확인해야 하는지

중요:
- 아직 한국장은 시작 전이다
- 장마감 요약처럼 쓰지 말 것
- "준비", "영향", "개장 후 확인" 관점으로 작성
""",
        "kr_stock_morning": """
당신은 한국 주식시장 오전 시황 에디터입니다.

반드시 반영할 것:
- 오전 실제 흐름 요약
- 지수/섹터 강약
- 장 초반 자금이 어디에 몰리는지
- 남은 장에서 무엇을 확인해야 하는지

중요:
- 아직 장중이다
- 마감 요약처럼 쓰지 말 것
- "오전 기준", "장 초반", "남은 장" 관점으로 작성
""",
        "kr_stock_close": """
당신은 한국 주식시장 장마감 시황 에디터입니다.

반드시 반영할 것:
- 오늘 시장이 어떻게 끝났는지
- 강/약 업종 정리
- 수급 흐름 의미
- 내일 체크할 변수

중요:
- 장이 끝난 시점이다
- 하루 결론과 내일 준비 관점으로 작성
""",
    }

    prompt = f"""
{stage_prompt_map.get(raw["market_type"], "")}

아래 입력 데이터만 바탕으로 카카오 메시지용 시황 텍스트를 작성하세요.

규칙:
- 한국어
- 투자 초보도 이해할 수 있게
- 하지만 내용은 얕지 않게
- 입력에 없는 사실은 만들지 말 것
- 숫자는 입력 데이터만 사용
- 전체 700자 내외 권장
- 마지막 줄은 반드시 "👇 아래에서 시장 풀분석도 확인하세요"로 끝낼 것

입력 데이터:
{json.dumps(raw, ensure_ascii=False)}
"""
    if client:
        try:
            response = client.responses.create(
                model="gpt-5.4-mini",
                input=prompt,
            )
            text = response.output_text.strip()
            if text:
                return text
        except Exception as e:
            print("generate_chat_text_with_gpt error:", repr(e))

    if raw["market_type"] == "kr_stock_preopen":
        return build_preopen_fallback_text(raw)
    if raw["market_type"] == "kr_stock_morning":
        return build_morning_fallback_text(raw)
    return build_close_fallback_text(raw)


def insert_shortcode_ad(content: str) -> str:
    parts = content.split("<h2>")
    if len(parts) >= 5:
        return "<h2>".join(parts[:4]) + ADINSERTER_SHORTCODE + "<h2>" + "<h2>".join(parts[4:])
    return content + ADINSERTER_SHORTCODE


def build_wordpress_article(raw: dict) -> tuple[str, str, str, str]:
    client = openai_client()
    prompt = f"""
당신은 한국 주식시장 전문 애널리스트이자 블로그 에디터입니다.

아래 데이터를 바탕으로 WordPress용 장마감 해설 글을 작성하세요.

중요 원칙:
- 한국어
- 초보자도 이해 가능하지만 얕지 않게
- 실제 투자 판단에 도움이 되도록 작성
- 입력 데이터에 없는 뉴스/사실/숫자는 만들지 말 것
- strong/weak sector는 입력 데이터 기준 사용
- news_items는 반드시 본문에 포함
- 과장 금지
- 숫자는 입력 데이터 그대로 유지
- HTML 형식
- 출력은 반드시 JSON
- JSON 키는 title, excerpt, content 3개

입력 데이터:
{json.dumps(raw, ensure_ascii=False)}
"""
    if client:
        try:
            response = client.responses.create(
                model="gpt-5.4-mini",
                input=prompt,
            )
            text = response.output_text.strip()
            parsed = json.loads(text)

            title = parsed["title"]
            excerpt = parsed["excerpt"]
            content = parsed["content"]
            slug = f"{raw['market_type']}-{raw['summary_date']}"
            return title, excerpt, insert_shortcode_ad(content), slug
        except Exception as e:
            print("build_wordpress_article gpt error:", repr(e))

    title = f"{raw['summary_date']} {raw['wp_keyword']} | 코스피·코스닥·환율 흐름"
    excerpt = raw["one_line"]
    content = f"""
<h2>📊 오늘 시장 한줄 요약</h2>
<p>{raw["one_line"]}</p>

<h2>📉 시장 전체 흐름 분석</h2>
<p>{raw["flow_summary"]}</p>

<h2>💡 투자 전략</h2>
<ul>{"".join(f"<li>{p}</li>" for p in raw["trade_points"])}</ul>
"""
    slug = f"{raw['market_type']}-{raw['summary_date']}"
    return title, excerpt, insert_shortcode_ad(content), slug


def publish_to_wordpress(title: str, excerpt: str, content: str, slug: str) -> tuple[str, str]:
    endpoint = f"{WP_SITE_URL}/wp-json/wp/v2/posts"

    token = base64.b64encode(f"{WP_USERNAME}:{WP_APP_PASSWORD}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "title": title,
        "excerpt": excerpt,
        "content": content,
        "slug": slug,
        "status": "publish",
        "categories": [WP_CATEGORY_ID],
    }

    response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
    print("wordpress_status:", response.status_code)
    print("wordpress_response:", response.text)

    if response.status_code not in [200, 201]:
        raise Exception(f"WordPress publish failed: {response.status_code} {response.text}")

    data = response.json()
    return data["title"]["rendered"], data["link"]


def upsert_summary(row: dict):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?on_conflict=summary_date,market_type"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    response = requests.post(url, headers=headers, json=row, timeout=30)
    print("supabase_status:", response.status_code)
    print("supabase_response:", response.text)

    if response.status_code not in [200, 201]:
        raise Exception(f"Supabase update failed: {response.status_code} {response.text}")


def build_preopen_one_line(kospi: dict, kosdaq: dict, fx: dict | None) -> str:
    if fx:
        return "밤사이 미국장과 환율 흐름을 반영하면, 오늘 한국장은 개장 초반 업종별 차별화 여부를 먼저 확인할 필요가 있습니다."
    return "밤사이 글로벌 변수와 미국장 흐름을 바탕으로 오늘 한국장 개장 초반 방향성을 점검할 필요가 있습니다."


def build_morning_one_line(kospi: dict, kosdaq: dict) -> str:
    return f"오전 기준 코스피는 {kospi['direction']}, 코스닥은 {kosdaq['direction']} 흐름입니다."


def build_close_one_line(kospi: dict, kosdaq: dict) -> str:
    return f"오늘 시장은 코스피 {kospi['direction']}, 코스닥 {kosdaq['direction']} 흐름을 보였습니다."


def build_raw_summary() -> dict:
    now = now_kst()
    stage = market_stage(now)
    meta = stage_meta(stage)

    print("FINAL STAGE:", stage)
    print("FINAL MARKET TYPE:", meta["market_type"])

    kospi = get_index_change("KS11", "코스피")
    kosdaq = get_index_change("KQ11", "코스닥")
    fx = get_fx_change()
    us_markets = get_us_market_snapshot() if stage == "pre_open" else {}

    strong_sectors, weak_sectors, sector_scores = infer_sectors_from_representatives()
    market_temp = get_market_temperature(kospi["change_pct"], kosdaq["change_pct"])
    flow_summary = get_flow_summary(kospi["change_pct"], kosdaq["change_pct"], fx)

    if stage == "pre_open":
        one_line = build_preopen_one_line(kospi, kosdaq, fx)
    elif stage == "intraday":
        one_line = build_morning_one_line(kospi, kosdaq)
    else:
        one_line = build_close_one_line(kospi, kosdaq)

    raw = {
        "summary_date": str(date.today()),
        "market_type": meta["market_type"],
        "title": meta["title"],
        "one_line": one_line,
        "market_temperature": market_temp,
        "flow_summary": flow_summary,
        "kospi": kospi,
        "kosdaq": kosdaq,
        "fx": fx,
        "us_markets": us_markets,
        "strong_sectors": strong_sectors,
        "weak_sectors": weak_sectors,
        "sector_scores": sector_scores,
        "check_label": meta["check_label"],
        "wp_keyword": meta["wp_keyword"],
    }

    raw["news_items"] = generate_news_items(raw)
    raw["trade_points"] = generate_trade_points_with_gpt(raw)

    return raw


def main():
    raw = build_raw_summary()
    chat_text = generate_chat_text_with_gpt(raw)

    post_title = ""
    post_url = ""

    if raw["market_type"] == "kr_stock_close":
        wp_title, wp_excerpt, wp_content, wp_slug = build_wordpress_article(raw)
        post_title, post_url = publish_to_wordpress(wp_title, wp_excerpt, wp_content, wp_slug)

    row = {
        "summary_date": raw["summary_date"],
        "market_type": raw["market_type"],
        "one_line": raw["one_line"],
        "kospi_text": f"코스피는 {raw['kospi']['close']}에서 {raw['kospi']['change_pct']}% {raw['kospi']['direction']} 흐름입니다.",
        "kosdaq_text": f"코스닥은 {raw['kosdaq']['close']}에서 {raw['kosdaq']['change_pct']}% {raw['kosdaq']['direction']} 흐름입니다.",
        "strong_sectors": ", ".join(raw["strong_sectors"]),
        "weak_sectors": ", ".join(raw["weak_sectors"]),
        "tomorrow_points": " / ".join(raw["trade_points"]),
        "full_text": chat_text,
        "post_title": post_title,
        "post_url": post_url,
    }

    upsert_summary(row)


if __name__ == "__main__":
    main()
