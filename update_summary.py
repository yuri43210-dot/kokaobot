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
            "one_line": "개장 전 주요 변수와 최근 시장 흐름을 기준으로 장을 준비하는 시간입니다.",
            "check_label": "오늘 체크",
            "wp_keyword": "개장 전 시황",
            "fallback_post_title": "📘 어제 장마감 분석 보기",
        }
    if stage == "intraday":
        return {
            "market_type": "kr_stock_morning",
            "title": "오늘의 오전 시황",
            "one_line": None,
            "check_label": "남은 장 체크",
            "wp_keyword": "오전 시황",
            "fallback_post_title": "📘 최근 장마감 분석 보기",
        }
    return {
        "market_type": "kr_stock_close",
        "title": "오늘의 장마감 요약",
        "one_line": None,
        "check_label": "내일 체크",
        "wp_keyword": "장마감 시황",
        "fallback_post_title": "📊 오늘 장마감 분석 보기",
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


def build_trade_points_fallback(
    strong_sectors: list[str],
    weak_sectors: list[str],
    sector_scores: dict,
    kospi: dict,
    kosdaq: dict,
    fx: dict | None
) -> list[str]:
    points = []

    market_up = kospi["change_pct"] > 0 and kosdaq["change_pct"] > 0
    market_down = kospi["change_pct"] < 0 and kosdaq["change_pct"] < 0

    if strong_sectors:
        s = strong_sectors[0]
        score = sector_scores.get(s, 0)
        if market_up:
            points.append(f"{s}는 시장 상승 속에서 {score}% 강세를 보이며 주도 섹터 가능성이 높습니다.")
        elif market_down:
            points.append(f"{s}는 시장 약세에도 {score}% 상승하며 방어적 강세가 나타나고 있습니다.")
        else:
            points.append(f"{s}는 혼조 장세 속에서 상대적으로 강한 흐름({score}%)을 유지하고 있습니다.")

    if weak_sectors:
        w = weak_sectors[0]
        score = sector_scores.get(w, 0)
        if market_up:
            points.append(f"{w}는 시장 상승에도 불구하고 {score}% 약세를 보이며 소외 흐름이 나타나고 있습니다.")
        elif market_down:
            points.append(f"{w}는 시장 하락과 함께 {score}% 약세를 보이며 동반 하락 흐름입니다.")
        else:
            points.append(f"{w}는 방향성 없는 장세에서 {score}% 약세를 보이며 힘이 부족한 상태입니다.")

    if fx:
        if fx["direction"] == "상승":
            points.append("환율 상승 흐름은 외국인 수급 부담과 수출주 강세 가능성을 함께 시사합니다.")
        else:
            points.append("환율 하락은 외국인 수급 개선 가능성을 시사합니다.")

    if kospi["change_pct"] >= 1 or kosdaq["change_pct"] >= 1:
        points.append("지수 강세 구간에서는 추격 매수보다 주도 섹터 집중 전략이 유효합니다.")
    elif kospi["change_pct"] <= -1 or kosdaq["change_pct"] <= -1:
        points.append("지수 약세 구간에서는 신규 진입보다 리스크 관리가 우선입니다.")
    else:
        points.append("혼조 구간에서는 거래대금이 몰리는 종목 중심 대응이 중요합니다.")

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
    prompt = f"""
당신은 한국 주식시장 데일리 시황 애널리스트입니다.

아래 입력 데이터만 바탕으로, 오늘의 매매 포인트 4개를 작성하세요.

규칙:
- 한국어
- 실제 투자 리포트처럼 자연스럽게
- strong_sectors / weak_sectors / 지수흐름 / 환율 방향을 반드시 반영
- 입력에 없는 뉴스, 숫자, 사실을 상상해서 쓰지 말 것
- 과장 금지
- 실질적으로 도움이 되도록 작성
- 각 문장은 서로 다른 관점이어야 함
- 반드시 JSON 배열 형식으로 출력
- 문장 길이는 한 항목당 45자~90자 정도
- "무조건 상승", "확실", "반드시" 같은 표현 금지

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

    return build_trade_points_fallback(
        raw["strong_sectors"],
        raw["weak_sectors"],
        raw["sector_scores"],
        raw["kospi"],
        raw["kosdaq"],
        raw["fx"],
    )


def build_chat_text_fallback(raw: dict) -> str:
    fx = raw["fx"]
    fx_line = ""
    if fx:
        fx_line = f"\n- 원달러 환율: {fx['close']} ({fx['direction']}, {fx['change_pct']}%)"

    strong_lines = []
    for sector in raw["strong_sectors"]:
        score = raw["sector_scores"].get(sector)
        if score is not None:
            strong_lines.append(f"- {sector} ({score}%)")
        else:
            strong_lines.append(f"- {sector}")

    weak_lines = []
    for sector in raw["weak_sectors"]:
        score = raw["sector_scores"].get(sector)
        if score is not None:
            weak_lines.append(f"- {sector} ({score}%)")
        else:
            weak_lines.append(f"- {sector}")

    point_lines = [f"- {p}" for p in raw["trade_points"]]

    return f"""🌳 [{raw['title']}]

📌 오늘의 한줄
{raw['one_line']}

🌡 시장 온도
- {raw['market_temperature']}

📊 지수 흐름
- 코스피: {raw['kospi']['close']} ({raw['kospi']['change_pct']}%)
- 코스닥: {raw['kosdaq']['close']} ({raw['kosdaq']['change_pct']}%){fx_line}

💰 수급 흐름
- {raw['flow_summary']}

🔥 강한 섹터
{chr(10).join(strong_lines)}

🍂 약한 섹터
{chr(10).join(weak_lines)}

🎯 오늘의 매매 포인트
{chr(10).join(point_lines)}

📅 {raw['check_label']}
- 미국 증시 흐름
- 환율
- 외국인 수급

👇 아래에서 시장 풀분석도 확인하세요
"""


def generate_chat_text_with_gpt(raw: dict) -> str:
    client = openai_client()
    prompt = f"""
당신은 카카오 채널용 한국 주식시장 시황 에디터입니다.

아래 입력 데이터만 바탕으로 카카오 메시지용 시황 텍스트를 작성하세요.

규칙:
- 한국어
- 투자 초보도 이해할 수 있게
- 하지만 내용은 얕지 않게
- 반드시 아래 섹션 순서를 유지
- 입력에 없는 사실은 만들지 말 것
- 숫자는 입력 데이터의 값만 사용
- 지나치게 길지 않게, 전체 700자 이내 권장
- 마지막 줄은 반드시 "👇 아래에서 시장 풀분석도 확인하세요"로 끝낼 것

출력 형식:
🌳 [제목]

📌 오늘의 한줄
...

🌡 시장 온도
- ...

📊 지수 흐름
- ...
- ...
- ...(환율 있으면)

💰 수급 흐름
- ...

🔥 강한 섹터
- ...
- ...
- ...

🍂 약한 섹터
- ...
- ...
- ...

🎯 오늘의 매매 포인트
- ...
- ...
- ...
- ...

📅 체크 라벨
- 미국 증시 흐름
- 환율
- 외국인 수급

👇 아래에서 시장 풀분석도 확인하세요

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

    return build_chat_text_fallback(raw)


def insert_shortcode_ad(content: str) -> str:
    parts = content.split("<h2>")
    if len(parts) >= 5:
        return "<h2>".join(parts[:4]) + ADINSERTER_SHORTCODE + "<h2>" + "<h2>".join(parts[4:])
    return content + ADINSERTER_SHORTCODE


def build_wordpress_article(raw: dict) -> tuple[str, str, str, str]:
    client = openai_client()
    prompt = f"""
당신은 한국 주식시장 전문 애널리스트이자 블로그 에디터입니다.

아래 데이터를 바탕으로 WordPress용 시장 해설 글을 작성하세요.

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

본문 구조:
1. 오늘 시장 한줄 요약
2. 시장 전체 흐름 분석
3. 지수 상세 분석
4. 강한 섹터와 그 이유
5. 약한 섹터와 그 이유
6. 글로벌 경제 이슈
7. 관련 뉴스 2개
8. {raw["check_label"]}
9. 투자 전략

투자 전략 섹션은 특히 중요:
- 단순 반복 문장 금지
- 오늘 시장 구조에서 무엇을 먼저 확인해야 하는지
- 추격/관망/압축/분산 중 어떤 태도가 유리한지
- 강한 섹터와 약한 섹터를 어떻게 다르게 봐야 하는지
를 구체적으로 써야 함

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

    fx_html = ""
    if raw["fx"]:
        fx_html = f"<li>원달러 환율: {raw['fx']['close']} ({raw['fx']['direction']}, {raw['fx']['change_pct']}%)</li>"

    strong_html = "".join(
        f"<li>{sector} ({raw['sector_scores'].get(sector, 'n/a')}%)</li>"
        for sector in raw["strong_sectors"]
    )
    weak_html = "".join(
        f"<li>{sector} ({raw['sector_scores'].get(sector, 'n/a')}%)</li>"
        for sector in raw["weak_sectors"]
    )
    points_html = "".join(f"<li>{p}</li>" for p in raw["trade_points"])
    news_html = "".join(f"<li>{item}</li>" for item in raw["news_items"])

    title = f"{raw['summary_date']} {raw['wp_keyword']} | 코스피·코스닥·환율 흐름"
    excerpt = raw["one_line"]

    content = f"""
<h2>📊 오늘 시장 한줄 요약</h2>
<p>{raw["one_line"]}</p>

<h2>🌡 시장 온도</h2>
<p>{raw["market_temperature"]}</p>

<h2>📉 시장 전체 흐름 분석</h2>
<p>{raw["flow_summary"]}</p>

<h2>📈 지수 상세 분석</h2>
<ul>
  <li>코스피: {raw['kospi']['close']} ({raw['kospi']['change_pct']}%)</li>
  <li>코스닥: {raw['kosdaq']['close']} ({raw['kosdaq']['change_pct']}%)</li>
  {fx_html}
</ul>

<h2>🔥 강한 섹터와 그 이유</h2>
<ul>{strong_html}</ul>
<p>상대적으로 강세를 보인 섹터들로, 대형주 중심의 가중치와 수급 흐름을 함께 해석할 필요가 있습니다.</p>

<h2>🍂 약한 섹터와 그 이유</h2>
<ul>{weak_html}</ul>
<p>상대적으로 약세를 보인 섹터들로, 차익실현 또는 투자심리 위축 영향을 받았을 가능성이 있습니다.</p>

<h2>🌍 글로벌 경제 이슈</h2>
<p>미국 증시 흐름, 달러 움직임, 금리 기대 변화는 국내 주식시장에 직접적인 영향을 줄 수 있습니다. 특히 환율 방향은 외국인 수급과 연결될 가능성이 높습니다.</p>

<h2>📰 관련 뉴스</h2>
<ul>{news_html}</ul>

<h2>📌 {raw["check_label"]}</h2>
<p>미국 증시 방향, 원달러 환율, 외국인 수급 지속 여부를 함께 확인해야 합니다. 특히 강한 섹터가 계속 주도권을 유지하는지가 중요합니다.</p>

<h2>💡 투자 전략</h2>
<ul>{points_html}</ul>
<p>강한 섹터는 추세 지속 여부를, 약한 섹터는 추가 하락 여부를 중심으로 보는 대응이 유효합니다.</p>
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


def fetch_latest_close_post_meta() -> tuple[str, str]:
    url = (
        f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
        f"?select=post_title,post_url,summary_date"
        f"&market_type=eq.kr_stock_close"
        f"&post_url=not.is.null"
        f"&order=summary_date.desc"
        f"&limit=1"
    )

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers, timeout=20)
    print("latest_close_status:", response.status_code)
    print("latest_close_response:", response.text[:500])

    if response.status_code not in [200]:
        return "", ""

    data = response.json()
    if not isinstance(data, list) or len(data) == 0:
        return "", ""

    latest = data[0]
    return (latest.get("post_title") or "").strip(), (latest.get("post_url") or "").strip()


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


def build_raw_summary() -> dict:
    now = now_kst()
    stage = market_stage(now)
    meta = stage_meta(stage)

    kospi = get_index_change("KS11", "코스피")
    kosdaq = get_index_change("KQ11", "코스닥")
    fx = get_fx_change()

    strong_sectors, weak_sectors, sector_scores = infer_sectors_from_representatives()
    market_temp = get_market_temperature(kospi["change_pct"], kosdaq["change_pct"])
    flow_summary = get_flow_summary(kospi["change_pct"], kosdaq["change_pct"], fx)

    if stage == "pre_open":
        one_line = meta["one_line"]
    elif stage == "intraday":
        one_line = f"오전 기준 코스피는 {kospi['direction']}, 코스닥은 {kosdaq['direction']} 흐름입니다."
    else:
        one_line = f"오늘 시장은 코스피 {kospi['direction']}, 코스닥 {kosdaq['direction']} 흐름을 보였습니다."

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
        "strong_sectors": strong_sectors,
        "weak_sectors": weak_sectors,
        "sector_scores": sector_scores,
        "check_label": meta["check_label"],
        "wp_keyword": meta["wp_keyword"],
        "fallback_post_title": meta["fallback_post_title"],
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
    else:
        last_close_title, last_close_url = fetch_latest_close_post_meta()
        post_title = raw["fallback_post_title"]
        post_url = last_close_url

        if not post_url:
            post_title = raw["title"]
            post_url = ""

        if last_close_title and raw["market_type"] == "kr_stock_morning":
            post_title = "📘 최근 장마감 분석 보기"

        if last_close_title and raw["market_type"] == "kr_stock_preopen":
            post_title = "📘 어제 장마감 분석 보기"

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
