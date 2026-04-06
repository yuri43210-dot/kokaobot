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

# 섹터별 대표 종목 + 가중치
# weight 합은 섹터별로 1.0 기준 권장
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
        {"ticker": "003490", "name": "대한전선", "weight": 0.12},
    ],
    "AI/인터넷": [
        {"ticker": "035420", "name": "NAVER", "weight": 0.40},
        {"ticker": "035720", "name": "카카오", "weight": 0.28},
        {"ticker": "304100", "name": "솔트룩스", "weight": 0.12},
        {"ticker": "318000", "name": "에스엠코어", "weight": 0.10},
        {"ticker": "047560", "name": "이스트소프트", "weight": 0.10},
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
        {"ticker": "055550", "name": "신한지주", "weight": 0.28},
        {"ticker": "105560", "name": "KB금융", "weight": 0.34},
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
        }
    if stage == "intraday":
        return {
            "market_type": "kr_stock_morning",
            "title": "오늘의 오전 시황",
            "one_line": None,  # 동적 생성
            "check_label": "남은 장 체크",
            "wp_keyword": "오전 시황",
        }
    return {
        "market_type": "kr_stock_close",
        "title": "오늘의 장마감 요약",
        "one_line": None,  # 동적 생성
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
    except Exception:
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
    except Exception:
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
    except Exception:
        return None


def infer_sectors_from_representatives() -> tuple[list[str], list[str], dict]:
    sector_scores = {}

    for sector, items in SECTOR_CONFIG.items():
        valid_items = []

        for item in items:
            pct = get_stock_change_pct(item["ticker"])
            print(f"[{sector}] {item['ticker']} {item['name']} = {pct}")

            if pct is not None:
                valid_items.append({
                    "ticker": item["ticker"],
                    "name": item["name"],
                    "weight": item["weight"],
                    "pct": pct,
                })

        if not valid_items:
            continue

        # 데이터가 있는 종목만 대상으로 가중치 재정규화
        weight_sum = sum(x["weight"] for x in valid_items)
        if weight_sum == 0:
            continue

        weighted_score = 0.0
        details = []

        for x in valid_items:
            normalized_weight = x["weight"] / weight_sum
            contrib = x["pct"] * normalized_weight
            weighted_score += contrib
            details.append(
                f"{x['name']}({x['pct']}% × {round(normalized_weight, 4)})"
            )

        sector_scores[sector] = round(weighted_score, 2)
        print(f"[{sector}] weighted avg = {sector_scores[sector]}, details = {details}")

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


def build_trade_points(
    strong_sectors: list[str],
    weak_sectors: list[str],
    sector_scores: dict,
    kospi: dict,
    kosdaq: dict,
) -> list[str]:
    points = []

    if strong_sectors:
        strong = strong_sectors[0]
        strong_score = sector_scores.get(strong)
        if strong_score is not None:
            points.append(f"{strong}는 상대 강도 {strong_score}%로 방어 또는 주도 여부를 확인할 필요가 있습니다.")

    if weak_sectors:
        weak = weak_sectors[0]
        weak_score = sector_scores.get(weak)
        if weak_score is not None:
            points.append(f"{weak}는 {weak_score}% 수준의 약세를 보여 추가 하락 여부를 주시해야 합니다.")

    if kospi["change_pct"] <= -2 or kosdaq["change_pct"] <= -2:
        points.append("지수 급락 구간에서는 단기 반등보다 리스크 관리가 우선입니다.")
    elif kospi["change_pct"] >= 1 or kosdaq["change_pct"] >= 1:
        points.append("강세 구간에서는 추격 매수보다 주도 섹터 압축 여부를 살피는 것이 좋습니다.")
    else:
        points.append("혼조 구간에서는 거래대금이 몰리는 종목과 테마를 체크해야 합니다.")

    return points[:3]


def generate_news_items(raw: dict) -> list[str]:
    prompt = f"""
당신은 한국 주식시장 뉴스 에디터입니다.

아래 시장 데이터 기반으로, 오늘 시간대에 맞는 뉴스 제목 2개를 작성하세요.

조건:
- 한국어
- 실제 뉴스 제목처럼 자연스럽게
- 자극적 과장 금지
- 시장 상황과 강한 섹터/약한 섹터를 반영
- 2개만 작성
- 반드시 JSON 배열 형식으로 출력

입력 데이터:
{json.dumps(raw, ensure_ascii=False)}
"""

    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            response = client.responses.create(
                model="gpt-5.4-mini",
                input=prompt,
            )
            text = response.output_text.strip()
            items = json.loads(text)
            if isinstance(items, list) and len(items) >= 2:
                return items[:2]
        except Exception:
            pass

    return [
        "코스피·코스닥 흐름 엇갈려… 환율과 수급이 핵심 변수",
        "강한 섹터 중심 매기 집중… 약한 업종은 추가 변동성 주의",
    ]


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
    trade_points = build_trade_points(strong_sectors, weak_sectors, sector_scores, kospi, kosdaq)

    if stage == "pre_open":
        one_line = meta["one_line"]
    elif stage == "intraday":
        one_line = f"오전 기준 코스피는 {kospi['direction']}, 코스닥은 {kosdaq['direction']} 흐름입니다."
    else:
        one_line = f"오늘 시장은 코스피 {kospi['direction']}, 코스닥 {kosdaq['direction']} 흐름을 보였습니다."

    news_items = generate_news_items({
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
        "trade_points": trade_points,
        "check_label": meta["check_label"],
    })

    return {
        "summary_date": str(date.today()),
        "market_type": meta["market_type"],
        "title": meta["title"],
        "one_line": one_line,
        "market_temperature": market_temp,
        "flow_summary": flow_summary,
        "trade_points": trade_points,
        "kospi": kospi,
        "kosdaq": kosdaq,
        "fx": fx,
        "strong_sectors": strong_sectors,
        "weak_sectors": weak_sectors,
        "sector_scores": sector_scores,
        "check_label": meta["check_label"],
        "news_items": news_items,
        "wp_keyword": meta["wp_keyword"],
    }


def build_chat_text(raw: dict) -> str:
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


def insert_shortcode_ad(content: str) -> str:
    parts = content.split("<h2>")
    if len(parts) >= 5:
        return "<h2>".join(parts[:4]) + ADINSERTER_SHORTCODE + "<h2>" + "<h2>".join(parts[4:])
    return content + ADINSERTER_SHORTCODE


def build_wordpress_article(raw: dict) -> tuple[str, str, str, str]:
    prompt = f"""
당신은 한국 주식시장 전문 애널리스트이자 블로그 에디터입니다.

아래 데이터를 바탕으로 WordPress용 시장 해설 글을 작성하세요.

조건:
- 한국어
- 너무 딱딱하지 않게
- 초보자도 이해할 수 있게
- 전문가처럼 이유와 흐름을 설명
- HTML 형식으로 작성
- 제목(title), 요약(excerpt), 본문(content)을 JSON 형식으로 출력
- 본문 구조:
  1. 오늘 시장 한줄 요약
  2. 시장 전체 흐름 분석
  3. 지수 상세 분석
  4. 강한 섹터와 그 이유
  5. 약한 섹터와 그 이유
  6. 글로벌 경제 이슈
  7. 관련 뉴스 2개
  8. 내일/남은 장 체크포인트
  9. 투자 전략
- strong/weak sector는 입력 데이터 기준 사용
- news_items는 본문에 포함
- 숫자는 유지
- 출력은 반드시 JSON

입력 데이터:
{json.dumps(raw, ensure_ascii=False)}
"""

    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
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
        except Exception:
            pass

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

<h2>🔥 강한 섹터와 이유</h2>
<ul>{strong_html}</ul>
<p>상대적으로 강세를 보인 섹터들로, 수급이 몰리거나 주도 테마로 해석될 가능성이 있습니다.</p>

<h2>🍂 약한 섹터와 이유</h2>
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
<p>강한 섹터는 추세 지속 여부를, 약한 섹터는 추가 하락 여부를 중심으로 보는 전략이 유효합니다.</p>
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


def main():
    raw = build_raw_summary()
    chat_text = build_chat_text(raw)

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
        "tomorrow_points": "미국 증시 흐름, 환율, 외국인 수급",
        "full_text": chat_text,
        "post_title": post_title,
        "post_url": post_url,
    }

    upsert_summary(row)


if __name__ == "__main__":
    main()
