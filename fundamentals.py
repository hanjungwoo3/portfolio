"""한국 주식 기업가치 지표 수집 모듈.

데이터 소스:
- finance.naver.com/item/main.naver  → 시가총액, PER, PBR, EPS, BPS, 배당수익률,
                                        52주 최고/최저, 외국인소진율, 동일업종 PER
- navercomp.wisereport.co.kr cF1001 → 매출액, 영업이익, 영업이익률, 순이익률,
                                        ROE, ROA, 부채비율, 현금DPS, 현금배당성향

캐시: data/fundamentals_cache.json (TTL 12시간).
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup


SCRIPT_DIR = Path(__file__).resolve().parent
CACHE_PATH = SCRIPT_DIR / "data" / "fundamentals_cache.json"
CACHE_TTL_SEC = 12 * 3600

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0 Safari/537.36")


# ──────────────────────────── 지표 라벨/단위/설명 ────────────────────────────
INDICATOR_SECTIONS = [
    ("📊 가치평가",   "주가가 비싼지 싼지 판단",
        ["market_cap_text", "per", "pbr", "eps", "bps", "industry_per"]),
    ("💰 수익성",     "회사가 얼마나 잘 버는지",
        ["revenue", "operating_income", "operating_margin", "net_margin", "roe"]),
    ("🎁 주주환원",   "주주에게 돌려주는 정도",
        ["dividend_yield", "dps", "dividend_payout"]),
    ("🏦 재무건전성", "빚이 너무 많지 않은지",
        ["debt_ratio"]),
    ("📈 가격 통계",  "최근 1년 가격 흐름과 외국인 매수세",
        ["high_52w", "low_52w", "foreign_ownership"]),
]

INDICATOR_LABELS = {
    "market_cap_text":   "시가총액",
    "per":               "PER",
    "pbr":               "PBR",
    "eps":               "EPS",
    "bps":               "BPS",
    "industry_per":      "동일업종 PER",
    "revenue":           "매출액",
    "operating_income":  "영업이익",
    "operating_margin":  "영업이익률",
    "net_margin":        "순이익률",
    "roe":               "ROE",
    "dividend_yield":    "배당수익률",
    "dps":               "DPS",
    "dividend_payout":   "배당성향",
    "debt_ratio":        "부채비율",
    "high_52w":          "52주 최고",
    "low_52w":           "52주 최저",
    "foreign_ownership": "외국인 보유율",
}

INDICATOR_UNITS = {
    "market_cap_text":   "",
    "per":               "배",
    "pbr":               "배",
    "eps":               "원",
    "bps":               "원",
    "industry_per":      "배",
    "revenue":           "억원",
    "operating_income":  "억원",
    "operating_margin":  "%",
    "net_margin":        "%",
    "roe":               "%",
    "dividend_yield":    "%",
    "dps":               "원",
    "dividend_payout":   "%",
    "debt_ratio":        "%",
    "high_52w":          "원",
    "low_52w":           "원",
    "foreign_ownership": "%",
}

INDICATOR_DESCRIPTIONS = {
    "market_cap_text":
        "회사 전체의 시장 가격. 발행주식수 × 주가. 회사 규모 판단 기준.",
    "per":
        "주가 ÷ 1주당 순이익(EPS). 회사가 번 돈으로 투자금을 회수하는 데 "
        "몇 년 걸리는지 의미. 낮을수록 저평가. 시장 평균 약 15배.",
    "pbr":
        "주가 ÷ 1주당 순자산(BPS). 회사를 청산했을 때 받을 자산가치 대비 "
        "주가 수준. 1 미만이면 자산가치보다 싸게 거래되는 중.",
    "eps":
        "1주당 순이익. 회사가 1년 동안 번 순이익을 발행주식수로 나눈 값. "
        "클수록 수익성 좋음.",
    "bps":
        "1주당 순자산. 회사 총자산에서 부채를 뺀 후 주식수로 나눈 값. "
        "청산가치의 기준.",
    "industry_per":
        "같은 업종 평균 PER. 종목의 PER 가 이 값보다 낮으면 동종업계 "
        "대비 저평가, 높으면 고평가.",
    "revenue":
        "1년 동안 회사가 판매한 총금액(연간). 회사의 외형 크기를 보여줌.",
    "operating_income":
        "본업으로 번 이익(연간). 매출 − 매출원가 − 판관비. "
        "영업외 손익 제외.",
    "operating_margin":
        "영업이익 ÷ 매출액. 본업으로 매출 100원 중 몇 원을 남기는지. "
        "높을수록 경쟁력 있음.",
    "net_margin":
        "순이익 ÷ 매출액. 모든 비용·세금 제하고 매출 중 남는 비율.",
    "roe":
        "자기자본수익률(ROE). 주주 돈 100원으로 1년 동안 몇 원을 벌었는지. "
        "워런 버핏 기준 15% 이상 선호.",
    "dividend_yield":
        "1주당 연 배당금 ÷ 주가. 주식 보유만으로 받는 이자율 같은 개념.",
    "dps":
        "1주당 연간 배당금. 100주 보유 시 연간 받는 배당금 = DPS × 100.",
    "dividend_payout":
        "순이익 중 배당으로 푸는 비율. 너무 높으면 성장 재투자가 줄어들 수 있음.",
    "debt_ratio":
        "부채총계 ÷ 자기자본. 빚이 자기자본의 몇 배인지. "
        "200% 이하 권장, 100% 이하 우량.",
    "high_52w":
        "최근 1년간 최고가. 현재가가 여기 가까우면 신고가 부근.",
    "low_52w":
        "최근 1년간 최저가. 현재가가 여기 가까우면 바닥권.",
    "foreign_ownership":
        "외국인이 보유한 주식 비율. 높고 꾸준히 늘면 외국인이 좋게 평가.",
}


# ──────────────────────────── 캐시 ────────────────────────────
def _load_cache() -> dict:
    if not CACHE_PATH.exists():
        return {}
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ──────────────────────────── 파서 헬퍼 ────────────────────────────
def _to_float(s: str) -> float | None:
    if s is None:
        return None
    s = s.replace(",", "").replace("%", "").strip()
    if not s or s in ("-", "—", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(s: str) -> int | None:
    v = _to_float(s)
    return int(v) if v is not None else None


def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


# ──────────────────────────── 메인 페이지 (현재가 기준 지표) ────────────────────────────
def _fetch_main_page(code: str) -> dict[str, Any]:
    """finance.naver.com 메인 페이지 — 현재가 기준 PER/PBR/시총/배당수익률 등."""
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=8)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    out: dict[str, Any] = {}

    # 종목명
    title_node = soup.select_one("div.wrap_company h2 a")
    if title_node:
        out["name"] = title_node.get_text(strip=True)

    # 현재가
    cur_node = soup.select_one("p.no_today span.blind")
    if cur_node:
        out["price"] = _to_int(cur_node.get_text(strip=True))

    # em#_xxx 매핑
    em_map = {
        "_market_sum":  "market_cap_text",   # 텍스트 그대로 (1,312조 4,895)
        "_per":         "per",
        "_eps":         "eps",
        "_pbr":         "pbr",
        "_dvr":         "dividend_yield",
    }
    for em_id, key in em_map.items():
        node = soup.select_one(f"em#{em_id}")
        if not node:
            continue
        raw = _clean_ws(node.get_text(" ", strip=True))
        if key == "market_cap_text":
            out[key] = raw + "억원" if raw else None
        elif key in ("eps",):
            out[key] = _to_int(raw)
        else:
            out[key] = _to_float(raw)

    # BPS — PBR 행에 함께 노출 (em#_pbr 다음 em)
    pbr_row = None
    for tr in soup.select("div.aside_invest_info table tr"):
        if "PBR" in tr.get_text(" ", strip=True) and "BPS" in tr.get_text(" ", strip=True):
            pbr_row = tr
            break
    if pbr_row:
        ems = pbr_row.select("td em")
        # ems[0] = PBR, ems[1] = BPS
        if len(ems) >= 2:
            out["bps"] = _to_int(ems[1].get_text(strip=True))

    # 52주 최고 / 최저
    for tr in soup.select("div.aside_invest_info table tr"):
        txt = _clean_ws(tr.get_text(" ", strip=True))
        if txt.startswith("52주최고"):
            ems = tr.select("td em")
            if len(ems) >= 2:
                out["high_52w"] = _to_int(ems[0].get_text(strip=True))
                out["low_52w"]  = _to_int(ems[1].get_text(strip=True))
            break

    # 외국인 소진율
    for tr in soup.select("div.aside_invest_info table tr"):
        txt = _clean_ws(tr.get_text(" ", strip=True))
        if "외국인소진율" in txt:
            m = re.search(r"(\d+(?:\.\d+)?)\s*%", txt)
            if m:
                out["foreign_ownership"] = _to_float(m.group(1))
            break

    # 동일업종 PER
    for tr in soup.select("div.aside_invest_info table tr"):
        txt = _clean_ws(tr.get_text(" ", strip=True))
        if "동일업종 PER" in txt:
            m = re.search(r"(\d+(?:\.\d+)?)\s*배", txt)
            if m:
                out["industry_per"] = _to_float(m.group(1))
            break

    # 투자의견(점수+텍스트) / 공식 컨센서스 목표주가 — 네이버/에프앤가이드 가중평균값
    th_target = soup.find(lambda t: t.name == "th" and "목표주가" in t.get_text())
    if th_target:
        td = th_target.find_next_sibling("td")
        if td:
            span = td.find("span",
                            class_=lambda c: bool(c) and c.startswith("f_"))
            if span:
                em = span.find("em")
                em_text = em.get_text(strip=True) if em else ""
                if em_text:
                    out["consensus_score"] = _to_float(em_text)
                full = _clean_ws(span.get_text(" ", strip=True))
                opinion = full.replace(em_text, "").strip() if em_text else full
                if opinion:
                    out["consensus_opinion"] = opinion
            for em in td.find_all("em"):
                if em.find_parent("span"):
                    continue
                val = em.get_text(strip=True).replace(",", "")
                if val.isdigit():
                    out["consensus_target_official"] = int(val)
                    break

    return out


# ──────────────────────────── wisereport (재무 상세) ────────────────────────────
def _fetch_wisereport(code: str) -> dict[str, Any]:
    """wisereport cF1001 — 영업이익/ROE/부채비율/배당성향 등 (최근 연간)."""
    url = (f"https://navercomp.wisereport.co.kr/v2/company/cF1001.aspx"
           f"?cmp_cd={code}&fin_typ=0&freq_typ=Y")
    r = requests.get(url, headers={"User-Agent": USER_AGENT,
                                    "Referer": "https://finance.naver.com/"},
                     timeout=8)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    tbl = soup.select_one("table#cTB26")
    if not tbl:
        return {}

    # 첫 4 컬럼 = 4개 연도 (가장 최근이 4번째). 첫 컬럼 idx=0, 가장 최근 = 3.
    # 단, 가장 최근 연도가 추정치(예: "2026/12 (E)") 인 경우가 있음 → 실적/추정 구분 어려움.
    # 보수적으로 3번째(idx=2) = 직전 확정 연도 우선, 없으면 4번째(idx=3).
    row_map: dict[str, str | None] = {}
    for tr in tbl.select("tbody tr"):
        th = tr.select_one("th")
        if not th:
            continue
        key = _clean_ws(th.get_text(" ", strip=True))
        tds = [_clean_ws(td.get_text(" ", strip=True)) for td in tr.select("td")]
        if not tds:
            continue
        # 4번째(최근 확정/추정 연도) 우선, 비어 있으면 3번째
        val = tds[3] if len(tds) > 3 and tds[3] else (tds[2] if len(tds) > 2 else "")
        row_map[key] = val

    def get(name: str) -> str | None:
        return row_map.get(name) or None

    out: dict[str, Any] = {
        "revenue":          _to_int(get("매출액") or ""),
        "operating_income": _to_int(get("영업이익") or ""),
        "operating_margin": _to_float(get("영업이익률") or ""),
        "net_margin":       _to_float(get("순이익률") or ""),
        "roe":              _to_float(get("ROE(%)") or ""),
        "debt_ratio":       _to_float(get("부채비율") or ""),
        "dps":              _to_int(get("현금DPS(원)") or ""),
        "dividend_payout":  _to_float(get("현금배당성향(%)") or ""),
    }
    return out


# ──────────────────────────── 컨센서스 / 주주 ────────────────────────────
# 증권사 약칭 → 주주 목록 매칭 시 사용할 후보 토큰 매핑.
# 리포트 제공처 칸에는 보통 약칭이 들어옴. 주주 목록은 풀네임 변형이 다양하므로
# 부분일치 양방향으로 비교한다.
BROKER_ALIASES = {
    "KB":     ["KB", "케이비"],
    "미래에셋": ["미래에셋"],
    "한국투자": ["한국투자", "한투"],
    "한투":   ["한국투자", "한투"],
    "NH":     ["NH", "농협"],
    "신한":   ["신한"],
    "키움":   ["키움"],
    "삼성":   ["삼성증권"],          # '삼성'만으로는 삼성생명/전자와 충돌 → 풀네임 강제
    "하나":   ["하나증권", "하나금융투자"],
    "메리츠": ["메리츠"],
    "유진":   ["유진"],
    "BNK":    ["BNK"],
    "DB":     ["DB금융", "DB증권"],
    "iM":     ["iM증권", "아이엠증권"],
    "현대차": ["현대차"],
    "교보":   ["교보"],
    "대신":   ["대신"],
    "이베스트": ["이베스트"],
    "SK":     ["SK증권"],
    "다올":   ["다올"],
    "유안타": ["유안타"],
    "한화":   ["한화"],
    "하이":   ["하이투자"],
    "IBK":    ["IBK"],
}


def _broker_match_tokens(broker: str) -> list[str]:
    """리포트 제공처명 → 주주 목록 매칭에 쓸 토큰 후보."""
    b = (broker or "").strip()
    if not b:
        return []
    # 매핑 키 우선 사용
    for key, tokens in BROKER_ALIASES.items():
        if key.lower() in b.lower():
            return tokens
    # 매핑에 없으면 원문 + 원문에 '증권' 붙인 변형
    return [b, f"{b}증권"]


def _match_broker_to_shareholder(broker: str,
                                   shareholders: list[dict]) -> dict | None:
    """리포트 제공처 vs 주요주주 부분일치 매칭. 첫 매치 반환."""
    tokens = _broker_match_tokens(broker)
    if not tokens or not shareholders:
        return None
    for sh in shareholders:
        nm = sh.get("name", "")
        for tok in tokens:
            if tok and tok in nm:
                return sh
    return None


def _fetch_consensus_reports(code: str, limit: int = 8) -> list[dict]:
    """wisereport c1080001 — 최근 애널리스트 리포트 목록.
    Returns: [{date, title, analyst, broker, opinion, target}, ...]
    """
    url = (f"https://navercomp.wisereport.co.kr/v2/company/c1080001.aspx"
           f"?cmp_cd={code}")
    r = requests.get(url, headers={"User-Agent": USER_AGENT,
                                    "Referer": "https://finance.naver.com/"},
                     timeout=8)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    # caption 이 '최근리포트' 인 테이블
    target_tbl = None
    for tbl in soup.select("table"):
        cap = tbl.select_one("caption")
        if cap and "최근리포트" in cap.get_text(strip=True):
            target_tbl = tbl
            break
    if target_tbl is None:
        return []

    rows: list[dict] = []
    # 헤더: 일자/제목/작성자/제공처/투자의견/목표가/분량
    # 데이터 행 + 요약(설명) 행이 섞여 있어 td 7개짜리만 채택
    for tr in target_tbl.select("tr"):
        tds = tr.select("td")
        if len(tds) < 7:
            continue
        cells = [_clean_ws(td.get_text(" ", strip=True)) for td in tds]
        date, title, analyst, broker, opinion, target_s, _vol = cells[:7]
        if not date or not broker:
            continue
        rows.append({
            "date":     date,
            "title":    title,
            "analyst":  analyst,
            "broker":   broker,
            "opinion":  opinion or "",
            "target":   _to_int(target_s) if target_s else None,
        })
        if len(rows) >= limit:
            break
    return rows


def _fetch_major_shareholders(code: str) -> list[dict]:
    """wisereport c1010001 — 주요주주 목록.
    Returns: [{name, shares, pct}, ...]
    """
    url = (f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx"
           f"?cmp_cd={code}")
    r = requests.get(url, headers={"User-Agent": USER_AGENT,
                                    "Referer": "https://finance.naver.com/"},
                     timeout=8)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    target_tbl = None
    for tbl in soup.select("table"):
        cap = tbl.select_one("caption")
        if cap and "주요주주" in cap.get_text(strip=True):
            target_tbl = tbl
            break
    if target_tbl is None:
        return []

    rows: list[dict] = []
    for tr in target_tbl.select("tbody tr"):
        cells = [_clean_ws(td.get_text(" ", strip=True))
                  for td in tr.select("th, td")]
        # tbody 행은 [name, shares, pct] 형태가 일반적
        # 단, '주요주주명, 보유주식수(보통주), 보유지분(%) 목록' 헤더 행 등이 섞일 수 있음
        if len(cells) < 3:
            continue
        name, shares_s, pct_s = cells[0], cells[1], cells[2]
        if name in ("주요주주",) or not name:
            continue
        # 같은 이름이 두 번 (a 태그로 두 번) 들어오는 경우 정리
        # 예: '삼성생명보험 외 15인 삼성생명보험 외 15인'
        toks = name.split(" ")
        # 정확히 절반씩 같은 어절이 반복되면 절반만 유지
        half = len(toks) // 2
        if half > 0 and toks[:half] == toks[half:half * 2]:
            name = " ".join(toks[:half])
        shares = _to_int(shares_s)
        pct = _to_float(pct_s)
        if shares is None and pct is None:
            continue
        rows.append({"name": name, "shares": shares, "pct": pct})
    return rows


def fetch_korean_consensus(ticker: str, *, force: bool = False) -> dict[str, Any]:
    """한국 종목 컨센서스(애널리스트 리포트) + 주요주주 dict 반환.

    Returns:
        {
            "reports": [...],            # 매칭된 보유지분 정보 포함
            "shareholders": [...],
            "avg_target": int | None,    # 목표가 평균
            "report_count": int,
        }
    """
    if not (ticker and ticker.isdigit() and len(ticker) == 6):
        return {}

    cache = _load_cache()
    now = time.time()
    cache_key = f"consensus:{ticker}"
    entry = cache.get(cache_key) if isinstance(cache, dict) else None
    if (not force) and entry and isinstance(entry, dict):
        ts = entry.get("ts", 0)
        if now - ts < 24 * 3600 and entry.get("data"):
            return entry["data"]

    reports: list[dict] = []
    shareholders: list[dict] = []
    err: list[str] = []
    try:
        reports = _fetch_consensus_reports(ticker)
    except Exception as e:
        err.append(f"reports: {e}")
    try:
        shareholders = _fetch_major_shareholders(ticker)
    except Exception as e:
        err.append(f"shareholders: {e}")

    # 리포트 ↔ 주주 매칭
    for rp in reports:
        match = _match_broker_to_shareholder(rp.get("broker", ""), shareholders)
        if match:
            rp["holding"] = {
                "name":   match.get("name"),
                "shares": match.get("shares"),
                "pct":    match.get("pct"),
            }

    # 평균 목표가
    targets = [r["target"] for r in reports if r.get("target")]
    avg_target = int(sum(targets) / len(targets)) if targets else None

    out = {
        "reports":      reports,
        "shareholders": shareholders,
        "avg_target":   avg_target,
        "report_count": len(reports),
        "_fetched_at":  datetime.now().isoformat(timespec="seconds"),
    }
    if err:
        out["_errors"] = err

    cache[cache_key] = {"ts": now, "data": out}
    try:
        _save_cache(cache)
    except Exception:
        pass
    return out


# ──────────────────────────── 공개 API ────────────────────────────
def fetch_korean_fundamentals(ticker: str, *, force: bool = False) -> dict[str, Any]:
    """한국 종목 기업가치 지표 dict 반환. 12시간 캐시.

    - 6자리 숫자 ticker 만 지원.
    - 실패 시 빈 dict 또는 부분 dict 반환.
    """
    if not (ticker and ticker.isdigit() and len(ticker) == 6):
        return {}

    cache = _load_cache()
    now = time.time()
    entry = cache.get(ticker) if isinstance(cache, dict) else None
    if (not force) and entry and isinstance(entry, dict):
        ts = entry.get("ts", 0)
        if now - ts < CACHE_TTL_SEC and entry.get("data"):
            return entry["data"]

    out: dict[str, Any] = {"ticker": ticker}
    try:
        out.update(_fetch_main_page(ticker))
    except Exception as e:
        out["_main_error"] = str(e)
    try:
        out.update(_fetch_wisereport(ticker))
    except Exception as e:
        out["_wisereport_error"] = str(e)
    out["_fetched_at"] = datetime.now().isoformat(timespec="seconds")

    cache[ticker] = {"ts": now, "data": out}
    try:
        _save_cache(cache)
    except Exception:
        pass
    return out


def judge_indicator(key: str, value: Any, data: dict) -> str:
    """지표 값을 보고 투자 매력도 판정.

    반환값: "good" (긍정 → 빨강) / "bad" (부정 → 파랑) / "neutral" (정보용 또는 모름)

    한국 증시 컨벤션: 빨강 = 긍정/상승, 파랑 = 부정/하락.
    값이 없으면 (None) 중립.
    """
    if value is None or value == "":
        return "neutral"

    # 정보용 — 항상 중립
    if key in ("market_cap_text", "bps", "revenue",
                "high_52w", "low_52w", "industry_per"):
        return "neutral"

    try:
        v = float(value)
    except (TypeError, ValueError):
        return "neutral"

    if key == "per":
        # 동일업종 대비 우선, 없으면 절대값 기준
        ind = data.get("industry_per")
        if ind:
            try:
                ind_v = float(ind)
                if v < ind_v * 0.8:
                    return "good"
                if v > ind_v * 1.5:
                    return "bad"
            except (TypeError, ValueError):
                pass
        if v <= 0:
            return "bad"   # 적자(음수 PER)
        if v < 10:
            return "good"
        if v > 30:
            return "bad"
        return "neutral"

    if key == "pbr":
        if v <= 0:
            return "bad"
        if v < 1.0:
            return "good"
        if v > 3.0:
            return "bad"
        return "neutral"

    if key == "eps":
        return "good" if v > 0 else "bad"

    if key == "operating_income":
        return "good" if v > 0 else "bad"

    if key == "operating_margin":
        if v >= 15:
            return "good"
        if v < 5:
            return "bad"
        return "neutral"

    if key == "net_margin":
        if v >= 10:
            return "good"
        if v < 3:
            return "bad"
        return "neutral"

    if key == "roe":
        if v >= 15:
            return "good"
        if v < 5:
            return "bad"
        return "neutral"

    if key == "dividend_yield":
        if v >= 4:
            return "good"
        if v < 1:
            return "bad"
        return "neutral"

    if key == "dps":
        return "good" if v > 0 else "bad"

    if key == "dividend_payout":
        if v == 0:
            return "bad"
        if 20 <= v <= 50:
            return "good"
        if v > 80:
            return "bad"
        return "neutral"

    if key == "debt_ratio":
        if v < 100:
            return "good"
        if v > 200:
            return "bad"
        return "neutral"

    if key == "foreign_ownership":
        if v >= 30:
            return "good"
        if v < 5:
            return "bad"
        return "neutral"

    return "neutral"


def _format_eokwon(n: int) -> str:
    """억원 단위 정수를 조/억 결합 표기로 변환.
    예) 971_467 → '97조 1,467억원', 8_000 → '8,000억원',
        -120_000 → '-12조원', 0 → '0억원'.
    """
    sign = "-" if n < 0 else ""
    n = abs(int(n))
    if n >= 10000:
        jo = n // 10000
        ek = n % 10000
        if ek == 0:
            return f"{sign}{jo:,}조원"
        return f"{sign}{jo:,}조 {ek:,}억원"
    return f"{sign}{n:,}억원"


def format_indicator_value(key: str, value: Any) -> str:
    """지표 값 → 표시용 문자열 (단위 포함)."""
    if value is None or value == "":
        return "—"
    unit = INDICATOR_UNITS.get(key, "")

    if key == "market_cap_text":
        return str(value)

    # 억원 단위(매출액/영업이익) — 조 단위 넘어가면 조+억 결합 표기
    if unit == "억원":
        try:
            return _format_eokwon(int(value))
        except (TypeError, ValueError):
            pass

    if isinstance(value, float):
        # 비율류는 소수 둘째자리, 그 외 한 자리
        if unit in ("%", "배"):
            return f"{value:.2f}{unit}"
        return f"{value:,.2f}{unit}"
    if isinstance(value, int):
        return f"{value:,}{unit}" if unit else f"{value:,}"
    return f"{value}{unit}" if unit else str(value)


__all__ = [
    "fetch_korean_fundamentals",
    "fetch_korean_consensus",
    "format_indicator_value",
    "judge_indicator",
    "INDICATOR_SECTIONS",
    "INDICATOR_LABELS",
    "INDICATOR_DESCRIPTIONS",
    "INDICATOR_UNITS",
]
