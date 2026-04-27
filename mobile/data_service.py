"""
모바일 데이터 서비스 — yfinance 제거, requests 로 Yahoo Finance 직접 호출

재사용 가능: 데스크탑 앱의 Toss/Naver 페칭 로직
신규: Yahoo Finance v7 quote API 로 yfinance 대체 (Android 빌드 경량화)
"""
from __future__ import annotations  # Android Python 버전이 3.9 일 수도 있어 PEP 604 회피

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0.0.0 Safari/537.36")

# Yahoo 는 Mac UA 로 finance 홈페이지 방문 시 429 유발 사례 확인 → Windows UA 사용
YAHOO_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36")

# Yahoo Finance 전용 세션 (쿠키만 관리 — crumb 불필요, v8 chart API 사용)
_yahoo_session = None


def _get_yahoo_session():
    """finance.yahoo.com 방문으로 A1/A1S/A3 쿠키 획득.
    v7 quote API 는 2024년 이후 401 차단 → v8 chart API 만 사용.
    """
    global _yahoo_session
    if _yahoo_session is not None:
        return _yahoo_session
    s = requests.Session()
    s.headers.update({
        "User-Agent": YAHOO_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    try:
        s.get("https://finance.yahoo.com", timeout=6, allow_redirects=True)
    except Exception as e:
        print(f"[yahoo-auth] {e}")
    _yahoo_session = s
    return s

# ─── 섹터 메타데이터 (데스크탑과 동일 구조) ────────────────────────────
TIER0 = [
    ("EWY", "EWY", "MSCI Korea — 외국인 투심", "direct"),
    ("KRW=X", "USD/KRW", "원달러 환율 — 수출주·외국인 수급", "inverse"),
    ("^VIX", "VIX", "공포지수 — 20↑ 경계, 30↑ 공포", "inverse"),
    ("^GSPC", "S&P 500", "미국 대형주 — 글로벌 리스크 온/오프", "direct"),
]

SECTOR_INDICATORS = [
    ("반도체", "반도체", [
        ("^SOX", "필라델피아반도체", "미국 반도체 30개사 지수"),
        ("NVDA", "NVIDIA", "AI 칩 대장 — HBM 수요"),
        ("TSM", "TSMC", "파운드리 1위 — 업황 대표"),
        ("SOX=F", "SOX 선물", "반도체 선물 — 정규장 외 흐름 체크"),
    ]),
    ("방산", "방산", [
        ("LMT", "Lockheed Martin", "방산 대장 — 글로벌 방산 경기"),
    ]),
    ("중공업", "중공업/조선", [
        ("CAT", "Caterpillar", "중장비 — 경기 사이클 선행"),
    ]),
    ("리츠", "리츠", [
        ("^TNX", "미국 10Y", "10년물 국채금리 — 리츠·성장주 할인율"),
        ("ZN=F", "미국 10Y 선물", "10Y 선물 — 금리 선행 지표"),
        ("VNQ", "Vanguard REIT", "미국 리츠 ETF — 부동산 투심"),
    ]),
    ("에너지", "에너지", [
        ("CL=F", "WTI 원유", "국제 유가 — 정유·에너지 직결"),
    ]),
    ("자동차", "자동차", [
        ("TSLA", "Tesla", "EV 대장 — 자동차·2차전지 선행"),
    ]),
    ("건설", "건설", [
        ("DHI", "D.R. Horton", "미국 최대 주택건설사"),
    ]),
    ("금융", "금융", [
        ("JPM", "JPMorgan", "미국 금융 대장 — 은행주 투심"),
    ]),
    ("플랫폼", "플랫폼/AI", [
        ("^IXIC", "나스닥", "미국 기술주 전체"),
        ("NQ=F", "나스닥 선물", "나스닥 선물 — 정규장 외 흐름"),
        ("META", "Meta", "플랫폼 대장 — 광고·AI"),
    ]),
    ("바이오", "바이오", [
        ("XBI", "SPDR Biotech", "미국 바이오 ETF"),
    ]),
    ("한국지수", "한국지수", [
        ("^KS200", "KOSPI 200", "코스피 200 지수"),
        ("^KQ11", "KOSDAQ", "코스닥 지수"),
    ]),
]

SECTOR_ETFS = {
    "반도체": ["091160", "091230"],
    "방산": ["449450"],
    "중공업": ["446770"],
    "리츠": ["329200"],
    "에너지": [],
    "자동차": ["091180"],
    "건설": ["117700"],
    "금융": ["091170"],
    "플랫폼": ["365040"],
    "바이오": ["143860"],
    "한국지수": ["122630", "229200"],
}


_yahoo_cache = {"data": {}, "ts": 0}
_YAHOO_CACHE_TTL = 60  # 1분


def fetch_yahoo_batch(symbols: list) -> dict:
    """Yahoo Finance v8 chart API 를 requests 로 직접 호출 (yfinance 미사용).
    v7 quote API 는 2024년 이후 401 차단되어 v8 chart 만 사용.
    Android 빌드 경량화: pandas/numpy 등 무거운 의존성 제거. 1분 TTL 캐시.
    """
    if not symbols:
        return {}
    # 캐시 유효하면 그대로 반환 (전체 심볼 있을 때만)
    if (time.time() - _yahoo_cache["ts"] < _YAHOO_CACHE_TTL
            and _yahoo_cache["data"]):
        cached = _yahoo_cache["data"]
        existing = {s: cached[s] for s in symbols if s in cached}
        if len(existing) == len(symbols):
            return existing

    session = _get_yahoo_session()

    def _chart(sym):
        try:
            r = session.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                params={"range": "2d", "interval": "1d"}, timeout=6)
            if r.status_code != 200:
                return sym, None
            res = ((r.json().get("chart") or {}).get("result") or [])
            if not res:
                return sym, None
            meta = res[0].get("meta") or {}
            price = meta.get("regularMarketPrice")
            prev = meta.get("chartPreviousClose") or meta.get("previousClose")
            if price is not None and prev is not None:
                return sym, {"price": float(price), "prev": float(prev)}
        except Exception as e:
            print(f"[yahoo-chart] {sym}: {e}")
        return sym, None

    out = {}
    with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as pool:
        for sym, q in pool.map(_chart, symbols):
            if q:
                out[sym] = q

    _yahoo_cache["data"].update(out)
    _yahoo_cache["ts"] = time.time()
    return out


def fetch_us_indices() -> list:
    """Tier0 + 섹터 지표 전체를 한 번에 조회해서 dict 로 반환
    Returns: [{"symbol", "name", "tier", "sector", "price", "pct", "note"}, ...]
    """
    all_symbols = set()
    for sym, _, _, _ in TIER0:
        all_symbols.add(sym)
    for sec_key, _, indicators in SECTOR_INDICATORS:
        for item in indicators:
            all_symbols.add(item[0])

    quotes = fetch_yahoo_batch(sorted(all_symbols))

    result = []
    for sym, name, note, direction in TIER0:
        q = quotes.get(sym)
        if not q:
            continue
        pct = (q["price"] - q["prev"]) / q["prev"] * 100 if q["prev"] else 0
        result.append({
            "symbol": sym, "name": name, "note": note,
            "tier": "T0", "sector": "dashboard",
            "price": q["price"], "pct": pct,
        })

    for sec_key, sec_label, indicators in SECTOR_INDICATORS:
        for item in indicators:
            # (sym, name) 구식 2-tuple 과 (sym, name, note) 신식 3-tuple 모두 허용
            sym = item[0]
            name = item[1]
            note = item[2] if len(item) > 2 else ""
            q = quotes.get(sym)
            if not q:
                continue
            pct = (q["price"] - q["prev"]) / q["prev"] * 100 if q["prev"] else 0
            result.append({
                "symbol": sym, "name": name, "note": note,
                "tier": "T1", "sector": sec_key,
                "price": q["price"], "pct": pct,
            })
    return result


def fetch_nxt_supported(ticker: str) -> bool:
    """Toss API 로 NXT(대체거래소) 지원 여부 조회 — 데스크탑과 동일"""
    try:
        r = requests.get(
            f"https://wts-info-api.tossinvest.com/api/v1/stock-detail/ui/A{ticker}/common",
            headers={"User-Agent": USER_AGENT,
                     "Origin": "https://tossinvest.com",
                     "Referer": "https://tossinvest.com/"},
            timeout=5)
        result = r.json().get("result") or {}
        return (bool(result.get("nxtSupported"))
                 and not result.get("nxtTradingSuspended", False))
    except Exception:
        return False


# ─── Toss Invest (한국 종목 실시간 가격) ────────────────────────────
def fetch_toss_prices_batch(tickers: list) -> dict:
    """Toss API로 여러 한국 종목 현재가 + 전일 종가
    Returns: {ticker: {"price": int, "base": int, "volume": int, "trade_date": "YYYY-MM-DD"(KST)}, ...}
    """
    if not tickers:
        return {}
    codes = ",".join(f"A{t}" for t in tickers if t.isdigit() and len(t) == 6)
    if not codes:
        return {}
    url = (f"https://wts-info-api.tossinvest.com/api/v3/stock-prices/details"
           f"?productCodes={codes}")
    try:
        r = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Origin": "https://tossinvest.com",
            "Referer": "https://tossinvest.com/",
        }, timeout=6)
        data = r.json()
        result = {}
        for item in data.get("result", []):
            code = item.get("code", "").lstrip("A")
            if code and item.get("close") is not None:
                # 마지막 체결 시각 → KST 날짜로 변환 (활성 종목 판정)
                trade_date = ""
                raw_dt = item.get("tradeDateTime", "")
                if raw_dt:
                    try:
                        from zoneinfo import ZoneInfo
                        dt_utc = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
                        trade_date = dt_utc.astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
                    except Exception:
                        pass
                result[code] = {
                    "price": int(item["close"]),
                    "base": int(item.get("base") or 0),
                    "volume": int(item.get("volume") or 0),
                    "trade_date": trade_date,
                }
        return result
    except Exception as e:
        print(f"[toss] fail: {e}")
        return {}


# ─── 네이버 금융 스크래핑 ─────────────────────────────────────────
def fetch_stock_warning(ticker: str) -> str:
    """투자경고/주의/위험/단기과열/관리종목/거래정지 (2글자 축약)"""
    try:
        r = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers={"User-Agent": USER_AGENT}, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        texts = " ".join(em.get_text(strip=True)
                         for em in soup.select("em.warning, em.caution, em.danger, em.notice"))
        for full, short in (("투자위험", "위험"), ("관리종목", "관리"),
                            ("거래정지", "정지"), ("투자경고", "경고"),
                            ("단기과열", "과열"), ("투자주의", "주의")):
            if full in texts:
                return short
        return ""
    except Exception:
        return ""


def fetch_target_consensus(ticker: str) -> dict:
    """네이버 애널리스트 목표주가 + 투자의견 — 데스크탑과 동일 스크래핑.
    Returns: {'target': int, 'opinion': str, 'score': float}"""
    try:
        r = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers={"User-Agent": USER_AGENT}, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        th = soup.find(lambda t: t.name == "th" and "목표주가" in t.get_text())
        if not th:
            return {}
        td = th.find_next_sibling("td")
        if not td:
            return {}
        # 투자의견 + 점수 — span.f_up/f_down 내부 em=score, 나머지 text=의견
        score = None
        opinion = ""
        span = td.find("span", class_=lambda c: bool(c) and c.startswith("f_"))
        if span:
            em = span.find("em")
            em_text = em.get_text(strip=True) if em else ""
            try:
                score = float(em_text) if em_text else None
            except ValueError:
                score = None
            full = span.get_text(strip=True)
            opinion = full.replace(em_text, "").strip() if em_text else full
        # 목표주가: td 바로 아래 em (span 내부 제외)
        target = None
        for em in td.find_all("em"):
            if em.find_parent("span"):
                continue
            val = em.get_text(strip=True).replace(",", "")
            if val.isdigit():
                target = int(val)
                break
        if target is None and not opinion:
            return {}
        return {"target": target, "opinion": opinion, "score": score}
    except Exception:
        return {}


def fetch_stock_name(ticker: str) -> str:
    """네이버 금융에서 종목명 조회 (스크래핑)."""
    try:
        r = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers={"User-Agent": USER_AGENT}, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        wrap = soup.find("div", class_="wrap_company")
        if wrap:
            a = wrap.find("a")
            if a:
                return a.get_text(strip=True)
    except Exception:
        pass
    return ""


def fetch_stock_sector(ticker: str) -> str:
    """네이버 업종"""
    try:
        r = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers={"User-Agent": USER_AGENT}, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        link = soup.find("a", href=lambda h: h and "sise_group_detail" in h)
        return link.get_text(strip=True) if link else ""
    except Exception:
        return ""


# ─── 유틸 ─────────────────────────────────────────────────────────
def sign_color(n) -> str:
    try:
        n = float(n)
    except Exception:
        return "#888888"
    if n > 0:
        return "#c0392b"  # 빨강 (상승)
    if n < 0:
        return "#1f4e8f"  # 파랑 (하락)
    return "#888888"


def format_signed(n) -> str:
    try:
        n = int(n)
    except Exception:
        return "0"
    return f"+{n:,}" if n > 0 else f"{n:,}"


# ─── 홀딩스 JSON 관리 ──────────────────────────────────────────────
def load_holdings(path: Path) -> dict:
    if not path.exists():
        return {"holdings": [], "total_invested": 0, "history": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"holdings": [], "total_invested": 0, "history": []}


def save_holdings(path: Path, data: dict):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8")


# ─── 캐시 ──────────────────────────────────────────────────────────
class Cache:
    """TTL 기반 캐시"""
    def __init__(self, ttl_sec: int = 300):
        self.ttl = ttl_sec
        self.store = {}  # {key: (timestamp, value)}

    def get(self, key):
        entry = self.store.get(key)
        if not entry:
            return None
        ts, val = entry
        if time.time() - ts > self.ttl:
            return None
        return val

    def set(self, key, value):
        self.store[key] = (time.time(), value)


# 전역 캐시 인스턴스 (앱 생명주기 동안 유지)
warning_cache = Cache(ttl_sec=6 * 3600)   # 6시간
sector_cache = Cache(ttl_sec=24 * 3600)   # 24시간


def refresh_warning_sector_cache(tickers: list):
    """누락된 티커만 병렬로 조회해 캐시 채움"""
    missing = [t for t in tickers
               if t.isdigit() and len(t) == 6
               and (warning_cache.get(t) is None or sector_cache.get(t) is None)]
    if not missing:
        return

    def _fetch(t):
        return t, fetch_stock_warning(t), fetch_stock_sector(t)

    with ThreadPoolExecutor(max_workers=min(len(missing), 8)) as pool:
        for t, warn, sector in pool.map(_fetch, missing):
            warning_cache.set(t, warn)
            sector_cache.set(t, sector)


# ─── 피크가 (데스크탑과 공유: ../data/peaks.json) ──────────────────
PEAKS_PATH = Path(__file__).resolve().parent.parent / "data" / "peaks.json"


def load_peaks() -> dict:
    """{ticker: peak_price} — 데스크탑이 누적 갱신하는 파일 공유"""
    try:
        if PEAKS_PATH.exists():
            return json.loads(PEAKS_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[peaks] load fail: {e}")
    return {}


# ─── 트레이딩 임계값 (../data/config.json 공유) ─────────────────────
CONFIG_PATH = Path(__file__).resolve().parent.parent / "data" / "config.json"


def load_thresholds() -> dict:
    """stop_loss_alert_pct, trailing_stop_alert_pct 읽기"""
    defaults = {"stop_loss_alert_pct": -9.0, "trailing_stop_alert_pct": -9.0,
                "sell_fee_pct": 0.2}
    try:
        if CONFIG_PATH.exists():
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            for k in defaults:
                if k in data:
                    defaults[k] = data[k]
    except Exception:
        pass
    return defaults


# ─── 컨센서스(목표주가) 캐시 1시간 TTL ─────────────────────────────
consensus_cache = Cache(ttl_sec=3600)


def refresh_consensus_cache(tickers: list):
    missing = [t for t in tickers
               if t.isdigit() and len(t) == 6
               and consensus_cache.get(t) is None]
    if not missing:
        return
    with ThreadPoolExecutor(max_workers=min(len(missing), 8)) as pool:
        for t, data in zip(missing, pool.map(fetch_target_consensus, missing)):
            consensus_cache.set(t, data or {})


# ─── NXT 지원 캐시 (24시간 TTL) ────────────────────────────────────
nxt_support_cache = Cache(ttl_sec=24 * 3600)


def refresh_nxt_cache(tickers: list):
    """누락된 티커의 NXT 지원 여부 병렬 조회"""
    missing = [t for t in tickers
               if t.isdigit() and len(t) == 6
               and nxt_support_cache.get(t) is None]
    if not missing:
        return
    with ThreadPoolExecutor(max_workers=min(len(missing), 8)) as pool:
        for t, supported in zip(missing, pool.map(fetch_nxt_supported, missing)):
            nxt_support_cache.set(t, supported)


# ─── 수급 (개인·외국인·기관·연기금) — 1시간 TTL ─────────────────
investor_cache = Cache(ttl_sec=3600)


def fetch_investor_flow(ticker: str) -> dict | None:
    """토스증권 공개 API — 개인/외국인/기관/연기금 최근 일자 순매수
    데스크톱 portfolio_window.fetch_investor_flow 와 동일 스펙.

    8시(KST) 이전 — body[0] 이 전부 0 이면 body[1] 직전 영업일로 폴백
    8시(KST) 이후 — body[0] 그대로 (오늘 데이터, 0이어도 reset 의미)
    """
    try:
        url = (f"https://wts-info-api.tossinvest.com/api/v1/stock-infos/"
               f"trade/trend/trading-trend?productCode=A{ticker}&size=60")
        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Origin": "https://tossinvest.com",
            "Referer": "https://tossinvest.com/",
        }, timeout=5)
        data = resp.json()
        body = data.get("result", {}).get("body", [])
        if not body:
            return None
        # 8시 이전 폴백 — 새벽엔 어제 데이터, 8시 이후엔 오늘(reset) 사용
        net_keys = (
            "netIndividualsBuyVolume", "netForeignerBuyVolume",
            "netInstitutionBuyVolume", "netPensionFundBuyVolume",
        )
        def _all_zero(b):
            return all((b.get(k) or 0) == 0 for k in net_keys)
        try:
            from zoneinfo import ZoneInfo
            from datetime import datetime as _dt
            kst_hour = _dt.now(ZoneInfo("Asia/Seoul")).hour
        except Exception:
            from datetime import datetime as _dt
            kst_hour = _dt.now().hour
        item = body[0]
        if kst_hour < 8 and _all_zero(item) and len(body) >= 2:
            item = body[1]
        return {
            "date": item.get("baseDate", ""),
            "개인": int(item.get("netIndividualsBuyVolume", 0)),
            "외국인": int(item.get("netForeignerBuyVolume", 0)),
            "기관": int(item.get("netInstitutionBuyVolume", 0)),
            "연기금": int(item.get("netPensionFundBuyVolume", 0)),
            "외국인비율": float(item.get("foreignerRatio") or 0),
        }
    except Exception as e:
        print(f"[WARN] 수급 조회 실패 {ticker}: {e}")
    return None


def refresh_investor_cache(tickers: list):
    """누락된 티커의 수급 병렬 조회"""
    missing = [t for t in tickers
               if t.isdigit() and len(t) == 6
               and investor_cache.get(t) is None]
    if not missing:
        return
    with ThreadPoolExecutor(max_workers=min(len(missing), 8)) as pool:
        for t, flow in zip(missing, pool.map(fetch_investor_flow, missing)):
            investor_cache.set(t, flow or {})
