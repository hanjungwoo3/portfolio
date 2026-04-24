"""
모바일 데이터 서비스 — yfinance 제거, requests 로 Yahoo Finance 직접 호출

재사용 가능: 데스크탑 앱의 Toss/Naver 페칭 로직
신규: Yahoo Finance v7 quote API 로 yfinance 대체 (Android 빌드 경량화)
"""

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

# Yahoo Finance 전용 세션 (cookie/crumb 자동 관리) — yfinance 방식
_yahoo_session = None
_yahoo_crumb = None


def _get_yahoo_session_crumb():
    """yfinance 와 동일: 쿠키 + crumb 토큰 획득"""
    global _yahoo_session, _yahoo_crumb
    if _yahoo_session is not None and _yahoo_crumb is not None:
        return _yahoo_session, _yahoo_crumb
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        # 1) 쿠키 획득 (fc.yahoo.com 404 이어도 Set-Cookie 수신됨)
        s.get("https://fc.yahoo.com", timeout=5, allow_redirects=True)
        # 2) crumb 토큰 획득
        r = s.get("https://query2.finance.yahoo.com/v1/test/getcrumb",
                    timeout=5)
        if r.status_code == 200 and r.text:
            _yahoo_crumb = r.text.strip()
            _yahoo_session = s
            return s, _yahoo_crumb
    except Exception as e:
        print(f"[yahoo-auth] {e}")
    return s, None

# ─── 섹터 메타데이터 (데스크탑과 동일 구조) ────────────────────────────
TIER0 = [
    ("EWY", "EWY", "MSCI Korea — 외국인 투심", "direct"),
    ("KRW=X", "USD/KRW", "원달러 환율", "inverse"),
    ("^VIX", "VIX", "공포지수", "inverse"),
    ("^GSPC", "S&P 500", "미국 대형주", "direct"),
]

SECTOR_INDICATORS = [
    ("반도체", "반도체", [
        ("^SOX", "필라델피아반도체"),
        ("NVDA", "NVIDIA"),
        ("TSM", "TSMC"),
    ]),
    ("방산", "방산", [("LMT", "Lockheed Martin")]),
    ("중공업", "중공업/조선", [("CAT", "Caterpillar")]),
    ("리츠", "리츠", [
        ("^TNX", "미국 10Y"),
        ("VNQ", "Vanguard REIT"),
    ]),
    ("에너지", "에너지", [("CL=F", "WTI 원유")]),
    ("자동차", "자동차", [("TSLA", "Tesla")]),
    ("건설", "건설", [("DHI", "D.R. Horton")]),
    ("금융", "금융", [("JPM", "JPMorgan")]),
    ("플랫폼", "플랫폼/AI", [
        ("^IXIC", "나스닥"),
        ("META", "Meta"),
    ]),
    ("바이오", "바이오", [("XBI", "SPDR Biotech")]),
    ("한국지수", "한국지수", [
        ("^KS200", "KOSPI 200"),
        ("^KQ11", "KOSDAQ"),
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
    """yfinance 로 심볼 batch 조회 + 1분 TTL 캐시"""
    if not symbols:
        return {}
    # 캐시 유효하면 그대로 반환 (전체 심볼 있을 때만)
    if (time.time() - _yahoo_cache["ts"] < _YAHOO_CACHE_TTL
            and _yahoo_cache["data"]):
        cached = _yahoo_cache["data"]
        existing = {s: cached[s] for s in symbols if s in cached}
        if len(existing) == len(symbols):
            return existing

    try:
        import yfinance as yf
    except ImportError:
        print("[yahoo] yfinance not installed")
        return {}

    out = {}
    from concurrent.futures import ThreadPoolExecutor

    def _fetch(sym):
        try:
            t = yf.Ticker(sym)
            info = t.info
            price = info.get("regularMarketPrice")
            prev = info.get("regularMarketPreviousClose")
            if price is not None and prev is not None:
                return sym, {"price": float(price), "prev": float(prev)}
        except Exception as e:
            print(f"[yahoo] {sym}: {e}")
        return sym, None

    with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as pool:
        for sym, data in pool.map(_fetch, symbols):
            if data:
                out[sym] = data

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
        for sym, _ in indicators:
            all_symbols.add(sym)

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
        for sym, name in indicators:
            q = quotes.get(sym)
            if not q:
                continue
            pct = (q["price"] - q["prev"]) / q["prev"] * 100 if q["prev"] else 0
            result.append({
                "symbol": sym, "name": name, "note": "",
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
    Returns: {ticker: {"price": int, "base": int, "volume": int}, ...}
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
                result[code] = {
                    "price": int(item["close"]),
                    "base": int(item.get("base") or 0),
                    "volume": int(item.get("volume") or 0),
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
    데스크톱 portfolio_window.fetch_investor_flow 와 동일 스펙."""
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
        item = body[0]
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
