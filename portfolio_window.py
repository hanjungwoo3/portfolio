#!/usr/bin/env python3
"""
포트폴리오 플로팅 윈도우

- 항상 위에 떠있는 작은 창 (Always on Top)
- 터미널처럼 종목별 표 형식으로 표시
- 5초 간격 자동 갱신
- 손절/익절 임계 도달 시 모달 알림 + 행 강조

Usage:
    python3 portfolio_window.py
"""

import os
import sys
import json
import subprocess
import webbrowser
import tkinter as tk
from tkinter import ttk, font
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def get_stock_info_fast(code: str) -> dict:
    """네이버 금융 현재가 간단 조회 (토스 API 실패 시 폴백)"""
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        price_el = soup.select_one("p.no_today span.blind")
        if price_el:
            return {"current_price": int(price_el.get_text(strip=True).replace(",", ""))}
    except Exception:
        pass
    return {}


def _parse_number(s: str) -> int:
    s = (s or "").strip().replace(",", "").replace("+", "")
    if not s or s in ("-", "—"):
        return 0
    try:
        return int(s)
    except ValueError:
        return 0


def fetch_nxt_supported(ticker: str) -> bool:
    """Toss 공개 API 로 종목의 NXT(대체거래소) 지원 여부 조회.
    True 면 프리/애프터마켓 (08:00-08:50 / 15:30-20:00) 실시간 주문 가능."""
    try:
        r = requests.get(
            f"https://wts-info-api.tossinvest.com/api/v1/stock-detail/ui/A{ticker}/common",
            headers={"User-Agent": USER_AGENT, "Origin": "https://tossinvest.com",
                     "Referer": "https://tossinvest.com/"}, timeout=5,
        )
        result = r.json().get("result") or {}
        return bool(result.get("nxtSupported")) and not result.get("nxtTradingSuspended", False)
    except Exception:
        return False


def fetch_stock_sector(ticker: str) -> str:
    """네이버 금융에서 업종(섹터) 스크래핑 — 동일업종 링크 텍스트 파싱"""
    try:
        r = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers={"User-Agent": USER_AGENT}, timeout=5,
        )
        soup = BeautifulSoup(r.text, "html.parser")
        link = soup.find("a", href=lambda h: h and "sise_group_detail" in h)
        return link.get_text(strip=True) if link else ""
    except Exception:
        return ""


def fetch_stock_warning(ticker: str) -> str:
    """네이버 금융에서 투자경고/주의/위험/단기과열/관리종목/거래정지 스크래핑
    Returns: 2글자 축약 ("주의", "경고", "위험", "과열", "관리", "정지") 또는 ""
    """
    try:
        r = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers={"User-Agent": USER_AGENT}, timeout=5,
        )
        soup = BeautifulSoup(r.text, "html.parser")
        texts = []
        for em in soup.select("em.warning, em.caution, em.danger, em.notice"):
            t = em.get_text(strip=True)
            if t:
                texts.append(t)
        # 우선순위: 위험 > 관리 > 정지 > 경고 > 과열 > 주의
        full = " ".join(texts)
        for full_tag, short in (("투자위험", "위험"), ("관리종목", "관리"),
                                 ("거래정지", "정지"), ("투자경고", "경고"),
                                 ("단기과열", "과열"), ("투자주의", "주의")):
            if full_tag in full:
                return short
        return ""
    except Exception:
        return ""


def fetch_target_consensus(ticker: str) -> dict | None:
    """네이버 금융에서 애널리스트 목표주가 / 투자의견 스크래핑
    Returns: {'target': int, 'opinion': str, 'score': float} or None
    """
    try:
        r = requests.get(
            f"https://finance.naver.com/item/main.naver?code={ticker}",
            headers={"User-Agent": USER_AGENT}, timeout=5,
        )
        soup = BeautifulSoup(r.text, "html.parser")
        th = soup.find(lambda t: t.name == "th" and "목표주가" in t.get_text())
        if not th:
            return None
        td = th.find_next_sibling("td")
        if not td:
            return None
        # 투자의견: span.f_up/f_down 안의 [점수 em] + 텍스트
        score = None
        opinion = ""
        # BeautifulSoup: multi-valued class 를 개별 문자열로 전달하므로 string 비교
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
            return None
        return {"target": target, "opinion": opinion, "score": score}
    except Exception:
        return None


def fetch_investor_flow(ticker: str) -> dict | None:
    """
    토스증권 공개 API에서 최근 일자 개인/외국인/기관 순매수 조회
    토스 앱과 완전히 동일한 숫자 (외국인 = 순수 외국인, 기타외국인은 개인에 포함)

    8시(KST) 이전 — body[0] 이 전부 0 이면 body[1] 직전 영업일로 폴백
    8시(KST) 이후 — body[0] 그대로 (오늘 데이터, 0이어도 reset 의미)
    """
    try:
        url = (
            f"https://wts-info-api.tossinvest.com/api/v1/stock-infos/trade/trend/trading-trend"
            f"?productCode=A{ticker}&size=60"
        )
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
            "netFinancialInvestmentBuyVolume", "netTrustBuyVolume",
            "netPrivateEquityFundBuyVolume", "netInsuranceBuyVolume",
            "netBankBuyVolume", "netOtherFinancialInstitutionsBuyVolume",
            "netOtherCorporationBuyVolume",
        )
        def _all_zero(b):
            return all((b.get(k) or 0) == 0 for k in net_keys)
        try:
            from zoneinfo import ZoneInfo
            kst_hour = datetime.now(ZoneInfo("Asia/Seoul")).hour
        except Exception:
            kst_hour = datetime.now().hour
        item = body[0]
        if kst_hour < 8 and _all_zero(item) and len(body) >= 2:
            item = body[1]
        return {
            "date": item.get("baseDate", ""),
            "개인": int(item.get("netIndividualsBuyVolume", 0)),
            "외국인": int(item.get("netForeignerBuyVolume", 0)),
            "기관": int(item.get("netInstitutionBuyVolume", 0)),
            "연기금": int(item.get("netPensionFundBuyVolume", 0)),
            "금융투자": int(item.get("netFinancialInvestmentBuyVolume", 0)),
            "투신": int(item.get("netTrustBuyVolume", 0)),
            "사모": int(item.get("netPrivateEquityFundBuyVolume", 0)),
            "보험": int(item.get("netInsuranceBuyVolume", 0)),
            "은행": int(item.get("netBankBuyVolume", 0)),
            "기타금융": int(item.get("netOtherFinancialInstitutionsBuyVolume", 0)),
            "기타법인": int(item.get("netOtherCorporationBuyVolume", 0)),
            "외국인비율": float(item.get("foreignerRatio") or 0),
        }
    except Exception as e:
        print(f"[WARN] 수급 조회 실패 {ticker}: {e}")
    return None


def format_signed(n: int) -> str:
    """+ 기호 + 콤마 포맷 (예: +48490 -> '+48,490')"""
    if n == 0:
        return "0"
    return f"{n:+,}"


def sign_color(n: float) -> str:
    """한국 증시 컨벤션 컬러 (0 은 투명감 있는 연회색)"""
    if n > 0:
        return "#c0392b"  # 빨강 (상승/매수)
    if n < 0:
        return "#1f4e8f"  # 파랑 (하락/매도)
    return "#bbb"         # 0 값 — 연회색 (투명감)

DATA_DIR = SCRIPT_DIR / "data"
HOLDINGS_PATH = DATA_DIR / "holdings.json"
CONFIG_PATH = DATA_DIR / "config.json"
PEAKS_PATH = DATA_DIR / "peaks.json"
ALERTS_DIR = DATA_DIR / "alerts"


def load_json(path: Path, default=None):
    if not path.exists():
        return default if default is not None else {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _fetch_price(ticker: str):
    try:
        return get_stock_info_fast(ticker).get("current_price")
    except Exception:
        return None


def fetch_us_indices_with_futures() -> list:
    """
    미국 증시 현물 + 선물 등락률 조회 (yfinance)
    Returns: [{"name", "price", "pct", "fut_pct"}, ...]
    """
    try:
        import yfinance as yf
    except ImportError:
        return []

    # 버핏식 슬림화 — 섹터별 선행지표 (Tier 0 / Tier 1 / Tier 2)
    # (심볼, 이름, 설명, 선물, tier, sector, 방향성)
    # tier: "T0"=핵심 대시보드, "T1"=내 섹터, "T2"=관심 섹터
    pairs = [
        # --- 🔴 Tier 0: 핵심 대시보드 (항상 최상단) ---
        ("EWY", "EWY", "MSCI Korea — 외국인 투심", None, "T0", "dashboard", "direct"),
        ("KRW=X", "USD/KRW", "원달러 환율 — 수출주·외국인 수급", None, "T0", "dashboard", "inverse"),
        ("^VIX", "VIX", "공포지수 — 20↑ 경계, 30↑ 공포", None, "T0", "dashboard", "inverse"),
        ("^GSPC", "S&P 500", "미국 대형주 — 글로벌 리스크 온/오프", "ES=F", "T0", "dashboard", "direct"),

        # --- 💪 Tier 1: 내 섹터 (굵게) ---
        # 🔧 반도체 (기가비스·예스티)
        ("^SOX", "필라델피아반도체", "미국 반도체 30개사 지수", "SOX=F", "T1", "반도체", "direct"),
        ("NVDA", "NVIDIA", "AI 칩 대장 — HBM 수요", None, "T1", "반도체", "direct"),
        ("TSM", "TSMC", "파운드리 1위 — 업황 대표", None, "T1", "반도체", "direct"),
        # 🛡️ 방산 (퍼스텍)
        ("LMT", "Lockheed Martin", "방산 대장 — 글로벌 방산 경기", None, "T1", "방산", "direct"),
        # 🚢 중공업/조선 (HJ중공업)
        ("CAT", "Caterpillar", "중장비 — 경기 사이클 선행", None, "T1", "중공업", "direct"),
        ("HG=F", "구리", "Dr. Copper — 글로벌 경기 선행지표", None, "T1", "중공업", "direct"),
        # 🏢 리츠 (삼성FN리츠)
        ("^TNX", "미국 10Y", "10년물 국채금리 — 리츠·성장주 할인율", "ZN=F", "T1", "리츠", "inverse"),
        ("VNQ", "Vanguard REIT", "미국 리츠 ETF — 부동산 투심", None, "T1", "리츠", "direct"),
        # ⚡ 에너지 (흥구석유)
        ("CL=F", "WTI 원유", "국제 유가 — 정유·에너지 직결", None, "T1", "에너지", "neutral"),
        ("NG=F", "천연가스", "헨리허브 — LNG·발전·난방", None, "T1", "에너지", "neutral"),

        # --- 👀 Tier 2: 관심 섹터 (연하게) ---
        # 🚗 자동차
        ("TSLA", "Tesla", "EV 대장 — 자동차·2차전지 선행", None, "T2", "자동차", "direct"),
        # 🏗️ 건설
        ("DHI", "D.R. Horton", "미국 최대 주택건설사", None, "T2", "건설", "direct"),
        # 💰 금융
        ("JPM", "JPMorgan", "미국 금융 대장 — 은행주 투심", None, "T2", "금융", "direct"),
        # 📱 플랫폼/AI
        ("^IXIC", "나스닥", "미국 기술주 전체", "NQ=F", "T2", "플랫폼", "direct"),
        ("META", "Meta", "플랫폼 대장 — 광고·AI", None, "T2", "플랫폼", "direct"),
        # 🧬 바이오
        ("XBI", "SPDR Biotech", "미국 바이오 ETF", None, "T2", "바이오", "direct"),
        # 🤖 로봇 (로보티즈)
        ("BOTZ", "BOTZ", "Global X 로봇·AI ETF — 로봇 섹터 대표", None, "T2", "로봇", "direct"),
        # 🇰🇷 한국지수 (컨텍스트용)
        ("^N225", "닛케이 225", "일본 대형주 — 아시아 센티멘트", "NKD=F", "T2", "한국지수", "direct"),
        ("^KS200", "KOSPI 200", "코스피 200 지수", None, "T2", "한국지수", "direct"),
        ("^KQ11", "KOSDAQ", "코스닥 지수", None, "T2", "한국지수", "direct"),
    ]
    import math
    def _is_valid(v):
        return v is not None and not math.isnan(float(v)) and float(v) != 0

    def _fast_quote(symbol):
        """현재가 + 기준 종가 조회.

        시장 상태별 분기:
        - PRE  (프리마켓 활성)  → 현재가=preMarketPrice,  기준=regularMarketPrice
        - POST (애프터마켓 활성) → 현재가=postMarketPrice, 기준=regularMarketPrice
        - REGULAR/CLOSED        → 현재가=regularMarketPrice, 기준=regularMarketPreviousClose

        토스/네이버와 동일한 표시 (장 외 시간엔 연장거래 가격 우선).
        info.regularMarketPreviousClose 가 Yahoo 웹과 일치하지만,
        일부 선물(SOX=F 등)에서는 last==prev 로 잘못 반환 → fast_info 폴백.
        """
        tk = yf.Ticker(symbol)
        info_last = info_prev = None
        market_state = ""
        try:
            info = tk.info
            market_state = (info.get("marketState") or "").upper()
            pre_p = info.get("preMarketPrice")
            post_p = info.get("postMarketPrice")
            reg_p = info.get("regularMarketPrice")
            reg_prev = info.get("regularMarketPreviousClose")
            if market_state == "PRE" and _is_valid(pre_p) and _is_valid(reg_p):
                # 프리마켓: 현재가=프리마켓, 기준=직전 정규장 종가
                info_last, info_prev = pre_p, reg_p
            elif market_state == "POST" and _is_valid(post_p) and _is_valid(reg_p):
                # 애프터마켓: 현재가=포스트마켓, 기준=오늘 정규장 종가
                info_last, info_prev = post_p, reg_p
            else:
                # 정규장 또는 완전 휴장
                info_last = reg_p
                info_prev = reg_prev
        except Exception:
            pass
        fi_last = fi_prev = None
        try:
            fi = tk.fast_info
            fi_last = fi.last_price
            fi_prev = fi.regular_market_previous_close
        except Exception:
            pass
        # 1) info 가 유효 AND last != prev (정상값) → info 사용
        if (_is_valid(info_last) and _is_valid(info_prev)
                and float(info_last) != float(info_prev)):
            return float(info_last), float(info_prev)
        # 2) fast_info 폴백 (info 에서 last==prev 로 의심되는 경우)
        if _is_valid(fi_last) and _is_valid(fi_prev):
            return float(fi_last), float(fi_prev)
        # 3) info 에 last==prev 라도 마지막 폴백
        if _is_valid(info_last) and _is_valid(info_prev):
            return float(info_last), float(info_prev)
        # 4) history 폴백
        try:
            h = tk.history(period="5d", auto_adjust=False)
            if not h.empty and len(h) >= 2:
                c1 = float(h["Close"].iloc[-1])
                c2 = float(h["Close"].iloc[-2])
                if _is_valid(c1) and _is_valid(c2):
                    return c1, c2
        except Exception:
            pass
        return None, None

    def _impact(pct, fut_pct, direction):
        """현물 + 선물 + 방향성 → (icon, color, text)
        선물이 있으면 현물/선물 평균으로 최신 방향성 판정
        """
        if direction == "neutral":
            return ("", "#888", "")
        # 신호값: 선물 있으면 현물+선물 평균 (선물이 더 최신)
        if fut_pct is not None:
            signal = (pct + fut_pct) / 2
        else:
            signal = pct
        if abs(signal) < 0.1:
            return ("", "#888", "")
        is_up = signal > 0
        beneficial = (is_up and direction == "direct") or (not is_up and direction == "inverse")
        if beneficial:
            return ("+", "#c0392b", "긍정")
        return ("-", "#1f4e8f", "부정")

    # 병렬 조회 — 각 심볼 ~0.4s 소요, 30개 직렬 시 13s → ThreadPool 로 단축
    symbols: list = []
    seen: set = set()
    for cash, _, _, fut, _, _, _ in pairs:
        for s in (cash, fut):
            if s and s not in seen:
                seen.add(s)
                symbols.append(s)
    quotes: dict = {}
    with ThreadPoolExecutor(max_workers=12) as pool:
        for sym, res in zip(symbols, pool.map(_fast_quote, symbols)):
            quotes[sym] = res

    out = []
    for cash, name, note, fut, tier, sector, direction in pairs:
        close, prev = quotes.get(cash, (None, None))
        if close is None:
            continue
        pct = (close - prev) / prev * 100 if prev else 0

        fut_pct = None
        fut_price = None
        if fut:
            fclose, fprev = quotes.get(fut, (None, None))
            if fclose is not None and fprev:
                fut_pct = (fclose - fprev) / fprev * 100
                fut_price = fclose

        icon, icon_color, impact_text = _impact(pct, fut_pct, direction)
        out.append({
            "symbol": cash,
            "fut_symbol": fut,
            "fut_price": fut_price,
            "name": name, "note": note, "price": close, "pct": pct,
            "fut_pct": fut_pct, "tier": tier, "sector": sector,
            "impact": impact_text, "icon": icon, "icon_color": icon_color,
        })
    return out


# 선물 심볼 → 풀네임 매핑
FUT_FULL_NAME = {
    "ES=F": "S&P 500 선물",
    "NQ=F": "나스닥 선물",
    "SOX=F": "SOX 선물",
    "ZN=F": "미국 10Y 선물",
    "YM=F": "다우 선물",
    "RTY=F": "러셀 선물",
    "NKD=F": "니케이 선물",
}


def resolve_us_indicator_url(symbol: str) -> str:
    """미국 증시 지표 심볼 → 외부 링크 URL
    - Yahoo(`^`)/환율·선물(`=`)/암호화폐(`-`)·DX-Y.NYB 계열: Yahoo Finance
    - KOSPI/KOSDAQ 개별 종목(`.KS`): tossinvest 한국 페이지
    - 그 외 순수 미국 티커(PKX/NVDA/EWY 등): tossinvest 미국 페이지
    """
    if not symbol:
        return "https://finance.yahoo.com/"
    if symbol.endswith(".KS"):
        code = symbol.rsplit(".", 1)[0]
        return f"https://tossinvest.com/stocks/A{code}"
    if symbol.startswith("^") or "=" in symbol or symbol == "DX-Y.NYB" or "-" in symbol:
        return f"https://finance.yahoo.com/quote/{symbol}"
    return f"https://tossinvest.com/stocks/{symbol}"


def kr_session_phase() -> str:
    """KR 시장의 현재 세션 구분 ('REGULAR'|'EXTENDED'|'CLOSED')
    Toss 안내 기준:
    - REGULAR: 09:00-15:20 (모든 종목)
    - EXTENDED:
      · 프리마켓 08:00-08:50 (NXT 지원 종목만)
      · 애프터마켓 15:30-20:00 (NXT 지원 종목만)
    - CLOSED: 그 외 (NXT 휴장 08:50-09:00 및 15:20-15:30 포함)
    """
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Asia/Seoul"))
        if now.weekday() >= 5:
            return "CLOSED"
        hhmm = now.hour * 60 + now.minute
        if 9 * 60 <= hhmm < 15 * 60 + 20:
            return "REGULAR"
        if (8 * 60 <= hhmm < 8 * 60 + 50) or (15 * 60 + 30 <= hhmm < 20 * 60):
            return "EXTENDED"
        return "CLOSED"
    except Exception:
        return "REGULAR"


def is_market_open(market_key: str) -> bool:
    """지수/ETF 용 거래 가능 시간
    - KR: 09:00-15:30 정규장만
    - US: 04:00-20:00 ET (개별 주식 프리+정규+애프터)
    - US_INDEX: 09:30-16:00 ET (지수는 정규장만 값 갱신)
    - JP: 08:30-15:30
    - OTHER: 항상 열림 (환율/선물/암호화폐)
    """
    try:
        from zoneinfo import ZoneInfo
        tz_map = {"KR": "Asia/Seoul", "US": "America/New_York",
                  "US_INDEX": "America/New_York", "JP": "Asia/Tokyo"}
        tz = tz_map.get(market_key)
        if not tz:
            return True
        now = datetime.now(ZoneInfo(tz))
        if now.weekday() >= 5:
            return False
        hhmm = now.hour * 60 + now.minute
        if market_key == "KR":
            return 9 * 60 <= hhmm < 15 * 60 + 30
        if market_key == "JP":
            return 8 * 60 + 30 <= hhmm < 15 * 60 + 30
        if market_key == "US_INDEX":
            return 9 * 60 + 30 <= hhmm < 16 * 60
        return 4 * 60 <= hhmm < 20 * 60  # US stocks extended
    except Exception:
        return True


def market_of_symbol(symbol: str) -> str:
    """yfinance 심볼 → 'KR' | 'US' | 'US_INDEX' | 'JP' | 'OTHER' (24h 자산)
    US 지수 (^GSPC, ^IXIC 등) 는 정규장만 값 갱신하므로 US_INDEX 로 분리.
    """
    if not symbol:
        return "OTHER"
    if symbol.endswith(".KS") or symbol in ("^KS200", "^KQ11"):
        return "KR"
    if symbol == "^N225":
        return "JP"
    # 환율/선물/암호화폐/달러인덱스 = OTHER (24h에 가까움)
    if "=" in symbol or symbol == "DX-Y.NYB" or "-" in symbol:
        return "OTHER"
    # ^ 로 시작하는 나머지 = 미국 지수 (정규장 한정)
    if symbol.startswith("^"):
        return "US_INDEX"
    return "US"


def fetch_peak_since_buy(ticker: str, buy_date: str) -> int | None:
    """
    토스 차트 API로 매수일 이후 최고 고가(high) 반환
    buy_date: "YYYYMMDD" 형식
    """
    if not buy_date or len(buy_date) != 8:
        return None
    try:
        url = (
            f"https://wts-info-api.tossinvest.com/api/v1/c-chart/kr-s/A{ticker}/day:1"
            f"?count=300&useAdjustedRate=true"
        )
        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Origin": "https://tossinvest.com",
            "Referer": "https://tossinvest.com/",
        }, timeout=5)
        candles = resp.json().get("result", {}).get("candles", [])
        # buy_date 이후 candle의 high 최댓값
        buy_iso = f"{buy_date[:4]}-{buy_date[4:6]}-{buy_date[6:8]}"
        peak = 0
        for c in candles:
            dt = c.get("dt", "")[:10]  # "2026-04-20"
            if dt >= buy_iso:
                h = c.get("high", 0)
                if h > peak:
                    peak = h
        return peak if peak > 0 else None
    except Exception as e:
        print(f"[WARN] 피크 조회 실패 {ticker}: {e}")
    return None


def fetch_toss_prices_batch(tickers: list) -> dict:
    """
    토스 공개 API로 여러 종목 현재가 + 거래량 한 번에 조회
    Returns: {ticker: {"price": int, "volume": int, "base": int, "trade_date": "YYYY-MM-DD" KST}, ...}
    """
    if not tickers:
        return {}
    codes = ",".join(f"A{t}" for t in tickers)
    url = (
        f"https://wts-info-api.tossinvest.com/api/v3/stock-prices/details"
        f"?productCodes={codes}"
    )
    try:
        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Origin": "https://tossinvest.com",
            "Referer": "https://tossinvest.com/",
        }, timeout=5)
        data = resp.json()
        result = {}
        for item in data.get("result", []):
            code = item.get("code", "").lstrip("A")
            close = item.get("close")
            volume = item.get("volume", 0)
            base = item.get("base", 0)  # 전일 종가
            # 마지막 체결 시각 — 오늘 거래 여부 + 10분 경과 판정용
            trade_date = ""
            trade_dt_kst = ""  # ISO 문자열 (KST timezone-aware)
            raw_dt = item.get("tradeDateTime", "")
            if raw_dt:
                try:
                    from zoneinfo import ZoneInfo
                    dt_utc = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
                    kst_dt = dt_utc.astimezone(ZoneInfo("Asia/Seoul"))
                    trade_date = kst_dt.strftime("%Y-%m-%d")
                    trade_dt_kst = kst_dt.isoformat()
                except Exception:
                    pass
            if code and close is not None:
                result[code] = {
                    "price": int(close),
                    "volume": int(volume),
                    "base": int(base),
                    "trade_date": trade_date,
                    "trade_dt": trade_dt_kst,
                    "open": int(item.get("open") or 0),
                }
        return result
    except Exception as e:
        print(f"[WARN] 토스 가격 조회 실패: {e}")
        return {}


def format_volume(n: int) -> str:
    """거래량 포맷 (예: 156903 → '15.7만')"""
    if n <= 0:
        return "-"
    if n >= 100_000_000:
        return f"{n / 100_000_000:.2f}억"
    if n >= 10_000:
        return f"{n / 10_000:.1f}만"
    return f"{n:,}"


def show_modal_alert(title: str, message: str):
    safe_title = title.replace('"', '\\"')
    safe_msg = message.replace('"', '\\"').replace("\n", " / ")
    script = (
        f'display alert "{safe_title}" message "{safe_msg}" '
        f'as critical buttons {{"확인"}}'
    )
    try:
        subprocess.Popen(["osascript", "-e", script])
    except Exception as e:
        print(f"[ERROR] 알림 실패: {e}")


class PortfolioWindow:
    # 2-line paired 컬럼 구조: (top_key, bot_key, top_title, bot_title, width, align)
    # - bot_key=None → 하단 공백
    # - 기관 이후 (연기금~개인) 는 상단만 채우는 single-line 컬럼
    COLS_FULL = [
        ("name",          "sector",           "종목",         "섹터",      120, "w"),
        ("volume",        "shares",           "거래량",       "보유량",     70, "e"),
        ("buy",           "cur",              "매수가",       "현재가",     95, "e"),
        ("pnl_combined",  "day_combined",     "손익금액",     "전일대비",  150, "e"),
        ("peak_combined", "target_combined",  "피크가",       "목표주가",  150, "e"),
        ("opinion",       None,               "투자의견",     "",           80, "center"),
        # 이하 single-line (하단 공백)
        ("foreign_combined", None,            "외국인(보유%)", "",         120, "e"),
        ("inst",          None,               "기관",         "",           80, "e"),
        ("pension",       None,               "연기금",       "",           70, "e"),
        ("fin_inv",       None,               "금융투자",     "",           75, "e"),
        ("trust",         None,               "투신",         "",           70, "e"),
        ("pef",           None,               "사모",         "",           70, "e"),
        ("insurance",     None,               "보험",         "",           70, "e"),
        ("bank",          None,               "은행",         "",           70, "e"),
        ("other_fin",     None,               "기타금융",     "",           75, "e"),
        ("other_corp",    None,               "기타법인",     "",           75, "e"),
        ("indiv",         None,               "개인",         "",           80, "e"),
    ]
    COLS_COMPACT = [
        ("name",    "sector",     "종목",    "섹터",    140, "w"),
        ("buy",     "cur",        "매수가",  "현재가",  100, "e"),
        ("volume",  "shares",     "거래량",  "수량",     75, "e"),
    ]

    # 외국인 이전까지는 왼쪽 고정, 이후는 오른쪽 스크롤
    # 왼쪽 고정 영역: pair 의 top_key 로 구분
    FROZEN_KEYS = {"name", "volume", "buy", "target_combined",
                   "pnl_combined", "peak_combined"}

    # 관심 주식/ETF 전용 컬럼 — 전부 single-line (섹터·투자의견 분리 컬럼)
    COLS_WATCH = [
        ("sector",           None, "섹터",          "", 140, "w"),
        ("name",             None, "종목",          "", 150, "w"),
        ("volume",           None, "거래량",        "",  70, "e"),
        ("cur",              None, "현재가",        "",  95, "e"),
        ("day_combined",     None, "전일대비",      "", 120, "e"),
        ("target_combined",  None, "목표주가",      "", 140, "e"),
        ("opinion",          None, "투자의견",      "",  80, "center"),
        ("foreign_combined", None, "외국인(보유%)", "", 150, "e"),
        ("inst",             None, "기관",          "",  80, "e"),
        ("pension",          None, "연기금",        "",  70, "e"),
        ("fin_inv",          None, "금융투자",      "",  75, "e"),
        ("trust",            None, "투신",          "",  70, "e"),
        ("pef",              None, "사모",          "",  70, "e"),
        ("insurance",        None, "보험",          "",  70, "e"),
        ("bank",             None, "은행",          "",  70, "e"),
        ("other_fin",        None, "기타금융",      "",  75, "e"),
        ("other_corp",       None, "기타법인",      "",  75, "e"),
        ("indiv",            None, "개인",          "",  80, "e"),
    ]
    WATCH_FROZEN_KEYS = {"name", "volume", "cur", "day_combined"}

    # 퇴직연금 전용 컬럼 — 모두 single-line
    COLS_PENSION = [
        ("name",         None, "종목",     "",  160, "w"),
        ("shares",       None, "보유량",   "",   65, "e"),
        ("buy",          None, "매수가",   "",   95, "e"),
        ("cur",          None, "현재가",   "",   95, "e"),
        ("pnl_combined", None, "손익금액", "",  180, "e"),
        ("day_combined", None, "전일대비", "",  150, "e"),
        ("peak_combined", None, "피크가",  "",  130, "e"),
    ]
    PENSION_FROZEN_KEYS = {"name", "shares", "buy", "cur",
                            "pnl_combined", "day_combined", "peak_combined"}

    @property
    def COLS(self):
        return self.COLS_COMPACT if self.compact_mode else self.COLS_FULL

    @property
    def COLS_LEFT(self):
        return [c for c in self.COLS if c[0] in self.FROZEN_KEYS]

    @property
    def COLS_RIGHT(self):
        return [c for c in self.COLS if c[0] not in self.FROZEN_KEYS]

    @property
    def COLS_WATCH_LEFT(self):
        return [c for c in self.COLS_WATCH if c[0] in self.WATCH_FROZEN_KEYS]

    @property
    def COLS_WATCH_RIGHT(self):
        return [c for c in self.COLS_WATCH if c[0] not in self.WATCH_FROZEN_KEYS]

    @property
    def COLS_PENSION_LEFT(self):
        return [c for c in self.COLS_PENSION if c[0] in self.PENSION_FROZEN_KEYS]

    @property
    def COLS_PENSION_RIGHT(self):
        return [c for c in self.COLS_PENSION if c[0] not in self.PENSION_FROZEN_KEYS]

    def _cols_for_parent(self, parent):
        """parent frame 기반으로 (cols, []) 반환 — 좌우 분리 없이 전체 컬럼"""
        watch_frames = (
            getattr(self, "watchlist_rows_frame", None),
            getattr(self, "watchlist_header_frame", None),
        )
        pension_frames = (
            getattr(self, "pension_rows_frame", None),
            getattr(self, "pension_header_frame", None),
        )
        if parent in watch_frames:
            return self.COLS_WATCH, []
        if parent in pension_frames:
            return self.COLS_PENSION, []
        return self.COLS, []

    def __init__(self):
        self.cooldowns = {}
        self.investor_cache = {}  # {ticker: {"date": ..., "기관": ..., "외국인": ..., "개인": ...}}
        self.investor_cache_ts = 0  # 마지막 수급 조회 시각
        self.consensus_cache = {}  # {ticker: {"target": int, "opinion": str, "score": float}}
        self.consensus_cache_ts = 0  # 마지막 애널리스트 컨센서스 조회 시각
        self.sector_cache = {}  # {ticker: str} 업종/섹터
        self.sector_cache_ts = 0  # 마지막 섹터 조회 시각
        self.warning_cache = {}  # {ticker: str} 투자경고 축약 ("경고"/"주의"/...)
        self.warning_cache_ts = 0  # 마지막 경고 조회 시각
        self.nxt_cache = {}  # {ticker: bool} NXT 지원 여부
        self.nxt_cache_ts = 0  # 마지막 NXT 조회 시각
        self.us_indices = []  # 미국 증시 데이터
        self.us_indices_ts = 0  # 마지막 조회 시각
        self.compact_mode = False  # 간략히 보기 모드

        self.holdings_data = load_json(HOLDINGS_PATH)
        self.holdings = self.holdings_data.get("holdings", [])
        self.config = load_json(CONFIG_PATH, default={
            "stop_loss_alert_pct": -9.0,
            "trailing_stop_alert_pct": -9.0,
            "polling_interval_seconds": 5,
            "alert_cooldown_minutes": 15,
        })
        self.peaks = load_json(PEAKS_PATH, default={})

        self.root = tk.Tk()
        self.root.title("포트폴리오 모니터")
        self.root.attributes("-topmost", False)  # 기본값: 항상 위 OFF (체크박스로 켜기)
        self.root.attributes("-alpha", 1.0)     # 기본 불투명
        self.root.geometry("1080x480+50+50")

        # Dock 아이콘 숨김 — 메뉴바 런처만 노출 (NSApplicationActivationPolicyAccessory=1)
        try:
            from AppKit import NSApplication, NSApp
            NSApplication.sharedApplication().setActivationPolicy_(1)
            # accessory 정책 적용 후 창이 뒤로 숨지 않도록 명시적으로 앞으로
            try:
                NSApp.activateIgnoringOtherApps_(True)
            except Exception:
                pass
        except Exception:
            pass
        # 창 최초 등장 시 앞으로 (topmost 는 기본 OFF 여도 첫 표시는 앞에)
        self.root.lift()
        self.root.focus_force()

        # 티커 → row_id 매핑 (클릭 시 토스 페이지 이동용)
        self._row_ticker = {}

        # 창 종료 버튼(X) / 런처의 terminate(SIGTERM) → 깔끔하게 프로세스 종료
        self.root.protocol("WM_DELETE_WINDOW", self._on_quit)
        import signal as _sig
        try:
            _sig.signal(_sig.SIGTERM, lambda *_: self._on_quit())
        except Exception:
            pass

        self._build_ui()
        self.interval_ms = self.config.get("polling_interval_seconds", 5) * 1000

        # 앱 시작 시 역사적 피크 + 미국 증시 동기화
        self._sync_historical_peaks()
        self._refresh_us_indices_if_needed()
        self.refresh()
        # 초기 내용에 맞춰 크기 조정
        self.root.after(100, lambda: self._autosize_height(width=1000))

    def _build_ui(self):
        if getattr(self, "_ui_built", False):
            return
        self._ui_built = True

        self.title_label = None
        # 이전 공유 스크롤바 호환용 (더 이상 사용 안함)
        self._canvases = []

        # === 전체 창 스크롤 래퍼 (미국증시 + 3 테이블 모두 포함) ===
        self.outer_scroll_wrap = tk.Frame(self.root, bg="white")
        self.outer_scroll_wrap.pack(fill=tk.BOTH, expand=True)
        self.outer_canvas = tk.Canvas(self.outer_scroll_wrap, bg="white",
                                      highlightthickness=0)
        self.outer_canvas.grid(row=0, column=0, sticky="nsew")
        self.outer_yscroll = ttk.Scrollbar(self.outer_scroll_wrap, orient="vertical",
                                           command=self.outer_canvas.yview)
        self.outer_yscroll.grid(row=0, column=1, sticky="ns")
        self.outer_xscroll = ttk.Scrollbar(self.outer_scroll_wrap, orient="horizontal",
                                           command=self.outer_canvas.xview)
        self.outer_xscroll.grid(row=1, column=0, sticky="ew")
        self.outer_canvas.configure(
            yscrollcommand=self.outer_yscroll.set,
            xscrollcommand=self.outer_xscroll.set,
        )
        self.outer_scroll_wrap.grid_rowconfigure(0, weight=1)
        self.outer_scroll_wrap.grid_columnconfigure(0, weight=1)
        self.outer_inner = tk.Frame(self.outer_canvas, bg="white")
        self._outer_inner_id = self.outer_canvas.create_window(
            (0, 0), window=self.outer_inner, anchor="nw"
        )

        def _on_outer_resize(e=None):
            self.outer_canvas.configure(scrollregion=self.outer_canvas.bbox("all"))
        self.outer_inner.bind("<Configure>", _on_outer_resize)

        def _on_mousewheel(event):
            self.outer_canvas.yview_scroll(int(-1 * event.delta), "units")
        def _on_shift_mousewheel(event):
            self.outer_canvas.xview_scroll(int(-1 * event.delta), "units")
        self.outer_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        self.outer_canvas.bind_all("<Shift-MouseWheel>", _on_shift_mousewheel)

        # 미국 증시 패널 (outer_inner 내부)
        self.us_container = tk.Frame(self.outer_inner, bg="white")
        self.us_container.pack(fill=tk.X, padx=6, pady=(6, 4))
        us_title = tk.Label(
            self.us_container, text="🇺🇸 미국 증시 (실시간 · 선물)",
            font=("SF Pro", 10, "bold"), bg="white", fg="#333",
            anchor="w", padx=4,
        )
        us_title.pack(fill=tk.X)
        self.us_frame = tk.Frame(self.us_container, bg="white")
        self.us_frame.pack(fill=tk.X)

        def _split_table(parent, title_text, show_scrollbar=False):
            """단일 연속 테이블 (좌/우 분리 없음).
            outer_canvas 가 전체 H 스크롤을 담당하므로 내부 분리 불필요."""
            outer = tk.Frame(parent, bg="white")
            outer.pack(fill=tk.X, padx=6, pady=(0, 6))

            tk.Label(
                outer, text=title_text,
                font=("SF Pro", 10, "bold"), bg="white", fg="#333",
                anchor="w", padx=4,
            ).pack(fill=tk.X, anchor="w")

            hdr = tk.Frame(outer, bg="#e8e8e8")
            hdr.pack(fill=tk.X, anchor="w")
            rows = tk.Frame(outer, bg="#e0e0e0")
            rows.pack(fill=tk.X, anchor="w")

            # 호환: (hdr, rows, None, None) 4-튜플 반환 (기존 4 언패킹 코드 유지)
            return hdr, rows, None, None

        # 메인 보유종목 테이블
        self.table_container = tk.Frame(self.outer_inner, bg="white")
        self.table_container.pack(fill=tk.X)
        (self.header_frame, self.rows_frame,
         self.header_frame_r, self.rows_frame_r) = _split_table(
            self.table_container, "💼 보유종목"
        )

        # 관심 주식 테이블
        self.watchlist_container = tk.Frame(self.outer_inner, bg="white")
        self.watchlist_container.pack(fill=tk.X)
        (self.watchlist_header_frame, self.watchlist_rows_frame,
         self.watchlist_header_frame_r, self.watchlist_rows_frame_r) = _split_table(
            self.watchlist_container, "⭐ 관심 주식"
        )

        # 퇴직연금 테이블
        self.pension_container = tk.Frame(self.outer_inner, bg="white")
        self.pension_container.pack(fill=tk.X)
        (self.pension_header_frame, self.pension_rows_frame,
         self.pension_header_frame_r, self.pension_rows_frame_r) = _split_table(
            self.pension_container, "🏦 퇴직연금"
        )

        # 모든 테이블 생성 후 헤더 일괄 렌더
        self._render_header()

        # 하단 컨트롤 (단 1회만 생성)
        bottom = ttk.Frame(self.root, padding=(6, 0, 6, 6))
        bottom.pack(fill=tk.X)

        ttk.Button(bottom, text="새로고침", command=self.refresh).pack(side=tk.LEFT)
        ttk.Button(bottom, text="리로드", command=self.reload_data).pack(side=tk.LEFT, padx=3)

        self.compact_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            bottom, text="간략히",
            variable=self.compact_var,
            command=self._toggle_compact,
        ).pack(side=tk.LEFT, padx=6)

        self.us_visible_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            bottom, text="미국증시",
            variable=self.us_visible_var,
            command=self._toggle_us_panel,
        ).pack(side=tk.LEFT, padx=3)

        self.topmost_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            bottom, text="항상 위",
            variable=self.topmost_var,
            command=self._toggle_topmost,
        ).pack(side=tk.LEFT, padx=6)

        self.maximize_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            bottom, text="가로 최대",
            variable=self.maximize_var,
            command=self._toggle_maximize,
        ).pack(side=tk.LEFT, padx=3)

        # 장마감 종목 fade (💤) on/off — 기본 켜짐
        self.fade_sleeping_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            bottom, text="장마감",
            variable=self.fade_sleeping_var,
            command=self.refresh,
        ).pack(side=tk.LEFT, padx=3)

        ttk.Button(bottom, text="💼 보유 추가",
                   command=self._add_holding).pack(side=tk.LEFT, padx=(6, 2))
        ttk.Button(bottom, text="💼 보유 삭제",
                   command=self._prompt_delete_holding).pack(side=tk.LEFT, padx=2)
        ttk.Button(bottom, text="⭐ 관심 추가",
                   command=self._add_watchlist).pack(side=tk.LEFT, padx=(6, 2))
        ttk.Button(bottom, text="⭐ 관심 삭제",
                   command=self._prompt_delete_watchlist).pack(side=tk.LEFT, padx=2)

        ttk.Button(bottom, text="📤 JSON 내보내기",
                   command=self._export_holdings_json).pack(side=tk.LEFT, padx=(6, 2))
        ttk.Button(bottom, text="📥 JSON 가져오기",
                   command=self._import_holdings_json).pack(side=tk.LEFT, padx=2)

        tk.Label(bottom, text="투명도", font=("SF Pro", 9), foreground="#666").pack(side=tk.LEFT, padx=(8, 2))
        self.alpha_scale = tk.Scale(
            bottom, from_=0.3, to=1.0, resolution=0.05,
            orient=tk.HORIZONTAL, length=100, showvalue=False,
            command=self._on_alpha_change,
        )
        self.alpha_scale.set(1.0)
        self.alpha_scale.pack(side=tk.LEFT)

        self.time_label = ttk.Label(
            bottom, text="-", font=("SF Mono", 9), foreground="#888",
        )
        self.time_label.pack(side=tk.LEFT, padx=8)

        ttk.Button(bottom, text="종료", command=self._on_quit).pack(side=tk.RIGHT)

    def _render_header(self):
        """헤더 재구성 — 각 컬럼이 상단/하단 2줄 타이틀. 모든 bot_key=None 이면 1줄 (짧은 높이)."""
        def render_to(target, cols):
            if target is None:
                return
            for w in target.winfo_children():
                w.destroy()
            single_line = all(c[1] is None for c in cols) if cols else False
            cell_h = 22 if single_line else 40
            for idx, col in enumerate(cols):
                top_key, bot_key, top_title, bot_title, pw, align = col
                anchor = {"w": "w", "e": "e", "center": "center"}[align]
                cell = tk.Frame(target, bg="#e8e8e8", width=pw, height=cell_h)
                cell.grid(row=0, column=idx, sticky="nsew")
                cell.pack_propagate(False)
                is_tall_single = (bot_key is None and not single_line)
                top_lbl = tk.Label(cell, text=top_title,
                                   font=("SF Mono", 9, "bold"), bg="#e8e8e8", fg="#222",
                                   anchor=anchor, padx=3, pady=0,
                                   borderwidth=0)
                if is_tall_single:
                    top_lbl.pack(fill=tk.BOTH, expand=True)
                else:
                    top_lbl.pack(fill=tk.X)
                if not single_line and bot_key is not None:
                    tk.Label(cell, text=bot_title,
                             font=("SF Mono", 9, "bold"), bg="#e8e8e8", fg="#222",
                             anchor=anchor, padx=3, pady=0,
                             borderwidth=0).pack(fill=tk.X)

        render_to(self.header_frame, self.COLS)
        render_to(getattr(self, "pension_header_frame", None), self.COLS_PENSION)
        render_to(getattr(self, "watchlist_header_frame", None), self.COLS_WATCH)

    def _toggle_topmost(self):
        self.root.attributes("-topmost", self.topmost_var.get())

    def _toggle_maximize(self):
        """가로 최대 — 현재 창이 위치한 모니터의 가로 전체로 확장 (위치 이동 없음)"""
        self.root.update_idletasks()
        cur_x = self.root.winfo_x()
        cur_y = self.root.winfo_y()
        h = self.root.winfo_reqheight()

        if self.maximize_var.get():
            # 현재 창이 있는 모니터의 크기 파악 (macOS PyObjC 우선, 실패 시 폴백)
            mon_x, mon_w = self._current_monitor_frame(cur_x, cur_y)
            self.root.geometry(f"{mon_w}x{h}+{mon_x}+{cur_y}")
        else:
            default_w = 400 if self.compact_mode else 1000
            self.root.geometry(f"{default_w}x{h}+{cur_x}+{cur_y}")

    def _current_monitor_frame(self, x, y):
        """창 좌표 (x,y) 가 있는 모니터의 (origin_x, width) 반환"""
        try:
            from AppKit import NSScreen
            screens = NSScreen.screens()
            # y 좌표 반전 (AppKit 은 bottom-left 기준, tkinter 는 top-left)
            main_height = NSScreen.mainScreen().frame().size.height
            for s in screens:
                f = s.frame()
                sx = int(f.origin.x)
                sw = int(f.size.width)
                sy_top = int(main_height - (f.origin.y + f.size.height))
                sh = int(f.size.height)
                if sx <= x < sx + sw and sy_top <= y < sy_top + sh:
                    return sx, sw
            # 일치하는 모니터 없으면 첫 번째
            f = screens[0].frame()
            return int(f.origin.x), int(f.size.width)
        except Exception:
            return 0, self.root.winfo_screenwidth()

    def _on_alpha_change(self, value):
        try:
            self.root.attributes("-alpha", float(value))
        except Exception:
            pass

    def _sync_xview(self, *args):
        """스크롤바 → 모든 캔버스 xview 동기화"""
        for cv in getattr(self, "_canvases", []):
            cv.xview(*args)

    def _on_canvas_scroll(self, first, last):
        """캔버스 xview 변경 → 공유 스크롤바 위치 업데이트"""
        if hasattr(self, "shared_xbar"):
            self.shared_xbar.set(first, last)
        # 다른 캔버스도 같은 위치로 동기화
        for cv in getattr(self, "_canvases", []):
            try:
                if cv.xview() != (float(first), float(last)):
                    cv.xview_moveto(float(first))
            except Exception:
                pass

    def _toggle_compact(self):
        self.compact_mode = self.compact_var.get()
        if self.compact_mode:
            self.us_visible_var.set(False)
            self._toggle_us_panel()
            width = 400
        else:
            width = 1000
        self._render_header()
        self.refresh()
        # 폭은 모드별, 높이는 내용에 맞춰
        self.root.after(20, lambda: self._autosize_height(width=width))

    def _autosize_height(self, width=None):
        """내용 크기에 맞춰 창 크기 자동 조정"""
        self.root.update_idletasks()
        req_h = self.root.winfo_reqheight()
        req_w = width or self.root.winfo_reqwidth()
        self.root.geometry(f"{req_w}x{req_h}")

    def _fetch_stock_name(self, code: str) -> str:
        """종목명 조회 — Toss 실패 시 네이버 폴백"""
        try:
            r = requests.get(
                f"https://wts-info-api.tossinvest.com/api/v2/stock-infos/A{code}/summary",
                headers={"User-Agent": USER_AGENT, "Origin": "https://tossinvest.com",
                         "Referer": "https://tossinvest.com/"}, timeout=5)
            name = (r.json().get("result") or {}).get("name") or ""
            if name:
                return name
        except Exception:
            pass
        try:
            resp = requests.get(f"https://finance.naver.com/item/main.naver?code={code}",
                                headers={"User-Agent": USER_AGENT}, timeout=5)
            soup = BeautifulSoup(resp.text, "html.parser")
            node = soup.select_one("div.wrap_company h2 a")
            if node:
                return node.get_text(strip=True)
        except Exception:
            pass
        return ""

    def _add_holding(self):
        """보유 종목 추가 — 종목코드/수량/평균가/매수일 입력받아 holdings.json 갱신"""
        from tkinter import messagebox
        # 메인 창이 -topmost 면 다이얼로그가 가려지므로 잠시 해제
        _prev_tm = self.root.attributes("-topmost")
        self.root.attributes("-topmost", False)
        dlg = tk.Toplevel(self.root)
        dlg.title("💼 보유 종목 추가")
        dlg.transient(self.root)
        dlg.grab_set()
        dlg.resizable(False, False)
        dlg.attributes("-topmost", True)
        dlg.lift()
        dlg.focus_force()
        # 닫힐 때 원래대로 복원
        def _restore_topmost():
            try:
                self.root.attributes("-topmost", _prev_tm)
            except Exception:
                pass
        dlg.bind("<Destroy>", lambda e: _restore_topmost() if e.widget is dlg else None)
        frm = ttk.Frame(dlg, padding=12)
        frm.grid(sticky="nsew")

        labels = ["종목코드 (6자리)", "수량", "평균 매수가 (원)", "매수일 (YYYYMMDD)"]
        vars_ = [tk.StringVar() for _ in labels]
        vars_[3].set(datetime.now().strftime("%Y%m%d"))
        entries = []
        for i, (lbl, v) in enumerate(zip(labels, vars_)):
            ttk.Label(frm, text=lbl).grid(row=i, column=0, sticky="w", padx=4, pady=4)
            e = ttk.Entry(frm, textvariable=v, width=20)
            e.grid(row=i, column=1, padx=4, pady=4)
            entries.append(e)
        entries[0].focus_set()

        result = {"ok": False}

        def _submit():
            code = vars_[0].get().strip()
            if not (code.isdigit() and len(code) == 6):
                messagebox.showerror("입력 오류", "6자리 숫자 종목코드 필요", parent=dlg)
                return
            try:
                shares = int(vars_[1].get().strip().replace(",", ""))
                avg_price = int(vars_[2].get().strip().replace(",", ""))
            except ValueError:
                messagebox.showerror("입력 오류", "수량/평균가는 숫자만", parent=dlg)
                return
            if shares <= 0 or avg_price <= 0:
                messagebox.showerror("입력 오류", "수량/평균가는 0보다 커야 함", parent=dlg)
                return
            buy_date = vars_[3].get().strip()
            if not (buy_date.isdigit() and len(buy_date) == 8):
                messagebox.showerror("입력 오류", "매수일 YYYYMMDD 8자리", parent=dlg)
                return
            if any(s["ticker"] == code and s.get("account", "") not in ("관심",) for s in self.holdings):
                messagebox.showwarning("중복", "이미 보유/퇴직연금 목록에 있음", parent=dlg)
                return
            result.update({
                "ok": True, "code": code, "shares": shares,
                "avg_price": avg_price, "buy_date": buy_date,
            })
            dlg.destroy()

        btns = ttk.Frame(frm)
        btns.grid(row=len(labels), column=0, columnspan=2, pady=(8, 0))
        ttk.Button(btns, text="추가", command=_submit).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="취소", command=dlg.destroy).pack(side=tk.LEFT, padx=4)
        dlg.bind("<Return>", lambda e: _submit())
        dlg.bind("<Escape>", lambda e: dlg.destroy())
        self.root.wait_window(dlg)

        if not result.get("ok"):
            return
        code = result["code"]
        # 관심/관심ETF 에 있던 종목이면 제거 후 보유로 전환
        self.holdings_data["holdings"] = [
            s for s in self.holdings_data.get("holdings", [])
            if not (s["ticker"] == code and s.get("account") in ("관심", "관심ETF"))
        ]
        name = self._fetch_stock_name(code) or code
        invested = result["shares"] * result["avg_price"]
        new_entry = {
            "ticker": code, "name": name,
            "shares": result["shares"], "avg_price": result["avg_price"],
            "invested": invested, "buy_date": result["buy_date"],
            "market": "KOSPI",
        }
        self.holdings_data.setdefault("holdings", []).append(new_entry)
        # 총 투자금액 / 히스토리 업데이트
        self.holdings_data["total_invested"] = (
            self.holdings_data.get("total_invested", 0) + invested
        )
        self.holdings_data.setdefault("history", []).append({
            "date": result["buy_date"],
            "event": "매수",
            "detail": f"{name} {result['shares']}주 @{result['avg_price']:,}",
        })
        save_json(HOLDINGS_PATH, self.holdings_data)
        self.reload_data()

    def _edit_holding(self, ticker: str):
        """보유/퇴직연금 종목의 수량·평균가·매수일 수정"""
        from tkinter import messagebox
        stock = next((s for s in self.holdings_data.get("holdings", [])
                      if s["ticker"] == ticker
                      and s.get("account") != "관심"), None)
        if not stock:
            return
        _prev_tm = self.root.attributes("-topmost")
        self.root.attributes("-topmost", False)
        dlg = tk.Toplevel(self.root)
        dlg.title(f"✏️ 수정: {stock.get('name', ticker)}")
        dlg.transient(self.root)
        dlg.grab_set()
        dlg.resizable(False, False)
        dlg.attributes("-topmost", True)
        dlg.lift()
        dlg.focus_force()
        def _restore_topmost():
            try:
                self.root.attributes("-topmost", _prev_tm)
            except Exception:
                pass
        dlg.bind("<Destroy>", lambda e: _restore_topmost() if e.widget is dlg else None)
        frm = ttk.Frame(dlg, padding=12)
        frm.grid(sticky="nsew")

        labels = ["수량", "평균 매수가 (원)", "매수일 (YYYYMMDD)"]
        vars_ = [
            tk.StringVar(value=str(stock.get("shares", 0))),
            tk.StringVar(value=str(stock.get("avg_price", 0))),
            tk.StringVar(value=str(stock.get("buy_date", ""))),
        ]
        entries = []
        for i, (lbl, v) in enumerate(zip(labels, vars_)):
            ttk.Label(frm, text=lbl).grid(row=i, column=0, sticky="w", padx=4, pady=4)
            e = ttk.Entry(frm, textvariable=v, width=20)
            e.grid(row=i, column=1, padx=4, pady=4)
            entries.append(e)
        entries[0].focus_set()

        result = {"ok": False}
        old_invested = stock.get("invested", stock.get("shares", 0) * stock.get("avg_price", 0))

        def _submit():
            try:
                shares = int(vars_[0].get().strip().replace(",", ""))
                avg_price = int(vars_[1].get().strip().replace(",", ""))
            except ValueError:
                messagebox.showerror("입력 오류", "수량/평균가는 숫자만", parent=dlg)
                return
            if shares < 0 or avg_price < 0:
                messagebox.showerror("입력 오류", "음수 불가", parent=dlg)
                return
            buy_date = vars_[2].get().strip()
            if buy_date and not (buy_date.isdigit() and len(buy_date) == 8):
                messagebox.showerror("입력 오류", "매수일 YYYYMMDD 8자리", parent=dlg)
                return
            result.update({"ok": True, "shares": shares,
                           "avg_price": avg_price, "buy_date": buy_date})
            dlg.destroy()

        btns = ttk.Frame(frm)
        btns.grid(row=len(labels), column=0, columnspan=2, pady=(8, 0))
        ttk.Button(btns, text="저장", command=_submit).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="취소", command=dlg.destroy).pack(side=tk.LEFT, padx=4)
        dlg.bind("<Return>", lambda e: _submit())
        dlg.bind("<Escape>", lambda e: dlg.destroy())
        self.root.wait_window(dlg)

        if not result.get("ok"):
            return
        new_shares = result["shares"]
        new_avg = result["avg_price"]
        new_invested = new_shares * new_avg
        stock["shares"] = new_shares
        stock["avg_price"] = new_avg
        stock["invested"] = new_invested
        if result["buy_date"]:
            stock["buy_date"] = result["buy_date"]
        # total_invested 보정
        self.holdings_data["total_invested"] = max(
            0, self.holdings_data.get("total_invested", 0) - old_invested + new_invested
        )
        # 변동 내역 간단 히스토리 (추가 매수/일부 매도)
        today = datetime.now().strftime("%Y%m%d")
        delta_shares = new_shares - stock.get("shares_prev", stock.get("shares", new_shares))
        # stock dict 을 미리 저장하지 않았으므로 old invested 로 표현
        diff_invested = new_invested - old_invested
        if diff_invested != 0:
            event = "매수 (추가)" if diff_invested > 0 else "매도/보정"
            self.holdings_data.setdefault("history", []).append({
                "date": today,
                "event": event,
                "detail": f"{stock.get('name', ticker)} 수정: shares {new_shares}, avg @{new_avg:,} (차액 {diff_invested:+,})",
            })
        save_json(HOLDINGS_PATH, self.holdings_data)
        self.reload_data()

    def _prompt_delete_holding(self):
        """일반 보유 종목 목록을 팝업 메뉴로 보여주고 선택 시 삭제"""
        from tkinter import messagebox
        holds = [s for s in self.holdings
                 if not s.get("account") or s.get("account") not in ("관심", "퇴직연금")]
        if not holds:
            messagebox.showinfo("보유 종목", "보유 종목이 없습니다.", parent=self.root)
            return
        menu = tk.Menu(self.root, tearoff=0)
        for s in holds:
            t = s["ticker"]
            menu.add_command(
                label=f"{s.get('name', t)} ({t}) {s.get('shares', 0)}주",
                command=lambda t=t: self._delete_holding(t),
            )
        x = self.root.winfo_pointerx()
        y = self.root.winfo_pointery()
        try:
            menu.tk_popup(x, y)
        finally:
            menu.grab_release()

    def _delete_holding(self, ticker: str):
        """보유 종목 1건 삭제 — total_invested 및 history 반영"""
        from tkinter import messagebox
        stock = next(
            (s for s in self.holdings if s["ticker"] == ticker
             and (not s.get("account") or s["account"] not in ("관심", "퇴직연금"))),
            None)
        if not stock:
            return
        name = stock.get("name", ticker)
        shares = stock.get("shares", 0)
        avg = stock.get("avg_price", 0)
        invested = stock.get("invested", shares * avg)
        if not messagebox.askyesno(
                "보유 종목 삭제",
                f"{name} ({ticker}) {shares}주 @{avg:,} 를 보유 목록에서 제거할까요?\n\n"
                "전량 매도로 기록되고, 관심 주식으로 이동됩니다.",
                parent=self.root):
            return
        self.holdings_data["holdings"] = [
            s for s in self.holdings_data.get("holdings", [])
            if not (s["ticker"] == ticker
                    and (not s.get("account") or s["account"] not in ("관심", "퇴직연금")))
        ]
        self.holdings_data["total_invested"] = max(
            0, self.holdings_data.get("total_invested", 0) - invested)
        self.holdings_data.setdefault("history", []).append({
            "date": datetime.now().strftime("%Y%m%d"),
            "event": "매도",
            "detail": f"{name} {shares}주 전량 매도 (avg @{avg:,})",
        })
        # 관심 주식으로 자동 이동 — 이미 관심/관심ETF 에 있으면 스킵
        already_watch = any(
            s["ticker"] == ticker and s.get("account") in ("관심", "관심ETF")
            for s in self.holdings_data["holdings"])
        if not already_watch:
            self.holdings_data["holdings"].append({
                "ticker": ticker,
                "name": name,
                "shares": 0,
                "avg_price": 0,
                "invested": 0,
                "buy_date": "",
                "market": stock.get("market", "KOSPI"),
                "account": "관심",
            })
        save_json(HOLDINGS_PATH, self.holdings_data)
        self.reload_data()

    def _add_watchlist(self):
        """관심 주식 추가 — 티커 입력"""
        from tkinter import messagebox
        # 메인 창 topmost 일시 해제 (다이얼로그 앞에 뜨게)
        prev_topmost = self.root.attributes("-topmost")
        self.root.attributes("-topmost", False)
        dlg = tk.Toplevel(self.root)
        dlg.title("⭐ 관심 주식 추가")
        dlg.transient(self.root)
        dlg.grab_set()
        dlg.resizable(False, False)
        dlg.attributes("-topmost", True)
        dlg.lift()
        dlg.focus_force()

        def _restore_topmost():
            try:
                self.root.attributes("-topmost", prev_topmost)
            except Exception:
                pass
        dlg.bind("<Destroy>", lambda e: _restore_topmost() if e.widget is dlg else None)

        frm = ttk.Frame(dlg, padding=12)
        frm.grid(sticky="nsew")
        # 티커 입력
        ttk.Label(frm, text="종목코드 (6자리)").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        code_var = tk.StringVar()
        e = ttk.Entry(frm, textvariable=code_var, width=20)
        e.grid(row=0, column=1, padx=4, pady=4)
        e.focus_set()

        result = {"ok": False}

        def _submit():
            code = code_var.get().strip()
            if not (code.isdigit() and len(code) == 6):
                messagebox.showerror("입력 오류", "6자리 숫자 종목코드 필요", parent=dlg)
                return
            if any(s["ticker"] == code for s in self.holdings):
                messagebox.showwarning("중복", "이미 목록에 있는 종목", parent=dlg)
                return
            result["ok"] = True
            result["code"] = code
            dlg.destroy()

        btns = ttk.Frame(frm)
        btns.grid(row=1, column=0, columnspan=2, pady=(8, 0))
        ttk.Button(btns, text="추가", command=_submit).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="취소", command=dlg.destroy).pack(side=tk.LEFT, padx=4)
        dlg.bind("<Return>", lambda ev: _submit())
        dlg.bind("<Escape>", lambda ev: dlg.destroy())
        self.root.wait_window(dlg)

        if not result.get("ok"):
            return
        code = result["code"]
        account_type = "관심"
        # 종목명 조회 (Toss summary → 실패 시 Naver)
        name = ""
        try:
            r = requests.get(
                f"https://wts-info-api.tossinvest.com/api/v2/stock-infos/A{code}/summary",
                headers={"User-Agent": USER_AGENT, "Origin": "https://tossinvest.com",
                         "Referer": "https://tossinvest.com/"}, timeout=5)
            name = r.json().get("result", {}).get("name", "") or ""
        except Exception:
            pass
        if not name:
            try:
                url = f"https://finance.naver.com/item/main.naver?code={code}"
                resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=5)
                soup = BeautifulSoup(resp.text, "html.parser")
                node = soup.select_one("div.wrap_company h2 a")
                if node:
                    name = node.get_text(strip=True)
            except Exception:
                pass
        if not name:
            name = code
        self.holdings_data.setdefault("holdings", []).append({
            "ticker": code, "name": name, "shares": 0, "avg_price": 0,
            "invested": 0, "buy_date": "", "market": "KOSPI", "account": account_type,
        })
        save_json(HOLDINGS_PATH, self.holdings_data)
        self.reload_data()

    def _prompt_delete_watchlist(self):
        """관심 주식/ETF 목록을 팝업 메뉴로 보여주고 선택 시 삭제"""
        from tkinter import messagebox
        watches = [s for s in self.holdings
                   if s.get("account") in ("관심", "관심ETF")]
        if not watches:
            messagebox.showinfo("관심 목록", "관심 주식/ETF 가 없습니다.", parent=self.root)
            return
        menu = tk.Menu(self.root, tearoff=0)
        for s in watches:
            t = s["ticker"]
            icon = "📊" if s.get("account") == "관심ETF" else "⭐"
            menu.add_command(
                label=f"{icon} {s.get('name', t)} ({t})",
                command=lambda t=t: self._delete_watchlist(t),
            )
        x = self.root.winfo_pointerx()
        y = self.root.winfo_pointery()
        try:
            menu.tk_popup(x, y)
        finally:
            menu.grab_release()

    def _delete_watchlist(self, ticker: str):
        """관심 주식/ETF 1건 삭제"""
        from tkinter import messagebox
        stock = next((s for s in self.holdings if s["ticker"] == ticker), None)
        if not stock or stock.get("account") not in ("관심", "관심ETF"):
            return
        if not messagebox.askyesno(
                "관심 삭제",
                f"{stock.get('name', ticker)} ({ticker}) 를 목록에서 제거할까요?",
                parent=self.root):
            return
        self.holdings_data["holdings"] = [
            s for s in self.holdings_data.get("holdings", [])
            if not (s["ticker"] == ticker
                    and s.get("account") in ("관심", "관심ETF"))
        ]
        save_json(HOLDINGS_PATH, self.holdings_data)
        self.reload_data()

    # ─── JSON 내보내기/가져오기 (모바일과 공유) ────────────────
    @staticmethod
    def _is_syncable(stock: dict) -> bool:
        """sync 대상: 일반 보유 + 관심 주식 (관심ETF / 퇴직연금 제외)."""
        acc = stock.get("account") or ""
        return acc in ("", "관심")

    def _export_holdings_json(self):
        """보유 + 관심 주식만 팝업으로 표시 + 복사/파일저장 (ETF/퇴직연금 제외)."""
        import json as _json
        from tkinter import filedialog, messagebox
        filtered = [s for s in self.holdings_data.get("holdings", [])
                    if self._is_syncable(s)]
        export_data = {"holdings": filtered}
        text = _json.dumps(export_data, ensure_ascii=False, indent=2)

        dlg = tk.Toplevel(self.root)
        dlg.title("📤 JSON 내보내기")
        dlg.geometry("700x500")

        txt = tk.Text(dlg, wrap="none", font=("SF Mono", 11))
        txt.insert("1.0", text)
        txt.config(state="disabled")
        txt.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        btn_row = ttk.Frame(dlg)
        btn_row.pack(fill=tk.X, padx=6, pady=(0, 6))

        def _copy():
            self.root.clipboard_clear()
            self.root.clipboard_append(text)
            messagebox.showinfo("복사", "클립보드에 복사됨", parent=dlg)

        def _save_file():
            from pathlib import Path as _P
            default = f"portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            initial = str(_P.home() / "Downloads")
            fpath = filedialog.asksaveasfilename(
                parent=dlg, initialdir=initial,
                initialfile=default, defaultextension=".json",
                filetypes=[("JSON", "*.json")])
            if fpath:
                _P(fpath).write_text(text, encoding="utf-8")
                messagebox.showinfo("저장", f"{fpath} 에 저장됨", parent=dlg)

        ttk.Button(btn_row, text="닫기", command=dlg.destroy).pack(side=tk.RIGHT, padx=2)
        ttk.Button(btn_row, text="📋 복사", command=_copy).pack(side=tk.RIGHT, padx=2)
        ttk.Button(btn_row, text="💾 파일 저장", command=_save_file).pack(side=tk.RIGHT, padx=2)

    def _import_holdings_json(self):
        """파일 선택 or paste → 검증 → 교체."""
        import json as _json
        from tkinter import filedialog, messagebox

        dlg = tk.Toplevel(self.root)
        dlg.title("📥 JSON 가져오기")
        dlg.geometry("700x500")

        ttk.Label(dlg, text="전체 holdings.json 내용을 붙여넣거나 파일을 선택하세요",
                   foreground="#666").pack(anchor="w", padx=6, pady=(6, 2))

        txt = tk.Text(dlg, wrap="none", font=("SF Mono", 11))
        txt.pack(fill=tk.BOTH, expand=True, padx=6, pady=(0, 6))

        def _pick_file():
            from pathlib import Path as _P
            fpath = filedialog.askopenfilename(
                parent=dlg, initialdir=str(_P.home() / "Downloads"),
                filetypes=[("JSON", "*.json"), ("All", "*.*")])
            if fpath:
                try:
                    content = _P(fpath).read_text(encoding="utf-8")
                    txt.delete("1.0", "end")
                    txt.insert("1.0", content)
                except Exception as e:
                    messagebox.showerror("읽기 실패", str(e), parent=dlg)

        def _paste():
            try:
                content = self.root.clipboard_get()
                txt.delete("1.0", "end")
                txt.insert("1.0", content)
            except Exception:
                messagebox.showwarning("클립보드", "클립보드에 내용 없음", parent=dlg)

        def _apply():
            raw = txt.get("1.0", "end").strip()
            if not raw:
                messagebox.showwarning("입력 없음", "JSON 내용을 입력하세요", parent=dlg)
                return
            try:
                data = _json.loads(raw)
            except _json.JSONDecodeError as e:
                messagebox.showerror("JSON 파싱 실패", str(e), parent=dlg)
                return
            if not isinstance(data, dict) or "holdings" not in data:
                messagebox.showerror("형식 오류", "'holdings' 키가 없습니다", parent=dlg)
                return
            if not isinstance(data["holdings"], list):
                messagebox.showerror("형식 오류", "'holdings' 는 배열이어야 함", parent=dlg)
                return
            for i, s in enumerate(data["holdings"]):
                if not isinstance(s, dict) or not s.get("ticker"):
                    messagebox.showerror("형식 오류",
                                          f"{i}번 항목에 ticker 누락", parent=dlg)
                    return
            n_old = len(self.holdings_data.get("holdings", []))
            n_new = len(data["holdings"])
            if not messagebox.askyesno(
                    "확인",
                    f"현재 {n_old}개 종목이 삭제되고 {n_new}개로 교체됩니다.\n진행할까요?",
                    parent=dlg):
                return
            # 기존 ETF/퇴직연금 보존 + 유입은 보유/관심 주식만 적용
            preserved = [s for s in self.holdings_data.get("holdings", [])
                         if not self._is_syncable(s)]
            incoming = [s for s in data["holdings"]
                        if self._is_syncable(s)]
            self.holdings_data["holdings"] = preserved + incoming
            save_json(HOLDINGS_PATH, self.holdings_data)
            dlg.destroy()
            self.reload_data()
            messagebox.showinfo(
                "완료",
                f"{len(incoming)}개 적용됨 (ETF/퇴직연금 {len(preserved)}개 보존)",
                parent=self.root)

        btn_row = ttk.Frame(dlg)
        btn_row.pack(fill=tk.X, padx=6, pady=(0, 6))
        ttk.Button(btn_row, text="취소", command=dlg.destroy).pack(side=tk.RIGHT, padx=2)
        ttk.Button(btn_row, text="적용", command=_apply).pack(side=tk.RIGHT, padx=2)
        ttk.Button(btn_row, text="📋 붙여넣기", command=_paste).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_row, text="📁 파일 선택", command=_pick_file).pack(side=tk.LEFT, padx=2)

    def _on_quit(self):
        """창 X 버튼 / 종료 버튼 / SIGTERM → after 예약 취소 + 프로세스 종료"""
        for attr in ("_refresh_job", "_countdown_job"):
            job = getattr(self, attr, None)
            if job:
                try:
                    self.root.after_cancel(job)
                except Exception:
                    pass
        try:
            self.root.destroy()
        except Exception:
            pass
        os._exit(0)

    def _toggle_us_panel(self):
        if self.us_visible_var.get():
            self.us_container.pack(fill=tk.X, padx=6, pady=(6, 4),
                                   before=self.table_container)
            self._render_us_indices()
        else:
            self.us_container.pack_forget()
        # 창 크기 자동 조정 제거 — 사용자가 설정한 창 크기 유지

    def _on_row_click(self, ticker: str):
        pass

    def _on_row_right_click(self, ticker: str, event=None):
        """우클릭 컨텍스트 메뉴 — 계정별 수정/제거"""
        stock = next((s for s in self.holdings if s["ticker"] == ticker), None)
        if not stock:
            return
        account = stock.get("account", "")
        name = stock.get("name", ticker)
        menu = tk.Menu(self.root, tearoff=0)
        if account == "관심":
            menu.add_command(
                label=f"⭐ 관심에서 제거: {name}",
                command=lambda: self._delete_watchlist(ticker),
            )
        elif account == "관심ETF":
            menu.add_command(
                label=f"📊 관심 ETF 에서 제거: {name}",
                command=lambda: self._delete_watchlist(ticker),
            )
        elif account == "퇴직연금":
            menu.add_command(
                label=f"🏦 수량/평균가 수정: {name}",
                command=lambda: self._edit_holding(ticker),
            )
        else:
            menu.add_command(
                label=f"💼 수량/평균가 수정: {name}",
                command=lambda: self._edit_holding(ticker),
            )
            menu.add_separator()
            menu.add_command(
                label=f"💼 보유에서 제거 (전량 매도): {name}",
                command=lambda: self._delete_holding(ticker),
            )
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def _on_row_double_click(self, ticker: str):
        if ticker.startswith("__"):
            return  # 합계 등 특수 행
        url = f"https://tossinvest.com/stocks/A{ticker}"
        self._open_in_existing_tab(url)

    def _open_in_existing_tab(self, url: str):
        """Chrome/Safari에서 URL 의 host 와 동일한 탭이 있으면 그 탭 URL 교체, 없으면 새로"""
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""
        # tossinvest 한국(/stocks/A...) 과 미국(/stocks/XXX)이 같은 탭을 쓰도록 host 만 매칭
        chrome_script = f'''
tell application "Google Chrome"
    set foundTab to false
    repeat with w in windows
        set tabIdx to 0
        repeat with t in tabs of w
            set tabIdx to tabIdx + 1
            if URL of t contains "{host}" then
                set URL of t to "{url}"
                set active tab index of w to tabIdx
                set index of w to 1
                activate
                set foundTab to true
                return "OK"
            end if
        end repeat
    end repeat
    if not foundTab then
        open location "{url}"
        activate
    end if
end tell
'''
        safari_script = f'''
tell application "Safari"
    set foundTab to false
    repeat with w in windows
        set tabIdx to 0
        repeat with t in tabs of w
            set tabIdx to tabIdx + 1
            if URL of t contains "{host}" then
                set URL of t to "{url}"
                set current tab of w to t
                set index of w to 1
                activate
                set foundTab to true
                return "OK"
            end if
        end repeat
    end repeat
    if not foundTab then
        open location "{url}"
        activate
    end if
end tell
'''
        try:
            r = subprocess.run(["osascript", "-e", chrome_script],
                               capture_output=True, timeout=5)
            if r.returncode == 0:
                return
        except Exception:
            pass
        try:
            subprocess.run(["osascript", "-e", safari_script],
                           capture_output=True, timeout=5)
        except Exception:
            webbrowser.open(url)  # 최후 폴백

    def reload_data(self):
        self.holdings_data = load_json(HOLDINGS_PATH)
        self.holdings = self.holdings_data.get("holdings", [])
        self.peaks = load_json(PEAKS_PATH, default={})
        # 리로드 시 수급·컨센서스·섹터·US 캐시도 강제 초기화
        self.investor_cache.clear()
        self.investor_cache_ts = 0
        self.consensus_cache.clear()
        self.consensus_cache_ts = 0
        self.sector_cache.clear()
        self.sector_cache_ts = 0
        self.nxt_cache.clear()
        self.nxt_cache_ts = 0
        self.us_indices = []
        self.us_indices_ts = 0
        self._sync_historical_peaks()
        self.refresh()

    def _sync_historical_peaks(self):
        """매수일 이후 실제 최고가를 토스 차트 API로 가져와 피크 초기화/갱신"""
        for stock in self.holdings:
            ticker = stock["ticker"]
            buy_date = stock.get("buy_date", "")
            historical_peak = fetch_peak_since_buy(ticker, buy_date)
            if historical_peak:
                prev = self.peaks.get(ticker, 0)
                # 기록된 피크보다 역사적 최고가 높으면 갱신
                self.peaks[ticker] = max(prev, historical_peak)
        save_json(PEAKS_PATH, self.peaks)

    def _render_us_indices(self):
        """선행지표 패널 렌더 — Tier 0 스트립 + Tier 1/2 섹터 블록 그리드"""
        # 기존 us_frame 을 유지한 채 staging frame 에 전체 빌드 후 교체 (깜빡임 방지)
        old_us_frame = self.us_frame
        staging = tk.Frame(old_us_frame.master, bg=old_us_frame.cget("bg"))
        self.us_frame = staging
        if not self.us_indices:
            tk.Label(staging, text="로딩 중...", bg="white", fg="#999",
                     font=("SF Pro", 9)).pack(anchor="w", padx=4)
            try:
                info = old_us_frame.pack_info()
                kwargs = {(k if k != "in" else "in_"): v for k, v in info.items()}
                staging.pack(**kwargs)
            except Exception:
                staging.pack(fill=tk.X)
            old_us_frame.destroy()
            return

        # 섹터 메타데이터: (섹터키, 이모지+표시명)
        ALL_SECTORS = [
            ("반도체",   "🔧 반도체"),
            ("방산",     "🛡️ 방산"),
            ("중공업",   "🚢 중공업/조선"),
            ("리츠",     "🏢 리츠"),
            ("에너지",   "⚡ 에너지"),
            ("자동차",   "🚗 자동차"),
            ("건설",     "🏗️ 건설"),
            ("금융",     "💰 금융"),
            ("플랫폼",   "📱 플랫폼/AI"),
            ("바이오",   "🧬 바이오"),
            ("한국지수", "🇰🇷 한국지수"),
        ]
        # 섹터 → 관심 ETF (holdings 에서 account=="관심ETF")
        etfs_by_sector = {
            "반도체":   ["091160", "091230"],           # KODEX 반도체, TIGER 반도체
            "방산":     ["449450"],                      # TIGER 방산
            "중공업":   ["446770"],                      # KODEX 조선해양
            "리츠":     ["329200"],                      # TIGER 리츠부동산인프라
            "에너지":   [],
            "자동차":   ["091180"],                      # KODEX 자동차
            "건설":     ["117700"],                      # KODEX 건설
            "금융":     ["091170"],                      # KODEX 은행
            "플랫폼":   ["365040"],                      # TIGER AI코리아그로스
            "바이오":   ["143860"],                      # TIGER 바이오
            "한국지수": ["122630", "229200"],            # KODEX 레버리지, KODEX 코스닥150
        }
        # 섹터별 지표 index
        by_sector = {}
        for x in self.us_indices:
            by_sector.setdefault(x.get("sector"), []).append(x)

        # 🔴 Tier 0: 상단 가로 스트립
        tier0 = [x for x in self.us_indices if x.get("tier") == "T0"]
        self._render_tier0_strip(self.us_frame, tier0)

        # 섹터 블록 그리드 (Tier 1/2 구분 없이 하나로)
        sector_wrap = tk.Frame(self.us_frame, bg="white")
        sector_wrap.pack(fill=tk.X, pady=(8, 0))
        self._render_sector_blocks(sector_wrap, ALL_SECTORS, by_sector,
                                    etfs_by_sector, faded=False)

        # staging → 실제 위치로 스왑
        try:
            info = old_us_frame.pack_info()
            kwargs = {(k if k != "in" else "in_"): v for k, v in info.items()}
            staging.pack(**kwargs)
        except Exception:
            staging.pack(fill=tk.X)
        old_us_frame.destroy()

    def _render_tier0_strip(self, parent, indices: list):
        """Tier 0 — 상단 가로 스트립 (각 셀 content 크기, 왼쪽 정렬)"""
        strip = tk.Frame(parent, bg="#2c3e50")
        strip.pack(fill=tk.X, pady=(0, 6))
        for col_idx, idx in enumerate(indices):
            cell = tk.Frame(strip, bg="#2c3e50")
            cell.pack(side=tk.LEFT, padx=(0, 16))

            pct = idx.get("pct", 0)
            pct_color = "#ff6b6b" if pct > 0 else ("#5dade2" if pct < 0 else "#bbb")
            symbol = idx.get("symbol", "")
            url = resolve_us_indicator_url(symbol)
            closed = not is_market_open(market_of_symbol(symbol))
            name_disp = f"💤 {idx['name']}" if closed else idx["name"]
            note = idx.get("note", "")

            # 2줄 레이아웃: [name · price · pct] / [note]
            line1 = tk.Frame(cell, bg="#2c3e50")
            line1.pack(fill=tk.X, padx=8, pady=(6, 0))
            labels = []
            lbl_name = tk.Label(line1, text=name_disp, font=("SF Pro", 11, "bold"),
                                bg="#2c3e50", fg="white", cursor="pointinghand")
            lbl_name.pack(side=tk.LEFT)
            labels.append(lbl_name)
            lbl_price = tk.Label(line1, text=f"{idx['price']:,.2f}",
                                 font=("SF Mono", 10),
                                 bg="#2c3e50", fg="#ecf0f1", cursor="pointinghand")
            lbl_price.pack(side=tk.LEFT, padx=(10, 0))
            labels.append(lbl_price)
            lbl_pct = tk.Label(line1, text=f"{pct:+.2f}%",
                               font=("SF Mono", 11, "bold"),
                               bg="#2c3e50", fg=pct_color, cursor="pointinghand")
            lbl_pct.pack(side=tk.LEFT, padx=(6, 0))
            labels.append(lbl_pct)

            lbl_note = None
            if note:
                lbl_note = tk.Label(cell, text=note, font=("SF Pro", 8),
                                    bg="#2c3e50", fg="#95a5a6", anchor="w",
                                    cursor="pointinghand")
                lbl_note.pack(fill=tk.X, padx=8, pady=(0, 6))
                labels.append(lbl_note)

            for w in [cell, line1, *labels]:
                w.bind("<Button-1>", lambda e, u=url: self._open_in_existing_tab(u))

    def _render_sector_blocks(self, parent, sectors: list, by_sector: dict,
                              etfs_by_sector: dict, faded: bool):
        """섹터별 한 행 레이아웃 — [섹터명 | 선행지표들 inline | 관심ETF들 inline]"""
        hdr_bg = "#eaeaea" if faded else "#d5dbe0"
        hdr_fg = "#888" if faded else "#2c3e50"

        # 관심 ETF 가격 일괄 조회 (Toss)
        all_etf_tickers = [t for lst in etfs_by_sector.values() for t in lst]
        toss = fetch_toss_prices_batch(all_etf_tickers) if all_etf_tickers else {}
        holdings_by_ticker = {s["ticker"]: s for s in self.holdings}

        # 4 컬럼: 섹터명 | 현물 | 선물 | ETF
        parent.grid_columnconfigure(0, weight=0, minsize=140)
        parent.grid_columnconfigure(1, weight=0)
        parent.grid_columnconfigure(2, weight=0, minsize=170)
        parent.grid_columnconfigure(3, weight=1)

        for row_idx, (sector_key, sector_label) in enumerate(sectors):
            indices = by_sector.get(sector_key, [])
            etf_tickers = etfs_by_sector.get(sector_key, [])
            bg = "#fafafa" if row_idx % 2 == 1 else "white"

            # 섹터명 셀
            name_cell = tk.Frame(parent, bg=hdr_bg,
                                 highlightbackground="#e0e0e0", highlightthickness=1)
            name_cell.grid(row=row_idx, column=0, sticky="nsew")
            tk.Label(name_cell, text=sector_label, font=("SF Pro", 10, "bold"),
                     bg=hdr_bg, fg=hdr_fg, anchor="w",
                     padx=8, pady=5).pack(fill=tk.BOTH, expand=True)

            # 현물 셀 (테이블 정렬: name | price | pct | note)
            ind_cell = tk.Frame(parent, bg=bg,
                                highlightbackground="#e0e0e0", highlightthickness=1)
            ind_cell.grid(row=row_idx, column=1, sticky="nsew")
            ind_inner = tk.Frame(ind_cell, bg=bg)
            ind_inner.pack(fill=tk.BOTH, expand=True, padx=4, pady=3)
            ind_inner.grid_columnconfigure(0, minsize=120)  # name
            ind_inner.grid_columnconfigure(1, minsize=75)   # price
            ind_inner.grid_columnconfigure(2, minsize=65)   # pct
            for i, idx in enumerate(indices):
                self._render_indicator_inline(ind_inner, idx, bg, faded, row_i=i)

            # 선물 셀 (현물과 같은 row 에 맞춤, 없으면 빈 공간)
            fut_cell = tk.Frame(parent, bg=bg,
                                 highlightbackground="#e0e0e0", highlightthickness=1)
            fut_cell.grid(row=row_idx, column=2, sticky="nsew")
            fut_inner = tk.Frame(fut_cell, bg=bg)
            fut_inner.pack(fill=tk.BOTH, expand=True, padx=4, pady=3)
            fut_inner.grid_columnconfigure(0, minsize=100)  # name
            fut_inner.grid_columnconfigure(1, minsize=75)   # price
            fut_inner.grid_columnconfigure(2, minsize=60)   # pct
            for i, idx in enumerate(indices):
                self._render_futures_inline(fut_inner, idx, bg, faded, row_i=i)

            # 관심 ETF 셀 (테이블 정렬: name | price | pct)
            etf_cell = tk.Frame(parent, bg=bg,
                                highlightbackground="#e0e0e0", highlightthickness=1)
            etf_cell.grid(row=row_idx, column=3, sticky="nsew")
            etf_inner = tk.Frame(etf_cell, bg=bg)
            etf_inner.pack(fill=tk.BOTH, expand=True, padx=4, pady=3)
            etf_inner.grid_columnconfigure(0, minsize=220)  # name
            etf_inner.grid_columnconfigure(1, minsize=80)   # price
            etf_inner.grid_columnconfigure(2, minsize=60)   # pct
            etf_inner.grid_columnconfigure(3, weight=1)     # trailing spacer
            etf_row_i = 0
            for t in etf_tickers:
                stock = holdings_by_ticker.get(t)
                if not stock:
                    continue
                self._render_etf_inline(etf_inner, stock, toss.get(t, {}),
                                         bg, faded, row_i=etf_row_i)
                etf_row_i += 1

    def _render_indicator_inline(self, parent, idx: dict, bg: str, faded: bool,
                                  row_i: int = 0):
        """테이블 정렬 지표 — [name | price | pct% | note] 한 줄에 4 컬럼"""
        pct = idx.get("pct", 0)
        pct_color = sign_color(pct)
        if faded:
            pct_color = self._fade_hex(pct_color, 0.7)

        symbol = idx.get("symbol", "")
        url = resolve_us_indicator_url(symbol)
        closed = not is_market_open(market_of_symbol(symbol))
        name_fg = self._fade_hex("#222", 0.7) if faded else "#222"
        if closed:
            name_fg = self._fade_hex(name_fg, 0.85)
        name_disp = f"💤{idx['name']}" if closed else idx['name']
        note = idx.get("note", "")
        note_fg = self._fade_hex("#888", 0.5) if faded else "#888"

        price = idx.get("price", 0)
        price_txt = f"{price:,.2f}" if price else "-"
        price_fg = self._fade_hex(name_fg, 0.3) if faded else "#555"

        lbl_name = tk.Label(parent, text=name_disp, font=("SF Mono", 9),
                            bg=bg, fg=name_fg, anchor="w",
                            cursor="pointinghand")
        lbl_name.grid(row=row_i, column=0, sticky="w", pady=1)
        lbl_price = tk.Label(parent, text=price_txt, font=("SF Mono", 9),
                             bg=bg, fg=price_fg, anchor="e",
                             cursor="pointinghand")
        lbl_price.grid(row=row_i, column=1, sticky="e", padx=(4, 0), pady=1)
        lbl_pct = tk.Label(parent, text=f"{pct:+.2f}%",
                           font=("SF Mono", 9),
                           bg=bg, fg=pct_color, anchor="e",
                           cursor="pointinghand")
        lbl_pct.grid(row=row_i, column=2, sticky="e", padx=(4, 0), pady=1)
        lbl_note = None
        if note:
            lbl_note = tk.Label(parent, text=note, font=("SF Pro", 8),
                                bg=bg, fg=note_fg, anchor="w",
                                cursor="pointinghand")
            lbl_note.grid(row=row_i, column=3, sticky="w", padx=(8, 0), pady=1)

        widgets = [lbl_name, lbl_price, lbl_pct]
        if lbl_note is not None:
            widgets.append(lbl_note)
        for w in widgets:
            w.bind("<Button-1>", lambda e, u=url: self._open_in_existing_tab(u))

    def _render_futures_inline(self, parent, idx: dict, bg: str, faded: bool,
                                row_i: int = 0):
        """선물 셀 — [풀네임 (심볼) | price | pct%]. 선물 없으면 빈 행"""
        fut_symbol = idx.get("fut_symbol") or ""
        fut_pct = idx.get("fut_pct")
        fut_price = idx.get("fut_price")
        if not fut_symbol or fut_pct is None:
            return  # 빈 행 (그리드가 자연스레 공백)

        full_name = FUT_FULL_NAME.get(fut_symbol, fut_symbol)
        disp_name = f"{full_name} ({fut_symbol})"

        pct_color = sign_color(fut_pct)
        if faded:
            pct_color = self._fade_hex(pct_color, 0.7)

        name_fg = self._fade_hex("#222", 0.7) if faded else "#222"
        price_fg = self._fade_hex(name_fg, 0.3) if faded else "#555"

        price_txt = f"{fut_price:,.2f}" if fut_price else "-"

        url = resolve_us_indicator_url(fut_symbol)

        lbl_name = tk.Label(parent, text=disp_name, font=("SF Mono", 9),
                            bg=bg, fg=name_fg, anchor="w",
                            cursor="pointinghand")
        lbl_name.grid(row=row_i, column=0, sticky="w", pady=1)
        lbl_price = tk.Label(parent, text=price_txt, font=("SF Mono", 9),
                             bg=bg, fg=price_fg, anchor="e",
                             cursor="pointinghand")
        lbl_price.grid(row=row_i, column=1, sticky="e", padx=(4, 0), pady=1)
        lbl_pct = tk.Label(parent, text=f"{fut_pct:+.2f}%",
                           font=("SF Mono", 9),
                           bg=bg, fg=pct_color, anchor="e",
                           cursor="pointinghand")
        lbl_pct.grid(row=row_i, column=2, sticky="e", padx=(4, 0), pady=1)

        for w in (lbl_name, lbl_price, lbl_pct):
            w.bind("<Button-1>", lambda e, u=url: self._open_in_existing_tab(u))

    def _render_etf_inline(self, parent, stock: dict, price_data: dict,
                           bg: str, faded: bool, row_i: int = 0):
        """테이블 정렬 관심 ETF — [📊name | price | pct%]"""
        price = price_data.get("price", 0) or 0
        base = price_data.get("base", 0) or 0
        diff = price - base if (price and base) else 0
        pct = (diff / base * 100) if base else 0
        diff_color = sign_color(diff)
        if faded:
            diff_color = self._fade_hex(diff_color, 0.7)
        name_fg = self._fade_hex("#2c3e50", 0.7) if faded else "#2c3e50"
        name_font = ("SF Mono", 9) if faded else ("SF Mono", 9, "bold")

        t = stock["ticker"]
        url = f"https://tossinvest.com/stocks/A{t}"
        price_txt = f"{int(price):,}" if price else "-"
        diff_txt = f"{pct:+.2f}%" if diff else "0%"
        price_fg = self._fade_hex(name_fg, 0.3) if faded else "#555"

        lbl_name = tk.Label(parent, text=f"📊{stock.get('name', t)}",
                            font=name_font,
                            bg=bg, fg=name_fg, anchor="w",
                            cursor="pointinghand")
        lbl_name.grid(row=row_i, column=0, sticky="w", pady=1)
        lbl_price = tk.Label(parent, text=price_txt, font=("SF Mono", 9),
                             bg=bg, fg=price_fg, anchor="e",
                             cursor="pointinghand")
        lbl_price.grid(row=row_i, column=1, sticky="e", padx=(4, 0), pady=1)
        lbl_diff = tk.Label(parent, text=diff_txt, font=("SF Mono", 9),
                            bg=bg, fg=diff_color, anchor="e",
                            cursor="pointinghand")
        lbl_diff.grid(row=row_i, column=2, sticky="e", padx=(4, 0), pady=1)

        for w in (lbl_name, lbl_price, lbl_diff):
            w.bind("<Button-1>", lambda e, u=url: self._open_in_existing_tab(u))

    def _refresh_us_indices_if_needed(self):
        """미국 증시: 30초 TTL — 장중 현재가 실시간 반영"""
        import time as _t
        now = _t.time()
        if now - self.us_indices_ts < 30 and self.us_indices:
            return
        self.us_indices = fetch_us_indices_with_futures()
        self.us_indices_ts = now
        if hasattr(self, "us_frame") and self.us_visible_var.get():
            self._render_us_indices()

    def _check_alert(self, stock, current_price, peak_price, pnl_pct, from_peak_pct):
        """손절/익절 경고 체크 → 모달 알림"""
        ticker = stock["ticker"]
        name = stock["name"]
        buy_price = stock["avg_price"]
        stop_pct = self.config["stop_loss_alert_pct"]
        trail_pct = self.config["trailing_stop_alert_pct"]
        cooldown_min = self.config.get("alert_cooldown_minutes", 15)
        now = datetime.now()

        def in_cooldown(kind):
            key = f"{ticker}_{kind}"
            last = self.cooldowns.get(key)
            if not last:
                return False
            return (now - last).total_seconds() < cooldown_min * 60

        # 모달 팝업 제거 - 창에서 뱃지로 시각 표시만
        # 로그는 유지 (이력 추적용)
        triggered = None
        if pnl_pct <= stop_pct and not in_cooldown("stop_loss"):
            self.cooldowns[f"{ticker}_stop_loss"] = now
            self._save_alert("stop_loss", ticker, name, buy_price, current_price, pnl_pct, None, None)
            triggered = "stop_loss"
        elif pnl_pct > 0 and from_peak_pct <= trail_pct and not in_cooldown("trailing"):
            self.cooldowns[f"{ticker}_trailing"] = now
            self._save_alert("trailing_stop", ticker, name, buy_price, current_price,
                             pnl_pct, peak_price, from_peak_pct)
            triggered = "trailing"
        return triggered

    def _save_alert(self, kind, ticker, name, buy, current, pnl, peak, from_peak):
        ALERTS_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        fpath = ALERTS_DIR / f"portfolio_alerts_{today}.json"
        alerts = load_json(fpath, default=[])
        alerts.append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": kind, "ticker": ticker, "name": name,
            "buy_price": buy, "current_price": current, "pnl_pct": pnl,
            "peak_price": peak, "from_peak_pct": from_peak,
        })
        save_json(fpath, alerts)

    def _start_countdown(self):
        """다음 갱신까지 남은 초 카운트다운"""
        self.remaining_sec = self.config.get("polling_interval_seconds", 5)
        self._tick_countdown()

    def _tick_countdown(self):
        """1초마다 카운트다운 숫자 갱신"""
        self._update_time_label()
        if self.remaining_sec > 0:
            self.remaining_sec -= 1
            self._countdown_job = self.root.after(1000, self._tick_countdown)

    def _update_time_label(self):
        if hasattr(self, "last_refresh_time"):
            self.time_label.config(
                text=f"갱신: {self.last_refresh_time} ({self.remaining_sec}초)"
            )

    def _refresh_investor_cache_if_needed(self):
        """수급 데이터 갱신 — 2분 TTL (장중 값 변화 대응). KRX 6자리 종목만"""
        import time as _t
        now = _t.time()
        if now - self.investor_cache_ts < 120 and self.investor_cache:
            return
        tickers = [s["ticker"] for s in self.holdings
                   if s["ticker"].isdigit() and len(s["ticker"]) == 6]
        if not tickers:
            return
        with ThreadPoolExecutor(max_workers=min(len(tickers), 8)) as pool:
            results = pool.map(fetch_investor_flow, tickers)
            for t, r in zip(tickers, results):
                if r:
                    self.investor_cache[t] = r
        self.investor_cache_ts = now

    def _refresh_nxt_cache_if_needed(self):
        """NXT 지원 여부 갱신 — 24시간 TTL"""
        import time as _t
        now = _t.time()
        if now - self.nxt_cache_ts < 86400 and self.nxt_cache:
            return
        tickers = [s["ticker"] for s in self.holdings
                   if s["ticker"].isdigit() and len(s["ticker"]) == 6]
        if not tickers:
            return
        with ThreadPoolExecutor(max_workers=min(len(tickers), 8)) as pool:
            results = pool.map(fetch_nxt_supported, tickers)
            for t, r in zip(tickers, results):
                self.nxt_cache[t] = r
        self.nxt_cache_ts = now

    def _refresh_sector_cache_if_needed(self):
        """업종(섹터) 갱신 — 24시간 TTL. KRX 6자리만 (지수 제외)"""
        import time as _t
        now = _t.time()
        if now - self.sector_cache_ts < 86400 and self.sector_cache:
            return
        tickers = [s["ticker"] for s in self.holdings
                   if s["ticker"].isdigit() and len(s["ticker"]) == 6]
        if not tickers:
            return
        with ThreadPoolExecutor(max_workers=min(len(tickers), 8)) as pool:
            results = pool.map(fetch_stock_sector, tickers)
            for t, r in zip(tickers, results):
                if r:
                    self.sector_cache[t] = r
        self.sector_cache_ts = now

    def _refresh_warning_cache_if_needed(self):
        """투자경고 갱신 — 6시간 TTL (경고 상태는 하루에 몇 번 바뀔 수 있음)"""
        import time as _t
        now = _t.time()
        if now - self.warning_cache_ts < 6 * 3600 and self.warning_cache:
            return
        tickers = [s["ticker"] for s in self.holdings
                   if s["ticker"].isdigit() and len(s["ticker"]) == 6]
        if not tickers:
            return
        with ThreadPoolExecutor(max_workers=min(len(tickers), 8)) as pool:
            results = pool.map(fetch_stock_warning, tickers)
            for t, r in zip(tickers, results):
                self.warning_cache[t] = r  # "" 도 저장해서 재조회 방지
        self.warning_cache_ts = now

    def _refresh_consensus_cache_if_needed(self):
        """애널리스트 컨센서스 갱신 — 1시간 TTL (거의 매일 단위 업데이트). KRX 6자리만"""
        import time as _t
        now = _t.time()
        if now - self.consensus_cache_ts < 3600 and self.consensus_cache:
            return
        tickers = [s["ticker"] for s in self.holdings
                   if s["ticker"].isdigit() and len(s["ticker"]) == 6]
        if not tickers:
            return
        with ThreadPoolExecutor(max_workers=min(len(tickers), 8)) as pool:
            results = pool.map(fetch_target_consensus, tickers)
            for t, r in zip(tickers, results):
                if r:
                    self.consensus_cache[t] = r
        self.consensus_cache_ts = now

    # 세부 투자자 컬럼 — 작고 연한 폰트로 표시
    SUBTLE_KEYS = {"pension", "fin_inv", "trust", "pef",
                   "insurance", "bank", "other_fin", "other_corp"}

    @staticmethod
    def _fade_hex(color: str, ratio: float = 0.5) -> str:
        """hex 색상을 흰색과 혼합해 투명도 효과 (ratio=0.5 → 50% 페이드)
        #rgb (4자) 와 #rrggbb (7자) 모두 지원
        """
        if not isinstance(color, str) or not color.startswith("#"):
            return color
        # #rgb → #rrggbb 확장
        if len(color) == 4:
            color = "#" + "".join(c * 2 for c in color[1:])
        if len(color) != 7:
            return color
        try:
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            r = int(r + (255 - r) * ratio)
            g = int(g + (255 - g) * ratio)
            b = int(b + (255 - b) * ratio)
            return f"#{r:02x}{g:02x}{b:02x}"
        except Exception:
            return color

    def _make_row(self, row_idx: int, ticker: str, cells: list,
                  row_bg: str = "white", parent=None, parent_r=None,
                  faded: bool = False):
        """한 행 렌더 — 2-line pair 셀. cells 는 dict(key → cell tuple) 로 받음"""
        # cells 파라미터는 dict 또는 list(COLS 순서) 둘 다 허용
        if isinstance(cells, dict):
            cell_by_key = cells
        else:
            # 호환: list 로 들어오면 COLS top_key 순서로 매핑
            cell_by_key = {c[0]: cells[i] for i, c in enumerate(self.COLS)
                           if i < len(cells)}

        if not hasattr(self, "_row_cache"):
            self._row_cache = {}  # {(target_id, row_idx): {"frame": Frame, "labels": {key: Label}, "handlers": ticker}}

        def _paint(target, cols_info):
            if target is None:
                return
            cache_key = (id(target), row_idx)
            entry = self._row_cache.get(cache_key)
            if entry is None or not entry.get("frame").winfo_exists():
                frame = tk.Frame(target, bg=row_bg)
                frame.grid(row=row_idx, column=0, sticky="ew")
                target.grid_columnconfigure(0, weight=1)
                entry = {"frame": frame, "cells": {}, "ticker": ticker, "cols_sig": None}
                self._row_cache[cache_key] = entry
            frame = entry["frame"]
            frame.config(bg=row_bg)
            cells_dict = entry["cells"]  # {top_key: {"frame": F, "top": L, "bot": L or None}}

            cols_sig = tuple(c[0] for c in cols_info)
            if entry.get("cols_sig") != cols_sig:
                for c in cells_dict.values():
                    c.get("frame").destroy()
                cells_dict.clear()
                entry["cols_sig"] = cols_sig

            # 투자경고 뱃지 색상 맵 (모듈 레벨 이상 이동 가능하지만 클로저 내 사용)
            _WARN_BG = {"주의": "#f39c12", "경고": "#e67e22", "위험": "#c0392b",
                         "과열": "#e67e22", "관리": "#8b0000", "정지": "#666666"}

            def _apply_cell(lbl, cell, key, is_top, align):
                """cell = (text, fg, [bg, bold, [fill_mode]])
                fill_mode: "pill"(기본) = 텍스트 폭만 bg / "full" = 전체 셀 bg
                """
                text = cell[0] if cell else ""
                fg = cell[1] if cell and len(cell) > 1 else "#222"
                cell_bg = cell[2] if cell and len(cell) > 2 and cell[2] else row_bg
                cell_bold = cell[3] if cell and len(cell) > 3 else False
                fill_mode = cell[4] if cell and len(cell) > 4 else "pill"
                font_style = ("SF Mono", 10, "bold") if cell_bold else ("SF Mono", 10)
                has_badge_bg = cell_bold and cell_bg != row_bg
                is_pill = has_badge_bg and fill_mode == "pill"
                if key in self.SUBTLE_KEYS and not cell_bold:
                    if fg == "#c0392b":
                        fg = "#d06b5f"
                    elif fg == "#1f4e8f":
                        fg = "#5a7ca8"
                if faded:
                    fg = self._fade_hex(fg, 0.85)
                    if cell_bg != row_bg:
                        cell_bg = self._fade_hex(cell_bg, 0.85)
                lbl.config(text=text, bg=cell_bg, fg=fg, font=font_style,
                           padx=(6 if is_pill else 3),
                           pady=(1 if is_pill else 0),
                           borderwidth=0)
                # 종목명 뒤 투자경고 pill 여부 판정
                warn_text = ""
                if is_top and key == "name":
                    warn_text = self.warning_cache.get(ticker, "") or ""
                # 패킹 모드: pill (텍스트 폭) / full (셀 전체) / pill_warn (name + 경고)
                mode_key = "top_mode" if is_top else "bot_mode"
                current_mode = pair.get(mode_key)
                if warn_text:
                    target_mode = "pill_warn"
                elif is_pill:
                    target_mode = "pill"
                else:
                    target_mode = "full"
                # name 컬럼은 top_wrap 내부에 side=LEFT 로 배치되어 있어 pack 전환 불필요
                is_name_top = is_top and key == "name"
                if current_mode != target_mode and not is_name_top:
                    lbl.pack_forget()
                    pack_before = pair["bot"] if (is_top and pair.get("bot") is not None
                                                  and pair["bot"].winfo_exists()) else None
                    if target_mode == "pill":
                        pack_anchor = {"w": "w", "e": "e", "center": "center"}[align]
                        if pack_before is not None:
                            lbl.pack(anchor=pack_anchor, padx=3, pady=1,
                                     before=pack_before)
                        else:
                            lbl.pack(anchor=pack_anchor, padx=3, pady=1)
                    else:
                        if is_top and is_tall_single:
                            lbl.pack(fill=tk.BOTH, expand=True)
                        elif pack_before is not None:
                            lbl.pack(fill=tk.X, before=pack_before)
                        else:
                            lbl.pack(fill=tk.X)
                    pair[mode_key] = target_mode
                elif is_name_top and current_mode != target_mode:
                    # name: pill 일 땐 텍스트 폭만, full 일 땐 전체 채움
                    lbl.pack_forget()
                    if target_mode in ("pill", "pill_warn"):
                        lbl.pack(side=tk.LEFT, padx=3, pady=1)
                    else:
                        lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)
                    pair[mode_key] = target_mode
                # 경고 pill 관리 (name top 전용) — top_wrap 내부에 sibling 으로
                if is_name_top:
                    warn_lbl = pair.get("warn")
                    top_wrap = pair.get("top_wrap")
                    if warn_text and top_wrap is not None:
                        warn_bg = _WARN_BG.get(warn_text, "#666")
                        warn_fg = "white"
                        if faded:
                            warn_bg = self._fade_hex(warn_bg, 0.85)
                        if warn_lbl is None or not warn_lbl.winfo_exists():
                            warn_lbl = tk.Label(top_wrap, borderwidth=0,
                                                 padx=4, pady=1)
                            pair["warn"] = warn_lbl
                        warn_lbl.config(text=warn_text, fg=warn_fg, bg=warn_bg,
                                         font=("SF Mono", 9, "bold"))
                        if not warn_lbl.winfo_ismapped():
                            warn_lbl.pack(side=tk.LEFT, padx=(2, 0), pady=1)
                    else:
                        if warn_lbl is not None and warn_lbl.winfo_exists():
                            warn_lbl.pack_forget()

            # 모든 bot_key=None 이면 단일-라인 모드 (짧은 행 높이)
            single_line = all(c[1] is None for c in cols_info) if cols_info else False
            row_h = 22 if single_line else 40

            for col_idx, col in enumerate(cols_info):
                top_key, bot_key, _, _, pw, align = col
                anchor = {"w": "w", "e": "e", "center": "center"}[align]
                top_cell = cell_by_key.get(top_key, ("", "#555"))
                bot_cell = cell_by_key.get(bot_key, ("", "#555")) if bot_key else None

                # 이 컬럼이 단독(bot_key=None) 인데 row 는 2줄(single_line=False) 이면
                # top 라벨을 세로 중앙에 배치 (fill=BOTH, expand=True → 40px 전체에 anchor=center-y)
                is_tall_single = (bot_key is None and not single_line)
                pair = cells_dict.get(top_key)
                if pair is None or not pair.get("frame").winfo_exists():
                    pf = tk.Frame(frame, bg=row_bg, width=pw, height=row_h)
                    pf.grid(row=0, column=col_idx, sticky="nsew")
                    pf.pack_propagate(False)
                    # 종목명 컬럼은 경고 pill 이 옆에 붙을 수 있어 top_wrap Frame 으로 감쌈
                    # (sibling pill 과 bot(sector) 레이아웃 충돌 방지)
                    top_wrap = None
                    if top_key == "name":
                        top_wrap = tk.Frame(pf, bg=row_bg)
                        top_wrap.pack(fill=tk.X)
                        top_lbl = tk.Label(top_wrap, text="", font=("SF Mono", 10),
                                           bg=row_bg, fg="#222",
                                           anchor=anchor, padx=3, pady=0,
                                           borderwidth=0)
                        top_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)
                    else:
                        top_lbl = tk.Label(pf, text="", font=("SF Mono", 10),
                                           bg=row_bg, fg="#222",
                                           anchor=anchor, padx=3, pady=0,
                                           borderwidth=0)
                        if is_tall_single:
                            top_lbl.pack(fill=tk.BOTH, expand=True)
                        else:
                            top_lbl.pack(fill=tk.X)
                    if not single_line and bot_key is not None:
                        bot_lbl = tk.Label(pf, text="", font=("SF Mono", 10),
                                           bg=row_bg, fg="#555",
                                           anchor=anchor, padx=3, pady=0,
                                           borderwidth=0)
                        bot_lbl.pack(fill=tk.X)
                    else:
                        bot_lbl = None
                    pair = {"frame": pf, "top": top_lbl, "bot": bot_lbl,
                            "top_wrap": top_wrap,
                            "top_key": top_key, "bot_key": bot_key,
                            "top_mode": "full", "bot_mode": "full"}
                    cells_dict[top_key] = pair
                pf = pair["frame"]
                pf.config(bg=row_bg)
                _apply_cell(pair["top"], top_cell, top_key, True, align)
                if pair["bot"] is not None:
                    _apply_cell(pair["bot"], bot_cell, bot_key or top_key, False, align)

                # 클릭 재바인딩
                clickables = [pf, pair["top"]]
                if pair["bot"] is not None:
                    clickables.append(pair["bot"])
                for w in clickables:
                    w.unbind("<Button-1>"); w.unbind("<Double-1>")
                    w.unbind("<Button-2>"); w.unbind("<Button-3>")
                    w.unbind("<Control-Button-1>")
                    w.bind("<Button-1>", lambda e, t=ticker: self._on_row_click(t))
                    w.bind("<Double-1>", lambda e, t=ticker: self._on_row_double_click(t))
                    w.bind("<Button-2>", lambda e, t=ticker: self._on_row_right_click(t, e))
                    w.bind("<Button-3>", lambda e, t=ticker: self._on_row_right_click(t, e))
                    w.bind("<Control-Button-1>", lambda e, t=ticker: self._on_row_right_click(t, e))
            entry["ticker"] = ticker
            # 이번 refresh 에 "사용됨" 마킹
            entry["_used"] = self._refresh_tick

        left_target = parent if parent is not None else self.rows_frame
        right_target = parent_r
        left_cols, right_cols = self._cols_for_parent(left_target)
        _paint(left_target, left_cols)
        _paint(right_target, right_cols)

    def refresh(self):
        # 중복 갱신 방지: 기존 refresh / countdown 예약 취소
        if hasattr(self, "_refresh_job") and self._refresh_job:
            try:
                self.root.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        if hasattr(self, "_countdown_job"):
            try:
                self.root.after_cancel(self._countdown_job)
            except Exception:
                pass

        # 스크롤 위치 저장 (layout 변경으로 인한 스크롤 리셋 방지)
        try:
            _saved_yview = self.outer_canvas.yview()
            _saved_xview = self.outer_canvas.xview()
        except Exception:
            _saved_yview = _saved_xview = None
        def _restore_scroll():
            try:
                if _saved_yview:
                    self.outer_canvas.yview_moveto(_saved_yview[0])
                if _saved_xview:
                    self.outer_canvas.xview_moveto(_saved_xview[0])
            except Exception:
                pass
        # refresh 종료 직후 + idle 태스크 후에도 복원 (scrollregion 재계산 대응)
        self.root.after_idle(_restore_scroll)
        self.root.after(50, _restore_scroll)

        # 수급 캐시 갱신 (필요 시)
        self._refresh_investor_cache_if_needed()
        self._refresh_consensus_cache_if_needed()
        self._refresh_sector_cache_if_needed()
        self._refresh_warning_cache_if_needed()
        self._refresh_nxt_cache_if_needed()

        # 미국 증시 실시간 갱신 (2분마다, 표시 중일 때만)
        if hasattr(self, "us_visible_var") and self.us_visible_var.get():
            self._refresh_us_indices_if_needed()

        # in-place 업데이트 방식 — 기존 widget 재사용, 이번 refresh 의 "사용됨" 마킹용 tick
        self._refresh_tick = getattr(self, "_refresh_tick", 0) + 1
        if hasattr(self, "_row_ticker"):
            self._row_ticker.clear()

        if not self.holdings:
            if hasattr(self, "time_label"):
                self.time_label.config(text="보유 종목 없음")
            self._refresh_job = self.root.after(self.interval_ms, self.refresh)
            return

        # 토스 API로 batch 시세 + 거래량 조회 (KRX 6자리 코드만, ^ 지수 제외)
        tickers = [s["ticker"] for s in self.holdings
                   if s["ticker"].isdigit() and len(s["ticker"]) == 6]
        toss_data = fetch_toss_prices_batch(tickers)
        prices = {t: d["price"] for t, d in toss_data.items()}
        volumes = {t: d["volume"] for t, d in toss_data.items()}
        bases = {t: d["base"] for t, d in toss_data.items()}  # 전일 종가
        trade_dates = {t: d.get("trade_date", "") for t, d in toss_data.items()}
        opens = {t: d.get("open", 0) for t, d in toss_data.items()}  # 시가 (0=미결정)
        # 실패 종목은 네이버로 폴백 (거래량 없음)
        missing = [t for t in tickers if t not in prices]
        if missing:
            with ThreadPoolExecutor(max_workers=min(len(missing), 8)) as pool:
                for t, p in zip(missing, pool.map(_fetch_price, missing)):
                    if p is not None:
                        prices[t] = p

        total_invested = 0
        total_current = 0

        # 세후 평가용 매도 수수료율 (토스 기준 0.2%)
        sell_fee_pct = self.config.get("sell_fee_pct", 0.2)
        fee_multiplier = 1 - (sell_fee_pct / 100)

        # 일반/퇴직연금/관심/관심ETF 분리 — 테이블 별 인덱스 및 합계
        main_idx = pension_idx = watch_idx = watch_etf_idx = 0
        main_invested = main_current = 0
        main_shares = pension_shares = 0
        pension_invested = pension_current = 0
        # 전일대비 합계 (금액 + 전일 총 평가액 — % 계산용)
        main_day_change = main_prev_total = 0
        pension_day_change = pension_prev_total = 0

        # 각 테이블 내에서 거래 중인 종목은 위, 💤 는 아래로 정렬
        _phase = kr_session_phase()
        try:
            from zoneinfo import ZoneInfo
            _today_kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
        except Exception:
            _today_kst = datetime.now().strftime("%Y-%m-%d")

        def _sleep_flag(s):
            if _phase == "REGULAR":
                return 0
            if _phase == "EXTENDED":
                # 오늘 거래량(단일가 주문 접수 포함) 있으면 활성
                return 0 if volumes.get(s["ticker"], 0) > 0 else 1
            return 1  # CLOSED

        def _day_change_pct(s):
            t = s["ticker"]
            cur = prices.get(t, 0) or 0
            base = bases.get(t, 0) or 0
            return ((cur - base) / base * 100) if (cur and base) else 0.0

        _account_order = {"": 0, "관심ETF": 1, "관심": 2, "퇴직연금": 3}
        # 정렬 키: (계정 그룹, 💤 는 아래, 전일대비 등락률 내림차순)
        sorted_holdings = sorted(
            self.holdings,
            key=lambda s: (
                _account_order.get(s.get("account", ""), 9),
                _sleep_flag(s),
                -_day_change_pct(s),
            ),
        )

        for stock in sorted_holdings:
            account = stock.get("account", "")
            is_pension = account == "퇴직연금"
            is_watch = account == "관심"
            is_watch_etf = account == "관심ETF"
            is_watch_any = is_watch or is_watch_etf
            # 관심ETF 는 상단 별도 패널에서 렌더, 메인 테이블 루프에서는 건너뜀
            if is_watch_etf:
                continue
            parent_r = None  # 좌우 분리 없이 단일 frame 으로 통합 렌더
            if is_pension:
                parent = self.pension_rows_frame
                row_idx = pension_idx
                pension_idx += 1
            elif is_watch:
                parent = self.watchlist_rows_frame
                row_idx = watch_idx
                watch_idx += 1
            else:
                parent = self.rows_frame
                row_idx = main_idx
                main_idx += 1
            ticker = stock["ticker"]
            name = stock["name"]
            shares = stock["shares"]
            buy_price = stock["avg_price"]
            current_price = prices.get(ticker)

            if not is_watch_any:
                total_invested += buy_price * shares
                if is_pension:
                    pension_invested += buy_price * shares
                    pension_shares += shares
                else:
                    main_invested += buy_price * shares
                    main_shares += shares

            flow = self.investor_cache.get(ticker, {})
            indiv = flow.get("개인", 0) if flow else 0
            foreign = flow.get("외국인", 0) if flow else 0
            inst = flow.get("기관", 0) if flow else 0

            indiv_cell = (format_signed(indiv), sign_color(indiv)) if flow else ("-", "#999")
            # 외국인: 순매수량 / 보유율(%) 분리
            if flow:
                foreign_ratio = flow.get("외국인비율", 0)
                foreign_amt_cell = (format_signed(foreign), sign_color(foreign))
                ratio_text = f"({foreign_ratio:.2f}%)" if foreign_ratio > 0 else ""
                # %는 연한 회색 (방향성 없는 단순 비율)
                foreign_pct_cell = (ratio_text, "#888")
            else:
                foreign_amt_cell = ("-", "#999")
                foreign_pct_cell = ("", "#999")
            inst_cell = (format_signed(inst), sign_color(inst)) if flow else ("-", "#999")

            def _icell(key):
                v = flow.get(key, 0) if flow else 0
                return (format_signed(v), sign_color(v)) if flow else ("-", "#999")

            pension_cell = _icell("연기금")
            fin_inv_cell = _icell("금융투자")
            trust_cell = _icell("투신")
            pef_cell = _icell("사모")
            insurance_cell = _icell("보험")
            bank_cell = _icell("은행")
            other_fin_cell = _icell("기타금융")
            other_corp_cell = _icell("기타법인")

            if current_price is None:
                fail_map = {
                    "name": (name, "#333"),
                    "sector": (self.sector_cache.get(ticker, ""), "#666"),
                    "day_chg_amt": ("-", "#999"),
                    "day_chg_pct": ("", "#999"),
                    "shares": (str(shares), "#333"),
                    "buy": (f"{buy_price:,}", "#333"),
                    "cur": ("-", "#999"),
                    "target_money": ("-", "#999"),
                    "target_pct": ("", "#999"),
                    "opinion": ("-", "#999"),
                    "peak_money": ("-", "#999"),
                    "peak_pct": ("", "#999"),
                    "pnl_amt_money": ("-", "#999"),
                    "pnl_amt_pct": ("", "#999"),
                    "volume": ("-", "#999"),
                    "indiv": indiv_cell,
                    "foreign_amt": foreign_amt_cell,
                    "foreign_pct": foreign_pct_cell,
                    "inst": inst_cell,
                    "pension": pension_cell,
                    "fin_inv": fin_inv_cell,
                    "trust": trust_cell,
                    "pef": pef_cell,
                    "insurance": insurance_cell,
                    "bank": bank_cell,
                    "other_fin": other_fin_cell,
                    "other_corp": other_corp_cell,
                }
                # 조합 셀 추가
                fail_map.setdefault("target_combined", ("-", "#999"))
                fail_map.setdefault("pnl_combined", ("-", "#999"))
                fail_map.setdefault("day_combined", ("-", "#999"))
                fail_map.setdefault("peak_combined", ("-", "#999"))
                fail_map.setdefault("foreign_combined", fail_map.get("foreign_amt", ("-", "#999")))
                self._make_row(row_idx, ticker, fail_map, row_bg="white",
                               parent=parent, parent_r=parent_r)
                total_current += buy_price * shares
                if is_pension:
                    pension_current += buy_price * shares
                else:
                    main_current += buy_price * shares
                continue

            # 피크 초기값: 이전 피크 또는 현재가 (매수가로 시작하지 않음)
            prev_peak = self.peaks.get(ticker, current_price)
            peak_price = max(prev_peak, current_price)
            self.peaks[ticker] = peak_price

            # 세후 현재가 (수수료·세금 차감) - 토스 기준
            net_price = current_price * fee_multiplier
            pnl_pct = (net_price - buy_price) / buy_price * 100 if buy_price else 0
            from_peak_pct = (current_price - peak_price) / peak_price * 100 if peak_price else 0
            stock_current = round(net_price * shares) if shares else 0
            if not is_watch_any:
                total_current += stock_current
                if is_pension:
                    pension_current += stock_current
                else:
                    main_current += stock_current

            triggered = None if is_watch_any else self._check_alert(
                stock, current_price, peak_price, pnl_pct, from_peak_pct)

            # 행 배경 및 상태 텍스트
            if triggered == "stop_loss" or pnl_pct <= self.config["stop_loss_alert_pct"]:
                row_bg = "#e8f0fa"
                status = "🔻 손절"
                status_fg = "#1f4e8f"
            elif triggered == "trailing" or (pnl_pct > 0 and from_peak_pct <= self.config["trailing_stop_alert_pct"]):
                row_bg = "#fff5d6"
                status = "📉 익절"
                status_fg = "#a66d00"
            elif pnl_pct > 0:
                row_bg = "white"
                status = "▲"
                status_fg = "#c0392b"
            elif pnl_pct < 0:
                row_bg = "white"
                status = "▼"
                status_fg = "#1f4e8f"
            else:
                row_bg = "white"
                status = "-"
                status_fg = "#555"

            # 투자경고/주의/위험: 행 배경을 경고색으로 오버라이드 (손절/익절보다 우선)
            _warn_text = self.warning_cache.get(ticker, "") or ""
            if _warn_text:
                _row_warn_bg = {
                    "위험": "#ffd9d9", "관리": "#ffd9d9",   # 진한 빨강 계열
                    "경고": "#ffe4cc", "과열": "#ffe4cc",   # 주황 계열
                    "주의": "#fff3cc",                       # 노랑
                    "정지": "#e8e8e8",                       # 회색
                }
                row_bg = _row_warn_bg.get(_warn_text, row_bg)

            # 짝수줄(1-based) 은 연한 회색 배경으로 라인 구분 (상태 뱃지 색상은 건드리지 않음)
            if row_bg == "white" and row_idx % 2 == 1:
                row_bg = "#f5f5f5"

            name_color = sign_color(pnl_pct)
            pnl_amount = round((net_price - buy_price) * shares)
            # 위험 판정 (독립적으로 동시 적용 가능)
            is_stop = pnl_pct <= self.config["stop_loss_alert_pct"]
            is_peak_drop = from_peak_pct <= self.config["trailing_stop_alert_pct"] and abs(from_peak_pct) >= 0.01

            status_cell = (status, status_fg)

            # 상태 뱃지 — 손절이 우선 (빨강), 아니면 익절 (주황)
            if is_stop:
                status_cell = ("🔻 손절", "white", "#1f4e8f", True)
            elif is_peak_drop and pnl_pct > 0:
                status_cell = ("📉 익절", "white", "#c0392b", True)  # 수익 중 → 빨강
            elif is_peak_drop:
                status_cell = ("⚠ 하락", "white", "#4a90c2", True)  # 손실 중 → 파랑

            # 💤 표시 조건 — 토스가 알려주는 마지막 체결 날짜로 판정
            #  정규장: 항상 활성
            #  장전/장후(08-09 / 15:40-20): 오늘자 체결 있으면 활성, 없으면 💤
            #  그 외: 💤
            phase = kr_session_phase()
            if phase == "REGULAR":
                is_sleeping = False
            elif phase == "EXTENDED":
                # 프리/애프터마켓 거래 가능 여부 판정
                #  1) NXT 미지원 종목 → 💤
                #  2) NXT 지원 종목: 거래량 변화 추적해 2분 불변 시 💤
                if not self.nxt_cache.get(ticker, False):
                    is_sleeping = True
                else:
                    if not hasattr(self, "_vol_last_val"):
                        self._vol_last_val = {}
                        self._vol_last_change_ts = {}
                    import time as _t
                    _now = _t.time()
                    cur_vol = volumes.get(ticker, 0)
                    prev_vol = self._vol_last_val.get(ticker)
                    if prev_vol is None or cur_vol != prev_vol:
                        self._vol_last_change_ts[ticker] = _now
                    self._vol_last_val[ticker] = cur_vol
                    last_change = self._vol_last_change_ts.get(ticker, 0)
                    stale = (_now - last_change) > 120
                    is_sleeping = (cur_vol == 0) or stale
            else:
                is_sleeping = True
            name_display = f"💤 {name}" if is_sleeping else name

            # 종목명 뱃지 — 모두 pill (텍스트 폭만 bg) 방식
            # - 퇴직연금/관심 주식/ETF: #ecf0f3 + 방향 글자색
            # - 손절: 파랑 pill + white
            # - 익절 (수익 중 peak drop): 빨강 pill + white
            # - 그 외 (하락/정상 보유): 카톡 노랑 #FEE500 + 방향 글자색
            if is_watch_any or is_pension:
                if is_watch_any:
                    # 관심주식/ETF: 전일대비 가격 변동 색상
                    _bp = bases.get(ticker, 0)
                    fg_pill = sign_color(current_price - _bp) if _bp else "#555"
                else:
                    fg_pill = sign_color(pnl_pct)
                name_cell = (name_display, fg_pill, "#ecf0f3", True, "pill")
            elif is_stop:
                name_cell = (name_display, "white", "#1f4e8f", True, "pill")
            elif is_peak_drop and pnl_pct > 0:
                name_cell = (name_display, "white", "#c0392b", True, "pill")
            elif is_peak_drop:
                # 손실 중 + 피크 드롭 = ⚠ 하락 상태 뱃지와 동일 색상
                name_cell = (name_display, "white", "#4a90c2", True, "pill")
            else:
                name_cell = (name_display, sign_color(pnl_pct), "#FEE500", True, "pill")

            # 연한색 매핑 (부호 방향에 따라) — 먼저 정의 (뒤 로직에서 사용)
            def _light_color(n):
                if n > 0:
                    return "#d06b5f"
                if n < 0:
                    return "#5a7ca8"
                return "#888"

            # 현재가 — 직전 tick 대비 상승/하락 화살표 + 색상
            #  첫 전환: 속빈 세모 (▵/▽) / 연속 같은 방향: 속찬 세모 (▲/▼)
            if not hasattr(self, "_last_tick_prices"):
                self._last_tick_prices = {}
                self._last_tick_dirs = {}
            prev_tick = self._last_tick_prices.get(ticker)
            prev_dir = self._last_tick_dirs.get(ticker)
            if prev_tick is not None and current_price > prev_tick:
                new_dir = "up"
                arrow = "▲ " if prev_dir == "up" else "▵ "
                cur_fg = "#c0392b"
            elif prev_tick is not None and current_price < prev_tick:
                new_dir = "down"
                arrow = "▼ " if prev_dir == "down" else "▽ "
                cur_fg = "#1f4e8f"
            else:
                new_dir = prev_dir  # 불변 → 이전 방향 유지
                arrow, cur_fg = "", "#999"  # 변동 없으면 회색 (관심주식과 동일)
            self._last_tick_prices[ticker] = current_price
            self._last_tick_dirs[ticker] = new_dir
            cur_cell = (f"{arrow}{current_price:,}", cur_fg)
            pnl_cell = (f"{pnl_pct:+.2f}%", sign_color(pnl_pct))  # legacy (미사용)

            # 피크가(금액)/피크대비(%) — 손익금액 패턴과 동일
            if peak_price == current_price or peak_price <= buy_price:
                peak_money_cell = ("", "#222")
                peak_pct_cell = ("", "#222")
            else:
                peak_money_cell = (f"{peak_price:,}", "#c0392b")
                peak_pct_cell = (f"({from_peak_pct:+.2f}%)", _light_color(from_peak_pct))

            # 위험 뱃지: 손절 시
            if is_stop:
                pnl_cell = (f"{pnl_pct:+.2f}%", "white", "#1f4e8f", True)
            # 피크 하락 뱃지: 금액+% 양쪽 모두
            if is_peak_drop and peak_price > buy_price and peak_price != current_price:
                peak_bg = "#c0392b" if pnl_pct > 0 else "#4a90c2"
                peak_money_cell = (f"{peak_price:,}", "white", peak_bg, True)
                peak_pct_cell = (f"({from_peak_pct:+.2f}%)", "white", peak_bg, True)

            volume = volumes.get(ticker, 0)
            base_price = bases.get(ticker, 0)
            volume_cell = (format_volume(volume), "#555")

            # 전일대비 누계 (관심 주식/ETF 제외)
            if not is_watch_any and base_price > 0 and shares:
                _day_diff = (current_price - base_price) * shares
                _prev_val = base_price * shares
                if is_pension:
                    pension_day_change += _day_diff
                    pension_prev_total += _prev_val
                else:
                    main_day_change += _day_diff
                    main_prev_total += _prev_val

            # 전일 종가 대비: 주당 가격 변동 (%는 연한색)
            if base_price > 0:
                price_diff = current_price - base_price
                price_pct = (price_diff / base_price) * 100
                day_chg_amt_cell = (format_signed(price_diff) if price_diff else "0", sign_color(price_diff))
                day_chg_pct_cell = (f"({price_pct:+.2f}%)" if price_diff else "", _light_color(price_diff))
            else:
                day_chg_amt_cell = ("-", "#999")
                day_chg_pct_cell = ("", "#999")

            # 애널리스트 컨센서스 (목표주가 / 현재가 대비 / 투자의견)
            consensus = self.consensus_cache.get(ticker) or {}
            target_price = consensus.get("target")
            opinion_text = consensus.get("opinion") or ""
            if target_price and current_price:
                gap_pct = (target_price - current_price) / current_price * 100
                # 금액(진한색) + 괴리율(연한색) — 둘 다 부호에 따라 빨강/파랑
                target_money_cell = (f"{target_price:,}", sign_color(gap_pct))
                target_pct_cell = (f"({gap_pct:+.1f}%)", _light_color(gap_pct))
            else:
                target_money_cell = ("-", "#999")
                target_pct_cell = ("", "#999")
            score = consensus.get("score")
            if opinion_text or score is not None:
                op_color = "#c0392b" if "매수" in opinion_text else (
                    "#1f4e8f" if "매도" in opinion_text else "#555")
                # 점수만 표시, 색상으로 매수/매도 구분 — 점수 없으면 텍스트 사용
                if score is not None:
                    display = f"{score:.2f}"
                else:
                    display = opinion_text or "-"
                opinion_cell = (display, op_color)
            else:
                opinion_cell = ("-", "#999")

            # 손익금액/% 분리 (%는 연한색)
            pnl_amt_money_cell = (format_signed(pnl_amount) if pnl_amount else "0", sign_color(pnl_amount))
            pnl_amt_pct_cell = (f"({pnl_pct:+.2f}%)", _light_color(pnl_amount))
            # 손절 뱃지 적용 (금액+% 양쪽 모두)
            if is_stop:
                pnl_amt_money_cell = (format_signed(pnl_amount) if pnl_amount else "0",
                                       "white", "#1f4e8f", True)
                pnl_amt_pct_cell = (f"({pnl_pct:+.2f}%)", "white", "#1f4e8f", True)

            # 전체 키-셀 매핑
            _sector_raw = self.sector_cache.get(ticker, stock.get("sector", ""))
            _sector_text = f"({_sector_raw})" if _sector_raw else ""
            cell_by_key = {
                "name": name_cell,
                "sector": (_sector_text, "#aaa"),  # 괄호 + 연한 회색

                "day_chg_amt": day_chg_amt_cell,
                "day_chg_pct": day_chg_pct_cell,
                "shares": (str(shares), "#222"),
                "buy": (f"{buy_price:,}", "#999"),
                "cur": cur_cell,
                "target_money": target_money_cell,
                "target_pct": target_pct_cell,
                "opinion": opinion_cell,
                "peak_money": peak_money_cell,
                "peak_pct": peak_pct_cell,
                "pnl_amt_money": pnl_amt_money_cell,
                "pnl_amt_pct": pnl_amt_pct_cell,
                "volume": volume_cell,
                "indiv": indiv_cell,
                "foreign_amt": foreign_amt_cell,
                "foreign_pct": foreign_pct_cell,
                "inst": inst_cell,
                "pension": pension_cell,
                "fin_inv": fin_inv_cell,
                "trust": trust_cell,
                "pef": pef_cell,
                "insurance": insurance_cell,
                "bank": bank_cell,
                "other_fin": other_fin_cell,
                "other_corp": other_corp_cell,
            }
            # 관심 주식/ETF: 매수/수량/손익/피크 같은 보유 의존 컬럼은 공백
            if is_watch_any:
                for k in ("shares", "buy", "peak_money", "peak_pct",
                          "pnl_amt_money", "pnl_amt_pct"):
                    cell_by_key[k] = ("", "#999")

            # 2-line 조합 셀: "금액 (%)" 형식으로 하나로 묶음 (fg 는 금액 색상)
            def _combine(m_cell, p_cell):
                """금액 cell 과 % cell 을 '12,345 (+1.23%)' 로 결합"""
                m_text = m_cell[0] if m_cell else ""
                p_text = p_cell[0] if p_cell else ""
                fg = m_cell[1] if m_cell else "#555"
                if not m_text and not p_text:
                    return ("", "#999")
                if m_text and p_text:
                    text = f"{m_text} {p_text}"
                elif m_text:
                    text = m_text
                else:
                    text = p_text
                # 뱃지 보존 (len>=4: text, fg, bg, bold)
                if m_cell and len(m_cell) >= 4:
                    return (text, m_cell[1], m_cell[2], m_cell[3])
                return (text, fg)

            cell_by_key["target_combined"] = _combine(
                cell_by_key.get("target_money"), cell_by_key.get("target_pct"))
            cell_by_key["pnl_combined"] = _combine(
                cell_by_key.get("pnl_amt_money"), cell_by_key.get("pnl_amt_pct"))
            cell_by_key["day_combined"] = _combine(
                cell_by_key.get("day_chg_amt"), cell_by_key.get("day_chg_pct"))
            cell_by_key["peak_combined"] = _combine(
                cell_by_key.get("peak_money"), cell_by_key.get("peak_pct"))
            cell_by_key["foreign_combined"] = _combine(
                cell_by_key.get("foreign_amt"), cell_by_key.get("foreign_pct"))

            # 장마감 체크박스 꺼져 있으면 fade 적용 안 함
            _apply_fade = is_sleeping and getattr(
                self, "fade_sleeping_var", None) and self.fade_sleeping_var.get()
            self._make_row(row_idx, ticker, cell_by_key, row_bg=row_bg,
                           parent=parent, parent_r=parent_r, faded=_apply_fade)

        save_json(PEAKS_PATH, self.peaks)

        def _append_total(parent_frame_left, parent_frame_right, row_idx,
                          invested, current, shares_sum,
                          day_change=0, prev_total=0):
            pnl = current - invested
            tm = {
                "name": ("합계", "#222"),
                "sector": ("", "#555"),
                "day_chg_amt": ("", "#555"),
                "day_chg_pct": ("", "#555"),
                "shares": (f"{shares_sum}주", "#222"),
                "buy": (f"{invested:,}", "#222"),
                "cur": (f"{current:,}", sign_color(pnl)),
                "target_money": ("", "#555"),
                "target_pct": ("", "#555"),
                "opinion": ("", "#555"),
                "peak_money": ("", "#555"),
                "peak_pct": ("", "#555"),
                "pnl_amt_money": (format_signed(pnl) if pnl else "0", sign_color(pnl)),
                "pnl_amt_pct": (
                    f"({(pnl/invested*100 if invested else 0):+.2f}%)",
                    "#d06b5f" if pnl > 0 else ("#5a7ca8" if pnl < 0 else "#888")
                ),
                "volume": ("", "#555"),
                "indiv": ("", "#555"),
                "foreign_amt": ("", "#555"),
                "foreign_pct": ("", "#555"),
                "inst": ("", "#555"),
                "pension": ("", "#555"),
                "fin_inv": ("", "#555"),
                "trust": ("", "#555"),
                "pef": ("", "#555"),
                "insurance": ("", "#555"),
                "bank": ("", "#555"),
                "other_fin": ("", "#555"),
                "other_corp": ("", "#555"),
            }
            # 조합 셀 (2-line 레이아웃에 맞춰)
            tm["target_combined"] = ("", "#555")
            tm["peak_combined"] = ("", "#555")
            tm["foreign_combined"] = ("", "#555")
            pnl_pct_str = f"({(pnl/invested*100 if invested else 0):+.2f}%)"
            tm["pnl_combined"] = (
                f"{format_signed(pnl) if pnl else '0'} {pnl_pct_str}",
                sign_color(pnl),
            )
            # 전일대비 합계
            if prev_total and day_change:
                day_pct = day_change / prev_total * 100
                tm["day_combined"] = (
                    f"{format_signed(day_change) if day_change else '0'} "
                    f"({day_pct:+.2f}%)",
                    sign_color(day_change),
                )
            else:
                tm["day_combined"] = ("", "#555")

            total_cells_by_key = {k: (v[0], v[1], "#f5f5f5", True) for k, v in tm.items()}
            total_ticker = f"__total_{id(parent_frame_left)}__"
            self._make_row(row_idx, total_ticker, total_cells_by_key,
                           row_bg="#f5f5f5",
                           parent=parent_frame_left, parent_r=parent_frame_right)

        # 일반 합계
        if main_idx > 0:
            _append_total(self.rows_frame, self.rows_frame_r,
                          main_idx, main_invested, main_current, main_shares,
                          day_change=main_day_change, prev_total=main_prev_total)
        # 퇴직연금 합계 (우측 투자자 정보 없음)
        if pension_idx > 0:
            _append_total(self.pension_rows_frame, None,
                          pension_idx, pension_invested, pension_current, pension_shares,
                          day_change=pension_day_change, prev_total=pension_prev_total)

        # 이번 refresh 에서 사용되지 않은 row widgets 제거 (종목 수 감소 / 정렬 변동 대응)
        stale_keys = [k for k, e in getattr(self, "_row_cache", {}).items()
                      if e.get("_used") != self._refresh_tick]
        for k in stale_keys:
            e = self._row_cache.pop(k, None)
            if e and e.get("frame"):
                try:
                    e["frame"].destroy()
                except Exception:
                    pass

        self.last_refresh_time = datetime.now().strftime("%H:%M:%S")
        self._start_countdown()

        # 다음 갱신 예약 (기존 체인 취소 후 단일 예약)
        self._refresh_job = self.root.after(self.interval_ms, self.refresh)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    PortfolioWindow().run()
