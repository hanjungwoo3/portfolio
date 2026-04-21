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


def fetch_investor_flow(ticker: str) -> dict | None:
    """
    토스증권 공개 API에서 최근 일자 개인/외국인/기관 순매수 조회
    토스 앱과 완전히 동일한 숫자 (외국인 = 순수 외국인, 기타외국인은 개인에 포함)
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
        # 최신 데이터부터 확인, 3종 모두 0이면 그 전날 것 사용
        for item in body:
            indiv = int(item.get("netIndividualsBuyVolume", 0))
            foreign = int(item.get("netForeignerBuyVolume", 0))
            inst = int(item.get("netInstitutionBuyVolume", 0))
            if indiv != 0 or foreign != 0 or inst != 0:
                return {
                    "date": item.get("baseDate", ""),
                    "개인": indiv,
                    "외국인": foreign,
                    "기관": inst,
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
        return None
    except Exception as e:
        print(f"[WARN] 수급 조회 실패 {ticker}: {e}")
    return None


def format_signed(n: int) -> str:
    """+ 기호 + 콤마 포맷 (예: +48490 -> '+48,490')"""
    if n == 0:
        return "0"
    return f"{n:+,}"


def sign_color(n: float) -> str:
    """한국 증시 컨벤션 컬러"""
    if n > 0:
        return "#c0392b"  # 빨강 (상승/매수)
    if n < 0:
        return "#1f4e8f"  # 파랑 (하락/매도)
    return "#555"

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

    # 좌측: 한국 증시에 직접적 영향력 높은 지표
    # 우측: 간접 영향 / 보조 지표
    # (심볼, 이름, 설명, 선물심볼, 좌/우)
    pairs = [
        # --- 왼쪽 (핵심) ---
        ("KRW=X", "USD/KRW", "원달러 환율 — 외국인 수급·수출주 직결", None, "L"),
        ("EWY", "EWY", "MSCI Korea ETF — 외국인 투심 선행", None, "L"),
        ("^N225", "Nikkei 225", "일본 대표 지수 — 아시아 동조화", "NKD=F", "L"),
        ("^IXIC", "나스닥", "미국 기술주 중심 지수", "NQ=F", "L"),
        ("^GSPC", "S&P 500", "미국 대형주 500선", "ES=F", "L"),
        ("^DJI", "다우존스", "미국 산업 30대 기업", "YM=F", "L"),
        ("^VIX", "VIX", "S&P500 변동성 — 20↑ 경계, 30↑ 공포", None, "L"),
        ("KORU", "KORU", "MSCI Korea 3배 레버리지 ETF", None, "L"),
        ("^SOX", "필라델피아반도체", "미국 반도체 30개사 지수", "SOX=F", "L"),
        # --- 오른쪽 (보조) ---
        ("SOXX", "SOXX", "미국 반도체 30개사 가중 ETF", None, "R"),
        ("SOXL", "SOXL", "반도체 3배 레버리지 ETF", None, "R"),
        ("^HSI", "Hang Seng", "홍콩 증시 메인 지수", None, "R"),
        ("^TNX", "미국 10Y", "10년물 국채금리 — 성장주 벨류에이션", "ZN=F", "R"),
        ("GC=F", "Gold", "금 선물 — 위험 회피 시 상승", None, "R"),
        ("SI=F", "Silver", "은 선물 — 산업 수요 + 귀금속", None, "R"),
        ("HG=F", "Copper", "구리 — \"박사 구리\" 경기 선행지표", None, "R"),
        ("BTC-USD", "Bitcoin", "암호화폐 — 위험자산 심리", None, "R"),
        ("DX-Y.NYB", "달러인덱스", "USD 강도 — 신흥국 자금 흐름 영향", None, "R"),
        ("CL=F", "WTI 원유", "국제 유가 — 정유·에너지 직결", None, "R"),
        ("NG=F", "천연가스", "에너지 — 가스·유틸리티 영향", None, "R"),
    ]
    import math
    def _is_valid(v):
        return v is not None and not math.isnan(float(v)) and float(v) != 0

    def _fast_quote(symbol):
        """fast_info로 실시간 가격 + 전일 종가 조회. NaN/0 나오면 history 폴백"""
        try:
            fi = yf.Ticker(symbol).fast_info
            last = fi.last_price
            prev = fi.regular_market_previous_close
            if _is_valid(last) and _is_valid(prev):
                return float(last), float(prev)
        except Exception:
            pass
        # 폴백: history로 2일치
        try:
            h = yf.Ticker(symbol).history(period="5d", auto_adjust=False)
            if not h.empty and len(h) >= 2:
                c1 = float(h["Close"].iloc[-1])
                c2 = float(h["Close"].iloc[-2])
                if _is_valid(c1) and _is_valid(c2):
                    return c1, c2
        except Exception:
            pass
        return None, None

    out = []
    for cash, name, note, fut, side in pairs:
        close, prev = _fast_quote(cash)
        if close is None:
            continue
        pct = (close - prev) / prev * 100 if prev else 0

        fut_pct = None
        if fut:
            fclose, fprev = _fast_quote(fut)
            if fclose is not None and fprev:
                fut_pct = (fclose - fprev) / fprev * 100

        out.append({
            "name": name, "note": note, "price": close, "pct": pct,
            "fut_pct": fut_pct, "side": side,
        })
    return out


def fetch_nxt_support(ticker: str) -> bool:
    """토스 API로 NXT(시간외) 거래 지원 여부 조회"""
    try:
        url = f"https://wts-info-api.tossinvest.com/api/v1/stock-detail/ui/A{ticker}/common"
        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Origin": "https://tossinvest.com",
            "Referer": "https://tossinvest.com/",
        }, timeout=5)
        return bool(resp.json().get("result", {}).get("nxtSupported", False))
    except Exception:
        return False


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
    Returns: {ticker: {"price": int, "volume": int}, ...}
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
            if code and close is not None:
                result[code] = {
                    "price": int(close),
                    "volume": int(volume),
                    "base": int(base),  # 전일 종가
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
    # (key, 표시 이름, 픽셀 width, 정렬)
    COLS_FULL = [
        ("name", "종목", 90, "w"),
        ("day_chg", "전일대비", 115, "e"),
        ("volume", "거래량", 70, "e"),
        ("shares", "수량", 40, "e"),
        ("buy", "매수가", 65, "e"),
        ("cur", "현재가", 65, "e"),
        ("pnl", "손익%", 65, "e"),
        ("pnl_amt", "손익금액", 85, "e"),
        ("peak", "피크가", 65, "e"),
        ("from_peak", "피크대비", 65, "e"),
        ("foreign", "외국인 (보유%)", 140, "e"),
        ("inst", "기관", 75, "e"),
        ("pension", "연기금", 65, "e"),
        ("fin_inv", "금융투자", 75, "e"),
        ("trust", "투신", 65, "e"),
        ("pef", "사모", 65, "e"),
        ("insurance", "보험", 65, "e"),
        ("bank", "은행", 55, "e"),
        ("other_fin", "기타금융", 70, "e"),
        ("other_corp", "기타법인", 70, "e"),
    ]
    COLS_COMPACT = [
        ("name", "종목", 120, "w"),
        ("day_chg", "전일대비", 125, "e"),
        ("volume", "거래량", 75, "e"),
    ]

    # 외국인 이전까지는 왼쪽 고정, 이후는 오른쪽 스크롤
    FROZEN_KEYS = {"name", "day_chg", "volume", "shares", "buy", "cur",
                   "pnl", "pnl_amt", "peak", "from_peak"}

    @property
    def COLS(self):
        return self.COLS_COMPACT if self.compact_mode else self.COLS_FULL

    @property
    def COLS_LEFT(self):
        return [c for c in self.COLS if c[0] in self.FROZEN_KEYS]

    @property
    def COLS_RIGHT(self):
        return [c for c in self.COLS if c[0] not in self.FROZEN_KEYS]

    def __init__(self):
        self.cooldowns = {}
        self.investor_cache = {}  # {ticker: {"date": ..., "기관": ..., "외국인": ..., "개인": ...}}
        self.investor_cache_ts = 0  # 마지막 수급 조회 시각
        self.nxt_cache = {}  # {ticker: bool} NXT 지원 여부 (앱 시작 시 1회)
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
        self.root.attributes("-topmost", True)  # 항상 위
        self.root.attributes("-alpha", 1.0)     # 기본 불투명
        self.root.geometry("1080x480+50+50")

        # 티커 → row_id 매핑 (클릭 시 토스 페이지 이동용)
        self._row_ticker = {}

        self._build_ui()
        self.interval_ms = self.config.get("polling_interval_seconds", 5) * 1000

        # 앱 시작 시 역사적 피크 + NXT 지원 여부 + 미국 증시 동기화
        self._sync_historical_peaks()
        self._sync_nxt_support()
        self._refresh_us_indices_if_needed()
        self.refresh()
        # 초기 내용에 맞춰 크기 조정
        self.root.after(100, lambda: self._autosize_height(width=1400))

    def _build_ui(self):
        if getattr(self, "_ui_built", False):
            return
        self._ui_built = True

        self.title_label = None

        # 미국 증시 패널
        self.us_container = tk.Frame(self.root, bg="white")
        self.us_container.pack(fill=tk.X, padx=6, pady=(6, 4))

        us_title = tk.Label(
            self.us_container, text="🇺🇸 미국 증시 (실시간 · 선물)",
            font=("SF Pro", 10, "bold"), bg="white", fg="#333",
            anchor="w", padx=4,
        )
        us_title.pack(fill=tk.X)

        self.us_frame = tk.Frame(self.us_container, bg="white")
        self.us_frame.pack(fill=tk.X)

        # 공유 스크롤바 관리용
        self._canvases = []

        def _split_table(parent, title_text, show_scrollbar=False):
            """
            왼쪽(고정) + 오른쪽(가로스크롤) 2분할 테이블
            show_scrollbar=True 인 경우에만 스크롤바 생성 (마지막 테이블에만)
            returns (left_hdr, left_rows, right_hdr, right_rows, canvas)
            """
            outer = tk.Frame(parent, bg="white")
            outer.pack(fill=tk.X, padx=6, pady=(0, 6))

            tk.Label(
                outer, text=title_text,
                font=("SF Pro", 10, "bold"), bg="white", fg="#333",
                anchor="w", padx=4,
            ).pack(fill=tk.X)

            body = tk.Frame(outer, bg="white")
            body.pack(fill=tk.X)

            left_wrap = tk.Frame(body, bg="white")
            left_wrap.pack(side=tk.LEFT, fill=tk.Y)
            left_hdr = tk.Frame(left_wrap, bg="#e8e8e8")
            left_hdr.pack(fill=tk.X)
            left_rows = tk.Frame(left_wrap, bg="white")
            left_rows.pack(fill=tk.X)

            tk.Frame(body, width=2, bg="#ccc").pack(side=tk.LEFT, fill=tk.Y)

            right_wrap = tk.Frame(body, bg="white")
            right_wrap.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            canvas = tk.Canvas(right_wrap, bg="white", highlightthickness=0, height=1)
            canvas.pack(fill=tk.X, side=tk.TOP)

            inner = tk.Frame(canvas, bg="white")
            canvas.create_window((0, 0), window=inner, anchor="nw")
            right_hdr = tk.Frame(inner, bg="#e8e8e8")
            right_hdr.pack(fill=tk.X, anchor="w")
            right_rows = tk.Frame(inner, bg="white")
            right_rows.pack(fill=tk.X, anchor="w")

            def _on_resize(event=None):
                canvas.configure(scrollregion=canvas.bbox("all"))
                canvas.configure(height=inner.winfo_reqheight())
            inner.bind("<Configure>", _on_resize)

            self._canvases.append(canvas)
            return left_hdr, left_rows, right_hdr, right_rows

        # 메인 보유종목 테이블
        self.table_container = tk.Frame(self.root, bg="white")
        self.table_container.pack(fill=tk.X)
        (self.header_frame, self.rows_frame,
         self.header_frame_r, self.rows_frame_r) = _split_table(
            self.table_container, "💼 보유종목"
        )
        self._render_header()

        # 퇴직연금 테이블
        self.pension_container = tk.Frame(self.root, bg="white")
        self.pension_container.pack(fill=tk.X)
        (self.pension_header_frame, self.pension_rows_frame,
         self.pension_header_frame_r, self.pension_rows_frame_r) = _split_table(
            self.pension_container, "🏦 퇴직연금"
        )

        # 공유 스크롤바 (두 테이블 오른쪽 영역을 함께 스크롤)
        self.scroll_container = tk.Frame(self.root, bg="white")
        self.scroll_container.pack(fill=tk.X, padx=6, pady=(0, 6))
        # 왼쪽 고정영역 폭만큼 공간 비우기
        left_fixed_w = sum(max(5, c[2] // 8) * 8 for c in self.COLS_LEFT) + 80
        tk.Frame(self.scroll_container, bg="white", width=left_fixed_w).pack(side=tk.LEFT)
        self.shared_xbar = ttk.Scrollbar(
            self.scroll_container, orient="horizontal",
            command=self._sync_xview,
        )
        self.shared_xbar.pack(side=tk.LEFT, fill=tk.X, expand=True)
        # 각 캔버스의 xscrollcommand 설정
        for cv in self._canvases:
            cv.configure(xscrollcommand=self._on_canvas_scroll)

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

        self.topmost_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            bottom, text="항상 위",
            variable=self.topmost_var,
            command=self._toggle_topmost,
        ).pack(side=tk.LEFT, padx=6)

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

        ttk.Button(bottom, text="종료", command=self.root.quit).pack(side=tk.RIGHT)

    def _render_header(self):
        """헤더 재구성 — 왼쪽 고정 / 오른쪽 스크롤 각각"""
        def render_to(target, cols):
            if target is None:
                return
            for w in target.winfo_children():
                w.destroy()
            for idx, (key, title, width, align) in enumerate(cols):
                anchor = {"w": "w", "e": "e", "center": "center"}[align]
                tk.Label(
                    target, text=title, width=max(5, width // 8),
                    font=("SF Pro", 9, "bold"), bg="#e8e8e8", fg="#222",
                    anchor=anchor, padx=2, pady=2,
                ).grid(row=0, column=idx, sticky="nsew")

        # 일반
        render_to(self.header_frame, self.COLS_LEFT)
        render_to(getattr(self, "header_frame_r", None), self.COLS_RIGHT)
        # 퇴직연금
        render_to(getattr(self, "pension_header_frame", None), self.COLS_LEFT)
        render_to(getattr(self, "pension_header_frame_r", None), self.COLS_RIGHT)

    def _toggle_topmost(self):
        self.root.attributes("-topmost", self.topmost_var.get())

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
            width = 1400
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

    def _toggle_us_panel(self):
        if self.us_visible_var.get():
            self.us_container.pack(fill=tk.X, padx=6, pady=(6, 4),
                                   before=self.table_container)
            self._render_us_indices()
        else:
            self.us_container.pack_forget()
        # 내용 크기에 맞춰 창 높이 자동 조정
        self.root.after(10, self._autosize_height)

    def _on_row_click(self, ticker: str):
        pass

    def _on_row_double_click(self, ticker: str):
        url = f"https://tossinvest.com/stocks/A{ticker}"
        self._open_toss_in_existing_tab(url)

    def _open_toss_in_existing_tab(self, url: str):
        """기존 Chrome/Safari의 tossinvest.com 탭이 있으면 그 탭 URL 교체, 없으면 새로"""
        # Chrome 시도
        chrome_script = f'''
tell application "Google Chrome"
    set foundTab to false
    repeat with w in windows
        set tabIdx to 0
        repeat with t in tabs of w
            set tabIdx to tabIdx + 1
            if URL of t contains "tossinvest.com" then
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
        # Safari 시도 (fallback)
        safari_script = f'''
tell application "Safari"
    set foundTab to false
    repeat with w in windows
        set tabIdx to 0
        repeat with t in tabs of w
            set tabIdx to tabIdx + 1
            if URL of t contains "tossinvest.com" then
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
        """미국 증시 패널 렌더 — 좌/우 2컬럼 분할"""
        for w in self.us_frame.winfo_children():
            w.destroy()
        if not self.us_indices:
            tk.Label(self.us_frame, text="로딩 중...", bg="white", fg="#999",
                     font=("SF Pro", 9)).pack(anchor="w", padx=4)
            return

        def build_col(parent, indices, title_suffix=""):
            # 헤더
            hdr = tk.Frame(parent, bg="#e8e8e8")
            hdr.pack(fill=tk.X)
            for col_idx, title in enumerate(["지표", "종가", "등락률", "선물", "설명"]):
                w = [10, 9, 7, 7, 28][col_idx]
                anchor = "w" if col_idx in (0, 4) else "e"
                tk.Label(hdr, text=title, width=w, font=("SF Pro", 9, "bold"),
                         bg="#e8e8e8", fg="#222", anchor=anchor,
                         padx=3, pady=2).grid(row=0, column=col_idx, sticky="nsew")
            # 각 행
            for idx in indices:
                row = tk.Frame(parent, bg="white")
                row.pack(fill=tk.X)
                pct_color = sign_color(idx["pct"])
                fut_color = sign_color(idx["fut_pct"]) if idx.get("fut_pct") is not None else "#999"
                fut_txt = f"{idx['fut_pct']:+.2f}%" if idx.get("fut_pct") is not None else "-"

                tk.Label(row, text=idx["name"], width=10, font=("SF Mono", 10),
                         bg="white", fg="#222", anchor="w", padx=3, pady=1).grid(row=0, column=0, sticky="nsew")
                tk.Label(row, text=f"{idx['price']:,.2f}", width=9, font=("SF Mono", 10),
                         bg="white", fg="#222", anchor="e", padx=3, pady=1).grid(row=0, column=1, sticky="nsew")
                tk.Label(row, text=f"{idx['pct']:+.2f}%", width=7, font=("SF Mono", 10),
                         bg="white", fg=pct_color, anchor="e", padx=3, pady=1).grid(row=0, column=2, sticky="nsew")
                tk.Label(row, text=fut_txt, width=7, font=("SF Mono", 10),
                         bg="white", fg=fut_color, anchor="e", padx=3, pady=1).grid(row=0, column=3, sticky="nsew")
                # 설명 (작은 회색 글씨)
                tk.Label(row, text=idx.get("note", ""), width=28, font=("SF Pro", 9),
                         bg="white", fg="#777", anchor="w",
                         padx=3, pady=1).grid(row=0, column=4, sticky="nsew")

        # 좌/우 분할 컨테이너 (grid + sticky="n"으로 상단 정렬)
        cols_wrap = tk.Frame(self.us_frame, bg="white")
        cols_wrap.pack(fill=tk.X)
        cols_wrap.grid_columnconfigure(0, weight=1, uniform="col")
        cols_wrap.grid_columnconfigure(2, weight=1, uniform="col")

        left_col = tk.Frame(cols_wrap, bg="white")
        left_col.grid(row=0, column=0, sticky="new", padx=(0, 4))

        separator = tk.Frame(cols_wrap, width=1, bg="#ddd")
        separator.grid(row=0, column=1, sticky="ns")

        right_col = tk.Frame(cols_wrap, bg="white")
        right_col.grid(row=0, column=2, sticky="new", padx=(4, 0))

        left = [x for x in self.us_indices if x.get("side") == "L"]
        right = [x for x in self.us_indices if x.get("side") == "R"]
        build_col(left_col, left)
        build_col(right_col, right)

    def _refresh_us_indices_if_needed(self):
        """미국 증시: 2분마다 실시간 갱신 (장 열림 시)"""
        import time as _t
        now = _t.time()
        if now - self.us_indices_ts < 120 and self.us_indices:
            return
        self.us_indices = fetch_us_indices_with_futures()
        self.us_indices_ts = now
        if hasattr(self, "us_frame") and self.us_visible_var.get():
            self._render_us_indices()

    def _sync_nxt_support(self):
        """각 종목의 NXT(시간외) 거래 지원 여부 조회 (앱 시작 시 1회)"""
        for stock in self.holdings:
            ticker = stock["ticker"]
            if ticker not in self.nxt_cache:
                self.nxt_cache[ticker] = fetch_nxt_support(ticker)

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
        """수급 데이터는 일일 단위라 10분에 1회만 갱신"""
        import time as _t
        now = _t.time()
        if now - self.investor_cache_ts < 600 and self.investor_cache:
            return
        tickers = [s["ticker"] for s in self.holdings]
        with ThreadPoolExecutor(max_workers=min(len(tickers), 8)) as pool:
            results = pool.map(fetch_investor_flow, tickers)
            for t, r in zip(tickers, results):
                if r:
                    self.investor_cache[t] = r
        self.investor_cache_ts = now

    def _make_row(self, row_idx: int, ticker: str, cells: list,
                  row_bg: str = "white", parent=None, parent_r=None):
        """
        한 행을 왼쪽(고정) + 오른쪽(스크롤) 프레임에 나누어 생성
        parent / parent_r: 좌/우 부모 프레임
        """
        left_target = parent if parent is not None else self.rows_frame
        right_target = parent_r if parent_r is not None else getattr(self, "rows_frame_r", None)

        left_keys = [c[0] for c in self.COLS_LEFT]
        right_keys = [c[0] for c in self.COLS_RIGHT]
        all_keys = [c[0] for c in self.COLS]
        cell_by_key = dict(zip(all_keys, cells))

        def _paint(target, keys, cols_info):
            if target is None:
                return
            frame = tk.Frame(target, bg=row_bg)
            frame.grid(row=row_idx, column=0, sticky="ew")
            target.grid_columnconfigure(0, weight=1)
            for col_idx, ((key, title, width, align)) in enumerate(cols_info):
                cell = cell_by_key.get(key, ("", "#555"))
                text = cell[0]
                fg = cell[1]
                cell_bg = cell[2] if len(cell) > 2 and cell[2] else row_bg
                cell_bold = cell[3] if len(cell) > 3 else False
                anchor = {"w": "w", "e": "e", "center": "center"}[align]
                font_style = ("SF Mono", 10, "bold") if cell_bold else ("SF Mono", 10)
                lbl = tk.Label(
                    frame, text=text, width=max(5, width // 8),
                    font=font_style, bg=cell_bg, fg=fg,
                    anchor=anchor, padx=2, pady=1,
                )
                lbl.grid(row=0, column=col_idx, sticky="nsew")
                lbl.bind("<Button-1>", lambda e, t=ticker: self._on_row_click(t))
                lbl.bind("<Double-1>", lambda e, t=ticker: self._on_row_double_click(t))

        _paint(left_target, left_keys, self.COLS_LEFT)
        _paint(right_target, right_keys, self.COLS_RIGHT)

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

        # 수급 캐시 갱신 (필요 시)
        self._refresh_investor_cache_if_needed()

        # 미국 증시 실시간 갱신 (2분마다, 표시 중일 때만)
        if hasattr(self, "us_visible_var") and self.us_visible_var.get():
            self._refresh_us_indices_if_needed()

        # 기존 행 제거 (일반 + 퇴직연금, 좌/우 모두)
        for fr_name in ["rows_frame", "rows_frame_r",
                        "pension_rows_frame", "pension_rows_frame_r"]:
            fr = getattr(self, fr_name, None)
            if fr is not None:
                for w in fr.winfo_children():
                    w.destroy()
        if hasattr(self, "_row_ticker"):
            self._row_ticker.clear()

        if not self.holdings:
            if hasattr(self, "time_label"):
                self.time_label.config(text="보유 종목 없음")
            self._refresh_job = self.root.after(self.interval_ms, self.refresh)
            return

        # 토스 API로 batch 시세 + 거래량 조회
        tickers = [s["ticker"] for s in self.holdings]
        toss_data = fetch_toss_prices_batch(tickers)
        prices = {t: d["price"] for t, d in toss_data.items()}
        volumes = {t: d["volume"] for t, d in toss_data.items()}
        bases = {t: d["base"] for t, d in toss_data.items()}  # 전일 종가
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

        # 일반/퇴직연금 분리 — 테이블 별 인덱스 및 합계
        main_idx = pension_idx = 0
        main_invested = main_current = 0
        main_shares = pension_shares = 0
        pension_invested = pension_current = 0

        for stock in self.holdings:
            is_pension = stock.get("account") == "퇴직연금"
            parent = self.pension_rows_frame if is_pension else self.rows_frame
            parent_r = self.pension_rows_frame_r if is_pension else self.rows_frame_r
            if is_pension:
                row_idx = pension_idx
                pension_idx += 1
            else:
                row_idx = main_idx
                main_idx += 1
            ticker = stock["ticker"]
            name = stock["name"]
            shares = stock["shares"]
            buy_price = stock["avg_price"]
            current_price = prices.get(ticker)

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
            # 외국인: 순매수량 + 보유율(%)
            if flow:
                foreign_ratio = flow.get("외국인비율", 0)
                foreign_text = format_signed(foreign)
                if foreign_ratio > 0:
                    foreign_text = f"{format_signed(foreign)} ({foreign_ratio:.2f}%)"
                foreign_cell = (foreign_text, sign_color(foreign))
            else:
                foreign_cell = ("-", "#999")
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
                    "day_chg": ("-", "#999"),
                    "shares": (str(shares), "#333"),
                    "buy": (f"{buy_price:,}", "#333"),
                    "cur": ("-", "#999"),
                    "pnl": ("-", "#999"),
                    "peak": ("-", "#999"),
                    "from_peak": ("-", "#999"),
                    "pnl_amt": ("-", "#999"),
                    "volume": ("-", "#999"),
                    "foreign": foreign_cell,
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
                cells = [fail_map[c[0]] for c in self.COLS]
                self._make_row(row_idx, ticker, cells, row_bg="white", parent=parent, parent_r=parent_r)
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
            pnl_pct = (net_price - buy_price) / buy_price * 100
            from_peak_pct = (current_price - peak_price) / peak_price * 100 if peak_price else 0
            stock_current = round(net_price * shares)
            total_current += stock_current
            if is_pension:
                pension_current += stock_current
            else:
                main_current += stock_current

            triggered = self._check_alert(stock, current_price, peak_price, pnl_pct, from_peak_pct)

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

            # 시간외(NXT) 거래 가능하면 이름 뒤에 작은 시계 아이콘
            name_display = f"{name} 🕐" if self.nxt_cache.get(ticker) else name

            # 종목명 뱃지 — 손절/하락은 파랑, 익절은 빨강
            name_cell = (name_display, name_color)
            if is_stop:
                name_cell = (name_display, "white", "#1f4e8f", True)
            elif is_peak_drop and pnl_pct > 0:
                name_cell = (name_display, "white", "#c0392b", True)  # 익절 → 빨강
            elif is_peak_drop:
                name_cell = (name_display, "white", "#4a90c2", True)  # 하락 → 파랑

            # 현재가 / 손익% — 셀 분리 (각자 우측 정렬)
            cur_cell = (f"{current_price:,}", sign_color(pnl_pct))
            pnl_cell = (f"{pnl_pct:+.2f}%", sign_color(pnl_pct))

            # 피크가 / 피크대비 — 실제 고점이 있어야만 표시
            # (현재가 == 피크가이거나, 피크가 <= 매수가면 의미 없음)
            if peak_price == current_price or peak_price <= buy_price:
                peak_main_cell = ("", "#222")
                from_peak_cell = ("", "#222")
            else:
                peak_main_cell = (f"{peak_price:,}", "#c0392b")
                from_peak_cell = (f"{from_peak_pct:+.2f}%", sign_color(from_peak_pct))

            # 위험 뱃지: % 셀에만 적용 (금액은 색상만)
            if is_stop:
                pnl_cell = (f"{pnl_pct:+.2f}%", "white", "#1f4e8f", True)
            if is_peak_drop and peak_price > buy_price and peak_price != current_price:
                # 수익 중이면 익절 경고(빨강), 손실 중이면 추가 하락(파랑)
                peak_bg = "#c0392b" if pnl_pct > 0 else "#4a90c2"
                from_peak_cell = (f"{from_peak_pct:+.2f}%", "white", peak_bg, True)

            volume = volumes.get(ticker, 0)
            base_price = bases.get(ticker, 0)
            volume_cell = (format_volume(volume), "#555")

            # 전일 종가 대비 가격 변동: +350 (+1.31%)
            if base_price > 0:
                price_diff = current_price - base_price
                price_pct = (price_diff / base_price) * 100
                diff_text = f"{price_diff:+,} ({price_pct:+.2f}%)" if price_diff != 0 else "0"
                vol_diff_cell = (diff_text, sign_color(price_diff))
            else:
                vol_diff_cell = ("-", "#999")

            # 전체 키-셀 매핑
            cell_by_key = {
                "name": name_cell,
                "day_chg": vol_diff_cell,
                "shares": (str(shares), "#222"),
                "buy": (f"{buy_price:,}", "#222"),
                "cur": cur_cell,
                "pnl": pnl_cell,
                "peak": peak_main_cell,
                "from_peak": from_peak_cell,
                "pnl_amt": (format_signed(pnl_amount) if pnl_amount else "0", sign_color(pnl_amount)),
                "volume": volume_cell,
                "foreign": foreign_cell,
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
            # 현재 COLS 순서대로 배열
            cells = [cell_by_key[c[0]] for c in self.COLS]
            self._make_row(row_idx, ticker, cells, row_bg=row_bg, parent=parent, parent_r=parent_r)

        save_json(PEAKS_PATH, self.peaks)

        def _append_total(parent_frame_left, parent_frame_right, row_idx, invested, current, shares_sum):
            pnl = current - invested
            tm = {
                "name": ("합계", "#222"),
                "day_chg": ("", "#555"),
                "shares": (f"{shares_sum}주", "#222"),
                "buy": (f"{invested:,}", "#222"),
                "cur": (f"{current:,}", sign_color(pnl)),
                "pnl": ("", "#555"),
                "peak": ("", "#555"),
                "from_peak": ("", "#555"),
                "pnl_amt": (format_signed(pnl) if pnl else "0", sign_color(pnl)),
                "volume": ("", "#555"),
                "foreign": ("", "#555"),
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
            def _paint(target, cols):
                if target is None:
                    return
                tr = tk.Frame(target, bg="#f5f5f5")
                tr.grid(row=row_idx, column=0, sticky="ew")
                for col_idx, (key, title, width, align) in enumerate(cols):
                    text, fg = tm[key]
                    anchor = {"w": "w", "e": "e", "center": "center"}[align]
                    tk.Label(
                        tr, text=text, width=max(5, width // 8),
                        font=("SF Mono", 10, "bold"), bg="#f5f5f5", fg=fg,
                        anchor=anchor, padx=2, pady=2,
                    ).grid(row=0, column=col_idx, sticky="nsew")
            _paint(parent_frame_left, self.COLS_LEFT)
            _paint(parent_frame_right, self.COLS_RIGHT)

        # 일반 합계
        if main_idx > 0:
            _append_total(self.rows_frame, self.rows_frame_r,
                          main_idx, main_invested, main_current, main_shares)
        # 퇴직연금 합계
        if pension_idx > 0:
            _append_total(self.pension_rows_frame, self.pension_rows_frame_r,
                          pension_idx, pension_invested, pension_current, pension_shares)

        self.last_refresh_time = datetime.now().strftime("%H:%M:%S")
        self._start_countdown()

        # 다음 갱신 예약 (기존 체인 취소 후 단일 예약)
        self._refresh_job = self.root.after(self.interval_ms, self.refresh)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    PortfolioWindow().run()
