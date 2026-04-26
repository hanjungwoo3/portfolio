#!/usr/bin/env python3
"""
포트폴리오 플로팅 윈도우 v2 — Canvas 기반 카드 테이블

설계 원칙:
- 종목당 위젯 트리를 만들지 않고 Canvas 1개에 직접 텍스트/사각형을 그림.
- 갱신은 canvas.itemconfig() 로 in-place. destroy/create 없음.
- 보이지 않는 탭은 dirty 마킹만, 탭 전환 시점에 그림.

3 탭: 미국 증시 / 보유 종목 / 관심 종목
데이터 fetcher / 상수 / 알림 로직은 v1 (portfolio_window.py) 재사용.

Usage:
    python3 portfolio_window_v2.py
"""
import os
import sys
import signal
import subprocess
import threading
import webbrowser
import tkinter as tk
from tkinter import ttk
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

import requests
# v1 의 데이터 함수 + 상수 재사용
from portfolio_window import (  # noqa: E402
    USER_AGENT,
    HOLDINGS_PATH, CONFIG_PATH, PEAKS_PATH, ALERTS_DIR,
    load_json, save_json,
    sign_color, format_signed, format_volume,
    fetch_toss_prices_batch, fetch_investor_flow,
    fetch_stock_warning, fetch_stock_sector,
    fetch_target_consensus, fetch_us_indices_with_futures,
    fetch_peak_since_buy,
    FUT_FULL_NAME, resolve_us_indicator_url,
    is_market_open, market_of_symbol,
)


def _fetch_stock_name(code: str) -> str:
    """종목명 조회 — Toss → 네이버 폴백 (v1 로직 이식)"""
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
        from bs4 import BeautifulSoup
        resp = requests.get(f"https://finance.naver.com/item/main.naver?code={code}",
                              headers={"User-Agent": USER_AGENT}, timeout=5)
        soup = BeautifulSoup(resp.text, "html.parser")
        node = soup.select_one("div.wrap_company h2 a")
        if node:
            return node.get_text(strip=True)
    except Exception:
        pass
    return ""


# ─────────────────────────── 색상 ───────────────────────────
COL_BG_APP = "#f4f5f7"
COL_BG_CARD = "#ffffff"
COL_BG_PROFIT = "#fbe6e6"
COL_BG_LOSS = "#dde6f3"
COL_BG_RIGHT_PANEL = "#fafbfc"
COL_BORDER = "#e3e6ea"
COL_DIVIDER = "#e0e3e8"

COL_PRIMARY = "#1f2933"
COL_SECONDARY = "#6b7280"
COL_MUTED = "#9ca3af"

COL_PILL_DEFAULT = "#FEE500"
COL_PILL_PENSION = "#ecf0f3"
COL_PILL_STOP = "#1f4e8f"
COL_PILL_PEAK_PROFIT = "#c0392b"
COL_PILL_PEAK_LOSS = "#4a90c2"
COL_PILL_DANGER = "#c0392b"
COL_PILL_WARN = "#e67e22"
COL_PILL_HALT = "#e8e8e8"
COL_PILL_CAUTION = "#fff3cc"


# ─────────────────────────── 폰트 ───────────────────────────
def _font(size=12, weight="normal"):
    fam = "AppleSDGothicNeoSB00" if sys.platform == "darwin" else "TkDefaultFont"
    return (fam, size, weight)


# ─────────────────────────── 카드 레이아웃 상수 (Canvas 좌표) ───────────────────────────
CARD_PAD_X = 12
CARD_PAD_Y = 10
CARD_HEIGHT = 142   # 카드 1장 높이 (좌측 5줄 + 우측 6줄 모두 수용)
CARD_GAP = 6
LEFT_RATIO = 0.55   # 좌측 55% (우측 그리드 여유 확보)
LINE_HEIGHT = 22


def _name_pill_colors(account, warn_text, pnl_pct, is_stop, is_peak_drop, day_diff=0):
    # is_peak_drop 시각 강조는 종목명 pill 이 아니라 peak 텍스트 배경에서 처리
    if is_stop:
        return ("#ffffff", COL_PILL_STOP)
    if warn_text in ("위험", "관리"):
        return ("#ffffff", COL_PILL_DANGER)
    if warn_text in ("경고", "과열"):
        return ("#ffffff", COL_PILL_WARN)
    if warn_text == "정지":
        return ("#444444", COL_PILL_HALT)
    # 기본 가지들: 종목명 색을 현재가 색(어제보다 부호)과 동일하게
    name_fg = sign_color(day_diff) if day_diff else COL_PRIMARY
    if warn_text == "주의":
        return (name_fg, COL_PILL_CAUTION)
    if account == "퇴직연금":
        return (name_fg, COL_PILL_PENSION)
    return (name_fg, COL_PILL_DEFAULT)


def _amount_color(amount):
    if amount is None or amount == 0:
        return COL_MUTED
    return sign_color(amount)


def _format_signed_or_zero(amount):
    if amount is None:
        return "—"
    return format_signed(amount)


def _badge_bg_for(warn_text, is_stop):
    if is_stop:
        return "손절", COL_PILL_STOP
    if not warn_text:
        return "", None
    bg = {
        "위험": "#c0392b", "관리": "#c0392b",
        "경고": "#e67e22", "과열": "#e67e22",
        "정지": "#888", "주의": "#caa400",
    }.get(warn_text, "#888")
    return warn_text, bg


# ─────────────────────────── 토스 페이지 (v1 로직) ───────────────────────────
def open_toss_in_existing_tab(url: str):
    from urllib.parse import urlparse
    host = urlparse(url).hostname or ""
    chrome_script = f'''
tell application "Google Chrome"
    repeat with w in windows
        set tabIdx to 0
        repeat with t in tabs of w
            set tabIdx to tabIdx + 1
            if URL of t contains "{host}" then
                set URL of t to "{url}"
                set active tab index of w to tabIdx
                set index of w to 1
                activate
                return "OK"
            end if
        end repeat
    end repeat
    open location "{url}"
    activate
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
        webbrowser.open(url)
    except Exception:
        pass


# ─────────────────────────── 카드 캔버스 (보유/관심) ───────────────────────────
class StockCardsCanvas:
    """1개 Canvas 에 모든 종목 카드를 그린다.
    카드별 item id 를 사전에 저장해두고, refresh 때 itemconfig 로 텍스트/색만 갱신.
    """

    RIGHT_FIELDS = [
        # (label, key) — 12 항목 (외국인 보유율 + 11 분류)
        # 6행 × 2열 그리드로 좌우 2개씩 표시
        ("외국인보유", "외국인비율"),  # %, primary 색
        ("개인", "개인"),
        ("외국인", "외국인"), ("기관", "기관"),
        ("연기금", "연기금"), ("금융투자", "금융투자"),
        ("투신", "투신"), ("사모", "사모"),
        ("보험", "보험"), ("은행", "은행"),
        ("기타금융", "기타금융"), ("기타법인", "기타법인"),
    ]
    # 셀 배경 + 라벨 색으로 강조 (+빨강 / −파랑)
    HIGHLIGHT_KEYS = ("외국인", "기관", "연기금")
    # 큰 볼드 폰트로 한 단계 더 강조 (가장 중요한 두 항목)
    FONT_BUMP_KEYS = ("외국인", "기관")

    def __init__(self, parent, watchlist=False, on_click=None, on_right_click=None):
        self.parent = parent
        self.watchlist = watchlist
        self.on_click = on_click or (lambda t: None)
        self.on_right_click = on_right_click or (lambda t, e: None)

        self.frame = tk.Frame(parent, bg=COL_BG_APP)
        self.frame.pack(fill="both", expand=True)

        # 합계 영역 (스크롤 밖, 보유만)
        self.total_canvas = tk.Canvas(self.frame, height=70,
                                        bg=COL_BG_APP, highlightthickness=0)
        if not watchlist:
            self.total_canvas.pack(side="bottom", fill="x")

        # 카드 영역 (스크롤)
        self.canvas = tk.Canvas(self.frame, bg=COL_BG_APP, highlightthickness=0,
                                  yscrollincrement=10)
        self.vsb = ttk.Scrollbar(self.frame, orient="vertical",
                                  command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)
        self.vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.cards = {}    # ticker -> {item id 들}
        self.order = []    # 현재 그려진 순서
        # grouped 모드 상태 (render_grouped 사용 시) — _reflow 가 사용
        self._grouped_pos_map = None       # ticker -> idx
        self._grouped_y_offset_map = None  # ticker -> y 픽셀 오프셋 (그룹 갭)
        self._grouped_groups = None        # totals 재렌더용
        self._grouped_total_idx = 0        # scrollregion 계산용
        self._grouped_max_y_offset = 0     # scrollregion 계산용
        self.canvas.bind("<Configure>", self._on_resize)
        self.canvas.bind("<Enter>", lambda e: self._bind_wheel())
        self.canvas.bind("<Leave>", lambda e: self._unbind_wheel())
        self._width = 0

    # ───────── 스크롤 휠 (macOS 트랙패드/마우스 모두 부드럽게) ─────────
    def _bind_wheel(self):
        self.canvas.bind_all("<MouseWheel>", self._on_wheel)

    def _unbind_wheel(self):
        self.canvas.unbind_all("<MouseWheel>")

    def _on_wheel(self, e):
        # 트랙패드: e.delta = ±1~5 (작음, 그대로 사용)
        # 외장 마우스: e.delta = ±120 (정규화 필요)
        d = e.delta
        if d == 0:
            return
        if abs(d) >= 30:
            step = int(-d / 30)
        else:
            step = -d
        if step == 0:
            step = -1 if d > 0 else 1
        self.canvas.yview_scroll(step, "units")

    def _on_resize(self, event):
        new_w = event.width
        if new_w != self._width:
            self._width = new_w
            self._reflow()

    # ───────── 카드 그리기 ─────────
    def _create_card(self, idx, ticker):
        """카드 1장의 모든 item 을 미리 만든다 (텍스트는 비어 있음).
        좌표는 _reflow() 가 결정한다.
        """
        c = self.canvas
        # 사각형들
        bg_id = c.create_rectangle(0, 0, 0, 0, fill=COL_BG_CARD,
                                     outline=COL_BORDER, width=1,
                                     tags=(f"card:{ticker}", "card"))
        right_bg_id = c.create_rectangle(0, 0, 0, 0, fill=COL_BG_RIGHT_PANEL,
                                           outline=COL_DIVIDER, width=1,
                                           tags=(f"card:{ticker}",))
        pill_bg_id = c.create_rectangle(0, 0, 0, 0, fill=COL_PILL_DEFAULT,
                                          outline="",
                                          tags=(f"card:{ticker}",))
        badge_bg_id = c.create_rectangle(0, 0, 0, 0, fill="", outline="",
                                            tags=(f"card:{ticker}",))

        # 텍스트들
        def t(font_=None, fill=COL_PRIMARY, anchor="nw"):
            return c.create_text(0, 0, text="", font=font_ or _font(12),
                                  fill=fill, anchor=anchor,
                                  tags=(f"card:{ticker}",))

        # peak_bg: peak_text 보다 먼저 생성되어야 z-order 가 텍스트 아래
        peak_bg_id = c.create_rectangle(0, 0, 0, 0, fill="", outline="",
                                          tags=(f"card:{ticker}",))

        ids = {
            "bg": bg_id, "right_bg": right_bg_id,
            "pill_bg": pill_bg_id, "pill_text": t(_font(13, "bold"), "#000"),
            "badge_bg": badge_bg_id, "badge_text": t(_font(10, "bold"), "#fff", "center"),
            "sector_text": t(_font(10), COL_MUTED),

            "price_text": t(_font(17, "bold"), COL_PRIMARY),
            "vol_text": t(_font(10), COL_MUTED),
            "peak_bg": peak_bg_id,
            "peak_text": t(_font(10, "bold"), COL_MUTED),

            "day_label": t(_font(11), COL_SECONDARY),
            "day_value": t(_font(11, "bold"), COL_PRIMARY),

            "pnl_label": t(_font(11), COL_SECONDARY),
            "pnl_value": t(_font(11, "bold"), COL_PRIMARY),

            "target_label": t(_font(11), COL_SECONDARY),
            "target_value": t(_font(11), COL_PRIMARY),
            "target_gap": t(_font(11, "bold"), COL_PRIMARY),

            "date_text": t(_font(9), COL_MUTED, "ne"),
        }

        # 강조 셀 배경 (텍스트보다 먼저 생성 → z-order: 텍스트 아래)
        for hk in self.HIGHLIGHT_KEYS:
            ids[f"flow_bg_{hk}"] = c.create_rectangle(
                0, 0, 0, 0, fill="", outline="",
                tags=(f"card:{ticker}",))

        # 11 항목 그리드 — FONT_BUMP_KEYS 만 폰트 키우고 볼드
        for label, key in self.RIGHT_FIELDS:
            if key in self.FONT_BUMP_KEYS:
                ids[f"flow_label_{key}"] = t(_font(12, "bold"), COL_SECONDARY)
                ids[f"flow_value_{key}"] = t(_font(12, "bold"), COL_MUTED, "ne")
            else:
                ids[f"flow_label_{key}"] = t(_font(10), COL_SECONDARY)
                ids[f"flow_value_{key}"] = t(_font(10), COL_MUTED, "ne")

        # 카드 영역에 클릭 이벤트
        c.tag_bind(f"card:{ticker}", "<Button-1>",
                    lambda e, t=ticker: self.on_click(t))
        c.tag_bind(f"card:{ticker}", "<Button-2>",
                    lambda e, t=ticker: self.on_right_click(t, e))
        c.tag_bind(f"card:{ticker}", "<Button-3>",
                    lambda e, t=ticker: self.on_right_click(t, e))

        return ids

    def _position_card(self, ids, idx, width, y_offset=0):
        """카드 좌표 배치 — 한 줄에 2개씩 (좌/우 50%) ."""
        c = self.canvas
        col = idx % 2
        row = idx // 2
        side_pad = 6
        mid_gap = 6
        half_w = max(280, (width - side_pad * 2 - mid_gap) // 2)
        x0 = side_pad + col * (half_w + mid_gap)
        x1 = x0 + half_w
        y0 = side_pad + row * (CARD_HEIGHT + CARD_GAP) + y_offset
        y1 = y0 + CARD_HEIGHT
        c.coords(ids["bg"], x0, y0, x1, y1)

        # 좌/우 분할
        split_x = int(x0 + (x1 - x0) * LEFT_RATIO)
        rx0 = split_x + 4
        rx1 = x1 - 6
        ry0 = y0 + 6
        ry1 = y1 - 6
        c.coords(ids["right_bg"], rx0, ry0, rx1, ry1)

        # 좌측 시작 좌표
        lx = x0 + CARD_PAD_X
        ly = y0 + CARD_PAD_Y

        # Line 1: pill (텍스트는 후에 갱신, 좌표만 잡음)
        c.coords(ids["pill_bg"], lx, ly, lx + 8, ly + 22)  # 임시 폭
        c.coords(ids["pill_text"], lx + 6, ly + 11)
        c.itemconfig(ids["pill_text"], anchor="w")
        # badge — pill 뒤에 위치, 폭은 텍스트 갱신 후 _measure_and_layout 에서 조정
        c.coords(ids["badge_bg"], lx, ly, lx, ly)
        c.coords(ids["badge_text"], lx, ly + 11)
        c.coords(ids["sector_text"], lx, ly + 11)
        c.itemconfig(ids["sector_text"], anchor="w")

        # Line 2: price + vol + peak
        ly2 = ly + LINE_HEIGHT + 4
        c.coords(ids["price_text"], lx, ly2)
        c.coords(ids["vol_text"], lx + 100, ly2 + 6)
        c.coords(ids["peak_text"], lx + 180, ly2 + 6)  # vol_text 우측, _update 에서 동적 재배치

        # Line 3: 어제보다
        ly3 = ly2 + LINE_HEIGHT + 6
        c.coords(ids["day_label"], lx, ly3)
        c.coords(ids["day_value"], lx + 60, ly3)

        # Line 4: 전체수익 (보유만, 관심에선 빈 텍스트)
        ly4 = ly3 + LINE_HEIGHT
        c.coords(ids["pnl_label"], lx, ly4)
        c.coords(ids["pnl_value"], lx + 60, ly4)

        # Line 5: 목표 — target_gap 위치는 update 단계에서 target_value bbox 측정 후 동적 배치
        ly5 = ly4 + LINE_HEIGHT
        c.coords(ids["target_label"], lx, ly5)
        c.coords(ids["target_value"], lx + 36, ly5)
        c.coords(ids["target_gap"], lx + 200, ly5)  # 임시, update 에서 재배치

        # 우측: 12 항목 그리드 (6행 × 2열) — 외국인 보유율을 첫 칸으로 통합
        rx_l = rx0 + 10
        rx_r = rx1 - 10
        # 날짜는 표시하지 않음 — 화면 밖으로 보내 잔상 방지
        c.coords(ids["date_text"], -1000, -1000)

        # 12 항목: 2 컬럼, 6 행 모두 채움
        col_w = (rx_r - rx_l) // 2
        ry_grid = ry0 + 4  # 날짜 줄 제거로 위로 올림
        for i, (label, key) in enumerate(self.RIGHT_FIELDS):
            r = i // 2
            col = i % 2
            x_left = rx_l + col * col_w
            x_right = x_left + col_w - 8
            yy = ry_grid + r * (LINE_HEIGHT - 3)
            c.coords(ids[f"flow_label_{key}"], x_left, yy)
            c.coords(ids[f"flow_value_{key}"], x_right, yy)
            if key in self.HIGHLIGHT_KEYS:
                c.coords(ids[f"flow_bg_{key}"],
                         x_left - 2, yy - 1,
                         x_right + 2, yy + (LINE_HEIGHT - 4))

    def _update_card_values(self, ids, stock, price_data, peak_price,
                             thresholds, caches):
        c = self.canvas
        ticker = stock["ticker"]
        name = stock.get("name", ticker)
        avg = stock.get("avg_price", 0)
        shares = stock.get("shares", 0)
        account = stock.get("account") or ""

        cur_price = price_data.get("price", 0) if price_data else 0
        base_price = price_data.get("base", 0) if price_data else 0
        volume = price_data.get("volume", 0) if price_data else 0

        sell_fee_pct = caches.get("sell_fee_pct", 0.2)
        fee_mul = 1 - (sell_fee_pct / 100)
        net_price = cur_price * fee_mul
        pnl = round((net_price - avg) * shares) if (avg and shares) else 0
        pnl_pct = ((net_price - avg) / avg * 100) if avg else 0

        day_diff = cur_price - base_price if base_price else 0
        day_pct = (day_diff / base_price * 100) if base_price else 0

        warn_text = caches["warning"].get(ticker) or ""
        sector = caches["sector"].get(ticker) or ""
        flow = caches["investor"].get(ticker) or {}
        consensus = caches["consensus"].get(ticker) or {}
        target = consensus.get("target")
        score = consensus.get("score")
        target_gap_pct = ((target - cur_price) / cur_price * 100) if (target and cur_price) else 0

        stop_th = thresholds.get("stop_loss_alert_pct", -9.0)
        trail_th = thresholds.get("trailing_stop_alert_pct", -9.0)
        is_stop = (not self.watchlist) and avg and pnl_pct <= stop_th
        from_peak_pct = ((cur_price - peak_price) / peak_price * 100) if peak_price else 0
        # 피크 드롭: 매수가 위로 한 번이라도 올라간 적 있어야 (peaked) 트레일링 의미
        peaked_above_buy = bool(peak_price and avg and peak_price > avg)
        is_peak_drop = ((not self.watchlist) and peaked_above_buy
                         and from_peak_pct <= trail_th
                         and abs(from_peak_pct) >= 0.01)

        # 장마감 페이드 (KR 마감 + 토글 ON 일 때만)
        kr_closed = caches.get("kr_closed", False)
        fade = caches.get("fade_sleeping", False) and kr_closed
        def _fc(color, ratio=0.5):
            return _fade_hex(color, ratio) if fade else color

        # 카드 배경
        if not self.watchlist and pnl > 0:
            card_bg = COL_BG_PROFIT
        elif not self.watchlist and pnl < 0:
            card_bg = COL_BG_LOSS
        else:
            card_bg = COL_BG_CARD
        c.itemconfig(ids["bg"], fill=_fc(card_bg))

        # ─── pill
        zz = "💤 " if kr_closed else ""
        prefix = "[퇴] " if account == "퇴직연금" else ""
        suffix = f" ({shares}주)" if not self.watchlist else ""
        pill_text = f"  {zz}{prefix}{name}{suffix}  "
        pill_fg, pill_bg = _name_pill_colors(account, warn_text, pnl_pct,
                                              is_stop, is_peak_drop, day_diff)
        c.itemconfig(ids["pill_text"], text=pill_text, fill=_fc(pill_fg))
        c.itemconfig(ids["pill_bg"], fill=_fc(pill_bg))

        # pill bbox 측정 → 폭 보정
        bbox = c.bbox(ids["pill_text"])
        if bbox:
            tx0, ty0, tx1, ty1 = bbox
            c.coords(ids["pill_bg"], tx0 - 2, ty0 - 2, tx1 + 2, ty1 + 2)
            cursor_x = tx1 + 8
        else:
            cursor_x = c.coords(ids["pill_text"])[0] + 80

        # ─── badge
        badge_text, badge_bg = _badge_bg_for(warn_text, is_stop)
        if badge_text:
            c.itemconfig(ids["badge_text"], text=f" {badge_text} ",
                          fill="#ffffff" if badge_bg != COL_PILL_HALT else "#444",
                          anchor="w")
            # badge_text 위치 잡고 bbox 측정 후 bg 그림
            py = c.coords(ids["pill_text"])[1]
            c.coords(ids["badge_text"], cursor_x + 4, py)
            bb = c.bbox(ids["badge_text"])
            if bb:
                c.coords(ids["badge_bg"], bb[0] - 2, bb[1] - 1, bb[2] + 2, bb[3] + 1)
                c.itemconfig(ids["badge_bg"], fill=badge_bg)
                cursor_x = bb[2] + 6
        else:
            c.itemconfig(ids["badge_text"], text="")
            c.itemconfig(ids["badge_bg"], fill="")

        # ─── sector
        py = c.coords(ids["pill_text"])[1]
        c.coords(ids["sector_text"], cursor_x + 4, py)
        c.itemconfig(ids["sector_text"], text=sector if sector else "",
                     fill=_fc(COL_MUTED))

        # ─── price + vol + peak
        if cur_price:
            price_fg = sign_color(day_diff) if day_diff else COL_PRIMARY
            c.itemconfig(ids["price_text"],
                          text=f"{int(cur_price):,}원",
                          fill=_fc(price_fg))
        else:
            c.itemconfig(ids["price_text"], text="—", fill=_fc(COL_MUTED))
        bb = c.bbox(ids["price_text"])
        vx = bb[2] + 6 if bb else (c.coords(ids["price_text"])[0] + 100)
        py = c.coords(ids["price_text"])[1]
        # 거래량
        c.coords(ids["vol_text"], vx, py + 4)
        vol_str = f"({format_volume(volume)})" if volume else ""
        c.itemconfig(ids["vol_text"], text=vol_str, fill=_fc(COL_MUTED))
        # 피크: 현재가와 같으면 숨김, 피크 > 현재가면 빨강, 그 외(rare) 회색
        # is_peak_drop(예: from_peak<=-9%) 발생 시 배경 강조 (종목명 pill 대신 여기에)
        if (peak_price and cur_price
                and int(peak_price) != int(cur_price)):
            peak_str = f"{int(peak_price):,}원 ({from_peak_pct:+.2f}%)"
            if is_peak_drop:
                # 피크가 대비 -10% 이상 (트레일링 스탑 알림) — 항상 진빨강 배경 + 흰 글자
                peak_bg_fill = COL_PILL_PEAK_PROFIT
                peak_color = "#ffffff"
            else:
                peak_bg_fill = ""
                peak_color = "#c0392b" if peak_price > cur_price else COL_MUTED
        else:
            peak_str = ""
            peak_bg_fill = ""
            peak_color = COL_MUTED
        # vol_text 우측에 동적 배치
        if vol_str:
            vb = c.bbox(ids["vol_text"])
            px = vb[2] + 6 if vb else (vx + 80)
        else:
            px = vx
        c.coords(ids["peak_text"], px, py + 4)
        c.itemconfig(ids["peak_text"], text=peak_str, fill=_fc(peak_color))
        # peak_bg: 강조 시 peak_text bbox 둘러싸도록
        if peak_bg_fill and peak_str:
            c.itemconfig(ids["peak_bg"], fill=_fc(peak_bg_fill))
            pb = c.bbox(ids["peak_text"])
            if pb:
                c.coords(ids["peak_bg"],
                         pb[0] - 4, pb[1] - 1, pb[2] + 4, pb[3] + 1)
        else:
            c.itemconfig(ids["peak_bg"], fill="")
            c.coords(ids["peak_bg"], 0, 0, 0, 0)

        # ─── 어제보다
        c.itemconfig(ids["day_label"], text="어제보다", fill=_fc(COL_SECONDARY))
        if day_diff:
            c.itemconfig(ids["day_value"],
                          text=f"{format_signed(int(day_diff))}  ({day_pct:+.2f}%)",
                          fill=_fc(sign_color(day_diff)))
        else:
            c.itemconfig(ids["day_value"], text="—", fill=_fc(COL_MUTED))

        # ─── 전체수익 (보유만)
        if self.watchlist:
            c.itemconfig(ids["pnl_label"], text="")
            c.itemconfig(ids["pnl_value"], text="")
        else:
            c.itemconfig(ids["pnl_label"], text="전체수익", fill=_fc(COL_SECONDARY))
            if pnl:
                c.itemconfig(ids["pnl_value"],
                              text=f"{format_signed(pnl)}  ({pnl_pct:+.2f}%)",
                              fill=_fc(sign_color(pnl)))
            else:
                c.itemconfig(ids["pnl_value"], text="—", fill=_fc(COL_MUTED))

        # ─── 목표
        c.itemconfig(ids["target_label"], text="목표", fill=_fc(COL_SECONDARY))
        if target:
            score_str = f"({score:.2f}) " if score else ""
            c.itemconfig(ids["target_value"],
                          text=f"{score_str}{int(target):,}",
                          fill=_fc(COL_PRIMARY))
            c.itemconfig(ids["target_gap"],
                          text=f"({target_gap_pct:+.2f}%)",
                          fill=_fc(sign_color(target_gap_pct)))
            # target_value bbox 측정 → target_gap 위치 동적 배치 (좁은 카드에서 충돌 방지)
            tv_bbox = c.bbox(ids["target_value"])
            if tv_bbox:
                tv_y = c.coords(ids["target_value"])[1]
                c.coords(ids["target_gap"], tv_bbox[2] + 8, tv_y)
        else:
            c.itemconfig(ids["target_value"], text="—", fill=_fc(COL_MUTED))
            c.itemconfig(ids["target_gap"], text="")

        # ─── 우측 12 항목 그리드 (외국인 보유율 + 11 분류 통합)
        c.itemconfig(ids["date_text"], text="")

        for label, key in self.RIGHT_FIELDS:
            v = flow.get(key) if flow else None
            if key == "외국인비율":
                # % 형식 — 비율은 부호 의미 없으므로 primary 색 (값 있으면)
                if v:
                    text = f"{v:.2f}%"
                    color = COL_PRIMARY
                else:
                    text = "—"
                    color = COL_MUTED
                label_color = COL_SECONDARY
            else:
                text = _format_signed_or_zero(v)
                color = _amount_color(v)
                # + 값이면 라벨도 같은 빨강 (한눈에 보이도록)
                label_color = sign_color(v) if (v and v > 0) else COL_SECONDARY

            # 강조 셀: 부호별 배경 + 라벨 색 강조 (배경 위 가독성)
            if key in self.HIGHLIGHT_KEYS:
                if v and v > 0:
                    bg_fill = COL_BG_PROFIT
                elif v and v < 0:
                    bg_fill = COL_BG_LOSS
                else:
                    bg_fill = ""
                c.itemconfig(ids[f"flow_bg_{key}"],
                             fill=_fc(bg_fill) if bg_fill else "")
                label_color = sign_color(v) if v else COL_SECONDARY

            c.itemconfig(ids[f"flow_label_{key}"], text=label, fill=_fc(label_color))
            c.itemconfig(ids[f"flow_value_{key}"], text=text, fill=_fc(color))

    # ───────── 외부 API ─────────
    def render(self, stocks, prices, peaks, thresholds, caches,
               total_invested=0, total_current=0, total_yesterday=0):
        """전체 갱신 — 새 종목은 카드 생성, 사라진 종목은 카드 제거,
        나머지는 텍스트만 in-place 갱신.
        """
        # grouped 상태 클리어 (이전에 grouped 호출했을 수 있음)
        self._grouped_pos_map = None
        self._grouped_y_offset_map = None
        self._grouped_groups = None
        self._grouped_total_idx = 0
        self._grouped_max_y_offset = 0

        # 어제대비 등락률 (day_pct) 큰 순 — 모든 탭 동일
        def _key(s):
            pd = prices.get(s["ticker"]) or {}
            cur = pd.get("price", 0)
            base = pd.get("base", 0)
            day_pct = ((cur - base) / base * 100) if base else 0
            return -day_pct
        stocks_sorted = sorted(stocks, key=_key)

        new_order = [s["ticker"] for s in stocks_sorted]

        # 사라진 카드 제거
        old_set = set(self.cards.keys())
        new_set = set(new_order)
        for t in old_set - new_set:
            self.canvas.delete(f"card:{t}")
            del self.cards[t]

        # 신규 카드 생성
        for t in new_order:
            if t not in self.cards:
                self.cards[t] = self._create_card(0, t)  # 위치는 _reflow 에서

        self.order = new_order

        # 좌표 배치
        self._reflow()

        # 값 갱신
        for s in stocks_sorted:
            t = s["ticker"]
            self._update_card_values(
                self.cards[t], s, prices.get(t), peaks.get(t),
                thresholds, caches,
            )

        # 합계
        self._render_total(total_invested, total_current, total_yesterday)

        # 빈 메시지
        if not stocks_sorted:
            self.canvas.delete("empty")
            self.canvas.create_text(
                self._width / 2 if self._width > 0 else 200, 60,
                text=("관심 종목이 없습니다." if self.watchlist
                      else "보유 종목이 없습니다."),
                fill=COL_MUTED, font=_font(13),
                tags=("empty",), anchor="center")

    def _reflow(self):
        """카드를 self.order 에 따라 좌표 재배치 (2열 배치).
        grouped 모드면 _grouped_pos_map 의 idx 를 사용해 그룹 사이 빈 행 보존."""
        width = self._width or self.canvas.winfo_width() or 1000
        pos_map = self._grouped_pos_map
        if pos_map:
            y_offset_map = getattr(self, "_grouped_y_offset_map", {}) or {}
            for t in self.order:
                ids = self.cards.get(t)
                idx = pos_map.get(t)
                if ids and idx is not None:
                    self._position_card(ids, idx, width,
                                          y_offset=y_offset_map.get(t, 0))
            max_idx = self._grouped_total_idx
            rows = (max_idx + 1) // 2
            extra_h = getattr(self, "_grouped_max_y_offset", 0) or 0
        else:
            for idx, t in enumerate(self.order):
                ids = self.cards.get(t)
                if ids:
                    self._position_card(ids, idx, width)
            rows = (len(self.order) + 1) // 2
            extra_h = 0
        total_h = max(40, rows * (CARD_HEIGHT + CARD_GAP) + 12 + extra_h)
        self.canvas.configure(scrollregion=(0, 0, width, total_h))
        # grouped 모드면 totals 도 폭에 맞춰 다시 그림
        if pos_map and self._grouped_groups:
            self._render_total_split(self._grouped_groups)

    # ───────── 그룹 렌더 (보유 + 퇴직연금 통합 표시) ─────────
    def render_grouped(self, groups, prices, peaks, thresholds, caches):
        """다중 그룹 렌더 — 그룹 사이 빈 행 1줄, 합계는 그룹별 분할 표시.
        groups: [{"stocks": [...], "label": "보유 합계", "totals": (inv, cur, yes)}, ...]
        """
        def _key(s):
            pd = prices.get(s["ticker"]) or {}
            cur = pd.get("price", 0); base = pd.get("base", 0)
            return -(((cur - base) / base * 100) if base else 0)

        pos_map = {}
        y_offset_map = {}
        cursor = 0
        cur_y_offset = 0
        sorted_groups = []
        GROUP_GAP_PX = 10  # 그룹 사이 작은 시각적 간격
        for gi, g in enumerate(groups):
            sorted_stocks = sorted(g.get("stocks", []), key=_key)
            if gi > 0 and sorted_stocks:
                # 직전 그룹 끝을 짝수(=새 행 시작)로 올림 + 작은 픽셀 갭
                cursor = ((cursor + 1) // 2) * 2
                cur_y_offset += GROUP_GAP_PX
            for s in sorted_stocks:
                pos_map[s["ticker"]] = cursor
                y_offset_map[s["ticker"]] = cur_y_offset
                cursor += 1
            sorted_groups.append({**g, "sorted_stocks": sorted_stocks})

        self._grouped_pos_map = pos_map
        self._grouped_y_offset_map = y_offset_map
        self._grouped_total_idx = cursor
        self._grouped_max_y_offset = cur_y_offset
        self._grouped_groups = sorted_groups

        all_stocks = [s for g in sorted_groups for s in g["sorted_stocks"]]
        new_tickers = {s["ticker"] for s in all_stocks}

        # 사라진 카드 제거
        for t in set(self.cards.keys()) - new_tickers:
            self.canvas.delete(f"card:{t}")
            del self.cards[t]

        # 신규 카드 생성
        for s in all_stocks:
            if s["ticker"] not in self.cards:
                self.cards[s["ticker"]] = self._create_card(0, s["ticker"])

        self.order = [s["ticker"] for s in all_stocks]

        # 좌표 배치 (grouped 모드 → _reflow 가 pos_map 사용)
        self._reflow()

        # 값 갱신
        for s in all_stocks:
            t = s["ticker"]
            self._update_card_values(
                self.cards[t], s, prices.get(t), peaks.get(t),
                thresholds, caches,
            )

        # 합계 분할 (이미 _reflow 에서 호출되지만, 값 갱신 직후 한 번 더 보장)
        self._render_total_split(sorted_groups)

        # 빈 메시지
        if not all_stocks:
            self.canvas.delete("empty")
            self.canvas.create_text(
                self._width / 2 if self._width > 0 else 200, 60,
                text="보유 종목이 없습니다.",
                fill=COL_MUTED, font=_font(13),
                tags=("empty",), anchor="center")
        else:
            self.canvas.delete("empty")

    def _render_total_split(self, groups):
        """합계 영역 — 그룹별로 좌/우 분할. 빈 그룹/투자 0 그룹은 표시 안 함."""
        c = self.total_canvas
        c.delete("all")
        if self.watchlist:
            return
        visible = [g for g in groups
                    if g.get("sorted_stocks") and (g.get("totals") or (0, 0, 0))[0]]
        if not visible:
            return
        w = self.frame.winfo_width() or 1000
        pad = 6
        half_gap = 6
        n = len(visible)
        box_w = (w - pad * 2 - half_gap * (n - 1)) // n
        for i, g in enumerate(visible):
            invested, current, yesterday = g["totals"]
            pnl = current - invested
            pnl_pct = (pnl / invested * 100) if invested else 0
            day_diff = current - yesterday if yesterday else 0
            day_pct = (day_diff / yesterday * 100) if yesterday else 0
            x0 = pad + i * (box_w + half_gap)
            x1 = x0 + box_w
            y0, y1 = 4, 64
            c.create_rectangle(x0, y0, x1, y1, fill=COL_BG_CARD,
                                outline=COL_BORDER, width=1)
            c.create_text(x0 + 14, y0 + 12,
                           text=g.get("label", "합계"), anchor="w",
                           font=_font(13, "bold"), fill=COL_PRIMARY)
            c.create_text(x1 - 14, y0 + 12,
                           text=f"{int(current):,}원", anchor="e",
                           font=_font(13, "bold"), fill=COL_PRIMARY)
            c.create_text(x0 + 14, y0 + 32,
                           text="전체수익", anchor="w",
                           font=_font(11), fill=COL_SECONDARY)
            c.create_text(x1 - 14, y0 + 32,
                           text=f"{format_signed(int(pnl))} ({pnl_pct:+.2f}%)",
                           anchor="e", font=_font(11, "bold"),
                           fill=sign_color(pnl))
            c.create_text(x0 + 14, y0 + 50,
                           text="어제대비", anchor="w",
                           font=_font(11), fill=COL_SECONDARY)
            if day_diff:
                c.create_text(x1 - 14, y0 + 50,
                               text=f"{format_signed(int(day_diff))} ({day_pct:+.2f}%)",
                               anchor="e", font=_font(11, "bold"),
                               fill=sign_color(day_diff))

    def _render_total(self, invested, current, yesterday):
        """합계 영역 — total_canvas 에 직접 그림."""
        c = self.total_canvas
        c.delete("all")
        if self.watchlist or not invested:
            return
        pnl = current - invested
        pnl_pct = (pnl / invested * 100) if invested else 0
        day_diff = current - yesterday if yesterday else 0
        day_pct = (day_diff / yesterday * 100) if yesterday else 0

        w = self.frame.winfo_width() or 1000
        x0, y0, x1, y1 = 6, 4, w - 6, 64
        c.create_rectangle(x0, y0, x1, y1, fill=COL_BG_CARD,
                            outline=COL_BORDER, width=1)
        c.create_text(x0 + 14, y0 + 12,
                       text="합계", anchor="w",
                       font=_font(13, "bold"), fill=COL_PRIMARY)
        c.create_text(x1 - 14, y0 + 12,
                       text=f"{int(current):,}원", anchor="e",
                       font=_font(13, "bold"), fill=COL_PRIMARY)
        c.create_text(x0 + 14, y0 + 32,
                       text="전체수익", anchor="w",
                       font=_font(11), fill=COL_SECONDARY)
        c.create_text(x1 - 14, y0 + 32,
                       text=f"{format_signed(int(pnl))} ({pnl_pct:+.2f}%)",
                       anchor="e", font=_font(11, "bold"),
                       fill=sign_color(pnl))
        c.create_text(x0 + 14, y0 + 50,
                       text="어제대비", anchor="w",
                       font=_font(11), fill=COL_SECONDARY)
        if day_diff:
            c.create_text(x1 - 14, y0 + 50,
                           text=f"{format_signed(int(day_diff))} ({day_pct:+.2f}%)",
                           anchor="e", font=_font(11, "bold"),
                           fill=sign_color(day_diff))


def _fade_hex(color: str, ratio: float = 0.5, target: str = "#ffffff") -> str:
    """hex color 를 target 색으로 ratio 만큼 혼합 (기본은 흰색쪽 페이드)."""
    if not isinstance(color, str) or not color.startswith("#"):
        return color
    if len(color) == 4:
        color = "#" + "".join(c * 2 for c in color[1:])
    if len(color) != 7:
        return color
    if isinstance(target, str) and target.startswith("#") and len(target) == 4:
        target = "#" + "".join(c * 2 for c in target[1:])
    if not isinstance(target, str) or not target.startswith("#") or len(target) != 7:
        target = "#ffffff"
    try:
        r = int(color[1:3], 16); g = int(color[3:5], 16); b = int(color[5:7], 16)
        tr = int(target[1:3], 16); tg = int(target[3:5], 16); tb = int(target[5:7], 16)
        r = int(r + (tr - r) * ratio)
        g = int(g + (tg - g) * ratio)
        b = int(b + (tb - b) * ratio)
        return f"#{r:02x}{g:02x}{b:02x}"
    except Exception:
        return color


# ─────────────────────────── 미국 증시 (v1 스타일 섹터 테이블) ───────────────────────────
class USIndicesV1Style:
    """v1 의 미국 증시 렌더링 — Tier 0 스트립 + 섹터 블록 그리드.

    구조:
        Tier 0 가로 스트립 (다크 네이비)
        섹터 블록: [섹터명 | 현물지표 | 선물 | 한국 ETF]  × 11 섹터
    """

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
    ETFS_BY_SECTOR = {
        "반도체":   ["091160", "091230"],
        "방산":     ["449450"],
        "중공업":   ["446770"],
        "리츠":     ["329200"],
        "에너지":   [],
        "자동차":   ["091180"],
        "건설":     ["117700"],
        "금융":     ["091170"],
        "플랫폼":   ["365040"],
        "바이오":   ["143860"],
        "한국지수": ["122630", "229200"],
    }

    def __init__(self, parent, on_open_url):
        self.parent = parent
        self.on_open_url = on_open_url
        self.frame = tk.Frame(parent, bg="white")
        self.frame.pack(fill="both", expand=True)

        # 스크롤 컨테이너 — yscrollincrement 로 휠 1단위 = 20px 고정
        self.canvas = tk.Canvas(self.frame, bg="white", highlightthickness=0,
                                  yscrollincrement=10)
        self.vsb = ttk.Scrollbar(self.frame, orient="vertical",
                                  command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)
        self.vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.inner = tk.Frame(self.canvas, bg="white")
        self._inner_id = self.canvas.create_window((0, 0), window=self.inner,
                                                     anchor="nw")
        self.inner.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        self.canvas.bind(
            "<Configure>",
            lambda e: self.canvas.itemconfigure(self._inner_id, width=e.width)
        )
        self.canvas.bind("<Enter>", lambda e: self._bind_wheel())
        self.canvas.bind("<Leave>", lambda e: self._unbind_wheel())

        # 부분 갱신 레지스트리 (깜빡임 방지)
        self._fade_sleeping = False
        self._loading_label = None
        self._tier0_strip = None
        self._tier0_w = {}        # symbol -> {cell, line1, lbl_*}
        self._sector_root = None
        self._sector_cells = {}   # sector_key -> {bg, ind_inner, fut_inner, etf_inner}
        self._card_w = {}         # (sector_key, kind, key) -> 카드 위젯 dict

    def _bind_wheel(self):
        self.canvas.bind_all("<MouseWheel>", self._on_wheel)

    def _unbind_wheel(self):
        self.canvas.unbind_all("<MouseWheel>")

    def _on_wheel(self, e):
        d = e.delta
        if d == 0:
            return
        step = int(-d / 30) if abs(d) >= 30 else -d
        if step == 0:
            step = -1 if d > 0 else 1
        self.canvas.yview_scroll(step, "units")

    def render(self, indices, holdings, etf_prices=None, fade_sleeping=False):
        """부분 갱신 — 위젯을 destroy 없이 텍스트·색·bg만 itemconfig 로 갱신.
        보유종목 카드와 동일한 패턴이라 깜빡임 없음."""
        self._fade_sleeping = bool(fade_sleeping)

        if not indices:
            # 빈 데이터 — 전체 클리어 후 로딩 메시지
            for w in self.inner.winfo_children():
                w.destroy()
            self._tier0_strip = None; self._tier0_w = {}
            self._sector_root = None; self._sector_cells = {}; self._card_w = {}
            self._loading_label = tk.Label(
                self.inner, text="미국 증시 데이터 로딩 중...",
                bg="white", fg="#999", font=("SF Pro", 12))
            self._loading_label.pack(anchor="w", padx=8, pady=20)
            return

        # 데이터 도착 — 로딩 라벨 제거
        if self._loading_label and self._loading_label.winfo_exists():
            self._loading_label.destroy()
        self._loading_label = None

        # Tier 0 upsert
        tier0 = [x for x in indices if x.get("tier") == "T0"]
        self._upsert_tier0(tier0)

        # 섹터 그리드 골격은 한 번만 생성 (이후 재사용)
        if not self._sector_root or not self._sector_root.winfo_exists():
            self._sector_root = tk.Frame(self.inner, bg="white")
            self._sector_root.pack(fill="x", pady=(8, 0))
            self._build_sector_scaffolding()

        # 섹터 카드 upsert
        by_sector = {}
        for x in indices:
            by_sector.setdefault(x.get("sector"), []).append(x)
        self._upsert_all_sector_cards(by_sector, holdings, etf_prices or {})

        # scrollregion 갱신 (스크롤 위치는 자연 보존 — destroy 안 함)
        self.inner.update_idletasks()
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    # ─── Tier 0 스트립: upsert ───
    def _upsert_tier0(self, tier0_indices):
        if not self._tier0_strip or not self._tier0_strip.winfo_exists():
            self._tier0_strip = tk.Frame(self.inner, bg="#2c3e50")
            self._tier0_strip.pack(fill="x", pady=(0, 6), side="top")
            self._tier0_w = {}
        new_keys = {idx.get("symbol", "") for idx in tier0_indices if idx.get("symbol")}
        # 사라진 셀 제거
        for k in list(self._tier0_w.keys()):
            if k not in new_keys:
                self._tier0_w[k]["cell"].destroy()
                del self._tier0_w[k]
        # 신규/기존
        for idx in tier0_indices:
            symbol = idx.get("symbol", "")
            if not symbol:
                continue
            if symbol in self._tier0_w:
                self._update_tier0_cell(self._tier0_w[symbol], idx)
            else:
                self._tier0_w[symbol] = self._create_tier0_cell(idx)

    def _create_tier0_cell(self, idx):
        parent = self._tier0_strip
        cell = tk.Frame(parent, bg="#2c3e50")
        cell.pack(side="left", padx=(0, 16))

        pct = idx.get("pct", 0)
        symbol = idx.get("symbol", "")
        url = resolve_us_indicator_url(symbol)
        closed = not is_market_open(market_of_symbol(symbol))
        name_disp = f"💤 {idx['name']}" if closed else idx["name"]
        note = idx.get("note", "")
        price = idx.get("price", 0)
        # 장마감 페이드 — 다크 bg(#2c3e50)쪽으로 혼합해 어둡게
        fade = bool(self._fade_sleeping) and closed
        fade_to = "#2c3e50"
        def _fc(c):
            return _fade_hex(c, 0.5, target=fade_to) if fade else c
        pct_raw = "#ff6b6b" if pct > 0 else ("#5dade2" if pct < 0 else "#bbb")

        line1 = tk.Frame(cell, bg="#2c3e50")
        line1.pack(fill="x", padx=8, pady=(6, 0))
        lbl_name = tk.Label(line1, text=name_disp, font=("SF Pro", 16, "bold"),
                             bg="#2c3e50", fg=_fc("#ffffff"), cursor="pointinghand")
        lbl_name.pack(side="left")
        lbl_price = tk.Label(line1, text=f"{price:,.2f}", font=("SF Mono", 14),
                              bg="#2c3e50", fg=_fc("#ecf0f1"), cursor="pointinghand")
        lbl_price.pack(side="left", padx=(10, 0))
        lbl_pct = tk.Label(line1, text=f"{pct:+.2f}%", font=("SF Mono", 16, "bold"),
                            bg="#2c3e50", fg=_fc(pct_raw), cursor="pointinghand")
        lbl_pct.pack(side="left", padx=(6, 0))

        w = {"cell": cell, "line1": line1, "url": url,
             "lbl_name": lbl_name, "lbl_price": lbl_price, "lbl_pct": lbl_pct}
        if note:
            lbl_note = tk.Label(cell, text=note, font=("SF Pro", 12),
                                 bg="#2c3e50", fg=_fc("#95a5a6"), anchor="w",
                                 cursor="pointinghand")
            lbl_note.pack(fill="x", padx=8, pady=(0, 6))
            w["lbl_note"] = lbl_note
        for widget in [cell, line1, lbl_name, lbl_price, lbl_pct]:
            widget.bind("<Button-1>", lambda e, u=url: self.on_open_url(u))
        if "lbl_note" in w:
            w["lbl_note"].bind("<Button-1>", lambda e, u=url: self.on_open_url(u))
        return w

    def _update_tier0_cell(self, w, idx):
        pct = idx.get("pct", 0)
        symbol = idx.get("symbol", "")
        closed = not is_market_open(market_of_symbol(symbol))
        name_disp = f"💤 {idx['name']}" if closed else idx["name"]
        note = idx.get("note", "")
        price = idx.get("price", 0)
        fade = bool(self._fade_sleeping) and closed
        def _fc(c):
            return _fade_hex(c, 0.5, target="#2c3e50") if fade else c
        pct_raw = "#ff6b6b" if pct > 0 else ("#5dade2" if pct < 0 else "#bbb")

        w["lbl_name"].configure(text=name_disp, fg=_fc("#ffffff"))
        w["lbl_price"].configure(text=f"{price:,.2f}", fg=_fc("#ecf0f1"))
        w["lbl_pct"].configure(text=f"{pct:+.2f}%", fg=_fc(pct_raw))
        if note:
            if "lbl_note" in w:
                w["lbl_note"].configure(text=note, fg=_fc("#95a5a6"))
            else:
                url = w["url"]
                lbl_note = tk.Label(w["cell"], text=note, font=("SF Pro", 12),
                                     bg="#2c3e50", fg=_fc("#95a5a6"), anchor="w",
                                     cursor="pointinghand")
                lbl_note.pack(fill="x", padx=8, pady=(0, 6))
                lbl_note.bind("<Button-1>", lambda e, u=url: self.on_open_url(u))
                w["lbl_note"] = lbl_note
        elif "lbl_note" in w:
            w["lbl_note"].destroy()
            del w["lbl_note"]

    # ─── 섹터 그리드 골격 (한 번만 생성) ───
    def _build_sector_scaffolding(self):
        parent = self._sector_root
        parent.grid_columnconfigure(0, weight=0, minsize=160)
        parent.grid_columnconfigure(1, weight=1, uniform="data")
        parent.grid_columnconfigure(2, weight=1, uniform="data")
        parent.grid_columnconfigure(3, weight=1, uniform="data")
        hdr_fg = "#2c3e50"
        self._sector_cells = {}
        for row_idx, (sector_key, sector_label) in enumerate(self.ALL_SECTORS):
            # 짝/홀 섹터 배경 — 라인 전체(섹터명 + 종목 + 선물 + ETF) 동일 톤
            bg = "#eef0f4" if row_idx % 2 == 1 else "white"
            # 섹터명 셀도 row bg 와 동일 색
            name_cell = tk.Frame(parent, bg=bg, highlightthickness=0)
            name_cell.grid(row=row_idx, column=0, sticky="nsew")
            tk.Label(name_cell, text=sector_label, font=("SF Pro", 16, "bold"),
                      bg=bg, fg=hdr_fg, anchor="w",
                      padx=8, pady=5).pack(fill="both", expand=True)
            # 현물 / 선물 / ETF 인너 프레임
            inners = {}
            for kind, col in [("ind_inner", 1), ("fut_inner", 2), ("etf_inner", 3)]:
                cell = tk.Frame(parent, bg=bg, highlightthickness=0)
                cell.grid(row=row_idx, column=col, sticky="nsew")
                inner = tk.Frame(cell, bg=bg)
                inner.pack(fill="both", expand=True, padx=0, pady=0)
                inner.grid_columnconfigure(0, weight=1)
                inners[kind] = inner
            self._sector_cells[sector_key] = {"bg": bg, **inners}

    # ─── 섹터 카드 일괄 upsert ───
    def _upsert_all_sector_cards(self, by_sector, holdings, etf_prices):
        holdings_by_ticker = {s["ticker"]: s for s in holdings}
        new_keys = set()
        for sector_key, _ in self.ALL_SECTORS:
            cells = self._sector_cells.get(sector_key)
            if not cells:
                continue
            bg = cells["bg"]
            indices = by_sector.get(sector_key, [])
            etf_tickers = self.ETFS_BY_SECTOR.get(sector_key, [])
            # 현물지표
            for i, idx in enumerate(indices):
                self._upsert_indicator_card(cells["ind_inner"], sector_key,
                                              idx, bg, i)
                if idx.get("symbol"):
                    new_keys.add((sector_key, "indicator", idx["symbol"]))
            # 선물
            fut_row = 0
            for idx in indices:
                fs = idx.get("fut_symbol") or ""
                fp = idx.get("fut_pct")
                if not fs or fp is None:
                    continue
                self._upsert_futures_card(cells["fut_inner"], sector_key,
                                            idx, bg, fut_row)
                new_keys.add((sector_key, "futures", fs))
                fut_row += 1
            # ETF
            for ti, t in enumerate(etf_tickers):
                stock = holdings_by_ticker.get(t) or {"ticker": t, "name": t}
                self._upsert_etf_card(cells["etf_inner"], sector_key, stock,
                                        etf_prices.get(t, {}), bg, ti)
                new_keys.add((sector_key, "etf", t))
        # 사라진 카드 제거
        for k in list(self._card_w.keys()):
            if k not in new_keys:
                self._card_w[k]["card"].destroy()
                del self._card_w[k]

    def _upsert_card(self, *, parent_inner, reg_key, row_i, name_disp, note,
                      price_txt, pct_value, url, fade,
                      name_size=13, price_size=12, pct_size=13,
                      name_fg_base="#222", note_fg_base="#888",
                      bg_default="white"):
        """카드 1개 upsert — 기존이면 텍스트·색만 갱신, 없으면 신규 생성."""
        def _fc(color, ratio=0.5):
            return _fade_hex(color, ratio) if fade else color
        if pct_value is not None and pct_value > 0:
            card_bg = "#fbe6e6"
        elif pct_value is not None and pct_value < 0:
            card_bg = "#dde6f3"
        else:
            card_bg = bg_default
        card_bg = _fc(card_bg)
        if pct_value is not None and pct_value != 0:
            sc = sign_color(pct_value)
            name_fg = _fc(sc); price_fg = _fc(sc); pct_fg = _fc(sc)
        else:
            name_fg = _fc(name_fg_base)
            price_fg = _fc("#222")
            pct_fg = _fc("#888")
        note_fg = _fc(note_fg_base)
        pct_str = f"({pct_value:+.2f}%)" if pct_value is not None else ""

        existing = self._card_w.get(reg_key)
        if existing:
            w = existing
            if w.get("row_i") != row_i:
                w["card"].grid_forget()
                w["card"].grid(row=row_i, column=0, columnspan=4, sticky="we",
                               pady=(2, 6), padx=4)
                w["row_i"] = row_i
            w["card"].configure(bg=card_bg)
            w["line1"].configure(bg=card_bg)
            w["lbl_name"].configure(text=name_disp, bg=card_bg, fg=name_fg)
            if note:
                if "lbl_note" in w:
                    w["lbl_note"].configure(text=note, bg=card_bg, fg=note_fg)
                else:
                    lbl_note = tk.Label(w["line1"], text=note, font=("SF Pro", 11),
                                         bg=card_bg, fg=note_fg,
                                         cursor="pointinghand")
                    lbl_note.pack(side="left", padx=(6, 0))
                    lbl_note.bind("<Button-1>",
                                   lambda e, u=url: self.on_open_url(u))
                    w["lbl_note"] = lbl_note
            elif "lbl_note" in w:
                w["lbl_note"].destroy(); del w["lbl_note"]
            w["line2"].configure(bg=card_bg)
            w["lbl_price"].configure(text=price_txt, bg=card_bg, fg=price_fg)
            if pct_value is not None:
                if "lbl_pct" in w:
                    w["lbl_pct"].configure(text=pct_str, bg=card_bg, fg=pct_fg)
                else:
                    lbl_pct = tk.Label(w["line2"], text=pct_str,
                                        font=("SF Mono", pct_size, "bold"),
                                        bg=card_bg, fg=pct_fg,
                                        cursor="pointinghand")
                    lbl_pct.pack(side="left", padx=(8, 0))
                    lbl_pct.bind("<Button-1>",
                                  lambda e, u=url: self.on_open_url(u))
                    w["lbl_pct"] = lbl_pct
            elif "lbl_pct" in w:
                w["lbl_pct"].destroy(); del w["lbl_pct"]
            w["url"] = url
            return

        # 신규 생성
        card = tk.Frame(parent_inner, bg=card_bg)
        card.grid(row=row_i, column=0, columnspan=4, sticky="we",
                   pady=(2, 6), padx=4)
        line1 = tk.Frame(card, bg=card_bg)
        line1.pack(fill="x", padx=4, pady=(2, 0))
        lbl_name = tk.Label(line1, text=name_disp,
                             font=("SF Pro", name_size, "bold"),
                             bg=card_bg, fg=name_fg, anchor="w",
                             cursor="pointinghand")
        lbl_name.pack(side="left")
        w = {"card": card, "line1": line1, "lbl_name": lbl_name,
             "url": url, "row_i": row_i}
        if note:
            lbl_note = tk.Label(line1, text=note, font=("SF Pro", 11),
                                 bg=card_bg, fg=note_fg, cursor="pointinghand")
            lbl_note.pack(side="left", padx=(6, 0))
            w["lbl_note"] = lbl_note
        line2 = tk.Frame(card, bg=card_bg)
        line2.pack(fill="x", padx=4, pady=(0, 4))
        lbl_price = tk.Label(line2, text=price_txt,
                              font=("SF Mono", price_size),
                              bg=card_bg, fg=price_fg, cursor="pointinghand")
        lbl_price.pack(side="left")
        w["line2"] = line2
        w["lbl_price"] = lbl_price
        if pct_value is not None:
            lbl_pct = tk.Label(line2, text=pct_str,
                                font=("SF Mono", pct_size, "bold"),
                                bg=card_bg, fg=pct_fg, cursor="pointinghand")
            lbl_pct.pack(side="left", padx=(8, 0))
            w["lbl_pct"] = lbl_pct
        bind_widgets = [card, line1, lbl_name, line2, lbl_price]
        if "lbl_note" in w:
            bind_widgets.append(w["lbl_note"])
        if "lbl_pct" in w:
            bind_widgets.append(w["lbl_pct"])
        for widget in bind_widgets:
            widget.bind("<Button-1>", lambda e, u=url: self.on_open_url(u))
        self._card_w[reg_key] = w

    def _upsert_indicator_card(self, parent_inner, sector_key, idx, bg, row_i):
        pct = idx.get("pct", 0)
        symbol = idx.get("symbol", "")
        url = resolve_us_indicator_url(symbol)
        closed = not is_market_open(market_of_symbol(symbol))
        name_disp = f"💤{idx['name']}" if closed else idx["name"]
        note = idx.get("note", "")
        price = idx.get("price", 0)
        price_txt = f"{price:,.2f}" if price else "-"
        fade = bool(self._fade_sleeping) and closed
        self._upsert_card(parent_inner=parent_inner,
                            reg_key=(sector_key, "indicator", symbol),
                            row_i=row_i, name_disp=name_disp, note=note,
                            price_txt=price_txt, pct_value=pct,
                            url=url, fade=fade, bg_default=bg)

    def _upsert_futures_card(self, parent_inner, sector_key, idx, bg, row_i):
        fs = idx.get("fut_symbol") or ""
        fp = idx.get("fut_pct")
        fpr = idx.get("fut_price")
        if not fs or fp is None:
            return
        full_name = FUT_FULL_NAME.get(fs, fs)
        closed = not is_market_open(market_of_symbol(fs))
        zz = "💤" if closed else ""
        name_disp = f"{zz}{full_name}"
        note = f"({fs})"
        price_txt = f"{fpr:,.2f}" if fpr else "-"
        url = resolve_us_indicator_url(fs)
        fade = bool(self._fade_sleeping) and closed
        self._upsert_card(parent_inner=parent_inner,
                            reg_key=(sector_key, "futures", fs),
                            row_i=row_i, name_disp=name_disp, note=note,
                            price_txt=price_txt, pct_value=fp,
                            url=url, fade=fade, bg_default=bg)

    def _upsert_etf_card(self, parent_inner, sector_key, stock,
                           price_data, bg, row_i):
        price = price_data.get("price", 0) or 0
        base = price_data.get("base", 0) or 0
        diff = price - base if (price and base) else 0
        pct = (diff / base * 100) if base else 0
        t = stock["ticker"]
        url = f"https://tossinvest.com/stocks/A{t}"
        price_txt = f"{int(price):,}" if price else "-"
        closed = not is_market_open("KR")
        zz = "💤" if closed else ""
        name_disp = f"{zz}{stock.get('name', t)}"
        fade = bool(self._fade_sleeping) and closed
        self._upsert_card(parent_inner=parent_inner,
                            reg_key=(sector_key, "etf", t),
                            row_i=row_i, name_disp=name_disp, note="",
                            price_txt=price_txt, pct_value=pct,
                            url=url, fade=fade, bg_default=bg,
                            name_fg_base="#2c3e50")


# ─────────────────────────── 메인 윈도우 ───────────────────────────
class PortfolioWindowV2:
    INVESTOR_TTL = 120
    CONSENSUS_TTL = 3600
    SECTOR_TTL = 86400
    WARNING_TTL = 6 * 3600
    US_TTL = 60

    TAB_US = "us"
    TAB_HOLD = "hold"
    TAB_WATCH = "watch"

    def __init__(self):
        self.holdings_data = load_json(HOLDINGS_PATH, default={})
        all_stocks = self.holdings_data.get("holdings", [])
        # account 필드로 분류
        #   ""  / None      → 보유 종목 탭 (실보유)
        #   "퇴직연금"      → 퇴직연금 탭
        #   "관심"          → 관심 종목 탭
        #   "관심ETF"       → 미국 증시 탭 우측 ETF 컬럼
        self.holdings = [s for s in all_stocks
                          if not (s.get("account") or "")]
        self.pension = [s for s in all_stocks
                          if s.get("account") == "퇴직연금"]
        self.watchlist = [s for s in all_stocks
                            if s.get("account") == "관심"]
        self.etf_holdings = [s for s in all_stocks
                              if s.get("account") == "관심ETF"]
        self.config = load_json(CONFIG_PATH, default={
            "stop_loss_alert_pct": -9.0,
            "trailing_stop_alert_pct": -9.0,
            "polling_interval_seconds": 5,
            "alert_cooldown_minutes": 15,
            "sell_fee_pct": 0.2,
        })
        self.peaks = load_json(PEAKS_PATH, default={})
        self.cooldowns = {}

        # 캐시
        self.investor_cache = {}; self.investor_cache_ts = 0
        self.consensus_cache = {}; self.consensus_cache_ts = 0
        self.sector_cache = {}; self.sector_cache_ts = 0
        self.warning_cache = {}; self.warning_cache_ts = 0
        self.us_indices = []; self.us_indices_ts = 0
        self.us_etf_prices = {}  # 섹터 ETF 가격 — 매 갱신마다 업데이트
        self.last_prices = {}

        # dirty 마킹 (탭별)
        self.dirty = {self.TAB_US: True, self.TAB_HOLD: True,
                       self.TAB_WATCH: True}
        self.current_tab = self.TAB_HOLD

        # 윈도우
        self.root = tk.Tk()
        self.root.title("포트폴리오 모니터 v2")
        self.root.geometry("1280x780+50+50")
        self.root.configure(bg=COL_BG_APP)
        self.root.attributes("-topmost", False)

        try:
            from AppKit import NSApplication, NSApp
            NSApplication.sharedApplication().setActivationPolicy_(1)
            try:
                NSApp.activateIgnoringOtherApps_(True)
            except Exception:
                pass
        except Exception:
            pass
        self.root.lift()
        self.root.focus_force()

        self.root.protocol("WM_DELETE_WINDOW", self._on_quit)
        try:
            signal.signal(signal.SIGTERM, lambda *_: self._on_quit())
        except Exception:
            pass

        self._build_ui()
        self.interval_ms = self.config.get("polling_interval_seconds", 5) * 1000
        self.last_refresh_time = "--:--:--"
        self.remaining_sec = 0
        self._refresh_job = None
        self._countdown_job = None

        self._sync_historical_peaks()
        self.refresh()

    def _build_ui(self):
        style = ttk.Style()
        try:
            style.theme_use("aqua" if sys.platform == "darwin" else "clam")
        except Exception:
            pass
        style.configure("TNotebook", background=COL_BG_APP)
        style.configure("TNotebook.Tab", padding=(16, 8),
                         font=_font(12, "bold"))

        toolbar = tk.Frame(self.root, bg=COL_BG_APP)
        toolbar.pack(side="top", fill="x", padx=8, pady=(6, 0))

        self.topmost_var = tk.BooleanVar(value=False)
        tk.Checkbutton(toolbar, text="항상 위", variable=self.topmost_var,
                        bg=COL_BG_APP, fg=COL_PRIMARY,
                        font=_font(11),
                        command=self._toggle_topmost
                       ).pack(side="left")

        tk.Label(toolbar, text="투명도", bg=COL_BG_APP,
                  fg=COL_SECONDARY, font=_font(10)
                 ).pack(side="left", padx=(12, 4))
        self.alpha_var = tk.DoubleVar(value=1.0)
        tk.Scale(toolbar, from_=0.4, to=1.0, resolution=0.05,
                  orient="horizontal", variable=self.alpha_var,
                  showvalue=False, length=100,
                  bg=COL_BG_APP, fg=COL_PRIMARY,
                  highlightthickness=0,
                  command=lambda v: self.root.attributes("-alpha", float(v))
                 ).pack(side="left")

        tk.Button(toolbar, text="새로고침", command=self.refresh,
                   font=_font(11)).pack(side="left", padx=(12, 0))

        # 장마감 토글
        self.fade_sleeping_var = tk.BooleanVar(value=True)
        tk.Checkbutton(toolbar, text="장마감", variable=self.fade_sleeping_var,
                        bg=COL_BG_APP, fg=COL_PRIMARY, font=_font(11),
                        command=self._on_fade_toggle
                       ).pack(side="left", padx=(12, 0))

        # 액션 버튼들
        for txt, cmd in [
            ("💼 보유 추가",   self._add_holding),
            ("💼 보유 삭제",   self._prompt_delete_holding),
            ("⭐ 관심 추가",   self._add_watchlist),
            ("⭐ 관심 삭제",   self._prompt_delete_watchlist),
            ("📤 JSON 내보내기", self._export_holdings_json),
            ("📥 JSON 가져오기", self._import_holdings_json),
        ]:
            tk.Button(toolbar, text=txt, command=cmd,
                       font=_font(10)).pack(side="left", padx=(6, 0))

        self.time_label = tk.Label(toolbar, text="갱신: --:--:--",
                                    bg=COL_BG_APP, fg=COL_SECONDARY,
                                    font=_font(11))
        self.time_label.pack(side="right")

        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(side="top", fill="both", expand=True, padx=4, pady=4)

        # 미국 — v1 스타일 섹터 테이블 (ETF + 종목설명 포함)
        us_frame = tk.Frame(self.notebook, bg="white")
        self.notebook.add(us_frame, text="📈  미국 증시")
        self.us_panel = USIndicesV1Style(
            us_frame,
            on_open_url=lambda url: threading.Thread(
                target=lambda: open_toss_in_existing_tab(url), daemon=True
            ).start(),
        )

        # 보유
        hold_frame = tk.Frame(self.notebook, bg=COL_BG_APP)
        self.notebook.add(hold_frame, text="💼  보유 종목")
        self.holdings_panel = StockCardsCanvas(
            hold_frame, watchlist=False,
            on_click=self._on_card_click,
            on_right_click=self._on_card_right_click)

        # 관심
        watch_frame = tk.Frame(self.notebook, bg=COL_BG_APP)
        self.notebook.add(watch_frame, text="👀  관심 종목")
        self.watch_panel = StockCardsCanvas(
            watch_frame, watchlist=True,
            on_click=self._on_card_click,
            on_right_click=self._on_card_right_click)

        self.notebook.select(hold_frame)

        # 탭 전환 이벤트
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

    # ───────── 탭 전환 ─────────
    def _on_tab_changed(self, _evt):
        idx = self.notebook.index(self.notebook.select())
        self.current_tab = [self.TAB_US, self.TAB_HOLD, self.TAB_WATCH][idx]
        if self.dirty.get(self.current_tab):
            self._render_current_tab()

    def _toggle_topmost(self):
        self.root.attributes("-topmost", bool(self.topmost_var.get()))

    def _on_quit(self):
        try:
            for j in (self._refresh_job, self._countdown_job):
                if j:
                    self.root.after_cancel(j)
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass
        os._exit(0)

    # ───────── 클릭 ─────────
    def _on_card_click(self, ticker):
        url = f"https://tossinvest.com/stocks/A{ticker}"
        threading.Thread(target=lambda: open_toss_in_existing_tab(url),
                          daemon=True).start()

    def _on_card_right_click(self, ticker, event):
        m = tk.Menu(self.root, tearoff=0)
        m.add_command(label="토스에서 열기",
                       command=lambda: self._on_card_click(ticker))
        m.add_command(label="피크 초기화",
                       command=lambda: self._reset_peak(ticker))
        try:
            m.tk_popup(event.x_root, event.y_root)
        finally:
            m.grab_release()

    def _reset_peak(self, ticker):
        if ticker in self.peaks:
            del self.peaks[ticker]
            save_json(PEAKS_PATH, self.peaks)
            self._mark_dirty_all()
            self._render_current_tab()

    # ───────── 데이터 리로드 ─────────
    def _reload_data(self):
        """holdings.json 다시 읽고 분류 + 모든 탭 dirty + 즉시 갱신."""
        self.holdings_data = load_json(HOLDINGS_PATH, default={})
        all_stocks = self.holdings_data.get("holdings", [])
        self.holdings = [s for s in all_stocks if not (s.get("account") or "")]
        self.pension = [s for s in all_stocks if s.get("account") == "퇴직연금"]
        self.watchlist = [s for s in all_stocks if s.get("account") == "관심"]
        self.etf_holdings = [s for s in all_stocks if s.get("account") == "관심ETF"]
        self._mark_dirty_all()
        self.refresh()  # 가격 재조회 → 자동 렌더

    def _on_fade_toggle(self):
        """장마감 토글 — 다음 갱신 시 적용."""
        self._mark_dirty_all()
        self._render_current_tab()

    # ───────── 보유 추가/삭제 ─────────
    def _add_holding(self):
        from tkinter import messagebox
        prev_tm = self.root.attributes("-topmost")
        self.root.attributes("-topmost", False)
        dlg = tk.Toplevel(self.root)
        dlg.title("💼 보유 종목 추가")
        dlg.transient(self.root); dlg.grab_set()
        dlg.attributes("-topmost", True); dlg.lift(); dlg.focus_force()
        dlg.bind("<Destroy>", lambda e: self.root.attributes("-topmost", prev_tm)
                  if e.widget is dlg else None)

        frm = ttk.Frame(dlg, padding=12); frm.grid(sticky="nsew")
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
                messagebox.showerror("입력 오류", "6자리 숫자 종목코드 필요", parent=dlg); return
            try:
                shares = int(vars_[1].get().strip().replace(",", ""))
                avg_price = int(vars_[2].get().strip().replace(",", ""))
            except ValueError:
                messagebox.showerror("입력 오류", "수량/평균가는 숫자만", parent=dlg); return
            if shares <= 0 or avg_price <= 0:
                messagebox.showerror("입력 오류", "수량/평균가는 0보다 커야", parent=dlg); return
            buy_date = vars_[3].get().strip()
            if not (buy_date.isdigit() and len(buy_date) == 8):
                messagebox.showerror("입력 오류", "매수일 YYYYMMDD 8자리", parent=dlg); return
            existing = [s for s in self.holdings_data.get("holdings", [])
                         if s["ticker"] == code and (s.get("account") or "") in ("", "퇴직연금")]
            if existing:
                messagebox.showwarning("중복", "이미 보유/퇴직연금에 있음", parent=dlg); return
            result.update({"ok": True, "code": code, "shares": shares,
                            "avg_price": avg_price, "buy_date": buy_date})
            dlg.destroy()

        btns = ttk.Frame(frm); btns.grid(row=len(labels), column=0, columnspan=2, pady=(8, 0))
        ttk.Button(btns, text="추가", command=_submit).pack(side="left", padx=4)
        ttk.Button(btns, text="취소", command=dlg.destroy).pack(side="left", padx=4)
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
        name = _fetch_stock_name(code) or code
        invested = result["shares"] * result["avg_price"]
        self.holdings_data.setdefault("holdings", []).append({
            "ticker": code, "name": name, "shares": result["shares"],
            "avg_price": result["avg_price"], "invested": invested,
            "buy_date": result["buy_date"], "market": "KOSPI",
        })
        self.holdings_data["total_invested"] = (
            self.holdings_data.get("total_invested", 0) + invested)
        self.holdings_data.setdefault("history", []).append({
            "date": result["buy_date"], "event": "매수",
            "detail": f"{name} {result['shares']}주 @{result['avg_price']:,}",
        })
        save_json(HOLDINGS_PATH, self.holdings_data)
        self._reload_data()

    def _prompt_delete_holding(self):
        from tkinter import messagebox
        if not self.holdings:
            messagebox.showinfo("보유 종목", "보유 종목이 없습니다.", parent=self.root); return
        menu = tk.Menu(self.root, tearoff=0)
        for s in self.holdings:
            t = s["ticker"]
            menu.add_command(
                label=f"{s.get('name', t)} ({t}) {s.get('shares', 0)}주",
                command=lambda t=t: self._delete_holding(t))
        x = self.root.winfo_pointerx(); y = self.root.winfo_pointery()
        try:
            menu.tk_popup(x, y)
        finally:
            menu.grab_release()

    def _delete_holding(self, ticker):
        from tkinter import messagebox
        stock = next((s for s in self.holdings if s["ticker"] == ticker), None)
        if not stock:
            return
        name = stock.get("name", ticker)
        shares = stock.get("shares", 0)
        avg = stock.get("avg_price", 0)
        invested = stock.get("invested", shares * avg)
        if not messagebox.askyesno(
                "보유 종목 삭제",
                f"{name} ({ticker}) {shares}주 @{avg:,} 를 보유에서 제거할까요?\n\n"
                "전량 매도로 기록되고 관심으로 이동됩니다.",
                parent=self.root):
            return
        self.holdings_data["holdings"] = [
            s for s in self.holdings_data.get("holdings", [])
            if not (s["ticker"] == ticker and not (s.get("account") or ""))
        ]
        self.holdings_data["total_invested"] = max(
            0, self.holdings_data.get("total_invested", 0) - invested)
        self.holdings_data.setdefault("history", []).append({
            "date": datetime.now().strftime("%Y%m%d"), "event": "매도",
            "detail": f"{name} {shares}주 전량 매도 (avg @{avg:,})",
        })
        already_watch = any(
            s["ticker"] == ticker and s.get("account") in ("관심", "관심ETF")
            for s in self.holdings_data["holdings"])
        if not already_watch:
            self.holdings_data["holdings"].append({
                "ticker": ticker, "name": name, "shares": 0, "avg_price": 0,
                "invested": 0, "buy_date": "",
                "market": stock.get("market", "KOSPI"), "account": "관심",
            })
        save_json(HOLDINGS_PATH, self.holdings_data)
        self._reload_data()

    # ───────── 관심 추가/삭제 ─────────
    def _add_watchlist(self):
        from tkinter import messagebox
        prev_tm = self.root.attributes("-topmost")
        self.root.attributes("-topmost", False)
        dlg = tk.Toplevel(self.root); dlg.title("⭐ 관심 추가")
        dlg.transient(self.root); dlg.grab_set()
        dlg.attributes("-topmost", True); dlg.lift(); dlg.focus_force()
        dlg.bind("<Destroy>", lambda e: self.root.attributes("-topmost", prev_tm)
                  if e.widget is dlg else None)
        frm = ttk.Frame(dlg, padding=12); frm.grid(sticky="nsew")
        ttk.Label(frm, text="종목코드 (6자리)").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        code_var = tk.StringVar()
        e = ttk.Entry(frm, textvariable=code_var, width=20)
        e.grid(row=0, column=1, padx=4, pady=4); e.focus_set()
        result = {"ok": False}

        def _submit():
            code = code_var.get().strip()
            if not (code.isdigit() and len(code) == 6):
                messagebox.showerror("입력 오류", "6자리 숫자 종목코드 필요", parent=dlg); return
            if any(s["ticker"] == code for s in self.holdings_data.get("holdings", [])):
                messagebox.showwarning("중복", "이미 목록에 있음", parent=dlg); return
            result.update({"ok": True, "code": code}); dlg.destroy()

        btns = ttk.Frame(frm); btns.grid(row=1, column=0, columnspan=2, pady=(8, 0))
        ttk.Button(btns, text="추가", command=_submit).pack(side="left", padx=4)
        ttk.Button(btns, text="취소", command=dlg.destroy).pack(side="left", padx=4)
        dlg.bind("<Return>", lambda e: _submit())
        dlg.bind("<Escape>", lambda e: dlg.destroy())
        self.root.wait_window(dlg)
        if not result.get("ok"):
            return
        code = result["code"]
        name = _fetch_stock_name(code) or code
        self.holdings_data.setdefault("holdings", []).append({
            "ticker": code, "name": name, "shares": 0, "avg_price": 0,
            "invested": 0, "buy_date": "", "market": "KOSPI", "account": "관심",
        })
        save_json(HOLDINGS_PATH, self.holdings_data)
        self._reload_data()

    def _prompt_delete_watchlist(self):
        from tkinter import messagebox
        watches = [s for s in self.holdings_data.get("holdings", [])
                    if s.get("account") in ("관심", "관심ETF")]
        if not watches:
            messagebox.showinfo("관심 목록", "관심 주식/ETF 가 없습니다.", parent=self.root); return
        menu = tk.Menu(self.root, tearoff=0)
        for s in watches:
            t = s["ticker"]
            icon = "📊" if s.get("account") == "관심ETF" else "⭐"
            menu.add_command(label=f"{icon} {s.get('name', t)} ({t})",
                              command=lambda t=t: self._delete_watchlist(t))
        x = self.root.winfo_pointerx(); y = self.root.winfo_pointery()
        try:
            menu.tk_popup(x, y)
        finally:
            menu.grab_release()

    def _delete_watchlist(self, ticker):
        from tkinter import messagebox
        stock = next((s for s in self.holdings_data.get("holdings", [])
                       if s["ticker"] == ticker
                       and s.get("account") in ("관심", "관심ETF")), None)
        if not stock:
            return
        if not messagebox.askyesno("관심 삭제",
                f"{stock.get('name', ticker)} ({ticker}) 를 제거할까요?",
                parent=self.root):
            return
        self.holdings_data["holdings"] = [
            s for s in self.holdings_data.get("holdings", [])
            if not (s["ticker"] == ticker
                     and s.get("account") in ("관심", "관심ETF"))
        ]
        save_json(HOLDINGS_PATH, self.holdings_data)
        self._reload_data()

    # ───────── JSON 내보내기/가져오기 ─────────
    @staticmethod
    def _is_syncable(stock):
        return (stock.get("account") or "") in ("", "관심")

    def _export_holdings_json(self):
        import json as _json
        from tkinter import filedialog, messagebox
        filtered = [s for s in self.holdings_data.get("holdings", [])
                     if self._is_syncable(s)]
        text = _json.dumps({"holdings": filtered}, ensure_ascii=False, indent=2)

        dlg = tk.Toplevel(self.root); dlg.title("📤 JSON 내보내기")
        dlg.geometry("700x500")
        txt = tk.Text(dlg, wrap="none", font=("SF Mono", 11))
        txt.insert("1.0", text); txt.config(state="disabled")
        txt.pack(fill="both", expand=True, padx=6, pady=6)
        row = ttk.Frame(dlg); row.pack(fill="x", padx=6, pady=(0, 6))

        def _copy():
            self.root.clipboard_clear(); self.root.clipboard_append(text)
            messagebox.showinfo("복사", "클립보드에 복사됨", parent=dlg)

        def _save_file():
            from pathlib import Path as _P
            default = f"portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            initial = str(_P.home() / "Downloads")
            fpath = filedialog.asksaveasfilename(
                parent=dlg, initialdir=initial, initialfile=default,
                defaultextension=".json", filetypes=[("JSON", "*.json")])
            if fpath:
                _P(fpath).write_text(text, encoding="utf-8")
                messagebox.showinfo("저장", f"{fpath} 저장됨", parent=dlg)

        ttk.Button(row, text="닫기", command=dlg.destroy).pack(side="right", padx=2)
        ttk.Button(row, text="📋 복사", command=_copy).pack(side="right", padx=2)
        ttk.Button(row, text="💾 파일 저장", command=_save_file).pack(side="right", padx=2)

    def _import_holdings_json(self):
        import json as _json
        from tkinter import filedialog, messagebox
        dlg = tk.Toplevel(self.root); dlg.title("📥 JSON 가져오기")
        dlg.geometry("700x500")
        ttk.Label(dlg, text="holdings.json 내용을 붙여넣거나 파일 선택",
                   foreground="#666").pack(anchor="w", padx=6, pady=(6, 2))
        txt = tk.Text(dlg, wrap="none", font=("SF Mono", 11))
        txt.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        def _pick_file():
            from pathlib import Path as _P
            fpath = filedialog.askopenfilename(
                parent=dlg, initialdir=str(_P.home() / "Downloads"),
                filetypes=[("JSON", "*.json"), ("All", "*.*")])
            if fpath:
                try:
                    txt.delete("1.0", "end")
                    txt.insert("1.0", _P(fpath).read_text(encoding="utf-8"))
                except Exception as e:
                    messagebox.showerror("읽기 실패", str(e), parent=dlg)

        def _paste():
            try:
                txt.delete("1.0", "end")
                txt.insert("1.0", self.root.clipboard_get())
            except Exception:
                messagebox.showwarning("클립보드", "내용 없음", parent=dlg)

        def _apply():
            raw = txt.get("1.0", "end").strip()
            if not raw:
                messagebox.showwarning("입력 없음", "JSON 입력하세요", parent=dlg); return
            try:
                data = _json.loads(raw)
            except _json.JSONDecodeError as e:
                messagebox.showerror("JSON 파싱 실패", str(e), parent=dlg); return
            if not isinstance(data, dict) or "holdings" not in data:
                messagebox.showerror("형식 오류", "'holdings' 키 누락", parent=dlg); return
            if not isinstance(data["holdings"], list):
                messagebox.showerror("형식 오류", "'holdings' 는 배열", parent=dlg); return
            for i, s in enumerate(data["holdings"]):
                if not isinstance(s, dict) or not s.get("ticker"):
                    messagebox.showerror("형식 오류",
                                          f"{i}번에 ticker 누락", parent=dlg); return
            n_old = len(self.holdings_data.get("holdings", []))
            n_new = len(data["holdings"])
            if not messagebox.askyesno("확인",
                    f"현재 {n_old}개 → {n_new}개로 교체. 진행?", parent=dlg):
                return
            preserved = [s for s in self.holdings_data.get("holdings", [])
                          if not self._is_syncable(s)]
            incoming = [s for s in data["holdings"] if self._is_syncable(s)]
            self.holdings_data["holdings"] = preserved + incoming
            save_json(HOLDINGS_PATH, self.holdings_data)
            dlg.destroy()
            self._reload_data()
            messagebox.showinfo("완료",
                f"{len(incoming)}개 적용 (ETF/퇴직연금 {len(preserved)}개 보존)",
                parent=self.root)

        row = ttk.Frame(dlg); row.pack(fill="x", padx=6, pady=(0, 6))
        ttk.Button(row, text="취소", command=dlg.destroy).pack(side="right", padx=2)
        ttk.Button(row, text="적용", command=_apply).pack(side="right", padx=2)
        ttk.Button(row, text="📁 파일", command=_pick_file).pack(side="left", padx=2)
        ttk.Button(row, text="📋 붙여넣기", command=_paste).pack(side="left", padx=2)

    # ───────── 캐시 ─────────
    def _krx_tickers(self, source):
        return [s["ticker"] for s in source
                 if s.get("ticker", "").isdigit() and len(s["ticker"]) == 6]

    def _refresh_caches_async(self):
        import time as _t
        now = _t.time()
        # 보유 + 퇴직연금 + 관심 + ETF 모두 가격 fetch 대상
        all_stocks = (self.holdings + self.pension
                       + self.watchlist + self.etf_holdings)
        krx = self._krx_tickers(all_stocks)
        prices = fetch_toss_prices_batch(krx) if krx else {}

        if krx and (now - self.investor_cache_ts > self.INVESTOR_TTL or not self.investor_cache):
            with ThreadPoolExecutor(max_workers=min(len(krx), 8)) as pool:
                for t, r in zip(krx, pool.map(fetch_investor_flow, krx)):
                    if r:
                        self.investor_cache[t] = r
            self.investor_cache_ts = now

        if krx and (now - self.consensus_cache_ts > self.CONSENSUS_TTL or not self.consensus_cache):
            with ThreadPoolExecutor(max_workers=min(len(krx), 8)) as pool:
                for t, r in zip(krx, pool.map(fetch_target_consensus, krx)):
                    if r:
                        self.consensus_cache[t] = r
            self.consensus_cache_ts = now

        if krx and (now - self.sector_cache_ts > self.SECTOR_TTL or not self.sector_cache):
            with ThreadPoolExecutor(max_workers=min(len(krx), 8)) as pool:
                for t, r in zip(krx, pool.map(fetch_stock_sector, krx)):
                    if r:
                        self.sector_cache[t] = r
            self.sector_cache_ts = now

        if krx and (now - self.warning_cache_ts > self.WARNING_TTL or not self.warning_cache):
            with ThreadPoolExecutor(max_workers=min(len(krx), 8)) as pool:
                for t, r in zip(krx, pool.map(fetch_stock_warning, krx)):
                    self.warning_cache[t] = r
            self.warning_cache_ts = now

        if now - self.us_indices_ts > self.US_TTL or not self.us_indices:
            try:
                self.us_indices = fetch_us_indices_with_futures()
                self.us_indices_ts = now
            except Exception as e:
                print(f"[WARN] US fetch fail: {e}")

        # 섹터 ETF 가격 — 가격 fetch 와 합쳐 한 번에 처리
        etf_tickers = [t for lst in USIndicesV1Style.ETFS_BY_SECTOR.values() for t in lst]
        # holdings 에 이미 있는 ETF 는 위에서 받은 prices 에 포함됨 → 별도 호출 불필요
        # 부족한 ETF (혹시 보유에 없으면) 만 추가 fetch
        missing = [t for t in etf_tickers if t not in prices]
        if missing:
            extra = fetch_toss_prices_batch(missing)
            prices.update(extra)
        # us_etf_prices 는 ETF 만 추출
        self.us_etf_prices = {t: prices[t] for t in etf_tickers if t in prices}

        self.last_prices = prices
        self.root.after(0, self._after_refresh)

    def _sync_historical_peaks(self):
        for s in self.holdings:
            t = s["ticker"]
            buy = s.get("buy_date")
            if not (t.isdigit() and len(t) == 6 and buy):
                continue
            if t not in self.peaks:
                try:
                    p = fetch_peak_since_buy(t, buy)
                    if p:
                        self.peaks[t] = p
                except Exception:
                    pass
        save_json(PEAKS_PATH, self.peaks)

    def _mark_dirty_all(self):
        for k in self.dirty:
            self.dirty[k] = True

    def _after_refresh(self):
        # 피크 갱신 (보유 + 퇴직연금)
        for s in self.holdings + self.pension:
            t = s["ticker"]
            cur = (self.last_prices.get(t) or {}).get("price", 0)
            if cur and cur > (self.peaks.get(t) or 0):
                self.peaks[t] = cur
        save_json(PEAKS_PATH, self.peaks)

        # 모든 탭 dirty 마킹
        self._mark_dirty_all()

        # 알림 (보유 + 퇴직연금)
        for s in self.holdings + self.pension:
            t = s["ticker"]
            cur = (self.last_prices.get(t) or {}).get("price", 0)
            avg = s.get("avg_price", 0)
            if not (cur and avg):
                continue
            sell_fee_pct = self.config.get("sell_fee_pct", 0.2)
            net_price = cur * (1 - sell_fee_pct / 100)
            pnl_pct = (net_price - avg) / avg * 100
            peak = self.peaks.get(t, 0)
            from_peak_pct = ((cur - peak) / peak * 100) if peak else 0
            self._check_alert(s, cur, peak, pnl_pct, from_peak_pct)

        # 현재 탭만 즉시 렌더
        self._render_current_tab()

        self.last_refresh_time = datetime.now().strftime("%H:%M:%S")
        self._start_countdown()

        if self._refresh_job:
            try:
                self.root.after_cancel(self._refresh_job)
            except Exception:
                pass
        self._refresh_job = self.root.after(self.interval_ms, self.refresh)

    def _render_current_tab(self):
        caches = {
            "investor": self.investor_cache,
            "warning": self.warning_cache,
            "sector": self.sector_cache,
            "consensus": self.consensus_cache,
            "sell_fee_pct": self.config.get("sell_fee_pct", 0.2),
            "kr_closed": not is_market_open("KR"),
            "fade_sleeping": bool(self.fade_sleeping_var.get()),
        }
        def _totals(stocks):
            inv = cur = yes = 0
            for s in stocks:
                pd = self.last_prices.get(s["ticker"]) or {}
                inv += s.get("avg_price", 0) * s.get("shares", 0)
                cur += pd.get("price", 0) * s.get("shares", 0)
                yes += pd.get("base", 0) * s.get("shares", 0)
            return inv, cur, yes

        if self.current_tab == self.TAB_US:
            # ETF 매핑은 etf_holdings (account="관심ETF") 기준
            self.us_panel.render(self.us_indices, self.etf_holdings,
                                  etf_prices=self.us_etf_prices,
                                  fade_sleeping=bool(self.fade_sleeping_var.get()))
        elif self.current_tab == self.TAB_HOLD:
            hold_inv, hold_cur, hold_yes = _totals(self.holdings)
            pen_inv, pen_cur, pen_yes = _totals(self.pension)
            self.holdings_panel.render_grouped(
                [
                    {"stocks": self.holdings, "label": "보유 합계",
                     "totals": (hold_inv, hold_cur, hold_yes)},
                    {"stocks": self.pension, "label": "퇴직연금 합계",
                     "totals": (pen_inv, pen_cur, pen_yes)},
                ],
                self.last_prices, self.peaks, self.config, caches,
            )
        elif self.current_tab == self.TAB_WATCH:
            self.watch_panel.render(
                self.watchlist, self.last_prices, self.peaks, self.config, caches,
            )
        self.dirty[self.current_tab] = False

    def refresh(self):
        threading.Thread(target=self._refresh_caches_async, daemon=True).start()

    # ───────── 알림 ─────────
    def _check_alert(self, stock, current_price, peak_price, pnl_pct, from_peak_pct):
        ticker = stock["ticker"]
        name = stock.get("name", ticker)
        buy_price = stock["avg_price"]
        stop_pct = self.config["stop_loss_alert_pct"]
        trail_pct = self.config["trailing_stop_alert_pct"]
        cooldown_min = self.config.get("alert_cooldown_minutes", 15)
        now = datetime.now()

        def in_cooldown(kind):
            key = f"{ticker}_{kind}"
            last = self.cooldowns.get(key)
            return last and (now - last).total_seconds() < cooldown_min * 60

        if pnl_pct <= stop_pct and not in_cooldown("stop_loss"):
            self.cooldowns[f"{ticker}_stop_loss"] = now
            self._save_alert("stop_loss", ticker, name, buy_price, current_price,
                              pnl_pct, None, None)
        elif pnl_pct > 0 and from_peak_pct <= trail_pct and not in_cooldown("trailing"):
            self.cooldowns[f"{ticker}_trailing"] = now
            self._save_alert("trailing_stop", ticker, name, buy_price, current_price,
                              pnl_pct, peak_price, from_peak_pct)

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

    # ───────── 카운트다운 ─────────
    def _start_countdown(self):
        self.remaining_sec = self.config.get("polling_interval_seconds", 5)
        self._tick_countdown()

    def _tick_countdown(self):
        if hasattr(self, "time_label"):
            self.time_label.config(
                text=f"갱신: {self.last_refresh_time} ({self.remaining_sec}초)"
            )
        if self.remaining_sec > 0:
            self.remaining_sec -= 1
            self._countdown_job = self.root.after(1000, self._tick_countdown)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    PortfolioWindowV2().run()
