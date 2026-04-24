"""탭 2: 보유종목 — 토스 스타일 단순 2행 레이아웃 + 데스크탑 뱃지

행 1: [💤? 종목명 pill] (보유량)    매수가     손익금액 (손익%)
행 2: (섹터)                          (옅은 회색 작게)
행 3: 거래량                          현재가     전일대비%
"""
import threading
from datetime import datetime

from kivy.clock import mainthread
from kivy.metrics import sp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView

from data_service import (fetch_toss_prices_batch, sign_color, format_signed,
                           refresh_warning_sector_cache,
                           refresh_nxt_cache, refresh_consensus_cache,
                           refresh_investor_cache,
                           warning_cache, sector_cache, nxt_support_cache,
                           consensus_cache, investor_cache,
                           load_peaks, load_thresholds)


def kr_session_phase() -> str:
    """REGULAR(09:00-15:20 평일) | EXTENDED | CLOSED"""
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

FONT_XS = sp(11)
FONT_SMALL = sp(12)
FONT_MD = sp(14)
FONT_LG = sp(15)

# 매도 수수료 0.2% (토스 기준) — 데스크탑과 동일
SELL_FEE_PCT = 0.2
FEE_MULTIPLIER = 1 - (SELL_FEE_PCT / 100)

# 4컬럼 폭 비율 (행 1·2 공통)
COL_NAME = 0.36   # 종목명 / 거래량
COL_PRICE = 0.18  # 매수가 / 현재가
COL_PNL = 0.23    # 손익금액(%) / 전일대비(%)
COL_PEAK = 0.23   # 피크가(%) / 목표가(%)


_NAMED = {"white": "#ffffff", "black": "#000000", "gray": "#888888",
           "grey": "#888888", "red": "#ff0000", "blue": "#0000ff"}


def rgba(s, alpha=1.0):
    if not s:
        return (1, 1, 1, alpha)
    if s in _NAMED:
        s = _NAMED[s]
    h = s.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    try:
        return (int(h[0:2], 16) / 255, int(h[2:4], 16) / 255,
                 int(h[4:6], 16) / 255, alpha)
    except Exception:
        return (0.5, 0.5, 0.5, alpha)


def _fade_hex(color: str, ratio: float = 0.7) -> str:
    """데스크톱과 동일: hex 색을 흰색과 혼합해 투명도 효과."""
    if not isinstance(color, str) or not color.startswith("#"):
        return color
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


def _bind_align(lbl):
    """halign/valign 이 실제 적용되도록 text_size 를 위젯 크기에 바인딩."""
    lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
    return lbl


def _estimate_text_width(text: str, font_size, bold=False) -> float:
    """간단한 텍스트 폭 추정 (한글 1.0배, 영문 0.55배)"""
    w = 0
    for c in text:
        if ord(c) > 127:
            w += font_size * 1.0
        else:
            w += font_size * 0.55
    if bold:
        w *= 1.05
    return w


def _pill_label(text, fg, bg, font_size=None, bold=True, height=None):
    """텍스트 폭만 차지하는 알약(pill) 라벨 — 폭 사전 추정"""
    from kivy.graphics import Color, RoundedRectangle
    fs = font_size or sp(15)
    h = height or sp(24)
    pad_x = sp(8)
    text_w = _estimate_text_width(text, fs, bold=bold)
    width = text_w + pad_x * 2

    lbl = Label(text=text, font_size=fs, bold=bold,
                 color=rgba(fg), size_hint=(None, None),
                 width=width, height=h,
                 halign="center", valign="middle",
                 text_size=(width, h))

    with lbl.canvas.before:
        Color(*rgba(bg))
        lbl._pill = RoundedRectangle(pos=lbl.pos, size=lbl.size,
                                       radius=[sp(6)])

    def _on_resize(w, _v):
        w._pill.pos = w.pos
        w._pill.size = w.size
    lbl.bind(pos=_on_resize, size=_on_resize)
    return lbl


def _name_pill_colors(account, warn_text, pnl_pct,
                        is_stop=False, is_peak_drop=False):
    """종목명 pill 의 (fg, bg) 결정 — 데스크탑 룰 매핑

    우선순위 (높음→낮음):
      1) is_stop                        → 짙은 파랑 + 흰글  (손절)
      2) is_peak_drop + pnl > 0         → 빨강 + 흰글       (익절)
      3) is_peak_drop + pnl < 0         → 옅은 파랑 + 흰글  (⚠ 하락)
      4) 위험/관리                       → 빨강 + 흰글
      5) 경고/과열                       → 주황 + 흰글
      6) 정지                            → 회색 + 회글
      7) 주의                            → 옅은 노랑
      8) 퇴직연금                        → 옅은 회색
      9) default                        → 카톡 노랑
    """
    if is_stop:
        return ("#ffffff", "#1f4e8f")
    if is_peak_drop and pnl_pct > 0:
        return ("#ffffff", "#c0392b")
    if is_peak_drop:
        return ("#ffffff", "#4a90c2")
    if warn_text in ("위험", "관리"):
        return ("#ffffff", "#c0392b")
    if warn_text in ("경고", "과열"):
        return ("#ffffff", "#e67e22")
    if warn_text == "정지":
        return ("#444444", "#e8e8e8")
    if warn_text == "주의":
        return (sign_color(pnl_pct), "#fff3cc")
    if account == "퇴직연금":
        return (sign_color(pnl_pct), "#ecf0f3")
    return (sign_color(pnl_pct), "#FEE500")


def format_volume(v: int) -> str:
    """거래량 포맷 (한국식): 123,456,789 -> 1.2억, 1,094,000 -> 109.4만"""
    try:
        v = int(v)
    except Exception:
        return "-"
    if v >= 100_000_000:        # 1억 이상
        return f"{v / 100_000_000:.1f}억"
    if v >= 10_000:             # 1만 이상
        return f"{v / 10_000:.1f}만"
    return f"{v:,}"


class TabHoldings(BoxLayout):
    def __init__(self, holdings_data, **kwargs):
        super().__init__(orientation="vertical", **kwargs)
        self.holdings_data = holdings_data

        self.scroll = ScrollView(do_scroll_x=False, do_scroll_y=True,
                                   bar_width=sp(4))
        self.container = BoxLayout(orientation="vertical",
                                    size_hint_y=None,
                                    spacing=sp(1),
                                    padding=(sp(8), sp(8)))
        self.container.bind(minimum_height=self.container.setter("height"))
        self.scroll.add_widget(self.container)
        self.add_widget(self.scroll)

        # 스크롤 밖 고정 합계 영역 — 버튼 위에 항상 떠있음
        self.total_container = BoxLayout(orientation="vertical",
                                           size_hint_y=None, height=0)
        self.total_container.bind(minimum_height=self.total_container.setter("height"))
        self.add_widget(self.total_container)

        # 하단 툴바: [+ 보유추가] [- 보유삭제]  [✓ 장마감투명]
        from kivy.uix.button import Button
        from kivy.uix.checkbox import CheckBox
        from ui.dialogs import show_add_holding, show_delete_holding
        from ui import app_state
        toolbar = BoxLayout(orientation="horizontal", size_hint_y=None,
                             height=sp(44), spacing=sp(6),
                             padding=(sp(8), sp(6)))
        from kivy.graphics import Color, Rectangle
        with toolbar.canvas.before:
            Color(*rgba("#f0f2f5"))
            toolbar._bg = Rectangle(pos=toolbar.pos, size=toolbar.size)
        toolbar.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                      size=lambda w, v: setattr(w._bg, "size", v))

        def _tbtn(text, color, on_release):
            b = Button(text=text, font_size=sp(14), bold=True,
                        color=(1, 1, 1, 1),
                        background_color=color, background_normal="")
            b.bind(on_release=on_release)
            return b

        toolbar.add_widget(_tbtn("+ 보유 추가", (0.2, 0.5, 0.9, 1),
                                    lambda *_: show_add_holding(
                                        self.holdings_data, self.refresh)))
        toolbar.add_widget(_tbtn("- 보유 삭제", (0.75, 0.35, 0.35, 1),
                                    lambda *_: show_delete_holding(
                                        self.holdings_data, self.refresh)))

        # 장마감 투명도 체크박스 영역
        fade_box = BoxLayout(orientation="horizontal", size_hint_x=None,
                              width=sp(110), spacing=sp(2))
        self.fade_cb = CheckBox(size_hint_x=None, width=sp(28),
                                  active=app_state.get("fade_sleeping"))
        self.fade_cb.bind(active=self._on_fade_toggle)
        fade_box.add_widget(self.fade_cb)
        fade_box.add_widget(Label(
            text="장마감\n투명", font_size=sp(10),
            color=(0.3, 0.3, 0.3, 1), halign="left", valign="middle",
            text_size=(sp(70), sp(36))))
        toolbar.add_widget(fade_box)
        self.add_widget(toolbar)

        self.prices = {}

        self.container.add_widget(Label(
            text="로딩 중...", font_size=FONT_MD, color=rgba("#888"),
            size_hint_y=None, height=sp(30)))

    def refresh(self):
        threading.Thread(target=self._fetch_and_render, daemon=True).start()

    def _on_fade_toggle(self, _cb, active):
        from ui import app_state
        app_state.set("fade_sleeping", bool(active))
        self.refresh()

    def _fetch_and_render(self):
        holdings = [s for s in self.holdings_data.get("holdings", [])
                    if (not s.get("account")
                         or s["account"] == "퇴직연금")]
        tickers = [s["ticker"] for s in holdings]

        refresh_warning_sector_cache(tickers)
        refresh_nxt_cache(tickers)
        refresh_consensus_cache(tickers)
        refresh_investor_cache(tickers)

        prices = fetch_toss_prices_batch(tickers)
        peaks = load_peaks()
        thresholds = load_thresholds()
        self._render(holdings, prices, peaks, thresholds)

    @mainthread
    def _render(self, holdings, prices, peaks, thresholds):
        self.container.clear_widgets()
        self.container.add_widget(self._build_header())
        total_invested = 0
        total_current = 0
        total_yesterday = 0

        phase = kr_session_phase()

        def _is_sleeping(t, vol):
            """per-stock sleeping — REGULAR 항상 활성 / EXTENDED 는 NXT+거래량 / 그외 sleeping"""
            if phase == "REGULAR":
                return False
            if phase == "EXTENDED":
                return not (nxt_support_cache.get(t) and vol > 0)
            return True

        # 정렬: 계정(일반→퇴직) → 비sleeping 우선 → 전일대비% 내림차순
        def _sort_key(s):
            t = s["ticker"]
            account_rank = 0 if not s.get("account") else 1
            d = prices.get(t, {})
            p, b = d.get("price", 0), d.get("base", 0)
            day_pct = ((p - b) / b * 100) if (p and b) else 0
            sleep_rank = 1 if _is_sleeping(t, d.get("volume", 0)) else 0
            return (account_rank, sleep_rank, -day_pct)

        holdings = sorted(holdings, key=_sort_key)

        for stock in holdings:
            t = stock["ticker"]
            shares = stock.get("shares", 0)
            avg = stock.get("avg_price", 0)
            invested = shares * avg
            price_info = prices.get(t, {})
            cur_price = price_info.get("price", 0) or avg
            base_price = price_info.get("base", 0) or cur_price
            volume = price_info.get("volume", 0)
            net_price = cur_price * FEE_MULTIPLIER
            current_val = round(net_price * shares)
            yesterday_val = round(base_price * FEE_MULTIPLIER * shares)

            total_invested += invested
            total_current += current_val
            total_yesterday += yesterday_val

            sleeping = _is_sleeping(t, volume)
            peak = max(peaks.get(t, cur_price), cur_price)
            self.container.add_widget(
                self._build_holding_row(stock, cur_price, base_price, volume,
                                         invested, current_val,
                                         peak=peak, thresholds=thresholds,
                                         sleeping=sleeping))

        # 합계 는 스크롤 밖 고정 컨테이너에 둠 (버튼 위에 항상 표시)
        self.total_container.clear_widgets()
        self.total_container.add_widget(
            self._build_total_row(total_invested, total_current, total_yesterday))

    def _build_header(self):
        """2행 4컬럼 헤더 (회색 배경)"""
        box = BoxLayout(orientation="vertical", size_hint_y=None,
                         padding=(sp(4), sp(6)), spacing=sp(2))
        box.bind(minimum_height=box.setter("height"))
        from kivy.graphics import Color, Rectangle
        with box.canvas.before:
            Color(*rgba("#eef1f5"))
            box._bg = Rectangle(pos=box.pos, size=box.size)
        box.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                  size=lambda w, v: setattr(w._bg, "size", v))

        def _hdr_cell(text, sx, halign):
            return Label(
                text=text, bold=True, font_size=FONT_XS,
                color=rgba("#666"), halign=halign, valign="middle",
                size_hint_x=sx, text_size=(None, sp(18)))

        l1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        l1.add_widget(_hdr_cell("종목명 (보유량)", COL_NAME, "left"))
        l1.add_widget(_hdr_cell("매수가", COL_PRICE, "right"))
        l1.add_widget(_hdr_cell("손익금액 (%)", COL_PNL, "right"))
        l1.add_widget(_hdr_cell("피크가 (%)", COL_PEAK, "right"))
        box.add_widget(l1)

        l2 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        l2.add_widget(_hdr_cell("거래량", COL_NAME, "left"))
        l2.add_widget(_hdr_cell("현재가", COL_PRICE, "right"))
        l2.add_widget(_hdr_cell("전일대비 (%)", COL_PNL, "right"))
        l2.add_widget(_hdr_cell("목표가 (%)", COL_PEAK, "right"))
        box.add_widget(l2)

        l3 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        l3.add_widget(_hdr_cell("투자의견", COL_NAME, "left"))
        l3.add_widget(_hdr_cell("기관", COL_PRICE, "right"))
        l3.add_widget(_hdr_cell("외국인(보유%)", COL_PNL, "right"))
        l3.add_widget(_hdr_cell("연기금", COL_PEAK, "right"))
        box.add_widget(l3)
        return box

    def _build_holding_row(self, stock, cur_price, base_price, volume,
                            invested, current_val,
                            peak=None, thresholds=None, sleeping=False):
        ticker = stock["ticker"]
        name = stock.get("name", ticker)
        avg = stock.get("avg_price", 0)
        shares = stock.get("shares", 0)
        account = stock.get("account") or ""
        # 손익금액 — 데스크탑과 동일: round((net_price - avg) * shares)
        net_price = cur_price * FEE_MULTIPLIER
        pnl = round((net_price - avg) * shares)
        pnl_pct = ((net_price - avg) / avg * 100) if avg else 0
        # 전일대비 — 주당 절대 변동
        day_diff = cur_price - base_price
        day_pct = (day_diff / base_price * 100) if base_price else 0

        warn_text = warning_cache.get(ticker) or ""
        is_sleeping = sleeping

        # 휴면 시 색상을 흰색 쪽으로 70% 페이드 (데스크톱과 동일)
        # 단, 사용자가 장마감 투명도를 끈 경우 페이드 생략
        from ui import app_state
        fade_on = app_state.get("fade_sleeping")
        def _c(hex_color):
            return _fade_hex(hex_color, 0.7) if (is_sleeping and fade_on) else hex_color

        # 손절 / 피크드롭 판정 (데스크탑과 동일)
        thresholds = thresholds or {}
        stop_th = thresholds.get("stop_loss_alert_pct", -9.0)
        trail_th = thresholds.get("trailing_stop_alert_pct", -9.0)
        is_stop = pnl_pct <= stop_th
        from_peak_pct = ((cur_price - peak) / peak * 100) if peak else 0
        is_peak_drop = (from_peak_pct <= trail_th
                         and abs(from_peak_pct) >= 0.01)

        # 컨테이너
        box = BoxLayout(orientation="vertical", size_hint_y=None,
                         padding=(sp(4), sp(6)), spacing=sp(2))
        box.bind(minimum_height=box.setter("height"))

        from kivy.graphics import Color, Rectangle, Line
        with box.canvas.before:
            Color(*rgba("#ffffff"))
            box._bg = Rectangle(pos=box.pos, size=box.size)
            Color(*rgba("#eeeeee"))
            box._line = Line(points=[0, 0, 1, 0], width=1)

        def _on_box_resize(w, _v):
            w._bg.pos = w.pos
            w._bg.size = w.size
            x, y = w.pos
            w._line.points = [x, y, x + w.size[0], y]
        box.bind(pos=_on_box_resize, size=_on_box_resize)

        # 피크 / 목표 + 괴리율
        target = (consensus_cache.get(ticker) or {}).get("target")
        peak_gap_pct = ((cur_price - peak) / peak * 100) if peak else 0
        target_gap_pct = ((target - cur_price) / cur_price * 100) if (target and cur_price) else 0

        # ─── 행 1: 종목명 (보유량) | 매수가 | 손익금액(%) | 피크가(%)
        # — 관심종목 탭과 동일 테이블 구조: 플레인 텍스트 + 좌측정렬, 상태는 작은 우측 뱃지
        l1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(28), spacing=sp(4))
        prefix = "zZ " if is_sleeping else ""
        if account == "퇴직연금":
            prefix += "[퇴] "
        # 상태 뱃지 텍스트 (손절 / warn_text — 익절·피크 뱃지는 표시 안 함)
        if is_stop:
            badge_text, badge_bg = "손절", "#1f4e8f"
        elif warn_text:
            badge_text, badge_bg = warn_text, {
                "위험": "#c0392b", "관리": "#c0392b",
                "경고": "#e67e22", "과열": "#e67e22",
                "정지": "#e8e8e8", "주의": "#ffd54f",
            }.get(warn_text, "#888")
        else:
            badge_text, badge_bg = "", None

        name_col = BoxLayout(orientation="horizontal", size_hint_x=COL_NAME,
                              spacing=sp(4))
        # 종목명 — 텍스트 실제 폭만 차지하도록 바인딩 (뱃지가 이름 바로 옆에 붙도록)
        name_lbl = Label(
            text=f"{prefix}{name} ({shares})",
            bold=not (is_sleeping and fade_on), font_size=FONT_LG,
            color=rgba(_c(sign_color(pnl_pct))),
            size_hint=(None, 1),
            halign="left", valign="middle")
        name_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
        name_col.add_widget(name_lbl)
        if badge_text:
            from kivy.graphics import Color, RoundedRectangle
            badge_w = sp(28)
            badge = Label(
                text=badge_text, bold=True, font_size=sp(9),
                color=rgba(_c("#ffffff")),
                size_hint=(None, None), width=badge_w, height=sp(16),
                halign="center", valign="middle",
                text_size=(badge_w, sp(16)))
            with badge.canvas.before:
                Color(*rgba(_c(badge_bg)))
                badge._bg = RoundedRectangle(pos=badge.pos, size=badge.size,
                                              radius=[sp(4)])
            badge.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                        size=lambda w, v: setattr(w._bg, "size", v))
            name_col.add_widget(badge)
        # 우측 스페이서 — 이름+뱃지를 좌측에 밀착시킴
        name_col.add_widget(BoxLayout())
        l1.add_widget(name_col)
        l1.add_widget(Label(
            text=f"{avg:,}원", font_size=FONT_MD,
            color=rgba(_c("#888")), halign="right", valign="middle",
            size_hint_x=COL_PRICE, text_size=(None, sp(28))))
        l1.add_widget(Label(
            text=f"{format_signed(pnl)} ({pnl_pct:+.2f}%)",
            bold=not (is_sleeping and fade_on), font_size=FONT_MD,
            color=rgba(_c(sign_color(pnl))), halign="right", valign="middle",
            size_hint_x=COL_PNL, text_size=(None, sp(28))))
        if peak:
            l1.add_widget(Label(
                text=f"{int(peak):,} ({peak_gap_pct:+.2f}%)",
                font_size=FONT_MD,
                color=rgba(_c(sign_color(peak_gap_pct) if peak_gap_pct < 0 else "#888")),
                halign="right", valign="middle",
                size_hint_x=COL_PEAK, text_size=(None, sp(28))))
        else:
            l1.add_widget(Label(text="-", color=rgba(_c("#aaa")),
                                  size_hint_x=COL_PEAK, halign="right",
                                  valign="middle", text_size=(None, sp(28))))
        box.add_widget(l1)

        # ─── 행 2: 거래량 | 현재가 | 전일대비(%) | 목표가(%)
        l2 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(24), spacing=sp(4))
        l2.add_widget(Label(
            text=format_volume(volume),
            font_size=FONT_SMALL, color=rgba(_c("#888")),
            halign="left", valign="middle",
            size_hint_x=COL_NAME, text_size=(None, sp(24))))
        l2.add_widget(Label(
            text=f"{cur_price:,}원", font_size=FONT_MD,
            color=rgba(_c("#444")), halign="right", valign="middle",
            size_hint_x=COL_PRICE, text_size=(None, sp(24))))
        l2.add_widget(Label(
            text=f"{format_signed(day_diff)} ({day_pct:+.2f}%)",
            font_size=FONT_MD, bold=not (is_sleeping and fade_on),
            color=rgba(_c(sign_color(day_diff))),
            halign="right", valign="middle",
            size_hint_x=COL_PNL, text_size=(None, sp(24))))
        if target:
            l2.add_widget(Label(
                text=f"{target:,} ({target_gap_pct:+.2f}%)",
                font_size=FONT_MD,
                color=rgba(_c(sign_color(target_gap_pct))),
                halign="right", valign="middle",
                size_hint_x=COL_PEAK, text_size=(None, sp(24))))
        else:
            l2.add_widget(Label(text="-", color=rgba(_c("#aaa")),
                                  size_hint_x=COL_PEAK, halign="right",
                                  valign="middle", text_size=(None, sp(24))))
        box.add_widget(l2)

        # ─── 행 3: 투자의견 | 기관 | 외국인(보유%) | 연기금
        flow = investor_cache.get(ticker) or {}
        consensus = consensus_cache.get(ticker) or {}
        l3 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(24), spacing=sp(4))

        # 투자의견 — 점수 우선, 없으면 텍스트 (데스크톱과 동일 규칙)
        opinion_text = consensus.get("opinion") or ""
        score = consensus.get("score")
        if opinion_text or score is not None:
            op_color = ("#c0392b" if "매수" in opinion_text else
                         "#1f4e8f" if "매도" in opinion_text else "#555")
            if score is not None:
                op_display = f"{score:.2f}"
            else:
                op_display = opinion_text or "-"
        else:
            op_display, op_color = "-", "#aaa"
        l3.add_widget(Label(
            text=op_display, font_size=FONT_MD,
            color=rgba(_c(op_color)),
            halign="left", valign="middle",
            size_hint_x=COL_NAME, text_size=(None, sp(24))))

        if flow:
            inst = flow.get("기관", 0)
            foreign = flow.get("외국인", 0)
            foreign_ratio = flow.get("외국인비율", 0)
            pension = flow.get("연기금", 0)
            # 기관
            l3.add_widget(Label(
                text=format_signed(inst), font_size=FONT_MD,
                color=rgba(_c(sign_color(inst))),
                halign="right", valign="middle",
                size_hint_x=COL_PRICE, text_size=(None, sp(24))))
            # 외국인 순매수 + (보유%)
            foreign_text = format_signed(foreign)
            if foreign_ratio > 0:
                foreign_text += f" ({foreign_ratio:.2f}%)"
            l3.add_widget(Label(
                text=foreign_text, font_size=FONT_MD,
                color=rgba(_c(sign_color(foreign))),
                halign="right", valign="middle",
                size_hint_x=COL_PNL, text_size=(None, sp(24))))
            # 연기금
            l3.add_widget(Label(
                text=format_signed(pension), font_size=FONT_MD,
                color=rgba(_c(sign_color(pension))),
                halign="right", valign="middle",
                size_hint_x=COL_PEAK, text_size=(None, sp(24))))
        else:
            for col_w in (COL_PRICE, COL_PNL, COL_PEAK):
                l3.add_widget(Label(text="-", color=rgba(_c("#aaa")),
                                      size_hint_x=col_w, halign="right",
                                      valign="middle",
                                      text_size=(None, sp(24))))
        box.add_widget(l3)
        return box

    def _build_total_row(self, invested, current, yesterday):
        pnl = current - invested
        pnl_pct = (pnl / invested * 100) if invested else 0
        day_diff = current - yesterday
        day_pct = (day_diff / yesterday * 100) if yesterday else 0

        box = BoxLayout(orientation="vertical", size_hint_y=None,
                         padding=(sp(4), sp(8)), spacing=sp(3))
        box.bind(minimum_height=box.setter("height"))
        from kivy.graphics import Color, Rectangle
        with box.canvas.before:
            Color(*rgba("#f5f6f8"))
            box._bg = Rectangle(pos=box.pos, size=box.size)
        box.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                  size=lambda w, v: setattr(w._bg, "size", v))

        # 행 1: 매수가 합계 | invested | 누적손익(%)
        l1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(24), spacing=sp(4))
        l1.add_widget(Label(
            text="매수가 합계", bold=True, font_size=FONT_MD,
            color=rgba("#000"), size_hint_x=COL_NAME, halign="left",
            valign="middle", text_size=(None, sp(24))))
        l1.add_widget(Label(
            text=f"{int(invested):,}원", bold=True, font_size=FONT_MD,
            color=rgba("#222"), size_hint_x=COL_PRICE, halign="right",
            valign="middle", text_size=(None, sp(24))))
        l1.add_widget(Label(
            text=f"{format_signed(pnl)} ({pnl_pct:+.2f}%)",
            bold=True, font_size=FONT_MD,
            color=rgba(sign_color(pnl)), size_hint_x=COL_PNL, halign="right",
            valign="middle", text_size=(None, sp(24))))
        l1.add_widget(BoxLayout(size_hint_x=COL_PEAK))
        box.add_widget(l1)

        # 행 2: 현재가 합계 | current | 전일대비(%)
        l2 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(24), spacing=sp(4))
        l2.add_widget(Label(
            text="현재가 합계", bold=True, font_size=FONT_MD,
            color=rgba("#000"), size_hint_x=COL_NAME, halign="left",
            valign="middle", text_size=(None, sp(24))))
        l2.add_widget(Label(
            text=f"{int(current):,}원", bold=True, font_size=FONT_MD,
            color=rgba("#222"), size_hint_x=COL_PRICE, halign="right",
            valign="middle", text_size=(None, sp(24))))
        l2.add_widget(Label(
            text=f"{format_signed(day_diff)} ({day_pct:+.2f}%)",
            bold=True, font_size=FONT_MD,
            color=rgba(sign_color(day_diff)), size_hint_x=COL_PNL, halign="right",
            valign="middle", text_size=(None, sp(24))))
        l2.add_widget(BoxLayout(size_hint_x=COL_PEAK))
        box.add_widget(l2)
        return box
