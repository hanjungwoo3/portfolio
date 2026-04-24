"""탭 3: 관심종목 — 3행 레이아웃 (보유종목과 동일 스타일)

Row 1: [종목명 (거래량) · 섹터]  [뱃지]         ← 왼쪽 정렬 + 배경색
Row 2: 현재가   |  전일대비(%)   |  외국인(보유%)
Row 3: 목표가(%) | 피크가(%)     |  기관
"""
import threading
from datetime import datetime

from kivy.clock import mainthread
from kivy.metrics import sp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView

from data_service import (fetch_toss_prices_batch, sign_color, format_signed,
                           warning_cache, sector_cache, nxt_support_cache,
                           consensus_cache, investor_cache,
                           refresh_warning_sector_cache, refresh_nxt_cache,
                           refresh_consensus_cache, refresh_investor_cache,
                           load_peaks)
from ui.tab_holdings import format_volume, make_amt_pct_cell

FONT_XS = sp(11)
FONT_SMALL = sp(12)
FONT_MD = sp(16)
FONT_LG = sp(17)
FONT_XL = sp(19)

# 행 2~3 공통 3등분 컬럼
COL_A = 0.34
COL_B = 0.33
COL_C = 0.33


_NAMED = {"white": "#ffffff", "black": "#000000"}

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
    lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
    return lbl


class TabWatch(BoxLayout):
    def __init__(self, holdings_data, **kwargs):
        super().__init__(orientation="vertical", **kwargs)
        self.holdings_data = holdings_data

        # 상단 고정 헤더
        self.header_container = BoxLayout(orientation="vertical",
                                            size_hint_y=None, height=0)
        self.header_container.bind(
            minimum_height=self.header_container.setter("height"))
        self.add_widget(self.header_container)

        self.scroll = ScrollView(do_scroll_x=False, do_scroll_y=True,
                                   bar_width=sp(4))
        self.container = BoxLayout(orientation="vertical",
                                    size_hint_y=None,
                                    spacing=sp(1),
                                    padding=(sp(6), sp(6)))
        self.container.bind(minimum_height=self.container.setter("height"))
        self.scroll.add_widget(self.container)
        self.add_widget(self.scroll)

        # 하단 툴바
        from kivy.uix.button import Button
        from kivy.uix.checkbox import CheckBox
        from ui.dialogs import (show_add_watch, show_delete_watch,
                                   show_json_menu)
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

        toolbar.add_widget(_tbtn("+ 관심 추가", (0.2, 0.5, 0.9, 1),
                                    lambda *_: show_add_watch(
                                        self.holdings_data, self.refresh)))
        toolbar.add_widget(_tbtn("- 관심 삭제", (0.75, 0.35, 0.35, 1),
                                    lambda *_: show_delete_watch(
                                        self.holdings_data, self.refresh)))
        toolbar.add_widget(_tbtn("JSON", (0.45, 0.45, 0.55, 1),
                                    lambda *_: show_json_menu(
                                        self.holdings_data, self.refresh)))

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

        self.container.add_widget(Label(
            text="로딩 중...", font_size=FONT_MD, color=rgba("#888"),
            size_hint_y=None, height=sp(30)))

    def refresh(self):
        threading.Thread(target=self._fetch_and_render, daemon=True).start()

    def _on_fade_toggle(self, _cb, active):
        from ui import app_state
        app_state.set("fade_sleeping", bool(active))
        self.refresh()

    @staticmethod
    def _session_phase() -> str:
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

    def _is_sleeping_stock(self, ticker: str, volume: int) -> bool:
        phase = self._session_phase()
        if phase == "REGULAR":
            return False
        if phase == "EXTENDED":
            return not (nxt_support_cache.get(ticker) and volume > 0)
        return True

    def _fetch_and_render(self):
        watches = [s for s in self.holdings_data.get("holdings", [])
                   if s.get("account") == "관심"]
        tickers = [s["ticker"] for s in watches]

        refresh_warning_sector_cache(tickers)
        refresh_nxt_cache(tickers)
        refresh_consensus_cache(tickers)
        refresh_investor_cache(tickers)
        prices = fetch_toss_prices_batch(tickers)
        peaks = load_peaks()

        self._render(watches, prices, peaks)

    @mainthread
    def _render(self, watches, prices, peaks):
        # 헤더는 고정 컨테이너로
        self.header_container.clear_widgets()
        self.header_container.add_widget(self._build_header())

        self.container.clear_widgets()

        def _pct(stock):
            d = prices.get(stock["ticker"], {})
            p, b = d.get("price", 0), d.get("base", 0)
            return ((p - b) / b * 100) if (p and b) else 0
        watches = sorted(watches, key=_pct, reverse=True)

        for stock in watches:
            self.container.add_widget(
                self._build_row(stock,
                                 prices.get(stock["ticker"], {}),
                                 peaks))

    def _build_header(self):
        """3행 헤더 — 행1은 스킵(종목명 라인 자체가 본문에 있음), 컬럼 헤더만"""
        box = BoxLayout(orientation="vertical", size_hint_y=None,
                         padding=(sp(4), sp(6)), spacing=sp(2))
        box.bind(minimum_height=box.setter("height"))
        from kivy.graphics import Color, Rectangle
        with box.canvas.before:
            Color(*rgba("#eef1f5"))
            box._bg = Rectangle(pos=box.pos, size=box.size)
        box.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                  size=lambda w, v: setattr(w._bg, "size", v))

        def _hdr(text, sx, halign):
            return _bind_align(Label(
                text=text, bold=True, font_size=FONT_XS,
                color=rgba("#666"), halign=halign, valign="middle",
                size_hint_x=sx))

        # Row 1: 현재가 | 전일대비(%) | 외국인(보유%)
        r1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        r1.add_widget(_hdr("현재가", COL_A, "center"))
        r1.add_widget(_hdr("전일대비 (%)", COL_B, "center"))
        r1.add_widget(_hdr("외국인", COL_C, "center"))
        box.add_widget(r1)

        # Row 2: 목표가(%) | 피크가(%) | 기관
        r2 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        r2.add_widget(_hdr("목표가 (%)", COL_A, "center"))
        r2.add_widget(_hdr("피크가 (%)", COL_B, "center"))
        r2.add_widget(_hdr("기관", COL_C, "center"))
        box.add_widget(r2)
        return box

    def _build_row(self, stock, price_info, peaks):
        t = stock["ticker"]
        name = stock.get("name", t)
        price = price_info.get("price", 0)
        base = price_info.get("base", 0)
        volume = price_info.get("volume", 0)
        diff = price - base if (price and base) else 0
        pct = (diff / base * 100) if base else 0
        diff_color = sign_color(diff)
        warn_text = warning_cache.get(t) or ""
        sector = sector_cache.get(t) or ""
        is_sleeping = self._is_sleeping_stock(t, volume)

        from ui import app_state
        fade_on = app_state.get("fade_sleeping")
        apply_fade = is_sleeping and fade_on
        def _c(hex_color):
            return _fade_hex(hex_color, 0.7) if apply_fade else hex_color
        name_bold = not apply_fade

        # 상태 뱃지 (warn_text)
        if warn_text:
            badge_text = warn_text
            badge_bg = {
                "위험": "#c0392b", "관리": "#c0392b",
                "경고": "#e67e22", "과열": "#e67e22",
                "정지": "#e8e8e8", "주의": "#ffd54f",
            }.get(warn_text, "#888")
        else:
            badge_text, badge_bg = "", None

        # 목표가 / 피크가
        target = (consensus_cache.get(t) or {}).get("target")
        target_gap_pct = ((target - price) / price * 100) if (target and price) else 0
        peak = peaks.get(t)
        peak_gap_pct = ((price - peak) / peak * 100) if (peak and price) else 0

        # 수급
        flow = investor_cache.get(t) or {}
        inst = flow.get("기관") if flow else None
        foreign = flow.get("외국인") if flow else None
        foreign_ratio = flow.get("외국인비율", 0) if flow else 0

        # 컨테이너
        box = BoxLayout(orientation="vertical", size_hint_y=None,
                         padding=(sp(4), sp(4)), spacing=sp(2))
        box.bind(minimum_height=box.setter("height"))

        from kivy.graphics import Color, Rectangle, Line
        with box.canvas.before:
            Color(*rgba("#ffffff"))
            box._bg = Rectangle(pos=box.pos, size=box.size)
            Color(*rgba("#eeeeee"))
            box._line = Line(points=[0, 0, 1, 0], width=1)
        def _on_box_resize(w, _v):
            w._bg.pos = w.pos; w._bg.size = w.size
            x, y = w.pos
            w._line.points = [x, y, x + w.size[0], y]
        box.bind(pos=_on_box_resize, size=_on_box_resize)

        # ─── 행 1: 2줄 카드 — (이름 + 거래량 + 뱃지) + (섹터 + 외국인 보유%)
        l1_wrap = BoxLayout(orientation="vertical", size_hint_y=None,
                              height=sp(52), padding=(sp(4), sp(2)))
        l1 = BoxLayout(orientation="vertical", size_hint_y=None,
                        height=sp(48), padding=(sp(10), sp(4)), spacing=sp(1))
        l1_wrap.add_widget(l1)
        name_bg = _c("#dce6f2") if not is_sleeping else _c("#ececec")
        from kivy.graphics import RoundedRectangle as _RR
        with l1.canvas.before:
            Color(*rgba(name_bg))
            l1._bg = _RR(pos=l1.pos, size=l1.size, radius=[sp(8)])
        l1.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                 size=lambda w, v: setattr(w._bg, "size", v))

        prefix = "zZ " if is_sleeping else ""

        # Line 1: 종목명 (거래량:xx) [뱃지]
        line_top = BoxLayout(orientation="horizontal", size_hint_y=None,
                              height=sp(26), spacing=sp(4))
        name_text = f"{prefix}{name} (거래량:{format_volume(volume)})"
        name_lbl = Label(
            text=name_text, bold=name_bold, font_size=FONT_XL,
            color=rgba(_c(diff_color)),
            size_hint=(None, 1),
            halign="left", valign="middle")
        name_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
        line_top.add_widget(name_lbl)

        if badge_text:
            from kivy.graphics import Color as _Col, RoundedRectangle
            badge_w = sp(30)
            badge = Label(
                text=badge_text, bold=True, font_size=sp(10),
                color=rgba(_c("#ffffff")),
                size_hint=(None, None), width=badge_w, height=sp(18),
                halign="center", valign="middle",
                text_size=(badge_w, sp(18)))
            with badge.canvas.before:
                _Col(*rgba(_c(badge_bg)))
                badge._bg = RoundedRectangle(pos=badge.pos, size=badge.size,
                                              radius=[sp(4)])
            badge.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                        size=lambda w, v: setattr(w._bg, "size", v))
            line_top.add_widget(badge)
        line_top.add_widget(BoxLayout())  # 우측 스페이서
        l1.add_widget(line_top)

        # Line 2: 섹터:xxx  외국인:xx.xx% 보유
        sub_parts = []
        if sector:
            sub_parts.append(f"섹터:{sector}")
        if foreign_ratio and foreign_ratio > 0:
            sub_parts.append(f"외국인:{foreign_ratio:.2f}% 보유")
        line_bot = Label(
            text="   ".join(sub_parts),
            font_size=FONT_SMALL, color=rgba(_c("#666")),
            size_hint_y=None, height=sp(18),
            halign="left", valign="middle",
            max_lines=1, shorten=True, shorten_from="right")
        line_bot.bind(size=lambda w, v: setattr(w, "text_size", v))
        l1.add_widget(line_bot)

        box.add_widget(l1_wrap)

        # 모든 데이터 셀 동일 구조 — 금액 위 / (%) 아래 스택
        def _cell(amt, pct, amt_color, pct_color=None, col=COL_A, bold=False):
            return make_amt_pct_cell(
                amt, pct, amt_color, pct_color or amt_color,
                size_hint_x=col, bold=bold,
                font_size=FONT_MD, height=sp(40))

        # ─── 행 2: 현재가 | 전일대비(%) | 외국인
        l2 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(40), spacing=sp(4))
        l2.add_widget(_cell(f"{price:,}" if price else "", "",
                               _c("#333"), col=COL_A))
        if diff:
            l2.add_widget(_cell(format_signed(diff), f"({pct:+.2f}%)",
                                   _c(diff_color), col=COL_B, bold=name_bold))
        else:
            l2.add_widget(_cell("0", "", _c(diff_color), col=COL_B))
        l2.add_widget(_cell(format_signed(foreign) if foreign else "", "",
                               _c(sign_color(foreign)) if foreign else _c("#aaa"),
                               col=COL_C))
        box.add_widget(l2)

        # ─── 행 3: 목표가(%) | 피크가(%) | 기관
        l3 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(40), spacing=sp(4))
        if target:
            l3.add_widget(_cell(f"{target:,}", f"({target_gap_pct:+.2f}%)",
                                  _c(sign_color(target_gap_pct)), col=COL_A))
        else:
            l3.add_widget(_cell("", "", _c("#aaa"), col=COL_A))
        if peak:
            peak_c = _c(sign_color(peak_gap_pct) if peak_gap_pct < 0 else "#888")
            l3.add_widget(_cell(f"{int(peak):,}", f"({peak_gap_pct:+.2f}%)",
                                  peak_c, col=COL_B))
        else:
            l3.add_widget(_cell("", "", _c("#aaa"), col=COL_B))
        l3.add_widget(_cell(format_signed(inst) if inst else "", "",
                               _c(sign_color(inst)) if inst else _c("#aaa"),
                               col=COL_C))
        box.add_widget(l3)
        return box
