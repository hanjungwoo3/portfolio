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
from ui.tab_holdings import (format_volume, make_amt_pct_cell,
                                TappableBox, open_toss_stock)

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

        from kivy.uix.togglebutton import ToggleButton
        initial_on = app_state.get("fade_sleeping")
        self.fade_btn = ToggleButton(
            text="장마감 투명", font_size=sp(12), bold=True,
            state="down" if initial_on else "normal",
            size_hint_x=None, width=sp(100))
        self.fade_btn.bind(state=self._on_fade_toggle)
        toolbar.add_widget(self.fade_btn)
        self.add_widget(toolbar)

        self.container.add_widget(Label(
            text="로딩 중...", font_size=FONT_MD, color=rgba("#888"),
            size_hint_y=None, height=sp(30)))

    def refresh(self):
        threading.Thread(target=self._fetch_and_render, daemon=True).start()

    def _on_fade_toggle(self, _btn, state):
        from ui import app_state
        active = (state == "down") if isinstance(state, str) else bool(state)
        app_state.set("fade_sleeping", active)
        self.refresh()

    @staticmethod
    def _session_phase() -> str:
        try:
            from datetime import timezone, timedelta
            now = datetime.now(timezone(timedelta(hours=9)))
            if now.weekday() >= 5:
                return "CLOSED"
            hhmm = now.hour * 60 + now.minute
            if 9 * 60 <= hhmm < 15 * 60 + 20:
                return "REGULAR"
            if (8 * 60 <= hhmm < 8 * 60 + 50) or (15 * 60 + 30 <= hhmm < 20 * 60):
                return "EXTENDED"
            return "CLOSED"
        except Exception as e:
            print(f"[watch-session] {e}")
            return "CLOSED"

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
        """헤더 제거 — 카드 자체에 모든 정보 inline 표시"""
        return BoxLayout(orientation="vertical", size_hint_y=None, height=0)

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

        # 목표가 / 피크가 / 컨센서스 score
        consensus = consensus_cache.get(t) or {}
        target = consensus.get("target")
        score = consensus.get("score")
        target_gap_pct = ((target - price) / price * 100) if (target and price) else 0
        peak = peaks.get(t)
        peak_gap_pct = ((price - peak) / peak * 100) if (peak and price) else 0

        # 수급
        flow = investor_cache.get(t) or {}
        inst = flow.get("기관") if flow else None
        foreign = flow.get("외국인") if flow else None
        foreign_ratio = flow.get("외국인비율", 0) if flow else 0
        pension = flow.get("연기금") if flow else None

        # 컨테이너
        box = BoxLayout(orientation="vertical", size_hint_y=None,
                         padding=(sp(4), sp(2)), spacing=sp(0))
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

        # ─── 카드: 좌(종목 정보 4줄) / 우(수급 4줄)
        l1_wrap = TappableBox(orientation="vertical", size_hint_y=None,
                                size_hint_x=1, padding=(sp(4), sp(4)))
        l1_wrap.bind(minimum_height=l1_wrap.setter("height"))
        l1_wrap.bind(on_release=lambda *_: open_toss_stock(t))

        l1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        padding=(sp(10), sp(8)), spacing=sp(8))
        l1.bind(minimum_height=l1.setter("height"))
        l1_wrap.add_widget(l1)
        name_bg = _c("#dce6f2") if not is_sleeping else _c("#ececec")
        from kivy.graphics import RoundedRectangle as _RR
        with l1.canvas.before:
            Color(*rgba(name_bg))
            l1._bg = _RR(pos=l1.pos, size=l1.size, radius=[sp(8)])
        l1.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                 size=lambda w, v: setattr(w._bg, "size", v))

        prefix = "zZ " if is_sleeping else ""

        big_px = int(sp(17))
        small_px = int(sp(11))
        gray_hex = "999999"
        body_hex = _c("#222").lstrip("#")

        def _line(markup_text, h=sp(22)):
            lbl = Label(
                text=markup_text, markup=True, font_size=FONT_SMALL,
                color=rgba(_c("#222")),
                size_hint_y=None, height=h,
                halign="left", valign="middle",
                max_lines=1, shorten=True, shorten_from="right")
            lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            return lbl

        # ─── 좌측 4줄
        left_col = BoxLayout(orientation="vertical", size_hint_x=0.62,
                              spacing=sp(2))

        # Line 1: [zZ] 종목명  [뱃지]  섹터
        name_line = BoxLayout(orientation="horizontal", size_hint_y=None,
                               height=sp(22), spacing=sp(4))
        name_color_hex = _c(diff_color).lstrip("#")
        name_only_markup = f"[color={name_color_hex}][b]{prefix}{name}[/b][/color]"
        name_lbl = Label(
            text=name_only_markup, markup=True, font_size=FONT_MD,
            color=rgba(_c("#222")),
            size_hint=(None, 1),
            halign="left", valign="middle")
        name_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
        name_line.add_widget(name_lbl)
        if badge_text:
            from kivy.graphics import Color as _Col, RoundedRectangle
            badge_w = sp(28)
            badge = Label(
                text=badge_text, bold=True, font_size=sp(10),
                color=rgba(_c("#ffffff")),
                size_hint=(None, None), width=badge_w, height=sp(16),
                halign="center", valign="middle",
                text_size=(badge_w, sp(16)))
            with badge.canvas.before:
                _Col(*rgba(_c(badge_bg)))
                badge._bg = RoundedRectangle(pos=badge.pos, size=badge.size,
                                              radius=[sp(4)])
            badge.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                        size=lambda w, v: setattr(w._bg, "size", v))
            name_line.add_widget(badge)
        if sector:
            sector_lbl = Label(
                text=sector, font_size=sp(11),
                color=rgba(_c("#999")),
                halign="left", valign="middle",
                max_lines=1, shorten=True, shorten_from="right")
            sector_lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            name_line.add_widget(sector_lbl)
        else:
            name_line.add_widget(BoxLayout())
        left_col.add_widget(name_line)

        # Line 2: 현재가원 (거래량)
        vol_str = f"({format_volume(volume)})" if volume else ""
        price_color_hex = _c(diff_color if diff else "#222").lstrip("#")
        if price:
            price_markup = (f"[color={price_color_hex}]"
                             f"[size={big_px}][b]{int(price):,}원[/b][/size]"
                             f"[/color]"
                             + (f"  [color={gray_hex}][size={small_px}]{vol_str}[/size][/color]"
                                if vol_str else ""))
        else:
            price_markup = f"[color={gray_hex}]가격 정보 없음[/color]"
        left_col.add_widget(_line(price_markup, h=sp(26)))

        # Line 3: 어제보다 +diff (pct%)
        if diff:
            day_color_hex = _c(diff_color).lstrip("#")
            day_markup = (f"[color={body_hex}]어제보다[/color] "
                          f"[color={day_color_hex}][b]{format_signed(diff)}[/b] "
                          f"({pct:+.2f}%)[/color]")
        else:
            day_markup = f"[color={gray_hex}]어제보다[/color]"
        left_col.add_widget(_line(day_markup))

        # Line 4: 목표 (score) target (+gap%)
        if target:
            gap_color_hex = _c(sign_color(target_gap_pct)).lstrip("#")
            score_str = f"({score:.2f}) " if score else ""
            target_markup = (f"[color={body_hex}]목표[/color] "
                             f"[color={body_hex}]{score_str}{int(target):,}[/color] "
                             f"[color={gap_color_hex}]({target_gap_pct:+.2f}%)[/color]")
        else:
            target_markup = f"[color={gray_hex}]목표[/color]"
        left_col.add_widget(_line(target_markup))

        l1.add_widget(left_col)

        # ─── 우측 4줄 (수급)
        right_col = BoxLayout(orientation="vertical", size_hint_x=0.38,
                               spacing=sp(2))

        if foreign_ratio and foreign_ratio > 0:
            ratio_hex = _c(sign_color(1)).lstrip("#")
            ratio_markup = (f"[color={body_hex}]외국인 보유[/color] "
                             f"[color={ratio_hex}][b]{foreign_ratio:.2f}%[/b][/color]")
        else:
            ratio_markup = f"[color={gray_hex}]외국인 보유[/color]"
        right_col.add_widget(_line(ratio_markup))

        def _flow_line(label, amount):
            if amount:
                col_hex = _c(sign_color(amount)).lstrip("#")
                return _line(f"[color={body_hex}]{label}[/color] "
                              f"[color={col_hex}][b]{format_signed(amount)}[/b][/color]")
            return _line(f"[color={gray_hex}]{label}[/color]")

        right_col.add_widget(_flow_line("외국인", foreign))
        right_col.add_widget(_flow_line("기관", inst))
        right_col.add_widget(_flow_line("연기금", pension))

        l1.add_widget(right_col)
        box.add_widget(l1_wrap)
        return box
