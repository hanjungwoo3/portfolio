"""탭 3: 관심종목 — 현재가 + 전일대비"""
import threading
from datetime import datetime

from kivy.clock import mainthread
from kivy.metrics import sp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView

from data_service import (fetch_toss_prices_batch, sign_color,
                           warning_cache, sector_cache, nxt_support_cache,
                           refresh_warning_sector_cache, refresh_nxt_cache)
from ui.tab_holdings import format_volume

FONT_XS = sp(11)
FONT_SMALL = sp(12)
FONT_MD = sp(14)
FONT_LG = sp(15)


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
    """halign/valign 이 실제로 적용되도록 text_size 를 위젯 크기에 바인딩."""
    lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
    return lbl


class TabWatch(BoxLayout):
    def __init__(self, holdings_data, **kwargs):
        super().__init__(orientation="vertical", **kwargs)
        self.holdings_data = holdings_data

        self.scroll = ScrollView(do_scroll_x=False, do_scroll_y=True,
                                   bar_width=sp(4))
        self.container = BoxLayout(orientation="vertical",
                                    size_hint_y=None,
                                    spacing=sp(1),
                                    padding=(sp(6), sp(6)))
        self.container.bind(minimum_height=self.container.setter("height"))
        self.scroll.add_widget(self.container)
        self.add_widget(self.scroll)

        # 하단 툴바: [+ 관심추가] [- 관심삭제]  [✓ 장마감투명]
        from kivy.uix.button import Button
        from kivy.uix.checkbox import CheckBox
        from ui.dialogs import show_add_watch, show_delete_watch
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

        # 장마감 투명도 체크박스
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
        """REGULAR | EXTENDED | CLOSED"""
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
        """per-stock sleeping — 데스크탑과 동일 룰"""
        phase = self._session_phase()
        if phase == "REGULAR":
            return False
        if phase == "EXTENDED":
            return not (nxt_support_cache.get(ticker) and volume > 0)
        return True

    def _fetch_and_render(self):
        # 관심 주식만 (ETF 제외 — Tab1 의 섹터블록에서 표시)
        watches = [s for s in self.holdings_data.get("holdings", [])
                   if s.get("account") == "관심"]
        tickers = [s["ticker"] for s in watches]

        refresh_warning_sector_cache(tickers)
        refresh_nxt_cache(tickers)
        prices = fetch_toss_prices_batch(tickers)

        self._render(watches, prices)

    @mainthread
    def _render(self, watches, prices):
        self.container.clear_widgets()
        self.container.add_widget(self._build_header())

        # 전일대비 등락률 내림차순
        def _pct(stock):
            d = prices.get(stock["ticker"], {})
            p, b = d.get("price", 0), d.get("base", 0)
            return ((p - b) / b * 100) if (p and b) else 0
        watches = sorted(watches, key=_pct, reverse=True)

        for i, stock in enumerate(watches):
            self.container.add_widget(self._build_row(stock,
                                                        prices.get(stock["ticker"], {}),
                                                        stripe=i % 2 == 1))

    def _build_header(self):
        row = BoxLayout(orientation="horizontal", size_hint_y=None,
                         height=sp(22), padding=(sp(6), sp(2)),
                         spacing=sp(4))
        from kivy.graphics import Color, Rectangle
        with row.canvas.before:
            Color(*rgba("#eef1f5"))
            row._bg = Rectangle(pos=row.pos, size=row.size)
        row.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                  size=lambda w, v: setattr(w._bg, "size", v))

        def _h(text, sx, halign):
            lbl = Label(
                text=text, bold=True, font_size=FONT_XS,
                color=rgba("#666"), halign=halign, valign="middle",
                size_hint_x=sx)
            return _bind_align(lbl)

        row.add_widget(_h("섹터", 0.15, "left"))
        row.add_widget(_h("종목", 0.24, "left"))
        row.add_widget(_h("거래량", 0.12, "right"))
        row.add_widget(_h("현재가", 0.15, "right"))
        row.add_widget(_h("전일대비 금액(%)", 0.34, "right"))
        return row

    def _build_row(self, stock, price_info, stripe=False):
        t = stock["ticker"]
        name = stock.get("name", t)
        price = price_info.get("price", 0)
        base = price_info.get("base", 0)
        diff = price - base if (price and base) else 0
        pct = (diff / base * 100) if base else 0
        diff_color = sign_color(diff)
        warn_text = warning_cache.get(t) or ""
        sector = sector_cache.get(t) or ""
        is_sleeping = self._is_sleeping_stock(t, price_info.get("volume", 0))

        # 데스크톱과 동일: 휴면 시 색상을 흰색 쪽으로 70% 페이드 + bold 해제
        # 단, 장마감 투명도 체크박스 OFF 면 페이드/bold 해제 생략
        from ui import app_state
        fade_on = app_state.get("fade_sleeping")
        apply_fade = is_sleeping and fade_on
        def _c(hex_color):
            return _fade_hex(hex_color, 0.7) if apply_fade else hex_color
        name_bold = not apply_fade
        pct_bold = not apply_fade

        bg_color = "#f5f5f5" if stripe else "white"
        box = BoxLayout(orientation="horizontal", size_hint_y=None,
                         height=sp(28), padding=(sp(6), sp(2)), spacing=sp(4))

        from kivy.graphics import Color, Rectangle
        with box.canvas.before:
            Color(*rgba(bg_color))
            box._bg = Rectangle(pos=box.pos, size=box.size)
        box.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                  size=lambda w, v: setattr(w._bg, "size", v))

        # 섹터 — 왼쪽 정렬
        box.add_widget(_bind_align(Label(
            text=f"({sector})" if sector else "", font_size=sp(9),
            color=rgba(_c("#aaa")), size_hint_x=0.15,
            halign="left", valign="middle")))

        # 종목명 + 경고 — 왼쪽 정렬
        name_col = BoxLayout(orientation="horizontal", size_hint_x=0.24,
                              spacing=sp(3))
        prefix = "zZ " if is_sleeping else ""
        name_col.add_widget(_bind_align(Label(
            text=f"{prefix}{name}", bold=name_bold, font_size=FONT_LG,
            color=rgba(_c(diff_color)),
            halign="left", valign="middle")))
        if warn_text:
            name_col.add_widget(Label(
                text=warn_text, bold=True, font_size=sp(9),
                color=rgba("#fff"), size_hint_x=None, width=sp(28)))
        box.add_widget(name_col)

        # 거래량 — 오른쪽 정렬
        volume = price_info.get("volume", 0)
        box.add_widget(_bind_align(Label(
            text=format_volume(volume) if volume else "-",
            font_size=FONT_SMALL, color=rgba(_c("#888")),
            size_hint_x=0.12, halign="right", valign="middle")))

        # 현재가 — 오른쪽 정렬
        box.add_widget(_bind_align(Label(
            text=f"{price:,}" if price else "-", font_size=FONT_MD,
            color=rgba(_c("#555")), size_hint_x=0.15,
            halign="right", valign="middle")))

        # 전일대비 금액(%) — 오른쪽 정렬 (데스크톱과 동일 포맷)
        if diff:
            diff_text = f"{diff:+,} ({pct:+.2f}%)"
        else:
            diff_text = "0"
        box.add_widget(_bind_align(Label(
            text=diff_text, bold=pct_bold,
            font_size=FONT_MD, color=rgba(_c(diff_color)),
            size_hint_x=0.34, halign="right", valign="middle")))
        return box
