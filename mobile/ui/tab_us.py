"""탭 1: 미국증시 — 토스 스타일 단순 세로 리스트 (가로 스크롤 제거)

상단: Tier 0 카드 4개 (EWY / USD-KRW / VIX / S&P500) — 다크 블루
하단: 섹터 헤더 + 지표/ETF 한 줄 행 (세로 스크롤만)
"""
import threading

from kivy.clock import mainthread
from kivy.metrics import sp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView

from data_service import (SECTOR_INDICATORS, SECTOR_ETFS,
                           fetch_us_indices, fetch_toss_prices_batch,
                           sign_color)

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


def _bg(widget, color):
    from kivy.graphics import Color, Rectangle
    with widget.canvas.before:
        Color(*rgba(color))
        widget._bg = Rectangle(pos=widget.pos, size=widget.size)
    widget.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                 size=lambda w, v: setattr(w._bg, "size", v))


def _bottom_line(widget, color="#eeeeee"):
    from kivy.graphics import Color, Line
    with widget.canvas.after:
        Color(*rgba(color))
        widget._line = Line(points=[0, 0, 1, 0], width=1)

    def _resize(w, _v):
        x, y = w.pos
        w._line.points = [x, y, x + w.size[0], y]
    widget.bind(pos=_resize, size=_resize)


ETF_NAMES = {
    "091160": "KODEX 반도체", "091230": "TIGER 반도체",
    "449450": "TIGER 방산", "446770": "KODEX 조선해양",
    "329200": "TIGER 리츠부동산인프라", "091180": "KODEX 자동차",
    "117700": "KODEX 건설", "091170": "KODEX 은행",
    "365040": "TIGER AI코리아그로스액티브", "143860": "TIGER 바이오",
    "122630": "KODEX 레버리지", "229200": "KODEX 코스닥150",
}


class TabUS(BoxLayout):
    def __init__(self, **kwargs):
        super().__init__(orientation="vertical", **kwargs)

        # 세로 스크롤만
        self.scroll = ScrollView(do_scroll_x=False, do_scroll_y=True,
                                   bar_width=sp(4))
        self.container = BoxLayout(orientation="vertical",
                                    size_hint_y=None,
                                    spacing=sp(1),
                                    padding=(sp(8), sp(8)))
        self.container.bind(minimum_height=self.container.setter("height"))
        self.scroll.add_widget(self.container)
        self.add_widget(self.scroll)

        self.etf_data = {}
        self.container.add_widget(Label(
            text="로딩 중...", font_size=FONT_MD, color=rgba("#888"),
            size_hint_y=None, height=sp(30)))

    def refresh(self):
        threading.Thread(target=self._fetch_and_render, daemon=True).start()

    def _fetch_and_render(self):
        us = fetch_us_indices()
        etf_tickers = [t for lst in SECTOR_ETFS.values() for t in lst]
        etfs = fetch_toss_prices_batch(etf_tickers) if etf_tickers else {}
        self._render(us, etfs)

    @mainthread
    def _render(self, us_data, etfs):
        self.etf_data = etfs
        self.container.clear_widgets()

        by_sector = {}
        for x in us_data:
            by_sector.setdefault(x["sector"], []).append(x)

        # Tier 0 카드
        self.container.add_widget(self._build_tier0(by_sector.get("dashboard", [])))

        # 컬럼 헤더 (한 번만, 섹터 위)
        self.container.add_widget(self._sector_section_header())

        # 섹터별 (좌측에 섹터명, 우측에 지표/ETF 행)
        for sec_key, sec_label, _ in SECTOR_INDICATORS:
            indicators = by_sector.get(sec_key, [])
            etf_tickers = SECTOR_ETFS.get(sec_key, [])
            visible_etfs = [t for t in etf_tickers if t in etfs]
            if not indicators and not visible_etfs:
                continue
            self.container.add_widget(
                self._build_sector_row(sec_label, indicators, visible_etfs))

    def _section_label(self, text, dark=False):
        """섹션 위 작은 캡션 (예: 대시보드)"""
        wrap = BoxLayout(size_hint_y=None, height=sp(18),
                          padding=(sp(2), sp(2)))
        wrap.add_widget(Label(
            text=text, bold=True, font_size=FONT_XS,
            color=rgba("#888"), halign="left", valign="middle",
            text_size=(None, sp(14))))
        return wrap

    SEC_W = 0.18
    NAME_W = 0.42
    PRICE_W = 0.20
    PCT_W = 0.20

    def _sector_section_header(self):
        """4컬럼 헤더: 섹터 / 종목 / 현재가 / 등락%"""
        row = BoxLayout(orientation="horizontal", size_hint_y=None,
                         height=sp(22), padding=(sp(4), sp(2)),
                         spacing=sp(2))
        from kivy.graphics import Color, Rectangle
        with row.canvas.before:
            Color(*rgba("#eef1f5"))
            row._bg = Rectangle(pos=row.pos, size=row.size)
        row.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                  size=lambda w, v: setattr(w._bg, "size", v))

        def _h(text, sx, halign):
            return Label(
                text=text, bold=True, font_size=FONT_XS,
                color=rgba("#666"), halign=halign, valign="middle",
                size_hint_x=sx, text_size=(None, sp(22)))

        row.add_widget(_h("섹터", self.SEC_W, "left"))
        row.add_widget(_h("종목", self.NAME_W, "left"))
        row.add_widget(_h("현재가", self.PRICE_W, "right"))
        row.add_widget(_h("등락%", self.PCT_W, "right"))
        return row

    # ─── Tier 0 ────────────────────────────────────────────────
    def _build_tier0(self, indices):
        wrap = BoxLayout(orientation="vertical", size_hint_y=None,
                          spacing=0, padding=0)
        wrap.bind(minimum_height=wrap.setter("height"))
        _bg(wrap, "#2c3e50")
        for idx in indices:
            wrap.add_widget(self._tier0_row(idx))
        return wrap

    def _tier0_row(self, idx):
        """1줄/항목: [이름 (설명)] [가격] [등락%]"""
        pct = idx.get("pct", 0)
        pct_color = "#ff6b6b" if pct > 0 else "#5dade2" if pct < 0 else "#bbb"
        note = idx.get("note", "")
        name_text = (f"{idx['name']}  ({note})" if note else idx["name"])
        row = BoxLayout(orientation="horizontal", size_hint_y=None,
                         height=sp(26), padding=(sp(10), sp(2)),
                         spacing=sp(4))
        row.add_widget(Label(
            text=name_text, bold=True, color=rgba("#fff"),
            font_size=FONT_SMALL, halign="left", valign="middle",
            size_hint_x=0.55, text_size=(None, sp(26)),
            shorten=True, shorten_from="right"))
        row.add_widget(Label(
            text=f"{idx['price']:,.2f}", color=rgba("#ecf0f1"),
            font_size=FONT_SMALL, halign="right", valign="middle",
            size_hint_x=0.22, text_size=(None, sp(26))))
        row.add_widget(Label(
            text=f"{pct:+.2f}%", bold=True, color=rgba(pct_color),
            font_size=FONT_SMALL, halign="right", valign="middle",
            size_hint_x=0.23, text_size=(None, sp(26))))
        return row

    # ─── 섹터 행 (좌측 섹터 라벨 + 우측 지표/ETF 스택) ────────
    def _build_sector_row(self, sec_label, indicators, etf_tickers):
        # 우측 데이터 행들 미리 계산해서 높이 결정
        row_h = sp(30)
        n_rows = len(indicators) + sum(
            1 for t in etf_tickers if t in self.etf_data)
        n_rows = max(n_rows, 1)
        total_h = row_h * n_rows + sp(2)

        outer = BoxLayout(orientation="horizontal", size_hint_y=None,
                           height=total_h, spacing=0, padding=0)
        _bg(outer, "#ffffff")
        _bottom_line(outer, color="#dddddd")

        # 좌측 섹터 라벨 (수직 중앙)
        sec_cell = BoxLayout(size_hint_x=self.SEC_W,
                              padding=(sp(6), sp(2)))
        _bg(sec_cell, "#d5dbe0")
        sec_cell.add_widget(Label(
            text=sec_label, bold=True, color=rgba("#2c3e50"),
            font_size=FONT_SMALL, halign="left", valign="middle",
            text_size=(None, total_h),
            shorten=True, shorten_from="right"))
        outer.add_widget(sec_cell)

        # 우측 데이터 스택 (지표 → ETF)
        right = BoxLayout(orientation="vertical",
                           size_hint_x=(self.NAME_W + self.PRICE_W + self.PCT_W),
                           spacing=0, padding=(0, 0))
        for idx in indicators:
            right.add_widget(self._indicator_row(idx, row_h))
        for t in etf_tickers:
            info = self.etf_data.get(t)
            if not info:
                continue
            right.add_widget(self._etf_row(t, info, row_h))
        outer.add_widget(right)
        return outer

    def _indicator_row(self, idx, row_h):
        pct = idx.get("pct", 0)
        # 비율 재정규화 (우측 스택 = NAME+PRICE+PCT)
        total = self.NAME_W + self.PRICE_W + self.PCT_W
        nw, pw, ww = (self.NAME_W / total, self.PRICE_W / total,
                       self.PCT_W / total)
        row = BoxLayout(orientation="horizontal", size_hint_y=None,
                         height=row_h, padding=(sp(8), sp(2)),
                         spacing=sp(2))
        row.add_widget(Label(
            text=idx["name"], bold=True, color=rgba("#222"),
            font_size=FONT_SMALL, halign="left", valign="middle",
            size_hint_x=nw, text_size=(None, row_h),
            shorten=True, shorten_from="right"))
        row.add_widget(Label(
            text=f"{idx['price']:,.2f}", color=rgba("#444"),
            font_size=FONT_SMALL, halign="right", valign="middle",
            size_hint_x=pw, text_size=(None, row_h)))
        row.add_widget(Label(
            text=f"{pct:+.2f}%", bold=True, color=rgba(sign_color(pct)),
            font_size=FONT_SMALL, halign="right", valign="middle",
            size_hint_x=ww, text_size=(None, row_h)))
        return row

    def _etf_row(self, ticker, info, row_h):
        price = info.get("price", 0)
        base = info.get("base", 0)
        diff = price - base if (price and base) else 0
        pct = (diff / base * 100) if base else 0
        total = self.NAME_W + self.PRICE_W + self.PCT_W
        nw, pw, ww = (self.NAME_W / total, self.PRICE_W / total,
                       self.PCT_W / total)
        row = BoxLayout(orientation="horizontal", size_hint_y=None,
                         height=row_h, padding=(sp(8), sp(2)),
                         spacing=sp(2))
        _bg(row, "#fffaf0")
        name = ETF_NAMES.get(ticker, ticker)
        row.add_widget(Label(
            text=f"· {name}", color=rgba("#555"),
            font_size=FONT_SMALL, halign="left", valign="middle",
            size_hint_x=nw, text_size=(None, row_h),
            shorten=True, shorten_from="right"))
        row.add_widget(Label(
            text=f"{int(price):,}" if price else "-",
            color=rgba("#444"), font_size=FONT_SMALL,
            halign="right", valign="middle",
            size_hint_x=pw, text_size=(None, row_h)))
        row.add_widget(Label(
            text=f"{pct:+.2f}%" if diff else "0.00%",
            bold=True, color=rgba(sign_color(diff)),
            font_size=FONT_SMALL, halign="right", valign="middle",
            size_hint_x=ww, text_size=(None, row_h)))
        return row
