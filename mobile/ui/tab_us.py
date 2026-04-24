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

FONT_XS = sp(12)
FONT_SMALL = sp(13)
FONT_MD = sp(16)
FONT_LG = sp(17)


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

    # ─── Tier 0 (2 x 2 그리드 카드) ────────────────────────────
    def _build_tier0(self, indices):
        """데스크톱 스타일 — 2개씩 한 줄, 2줄 카드 (이름/가격/등락% + 설명).
        각 카드는 둥근 배경으로 시각 구분."""
        wrap = BoxLayout(orientation="vertical", size_hint_y=None,
                          spacing=sp(6), padding=(sp(6), sp(6)))
        wrap.bind(minimum_height=wrap.setter("height"))
        _bg(wrap, "#2c3e50")  # 바깥 어두운 배경

        # 2개씩 묶어서 행 구성
        for i in range(0, len(indices), 2):
            pair = indices[i:i + 2]
            row = BoxLayout(orientation="horizontal", size_hint_y=None,
                             height=sp(64), spacing=sp(6))
            for idx in pair:
                row.add_widget(self._tier0_card(idx))
            if len(pair) == 1:
                row.add_widget(BoxLayout())  # 빈 자리
            wrap.add_widget(row)
        return wrap

    @staticmethod
    def _us_market_closed() -> bool:
        """미국 정규장(NYSE) 한국시간 기준 대략 23:30 ~ 06:00 외에는 휴장.
        Android tzdata 미설치 대응: UTC+9 고정 오프셋 사용."""
        try:
            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone(timedelta(hours=9)))
            if now.weekday() >= 5:
                return True
            hhmm = now.hour * 60 + now.minute
            return not (hhmm >= (23 * 60 + 30) or hhmm < 6 * 60)
        except Exception as e:
            print(f"[us-session] {e}")
            return True

    @staticmethod
    def _kr_market_closed() -> bool:
        """KOSPI/KOSDAQ 정규장: 한국시간 09:00 ~ 15:20 이외 휴장."""
        try:
            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone(timedelta(hours=9)))
            if now.weekday() >= 5:
                return True
            hhmm = now.hour * 60 + now.minute
            return not (9 * 60 <= hhmm < 15 * 60 + 20)
        except Exception as e:
            print(f"[kr-session] {e}")
            return True

    # 한국 심볼 (zZ는 한국장 휴장시)
    _KR_SYMBOLS = {"^KS200", "^KQ11"}

    def _is_symbol_sleeping(self, symbol: str) -> bool:
        """심볼별 휴장 판정 — 한국 지수는 KR 장, 나머지는 US 장 기준."""
        if symbol in self._KR_SYMBOLS:
            return self._kr_market_closed()
        return self._us_market_closed()

    def _tier0_card(self, idx):
        """2줄 카드: Line 1 [zZ 이름 가격 등락%] + Line 2 [설명] — 둥근 배경."""
        pct = idx.get("pct", 0)
        pct_color = "#ff6b6b" if pct > 0 else "#5dade2" if pct < 0 else "#bbb"
        note = idx.get("note", "")

        # zZ 마커 — 심볼별 시장 상태 (USD/KRW 는 24h FX 라 제외)
        symbol = idx.get("symbol", "")
        show_zzz = (symbol != "KRW=X"
                     and self._is_symbol_sleeping(symbol))

        card = BoxLayout(orientation="vertical", size_hint_y=None,
                          height=sp(64), padding=(sp(10), sp(6)),
                          spacing=sp(1))

        # 카드 배경 (약간 밝은 블루그레이 + 둥근 모서리)
        from kivy.graphics import Color, RoundedRectangle
        with card.canvas.before:
            Color(*rgba("#3c5470"))
            card._bg = RoundedRectangle(pos=card.pos, size=card.size,
                                          radius=[sp(8)])
        card.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                   size=lambda w, v: setattr(w._bg, "size", v))

        # Line 1: [zZ] 이름 / 가격 / 등락%
        line1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                           height=sp(28), spacing=sp(4))
        if show_zzz:
            zzz_lbl = Label(
                text="zZ", bold=True, color=rgba("#7aa3d4"),
                font_size=FONT_XS, halign="left", valign="middle",
                size_hint_x=None)
            zzz_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
            line1.add_widget(zzz_lbl)
        name_lbl = Label(
            text=idx['name'], bold=True, color=rgba("#fff"),
            font_size=FONT_MD, halign="left", valign="middle",
            size_hint_x=None)
        name_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
        line1.add_widget(name_lbl)
        price_lbl = Label(
            text=f"{idx['price']:,.2f}", color=rgba("#ecf0f1"),
            font_size=FONT_SMALL, halign="left", valign="middle",
            max_lines=1, shorten=True, shorten_from="left")
        price_lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
        line1.add_widget(price_lbl)
        pct_lbl = Label(
            text=f"{pct:+.2f}%", bold=True, color=rgba(pct_color),
            font_size=FONT_MD, halign="right", valign="middle",
            size_hint_x=None)
        pct_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
        line1.add_widget(pct_lbl)
        card.add_widget(line1)

        # Line 2: 설명 (note) — 데스크톱과 동일 문구
        note_lbl = Label(
            text=note, color=rgba("#b0bec5"),
            font_size=FONT_XS, halign="left", valign="middle",
            size_hint_y=None, height=sp(20),
            max_lines=1, shorten=True, shorten_from="right")
        note_lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
        card.add_widget(note_lbl)

        return card

    # ─── 섹터 행 (좌측 섹터 라벨 + 우측 지표/ETF 스택) ────────
    def _build_sector_row(self, sec_label, indicators, etf_tickers):
        """각 지표가 동적 높이(설명 유무에 따라 다름)라 컨테이너도 동적."""
        row_h = sp(34)  # ETF 고정 높이

        outer = BoxLayout(orientation="horizontal", size_hint_y=None,
                           spacing=0, padding=0)
        outer.bind(minimum_height=outer.setter("height"))
        _bg(outer, "#ffffff")
        _bottom_line(outer, color="#dddddd")

        # 우측 데이터 스택 (먼저 구성하여 높이 결정)
        right = BoxLayout(orientation="vertical",
                           size_hint=(self.NAME_W + self.PRICE_W + self.PCT_W, None),
                           spacing=0, padding=(0, 0))
        right.bind(minimum_height=right.setter("height"))
        for idx in indicators:
            right.add_widget(self._indicator_row(idx, row_h))
        for t in etf_tickers:
            info = self.etf_data.get(t)
            if not info:
                continue
            right.add_widget(self._etf_row(t, info, row_h))

        # 좌측 섹터 라벨 — 우측 높이를 따라가도록 size_hint_y=1 로 stretch
        sec_cell = BoxLayout(size_hint_x=self.SEC_W,
                              padding=(sp(6), sp(2)))
        _bg(sec_cell, "#d5dbe0")
        sec_lbl = Label(
            text=sec_label, bold=True, color=rgba("#2c3e50"),
            font_size=FONT_SMALL, halign="left", valign="middle",
            max_lines=1, shorten=True, shorten_from="right")
        sec_lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
        sec_cell.add_widget(sec_lbl)

        outer.add_widget(sec_cell)
        outer.add_widget(right)
        return outer

    def _indicator_row(self, idx, row_h):
        """지표 한 줄 — 이름+zZ(좌) 가격/%(우), 아래에 설명(note) 작게."""
        pct = idx.get("pct", 0)
        note = idx.get("note", "")
        symbol = idx.get("symbol", "")
        sleeping = self._is_symbol_sleeping(symbol)
        is_futures = symbol.endswith("=F")

        # 비율 재정규화
        total = self.NAME_W + self.PRICE_W + self.PCT_W
        nw, pw, ww = (self.NAME_W / total, self.PRICE_W / total,
                       self.PCT_W / total)

        # note 있으면 2줄 구성
        card = BoxLayout(orientation="vertical", size_hint_y=None,
                          padding=(sp(8), sp(3)), spacing=0)
        card.bind(minimum_height=card.setter("height"))
        # 선물은 옅은 노란 배경으로 구분 (ETF 와 비슷한 강조)
        if is_futures:
            _bg(card, "#fff4d6")

        # Line 1: [zZ] name  |  price  |  pct
        line1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                           height=sp(26), spacing=sp(4))
        name_box = BoxLayout(orientation="horizontal", size_hint_x=nw,
                              spacing=sp(3))
        if sleeping:
            zzz = Label(text="zZ", bold=True, color=rgba("#7aa3d4"),
                         font_size=FONT_XS,
                         size_hint_x=None, halign="left", valign="middle")
            zzz.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
            name_box.add_widget(zzz)
        name_lbl = Label(
            text=idx["name"], bold=True, color=rgba("#222"),
            font_size=FONT_SMALL, halign="left", valign="middle",
            size_hint_x=None,
            max_lines=1, shorten=True, shorten_from="right")
        name_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
        name_box.add_widget(name_lbl)
        name_box.add_widget(BoxLayout())  # 좌측 정렬용 우측 스페이서
        line1.add_widget(name_box)
        line1.add_widget(Label(
            text=f"{idx['price']:,.2f}", color=rgba("#444"),
            font_size=FONT_SMALL, halign="right", valign="middle",
            size_hint_x=pw, text_size=(None, sp(26))))
        line1.add_widget(Label(
            text=f"{pct:+.2f}%", bold=True, color=rgba(sign_color(pct)),
            font_size=FONT_SMALL, halign="right", valign="middle",
            size_hint_x=ww, text_size=(None, sp(26))))
        card.add_widget(line1)

        # Line 2: note 작게 회색
        if note:
            note_lbl = Label(
                text=note, color=rgba("#999"),
                font_size=sp(12), halign="left", valign="middle",
                size_hint_y=None, height=sp(16),
                max_lines=1, shorten=True, shorten_from="right")
            note_lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            card.add_widget(note_lbl)
        return card

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
