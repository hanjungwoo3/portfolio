"""탭 2: 보유종목 — 토스 스타일 단순 2행 레이아웃 + 데스크탑 뱃지

행 1: [💤? 종목명 pill] (보유량)    매수가     손익금액 (손익%)
행 2: (섹터)                          (옅은 회색 작게)
행 3: 거래량                          현재가     전일대비%
"""
import threading
from datetime import datetime, timezone, timedelta

# Android 에 tzdata 없어도 동작하도록 UTC+9 오프셋 직접 사용 (zoneinfo 미사용)
KST = timezone(timedelta(hours=9))

from kivy.clock import mainthread
from kivy.metrics import sp
from kivy.uix.behaviors import ButtonBehavior
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView


class TappableBox(ButtonBehavior, BoxLayout):
    """BoxLayout 에 탭 이벤트 지원을 더한 공용 위젯 (종목명 → Toss 이동)."""
    pass


def open_toss_stock(ticker: str, is_us: bool = False):
    """Toss Invest 종목 상세 페이지 열기.
    AppsFlyer 마케팅 링크에서 확인된 실제 포맷:
      supertoss://securities?url=https://service.tossinvest.com?nextLandingUrl=/stocks/A005930
    Android 에선 이 딥링크로 Toss 앱 내 Securities 핸들러 경유 → 종목 화면 진입.
    PC 실행 시엔 https 브라우저로 폴백.
    """
    from urllib.parse import quote
    code = ticker if is_us else f"A{ticker}"
    inner = f"https://service.tossinvest.com?nextLandingUrl=/stocks/{code}"
    deep = f"supertoss://securities?url={quote(inner, safe='')}"
    https = f"https://tossinvest.com/stocks/{code}"

    try:
        from jnius import autoclass
        Intent = autoclass("android.content.Intent")
        Uri = autoclass("android.net.Uri")
        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        intent = Intent(Intent.ACTION_VIEW, Uri.parse(deep))
        intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        PythonActivity.mActivity.startActivity(intent)
        return
    except Exception as e:
        print(f"[toss-deep] {e}")

    try:
        import webbrowser
        webbrowser.open(https)
    except Exception as e:
        print(f"[toss-web] {e}")

from data_service import (fetch_toss_prices_batch, sign_color, format_signed,
                           refresh_warning_sector_cache,
                           refresh_nxt_cache, refresh_consensus_cache,
                           refresh_investor_cache,
                           warning_cache, sector_cache, nxt_support_cache,
                           consensus_cache, investor_cache,
                           load_peaks, load_thresholds)


def kr_session_phase() -> str:
    """REGULAR(09:00-15:20 평일) | EXTENDED | CLOSED — KST 고정 오프셋"""
    try:
        now = datetime.now(KST)
        if now.weekday() >= 5:
            return "CLOSED"
        hhmm = now.hour * 60 + now.minute
        if 9 * 60 <= hhmm < 15 * 60 + 20:
            return "REGULAR"
        if (8 * 60 <= hhmm < 8 * 60 + 50) or (15 * 60 + 30 <= hhmm < 20 * 60):
            return "EXTENDED"
        return "CLOSED"
    except Exception as e:
        print(f"[kr-session] {e}")
        return "CLOSED"

FONT_XS = sp(11)
FONT_SMALL = sp(12)
FONT_MD = sp(16)
FONT_LG = sp(17)
FONT_XL = sp(19)

# 매도 수수료 0.2% (토스 기준) — 데스크탑과 동일
SELL_FEE_PCT = 0.2
FEE_MULTIPLIER = 1 - (SELL_FEE_PCT / 100)

# 행 1 (이름/거래량) 컬럼
COL_NAME_W = 0.72   # 종목명 (보유수) — 넓게
COL_VOL_W = 0.28    # 거래량 — 우측

# 행 2~4 공통 3등분 (매수가/손익/피크 · 현재가/전일/목표 · 기관/외국인/연기금)
COL_A = 0.34   # 매수가 / 현재가 / 기관
COL_B = 0.33   # 손익금액(%) / 전일대비(%) / 외국인(보유%)
COL_C = 0.33   # 피크가(%) / 목표가(%) / 연기금


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


def make_amt_pct_cell(amt_text: str, pct_text: str,
                        amt_color: str, pct_color: str,
                        size_hint_x: float, bold: bool = False,
                        font_size=None, height=None):
    """금액 + (%) 를 세로 2단 스택으로 렌더 — 금액 위 / pct 아래 작은 폰트.
    모든 셀이 항상 58/42 분할이라 단일값 셀과 스택 셀의 amt 상단선이 일치.
    둘 다 오른쪽 정렬 + top 정렬."""
    fs = font_size or sp(16)
    pct_fs = sp(12)
    h = height or sp(40)
    wrap = BoxLayout(orientation="vertical", size_hint_x=size_hint_x,
                      size_hint_y=None, height=h, spacing=0)
    amt = Label(text=amt_text, font_size=fs, bold=bold,
                 color=rgba(amt_color),
                 halign="right", valign="top",
                 size_hint_y=0.58)
    amt.bind(size=lambda w, v: setattr(w, "text_size", v))
    wrap.add_widget(amt)
    # pct 없어도 빈 Label 로 하단 공간 유지 → amt 위치 일관성 확보
    pct = Label(text=pct_text, font_size=pct_fs,
                 color=rgba(pct_color),
                 halign="right", valign="top",
                 size_hint_y=0.42)
    pct.bind(size=lambda w, v: setattr(w, "text_size", v))
    wrap.add_widget(pct)
    return wrap


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

        # 상단 고정 헤더 (스크롤 안됨)
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
        from ui.dialogs import (show_add_holding, show_delete_holding,
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

        toolbar.add_widget(_tbtn("+ 보유 추가", (0.2, 0.5, 0.9, 1),
                                    lambda *_: show_add_holding(
                                        self.holdings_data, self.refresh)))
        toolbar.add_widget(_tbtn("- 보유 삭제", (0.75, 0.35, 0.35, 1),
                                    lambda *_: show_delete_holding(
                                        self.holdings_data, self.refresh)))
        toolbar.add_widget(_tbtn("JSON", (0.45, 0.45, 0.55, 1),
                                    lambda *_: show_json_menu(
                                        self.holdings_data, self.refresh)))

        # 장마감 투명도 토글 — ToggleButton 단일 위젯 (탭 영역 전체)
        from kivy.uix.togglebutton import ToggleButton
        initial_on = app_state.get("fade_sleeping")
        self.fade_btn = ToggleButton(
            text="장마감 투명", font_size=sp(12), bold=True,
            state="down" if initial_on else "normal",
            size_hint_x=None, width=sp(100))
        self.fade_btn.bind(state=self._on_fade_toggle)
        toolbar.add_widget(self.fade_btn)
        self.add_widget(toolbar)

        self.prices = {}

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
        # 헤더는 스크롤 밖 고정 컨테이너로
        self.header_container.clear_widgets()
        self.header_container.add_widget(self._build_header())

        self.container.clear_widgets()
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

        # 1차 패스: 합계 계산 (카드 배경색을 전체 손익 기준으로 정하기 위함)
        for stock in holdings:
            t = stock["ticker"]
            shares = stock.get("shares", 0)
            avg = stock.get("avg_price", 0)
            price_info = prices.get(t, {})
            cur_price = price_info.get("price", 0) or avg
            base_price = price_info.get("base", 0) or cur_price
            net_price = cur_price * FEE_MULTIPLIER

            total_invested += shares * avg
            total_current += round(net_price * shares)
            total_yesterday += round(base_price * FEE_MULTIPLIER * shares)

        # 2차 패스: 카드 렌더 (각 카드 색은 _build_holding_row 내부에서 종목별 손익으로 결정)
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
        """헤더 제거 — 카드 자체에 모든 정보 inline 표시"""
        box = BoxLayout(orientation="vertical", size_hint_y=None,
                         height=0)
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
                         padding=(sp(4), sp(3)), spacing=sp(0))
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

        # 상태 뱃지 텍스트 (손절 / warn_text)
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

        name_bold = not (is_sleeping and fade_on)

        # 수급 데이터 미리 파싱
        flow = investor_cache.get(ticker) or {}
        inst = flow.get("기관", 0) if flow else None
        foreign = flow.get("외국인", 0) if flow else None
        foreign_ratio = flow.get("외국인비율", 0) if flow else 0
        pension = flow.get("연기금", 0) if flow else None

        # 컨센서스 score
        consensus = consensus_cache.get(ticker) or {}
        target = consensus.get("target")
        score = consensus.get("score")
        target_gap_pct = ((target - cur_price) / cur_price * 100) if (target and cur_price) else 0

        # ─── 카드 컨테이너: 좌(종목 정보) / 우(수급)
        l1_wrap = TappableBox(orientation="vertical", size_hint_y=None,
                                size_hint_x=1, padding=(sp(4), sp(4)))
        l1_wrap.bind(minimum_height=l1_wrap.setter("height"))
        l1_wrap.bind(on_release=lambda *_: open_toss_stock(ticker))

        l1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        padding=(sp(10), sp(8)), spacing=sp(8))
        l1.bind(minimum_height=l1.setter("height"))
        l1_wrap.add_widget(l1)
        # 카드 색 — 종목별 손익 부호: 수익(+) 연빨강, 손실(-) 연파랑, 0 흰색
        # 휴면+fade_on 인 경우엔 _c() 가 70% 흰색 페이드 적용 → 색상 유지하면서 흐려짐
        if pnl > 0:
            card_bg = "#f8dcdc"   # 연한 빨강
        elif pnl < 0:
            card_bg = "#dce6f2"   # 연한 파랑
        else:
            card_bg = "#ffffff"
        name_bg = _c(card_bg)
        from kivy.graphics import Color as _Col, RoundedRectangle as _RR
        with l1.canvas.before:
            _Col(*rgba(name_bg))
            l1._bg = _RR(pos=l1.pos, size=l1.size, radius=[sp(8)])
        l1.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                 size=lambda w, v: setattr(w._bg, "size", v))

        prefix = "zZ " if is_sleeping else ""
        if account == "퇴직연금":
            prefix += "[퇴] "

        big_px = int(sp(17))     # 종목명·가격 강조
        med_px = int(sp(14))     # 본문 라인 숫자
        small_px = int(sp(11))   # 라벨 / 회색 텍스트
        gray_hex = "999999"
        body_hex = _c("#222").lstrip("#")
        sector = sector_cache.get(ticker) or ""

        def _line(markup_text, h=sp(22)):
            lbl = Label(
                text=markup_text, markup=True, font_size=FONT_SMALL,
                color=rgba(_c("#222")),
                size_hint_y=None, height=h,
                halign="left", valign="middle",
                max_lines=1, shorten=True, shorten_from="right")
            lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            return lbl

        # ─── 좌측: 5줄
        left_col = BoxLayout(orientation="vertical", size_hint_x=0.62,
                              size_hint_y=None, spacing=sp(2))
        left_col.bind(minimum_height=left_col.setter("height"))

        # Line 1: [zZ] 종목명 (보유수)  [뱃지]  섹터설명
        name_line = BoxLayout(orientation="horizontal", size_hint_y=None,
                               height=sp(22), spacing=sp(4))
        name_color_hex = _c(sign_color(pnl_pct)).lstrip("#")
        # 이름은 auto-width — texture_size 로 실제 텍스트 폭만 차지
        name_only_markup = (f"[color={name_color_hex}]"
                             f"[b]{prefix}{name} ({shares}주)[/b]"
                             f"[/color]")
        name_lbl = Label(
            text=name_only_markup, markup=True, font_size=FONT_MD,
            color=rgba(_c("#222")),
            size_hint=(None, 1),
            halign="left", valign="middle")
        name_lbl.bind(texture_size=lambda w, v: setattr(w, "width", v[0]))
        name_line.add_widget(name_lbl)

        if badge_text:
            from kivy.graphics import Color, RoundedRectangle
            badge_w = sp(28)
            badge = Label(
                text=badge_text, bold=True, font_size=sp(10),
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
            name_line.add_widget(badge)

        # 섹터는 라벨 옆에 회색 작게
        if sector:
            sector_lbl = Label(
                text=sector, font_size=sp(11),
                color=rgba(_c("#999")),
                halign="left", valign="middle",
                max_lines=1, shorten=True, shorten_from="right")
            sector_lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            name_line.add_widget(sector_lbl)
        else:
            name_line.add_widget(BoxLayout())  # 우측 스페이서
        left_col.add_widget(name_line)

        # Line 2: 현재가원 (거래량)
        vol_str = f"({format_volume(volume)})" if volume else ""
        price_color_hex = _c(sign_color(day_diff) if day_diff else "#222").lstrip("#")
        price_markup = (f"[color={price_color_hex}]"
                         f"[size={big_px}][b]{int(cur_price):,}원[/b][/size]"
                         f"[/color]"
                         + (f"  [color={gray_hex}][size={small_px}]{vol_str}[/size][/color]"
                            if vol_str else ""))
        left_col.add_widget(_line(price_markup, h=sp(26)))

        # Line 3: 어제보다 +diff (pct%)
        if day_diff:
            day_color_hex = _c(sign_color(day_diff)).lstrip("#")
            day_markup = (f"[color={body_hex}]어제보다[/color] "
                          f"[color={day_color_hex}][b]{format_signed(day_diff)}[/b] "
                          f"({day_pct:+.2f}%)[/color]")
        else:
            day_markup = f"[color={gray_hex}]어제보다[/color]"
        left_col.add_widget(_line(day_markup))

        # Line 4: 전체수익 +pnl (pct%)
        if pnl != 0:
            pnl_color_hex = _c(sign_color(pnl)).lstrip("#")
            pnl_markup = (f"[color={body_hex}]전체수익[/color] "
                          f"[color={pnl_color_hex}][b]{format_signed(pnl)}[/b] "
                          f"({pnl_pct:+.2f}%)[/color]")
        else:
            pnl_markup = f"[color={gray_hex}]전체수익[/color]"
        left_col.add_widget(_line(pnl_markup))

        # Line 5: 목표 (score) target (+gap%) — 없으면 그냥 "목표" 회색
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

        # ─── 우측: 4줄 (수급)
        right_col = BoxLayout(orientation="vertical", size_hint_x=0.38,
                               size_hint_y=None, spacing=sp(2))
        right_col.bind(minimum_height=right_col.setter("height"))

        # 외국인 보유 x.xx% — 숫자는 검정 (자산 비율은 +/- 의미 없음)
        if foreign_ratio and foreign_ratio > 0:
            ratio_markup = (f"[color={body_hex}]외국인 보유[/color] "
                             f"[color={body_hex}][b]{foreign_ratio:.2f}%[/b][/color]")
        else:
            ratio_markup = f"[color={gray_hex}]외국인 보유[/color]"
        right_col.add_widget(_line(ratio_markup))

        # 라벨 +amount 형식 헬퍼
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

    def _build_total_row(self, invested, current, yesterday):
        pnl = current - invested
        pnl_pct = (pnl / invested * 100) if invested else 0
        day_diff = current - yesterday
        day_pct = (day_diff / yesterday * 100) if yesterday else 0

        # 외곽 wrap (스크롤 밖이라 약간의 padding 유지)
        outer = BoxLayout(orientation="vertical", size_hint_y=None,
                           padding=(sp(8), sp(6)), spacing=0)
        outer.bind(minimum_height=outer.setter("height"))

        # 카드보드 박스 — 흰 배경 + 회색 테두리 둥근 모서리
        card = BoxLayout(orientation="vertical", size_hint_y=None,
                          padding=(sp(12), sp(8)), spacing=sp(4))
        card.bind(minimum_height=card.setter("height"))
        from kivy.graphics import Color, Line, RoundedRectangle
        with card.canvas.before:
            Color(*rgba("#ffffff"))
            card._bg = RoundedRectangle(pos=card.pos, size=card.size,
                                          radius=[sp(8)])
            Color(*rgba("#dcdcdc"))
            card._border = Line(rounded_rectangle=(0, 0, 1, 1, sp(8)),
                                 width=1)
        def _card_resize(w, _v):
            w._bg.pos = w.pos
            w._bg.size = w.size
            w._border.rounded_rectangle = (w.x, w.y, w.width, w.height, sp(8))
        card.bind(pos=_card_resize, size=_card_resize)

        body_hex = "222222"
        pnl_color_hex = sign_color(pnl).lstrip("#")
        day_color_hex = sign_color(day_diff).lstrip("#")
        med_px = int(sp(15))   # 금액 강조

        # 행 1: 매수가 X,XXX,XXX    어제보다 +X,XXX (X.XX%)
        row1_text = (f"[color={body_hex}]매수가 [b]{int(invested):,}[/b][/color]"
                      f"    "
                      f"[color={body_hex}]어제보다[/color] "
                      f"[color={day_color_hex}][size={med_px}][b]{format_signed(day_diff)}[/b][/size] "
                      f"({day_pct:+.2f}%)[/color]")
        # 행 2: 현재가 X,XXX,XXX    전체수익 +XXX,XXX (X.XX%)
        row2_text = (f"[color={body_hex}]현재가 [b]{int(current):,}[/b][/color]"
                      f"    "
                      f"[color={body_hex}]전체수익[/color] "
                      f"[color={pnl_color_hex}][size={med_px}][b]{format_signed(pnl)}[/b][/size] "
                      f"({pnl_pct:+.2f}%)[/color]")

        for txt, h in ((row1_text, sp(24)), (row2_text, sp(24))):
            lbl = Label(
                text=txt, markup=True, font_size=FONT_SMALL,
                color=rgba("#222"),
                size_hint_y=None, height=h,
                halign="left", valign="middle",
                max_lines=1, shorten=True, shorten_from="right")
            lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            card.add_widget(lbl)

        outer.add_widget(card)
        return outer
