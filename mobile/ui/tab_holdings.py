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
    Android 에선 supertoss:// 딥링크로 Toss 앱 직접 실행 (설치된 경우),
    스킴 핸들러가 없거나 PC 실행 시 https 브라우저로 폴백.
    """
    code = ticker if is_us else f"A{ticker}"
    # 이전 시도: stocks/ 와 stock/ 모두 앱은 열리되 홈. stockpick/ 로 재시도
    deep = f"supertoss://stockpick/{code}"
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
        """4행 헤더 (회색 배경) — 종목명/거래량 + 3x3 데이터 그리드"""
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

        # Row A: 외국인 | 기관 | 연기금 (맨 위로)
        l2 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        l2.add_widget(_hdr_cell("외국인", COL_A, "right"))
        l2.add_widget(_hdr_cell("기관", COL_B, "right"))
        l2.add_widget(_hdr_cell("연기금", COL_C, "right"))
        box.add_widget(l2)

        # Row B: 매수가 | 피크가(%) | 거래량
        l3 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        l3.add_widget(_hdr_cell("매수가", COL_A, "right"))
        l3.add_widget(_hdr_cell("피크가 (%)", COL_B, "right"))
        l3.add_widget(_hdr_cell("거래량", COL_C, "right"))
        box.add_widget(l3)

        # Row C: 현재가 | 목표가(%) | 외국인보유%
        l4 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(18), spacing=sp(4))
        l4.add_widget(_hdr_cell("현재가", COL_A, "right"))
        l4.add_widget(_hdr_cell("목표가 (%)", COL_B, "right"))
        l4.add_widget(_hdr_cell("외국인보유%", COL_C, "right"))
        box.add_widget(l4)
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

        # 피크 / 목표 + 괴리율
        target = (consensus_cache.get(ticker) or {}).get("target")
        peak_gap_pct = ((cur_price - peak) / peak * 100) if peak else 0
        target_gap_pct = ((target - cur_price) / cur_price * 100) if (target and cur_price) else 0

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

        # ─── 행 1: 카드 50/50 분할
        # 좌: 종목명(보유수) + 뱃지 / 섹터
        # 우: 전체:누적손익(%) / 전일:전일대비(%)
        l1_wrap = TappableBox(orientation="vertical", size_hint_y=None,
                                height=sp(56), padding=(sp(4), sp(2)))
        l1_wrap.bind(on_release=lambda *_: open_toss_stock(ticker))
        l1 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(52), padding=(sp(10), sp(4)), spacing=sp(6))
        l1_wrap.add_widget(l1)
        name_bg = _c("#dce6f2") if not is_sleeping else _c("#ececec")
        from kivy.graphics import Color as _Col, RoundedRectangle as _RR
        with l1.canvas.before:
            _Col(*rgba(name_bg))
            l1._bg = _RR(pos=l1.pos, size=l1.size, radius=[sp(8)])
        l1.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                 size=lambda w, v: setattr(w._bg, "size", v))

        prefix = "zZ " if is_sleeping else ""
        if account == "퇴직연금":
            prefix += "[퇴] "

        # 좌측 블록 — 종목명(보유수) + 섹터, 뱃지 inline
        left_col = BoxLayout(orientation="vertical", size_hint_x=0.5,
                              spacing=sp(1))
        name_line = BoxLayout(orientation="horizontal", size_hint_y=None,
                               height=sp(24), spacing=sp(4))
        name_text = f"{prefix}{name}({shares}주)"
        name_lbl = Label(
            text=name_text, bold=name_bold, font_size=FONT_LG,
            color=rgba(_c(sign_color(pnl_pct))),
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
        name_line.add_widget(BoxLayout())  # 우측 스페이서
        left_col.add_widget(name_line)

        sector = sector_cache.get(ticker) or ""
        sector_lbl = Label(
            text=sector,
            font_size=FONT_SMALL, color=rgba(_c("#666")),
            size_hint_y=None, height=sp(18),
            halign="left", valign="middle",
            max_lines=1, shorten=True, shorten_from="right")
        sector_lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
        left_col.add_widget(sector_lbl)
        l1.add_widget(left_col)

        # 우측 블록 — 전체 +금액 (%) / 오늘 +금액 (%)
        # 라벨은 검정 작은 non-bold, 금액은 sign_color + bold + 큰 폰트, (%) 는 작은 non-bold
        right_col = BoxLayout(orientation="vertical", size_hint_x=0.5,
                               spacing=sp(1))

        def _sub_line(label_text, signed_amt, pct_value, color):
            color_hex = _c(color).lstrip("#")
            big_px = int(sp(15))  # 금액 강조용
            text = (f"[color=222222]{label_text}[/color] "
                    f"[color={color_hex}]"
                    f"[size={big_px}][b]{signed_amt}[/b][/size] "
                    f"({pct_value:+.2f}%)[/color]")
            lbl = Label(
                text=text, markup=True, font_size=FONT_SMALL,
                color=rgba(_c("#222")),
                size_hint_y=None, height=sp(24),
                halign="left", valign="middle",
                max_lines=1, shorten=True, shorten_from="right")
            lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            return lbl

        right_col.add_widget(_sub_line(
            "전체", format_signed(pnl), pnl_pct, sign_color(pnl)))
        right_col.add_widget(_sub_line(
            "오늘", format_signed(day_diff), day_pct, sign_color(day_diff)))
        l1.add_widget(right_col)

        box.add_widget(l1_wrap)

        # 모든 데이터 셀은 make_amt_pct_cell 로 동일 구조 — 금액 위 / (%) 아래 스택
        def _cell(amt, pct, amt_color, pct_color=None, col=COL_A, bold=False):
            return make_amt_pct_cell(
                amt, pct, amt_color, pct_color or amt_color,
                size_hint_x=col, bold=bold,
                font_size=FONT_MD, height=sp(40))

        # ─── 행 A: 외국인 | 기관 | 연기금 (맨 위)
        l2 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(40), spacing=sp(4))
        l2.add_widget(_cell(format_signed(foreign) if foreign else "", "",
                               _c(sign_color(foreign)) if foreign else _c("#aaa"),
                               col=COL_A))
        l2.add_widget(_cell(format_signed(inst) if inst else "", "",
                               _c(sign_color(inst)) if inst else _c("#aaa"),
                               col=COL_B))
        l2.add_widget(_cell(format_signed(pension) if pension else "", "",
                               _c(sign_color(pension)) if pension else _c("#aaa"),
                               col=COL_C))
        box.add_widget(l2)

        # ─── 행 B: 매수가 | 피크가(%) | 거래량
        l3 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(40), spacing=sp(4))
        l3.add_widget(_cell(f"{avg:,}", "", _c("#666"), col=COL_A))
        if peak:
            peak_c = _c(sign_color(peak_gap_pct) if peak_gap_pct < 0 else "#888")
            l3.add_widget(_cell(f"{int(peak):,}", f"({peak_gap_pct:+.2f}%)",
                                  peak_c, col=COL_B))
        else:
            l3.add_widget(_cell("", "", _c("#aaa"), col=COL_B))
        l3.add_widget(_cell(format_volume(volume) if volume else "",
                               "", _c("#666") if volume else _c("#aaa"),
                               col=COL_C))
        box.add_widget(l3)

        # ─── 행 C: 현재가 | 목표가(%) | 외국인보유%
        l4 = BoxLayout(orientation="horizontal", size_hint_y=None,
                        height=sp(40), spacing=sp(4))
        l4.add_widget(_cell(f"{cur_price:,}", "", _c("#333"), col=COL_A))
        if target:
            l4.add_widget(_cell(f"{target:,}", f"({target_gap_pct:+.2f}%)",
                                  _c(sign_color(target_gap_pct)), col=COL_B))
        else:
            l4.add_widget(_cell("", "", _c("#aaa"), col=COL_B))
        l4.add_widget(_cell(
            f"{foreign_ratio:.2f}%" if foreign_ratio and foreign_ratio > 0 else "",
            "", _c("#333") if foreign_ratio and foreign_ratio > 0 else _c("#aaa"),
            col=COL_C))
        box.add_widget(l4)
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

        # 합계는 단일 Label 로 넣어 2열 분할 ellipsis(...손익, ...대비) 방지.
        # 색상은 Kivy markup 으로 금액 구간만 지정.
        row_h = sp(28)

        def _single(amt_label, amt_value, pct_label, pct_value, pct_color):
            color_hex = pct_color.lstrip("#")
            text = (f"{amt_label}: {amt_value:,}    "
                    f"{pct_label}: [color={color_hex}]"
                    f"{pct_value}[/color]")
            lbl = Label(
                text=text, bold=True, font_size=FONT_MD, markup=True,
                color=rgba("#222"),
                size_hint_y=None, height=row_h,
                halign="left", valign="middle",
                max_lines=1, shorten=True, shorten_from="right")
            lbl.bind(size=lambda w, v: setattr(w, "text_size", v))
            return lbl

        box.add_widget(_single(
            "매수가", int(invested),
            "전체",
            f"{format_signed(pnl)} ({pnl_pct:+.2f}%)",
            sign_color(pnl)))
        box.add_widget(_single(
            "현재가", int(current),
            "오늘",
            f"{format_signed(day_diff)} ({day_pct:+.2f}%)",
            sign_color(day_diff)))
        return box
