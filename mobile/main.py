"""
포트폴리오 모바일 앱 (Kivy)

3 탭: 미국증시 / 보유종목 / 관심종목
- 좌우 스와이프 + 화살표 버튼으로 탭 전환 (Carousel)
- 앱이 foreground 일 때만 5초 간격 자동 갱신
"""
import sys
import traceback
from pathlib import Path

# ─── 크래시 디버깅: 예외를 화면에 표시 ─────────────────────────
# 앱 어디서든 예외 발생 시 스택 트레이스를 Label 로 띄운다.
_BOOT_ERROR = None


def _show_error_and_exit(msg: str):
    """Kivy 가 뜬 상태라면 루트를 에러 화면으로 교체."""
    from kivy.app import App
    from kivy.uix.scrollview import ScrollView
    from kivy.uix.label import Label
    from kivy.metrics import sp

    app = App.get_running_app()
    sv = ScrollView()
    lbl = Label(text=msg, font_size=sp(10), color=(1, 0.3, 0.3, 1),
                 halign="left", valign="top",
                 size_hint_y=None, padding=(sp(8), sp(8)))
    lbl.bind(texture_size=lambda w, v: setattr(w, "height", v[1]))
    lbl.bind(width=lambda w, v: setattr(w, "text_size", (v, None)))
    sv.add_widget(lbl)
    if app and app.root:
        app.root.clear_widgets()
        app.root.add_widget(sv)


def _excepthook(exc_type, exc_value, tb):
    msg = "".join(traceback.format_exception(exc_type, exc_value, tb))
    print("[CRASH]", msg)
    try:
        _show_error_and_exit(msg)
    except Exception as e:
        print(f"[CRASH-DISPLAY-FAIL] {e}")


sys.excepthook = _excepthook

try:
    from kivy.app import App
    from kivy.clock import Clock
    from kivy.core.text import LabelBase
    from kivy.core.window import Window
    from kivy.metrics import sp
    from kivy.uix.boxlayout import BoxLayout
    from kivy.uix.button import Button
    from kivy.uix.carousel import Carousel
    from kivy.uix.label import Label

    SCRIPT_DIR = Path(__file__).resolve().parent

    # 한글 폰트 등록 — 나눔고딕 Regular + Bold (없으면 NotoSansKR fallback)
    _KR_FONT_REG = SCRIPT_DIR / "assets" / "NanumGothic.ttf"
    _KR_FONT_BOLD = SCRIPT_DIR / "assets" / "NanumGothicBold.ttf"
    if not _KR_FONT_REG.exists():
        _KR_FONT_REG = SCRIPT_DIR / "assets" / "NotoSansKR.ttf"
        _KR_FONT_BOLD = _KR_FONT_REG
    if _KR_FONT_REG.exists():
        LabelBase.register(
            name="Roboto",
            fn_regular=str(_KR_FONT_REG),
            fn_bold=str(_KR_FONT_BOLD if _KR_FONT_BOLD.exists()
                         else _KR_FONT_REG))

    from data_service import load_holdings
    from ui.tab_us import TabUS
    from ui.tab_holdings import TabHoldings
    from ui.tab_watch import TabWatch
except Exception:
    _BOOT_ERROR = traceback.format_exc()
    print("[BOOT-CRASH]", _BOOT_ERROR)
    # Kivy 가 import 되었기를 기대하며 최소 앱으로 에러를 띄운다.
    from kivy.app import App
    from kivy.uix.scrollview import ScrollView
    from kivy.uix.label import Label
    from kivy.metrics import sp

    class _ErrorApp(App):
        def build(self):
            sv = ScrollView()
            lbl = Label(text=_BOOT_ERROR, font_size=sp(10),
                         color=(1, 0.3, 0.3, 1),
                         halign="left", valign="top",
                         size_hint_y=None, padding=(sp(8), sp(8)))
            lbl.bind(texture_size=lambda w, v: setattr(w, "height", v[1]))
            lbl.bind(width=lambda w, v: setattr(w, "text_size", (v, None)))
            sv.add_widget(lbl)
            return sv

    if __name__ == "__main__":
        _ErrorApp().run()
    raise SystemExit(0)

HOLDINGS_PATH = SCRIPT_DIR / "holdings.json"

VIEW_NAMES = ["미국증시", "보유종목", "관심종목"]


class PortfolioApp(App):
    title = "포트폴리오"

    def build(self):
        Window.clearcolor = (0.98, 0.98, 0.98, 1)
        self.holdings_data = load_holdings(HOLDINGS_PATH)

        root = BoxLayout(orientation="vertical")

        # ─── 상단 헤더: [<] 탭이름 [>] ──────────────
        header = BoxLayout(orientation="horizontal", size_hint_y=None,
                            height=sp(48), spacing=0, padding=0)
        from kivy.graphics import Color, Rectangle
        with header.canvas.before:
            Color(0.18, 0.22, 0.27, 1)  # #2c3744
            header._bg = Rectangle(pos=header.pos, size=header.size)
        header.bind(pos=lambda w, v: setattr(w._bg, "pos", v),
                     size=lambda w, v: setattr(w._bg, "size", v))

        self.btn_prev = Button(
            text="<", size_hint_x=None, width=sp(48),
            font_size=sp(22), bold=True,
            background_color=(0, 0, 0, 0),  # 투명
            color=(1, 1, 1, 1))
        self.btn_prev.bind(on_release=lambda *_: self._go_prev())
        header.add_widget(self.btn_prev)

        # 타이틀 = 클릭하면 현재 탭 새로고침
        self.title_lbl = Button(
            text=VIEW_NAMES[0], bold=True, font_size=sp(18),
            color=(1, 1, 1, 1),
            background_color=(0, 0, 0, 0),
            background_normal="", background_down="")
        self.title_lbl.bind(on_release=lambda *_: self._refresh_current(force=True))
        header.add_widget(self.title_lbl)

        self.btn_next = Button(
            text=">", size_hint_x=None, width=sp(40),
            font_size=sp(22), bold=True,
            background_color=(0, 0, 0, 0),
            color=(1, 1, 1, 1))
        self.btn_next.bind(on_release=lambda *_: self._go_next())
        header.add_widget(self.btn_next)

        # 자동 새로고침 간격 — 헤더 우측 사이클 버튼 (안함 → 5초 → 1분)
        from ui import app_state
        self._refresh_event = None
        self._refresh_options = [0, 5, 60]  # 초 단위, 0=안함
        self.refresh_btn = Button(
            text=self._refresh_label(app_state.get("refresh_interval") or 0),
            size_hint_x=None, width=sp(56),
            font_size=sp(11), bold=True,
            background_color=(0, 0, 0, 0),
            color=(0.85, 0.95, 1, 1))
        self.refresh_btn.bind(on_release=lambda *_: self._cycle_refresh())
        header.add_widget(self.refresh_btn)

        root.add_widget(header)

        # ─── Carousel 본문 (좌우 스와이프 비활성, 버튼 전용) ────────────
        # 세로 ScrollView 와 제스처가 충돌해 스크롤이 끊기는 문제 해결
        from kivy.uix.floatlayout import FloatLayout
        class _SwipelessCarousel(Carousel):
            """터치 이벤트를 자식에 그대로 전달 (스와이프 제스처 감지 스킵)."""
            def on_touch_down(self, touch):
                return FloatLayout.on_touch_down(self, touch)
            def on_touch_move(self, touch):
                return FloatLayout.on_touch_move(self, touch)
            def on_touch_up(self, touch):
                return FloatLayout.on_touch_up(self, touch)

        self.carousel = _SwipelessCarousel(direction="right", loop=False,
                                             anim_move_duration=0.18)
        self.tab_us = TabUS()
        self.tab_holdings = TabHoldings(self.holdings_data)
        self.tab_watch = TabWatch(self.holdings_data)
        self.tabs = [self.tab_us, self.tab_holdings, self.tab_watch]
        for t in self.tabs:
            self.carousel.add_widget(t)
        self.carousel.bind(index=self._on_index_change)
        root.add_widget(self.carousel)

        # 초기 데이터 로드 + 저장된 자동 새로고침 간격 적용
        Clock.schedule_once(lambda dt: self._refresh_current(), 0.2)
        self._apply_refresh_interval(app_state.get("refresh_interval") or 0)
        return root

    @staticmethod
    def _refresh_label(sec: int) -> str:
        if sec <= 0:
            return "수동"
        if sec < 60:
            return f"{sec}초"
        return f"{sec // 60}분"

    def _cycle_refresh(self):
        from ui import app_state
        cur = app_state.get("refresh_interval") or 0
        try:
            idx = self._refresh_options.index(cur)
        except ValueError:
            idx = 0
        nxt = self._refresh_options[(idx + 1) % len(self._refresh_options)]
        app_state.set("refresh_interval", nxt)
        self.refresh_btn.text = self._refresh_label(nxt)
        self._apply_refresh_interval(nxt)

    def _apply_refresh_interval(self, sec: int):
        if self._refresh_event is not None:
            try:
                self._refresh_event.cancel()
            except Exception:
                pass
            self._refresh_event = None
        if sec > 0:
            self._refresh_event = Clock.schedule_interval(
                lambda dt: self._refresh_current(), sec)

    # ─── 탭 전환 ─────────────────────────────────────
    def _on_index_change(self, _car, idx):
        self.title_lbl.text = VIEW_NAMES[idx]
        self._refresh_current()

    def _go_prev(self):
        self.carousel.load_previous()

    def _go_next(self):
        self.carousel.load_next()

    def _refresh_current(self, force=False):
        view = self.tabs[self.carousel.index]
        try:
            view.refresh()
            if force:
                # 짧은 시각 피드백 (헤더 깜빡임)
                from kivy.animation import Animation
                anim = Animation(opacity=0.4, duration=0.1) + Animation(opacity=1.0, duration=0.15)
                anim.start(self.title_lbl)
        except Exception:
            msg = traceback.format_exc()
            print(f"[view-refresh] {msg}")
            try:
                _show_error_and_exit(msg)
            except Exception:
                pass

    # ─── 라이프사이클 ────────────────────────────────
    def on_pause(self):
        return True

    def on_resume(self):
        self._refresh_current()


if __name__ == "__main__":
    PortfolioApp().run()
