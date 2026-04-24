"""
포트폴리오 모바일 앱 (Kivy)

3 탭: 미국증시 / 보유종목 / 관심종목
- 좌우 스와이프 + 화살표 버튼으로 탭 전환 (Carousel)
- 앱이 foreground 일 때만 5초 간격 자동 갱신
"""
from pathlib import Path

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

# 한글 폰트 등록
_KR_FONT = SCRIPT_DIR / "assets" / "NotoSansKR.ttf"
if _KR_FONT.exists():
    LabelBase.register(name="Roboto",
                        fn_regular=str(_KR_FONT),
                        fn_bold=str(_KR_FONT))

from data_service import load_holdings
from ui.tab_us import TabUS
from ui.tab_holdings import TabHoldings
from ui.tab_watch import TabWatch

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
            text=">", size_hint_x=None, width=sp(48),
            font_size=sp(22), bold=True,
            background_color=(0, 0, 0, 0),
            color=(1, 1, 1, 1))
        self.btn_next.bind(on_release=lambda *_: self._go_next())
        header.add_widget(self.btn_next)

        root.add_widget(header)

        # ─── Carousel 본문 (좌우 스와이프) ────────────
        self.carousel = Carousel(direction="right", loop=False,
                                   anim_move_duration=0.18,
                                   scroll_distance=sp(20))
        self.tab_us = TabUS()
        self.tab_holdings = TabHoldings(self.holdings_data)
        self.tab_watch = TabWatch(self.holdings_data)
        self.tabs = [self.tab_us, self.tab_holdings, self.tab_watch]
        for t in self.tabs:
            self.carousel.add_widget(t)
        self.carousel.bind(index=self._on_index_change)
        root.add_widget(self.carousel)

        # 초기 데이터 로드
        Clock.schedule_once(lambda dt: self._refresh_current(), 0.2)
        return root

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
        except Exception as e:
            print(f"[view-refresh] {e}")

    # ─── 라이프사이클 ────────────────────────────────
    def on_pause(self):
        return True

    def on_resume(self):
        self._refresh_current()


if __name__ == "__main__":
    PortfolioApp().run()
