"""모바일 UI 스크린샷 캡처 (탭별 PNG 저장)"""
import os
import sys
from pathlib import Path

# 모바일 폰 폭 시뮬레이션
os.environ.setdefault("KIVY_NO_ARGS", "1")

from kivy.config import Config
Config.set("graphics", "width", "390")    # iPhone 14 폭
Config.set("graphics", "height", "844")   # iPhone 14 높이
Config.set("graphics", "resizable", "0")

from kivy.app import App
from kivy.clock import Clock
from kivy.core.window import Window
from kivy.core.text import LabelBase
from kivy.metrics import sp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.spinner import Spinner

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

_KR_FONT = SCRIPT_DIR / "assets" / "NotoSansKR.ttf"
if _KR_FONT.exists():
    LabelBase.register(name="Roboto",
                        fn_regular=str(_KR_FONT),
                        fn_bold=str(_KR_FONT))

from data_service import load_holdings
from ui.tab_us import TabUS
from ui.tab_holdings import TabHoldings

HOLDINGS_PATH = SCRIPT_DIR / "holdings.json"
OUT_DIR = SCRIPT_DIR.parent / "data"
OUT_DIR.mkdir(exist_ok=True)


class CaptureApp(App):
    title = "포트폴리오 캡처"

    def build(self):
        Window.clearcolor = (0.98, 0.98, 0.98, 1)
        self.holdings_data = load_holdings(HOLDINGS_PATH)

        root = BoxLayout(orientation="vertical")
        self.spinner = Spinner(
            text="미국증시", values=["미국증시", "보유종목"],
            size_hint_y=None, height=sp(44), font_size=sp(16),
            background_color=(0.9, 0.9, 0.95, 1),
        )
        root.add_widget(self.spinner)

        self.content = BoxLayout(orientation="vertical")
        root.add_widget(self.content)

        self.tab_us = TabUS()
        self.tab_holdings = TabHoldings(self.holdings_data)

        self._step = 0
        Clock.schedule_once(self._show_us, 0.5)
        return root

    def _show_us(self, dt):
        print("[capture] showing 미국증시")
        self.spinner.text = "미국증시"
        self.content.clear_widgets()
        self.content.add_widget(self.tab_us)
        self.tab_us.refresh()
        Clock.schedule_once(self._snap_us, 6.0)

    def _snap_us(self, dt):
        path = str(OUT_DIR / "mobile_us.png")
        Window.screenshot(name=path)
        print(f"[capture] saved {path}")
        Clock.schedule_once(self._show_holdings, 0.5)

    def _show_holdings(self, dt):
        print("[capture] showing 보유종목")
        self.spinner.text = "보유종목"
        self.content.clear_widgets()
        self.content.add_widget(self.tab_holdings)
        self.tab_holdings.refresh()
        Clock.schedule_once(self._snap_holdings, 6.0)

    def _snap_holdings(self, dt):
        path = str(OUT_DIR / "mobile_holdings.png")
        Window.screenshot(name=path)
        print(f"[capture] saved {path}")
        Clock.schedule_once(lambda _dt: App.get_running_app().stop(), 0.5)


if __name__ == "__main__":
    CaptureApp().run()
