#!/usr/bin/env python3
"""
포트폴리오 메뉴바 런처

맥 상단 메뉴바에 상주하면서 클릭 시 플로팅 윈도우를 띄워주는 런처.
창을 닫아도 런처는 유지되어 언제든 다시 열 수 있음.

Usage:
    python3 portfolio_launcher.py
"""

import os
import sys
import subprocess
from pathlib import Path

import rumps

SCRIPT_DIR = Path(__file__).resolve().parent
WINDOW_SCRIPT = SCRIPT_DIR / "portfolio_window.py"
VENV_PYTHON = SCRIPT_DIR / "venv" / "bin" / "python3"
# venv가 없으면 시스템 python3 사용
if not VENV_PYTHON.exists():
    VENV_PYTHON = Path("/usr/bin/env")


class LauncherApp(rumps.App):
    def __init__(self):
        super().__init__("📈", quit_button=None)
        self.window_proc = None

        self.menu = [
            rumps.MenuItem("📊 포트폴리오 창 열기", callback=self.open_window),
            rumps.MenuItem("🛑 창 닫기", callback=self.close_window),
            None,
            rumps.MenuItem("종료", callback=rumps.quit_application),
        ]

    def _is_running(self) -> bool:
        return self.window_proc is not None and self.window_proc.poll() is None

    def open_window(self, _sender):
        if self._is_running():
            rumps.notification(
                title="포트폴리오 모니터",
                subtitle="",
                message="이미 실행 중입니다",
            )
            return
        try:
            cmd = [str(VENV_PYTHON)]
            if VENV_PYTHON.name == "env":
                cmd.append("python3")
            cmd.append(str(WINDOW_SCRIPT))
            self.window_proc = subprocess.Popen(
                cmd, cwd=str(SCRIPT_DIR),
            )
            self.title = "📈"  # 메뉴바 아이콘 유지
        except Exception as e:
            rumps.alert(title="실행 실패", message=str(e))

    def close_window(self, _sender):
        if self._is_running():
            self.window_proc.terminate()
            self.window_proc = None
        else:
            rumps.notification(
                title="포트폴리오 모니터",
                subtitle="",
                message="실행 중인 창이 없습니다",
            )


if __name__ == "__main__":
    LauncherApp().run()
