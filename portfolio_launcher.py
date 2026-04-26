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


def _hide_dock_icon():
    """Dock 아이콘 숨김 — 메뉴바 아이콘만 노출 (NSApplicationActivationPolicyAccessory=1)"""
    try:
        from AppKit import NSApplication
        NSApplication.sharedApplication().setActivationPolicy_(1)
    except Exception:
        pass
WINDOW_SCRIPT_V1 = SCRIPT_DIR / "portfolio_window.py"
WINDOW_SCRIPT_V2 = SCRIPT_DIR / "portfolio_window_v2.py"
VENV_PYTHON = SCRIPT_DIR / "venv" / "bin" / "python3"
# venv가 없으면 시스템 python3 사용
if not VENV_PYTHON.exists():
    VENV_PYTHON = Path("/usr/bin/env")


class LauncherApp(rumps.App):
    def __init__(self):
        super().__init__("📈", quit_button=None)
        # v1, v2 각각 독립 추적 — 동시 실행 가능
        self.procs = {"v1": None, "v2": None}

        self.menu = [
            rumps.MenuItem("📊 포트폴리오 창 열기 (신규 카드 UI)",
                            callback=self.open_window_v2),
            rumps.MenuItem("🗂 포트폴리오 창 열기 (기존 테이블 UI)",
                            callback=self.open_window_v1),
            None,
            rumps.MenuItem("🛑 신규 UI 닫기", callback=self.close_v2),
            rumps.MenuItem("🛑 기존 UI 닫기", callback=self.close_v1),
            None,
            rumps.MenuItem("종료", callback=rumps.quit_application),
        ]

    def _is_running(self, key: str) -> bool:
        p = self.procs.get(key)
        return p is not None and p.poll() is None

    def _spawn(self, key: str, script_path: Path, label: str):
        # 이미 실행 중이면 창을 앞으로 가져오기 (Mission Control / 데스크탑 뒤로 숨었을 경우 대응)
        if self._is_running(key):
            self._bring_to_front()
            rumps.notification(
                title="포트폴리오 모니터", subtitle=label,
                message="이미 실행 중 — 앞으로 가져옵니다",
            )
            return
        try:
            cmd = [str(VENV_PYTHON)]
            if VENV_PYTHON.name == "env":
                cmd.append("python3")
            cmd.append(str(script_path))
            self.procs[key] = subprocess.Popen(cmd, cwd=str(SCRIPT_DIR))
            self.title = "📈"
        except Exception as e:
            rumps.alert(title=f"실행 실패 ({label})", message=str(e))

    def _bring_to_front(self):
        """포트폴리오 창들을 모두 앞으로 가져오기 (다른 데스크탑/뒤에 숨었을 때)."""
        script = '''
tell application "System Events"
    set procs to (every process whose name contains "Python")
    repeat with p in procs
        try
            set frontmost of p to true
        end try
    end repeat
end tell
'''
        try:
            subprocess.Popen(["osascript", "-e", script])
        except Exception:
            pass

    def open_window_v2(self, _sender):
        self._spawn("v2", WINDOW_SCRIPT_V2, "신규 카드 UI")

    def open_window_v1(self, _sender):
        self._spawn("v1", WINDOW_SCRIPT_V1, "기존 테이블 UI")

    def _close(self, key: str, label: str):
        if self._is_running(key):
            p = self.procs[key]
            p.terminate()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            self.procs[key] = None
        else:
            rumps.notification(
                title="포트폴리오 모니터", subtitle=label,
                message="실행 중인 창이 없습니다",
            )

    def close_v2(self, _sender):
        self._close("v2", "신규 카드 UI")

    def close_v1(self, _sender):
        self._close("v1", "기존 테이블 UI")


if __name__ == "__main__":
    _hide_dock_icon()
    LauncherApp().run()
