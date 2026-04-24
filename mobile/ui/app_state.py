"""앱 UI 설정 — 간단한 JSON 기반 영속 저장."""
import json
from pathlib import Path

STATE_PATH = Path(__file__).resolve().parent.parent / ".app_state.json"

_DEFAULTS = {
    "fade_sleeping": True,  # 장마감 휴면 종목 투명도 적용 on/off
}

_state = dict(_DEFAULTS)


def _load():
    try:
        if STATE_PATH.exists():
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            for k, v in data.items():
                if k in _DEFAULTS:
                    _state[k] = v
    except Exception as e:
        print(f"[app_state] load fail: {e}")


def _save():
    try:
        STATE_PATH.write_text(json.dumps(_state, ensure_ascii=False, indent=2),
                                encoding="utf-8")
    except Exception as e:
        print(f"[app_state] save fail: {e}")


def get(key: str):
    return _state.get(key, _DEFAULTS.get(key))


def set(key: str, value):
    _state[key] = value
    _save()


# 앱 시작 시 1회 로드
_load()
