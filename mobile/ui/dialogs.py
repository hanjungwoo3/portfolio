"""공통 팝업 다이얼로그 — 보유/관심 추가·삭제 + JSON 내보내기/가져오기"""
import json
from datetime import datetime
from pathlib import Path

from kivy.clock import mainthread
from kivy.core.clipboard import Clipboard
from kivy.metrics import sp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.popup import Popup
from kivy.uix.scrollview import ScrollView
from kivy.uix.textinput import TextInput
from kivy.uix.togglebutton import ToggleButton

from data_service import fetch_stock_name, save_holdings


# 공유 holdings path (mobile/holdings.json)
HOLDINGS_PATH = Path(__file__).resolve().parent.parent / "holdings.json"


def _today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def _btn(text, on_press, bg=(0.2, 0.5, 0.9, 1)):
    b = Button(text=text, size_hint_y=None, height=sp(40),
                background_color=bg, background_normal="",
                color=(1, 1, 1, 1), bold=True, font_size=sp(14))
    b.bind(on_release=on_press)
    return b


def _input(hint, input_filter=None):
    return TextInput(
        hint_text=hint, multiline=False, size_hint_y=None, height=sp(40),
        input_filter=input_filter, font_size=sp(14),
        padding=(sp(8), sp(10), sp(8), sp(8)))


def _label(text, color=(0.3, 0.3, 0.3, 1), bold=False, size=sp(13)):
    return Label(text=text, color=color, font_size=size, bold=bold,
                  size_hint_y=None, height=sp(22),
                  halign="left", valign="middle",
                  text_size=(None, sp(22)))


def _toast(text, parent_popup=None):
    """짧은 안내 팝업 (2초 후 자동 닫힘)."""
    from kivy.clock import Clock
    p = Popup(title="", separator_height=0,
               content=Label(text=text, font_size=sp(14)),
               size_hint=(None, None), size=(sp(280), sp(100)))
    p.open()
    Clock.schedule_once(lambda dt: p.dismiss(), 1.8)


# ─── 보유 추가 ────────────────────────────────────────────────
def show_add_holding(holdings_data: dict, on_done, account: str = ""):
    """보유종목 추가 팝업 (account="" 일반 / "퇴직연금")."""
    content = BoxLayout(orientation="vertical", spacing=sp(6),
                         padding=sp(12))

    name_lbl = _label("종목명: -", color=(0.4, 0.4, 0.4, 1))

    ticker_in = _input("종목코드 (6자리)", input_filter="int")
    shares_in = _input("수량", input_filter="int")
    avg_in = _input("평균 매수가 (원)", input_filter="int")
    date_in = _input(f"매수일 (예: {_today_yyyymmdd()})",
                      input_filter="int")
    date_in.text = _today_yyyymmdd()

    # 종목코드 입력 시 자동으로 종목명 조회
    def _on_ticker(instance, value):
        v = value.strip()
        if len(v) == 6 and v.isdigit():
            import threading
            def _fetch():
                n = fetch_stock_name(v)
                @mainthread
                def _set():
                    name_lbl.text = f"종목명: {n or '(조회실패)'}"
                _set()
            threading.Thread(target=_fetch, daemon=True).start()
        else:
            name_lbl.text = "종목명: -"
    ticker_in.bind(text=_on_ticker)

    content.add_widget(_label("보유 종목 추가" + (f" ({account})" if account else ""),
                                bold=True, size=sp(16)))
    content.add_widget(ticker_in)
    content.add_widget(name_lbl)
    content.add_widget(shares_in)
    content.add_widget(avg_in)
    content.add_widget(date_in)

    popup = Popup(title="", separator_height=0,
                   content=content,
                   size_hint=(0.9, None), height=sp(340),
                   auto_dismiss=False)

    def _add(_btn):
        ticker = ticker_in.text.strip()
        if len(ticker) != 6 or not ticker.isdigit():
            _toast("종목코드 6자리 필수")
            return
        try:
            shares = int(shares_in.text.strip() or 0)
            avg = int(avg_in.text.strip() or 0)
        except ValueError:
            _toast("수량/매수가 입력 오류")
            return
        if shares <= 0 or avg <= 0:
            _toast("수량·매수가는 0보다 커야 함")
            return
        buy_date = date_in.text.strip() or _today_yyyymmdd()
        name = (fetch_stock_name(ticker) or ticker)

        new_item = {"ticker": ticker, "name": name,
                    "shares": shares, "avg_price": avg,
                    "invested": shares * avg,
                    "buy_date": buy_date, "market": ""}
        if account:
            new_item["account"] = account
        # 중복(같은 ticker + account) 이면 병합 (추가 매수)
        for s in holdings_data["holdings"]:
            if (s["ticker"] == ticker
                    and (s.get("account") or "") == account):
                total_sh = s.get("shares", 0) + shares
                total_inv = s.get("invested", 0) + shares * avg
                s["shares"] = total_sh
                s["avg_price"] = round(total_inv / total_sh) if total_sh else 0
                s["invested"] = total_inv
                s["buy_date"] = buy_date
                break
        else:
            holdings_data["holdings"].append(new_item)
        save_holdings(HOLDINGS_PATH, holdings_data)
        popup.dismiss()
        on_done()

    btn_row = BoxLayout(orientation="horizontal", spacing=sp(8),
                         size_hint_y=None, height=sp(44))
    btn_row.add_widget(_btn("취소", lambda *_: popup.dismiss(),
                              bg=(0.5, 0.5, 0.5, 1)))
    btn_row.add_widget(_btn("추가", _add))
    content.add_widget(btn_row)

    popup.open()


# ─── 관심 추가 ────────────────────────────────────────────────
def show_add_watch(holdings_data: dict, on_done):
    """관심 종목 추가 — 종목코드만 입력."""
    content = BoxLayout(orientation="vertical", spacing=sp(6),
                         padding=sp(12))
    name_lbl = _label("종목명: -", color=(0.4, 0.4, 0.4, 1))
    ticker_in = _input("종목코드 (6자리)", input_filter="int")

    def _on_ticker(instance, value):
        v = value.strip()
        if len(v) == 6 and v.isdigit():
            import threading
            def _fetch():
                n = fetch_stock_name(v)
                @mainthread
                def _set():
                    name_lbl.text = f"종목명: {n or '(조회실패)'}"
                _set()
            threading.Thread(target=_fetch, daemon=True).start()
        else:
            name_lbl.text = "종목명: -"
    ticker_in.bind(text=_on_ticker)

    content.add_widget(_label("관심 종목 추가", bold=True, size=sp(16)))
    content.add_widget(ticker_in)
    content.add_widget(name_lbl)

    popup = Popup(title="", separator_height=0,
                   content=content,
                   size_hint=(0.9, None), height=sp(200),
                   auto_dismiss=False)

    def _add(_btn):
        ticker = ticker_in.text.strip()
        if len(ticker) != 6 or not ticker.isdigit():
            _toast("종목코드 6자리 필수")
            return
        # 중복 체크
        for s in holdings_data["holdings"]:
            if s["ticker"] == ticker and s.get("account") == "관심":
                _toast("이미 관심종목에 있음")
                return
        name = (fetch_stock_name(ticker) or ticker)
        holdings_data["holdings"].append({
            "ticker": ticker, "name": name,
            "shares": 0, "avg_price": 0, "invested": 0,
            "buy_date": "", "market": "", "account": "관심",
        })
        save_holdings(HOLDINGS_PATH, holdings_data)
        popup.dismiss()
        on_done()

    btn_row = BoxLayout(orientation="horizontal", spacing=sp(8),
                         size_hint_y=None, height=sp(44))
    btn_row.add_widget(_btn("취소", lambda *_: popup.dismiss(),
                              bg=(0.5, 0.5, 0.5, 1)))
    btn_row.add_widget(_btn("추가", _add))
    content.add_widget(btn_row)

    popup.open()


# ─── 공통 삭제 팝업 ────────────────────────────────────────────
def _show_delete(holdings_data: dict, on_done,
                   filter_fn, title: str):
    """filter_fn(stock) -> bool 로 대상 필터링."""
    items = [s for s in holdings_data["holdings"] if filter_fn(s)]
    if not items:
        _toast("삭제할 종목 없음")
        return

    content = BoxLayout(orientation="vertical", spacing=sp(6),
                         padding=sp(12))
    content.add_widget(_label(title, bold=True, size=sp(16)))

    scroll = ScrollView(size_hint_y=1, bar_width=sp(4))
    list_box = BoxLayout(orientation="vertical", size_hint_y=None,
                          spacing=sp(2))
    list_box.bind(minimum_height=list_box.setter("height"))

    selected = set()  # 선택된 ticker 집합

    def _make_toggle(stock):
        t = stock["ticker"]
        name = stock.get("name", t)
        shares = stock.get("shares", 0)
        label = f"{name} ({t})"
        if shares > 0:
            label += f" · {shares}주"
        tb = ToggleButton(text=label, size_hint_y=None, height=sp(36),
                           font_size=sp(13),
                           background_color=(0.92, 0.92, 0.95, 1),
                           background_normal="", color=(0.2, 0.2, 0.2, 1),
                           halign="left", valign="middle")
        def _on_state(w, state):
            if state == "down":
                selected.add(t)
                w.background_color = (1, 0.8, 0.8, 1)
            else:
                selected.discard(t)
                w.background_color = (0.92, 0.92, 0.95, 1)
        tb.bind(state=_on_state)
        return tb

    for s in items:
        list_box.add_widget(_make_toggle(s))
    scroll.add_widget(list_box)
    content.add_widget(scroll)

    popup = Popup(title="", separator_height=0, content=content,
                   size_hint=(0.9, 0.8), auto_dismiss=False)

    def _delete(_btn):
        if not selected:
            _toast("선택된 종목 없음")
            return
        holdings_data["holdings"] = [
            s for s in holdings_data["holdings"]
            if not (filter_fn(s) and s["ticker"] in selected)
        ]
        save_holdings(HOLDINGS_PATH, holdings_data)
        popup.dismiss()
        on_done()

    btn_row = BoxLayout(orientation="horizontal", spacing=sp(8),
                         size_hint_y=None, height=sp(44))
    btn_row.add_widget(_btn("취소", lambda *_: popup.dismiss(),
                              bg=(0.5, 0.5, 0.5, 1)))
    btn_row.add_widget(_btn("삭제", _delete, bg=(0.8, 0.3, 0.3, 1)))
    content.add_widget(btn_row)
    popup.open()


def show_delete_holding(holdings_data: dict, on_done):
    def _is_holding(s):
        acc = s.get("account") or ""
        return acc != "관심"
    _show_delete(holdings_data, on_done, _is_holding, "보유 종목 삭제")


def show_delete_watch(holdings_data: dict, on_done):
    def _is_watch(s):
        return s.get("account") == "관심"
    _show_delete(holdings_data, on_done, _is_watch, "관심 종목 삭제")


# ─── JSON 일괄 관리 ────────────────────────────────────────────
def show_json_menu(holdings_data: dict, on_done):
    """JSON 관리 메뉴 — 내보내기/가져오기 선택."""
    content = BoxLayout(orientation="vertical", spacing=sp(8),
                         padding=sp(14))
    content.add_widget(_label("JSON 일괄 관리",
                                bold=True, size=sp(16)))

    popup = Popup(title="", separator_height=0, content=content,
                   size_hint=(0.82, None), height=sp(240),
                   auto_dismiss=True)

    def _export(_):
        popup.dismiss()
        show_export_json(holdings_data)

    def _import(_):
        popup.dismiss()
        show_import_json(holdings_data, on_done)

    content.add_widget(_btn("↑ 내보내기 (JSON 복사/저장)",
                              _export, bg=(0.2, 0.5, 0.9, 1)))
    content.add_widget(_btn("↓ 가져오기 (JSON 붙여넣기)",
                              _import, bg=(0.35, 0.65, 0.3, 1)))
    content.add_widget(_btn("취소", lambda *_: popup.dismiss(),
                              bg=(0.5, 0.5, 0.5, 1)))
    popup.open()


def _is_syncable(stock: dict) -> bool:
    """sync 대상: 일반 보유, 관심, 사용자 정의 그룹(배당주 등).
    제외: 관심ETF / 퇴직연금 (시스템 예약).
    데스크톱에서 사용자 그룹을 JSON 으로 가져온 경우 모바일에서 보기 가능하도록
    확장 (모바일은 그룹 추가 UI 없음)."""
    acc = stock.get("account") or ""
    return acc not in ("관심ETF", "퇴직연금")


def show_export_json(holdings_data: dict):
    """보유 + 관심 주식만 JSON 으로 표시 + 복사/파일 저장
    (관심ETF / 퇴직연금 제외)."""
    content = BoxLayout(orientation="vertical", spacing=sp(6),
                         padding=sp(10))
    content.add_widget(_label("JSON 내보내기",
                                bold=True, size=sp(16)))

    filtered = [s for s in holdings_data.get("holdings", [])
                if _is_syncable(s)]
    export_data = {"holdings": filtered}
    json_text = json.dumps(export_data, ensure_ascii=False, indent=2)

    text_view = TextInput(
        text=json_text, readonly=True, multiline=True,
        font_size=sp(11), background_color=(0.97, 0.97, 0.97, 1),
        foreground_color=(0.15, 0.15, 0.15, 1))
    content.add_widget(text_view)

    popup = Popup(title="", separator_height=0, content=content,
                   size_hint=(0.95, 0.9), auto_dismiss=False)

    def _copy(_):
        Clipboard.copy(json_text)
        _toast("클립보드에 복사됨")

    def _save(_):
        try:
            # Android 우선, 없으면 Mac/Linux Downloads
            if Path("/storage/emulated/0/Download").exists():
                base = Path("/storage/emulated/0/Download")
            else:
                base = Path.home() / "Downloads"
            base.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fpath = base / f"portfolio_{ts}.json"
            fpath.write_text(json_text, encoding="utf-8")
            _toast(f"저장: {fpath}")
        except Exception as e:
            _toast(f"저장 실패: {e}")

    btn_row = BoxLayout(orientation="horizontal", spacing=sp(6),
                         size_hint_y=None, height=sp(44))
    btn_row.add_widget(_btn("닫기", lambda *_: popup.dismiss(),
                              bg=(0.5, 0.5, 0.5, 1)))
    btn_row.add_widget(_btn("파일 저장", _save,
                              bg=(0.35, 0.65, 0.3, 1)))
    btn_row.add_widget(_btn("복사", _copy, bg=(0.2, 0.5, 0.9, 1)))
    content.add_widget(btn_row)
    popup.open()


def show_import_json(holdings_data: dict, on_done):
    """TextInput 에서 JSON paste → 검증 → 교체 (2-step 확인)."""
    content = BoxLayout(orientation="vertical", spacing=sp(6),
                         padding=sp(10))
    content.add_widget(_label("JSON 가져오기",
                                bold=True, size=sp(16)))
    content.add_widget(_label(
        "전체 holdings.json 내용을 붙여넣으세요",
        size=sp(11), color=(0.45, 0.45, 0.45, 1)))

    text_input = TextInput(
        text="", multiline=True, font_size=sp(11),
        hint_text='{\n  "holdings": [\n    { "ticker": "...", ... }\n  ]\n}')
    content.add_widget(text_input)

    popup = Popup(title="", separator_height=0, content=content,
                   size_hint=(0.95, 0.9), auto_dismiss=False)

    def _paste_clipboard(_):
        t = Clipboard.paste() or ""
        text_input.text = t

    def _apply(_):
        raw = text_input.text.strip()
        if not raw:
            _toast("JSON 내용이 비어있음")
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            _toast(f"JSON 파싱 실패: {str(e)[:40]}")
            return
        if not isinstance(data, dict) or "holdings" not in data:
            _toast("'holdings' 키가 없음")
            return
        if not isinstance(data["holdings"], list):
            _toast("'holdings' 는 배열이어야 함")
            return
        for i, s in enumerate(data["holdings"]):
            if not isinstance(s, dict) or not s.get("ticker"):
                _toast(f"{i}번 항목에 ticker 누락")
                return
        n_new = len(data["holdings"])
        n_old = len(holdings_data.get("holdings", []))
        _confirm_replace(n_old, n_new,
                          lambda: _do_apply(data, popup, on_done))

    def _do_apply(new_data, outer_popup, cb):
        # 기존 ETF/퇴직연금 등 non-syncable 항목 보존
        preserved = [s for s in holdings_data.get("holdings", [])
                     if not _is_syncable(s)]
        # 유입되는 데이터에서도 혹시 모를 ETF/퇴직연금은 버림
        incoming = [s for s in new_data["holdings"] if _is_syncable(s)]
        holdings_data["holdings"] = preserved + incoming
        save_holdings(HOLDINGS_PATH, holdings_data)
        outer_popup.dismiss()
        cb()
        _toast(f"{len(incoming)}개 적용 (보존 {len(preserved)}개)")

    btn_row = BoxLayout(orientation="horizontal", spacing=sp(6),
                         size_hint_y=None, height=sp(44))
    btn_row.add_widget(_btn("취소", lambda *_: popup.dismiss(),
                              bg=(0.5, 0.5, 0.5, 1)))
    btn_row.add_widget(_btn("붙여넣기", _paste_clipboard,
                              bg=(0.3, 0.6, 0.85, 1)))
    btn_row.add_widget(_btn("적용", _apply, bg=(0.8, 0.4, 0.2, 1)))
    content.add_widget(btn_row)
    popup.open()


def _confirm_replace(n_old: int, n_new: int, on_confirm):
    """교체 전 경고 팝업."""
    content = BoxLayout(orientation="vertical", spacing=sp(8),
                         padding=sp(16))
    content.add_widget(_label("! 주의", bold=True, size=sp(16),
                                color=(0.8, 0.3, 0.2, 1)))
    content.add_widget(_label(
        f"현재 {n_old}개 종목이 삭제되고 {n_new}개로 교체됩니다",
        size=sp(13), color=(0.3, 0.3, 0.3, 1)))

    popup = Popup(title="", separator_height=0, content=content,
                   size_hint=(0.85, None), height=sp(190),
                   auto_dismiss=False)

    def _yes(_):
        popup.dismiss()
        on_confirm()

    btn_row = BoxLayout(orientation="horizontal", spacing=sp(8),
                         size_hint_y=None, height=sp(44))
    btn_row.add_widget(_btn("취소", lambda *_: popup.dismiss(),
                              bg=(0.5, 0.5, 0.5, 1)))
    btn_row.add_widget(_btn("진행", _yes, bg=(0.8, 0.3, 0.3, 1)))
    content.add_widget(btn_row)
    popup.open()
