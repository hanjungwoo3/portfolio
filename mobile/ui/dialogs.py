"""공통 팝업 다이얼로그 — 보유/관심 추가·삭제"""
from datetime import datetime
from pathlib import Path

from kivy.clock import mainthread
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
