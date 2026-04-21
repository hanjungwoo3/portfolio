# 포트폴리오 모니터

macOS 메뉴바 기반 실시간 주식 포트폴리오 모니터링 앱.

## 주요 기능

- **실시간 가격**: 토스증권 공개 API로 5초 간격 조회 (세후 기준)
- **전일대비 / 거래량 / 피크가 추적**
- **손절/익절 뱃지**: 매수가 -9% / 피크가 -9% 시 색상 뱃지
- **투자자 수급**: 외국인(보유율%) / 기관 + 세부 8개 분류 (연기금, 금융투자, 투신, 사모, 보험, 은행, 기타금융, 기타법인)
- **미국 증시 패널**: 실시간 지수 + 선물 (20개, 좌우 2분할)
- **퇴직연금 별도 테이블**
- **시간외(NXT) 거래 지원 종목 표시**
- **좌우 분할**: 핵심 컬럼 고정 + 수급 컬럼 가로 스크롤

## 설치 및 실행

```bash
# 첫 실행 시 자동으로 venv 생성 + 패키지 설치
./run.sh
```

메뉴바의 **📈** 아이콘 클릭 → "포트폴리오 창 열기"

## 설정

### `data/holdings.json` — 보유 종목
```json
{
  "holdings": [
    {
      "ticker": "420770",
      "name": "기가비스",
      "shares": 6,
      "avg_price": 88666,
      "buy_date": "20260416",
      "market": "KOSDAQ"
    },
    {
      "ticker": "069500",
      "name": "KODEX 200",
      "shares": 811,
      "avg_price": 93004,
      "buy_date": "20260226",
      "market": "KOSPI",
      "account": "퇴직연금"
    }
  ]
}
```
- `account: "퇴직연금"` 지정 시 별도 테이블에 표시

### `data/config.json` — 알림 설정
```json
{
  "stop_loss_alert_pct": -9.0,        // 손절 경고 임계
  "trailing_stop_alert_pct": -9.0,    // 피크 하락 경고 임계
  "polling_interval_seconds": 5,      // 갱신 주기
  "alert_cooldown_minutes": 15,       // 경고 중복 방지
  "sell_fee_pct": 0.2                 // 매도 수수료·세금 차감율
}
```

## 시스템 요구사항

- macOS (메뉴바 + AppleScript 사용)
- Python 3.9+
- Chrome 또는 Safari (종목 더블클릭 시 토스 페이지 이동)

## 자동 시작 (선택)

```bash
cp com.jusic.portfolio.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.jusic.portfolio.plist
```

## 데이터 소스

- **가격 / 수급**: 토스증권 공개 API (`wts-info-api.tossinvest.com`)
- **미국 증시**: yfinance (Yahoo Finance)
- **현재가 폴백**: 네이버 금융
