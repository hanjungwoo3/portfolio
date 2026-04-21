#!/bin/bash
# 포트폴리오 메뉴바 런처 실행
# 메뉴바의 📈 아이콘을 클릭하면 포트폴리오 창이 열림

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# venv 자동 생성 및 활성화
if [ ! -d "venv" ]; then
    echo "[init] 가상환경 생성 중..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -q -r requirements.txt
else
    source venv/bin/activate
fi

python3 portfolio_launcher.py
