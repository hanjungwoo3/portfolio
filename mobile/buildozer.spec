[app]
title = 포트폴리오
package.name = portfolio
package.domain = com.hanjungwoo3.portfolio

source.dir = .
source.include_exts = py,png,jpg,json,ttf
source.include_patterns = holdings.json,ui/*.py

version = 1.0

# Python / Kivy
requirements = python3,kivy==2.3.0,requests,urllib3,charset-normalizer,idna,certifi,beautifulsoup4,soupsieve

# 한글 폰트 (Kivy 기본 폰트가 한글 미지원이라 추가 필요)
# NotoSansKR 또는 나눔고딕을 넣어주세요. 없으면 빌드 시 제거 가능.
# android.add_assets = assets/fonts/NotoSansKR-Regular.ttf

# 오리엔테이션 — 세로만
orientation = portrait

# Android 설정
android.api = 31
android.minapi = 26
android.ndk = 25b
android.arch = arm64-v8a,armeabi-v7a

# 인터넷 권한
android.permissions = INTERNET

# fullscreen off (상태바 보이게)
fullscreen = 0

# 아이콘 (선택)
# icon.filename = %(source.dir)s/icon.png

# 로그 레벨
log_level = 2

[buildozer]
log_level = 2
warn_on_root = 1
