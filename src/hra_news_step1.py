# src/hra_news_step1.py

import pandas as pd
import html
import re
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import sys

from utils.logger import log_info, log_error
from utils.file_manager import get_today_folder, get_today_filename

# 본문 스크랩핑 함수
def get_naver_news_body(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            return f"❌ 요청 실패: {response.status_code}"
    except Exception as e:
        return f"❌ 요청 실패: {e}"

    soup = BeautifulSoup(response.text, 'html.parser')
    content = soup.find('article', {'id': 'dic_area'})
    if not content:
        return "❌ 본문이 존재하지 않음"

    text = content.get_text(separator="\n", strip=True)
    return text


def main():
    log_info("\ud83d\udcc5 뉴스 데이터 불러오는 중...")
    today_folder = get_today_folder()
    input_file = os.path.join(today_folder, get_today_filename("step0_raw.csv"))
    output_file = os.path.join(today_folder, get_today_filename("step1_processed.csv"))

    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig")
    except Exception as e:
        log_error(f"❌ 파일 로딩 실패: {e}")
        sys.exit(1)

    tqdm.pandas()

    # 언론사 도메인 → 한글 언론사명 매핑
    domain_to_korean = {
        "shindonga.donga.com" : "월간 신동아"
        "magazine.hankyung.com" : "매거진 한경"
        "www.chosun.com": "조선일보",
        "biz.chosun.com": "조선비즈",
        "weekly.chosun.com": "주간조선",
        "www.joins.com": "중앙일보",
        "www.donga.com": "동아일보",
        "weekly.donga.com": "주간동아",
        "www.khan.co.kr": "경향신문",
        "weekly.khan.co.kr": "주간경향",
        "www.hani.co.kr": "한겨레",
        "www.hankyung.com": "한국경제",
        "www.mk.co.kr": "매일경제",
        "www.magazine.mk.co.kr": "매경이코노미",
        "www.hankookilbo.com": "한국일보",
        "view.asiae.co.kr": "아시아경제",
        "www.edaily.co.kr": "이데일리",
        "news.heraldcorp.com": "헤럴드경제",
        "www.fnnews.com": "파이낸셜뉴스",
        "news.mt.co.kr": "머니투데이",
        "www.sisain.co.kr": "시사IN"
    }
    df["매체명"] = df["매체명"].map(domain_to_korean).fillna(df["매체명"])

    # 제목 HTML 디코딩 + [대괄호] 제거
    df["헤드라인"] = df["헤드라인"].apply(html.unescape)
    df["헤드라인"] = df["헤드라인"].str.replace(r"\[.*?\]", "", regex=True).str.strip()

    # 기사 본문 수집
    log_info("\ud83d\udcf0 기사 본문 수집 중...")
    df["본문"] = df["URL"].progress_apply(get_naver_news_body)

    # 저장
    os.makedirs(today_folder, exist_ok=True)
    df = df.reset_index(drop=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log_info(f"✅ 저장 완료: {output_file}")


if __name__ == "__main__":
    main()
