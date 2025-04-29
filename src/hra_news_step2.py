# src/hra_news_step2.py - 본문 수집 + 요약 + 시트 업로드

import os
import sys
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime

from utils.logger import log_info, log_error
from utils.file_manager import get_today_folder, get_today_filename
from utils.gpt_utils import summarize_all_in_3_lines, analyze_articles_batch
from utils.sheet_uploader import upload_to_google_sheet

# ✅ 본문 수집 함수

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

    return content.get_text(separator="\n", strip=True)


def main():
    log_info("📄 중요 기사 로드 중...")
    today_folder = get_today_folder()
    input_file = os.path.join(today_folder, get_today_filename("step1_filtered.csv"))
    output_file = os.path.join(today_folder, get_today_filename("step2_final.csv"))

    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig")
    except Exception as e:
        log_error(f"❌ 파일 로딩 실패: {e}")
        sys.exit(1)

    log_info(f"✅ 중요 기사 수: {len(df)}건")

    # ✅ 본문 수집
    log_info("📰 중요 기사 본문 수집 중...")
    tqdm.pandas()
    df["본문"] = df["URL"].progress_apply(get_naver_news_body)

    # ✅ 요약 (선택 적용)
    df = summarize_all_in_3_lines(df)

    # ✅ 시트 업로드
    sheet_id = "1l89Eca3CsjLEjG-9_raVMy6Y_sYE4BLA-XRtgwEhHEc"  # <- 필요 시 수정
    sheet_name = "네이버API(첨부파일용)"
    try:
        upload_to_google_sheet(df, sheet_id, sheet_name)
    except Exception as e:
        log_error(f"❌ Google Sheets 업로드 실패: {e}")

    # ✅ 최종 저장
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log_info(f"✅ 최종 결과 저장 완료: {output_file}")


if __name__ == "__main__":
    main()
