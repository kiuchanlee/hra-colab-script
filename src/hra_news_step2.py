# src/hra_news_step2.py

import os
import sys
import pandas as pd
from datetime import datetime

from utils.logger import log_info, log_error
from utils.file_manager import get_today_folder, get_today_filename
from utils.gpt_utils import deduplicate_news_with_gpt_twopass, analyze_articles_batch
from utils.google_sheet_utils import upload_to_google_sheet


def main():
    log_info("\ud83d\udcc5 뉴스 데이터 로드 중...")
    today_folder = get_today_folder()
    input_file = os.path.join(today_folder, get_today_filename("step1_processed.csv"))
    output_file = os.path.join(today_folder, get_today_filename("step2_final.csv"))

    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig")
    except Exception as e:
        log_error(f"❌ 파일 로딩 실패: {e}")
        sys.exit(1)

    log_info(f"✅ 데이터 로드 완료: {len(df)}건")

    # 1. GPT 중복 제거
    df = deduplicate_news_with_gpt_twopass(df)

    # 2. 중요도 분석
    df = analyze_articles_batch(df)

    # 3. 컬럼 정리 및 정렬
    df = df[[
        "키워드", "일자", "헤드라인", "매체명", "본문",
        "대기업 관련", "HR 관련", "정책/법안 관련", "경제/산업 관련",
        "보험/금융 관련", "중요도", "중요여부"
    ]].copy()

    df = df.sort_values(
        by=["중요도", "키워드", "일자", "헤드라인"],
        ascending=[False, True, False, False]
    ).reset_index(drop=True)

    # 4. 저장
    os.makedirs(today_folder, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log_info(f"✅ 최종 결과 저장 완료: {output_file}")

    # 5. Google Sheets 업로드
    sheet_id = os.getenv("SHEET_ID")
    sheet_name = os.getenv("SHEET_NAME", "네이버API(첨부파일용)")

    if sheet_id:
        upload_to_google_sheet(df, sheet_id, sheet_name)
    else:
        log_error("❌ Google Sheet ID 환경변수(SHEET_ID)가 설정되어 있지 않습니다.")


if __name__ == "__main__":
    main()
