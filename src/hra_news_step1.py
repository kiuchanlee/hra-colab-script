# src/hra_news_step1.py - GPT 기반 중요 기사 선별 (본문 수집 없음)

import os
import sys
import pandas as pd
import html
from datetime import datetime
from utils.logger import log_info, log_error
from utils.file_manager import get_today_folder, get_today_filename
from utils.gpt_utils import analyze_articles_batch
from utils.gpt_utils import deduplicate_news_with_gpt
from utils.gpt_utils import deduplicate_news_with_gpt_twopass 

def main():
    log_info("📄 뉴스 헤드라인 데이터 불러오는 중...")
    today_folder = get_today_folder()
    input_file = os.path.join(today_folder, get_today_filename("step0_raw.csv"))
    output_file = os.path.join(today_folder, get_today_filename("step1_filtered.csv"))

    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig")
    except Exception as e:
        log_error(f"❌ 파일 로딩 실패: {e}")
        sys.exit(1)

    log_info(f"✅ 데이터 로드 완료: {len(df)}건")

    # ✅ 언론사 도메인 → 한글 언론사명 매핑
    domain_to_korean = {
        "shindonga.donga.com": "월간 신동아",
        "magazine.hankyung.com": "매거진 한경",
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
        "www.sisain.co.kr": "시사IN",
        "sports.khan.co.kr" : "스포츠경향",
        "sports.donga.com" : "스포츠동아",
        "insweek.co.kr" : "보험신보",
        "insjournal.co.kr" : "보험저널",
        "insnews.co.kr" : "한국보험신문"
    }
    df["매체명"] = df["매체명"].map(domain_to_korean).fillna(df["매체명"])

    # ✅ 제목 정리: HTML 제거 + 대괄호 제거
    df["헤드라인"] = df["헤드라인"].apply(html.unescape)
    df["헤드라인"] = df["헤드라인"].str.replace(r"\[.*?\]", "", regex=True).str.strip()

    
    # ✅ 중복 제거 먼저
    df = deduplicate_news_with_gpt_twopass(df)
    
    # ✅ GPT 분석 실행
    df = analyze_articles_batch(df)

    # ✅ 중요도 3 이상만 필터링
    df = df[df["중요도"] >= 3].reset_index(drop=True)
    log_info(f"✨ 중요도 3 이상 기사 수: {len(df)}건")

    # ✅ 저장
    os.makedirs(today_folder, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log_info(f"✅ 중요 기사 저장 완료: {output_file}")


if __name__ == "__main__":
    main()
