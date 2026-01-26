# src/hra_news_step1.py - GPT 기반 중요 기사 선별 (본문 수집 없음)


import os
import sys
import pandas as pd
import html
from utils.logger import log_info, log_error
from utils.file_manager import get_today_folder, get_today_filename
from utils.gpt_utils import analyze_articles_batch, deduplicate_news_with_gpt_twopass

def _normalize_headline(s: pd.Series) -> pd.Series:
    """중복 제거를 위한 제목 정규화"""
    return (
        s.fillna("")
         .str.replace(r"\[.*?\]", "", regex=True)
         .str.replace(r"\s+", " ", regex=True)
         .str.strip()
         .str.lower()
    )

def _safe_twopass_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """GPT 중복제거 실패 시 에러 없이 규칙 기반으로 전환"""
    if df.empty: return df
    base = df.reset_index(drop=True).copy()
    try:
        return deduplicate_news_with_gpt_twopass(base)
    except Exception as e:
        log_error(f"⚠️ GPT 중복제거 중 오류 발생(건너뜀): {e}")
        # 폴백: URL 및 제목 기반 단순 중복 제거
        fb = base.copy()
        fb["__norm__"] = _normalize_headline(fb["헤드라인"])
        fb = fb.drop_duplicates(subset=["URL"]).drop_duplicates(subset=["__norm__"])
        return fb.drop(columns=["__norm__"]).reset_index(drop=True)

def main():
    log_info("📄 Step 1: 중요 기사 선별 및 필터링 시작")
    today_folder = get_today_folder()
    input_file = os.path.join(today_folder, get_today_filename("step0_raw.csv"))
    output_file = os.path.join(today_folder, get_today_filename("step1_filtered.csv"))

    # 1. 파일 존재 여부 및 데이터 로드 체크
    if not os.path.exists(input_file):
        log_error(f"❌ 입력 파일이 없습니다: {input_file}")
        sys.exit(1)

    try:
        df = pd.read_csv(input_file, encoding="utf-8-sig")
    except Exception as e:
        log_error(f"❌ CSV 로드 실패: {e}")
        sys.exit(1)

    # 2. 데이터가 0건이거나 URL 컬럼이 없을 때의 방어 (KeyError 방지 핵심)
    if df.empty or "URL" not in df.columns:
        log_info("⚠️ 처리할 데이터가 0건입니다. 빈 결과 파일을 생성합니다.")
        # Step 2에서 기대하는 최소한의 컬럼 구조 생성
        empty_df = pd.DataFrame(columns=["구분", "키워드", "일자", "헤드라인", "요약", "매체명", "URL", "row_id", "중요도"])
        empty_df.to_csv(output_file, index=False, encoding="utf-8-sig")
        return

    # 3. 언론사 매핑 (도메인 기반)
    domain_to_korean = {
        "www.chosun.com": "조선일보", "biz.chosun.com": "조선비즈", 
        "www.hankyung.com": "한국경제", "www.mk.co.kr": "매일경제",
        "www.edaily.co.kr": "이데일리", "www.mt.co.kr": "머니투데이",
        "www.insnews.co.kr": "한국보험신문", "www.insjournal.co.kr": "보험저널",
        "www.joins.com" : "중앙일보", "www.donga.com" : "동아일보",
        "magazine.hankyung.com" : "매거진한경", "www.hani.co.kr" : "한겨레",
        "www.hankookilbo.com" : "한국일보", "view.asiae.co.kr" : "아시아경제",
        "heraldcorp.com" : "헤럴드경제", "www.fnnews.com" : "파이낸셜뉴스",
        "mk.co.kr" : "매일경제", "www.sisain.co.kr" : "시사인", 
        "weekly.chosun.com" : "주간조선", "insweek.co.kr" : "보험신보",
        "www.khan.co.kr" : "경향신문", "weekly.khan.co.kr" : "주간경향", 
        "weekly.donga.com" : "주간동아",  "mbn.mk.co.kr" : "매일경제",
         "sports.donga.com" : "스포츠동아", "sports.khan.co.kr" : "경향스포츠"
    }
    df["매체명"] = df["매체명"].map(domain_to_korean).fillna(df["매체명"])

    # 4. 텍스트 정제 및 인덱스 초기화
    df["헤드라인"] = df["헤드라인"].apply(html.unescape)
    df = df.reset_index(drop=True)

    # 5. GPT 중복 제거 (안전 래퍼)
    df = _safe_twopass_dedupe(df)
    
    # 6. GPT 기사 분석 (중요도 판별)
    df = df.reset_index(drop=True)
    df["row_id"] = df.index # GPT 응답과 매칭을 위한 고정 ID
    
    log_info(f"🤖 GPT 분석 실행 중... (대상: {len(df)}건)")
    try:
        df = analyze_articles_batch(df)
    except Exception as e:
        log_error(f"❌ GPT 분석 중 치명적 오류 발생: {e}")
        if "중요도" not in df.columns: df["중요도"] = 0

    # 7. 중요도 필터링 (중요도 3점 이상)
    if "중요도" in df.columns:
        df = df[df["중요도"] >= 3].reset_index(drop=True)
    
    log_info(f"✨ 필터링 완료: 최종 {len(df)}건 선별됨")

    # 8. 저장
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log_info(f"✅ Step 1 저장 완료: {output_file}")

if __name__ == "__main__":
    main()




# import os
# import sys
# import pandas as pd
# import html
# from datetime import datetime
# from utils.logger import log_info, log_error
# from utils.file_manager import get_today_folder, get_today_filename
# from utils.gpt_utils import analyze_articles_batch
# from utils.gpt_utils import deduplicate_news_with_gpt
# from utils.gpt_utils import deduplicate_news_with_gpt_twopass

# def _normalize_headline(s: pd.Series) -> pd.Series:
#     # 대괄호/공백/중복공백 등 정규화 (폴백 중복제거용)
#     return (
#         s.fillna("")
#          .str.replace(r"\[.*?\]", "", regex=True)
#          .str.replace(r"\s+", " ", regex=True)
#          .str.strip()
#          .str.lower()
#     )

# def _safe_twopass_dedupe(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     GPT 2패스 중복제거를 안전하게 감싸는 래퍼.
#     - 1차: reset_index 후 2패스 시도
#     - 2차: 실패 시 다시 한 번 강제 reset 후 재시도
#     - 3차(폴백): 헤드라인/URL 기반의 전통적 중복제거
#     """
#     base = df.reset_index(drop=True).copy()
#     try:
#         return deduplicate_news_with_gpt_twopass(base.reset_index(drop=True))
#     except IndexError as e:
#         log_error(f"[WARN] GPT 2패스 중복제거 IndexError 1차 발생: {e}. 재시도합니다.")
#         try:
#             return deduplicate_news_with_gpt_twopass(base.reset_index(drop=True))
#         except Exception as e2:
#             log_error(f"[FALLBACK] GPT 2패스 재시도 실패: {e2}. 규칙 기반 중복제거로 폴백합니다.")
#             fb = base.copy()
#             # 규칙 기반 중복 제거: URL 우선, 그 다음 정규화 헤드라인
#             fb["__norm_headline__"] = _normalize_headline(fb["헤드라인"])
#             fb = fb.drop_duplicates(subset=["URL"])
#             fb = fb.drop_duplicates(subset=["__norm_headline__"])
#             fb = fb.drop(columns=["__norm_headline__"])
#             fb = fb.reset_index(drop=True)
#             return fb

# def main():
#     log_info("📄 뉴스 헤드라인 데이터 불러오는 중...")
#     today_folder = get_today_folder()
#     input_file = os.path.join(today_folder, get_today_filename("step0_raw.csv"))
#     output_file = os.path.join(today_folder, get_today_filename("step1_filtered.csv"))

#     try:
#         df = pd.read_csv(input_file, encoding="utf-8-sig")
#     except Exception as e:
#         log_error(f"❌ 파일 로딩 실패: {e}")
#         sys.exit(1)

#     # ⚙️ 입력 안정화: URL 결측 제거, 연속 인덱스 + row_id 부여
#     if "URL" not in df.columns:
#         log_error("❌ 입력 데이터에 URL 컬럼이 없습니다.")
#         sys.exit(1)

#     df = df[df["URL"].notna()].reset_index(drop=True).copy()
#     if "row_id" not in df.columns:
#         df["row_id"] = df.index  # 0..n-1 고정 ID

#     log_info(f"✅ 데이터 로드 완료: {len(df)}건")

#     # ✅ 언론사 도메인 → 한글 언론사명 매핑
#     domain_to_korean = {
#         "shindonga.donga.com": "월간 신동아",
#         "magazine.hankyung.com": "매거진 한경",
#         "www.chosun.com": "조선일보",
#         "biz.chosun.com": "조선비즈",
#         "weekly.chosun.com": "주간조선",
#         "www.joins.com": "중앙일보",
#         "www.donga.com": "동아일보",
#         "weekly.donga.com": "주간동아",
#         "www.khan.co.kr": "경향신문",
#         "weekly.khan.co.kr": "주간경향",
#         "www.hani.co.kr": "한겨레",
#         "www.hankyung.com": "한국경제",
#         "www.mk.co.kr": "매일경제",
#         "h21.hani.co.kr": "한겨레21",
#         "insnews.co.kr": "한국보험신문",
#         "www.magazine.mk.co.kr": "매경이코노미",
#         "www.hankookilbo.com": "한국일보",
#         "view.asiae.co.kr": "아시아경제",
#         "www.edaily.co.kr": "이데일리",
#         "news.heraldcorp.com": "헤럴드경제",
#         "www.fnnews.com": "파이낸셜뉴스",
#         "www.mt.co.kr": "머니투데이",
#         "www.sisain.co.kr": "시사IN",
#         "sports.khan.co.kr": "스포츠경향",
#         "sports.donga.com": "스포츠동아",
#         "insweek.co.kr": "보험신보",
#         "insjournal.co.kr": "보험저널",
#         "insnews.co.kr": "한국보험신문"
#     }
#     df["매체명"] = df["매체명"].map(domain_to_korean).fillna(df["매체명"])

#     # ✅ 제목 정리: HTML 제거 + 대괄호 제거
#     df["헤드라인"] = df["헤드라인"].apply(html.unescape)
#     df["헤드라인"] = df["헤드라인"].str.replace(r"\[.*?\]", "", regex=True).str.strip()

#     # ====== ⛳️ 중요: GPT 중복 제거(안전 래퍼 사용) ======
#     # 내부에서 배치 슬라이싱 시 iloc 범위 문제가 생기지 않도록 입력을 항상 연속 인덱스로 제공
#     df = df.reset_index(drop=True)
#     df = _safe_twopass_dedupe(df)
#     df = df.reset_index(drop=True)

#     # ====== GPT 분석 실행 전에도 인덱스 정리 ======
#     df = df.reset_index(drop=True)
#     df = analyze_articles_batch(df)

#     # ✅ 중요도 3 이상만 필터링
#     if "중요도" not in df.columns:
#         log_error("❌ GPT 분석 결과에 '중요도' 컬럼이 없습니다.")
#         sys.exit(1)

#     df = df[df["중요도"] >= 3].reset_index(drop=True)
#     log_info(f"✨ 중요도 3 이상 기사 수: {len(df)}건")

#     # ✅ 저장
#     os.makedirs(today_folder, exist_ok=True)
#     df.to_csv(output_file, index=False, encoding="utf-8-sig")
#     log_info(f"✅ 중요 기사 저장 완료: {output_file}")

# if __name__ == "__main__":
#     main()
