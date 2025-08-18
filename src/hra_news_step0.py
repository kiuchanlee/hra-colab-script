# src/hra_news_step0.py

import urllib.request
import urllib.parse
import json
import pandas as pd
from datetime import datetime, timedelta
import html
import os
import sys

from utils.logger import log_info, log_error
from utils.file_manager import get_today_folder, get_today_filename


def search_naver_news_multi(queries, client_id, client_secret, display=300, filter_press_names=[]):
    all_results = []
    start = (datetime.now() - timedelta(days=1)).date()

    for query in queries:
        log_info(f"🔍 검색어 '{query}' 처리 중...")
        query_results = []

        for start_index in range(1, min(display, 1000)+1, 100):
            encText = urllib.parse.quote(query)
            url = f"https://openapi.naver.com/v1/search/news?query={encText}&display=100&start={start_index}&sort=date"

            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id", client_id)
            request.add_header("X-Naver-Client-Secret", client_secret)

            try:
                response = urllib.request.urlopen(request)
            except Exception as e:
                log_error(f"❌ API 요청 실패: {e} (query: '{query}')")
                continue

            if response.getcode() != 200:
                log_error(f"❌ Error Code: {response.getcode()} for query '{query}'")
                continue

            response_body = response.read().decode('utf-8')
            news_data = json.loads(response_body)

            for item in news_data['items']:
                title = html.unescape(item['title'].replace('<b>', '').replace('</b>', ''))
                description = html.unescape(item['description'].replace('<b>', '').replace('</b>', ''))
                link = item['link']
                pubDate = datetime.strptime(item['pubDate'], "%a, %d %b %Y %H:%M:%S %z")

                if 'originallink' in item:
                    parts = item['originallink'].split('/')
                else:
                    parts = item['link'].split('/')
                press_name = parts[2] if len(parts) > 2 else ''

                if pubDate.date() < start:
                    continue

                if filter_press_names and not any(name in press_name for name in filter_press_names):
                    continue

                query_results.append({
                    "검색어": query,
                    "제목": title,
                    "URL": link,
                    "요약": description,
                    "날짜": pubDate.strftime("%Y-%m-%d"),
                    "언론사": press_name
                })

        log_info(f"✅ '{query}' 완료 - {len(query_results)}건 수집")
        all_results.extend(query_results)

       # 결과 DF 구성
        df = pd.DataFrame(all_results)
    
        # 빈 결과면 컬럼만 맞춰서 반환 (step1에서 컬럼 기대 충족)
        if df.empty:
            return pd.DataFrame(columns=["구분","키워드","일자","헤드라인","요약","매체명","URL","row_id"])
    
        # 중복/필터/재구성
        df = df.drop_duplicates(subset=["URL"])
        df = df[df["URL"].astype(str).startswith("https://n.news.naver.com/mnews")].reset_index(drop=True)
    
        df = pd.DataFrame({
            "구분": "",
            "키워드": df["검색어"],
            "일자": df["날짜"],
            "헤드라인": df["제목"],
            "요약": df["요약"],
            "매체명": df["언론사"],
            "URL": df["URL"]
        }).reset_index(drop=True)
    
        # step1 안전성 위해 절대 위치 id 심기
        df["row_id"] = df.index  # 0..n-1
    
        # ⛔️ 여기 있던 df.to_csv(output_file, ...) 라인은 삭제!
  return df




def main():
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        log_error("❌ NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 환경변수가 없습니다.")
        sys.exit(1)

    queries = [
    
    # 🏢 인사 전략 (HR Strategy)
    # ▪ 조직 구조 및 인사 제도
    "조직개편", "인사제도", "직무중심 인사", "직무급제", "성과급제", "호봉제", "직급체계", "CVC",

    # ▪ 채용 및 인재 확보
    "채용공고", "신입공채", "컬처핏", "컬쳐핏", "신입채용", "경력채용",
    "채용시장", "블라인드채용", "채용공정성", "고용시장", "경력단절", "채용면접",
    "공채 폐지", "상시채용", "인재 확보", "리크루팅", "채용 전략", "수시채용", "글로벌 인재", "인재수혈", "서치펌",

    # ▪ 인사평가 및 보상
    "인사평가", "성과평가", "역량평가", "보상체계", "연봉제", "성과급", "인센티브",
    "임금인상", "임금협상", "기본급", "연차수당", "통상임금", "임금피크",
    "임금체계", "시급제", "최저임금", "고정급", "성과연봉", "퇴직금", "성과보수체계",
    "평균연봉", "근속", "포괄임금", "실업급여", "연금개혁","스톡옵션","RSU","임금분포제","퇴직금누진제","평균 급여", "연봉킹",

    # 💼 인재 개발 (Talent Development)
    # ▪ 교육 및 리스킬링
    "사내 교육", "리스킬링", "업스킬링", "교육훈련", "HRD", "사내대학","잡포스팅", "사내 양성", "인적 자본",

    # ▪ 경력 개발 및 승진
    "커리어패스", "경력개발", "직무순환", "승진제도", "리더십 프로그램", "후계자 양성", "전직","조기 퇴사", "조용한 퇴사", "베이비붐 퇴직",

    # 🧑‍💻 근무 환경 및 조직 문화 (Workplace & Culture)
    # ▪ 근무제도 및 유연근무
    "52시간", "유연근로", "재택근무", "유연근무제", "직장 내 어린이집",
    "선택근로제", "근로시간 단축", "주4일제", "4.5일제", "단축근로", "근무시간", "육아휴직", 
    "휴직제도", "출산휴가", "근속휴가", "사내복지","근무기강", "직원 기강", "임직원 군기","비상 경영", "대기업 사업 축소", "대기업 투자 축소 철회",
    "주 6일 출근", "사장단 회의","조직고령화","고용재설계","대기업 세대 역전", "기업 인력 구조","연차 사용", "휴가 일수", "연차 수당",

    # ▪ 조직문화 및 다양성
    "조직문화", "워라밸", "DEI", "사내 커뮤니케이션", "조직문화 혁신", "컬쳐덱", "컬쳐 에반젤리스트",

    # ⚖️ 정책 및 노동법 (HR Policy & Labor Law)
    # ▪ 정부 정책 및 제도 변화
    "인사 정책", "고용 정책", "노동시장 개편", "인사 행정", "인재 정책","법인세", "상법 개정", "리쇼어링","배임죄","횡령죄","횡재세","교육세","상생 금융",

    # ▪ 노동법 및 규제
    "근로기준법", "노동법", "노동시간", "중대재해법","노란봉투법","책무구조도","지배구조","로펌 노동",
    "직장 내 괴롭힘", "고용 안정성", "산업재해", "노조협약", "임단협", "단체교섭", "단체협약",
    "근로환경", "근로계약", "정년연장", "파업", "실업자", "노조활동", "재고용", "노동조합법","단체교섭권",
    "노사", "노사갈등", "구조조정", "임금협상", "희망퇴직", "인원감축", "사업 개편", "프라임 오피스","중대재해사고","노동 정책",

    # 📊 HR 테크 및 데이터 (HR Tech & Analytics)
    # ▪ HR 테크 도입 사례
    "HR Tech", "HR 자동화", "인사관리 시스템", "HRIS","정보보호","CISO",

    # ▪ 데이터 기반 인사
    "인사 데이터", "HR Analytics", "People Analytics","데이터 기반 평가","AI 기반 채용", "AI 채용 도입", "채용 AI 활용", "AI 면접" ,

    # 🏢 그룹/기업별 인사 키워드 (대기업 관련)
    "삼성 인사", "삼성 임원진", "삼성 조직", "삼성 경영진", "삼성 조직개편", "삼성 영입", "삼성 출신", "삼성 발령", "삼성 사외이사","삼성 M&A","삼성 사업지원 T/F","삼성 리더십",
    "현대해상 인사", "현대해상 임원진", "현대해상 조직", "현대해상 경영진", "현대해상 조직개편", "현대해상 영입", "현대해상 출신", "현대해상 사외이사",
    "DB손보 인사", "DB손보 임원진", "DB손보 조직", "DB손보 경영진", "DB손보 조직개편", "DB손보 영입", "DB손보 출신", "DB손보 사외이사",
    "KB손보 인사", "KB손보 임원진", "KB손보 조직", "KB손보 경영진", "KB손보 조직개편", "KB손보 영입", "KB손보 출신", "KB손보 사외이사",
    "메리츠 인사", "메리츠 임원진", "메리츠 조직", "메리츠 경영진", "메리츠 조직개편", "메리츠 영입", "메리츠 출신", "메리츠 사외이사","GA 영입", "GA 조직개편",
    "손보사 조직 개편", "생보사 조직 개편", "보험사 사외이사", "손보사 생보사 사외이사", "손보 전략적 제휴", "생보 전략적 제휴",
    "손보 노조", "생보 노조", "카드 노조", "은행 노조", "증권 노조", "금융권 조직", "보험사 조직", "손보 영입", "금융권 영입", "보험업계 영입", "보험업계 신설", "인사 담당 임원", "HR 담당 임원",
    "그룹 총수","은행 연합회", "여신 금융 협회", "4대 그룹", "금융 그룹", "금융 지주"]

    df = search_naver_news_multi(
        queries=queries,
        client_id=client_id,
        client_secret=client_secret,
        display=300,
        filter_press_names=[
            "www.chosun.com", "joins.com", "donga.com", "khan.co.kr", "hani.co.kr",
            "hankyung.com", "mk.co.kr", "hankookilbo.com", "biz.chosun.com",
            "asiae.co.kr", "edaily.co.kr", "news.heraldcorp.com", "fnnews.com",
            "mt.co.kr", "magazine.mk.co.kr", "sisain.co.kr", "weekly.chosun.com", "insnews.co.kr", "insjournal.co.kr", "insweek.co.kr"
        ]
    )

    output_dir = get_today_folder()
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, get_today_filename("step0_raw.csv"))

    df.to_csv(output_file, index=False, encoding="utf-8-sig", errors='ignore')
    log_info(f"📄 결과 저장 완료: {output_file}")


if __name__ == "__main__":
    main()
