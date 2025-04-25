import urllib.request
import urllib.parse
import json
import pandas as pd
from datetime import datetime, timedelta
import html  # 특수문자 제거용
import os

def search_naver_news_multi(queries, client_id, client_secret, display=300, filter_press_names=[]):
    all_results = []

    # 기준 날짜: 오늘 기준 1일 전
    start = (datetime.now() - timedelta(days=1)).date()

    for query in queries:
        print(f"\n🔍 검색어 '{query}' 처리 중...")

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
                print(f"❌ 요청 실패: {e} (query: '{query}')")
                continue

            if response.getcode() != 200:
                print(f"❌ Error Code: {response.getcode()} for query '{query}'")
                continue

            response_body = response.read().decode('utf-8')
            news_data = json.loads(response_body)

            for item in news_data['items']:
                title = html.unescape(item['title'].replace('<b>', '').replace('</b>', ''))
                description = html.unescape(item['description'].replace('<b>', '').replace('</b>', ''))
                link = item['link']
                pubDate = datetime.strptime(item['pubDate'], "%a, %d %b %Y %H:%M:%S %z")

                # 안전한 언론사 도메인 추출
                if 'originallink' in item:
                    parts = item['originallink'].split('/')
                    press_name = parts[2] if len(parts) > 2 else ''
                else:
                    parts = item['link'].split('/')
                    press_name = parts[2] if len(parts) > 2 else ''

                # 날짜 필터 (어제 이후)
                if pubDate.date() < start:
                    continue

                # 언론사 필터
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

        print(f"✅ '{query}' 완료 - {len(query_results)}건 수집됨")
        all_results.extend(query_results)

    df = pd.DataFrame(all_results)

    # ✅ 중복 URL 제거
    df = df.drop_duplicates(subset=["URL"])

    # ✅ URL 필터링 (네이버 뉴스 본문 URL만)
    df = df[df["URL"].str.startswith("https://n.news.naver.com/mnews")].reset_index(drop=True)

    return df

if __name__ == "__main__":
    # 환경 변수에서 설정값 읽기
    queries_str = os.getenv("QUERIES", "")
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")

    if not queries_str or not client_id or not client_secret:
        print("❌ 필수 환경변수가 누락되었습니다. (QUERIES, NAVER_CLIENT_ID, NAVER_CLIENT_SECRET)")
        exit(1)

    queries = [q.strip() for q in queries_str.split(",") if q.strip()]
    
    df = search_naver_news_multi(
        queries=queries,
        client_id=client_id,
        client_secret=client_secret,
        display=300,
        filter_press_names=
            [
            "chosun.com",         # 조선일보
            "joins.com",          # 중앙일보
            "donga.com",          # 동아일보
            "khan.co.kr",         # 경향신문
            "hani.co.kr",         # 한겨레
            "hankyung.com",       # 한국경제
            "mk.co.kr",           # 매일경제
            "hankookilbo.com",    # 한국일보
            "biz.chosun.com",     # 조선비즈
            "asiae.co.kr",        # 아시아경제
            "edaily.co.kr",       # 이데일리
            "news.heraldcorp.com",# 헤럴드경제
            "fnnews.com",         # 파이낸셜뉴스
            "mt.co.kr",           # 머니투데이
            "magazine.mk.co.kr",  # 매경이코노미
            "hankyung.com",       # 한경비즈니스 (한국경제 동일 도메인)
            "donga.com",          # 주간동아 (동아일보 동일 도메인)
            "sisain.co.kr",       # 시사IN
            "weekly.chosun.com"   # 주간조선
        ],
        queries = [
        #▪ 조직 구조 및 인사 제도
        "조직개편", "인사제도"
        # , "직무중심", "직무급제", "성과급제", "호봉제", "직급체계", "연공서열"
    
        # #▪ 채용 및 인재 확보
        # "채용공고", "대기업 공채", "신입채용", "경력채용", "채용시장", "블라인드채용", "고용시장", "경력단절", "채용면접",
        # "공채 폐지", "상시채용", "인재 확보", "리크루팅", "수시채용", "글로벌 인재","컬처핏",
    
        # # ▪ 인사평가 및 보상
        # "인사평가", "성과평가", "역량평가", "보상체계", "연봉제", "성과급", "인센티브",
        # "임금인상", "기본급", "연차수당", "통상임금", "임금피크", "임금체계", "최저임금", "고정급", "성과연봉", "퇴직금",
        # "평균연봉", "근속", "포괄임금", "실업급여", "연금개혁", "스톡옵션", "RSU", "임금협상",
    
        # # 💼 인재 개발 (Talent Development)
        # # ▪ 교육 및 리스킬링
        # "사내 교육", "리스킬링", "업스킬링", "교육훈련", "HRD", "사내대학", "잡포스팅", "사내 양성",
    
        # # ▪ 경력 개발 및 승진
        # "커리어패스", "경력개발", "직무순환", "승진제도", "리더십 프로그램", "후계자 양성", "전직",
    
        # # 🧑‍💻 근무 환경 및 조직 문화 (Workplace & Culture)
        # # ▪ 근무제도 및 유연근무
        # "52시간", "유연근로", "재택근무", "유연근무제", "선택근로제", "단축 근로", "주4일제", "주4.5일제", "근무시간", 
        # "육아휴직", "휴직제도", "출산휴가","근속휴가", "사내복지", "복지 포인트",
    
        # # ▪ 조직문화 및 다양성
        # "조직문화", "워라밸", "DEI", "사내 소통",
    
        # # ⚖️ 정책 및 노동법 (HR Policy & Labor Law)
        # # ▪ 정부 정책 및 제도 변화
        # "인사 정책", "고용 정책", "노동시장 개편", "인사 행정", "인재 정책",
    
        # # ▪ 노동법 및 규제
        # "근로기준법", "노동법", "노동시간", "중대재해법",
        # "직장 내 괴롭힘", "고용 안정성", "산업재해", "노조협약", "임단협", "단체교섭",
        # "근로환경", "근로계약", "정년연장", "파업", "실업자", "노조활동", "노사", "노사갈등", "구조조정", "희망퇴직",
    
        # # 📊 HR 테크 및 데이터 (HR Tech & Analytics)
        # # ▪ HR 테크 도입 사례
        # "HR Tech", "HR 자동화", "인사관리 시스템", "HRIS",
    
        # # ▪ 데이터 기반 인사
        # "인사 데이터", "HR Analytics", "인사 데이터 분석", "데이터 기반 평가",
    
        # # 🏢 그룹/기업별 인사 키워드 (대기업 관련)
        # # ▪ 삼성
        # "삼성 인사", "삼성 임원", "삼성 조직", "삼성 경영진", "삼성 조직개편", "삼성 영입", "삼성 내정", "삼성 발령", "삼성 사외이사",  "삼성 출신",
    
        # # ▪ 현대해상
        # "현대해상 인사", "현대해상 임원", "현대해상 조직", "현대해상 경영진", "현대해상 조직개편", "현대해상 영입", "현대해상 내정", "현대해상 발령", "현대해상 사외이사", "현대해상 출신",
    
        # # ▪ DB손보
        # "DB손보 인사", "DB손보 임원", "DB손보 조직", "DB손보 경영진", "DB손보 조직개편", "DB손보 영입", "DB손보 내정", "DB손보 발령", "DB손보 사외이사",  "DB손보 출신",
    
        # # ▪ KB손보
        # "KB손보 인사", "KB손보 임원", "KB손보 조직", "KB손보 경영진", "KB손보 조직개편", "KB손보 영입", "KB손보 내정", "KB손보 발령", "KB손보 사외이사",  "KB손보 출신",
    
        # # ▪ 메리츠
        # "메리츠 인사", "메리츠 임원", "메리츠 조직", "메리츠 경영진", "메리츠 조직개편", "메리츠 영입", "메리츠 내정", "메리츠 발령", "메리츠 사외이사"  "메리츠 출신"
    
       ])

    # CSV로 저장
    output_file = "crawled_news.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n📄 결과 저장 완료: {output_file}")
