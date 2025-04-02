
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime, timedelta
from tqdm import tqdm
import math

# ✅ 구글 인증 및 gspread 설정 (GitHub Actions용)
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

def authenticate_gspread():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file("creds.json", scopes=scopes)
    return gspread.authorize(creds)

gc = authenticate_gspread()

# ✅ 뉴스 크롤링 함수
def crawl_news(query, category, start_date, end_date, max_page=1):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.naver.com"
    }

    media_codes = ",".join([
        "023", "025", "020", "032", "028", "015", "009",
        "081", "005", "008", "014", "016", "018", "277", "001"
    ])

    results = []
    seen_links = set()

    for start in range(1, max_page * 10 + 1, 10):
        print(f"\n📄 [{category} - {query}] 페이지 {((start - 1)//10) + 1} 크롤링 중...")

        url = (
        f"https://search.naver.com/search.naver?where=news&query={query}"
        f"&pd=4&ds={start_date}&de={end_date}&office_type=1&office_section_code=1"
        f"&sort=0&start={start}"
        )

        try:
            res = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")
            page_links = list({a["href"] for a in soup.select("a.info") if "n.news.naver.com" in a["href"]})

            print(f"🔗 수집된 기사 링크 수: {len(page_links)}")

            for link in page_links:
                if link in seen_links:
                    continue
                seen_links.add(link)

                try:
                    article = requests.get(link, headers=headers, timeout=10)
                    article_soup = BeautifulSoup(article.text, "html.parser")

                    content = article_soup.select_one("div#newsct_article")
                    content_text = content.get_text(separator=" ").strip() if content else ""

                    title_tag = article_soup.select_one("h2#title_area span")
                    title = title_tag.get_text().strip() if title_tag else ""

                    press_tag = article_soup.select_one("img.media_end_head_top_logo_img")
                    press = press_tag['alt'].strip() if press_tag and 'alt' in press_tag.attrs else ""

                    date_tag = article_soup.select_one("span.media_end_head_info_datestamp_time")
                    raw_date = date_tag.get_text().strip() if date_tag else ""

                    try:
                        raw_date_fixed = raw_date.replace("오전", "AM").replace("오후", "PM")
                        dt = datetime.strptime(raw_date_fixed, "%Y.%m.%d. %p %I:%M")
                        weekday_kor = ["일", "월", "화", "수", "목", "금", "토"][(dt.weekday() + 1) % 7]
                        formatted_date = dt.strftime(f"%m.%d({weekday_kor})")
                    except Exception:
                        formatted_date = raw_date

                    results.append({
                        "구분": category,
                        "키워드": query,
                        "일자": formatted_date,
                        "헤드라인": title,
                        "본문": content_text,
                        "매체명": press,
                        "URL": link
                    })

                    print(f"✅ [{formatted_date}] [{press}] {title[:30]}...")
                    time.sleep(1)

                except Exception as e:
                    print(f"⚠️ 기사 수집 실패: {link} - {e}")
                    continue

        except Exception as e:
            print(f"❌ 페이지 요청 실패: {e}")
            continue

    return pd.DataFrame(results)

# ✅ 키워드 그룹 정의
keywordGroups =  [ {
        "category": "HR",
         "keywords": [
    # 🏢 인사 전략 (HR Strategy)
    # ▪ 조직 구조 및 인사 제도
    "조직개편", "인사제도", "직무중심 인사", "직무급제", "성과급제", "호봉제 개편", "직급체계"

    # ▪ 채용 및 인재 확보
    ,"채용공고", "신입공채", "컬처핏", "컬쳐핏", "신입채용", "경력채용", "인턴채용",
    "채용시장", "블라인드채용", "채용공정성", "고용시장", "경력단절", "채용면접",
    "공채 폐지", "상시채용", "인재 확보", "리크루팅", "채용 전략", "수시채용", "글로벌 인재"

    # ▪ 인사평가 및 보상
    "인사평가", "성과평가", "역량평가", "보상체계", "연봉제", "성과급", "인센티브",
    "임금인상", "임금협상", "기본급", "연차수당", "통상임금", "임금피크",
    "임금체계", "시급제", "최저임금", "고정급", "성과연봉", "퇴직금",
    "평균연봉", "근속", "포괄임금", "실업급여", "연금개혁","스톡옵션","RSU",

    # 💼 인재 개발 (Talent Development)
    # ▪ 교육 및 리스킬링
    "사내 교육", "리스킬링", "업스킬링", "교육훈련", "HRD", "사내대학","잡포스팅", "사내 양성",

    # ▪ 경력 개발 및 승진
    "커리어패스", "경력개발", "직무순환", "승진제도", "리더십 프로그램", "후계자 양성", "전직",

    # 🧑‍💻 근무 환경 및 조직 문화 (Workplace & Culture)
    # ▪ 근무제도 및 유연근무
    "52시간", "유연근로", "재택근무", "유연근무제",
    "선택근로제", "근로시간 단축", "주4일제", "단축근로", "근무시간", "육아휴직", "휴직제도", "출산휴가", "근속휴가", "사내복지",

    # ▪ 조직문화 및 다양성
    "조직문화", "워라밸", "DEI", "사내 커뮤니케이션",

    # ⚖️ 정책 및 노동법 (HR Policy & Labor Law)
    # ▪ 정부 정책 및 제도 변화
    "인사 정책", "고용 정책", "노동시장 개편", "인사 행정", "인재 정책",

    # ▪ 노동법 및 규제
    "근로기준법", "노동법", "노동시간", "중대재해법",
    "직장 내 괴롭힘", "고용 안정성", "산업재해", "노조협약", "임단협", "단체교섭",
    "근로환경", "근로계약", "정년연장", "파업", "실업자", "노조활동",
    "노사", "노사갈등", "구조조정", "임금협상", "희망퇴직",

    # 📊 HR 테크 및 데이터 (HR Tech & Analytics)
    # ▪ HR 테크 도입 사례
    "HR Tech", "AI 채용", "HR 자동화", "인사관리 시스템", "HRIS",

    # ▪ 데이터 기반 인사
    "인사 데이터", "HR Analytics", "인사 데이터 분석",
    "데이터 기반 평가",

    # 🏢 그룹/기업별 인사 키워드 (대기업 관련)
    "삼성 인사", "삼성 임원진", "삼성 조직", "삼성 경영진", "삼성 조직개편", "삼성 영입", "삼성 내정", "삼성 발령", "삼성 사외이사", "삼성 복귀"
    "현대해상 인사", "현대해상 임원진", "현대해상 조직", "현대해상 경영진", "현대해상 조직개편", "현대해상 영입", "현대해상 내정", "현대해상 발령", "현대해상 사외이사",
    "DB손보 인사", "DB손보 임원진", "DB손보 조직", "DB손보 경영진", "DB손보 조직개편", "DB손보 영입", "DB손보 내정", "DB손보 발령", "DB손보 사외이사",
    "KB손보 인사", "KB손보 임원진", "KB손보 조직", "KB손보 경영진", "KB손보 조직개편", "KB손보 영입", "KB손보 내정", "KB손보 발령", "KB손보 사외이사",
    "메리츠 인사", "메리츠 임원진", "메리츠 조직", "메리츠 경영진", "메리츠 조직개편", "메리츠 영입", "메리츠 내정", "메리츠 발령", "메리츠 사외이사"
]}]

# ✅ 날짜 설정
print("\n📆 수집 기간 설정")
days_ago = 1
end_date = datetime.today().strftime("%Y.%m.%d")
start_date = (datetime.today() - timedelta(days=days_ago)).strftime("%Y.%m.%d")
print(f"📆 수집 기간: {start_date} ~ {end_date}\n")

# ✅ 전체 키워드 크롤링 실행
all_results = []
for group in keywordGroups:
    for keyword in group["keywords"]:
        df = crawl_news(keyword, group["category"], start_date, end_date, max_page=1)
        all_results.append(df)

media_codes = [
    # 🗞️ 주요일간지
    "023",  # 조선일보
    "025",  # 중앙일보
    "020",  # 동아일보
    "032",  # 경향신문
    "028",  # 한겨레
    "015",  # 한국경제
    "009",  # 매일경제
    "001",  # 연합뉴스
    "469",  # 한국일보


    # 💰 경제 전문지/경제 매체
    "366",  # 조선비즈
    "277",  # 아시아경제
    "243",  # 이코노미스트 
    "018", # 이데일리 
    "016",  # 헤럴드경제
    "014",  # 파이낸셜뉴스
    "008",  # 머니투데이

    # 📰 경제 주간지 / 시사주간지(경제 중심)
    "024",  # 매경이코노미
    "050",  # 한경비즈니스
    "037",  # 주간동아
    "308",  # 시사IN
    "051",  # 주간조선
]

# ✅ 데이터 정리
df_total = pd.concat(all_results, ignore_index=True)
df_total = df_total.drop_duplicates(subset=["URL"])
# 3. article/다음 세자리 숫자 추출
df_total["media_code"] = df_total["URL"].str.extract(r'article/(\d{3})/')
df_total = df_total[df_total["media_code"].isin(media_codes)]
df_total = df_total[["구분", "키워드", "일자", "헤드라인", "본문", "매체명", "URL"]]
df_total['헤드라인'] = df_total['헤드라인'].str.replace(r"\[.*?\]", "", regex=True).str.strip()
df_total = df_total.sort_values(by=["구분", "일자", "헤드라인"], ascending=[True, False, True])
df_total.to_csv("crawled_news.csv", index=False)
print("\n✅ 크롤링 완료 및 csv 저장!")
