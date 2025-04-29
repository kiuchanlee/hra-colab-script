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

    df = pd.DataFrame(all_results)
    df = df.drop_duplicates(subset=["URL"])
    df = df[df["URL"].str.startswith("https://n.news.naver.com/mnews")].reset_index(drop=True)

    df = pd.DataFrame({
        "구분": "",
        "키워드": df["검색어"],
        "일자": df["날짜"],
        "헤드라인": df["제목"],
        "본문": df["요약"],
        "매체명": df["언론사"],
        "URL": df["URL"]
    })

    return df


def main():
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        log_error("❌ NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 환경변수가 없습니다.")
        sys.exit(1)

    queries = [
        "조직개편", "인사제도", "직무중심", "직무급제", "성과급제","호봉제", "직급체계", "연공서열"
        ,
        "채용공고", "신입채용", "경력채용", "채용시장", "블라인드채용", "고용시장", "채용면접",
        "공채 폐지", "상시채용", "인재 확보", "수시채용", "컬처핏",
        "인사평가", "성과평가", "역량평가", "보상체계", "연봉제", "성과급", "인센티브",
        "임금인상", "기본급", "연차수당", "통상임금", "임금피크", "최저임금", "퇴직금",
        "포괄임금", "연금개혁", "스톡옵션", "RSU",
        "사내 교육", "리스킬링", "업스킬링", "HRD", "사내대학",
        "커리어패스", "경력개발", "직무순환", "승진제도", "후계자 양성",
        "52시간", "유연근로", "재택근무", "유연근무제", "주4일제", "주4.5일제", "육아휴직", "사내복지",
        "조직문화", "워라밸", "DEI", "사내 소통",
        "인사 정책", "고용 정책", "노동시장 개편", "인사 행정",
        "근로기준법", "노동법", "중대재해법", "직장 내 괴롭힘", "고용 안정성", "노사갈등", "구조조정", "희망퇴직",
        "HR Tech", "HRIS", "HR Analytics",
        "삼성 인사", "현대해상 인사", "DB손보 인사", "KB손보 인사", "메리츠 인사"
    ]

    df = search_naver_news_multi(
        queries=queries,
        client_id=client_id,
        client_secret=client_secret,
        display=300,
        filter_press_names=[
            "www.chosun.com", "joins.com", "donga.com", "khan.co.kr", "hani.co.kr",
            "hankyung.com", "mk.co.kr", "hankookilbo.com", "biz.chosun.com",
            "asiae.co.kr", "edaily.co.kr", "news.heraldcorp.com", "fnnews.com",
            "mt.co.kr", "magazine.mk.co.kr", "sisain.co.kr", "weekly.chosun.com"
        ]
    )

    output_dir = get_today_folder()
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, get_today_filename("step0_raw.csv"))

    df.to_csv(output_file, index=False, encoding="utf-8-sig", errors='ignore')
    log_info(f"📄 결과 저장 완료: {output_file}")


if __name__ == "__main__":
    main()
