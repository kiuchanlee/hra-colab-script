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

                # 어느 URL에서 출처 보고
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
                    "uc694약": description,
                    "ub0a0짜": pubDate.strftime("%Y-%m-%d"),
                    "uc5b4른사": press_name
                })

        log_info(f"✅ '{query}' 완료 - {len(query_results)}개 수집")
        all_results.extend(query_results)

    df = pd.DataFrame(all_results)

    # 중복 URL 제거 + 네이버 뉴스 분문지 검색
    df = df.drop_duplicates(subset=["URL"])
    df = df[df["URL"].str.startswith("https://n.news.naver.com/mnews")].reset_index(drop=True)

    df = pd.DataFrame({
        "구매": "",
        "키워드": df["검색어"],
        "uc77c자": df["날짜"],
        "헤딩라인": df["제목"],
        "ubd80문": df["요약"],
        "ub9e4체명": df["어른사"],
        "URL": df["URL"]
    })

    return df


def main():
    queries = os.getenv("QUERIES", "").split(',')
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        log_error("❌ NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 환경 변수 부족")
        sys.exit(1)

    # queries는 사이버를 가지고 가는 것이 좋음 (json 파일에서 모은다가 간단 로딩)
    if not queries or queries == ['']:
        log_error("❌ 검색어 가 비워있습니다.")
        sys.exit(1)

    df = search_naver_news_multi(
        queries=queries,
        client_id=client_id,
        client_secret=client_secret,
        display=300,
        filter_press_names=[
            "chosun.com", "joins.com", "donga.com", "khan.co.kr", "hani.co.kr",
            "hankyung.com", "mk.co.kr", "hankookilbo.com", "biz.chosun.com",
            "asiae.co.kr", "edaily.co.kr", "news.heraldcorp.com", "fnnews.com",
            "mt.co.kr", "magazine.mk.co.kr", "hankyung.com", "donga.com",
            "sisain.co.kr", "weekly.chosun.com"
        ]
    )

    output_dir = get_today_folder()
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, get_today_filename("step0_raw.csv"))

    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log_info(f"📄 결과 저장 완료: {output_file}")


if __name__ == "__main__":
    main()
