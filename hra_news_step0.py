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
        ])

    # CSV로 저장
    output_file = "crawled_news.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n📄 결과 저장 완료: {output_file}")
