import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta

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
            f"&pd=4&ds={start_date}&de={end_date}&office_type=3&office_category=1"
            f"&sort=0&news_office_checked={media_codes}&start={start}"
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

# ✅ 날짜 설정 및 키워드 그룹 정의
keywordGroups = [...]  # 생략 가능, 두 번째 파일에서 import 또는 저장된 파일에서 불러오기

days_ago = 1
end_date = datetime.today().strftime("%Y.%m.%d")
start_date = (datetime.today() - timedelta(days=days_ago)).strftime("%Y.%m.%d")
print(f"\n📆 수집 기간: {start_date} ~ {end_date}\n")

# ✅ 전체 크롤링 실행
all_results = []
for group in keywordGroups:
    for keyword in group["keywords"]:
        df = crawl_news(keyword, group["category"], start_date, end_date, max_page=1)
        all_results.append(df)

# ✅ 저장
df_total = pd.concat(all_results, ignore_index=True)
df_total = df_total.drop_duplicates(subset=["URL"])
df_total.to_csv("crawled_news.csv", index=False)
print("\n✅ 크롤링 완료 및 csv 저장!")
