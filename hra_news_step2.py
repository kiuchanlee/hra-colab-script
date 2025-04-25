import os
import pandas as pd
from datetime import datetime
import math
from typing import List
from openai import OpenAI
from google.oauth2.service_account import Credentials
import gspread
import re
from gspread_dataframe import set_with_dataframe

# ✅ 인증 설정
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("creds.json", scopes=SCOPES)
gc = gspread.authorize(creds)

# ✅ OpenAI 설정
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")  # GitHub Actions에서는 secrets로 설정
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))  

# ✅ CSV 불러오기
df_total = pd.read_csv("crawled_news.csv")
print(f"📄 CSV 로드 완료: {len(df_total)}건")


#✅ 9. 중복기사제거
# ✅ 언론사 우선순위 리스트 (원하는 대로 수정 가능)
MEDIA_PRIORITY = ["조선일보", "중앙일보", "동아일보", "서울신문", "경향신문", "한겨레", "한국경제", "머니투데이"]

def parse_gpt_group_output(content: str) -> List[List[int]]:
    """
    GPT 응답에서 중복 그룹 리스트 추출 (예: [[1,2], [3], [4,5]])
    """
    try:
        print("🧠 GPT 응답 원문:", content.strip())
        group_strings = re.findall(r'\[([0-9,\s]+)\]', content)
        groups = []
        for group_str in group_strings:
            numbers = [int(n.strip()) - 1 for n in group_str.split(',')]
            groups.append(numbers)
        return groups
    except Exception as e:
        print("⚠️ 그룹 파싱 실패:", e)
        return []

def get_gpt_duplicate_groups(headlines: List[str], step_desc: str = "") -> List[List[int]]:
    system_prompt = "너는 뉴스 헤드라인 중 중복된 내용을 그룹으로 묶어주는 AI야."
    user_prompt = "다음은 뉴스 헤드라인 리스트야. 같은 사건이나 내용은 하나의 그룹으로 묶고, 그룹마다 기사 번호 리스트로 알려줘.\n\n"

    for i, h in enumerate(headlines):
        user_prompt += f"{i+1}. {h}\n"
    user_prompt += "\n출력은 다음 형식으로만 해줘: [[1, 2], [3], [4, 5]]"

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2
        )
        content = response.choices[0].message.content
        groups = parse_gpt_group_output(content)
        print(f"✅ {step_desc} 그룹 수: {len(groups)} / 기사 {len(headlines)}건")
        return groups
    except Exception as e:
        print(f"❌ GPT 호출 실패: {e}")
        return [[i] for i in range(len(headlines))]  # fallback: 전부 독립 기사

def choose_by_media_priority(group_df: pd.DataFrame) -> pd.Series:
    for media in MEDIA_PRIORITY:
        match = group_df[group_df["매체명"] == media]
        if not match.empty:
            return match.iloc[0]
    return group_df.iloc[0]

def deduplicate_news_with_gpt(df: pd.DataFrame, batch_size: int = 20) -> pd.DataFrame:
    print("🚀 GPT 중복 제거 시작")
    df = df.copy().reset_index(drop=True)

    headline_batches = [df.iloc[i:i+batch_size] for i in range(0, len(df), batch_size)]
    selected_rows = []

    for i, batch_df in enumerate(headline_batches):
        print(f"\n📦 배치 {i+1}/{len(headline_batches)}")
        headlines = batch_df['헤드라인'].tolist()
        groups = get_gpt_duplicate_groups(headlines, step_desc=f"배치 {i+1}")

        for group in groups:
            group_df = batch_df.iloc[group]
            selected = choose_by_media_priority(group_df)
            selected_rows.append(selected)

    result_df = pd.DataFrame(selected_rows).reset_index(drop=True)
    print(f"\n🎉 최종 대표 기사 수: {len(result_df)} / 원본 {len(df)}")
    return result_df


def deduplicate_news_with_gpt_twopass(df: pd.DataFrame, batch_size_first: int = 20, batch_size_second: int = 50) -> pd.DataFrame:
    """
    GPT 기반 뉴스 중복 제거 (2단계 방식)
    1차: 배치 단위로 중복 제거
    2차: 1차 대표 기사들 간 cross-batch 중복 제거
    """
    print("\n🚀 [1차] 배치별 중복 제거 시작")
    first_pass_df = deduplicate_news_with_gpt(df, batch_size=batch_size_first)

    print("\n🔁 [2차] 대표 기사들 간 cross-batch 중복 제거 시작")
    second_pass_df = deduplicate_news_with_gpt(first_pass_df, batch_size=batch_size_second)

    print(f"\n🎉 최종 대표 기사 수: {len(second_pass_df)} / 원본 {len(df)}")
    return second_pass_df


df_total = deduplicate_news_with_gpt_twopass(df_total)



# ✅ 9. 3줄 요약 함수
def summarize_all_in_3_lines(df):
    df["주요내용"] = ""

    print(f"\n📌 [1단계] 전체 기사 3줄 요약 시작 (총 {len(df)}건)")

    for idx, row in df.iterrows():
        content = row["본문"]
        if not isinstance(content, str) or not content.strip():
            print(f"⚠️ 본문 없음 (idx={idx}) → 건너뜀")
            continue

        prompt =  f"""너는 뉴스 요약 전문 Assistant야.

                    다음 뉴스 본문을 3줄 이내로 요약해줘.

                    - 헤드라인처럼 간결하고 핵심적인 내용 위주로 정리해.
                    - 주어는 생략하거나 최소화하고, 중요한 정보부터 전달해.
                    - 각 문장은 반드시 '~다'로 끝나는 서술형이어야 해.
                    - '~요'나 '~합니다' 같은 어투는 사용하지 마.
                    - 너의 생각보다는 사실을 알려주는데 중점을 두고 작성해줘.
                    - 설명조보다는 보도 문장 스타일로 써줘.
                    - 불필요한 수식어나 배경 설명은 생략해도 좋아.

                    {content}"""

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "뉴스 3줄 요약기"},
                    {"role": "user", "content": prompt}
                ]
            )
            summary = response.choices[0].message.content.strip()
            df.at[idx, "주요내용"] = summary
            print(f"✅ 요약 완료 (idx={idx}, 헤드라인: {row['헤드라인'][:10]})")

        except Exception as e:
            print(f"⚠️ 요약 실패 (idx={idx}): {e}")
            continue

    print("\n📌 전체 기사 3줄 요약 완료")
    return df

# df_total = summarize_all_in_3_lines(df_total)

# ✅ 10. 중요 기사 선별 함수

def analyze_articles_batch(df, batch_size=5, max_retries=5):
    df["대기업 관련"] = ""
    df["HR 관련"] = ""
    df["정책/법안 관련"] = ""
    df["중요여부"] = ""
    df["경제/산업 관련"] = ""
    df["보험/금융 관련"] = ""
    df["중요도"] = 0

    print(f"\n📌 [2단계] 기사 분석 시작 (총 {len(df)}건, {batch_size}개씩 묶음)")
    total_success = 0
    total_failed = 0
    retry_indices = set()

    def run_batch(batch_df, index_map):
        prompt_lines = []
        for i, idx in enumerate(batch_df.index, 1):
            prompt_lines.append(f"{i}. {batch_df.at[idx, '헤드라인']}")
            index_map[i] = idx

        prompt = "\n".join(prompt_lines)

        system_msg = (
            "당신은 대기업 인사팀의 인사 담당자입니다.\n"
            "다음 뉴스 기사 제목들을 검토하고 각 제목에 대해 다음 다섯 가지 항목에 대해 각각 'O' 또는 'X'로 판단하세요.\n\n"
            "1. 이 기사가 삼성그룹 혹은 대기업과 관련이 있는가?\n"
            "2. 이 기사가 HR 부서(채용, 인사, 제도, 임금, 교육, 노무 등)에서 관심을 가질만한 내용인가?\n"
            "3. 정책/법안 등의 제도 변화와 관련된 내용인가?\n"
            "4. 경제/산업계와 관련 내용인가?\n"
            "5. 보험/금융업계 관련 내용인가?\n\n"
            "각 기사에 대해 아래 형식으로 출력하세요:\n"
            "1. 대기업 관련: O, HR 관심: O, 정책/법안/판례 관련: X, 경제/산업 관련: O, 보험/금융 관련: X\n"
            "...\n\n"
            f"기사 목록:\n{prompt}"
        )

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_msg}]
            )
            reply = response.choices[0].message.content.strip()
            lines = reply.split("\n")

            success = 0
            failed_ids = []

            for line in lines:
                if not line.strip():
                    continue

                parts = line.split('. ', 1)
                if len(parts) < 2:
                    continue

                try:
                    gpt_num = int(parts[0])
                except:
                    continue

                content = parts[1]
                fields = [f.strip() for f in content.split(',')]
                if len(fields) < 5:
                    failed_ids.append(index_map[gpt_num])
                    continue

                values = []
                for f in fields:
                    if ':' in f:
                        values.append(f.split(':')[1].strip())
                    else:
                        values.append('X')

                original_idx = index_map.get(gpt_num)
                if original_idx is not None and len(values) == 5:
                    df.at[original_idx, "대기업 관련"] = values[0]
                    df.at[original_idx, "HR 관련"] = values[1]
                    df.at[original_idx, "정책/법안 관련"] = values[2]
                    df.at[original_idx, "경제/산업 관련"] = values[3]
                    df.at[original_idx, "보험/금융 관련"] = values[4]

                    relevance_score = values.count("O")
                    df.at[original_idx, "중요도"] = relevance_score
                    if relevance_score >= 3:
                        df.at[original_idx, "중요여부"] = "V"

                    success += 1
                else:
                    failed_ids.append(original_idx)

            return success, failed_ids

        except Exception as e:
            print(f"⚠️ GPT 분석 실패 (예외): {e}")
            return 0, list(index_map.values())

    # 1차 분석
    total_batches = math.ceil(len(df) / batch_size)
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(df))
        batch_df = df.iloc[start_idx:end_idx]
        index_map = {}

        print(f"\n📦 Batch {batch_num+1}: 크롤링 중...")
        success, failed = run_batch(batch_df, index_map)
        total_success += success
        total_failed += len(batch_df) - success
        retry_indices.update(failed)

        print(f"✅ 성공: {success} / ❌ 실패: {len(failed)}")

    # 재시도
    retry_count = 0
    while retry_indices and retry_count < max_retries:
        retry_count += 1
        print(f"\n🔁 재시도 {retry_count}차 시작 (남은 실패 기사 수: {len(retry_indices)})")

        retry_list = list(retry_indices)
        retry_indices = set()

        for i in range(0, len(retry_list), batch_size):
            batch_ids = retry_list[i:i+batch_size]
            batch_df = df.loc[batch_ids]
            index_map = {}

            success, failed = run_batch(batch_df, index_map)
            total_success += success
            total_failed -= success
            retry_indices.update(failed)

            print(f"↩️ 재시도 성공: {success} / 실패: {len(failed)}")

    print(f"\n✅ 분석 완료: 총 {total_success}건 성공 / {len(df)}건 중")
    if total_failed > 0:
        print(f"❗ 최종 실패 기사: {total_failed}건 (재시도 {max_retries}회 후 실패)")

    return df


# ✅ 12. 전체 실행 순서
print("\n🧠 ChatGPT를 이용한 중요 기사 체크 중...")
df_total = analyze_articles_batch(df_total)

# ✅ 12-1) 필요한 컬럼만 추출 (순서 보장)
df_total = df_total[[
    "키워드", "일자", "헤드라인", "매체명", "본문", "주요내용",
    "대기업 관련", "HR 관련", "정책/법안 관련", "경제/산업 관련",
    "보험/금융 관련", "중요도", "중요여부"
]].copy()
# 12-2) '구분'에 대한 카테고리 순서 지정
# category_order = ["채용", "노사", "임금", "제도", "복지", "관계사", "현대해상", "DB손보", "KB손보", "메리츠"]
# df_total["키워드"] = pd.Categorical(df_total["키워드"], categories=category_order, ordered=True)

#12-3) 정렬: 구분(위 순서대로), 일자(내림차순), 헤드라인(내림차순)
df_total = df_total.sort_values(
    by=["중요도", "키워드", "일자", "헤드라인"],
    ascending=[False, True, False, False]
)

print("\n🎉 전체 작업 완료!")


# ✅ 13. Google 시트 업로드 (덮어쓰기)
sheet_id = "1l89Eca3CsjLEjG-9_raVMy6Y_sYE4BLA-XRtgwEhHEc"
sheet_name = "네이버API(첨부파일용)"


# 시트 불러오기
sheet = gc.open_by_key(sheet_id)
worksheet = sheet.worksheet(sheet_name)

# ✅ A3 이후만 지우기
worksheet.batch_clear(["A3:Z"])

# ✅ A3 셀부터 DataFrame 저장
set_with_dataframe(worksheet, df_total, row=3, col=1)
worksheet.update("A2", [[f"업데이트 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]])


print("✅ Google 스프레드시트 저장 완료!")
print("🔗 링크:", sheet.url)
