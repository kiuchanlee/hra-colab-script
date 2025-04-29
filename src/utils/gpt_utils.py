# src/utils/gpt_utils.py

import re
import os
import math
from typing import List
from openai import OpenAI
import pandas as pd

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MEDIA_PRIORITY = ["조선일보", "중앙일보", "동아일보", "서울신문", "경향신문", "한겨레", "한국경제", "머니투데이"]

def parse_gpt_group_output(content: str) -> List[List[int]]:
    group_strings = re.findall(r'\[([0-9,\s]+)\]', content)
    groups = []
    for group_str in group_strings:
        numbers = [int(n.strip()) - 1 for n in group_str.split(',')]
        groups.append(numbers)
    return groups

def get_gpt_duplicate_groups(headlines: List[str]) -> List[List[int]]:
    system_prompt = "너는 뉴스 헤드라인 중 중복된 내용을 그룹으로 묶어주는 AI야."
    user_prompt = "\n".join([f"{i+1}. {h}" for i, h in enumerate(headlines)])
    user_prompt += "\n출력 형식: [[1, 2], [3], [4, 5]]"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2
    )
    content = response.choices[0].message.content
    return parse_gpt_group_output(content)

def choose_by_media_priority(group_df: pd.DataFrame) -> pd.Series:
    for media in MEDIA_PRIORITY:
        match = group_df[group_df["매체명"] == media]
        if not match.empty:
            return match.iloc[0]
    return group_df.iloc[0]

def deduplicate_news_with_gpt(df: pd.DataFrame, batch_size: int = 20) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    headline_batches = [df.iloc[i:i+batch_size] for i in range(0, len(df), batch_size)]
    selected_rows = []
    for batch_df in headline_batches:
        headlines = batch_df['헤드라인'].tolist()
        groups = get_gpt_duplicate_groups(headlines)
        for group in groups:
            group_df = batch_df.iloc[group]
            selected = choose_by_media_priority(group_df)
            selected_rows.append(selected)
    return pd.DataFrame(selected_rows).reset_index(drop=True)

def deduplicate_news_with_gpt_twopass(df: pd.DataFrame, batch_size_first: int = 20, batch_size_second: int = 50) -> pd.DataFrame:
    first_pass_df = deduplicate_news_with_gpt(df, batch_size=batch_size_first)
    return deduplicate_news_with_gpt(first_pass_df, batch_size=batch_size_second)

def analyze_articles_batch(df: pd.DataFrame, batch_size=5, max_retries=5) -> pd.DataFrame:
    df["대기업 관련"] = ""
    df["HR 관련"] = ""
    df["정책/법안 관련"] = ""
    df["경제/산업 관련"] = ""
    df["보험/금융 관련"] = ""
    df["중요여부"] = ""
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
