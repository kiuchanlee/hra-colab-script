# src/utils/gpt_utils.py

import re
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

def analyze_articles_batch(df: pd.DataFrame, batch_size=5) -> pd.DataFrame:
    df = df.copy()
    df["대기업 관련"] = ""
    df["HR 관련"] = ""
    df["정책/법안 관련"] = ""
    df["경제/산업 관련"] = ""
    df["보험/금융 관련"] = ""
    df["중요여부"] = ""
    df["중요도"] = 0
    return df
