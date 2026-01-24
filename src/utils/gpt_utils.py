import re
import os
import math
from typing import List
from openai import OpenAI
import pandas as pd
from utils.logger import log_info, log_error

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MEDIA_PRIORITY = ["조선일보", "중앙일보", "동아일보", "서울신문", "경향신문", "한겨레", "한국경제", "머니투데이"]

def parse_gpt_group_output(content: str) -> List[List[int]]:
    group_strings = re.findall(r'\[([0-9,\s]+)\]', content)
    groups = []
    for group_str in group_strings:
        try:
            numbers = [int(n.strip()) - 1 for n in group_str.split(',')]
            groups.append(numbers)
        except:
            continue
    return groups

def get_gpt_duplicate_groups(headlines: List[str]) -> List[List[int]]:
    system_prompt = "너는 뉴스 헤드라인 중 중복된 내용을 그룹으로 묶어주는 AI야."
    user_prompt = "\n".join([f"{i+1}. {h}" for i, h in enumerate(headlines)])
    user_prompt += "\n출력 형식: [[1, 2], [3], [4, 5]]"

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
        return parse_gpt_group_output(content)
    except Exception as e:
        log_error(f"⚠️ 중복 제거 GPT 호출 실패: {e}")
        return [[i] for i in range(len(headlines))]

def choose_by_media_priority(group_df: pd.DataFrame) -> pd.Series:
    for media in MEDIA_PRIORITY:
        match = group_df[group_df["매체명"] == media]
        if not match.empty:
            return match.iloc[0]
    return group_df.iloc[0]

def deduplicate_news_with_gpt(df: pd.DataFrame, batch_size: int = 20) -> pd.DataFrame:
    if df.empty: return df
    df = df.copy().reset_index(drop=True)
    headline_batches = [df.iloc[i:i+batch_size] for i in range(0, len(df), batch_size)]
    selected_rows = []
    for batch_df in headline_batches:
        headlines = batch_df['헤드라인'].tolist()
        groups = get_gpt_duplicate_groups(headlines)
        for group in groups:
            try:
                # 안전한 인덱스 접근
                valid_group = [g for g in group if 0 <= g < len(batch_df)]
                if not valid_group: continue
                group_df = batch_df.iloc[valid_group]
                selected = choose_by_media_priority(group_df)
                selected_rows.append(selected)
            except:
                continue
    return pd.DataFrame(selected_rows).reset_index(drop=True)

def deduplicate_news_with_gpt_twopass(df: pd.DataFrame, batch_size_first: int = 20, batch_size_second: int = 50) -> pd.DataFrame:
    first_pass_df = deduplicate_news_with_gpt(df, batch_size=batch_size_first)
    return deduplicate_news_with_gpt(first_pass_df, batch_size=batch_size_second)

def analyze_articles_batch(df: pd.DataFrame, batch_size=5, max_retries=5) -> pd.DataFrame:
    # 컬럼 초기화
    for col in ["대기업 관련", "HR 관련", "정책/법안 관련", "경제/산업 관련", "보험/금융 관련", "중요여부"]:
        df[col] = "X"
    df["중요도"] = 0

    log_info(f"📌 [2단계] 기사 분석 시작 (총 {len(df)}건, {batch_size}개씩 묶음)")
    total_success = 0
    retry_indices = set()

    def run_batch(batch_df, index_map):
        prompt_lines = []
        for i, idx in enumerate(batch_df.index, 1):
            summary = str(batch_df.at[idx, '요약'])[:200] # 토큰 절약 및 에러 방지
            prompt_lines.append(f"{i}. {summary}")
            index_map[i] = idx

        prompt = "\n".join(prompt_lines)
        system_msg = (
            "당신은 대기업 인사팀의 인사 담당자입니다.\n"
            "각 기사에 대해 1.대기업 관련, 2.HR 관심, 3.정책/법안, 4.경제/산업, 5.보험/금융 여부를 O/X로 판단하세요.\n"
            "형식: 1. 대기업 관련: O, HR 관심: O, 정책/법안/판례 관련: X, 경제/산업 관련: O, 보험/금융 관련: X"
        )

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": prompt}]
            )
            reply = response.choices[0].message.content.strip()
            lines = reply.split("\n")
            
            success_count = 0
            failed_ids = []

            for line in lines:
                if '. ' not in line: continue
                try:
                    parts = line.split('. ', 1)
                    gpt_num = int(re.search(r'\d+', parts[0]).group())
                    content = parts[1]
                    fields = [f.strip() for f in content.split(',')]
                    
                    original_idx = index_map.get(gpt_num)
                    if original_idx is not None and len(fields) >= 5:
                        vals = [f.split(':')[-1].strip() if ':' in f else 'X' for f in fields[:5]]
                        df.at[original_idx, "대기업 관련"] = vals[0]
                        df.at[original_idx, "HR 관련"] = vals[1]
                        df.at[original_idx, "정책/법안 관련"] = vals[2]
                        df.at[original_idx, "경제/산업 관련"] = vals[3]
                        df.at[original_idx, "보험/금융 관련"] = vals[4]
                        
                        score = vals.count("O")
                        df.at[original_idx, "중요도"] = score
                        df.at[original_idx, "중요여부"] = "V" if score >= 3 else ""
                        success_count += 1
                except:
                    continue
            
            # 응답에 포함되지 않은 ID들을 실패로 간주
            processed_gpt_nums = [int(re.search(r'\d+', l.split('. ')[0]).group()) for l in lines if '. ' in l and re.search(r'\d+', l.split('. ')[0])]
            for i in index_map:
                if i not in processed_gpt_nums:
                    failed_ids.append(index_map[i])

            return success_count, failed_ids

        except Exception as e:
            log_error(f"⚠️ GPT 배치 분석 실패: {e}")
            return 0, list(index_map.values())

    # 1차 분석 루프
    total_batches = math.ceil(len(df) / batch_size)
    for batch_num in range(total_batches):
        start = batch_num * batch_size
        batch_df = df.iloc[start : start + batch_size]
        index_map = {}
        success, failed = run_batch(batch_df, index_map)
        total_success += success
        retry_indices.update([f for f in failed if f is not None])
        log_info(f"📦 Batch {batch_num+1}/{total_batches}: 성공 {success}")

    # 재시도 루프 (에러 방어 강화)
    retry_count = 0
    while retry_indices and retry_count < max_retries:
        retry_count += 1
        log_info(f"🔁 재시도 {retry_count}차 (남은 기사: {len(retry_indices)})")
        
        # 🔥 KeyError 방어: None 제거 및 실제 df.index에 존재하는 것만 추출
        current_retry_list = [idx for idx in retry_indices if idx is not None and idx in df.index]
        retry_indices = set()

        for i in range(0, len(current_retry_list), batch_size):
            batch_ids = current_retry_list[i : i + batch_size]
            # 한 번 더 체크 (안전장치)
            batch_ids = [b for b in batch_ids if b in df.index]
            if not batch_ids: continue

            batch_df = df.loc[batch_ids]
            index_map = {}
            success, failed = run_batch(batch_df, index_map)
            total_success += success
            retry_indices.update([f for f in failed if f is not None])

    log_info(f"✅ 분석 완료: 총 {total_success}/{len(df)}건 성공")
    return df
