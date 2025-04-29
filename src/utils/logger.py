# src/utils/logger.py

import os
from datetime import datetime

def log_info(message):
    _log(message, level="INFO")

def log_error(message):
    _log(message, level="ERROR")

def _log(message, level="INFO"):
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{today}.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{level}] {timestamp} - {message}"

    # 출력 안전 처리
    try:
        print(full_message)
    except UnicodeEncodeError:
        print(full_message.encode('utf-8', 'ignore').decode('utf-8'))

    # 파일 기록도 안전 처리
    safe_message = full_message.encode('utf-8', 'ignore').decode('utf-8')
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(safe_message + "\n")
