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

    print(full_message)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(full_message + "\n")
