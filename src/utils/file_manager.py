import os
from datetime import datetime

def get_today_folder():
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join("data", today)

def get_today_filename(name):
    return name
