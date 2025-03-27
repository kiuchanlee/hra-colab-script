import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

def test_google_sheet_write():
    # 인증 범위
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # 서비스 계정 인증
    creds = Credentials.from_service_account_file("creds.json", scopes=scopes)
    client = gspread.authorize(creds)

    # 🔗 여기에 테스트할 Google Sheet URL 또는 ID를 넣어주세요
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1oqBSysH1OUZ6A97JfMciO2A0jH2CRi_C2KWiEvHSekU"
    worksheet = client.open_by_url(SPREADSHEET_URL).worksheet("시트2")  # 예: 시트 탭 이름이 '시트1'인 경우


    # 샘플 데이터 작성
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sample_data = [now, "GitHub Actions 테스트", "✅ 성공!"]

    worksheet.append_row(sample_data)
    print("✅ Google Sheet에 샘플 데이터 저장 완료:", sample_data)

if __name__ == "__main__":
    test_google_sheet_write()
