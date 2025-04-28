
# src/utils/google_sheet_utils.py

from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import gspread
from datetime import datetime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("creds.json", scopes=SCOPES)
gc = gspread.authorize(creds)

def upload_to_google_sheet(df, sheet_id, sheet_name):
    sheet = gc.open_by_key(sheet_id)
    worksheet = sheet.worksheet(sheet_name)
    worksheet.batch_clear(["A3:Z"])
    set_with_dataframe(worksheet, df, row=3, col=1)
    worksheet.update("A2", [[f"업데이트 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]])
    print("✅ Google 스프레드시트 저장 완료!")
