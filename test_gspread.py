import gspread
from google.oauth2.service_account import Credentials

creds = Credentials.from_service_account_file(
    "service_account.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

gc = gspread.authorize(creds)
sheet = gc.open_by_key("1j0kCwS8isclzcAVE-Wgl1Yp5VoHlf2VV8nHQsaavWWc")
print("✅ Connected to:", sheet.title)