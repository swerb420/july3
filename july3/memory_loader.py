from google.oauth2 import service_account
from googleapiclient.discovery import build
import sqlite3
from config import *

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES)

def sync_sheet():
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEETS_ID, range="Sheet1").execute()
    rows = result.get('values', [])

    for row in rows[1:]:
        wallet_id = row[0]
        label = row[1] if len(row) > 1 else ''
        trust = float(row[2]) if len(row) > 2 and row[2] else 0.0

        c.execute("UPDATE wallets SET behavior_label=?, trust_score=? WHERE wallet_id=?",
                  (label, trust, wallet_id))
        conn.commit()
    print("✅ Memory loaded.")

if __name__ == "__main__":
    sync_sheet()
