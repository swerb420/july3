import shutil
import datetime
import os
from config import DB_PATH, BACKUP_DIR

def backup():
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f'wallet_db_{now}.sqlite')
    shutil.copyfile(DB_PATH, dst)
    print(f"✅ Backup created: {dst}")

if __name__ == "__main__":
    backup()
