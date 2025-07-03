import shutil
import datetime
import os

DB_PATH = 'wallet_db.sqlite'
BACKUP_DIR = os.getenv('DB_BACKUP_PATH', './db_backups/')

def backup():
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f'wallet_db_{now}.sqlite')
    shutil.copyfile(DB_PATH, dst)
    print(f"✅ Backup created: {dst}")

if __name__ == "__main__":
    backup()
