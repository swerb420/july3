import shutil
import datetime
import os
import logging
from config import DB_PATH, BACKUP_DIR

logger = logging.getLogger(__name__)

def backup():
    try:
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = os.path.join(BACKUP_DIR, f'wallet_db_{now}.sqlite')
        shutil.copyfile(DB_PATH, dst)
        logger.info("Backup created: %s", dst)
    except Exception as e:
        logger.error("Backup failed: %s", e)

if __name__ == "__main__":
    try:
        backup()
    except Exception:
        logger.exception("Backup script crashed")
