import redis
import time
import logging
from config import *
from shared.utils import safe_request

logger = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def whale_alert_rss():
    url = 'https://feeds.whale-alert.io/transactions.rss'
    try:
        res = safe_request('get', url)
        r.set('whale_alert', res.text)
    except Exception as e:
        logger.error('Whale alert fetch failed: %s', e)

def arkham_labels():
    url = 'https://arkham-intelligence.com/api/labels'
    try:
        res = safe_request('get', url)
        r.set('arkham_labels', res.text)
    except Exception as e:
        logger.error('Arkham labels fetch failed: %s', e)

def main():
    while True:
        try:
            whale_alert_rss()
            arkham_labels()
            logger.info("Wallet watcher updated")
        except Exception:
            logger.exception("Watcher cycle failed")
        time.sleep(600)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Watcher crashed")
