import redis
import asyncio
import logging
from config import *
from shared.async_utils import safe_request_async

logger = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

async def whale_alert_rss():
    url = 'https://feeds.whale-alert.io/transactions.rss'
    try:
        if r.exists('whale_alert'):
            return
        text = await safe_request_async('get', url)
        r.setex('whale_alert', WATCHER_INTERVAL, text)
    except Exception as e:
        logger.error('Whale alert fetch failed: %s', e)

async def wallet_labels():
    url = 'https://api.ethplorer.io/getTop?apiKey=freekey&criteria=cap&limit=50'
    try:
        if r.exists('wallet_labels'):
            return
        text = await safe_request_async('get', url)
        r.setex('wallet_labels', WATCHER_INTERVAL, text)
    except Exception as e:
        logger.error('Ethplorer fetch failed: %s', e)

async def main():
    while True:
        try:
            await asyncio.gather(
                whale_alert_rss(),
                wallet_labels(),
            )
            logger.info("Wallet watcher updated")
        except Exception:
            logger.exception("Watcher cycle failed")
        await asyncio.sleep(WATCHER_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Watcher crashed")
