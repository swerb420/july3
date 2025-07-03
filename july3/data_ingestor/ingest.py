import ccxt
import redis
import json
import asyncio
import logging
import snscrape.modules.twitter as sntwitter
from transformers import pipeline
from config import *
from shared.async_utils import safe_request_async
import feedparser

logger = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
sentiment_pipeline = pipeline('sentiment-analysis')

async def fetch_kraken(symbol: str = 'BTC/USD') -> None:
    """Fetch ticker data from Kraken with basic error handling."""
    try:
        kraken = ccxt.kraken({'apiKey': KRAKEN_API_KEY, 'secret': KRAKEN_API_SECRET})
        ticker = await asyncio.to_thread(kraken.fetch_ticker, symbol)
        r.set(f'kraken:{symbol}', json.dumps(ticker))
    except Exception as e:
        logger.error("Kraken fetch failed: %s", e)

async def fetch_news() -> None:
    """Fetch cryptocurrency news with retries."""
    try:
        text = await safe_request_async(
            'get',
            "https://newsapi.org/v2/everything",
            params={"q": "crypto", "apiKey": NEWSAPI_KEY},
        )
        r.set('newsapi', text)
    except Exception as e:
        logger.error("News API fetch failed: %s", e)

async def fetch_rss_feeds() -> None:
    """Fetch additional RSS feeds and store a short summary."""
    feeds: dict[str, list[dict[str, str]]] = {}
    for url in RSS_FEEDS:
        try:
            text = await safe_request_async('get', url)
            parsed = feedparser.parse(text)
            feeds[url] = [
                {
                    'title': entry.get('title', ''),
                    'link': entry.get('link', '')
                }
                for entry in parsed.entries[:5]
            ]
        except Exception as e:
            logger.error("RSS fetch failed for %s: %s", url, e)
    if feeds:
        r.set('rss_feeds', json.dumps(feeds))

async def scrape_twitter() -> None:
    """Scrape a few tweets containing the keyword 'crypto'."""
    tweets = []
    try:
        for tweet in await asyncio.to_thread(lambda: list(sntwitter.TwitterSearchScraper('crypto').get_items())):
            tweets.append(tweet.content)
            if len(tweets) >= 10:
                break
    except Exception as e:
        logger.error("Twitter scrape failed: %s", e)
    r.set('twitter', json.dumps(tweets))

async def nlp_sentiment() -> None:
    """Run sentiment analysis on scraped tweets."""
    try:
        tweets = json.loads(r.get('twitter') or '[]')
        scores = await asyncio.to_thread(sentiment_pipeline, tweets)
        avg = sum([s['score'] for s in scores]) / len(scores) if scores else 0
        r.set('nlp_sentiment_score', avg)
    except Exception as e:
        logger.error("Sentiment analysis failed: %s", e)

async def main() -> None:
    """Continuously ingest data with delay."""
    while True:
        try:
            await asyncio.gather(
                fetch_kraken(),
                fetch_news(),
                fetch_rss_feeds(),
                scrape_twitter(),
            )
            await nlp_sentiment()
            logger.info("Ingested successfully")
        except Exception:
            logger.exception("Ingestion cycle failed")
        await asyncio.sleep(INGEST_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
