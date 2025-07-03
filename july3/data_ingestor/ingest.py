import ccxt
import redis
import json
import time
import logging
import snscrape.modules.twitter as sntwitter
from transformers import pipeline
from config import *
from shared.utils import safe_request

logger = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
sentiment_pipeline = pipeline('sentiment-analysis')

def fetch_kraken(symbol: str = 'BTC/USD') -> None:
    """Fetch ticker data from Kraken with basic error handling."""
    try:
        kraken = ccxt.kraken({'apiKey': KRAKEN_API_KEY, 'secret': KRAKEN_API_SECRET})
        ticker = kraken.fetch_ticker(symbol)
        r.set(f'kraken:{symbol}', json.dumps(ticker))
    except Exception as e:
        logger.error("Kraken fetch failed: %s", e)

def fetch_news() -> None:
    """Fetch cryptocurrency news with retries."""
    try:
        res = safe_request(
            'get',
            "https://newsapi.org/v2/everything",
            params={"q": "crypto", "apiKey": NEWSAPI_KEY},
        )
        r.set('newsapi', res.text)
    except Exception as e:
        logger.error("News API fetch failed: %s", e)

def scrape_twitter() -> None:
    """Scrape a few tweets containing the keyword 'crypto'."""
    tweets = []
    try:
        for tweet in sntwitter.TwitterSearchScraper('crypto').get_items():
            tweets.append(tweet.content)
            if len(tweets) >= 10:
                break
    except Exception as e:
        logger.error("Twitter scrape failed: %s", e)
    r.set('twitter', json.dumps(tweets))

def nlp_sentiment() -> None:
    """Run sentiment analysis on scraped tweets."""
    try:
        tweets = json.loads(r.get('twitter') or '[]')
        scores = sentiment_pipeline(tweets)
        avg = sum([s['score'] for s in scores]) / len(scores) if scores else 0
        r.set('nlp_sentiment_score', avg)
    except Exception as e:
        logger.error("Sentiment analysis failed: %s", e)

def main() -> None:
    """Continuously ingest data with delay."""
    while True:
        try:
            fetch_kraken()
            fetch_news()
            scrape_twitter()
            nlp_sentiment()
            logger.info("Ingested successfully")
        except Exception:
            logger.exception("Ingestion cycle failed")
        time.sleep(300)

if __name__ == "__main__":
    main()
