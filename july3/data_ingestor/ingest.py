import ccxt
import redis
import requests
import json
import time
import snscrape.modules.twitter as sntwitter
from transformers import pipeline
from config import *

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
sentiment_pipeline = pipeline('sentiment-analysis')

def fetch_kraken(symbol='BTC/USD'):
    kraken = ccxt.kraken({'apiKey': KRAKEN_API_KEY, 'secret': KRAKEN_API_SECRET})
    ticker = kraken.fetch_ticker(symbol)
    r.set(f'kraken:{symbol}', json.dumps(ticker))

def fetch_news():
    res = requests.get(f"https://newsapi.org/v2/everything?q=crypto&apiKey={NEWSAPI_KEY}")
    r.set('newsapi', res.text)

def scrape_twitter():
    tweets = []
    for tweet in sntwitter.TwitterSearchScraper('crypto').get_items():
        tweets.append(tweet.content)
        if len(tweets) >= 10:
            break
    r.set('twitter', json.dumps(tweets))

def nlp_sentiment():
    tweets = json.loads(r.get('twitter') or '[]')
    scores = sentiment_pipeline(tweets)
    avg = sum([s['score'] for s in scores]) / len(scores) if scores else 0
    r.set('nlp_sentiment_score', avg)

def main():
    while True:
        fetch_kraken()
        fetch_news()
        scrape_twitter()
        nlp_sentiment()
        print("✅ Ingested")
        time.sleep(300)

if __name__ == "__main__":
    main()
