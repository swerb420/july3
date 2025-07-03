import redis
import requests
import time
from config import *

r = redis.Redis(host='localhost', port=6379, db=0)

def whale_alert_rss():
    url = 'https://feeds.whale-alert.io/transactions.rss'
    res = requests.get(url)
    r.set('whale_alert', res.text)

def arkham_labels():
    url = 'https://arkham-intelligence.com/api/labels'
    res = requests.get(url)
    r.set('arkham_labels', res.text)

def main():
    while True:
        whale_alert_rss()
        arkham_labels()
        print("✅ Wallet watcher updated.")
        time.sleep(600)

if __name__ == "__main__":
    main()
