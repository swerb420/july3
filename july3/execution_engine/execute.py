import ccxt
import sqlite3
import redis
from config import *

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

c.execute('''CREATE TABLE IF NOT EXISTS trades
(trade_id TEXT, cluster_id TEXT, pnl REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
c.execute('''CREATE TABLE IF NOT EXISTS cluster_limits
(cluster_id TEXT PRIMARY KEY, max_exposure REAL)''')

def check_loss_limit():
    loss_limit = float(r.get('daily_loss_limit') or 0)
    c.execute("SELECT SUM(pnl) FROM trades WHERE DATE(timestamp)=DATE('now')")
    pnl_today = c.fetchone()[0] or 0
    return pnl_today < -loss_limit

def execute_trade(trade_id, cluster_id, action, symbol, amount, swing=False):
    if r.get('DRY_RUN') == b'true':
        print("🚨 DRY_RUN active — skipping real trade.")
        return

    if check_loss_limit():
        print("🚨 Daily loss limit hit.")
        return

    c.execute("SELECT max_exposure FROM cluster_limits WHERE cluster_id=?", (cluster_id,))
    limit = c.fetchone()
    if limit and amount > float(limit[0]):
        print(f"🚨 Exceeds limit for {cluster_id}")
        return

    if not r.get(f'trade:{trade_id}:approved'):
        print(f"🚨 Not approved.")
        return

    kraken = ccxt.krakenfutures() if swing else ccxt.kraken()
    kraken.apiKey = KRAKEN_API_KEY
    kraken.secret = KRAKEN_API_SECRET

    if action == 'long':
        kraken.create_market_buy_order(symbol, amount)
    elif action == 'short':
        kraken.create_market_sell_order(symbol, amount)

    pnl = 1.23  # Simulate real PnL logging
    c.execute("INSERT INTO trades VALUES (?, ?, ?, datetime('now'))", (trade_id, cluster_id, pnl))
    conn.commit()
    print(f"✅ Executed {action} {symbol} swing={swing}")

