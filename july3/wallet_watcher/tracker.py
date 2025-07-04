import redis
import sqlite3
import networkx as nx
import json
import requests
import logging
from config import *
from shared.utils import retry

logger = logging.getLogger(__name__)

# Connect Redis & SQLite
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Create table if needed
c.execute('''CREATE TABLE IF NOT EXISTS wallets
(wallet_id TEXT PRIMARY KEY,
 cluster_id TEXT,
 first_seen DATETIME,
 parent_wallet TEXT,
 avg_pnl REAL DEFAULT 0.0,
 hop_depth INTEGER DEFAULT 0,
 behavior_label TEXT DEFAULT '',
 trust_score REAL DEFAULT 0.0)''')

def estimate_wallet_pnl(wallet_id):
    """Approximate wallet PnL using the free Ethplorer API."""
    try:
        url = f"https://api.ethplorer.io/getAddressInfo/{wallet_id}?apiKey=freekey"
        data = requests.get(url, timeout=10).json()
        eth = data.get("ETH", {})
        total_in = float(eth.get("totalInUSD") or eth.get("totalIn", 0))
        total_out = float(eth.get("totalOutUSD") or eth.get("totalOut", 0))
        return total_in - total_out
    except Exception as e:
        logger.error("estimate_wallet_pnl error for %s: %s", wallet_id, e)
        return 0.0

def get_parent_depth(wallet_id):
    try:
        c.execute("SELECT hop_depth FROM wallets WHERE wallet_id=?", (wallet_id,))
        row = c.fetchone()
        if row and row[0] is not None:
            return row[0]
        else:
            return 0
    except Exception as e:
        logger.error("get_parent_depth error: %s", e)
        return 0

def track_hops():
    G = nx.DiGraph()
    # Replace with your parsed Whale Alert or Etherscan data:
    tx_data = [
        {'from': 'walletA', 'to': 'walletB'},
        {'from': 'walletB', 'to': 'walletC'}
    ]

    for tx in tx_data:
        from_wallet = tx['from']
        to_wallet = tx['to']

        hop_depth = 1 + get_parent_depth(from_wallet)
        avg_pnl = estimate_wallet_pnl(to_wallet)
        trust_score = max(0.0, min(avg_pnl / 10000, 1.0))

        try:
            c.execute(
                "INSERT OR IGNORE INTO wallets VALUES (?, ?, datetime('now'), ?, ?, ?, ?, ?)",
                (to_wallet, "cluster123", from_wallet, avg_pnl, hop_depth, '', trust_score),
            )
            conn.commit()
            logger.info(
                "Added wallet %s: depth=%s, pnl=%s, trust=%.2f",
                to_wallet, hop_depth, avg_pnl, trust_score,
            )
        except Exception as e:
            logger.error("DB insert error: %s", e)

        G.add_edge(from_wallet, to_wallet)

    try:
        nx.write_gexf(G, 'wallet_graph.gexf')
        logger.info("Wallet graph saved as GEXF")
    except Exception as e:
        logger.error("Graph write error: %s", e)

if __name__ == "__main__":
    try:
        track_hops()
    except Exception:
        logger.exception("Tracker crashed")
