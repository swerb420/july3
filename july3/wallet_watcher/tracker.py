import redis
import sqlite3
import networkx as nx
import json

# Connect Redis & SQLite
r = redis.Redis(host='localhost', port=6379, db=0)
conn = sqlite3.connect('wallet_db.sqlite')
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
    try:
        # Placeholder: plug in Etherscan or Solscan
        pnl = 50000.0
        return pnl
    except Exception as e:
        print(f"⚠️ estimate_wallet_pnl error for {wallet_id}: {e}")
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
        print(f"⚠️ get_parent_depth error: {e}")
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
            c.execute("INSERT OR IGNORE INTO wallets VALUES (?, ?, datetime('now'), ?, ?, ?, ?, ?)",
                      (to_wallet, "cluster123", from_wallet, avg_pnl, hop_depth, '', trust_score))
            conn.commit()
            print(f"✅ Added wallet {to_wallet}: depth={hop_depth}, pnl={avg_pnl}, trust={trust_score:.2f}")
        except Exception as e:
            print(f"⚠️ DB insert error: {e}")

        G.add_edge(from_wallet, to_wallet)

    try:
        nx.write_gexf(G, 'wallet_graph.gexf')
        print("✅ Wallet graph saved as GEXF.")
    except Exception as e:
        print(f"⚠️ Graph write error: {e}")

if __name__ == "__main__":
    track_hops()
