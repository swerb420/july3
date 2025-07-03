import redis
import sqlite3
import json
from openai import OpenAI
from config import *

r = redis.Redis(host='localhost', port=6379, db=0)
conn = sqlite3.connect('wallet_db.sqlite')

c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS signals
(signal_id TEXT PRIMARY KEY, cluster_id TEXT, signal_json TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')

def behavior_pattern_score(cluster_id):
    c.execute("SELECT trust_score FROM wallets WHERE cluster_id=?", (cluster_id,))
    row = c.fetchone()
    trust = row[0] if row else 0.0

    boost = 0.1 if trust > 0.7 else 0
    return {
        "trust_score": trust,
        "confidence_boost": boost
    }

def multi_wallet_check(token):
    return True  # Pseudo: real version checks cluster alignments in your wallet graph.

def final_llm_check(signals, signal_id, cluster_id):
    patterns = behavior_pattern_score(cluster_id)
    multi_wallets = multi_wallet_check(signals.get('token'))

    signals['patterns'] = patterns
    signals['multi_wallet_consensus'] = multi_wallets

    signals['swing_candidate'] = (
        signals['nlp_sentiment'] > 0.85 and multi_wallets
    )

    try:
        if LLM_PROVIDER == 'openai':
            client = OpenAI(api_key=OPENAI_API_KEY)
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "system", "content": "Quant risk engine."},
                          {"role": "user", "content": json.dumps(signals)}]
            )
            decision = response.choices[0].message
        else:
            decision = "Claude flow placeholder"

        c.execute("INSERT INTO signals VALUES (?, ?, ?, datetime('now'))",
                  (signal_id, cluster_id, json.dumps(signals)))
        conn.commit()
        return decision
    except:
        print("⚠️ LLM failed — fallback DRY_RUN.")
        return "DRY_RUN"

def main():
    # Example signal
    signals = {
        "token": "SOL",
        "nlp_sentiment": float(r.get('nlp_sentiment_score') or 0),
        "github_stars": int(r.get('github_stars_solana') or 0)
    }
    final_llm_check(signals, "sig001", "cluster123")

if __name__ == "__main__":
    main()
