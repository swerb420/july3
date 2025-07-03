import os
import redis
import sqlite3
import logging
from telegram import Update, Bot
from telegram.ext import Updater, CommandHandler, CallbackContext
from config import *

logger = logging.getLogger(__name__)

# Connect to Redis & SQLite
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
c = conn.cursor()

# Setup Bot (token and chat ID provided by config)
bot = Bot(TELEGRAM_BOT_TOKEN)

# === Handlers ===

# ✅ 1) Approve trade
def approve(update: Update, context: CallbackContext):
    if len(context.args) < 1:
        update.message.reply_text("Usage: /approve TRADE_ID")
        return
    trade_id = context.args[0]
    r.set(f'trade:{trade_id}:approved', 'true')
    update.message.reply_text(f"✅ Trade {trade_id} approved.")

# ✅ 2) Set exposure limit for cluster
def set_limit(update: Update, context: CallbackContext):
    if len(context.args) < 2:
        update.message.reply_text("Usage: /set_limit CLUSTER_ID LIMIT")
        return
    cluster_id, limit = context.args[0], context.args[1]
    try:
        c.execute("INSERT OR REPLACE INTO cluster_limits VALUES (?, ?)", (cluster_id, limit))
        conn.commit()
        update.message.reply_text(f"✅ Limit for {cluster_id} set to {limit}.")
    except Exception as e:
        update.message.reply_text(f"⚠️ DB error: {e}")

# ✅ 3) Set daily loss limit
def set_loss_limit(update: Update, context: CallbackContext):
    if len(context.args) < 1:
        update.message.reply_text("Usage: /set_loss_limit PERCENT")
        return
    limit = context.args[0]
    r.set('daily_loss_limit', limit)
    update.message.reply_text(f"✅ Daily loss limit set to {limit}%.")

# ✅ 4) Show recent logs
def logs(update: Update, context: CallbackContext):
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS trades
                     (trade_id TEXT, cluster_id TEXT, pnl REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        rows = c.execute('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 5').fetchall()
        if not rows:
            update.message.reply_text("No trades logged yet.")
        for row in rows:
            update.message.reply_text(f"Trade: {row}")
    except Exception as e:
        update.message.reply_text(f"⚠️ Logs error: {e}")

# ✅ 5) Panic kill switch
def panic(update: Update, context: CallbackContext):
    # Pseudo: here you'd cancel Kraken open orders too
    update.message.reply_text("🚨 PANIC! All open orders canceled. DRY_RUN mode enabled.")
    r.set('DRY_RUN', 'true')

# ✅ 6) Label wallet manually
def label_wallet(update: Update, context: CallbackContext):
    if len(context.args) < 2:
        update.message.reply_text("Usage: /label_wallet WALLET_ID LABEL")
        return
    wallet_id, label = context.args[0], context.args[1]
    try:
        c.execute("UPDATE wallets SET behavior_label=? WHERE wallet_id=?", (label, wallet_id))
        conn.commit()
        update.message.reply_text(f"✅ Wallet {wallet_id} labeled: {label}")
    except Exception as e:
        update.message.reply_text(f"⚠️ DB error: {e}")

# ✅ 7) Report cluster wallets
def wallet_report(update: Update, context: CallbackContext):
    if len(context.args) < 1:
        update.message.reply_text("Usage: /wallet_report CLUSTER_ID")
        return
    cluster_id = context.args[0]
    try:
        c.execute("SELECT * FROM wallets WHERE cluster_id=?", (cluster_id,))
        rows = c.fetchall()
        if not rows:
            update.message.reply_text(f"No wallets found for cluster {cluster_id}.")
        for row in rows:
            update.message.reply_text(f"Wallet: {row}")
    except Exception as e:
        update.message.reply_text(f"⚠️ Report error: {e}")

# === Register handlers ===

def main():
    updater = Updater(TELEGRAM_BOT_TOKEN)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("approve", approve))
    dp.add_handler(CommandHandler("set_limit", set_limit))
    dp.add_handler(CommandHandler("set_loss_limit", set_loss_limit))
    dp.add_handler(CommandHandler("logs", logs))
    dp.add_handler(CommandHandler("panic", panic))
    dp.add_handler(CommandHandler("label_wallet", label_wallet))
    dp.add_handler(CommandHandler("wallet_report", wallet_report))

    update_startup = (
        "✅ Bot online!\n"
        "/approve TRADE_ID\n"
        "/set_limit CLUSTER_ID LIMIT\n"
        "/set_loss_limit PERCENT\n"
        "/logs\n"
        "/panic\n"
        "/label_wallet WALLET_ID LABEL\n"
        "/wallet_report CLUSTER_ID"
    )
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=update_startup)
    logger.info("Telegram bot started")

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Telegram bot crashed")
