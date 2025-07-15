# July3 Trading Bot

This repository contains a set of lightweight Python services that form an experimental cryptocurrency trading assistant.  Components ingest market data, monitor wallets, analyze signals with an LLM, execute trades and expose a Telegram bot for manual control.

The code lives in the `july3/` directory.  Each module can be run on its own or orchestrated via Docker Compose.

## Available scripts

- `data_ingestor/ingest.py` – gather market data from Kraken, NewsAPI, RSS feeds and Twitter then store results in Redis.
- `wallet_watcher/watcher.py` – periodically pull whale alerts and wallet labels.
- `wallet_watcher/tracker.py` – build a graph of wallet hops and estimate PnL using Ethplorer.
- `wallet_watcher/advanced_tracker.py` – advanced wallet tracking utilities (example code).
- `signal_engine/analyze.py` – combine ingested data and evaluate trading signals with an LLM.
- `execution_engine/execute.py` – place orders on Kraken when trades are approved.
- `memory_loader.py` – sync wallet labels and trust scores from a Google Sheet.
- `telegram_control/telegram_bot.py` – Telegram bot for approving trades and issuing commands.
- `shared/db_backup.py` – create SQLite database backups.
- `setup_all.py` – helper script that installs system dependencies and starts Docker Compose (optional).

## Environment setup

1. Copy the example environment file and edit values:

   ```bash
   cd july3
   cp .env.example .env
   ```

   At a minimum set the following variables in `.env`:

   - `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` for the control bot.
   - Depending on `LLM_PROVIDER`, either `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`.
   - Any API keys you plan to use (e.g. `KRAKEN_API_KEY`, `NEWSAPI_KEY`, etc.).

2. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Start Redis and the services with Docker Compose:

   ```bash
   docker-compose up -d
   ```

   This will start `redis` along with the data ingestor, wallet watcher, signal engine, execution engine and Telegram bot containers.

4. To run a component manually without Docker, execute its script directly, e.g.:

   ```bash
   python data_ingestor/ingest.py
   ```

## Project purpose

The project demonstrates a modular architecture for an AI-assisted trading bot.  Market data and wallet activity are ingested to Redis, evaluated by a signal engine using an LLM, and trade execution is gated by Telegram commands.  The code is a prototype and should be reviewed and extended before using with real funds.

