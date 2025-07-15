#!/usr/bin/env python3
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def create_swap():
    subprocess.run(["sudo", "fallocate", "-l", "2G", "/swapfile"])
    subprocess.run(["sudo", "chmod", "600", "/swapfile"])
    subprocess.run(["sudo", "mkswap", "/swapfile"])
    subprocess.run(["sudo", "swapon", "/swapfile"])
    print("✅ Swap file created.")

def install_deps():
    subprocess.run(["sudo", "apt-get", "update"])
    subprocess.run(["sudo", "apt-get", "install", "-y", "docker.io", "docker-compose", "python3-pip", "redis-server"])
    requirements_path = str(BASE_DIR / "requirements.txt")
    subprocess.run(["pip3", "install", "-r", requirements_path])
    print("✅ Dependencies installed.")

def build_and_run():
    compose_file = str(BASE_DIR / "docker-compose.yml")
    subprocess.run(["docker-compose", "-f", compose_file, "up", "-d", "--build"])
    print("✅ Containers built and running.")

def setup_cron():
    cron_job = "@daily /usr/bin/python3 /opt/ai_trading_bot/shared/db_backup.py\n@weekly sqlite3 /opt/ai_trading_bot/wallet_db.sqlite \"VACUUM;\"\n"
    cron_file = "/tmp/ai_bot_cron"
    with open(cron_file, "w") as f:
        f.write(cron_job)
    subprocess.run(["crontab", cron_file])
    print("✅ Daily DB backup and weekly vacuum cron added.")

def main():
    create_swap()
    install_deps()
    build_and_run()
    setup_cron()

if __name__ == "__main__":
    main()
