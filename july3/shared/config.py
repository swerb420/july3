import os
import sys
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Config:
    """Configuration class with validation and defaults"""
    
    # Trading API Keys
    KRAKEN_API_KEY: Optional[str] = os.getenv('KRAKEN_API_KEY')
    KRAKEN_API_SECRET: Optional[str] = os.getenv('KRAKEN_API_SECRET')
    
    # Data Source API Keys
    NEWSAPI_KEY: Optional[str] = os.getenv('NEWSAPI_KEY')
    COINMARKETCAL_API_KEY: Optional[str] = os.getenv('COINMARKETCAL_API_KEY')
    ETHERSCAN_API_KEY: Optional[str] = os.getenv('ETHERSCAN_API_KEY')
    ARKHAM_API_KEY: Optional[str] = os.getenv('ARKHAM_API_KEY')
    
    # Google Services
    GOOGLE_SHEETS_ID: Optional[str] = os.getenv('GOOGLE_SHEETS_ID')
    GOOGLE_DOC_ID: Optional[str] = os.getenv('GOOGLE_DOC_ID')
    GOOGLE_SERVICE_ACCOUNT_JSON: Optional[str] = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    # LLM Configuration
    LLM_PROVIDER: str = os.getenv('LLM_PROVIDER', 'openai')
    OPENAI_API_KEY: Optional[str] = os.getenv('OPENAI_API_KEY')
    ANTHROPIC_API_KEY: Optional[str] = os.getenv('ANTHROPIC_API_KEY')
    
    # Telegram Bot
    TELEGRAM_BOT_TOKEN: Optional[str] = os.getenv('TELEGRAM_BOT_TOKEN')
    TELEGRAM_CHAT_ID: Optional[str] = os.getenv('TELEGRAM_CHAT_ID')
    
    # Database Configuration
    DB_PATH: str = os.getenv('DB_PATH', 'wallet_db.sqlite')
    BACKUP_DIR: str = os.getenv('DB_BACKUP_PATH', './db_backups/')
    
    # Redis Configuration
    REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB: int = int(os.getenv('REDIS_DB', '0'))
    
    # Application Settings
    DRY_RUN: bool = os.getenv('DRY_RUN', 'false').lower() == 'true'
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Trading Constants
    DEFAULT_SENTIMENT_THRESHOLD: float = float(os.getenv('SENTIMENT_THRESHOLD', '0.85'))
    DEFAULT_TRUST_THRESHOLD: float = float(os.getenv('TRUST_THRESHOLD', '0.7'))
    MAX_DAILY_LOSS_PERCENT: float = float(os.getenv('MAX_DAILY_LOSS_PERCENT', '5.0'))
    
    # API Rate Limits
    INGEST_INTERVAL: int = int(os.getenv('INGEST_INTERVAL', '300'))  # 5 minutes
    WATCHER_INTERVAL: int = int(os.getenv('WATCHER_INTERVAL', '600'))  # 10 minutes

    # Additional RSS feeds for news ingestion (comma separated URLs)
    RSS_FEEDS = os.getenv(
        'RSS_FEEDS',
        'https://cointelegraph.com/rss,https://www.coindesk.com/arc/outboundfeeds/rss/'
    ).split(',')
    
    @classmethod
    def validate_required_config(cls) -> None:
        """Validate that all required configuration is present"""
        required_configs = {
            'TELEGRAM_BOT_TOKEN': cls.TELEGRAM_BOT_TOKEN,
            'TELEGRAM_CHAT_ID': cls.TELEGRAM_CHAT_ID,
        }
        
        # Check LLM configuration
        if cls.LLM_PROVIDER == 'openai' and not cls.OPENAI_API_KEY:
            required_configs['OPENAI_API_KEY'] = None
        elif cls.LLM_PROVIDER == 'anthropic' and not cls.ANTHROPIC_API_KEY:
            required_configs['ANTHROPIC_API_KEY'] = None
        
        missing_configs = [key for key, value in required_configs.items() if not value]
        
        if missing_configs:
            logger.error(f"Missing required environment variables: {missing_configs}")
            sys.exit(1)
    
    @classmethod
    def validate_optional_config(cls) -> None:
        """Validate optional configuration and warn about missing values"""
        optional_configs = {
            'KRAKEN_API_KEY': cls.KRAKEN_API_KEY,
            'KRAKEN_API_SECRET': cls.KRAKEN_API_SECRET,
            'NEWSAPI_KEY': cls.NEWSAPI_KEY,
            'GOOGLE_SERVICE_ACCOUNT_JSON': cls.GOOGLE_SERVICE_ACCOUNT_JSON,
        }
        
        missing_optional = [key for key, value in optional_configs.items() if not value]
        
        if missing_optional:
            logger.warning(f"Missing optional environment variables (some features may be disabled): {missing_optional}")
    
    @classmethod
    def setup_logging(cls) -> None:
        """Setup logging configuration"""
        log_level = getattr(logging, cls.LOG_LEVEL.upper(), logging.INFO)
        logging.getLogger().setLevel(log_level)

# Initialize configuration
config = Config()

# Validate configuration on import
config.validate_required_config()
config.validate_optional_config()
config.setup_logging()

# Legacy compatibility - expose config values at module level
KRAKEN_API_KEY = config.KRAKEN_API_KEY
KRAKEN_API_SECRET = config.KRAKEN_API_SECRET
NEWSAPI_KEY = config.NEWSAPI_KEY
COINMARKETCAL_API_KEY = config.COINMARKETCAL_API_KEY
ETHERSCAN_API_KEY = config.ETHERSCAN_API_KEY
ARKHAM_API_KEY = config.ARKHAM_API_KEY
GOOGLE_SHEETS_ID = config.GOOGLE_SHEETS_ID
GOOGLE_DOC_ID = config.GOOGLE_DOC_ID
GOOGLE_SERVICE_ACCOUNT_JSON = config.GOOGLE_SERVICE_ACCOUNT_JSON
LLM_PROVIDER = config.LLM_PROVIDER
OPENAI_API_KEY = config.OPENAI_API_KEY
ANTHROPIC_API_KEY = config.ANTHROPIC_API_KEY
TELEGRAM_BOT_TOKEN = config.TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID = config.TELEGRAM_CHAT_ID
