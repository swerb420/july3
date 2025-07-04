import asyncio
import aiohttp
import json
import time
import sqlite3
import hashlib
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import numpy as np
import pandas as pd
from web3 import Web3
import requests
import re
from concurrent.futures import ThreadPoolExecutor
import threading
from enum import Enum


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TransactionType(Enum):
    SPOT_BUY = "spot_buy"
    SPOT_SELL = "spot_sell"
    PERP_OPEN = "perp_open"
    PERP_CLOSE = "perp_close"
    LIQUIDITY_ADD = "liquidity_add"
    LIQUIDITY_REMOVE = "liquidity_remove"
    BRIDGE = "bridge"
    STAKE = "stake"
    UNSTAKE = "unstake"
    LENDING = "lending"
    BORROWING = "borrowing"
    NFT_BUY = "nft_buy"
    NFT_SELL = "nft_sell"
    AIRDROP = "airdrop"
    ARBITRAGE = "arbitrage"
    MEV = "mev"
    UNKNOWN = "unknown"


@dataclass
class AdvancedTransaction:
    hash: str
    from_address: str
    to_address: str
    amount: float
    token: str
    timestamp: datetime
    chain: str
    tx_type: TransactionType
    gas_fee: float = 0.0
    block_number: int = 0
    exchange: str = ""
    price_usd: float = 0.0
    profit_loss: float = 0.0
    slippage: float = 0.0
    mev_detected: bool = False
    arbitrage_detected: bool = False
    tags: List[str] = field(default_factory=list)
    raw_data: Dict = field(default_factory=dict)


@dataclass
class PerpPosition:
    address: str
    exchange: str
    symbol: str
    side: str
    size: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    realized_pnl: float
    margin: float
    leverage: float
    liquidation_price: float
    timestamp: datetime
    is_open: bool = True


@dataclass
class LiquidityPosition:
    address: str
    protocol: str
    pair: str
    token0: str
    token1: str
    amount0: float
    amount1: float
    shares: float
    apr: float
    fees_earned: float
    impermanent_loss: float
    timestamp: datetime


@dataclass
class WalletProfile:
    address: str
    total_value_usd: float
    total_pnl: float
    win_rate: float
    total_trades: int
    avg_trade_size: float
    risk_score: float
    activity_score: float
    top_tokens: List[str]
    preferred_dexes: List[str]
    trading_pattern: str
    last_activity: datetime
    tags: List[str] = field(default_factory=list)


class AdvancedWalletTracker:
    def __init__(self, db_path: str = "advanced_wallet_tracker.db"):
        self.db_path = db_path
        self.session = None
        self.api_configs = self._setup_api_configs()
        self.contract_addresses = self._load_contract_addresses()
        self.price_cache = {}
        self.setup_advanced_database()
        self.executor = ThreadPoolExecutor(max_workers=10)

    def _setup_api_configs(self) -> Dict:
        return {
            'etherscan': {
                'primary': 'YOUR_ETHERSCAN_API_KEY_1',
                'secondary': 'YOUR_ETHERSCAN_API_KEY_2',
                'rate_limit': 5,
                'daily_limit': 100000
            },
            'bscscan': {
                'primary': 'YOUR_BSCSCAN_API_KEY_1',
                'secondary': 'YOUR_BSCSCAN_API_KEY_2',
                'rate_limit': 5,
                'daily_limit': 100000
            },
            'polygonscan': {
                'primary': 'YOUR_POLYGONSCAN_API_KEY_1',
                'secondary': 'YOUR_POLYGONSCAN_API_KEY_2',
                'rate_limit': 5,
                'daily_limit': 100000
            },
            'arbiscan': {
                'primary': 'YOUR_ARBISCAN_API_KEY_1',
                'secondary': 'YOUR_ARBISCAN_API_KEY_2',
                'rate_limit': 5,
                'daily_limit': 100000
            },
            'optimism': {
                'primary': 'YOUR_OPTIMISM_API_KEY_1',
                'secondary': 'YOUR_OPTIMISM_API_KEY_2',
                'rate_limit': 5,
                'daily_limit': 100000
            },
            'moralis': {
                'primary': 'YOUR_MORALIS_API_KEY_1',
                'secondary': 'YOUR_MORALIS_API_KEY_2',
                'rate_limit': 25,
                'monthly_limit': 40000
            },
            'alchemy': {
                'primary': 'YOUR_ALCHEMY_API_KEY_1',
                'secondary': 'YOUR_ALCHEMY_API_KEY_2',
                'rate_limit': 330,
                'monthly_limit': 300000000
            },
            'ankr': {
                'primary': 'YOUR_ANKR_API_KEY_1',
                'secondary': 'YOUR_ANKR_API_KEY_2',
                'rate_limit': 30,
                'monthly_limit': 500000000
            },
            'infura': {
                'primary': 'YOUR_INFURA_API_KEY_1',
                'secondary': 'YOUR_INFURA_API_KEY_2',
                'rate_limit': 10,
                'daily_limit': 100000
            },
            'coingecko': {
                'primary': 'YOUR_COINGECKO_API_KEY_1',
                'secondary': 'YOUR_COINGECKO_API_KEY_2',
                'rate_limit': 10,
                'monthly_limit': 30000
            },
            'coinmarketcap': {
                'primary': 'YOUR_CMC_API_KEY_1',
                'secondary': 'YOUR_CMC_API_KEY_2',
                'rate_limit': 30,
                'monthly_limit': 10000
            },
            'defillama': {
                'rate_limit': 10,
                'no_key_required': True
            },
            'dexscreener': {
                'rate_limit': 10,
                'no_key_required': True
            },
            '1inch': {
                'rate_limit': 10,
                'no_key_required': True
            },
            'paraswap': {
                'rate_limit': 10,
                'no_key_required': True
            },
            'thegraph': {
                'rate_limit': 10,
                'no_key_required': True
            }
        }

    def _load_contract_addresses(self) -> Dict:
        return {
            'uniswap_v2': '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
            'uniswap_v3': '0xE592427A0AEce92De3Edee1F18E0157C05861564',
            'sushiswap': '0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F',
            'pancakeswap': '0x10ED43C718714eb63d5aA57B78B54704E256024E',
            '1inch': '0x1111111254EEB25477B68fb85Ed929f73A960582',
            'paraswap': '0xDEF171Fe48CF0115B1d80b88dc8eAB59176FEe57',
            'gmx': '0x489ee077994B6658eAfA855C308275EAd8097C4A',
            'dydx': '0x65f7BA4Ec257AF7c55fd5854E5f6b345C2C6f3Fc',
            'perp_protocol': '0x82ac2CE43e33683c58BE4cDc40975E73aA50f459',
            'mux': '0x3e0199792Ce69DC29A0a36146bFa68bd7C8D6633',
            'aave': '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9',
            'compound': '0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B',
            'maker': '0x9759A6Ac90977b93B58547b4A71c78317f391A28',
            'lido': '0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84',
            'rocket_pool': '0xae78736Cd615f374D3085123A210448E74Fc6393',
            'multichain': '0x6b175474e89094c44da98b954eedeac495271d0f',
            'hop': '0x3666f603Cc164936C1b87e207F36BEBa4AC5f18a',
            'celer': '0x841ce48F9446C8E281D3F1444cB859b4A6D0738C',
            'synapse': '0x2796317b0fF8538F253012862c06787Adfb8cEb6',
            'binance_hot': ['0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE', '0xD551234Ae421e3BCBA99A0Da6d736074f22192FF'],
            'coinbase_hot': ['0x503828976D22510aad0201ac7EC88293211D23Da', '0xddfAbCdc4D8FfC6d5beaf154f18B778f892A0740'],
            'kraken_hot': ['0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0', '0x53d284357ec70cE289D6D64134DfAc8e511c8a3D'],
            'bybit_hot': ['0x5a52E96BAcdaBb82fd05763E25335261B270Efcb', '0x8b99F3660622e21f2910ECCA7fBE51d654a1517d'],
            'okx_hot': ['0x236F9F97e0E62388479bf9E5BA4889e46B0273C3', '0x2910543Af39abA0Cd09dBb2D50200b3E800A63D2'],
            'huobi_hot': ['0xAB5C66752a9e8167967685F1450532fB96138642', '0x5C985E89DDe482eFE97ea9f1950aD149Eb522e99'],
            'kucoin_hot': ['0xd89350284c7732163765b23338f2ff27449e0bf5', '0x88bd4d3e2997371bceefe8d9386c6b5b4de60346'],
            'gate_hot': ['0x7793cd85c11a924478d358d49b05b37e91b5810f', '0x0d0707963952f2fba59dd06f2b425ace40b492fe'],
            'bitfinex_hot': ['0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', '0x742d35Cc6634C0532925a3b8D6C8cf4b8b8a35C8'],
            'mexc_hot': ['0x0211F3ceDbEf3143223D3ACF0e589747933e8527', '0x3cc936b795A188F0e246cBB2D74C5Bd190aeCF18']
        }

    def setup_advanced_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS advanced_transactions (
                hash TEXT PRIMARY KEY,
                from_address TEXT,
                to_address TEXT,
                amount REAL,
                token TEXT,
                timestamp DATETIME,
                chain TEXT,
                tx_type TEXT,
                gas_fee REAL,
                block_number INTEGER,
                exchange TEXT,
                price_usd REAL,
                profit_loss REAL,
                slippage REAL,
                mev_detected BOOLEAN,
                arbitrage_detected BOOLEAN,
                tags TEXT,
                raw_data TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS perp_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT,
                exchange TEXT,
                symbol TEXT,
                side TEXT,
                size REAL,
                entry_price REAL,
                current_price REAL,
                unrealized_pnl REAL,
                realized_pnl REAL,
                margin REAL,
                leverage REAL,
                liquidation_price REAL,
                timestamp DATETIME,
                is_open BOOLEAN
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS liquidity_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT,
                protocol TEXT,
                pair TEXT,
                token0 TEXT,
                token1 TEXT,
                amount0 REAL,
                amount1 REAL,
                shares REAL,
                apr REAL,
                fees_earned REAL,
                impermanent_loss REAL,
                timestamp DATETIME
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wallet_profiles (
                address TEXT PRIMARY KEY,
                total_value_usd REAL,
                total_pnl REAL,
                win_rate REAL,
                total_trades INTEGER,
                avg_trade_size REAL,
                risk_score REAL,
                activity_score REAL,
                top_tokens TEXT,
                preferred_dexes TEXT,
                trading_pattern TEXT,
                last_activity DATETIME,
                tags TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_cache (
                token TEXT,
                price_usd REAL,
                timestamp DATETIME,
                PRIMARY KEY (token, timestamp)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_usage (
                api_name TEXT,
                endpoint TEXT,
                calls_made INTEGER,
                last_reset DATETIME,
                PRIMARY KEY (api_name, endpoint)
            )
        ''')
        conn.commit()
        conn.close()

    async def get_session(self):
        if self.session is None:
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, ttl_dns_cache=300, use_dns_cache=True)
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers={'User-Agent': 'Advanced-Wallet-Tracker/1.0'})
        return self.session

    async def get_comprehensive_transactions(self, address: str, chains: List[str] | None = None) -> List[AdvancedTransaction]:
        if chains is None:
            chains = ['ethereum', 'bsc', 'polygon', 'arbitrum', 'optimism', 'avalanche']
        all_transactions = []
        tasks = []
        for chain in chains:
            if chain == 'ethereum':
                tasks.append(self._get_ethereum_transactions(address))
            elif chain == 'bsc':
                tasks.append(self._get_bsc_transactions(address))
            elif chain == 'polygon':
                tasks.append(self._get_polygon_transactions(address))
            elif chain == 'arbitrum':
                tasks.append(self._get_arbitrum_transactions(address))
            elif chain == 'optimism':
                tasks.append(self._get_optimism_transactions(address))
            elif chain == 'avalanche':
                tasks.append(self._get_avalanche_transactions(address))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching transactions: {result}")
                continue
            all_transactions.extend(result)
        all_transactions.sort(key=lambda x: x.timestamp, reverse=True)
        analyzed_transactions = await self._analyze_transactions(all_transactions)
        return analyzed_transactions

    async def _get_ethereum_transactions(self, address: str) -> List[AdvancedTransaction]:
        transactions = []
        normal_txs = await self._fetch_etherscan_data(address, 'txlist')
        internal_txs = await self._fetch_etherscan_data(address, 'txlistinternal')
        erc20_txs = await self._fetch_etherscan_data(address, 'tokentx')
        erc721_txs = await self._fetch_etherscan_data(address, 'tokennfttx')
        for tx in normal_txs:
            transactions.append(await self._parse_ethereum_transaction(tx, 'normal'))
        for tx in internal_txs:
            transactions.append(await self._parse_ethereum_transaction(tx, 'internal'))
        for tx in erc20_txs:
            transactions.append(await self._parse_ethereum_transaction(tx, 'erc20'))
        for tx in erc721_txs:
            transactions.append(await self._parse_ethereum_transaction(tx, 'erc721'))
        return transactions

    async def _fetch_etherscan_data(self, address: str, action: str) -> List[Dict]:
        base_url = "https://api.etherscan.io/api"
        for api_key in [self.api_configs['etherscan']['primary'], self.api_configs['etherscan']['secondary']]:
            try:
                params = {
                    'module': 'account',
                    'action': action,
                    'address': address,
                    'startblock': 0,
                    'endblock': 99999999,
                    'sort': 'desc',
                    'apikey': api_key
                }
                session = await self.get_session()
                async with session.get(base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == '1':
                            return data.get('result', [])
            except Exception as e:
                logger.warning(f"Etherscan API error with key {api_key[:10]}...: {e}")
                continue
        return []

    async def _parse_ethereum_transaction(self, tx_data: Dict, tx_category: str) -> AdvancedTransaction:
        tx = AdvancedTransaction(
            hash=tx_data.get('hash', ''),
            from_address=tx_data.get('from', ''),
            to_address=tx_data.get('to', ''),
            amount=float(tx_data.get('value', 0)) / 1e18,
            token=tx_data.get('tokenSymbol', 'ETH'),
            timestamp=datetime.fromtimestamp(int(tx_data.get('timeStamp', 0))),
            chain='ethereum',
            tx_type=TransactionType.UNKNOWN,
            gas_fee=float(tx_data.get('gasUsed', 0)) * float(tx_data.get('gasPrice', 0)) / 1e18,
            block_number=int(tx_data.get('blockNumber', 0)),
            raw_data=tx_data
        )
        tx.tx_type = await self._categorize_transaction(tx)
        tx.mev_detected = await self._detect_mev(tx)
        tx.arbitrage_detected = await self._detect_arbitrage(tx)
        tx.price_usd = await self._get_token_price(tx.token, tx.timestamp)
        tx.exchange = self._detect_exchange(tx.to_address)
        return tx

    async def _categorize_transaction(self, tx: AdvancedTransaction) -> TransactionType:
        if tx.to_address.lower() in [addr.lower() for addr in self.contract_addresses.values() if isinstance(addr, str)]:
            if tx.raw_data.get('input', '').startswith('0x7ff36ab5'):
                return TransactionType.SPOT_BUY
            elif tx.raw_data.get('input', '').startswith('0x18cbafe5'):
                return TransactionType.SPOT_SELL
            else:
                return TransactionType.SPOT_BUY if tx.amount > 0 else TransactionType.SPOT_SELL
        cex_addresses = []
        for cex, addresses in self.contract_addresses.items():
            if cex.endswith('_hot') and isinstance(addresses, list):
                cex_addresses.extend(addresses)
        if tx.to_address.lower() in [addr.lower() for addr in cex_addresses]:
            return TransactionType.SPOT_BUY
        if tx.to_address.lower() == self.contract_addresses['gmx'].lower():
            return TransactionType.PERP_OPEN
        if tx.to_address.lower() in [self.contract_addresses['aave'].lower(), self.contract_addresses['compound'].lower()]:
            return TransactionType.LENDING
        if 'addLiquidity' in tx.raw_data.get('input', ''):
            return TransactionType.LIQUIDITY_ADD
        elif 'removeLiquidity' in tx.raw_data.get('input', ''):
            return TransactionType.LIQUIDITY_REMOVE
        return TransactionType.SPOT_BUY if tx.amount > 0 else TransactionType.SPOT_SELL

    async def _detect_mev(self, tx: AdvancedTransaction) -> bool:
        if tx.gas_fee > 0.1:
            return True
        if tx.arbitrage_detected:
            return True
        if 'flashloan' in tx.raw_data.get('input', '').lower():
            return True
        return False

    async def _detect_arbitrage(self, tx: AdvancedTransaction) -> bool:
        input_data = tx.raw_data.get('input', '')
        swap_functions = ['swapExactTokensForTokens', 'swapTokensForExactTokens', 'swapExactETHForTokens']
        swap_count = sum(1 for func in swap_functions if func in input_data)
        if swap_count > 1:
            return True
        return False

    async def _get_token_price(self, token: str, timestamp: datetime) -> float:
        cache_key = f"{token}_{timestamp.date()}"
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]
        price = await self._fetch_historical_price(token, timestamp)
        self.price_cache[cache_key] = price
        return price

    async def _fetch_historical_price(self, token: str, timestamp: datetime) -> float:
        try:
            price = await self._get_coingecko_historical_price(token, timestamp)
            if price > 0:
                return price
        except Exception as e:
            logger.warning(f"CoinGecko price fetch failed: {e}")
        try:
            price = await self._get_cmc_historical_price(token, timestamp)
            if price > 0:
                return price
        except Exception as e:
            logger.warning(f"CoinMarketCap price fetch failed: {e}")
        try:
            return await self._get_current_token_price(token)
        except Exception as e:
            logger.warning(f"Current price fetch failed: {e}")
            return 0.0

    def _detect_exchange(self, to_address: str) -> str:
        to_address = to_address.lower()
        for exchange, addresses in self.contract_addresses.items():
            if exchange.endswith('_hot') and isinstance(addresses, list):
                if to_address in [addr.lower() for addr in addresses]:
                    return exchange.replace('_hot', '')
        dex_mapping = {
            self.contract_addresses['uniswap_v2'].lower(): 'uniswap',
            self.contract_addresses['uniswap_v3'].lower(): 'uniswap',
            self.contract_addresses['sushiswap'].lower(): 'sushiswap',
            self.contract_addresses['pancakeswap'].lower(): 'pancakeswap',
            self.contract_addresses['1inch'].lower(): '1inch',
            self.contract_addresses['paraswap'].lower(): 'paraswap'
        }
        return dex_mapping.get(to_address, 'unknown')

    async def track_perpetual_positions(self, address: str) -> List[PerpPosition]:
        positions = []
        positions.extend(await self._get_gmx_positions(address))
        positions.extend(await self._get_dydx_positions(address))
        positions.extend(await self._get_perp_protocol_positions(address))
        positions.extend(await self._get_mux_positions(address))
        return positions

    async def _get_gmx_positions(self, address: str) -> List[PerpPosition]:
        query = """
        query getUserPositions($user: String!) {
            positions(where: {user: $user, isOpen: true}) {
                id
                user
                market
                collateralToken
                side
                size
                collateral
                entryPrice
                markPrice
                pnl
                leverage
                timestamp
            }
        }
        """
        variables = {"user": address.lower()}
        try:
            positions = []
            response = await self._query_thegraph('gmx', query, variables)
            for pos in response.get('data', {}).get('positions', []):
                position = PerpPosition(
                    address=address,
                    exchange='gmx',
                    symbol=pos.get('market', ''),
                    side=pos.get('side', ''),
                    size=float(pos.get('size', 0)),
                    entry_price=float(pos.get('entryPrice', 0)),
                    current_price=float(pos.get('markPrice', 0)),
                    unrealized_pnl=float(pos.get('pnl', 0)),
                    realized_pnl=0.0,
                    margin=float(pos.get('collateral', 0)),
                    leverage=float(pos.get('leverage', 0)),
                    liquidation_price=0.0,
                    timestamp=datetime.fromtimestamp(int(pos.get('timestamp', 0)))
                )
                positions.append(position)
            return positions
        except Exception as e:
            logger.error(f"Error fetching GMX positions: {e}")
            return []

    async def _get_dydx_positions(self, address: str) -> List[PerpPosition]:
        base_url = "https://indexer.dydx.trade/v4"
        try:
            session = await self.get_session()
            async with session.get(f"{base_url}/addresses/{address}/positions") as response:
                if response.status == 200:
                    data = await response.json()
                    positions = []
                    for pos in data.get('positions', []):
                        if pos.get('status') == 'OPEN':
                            position = PerpPosition(
                                address=address,
                                exchange='dydx',
                                symbol=pos.get('market', ''),
                                side=pos.get('side', ''),
                                size=float(pos.get('size', 0)),
                                entry_price=float(pos.get('entryPrice', 0)),
                                current_price=float(pos.get('oraclePrice', 0)),
                                unrealized_pnl=float(pos.get('unrealizedPnl', 0)),
                                realized_pnl=float(pos.get('realizedPnl', 0)),
                                margin=float(pos.get('margin', 0)),
                                leverage=float(pos.get('leverage', 0)),
                                liquidation_price=float(pos.get('liquidationPrice', 0)),
                                timestamp=datetime.fromisoformat(pos.get('createdAt', '').replace('Z', '+00:00'))
                            )
                            positions.append(position)
                    return positions
        except Exception as e:
            logger.error(f"Error fetching dYdX positions: {e}")
            return []

    async def _get_perp_protocol_positions(self, address: str) -> List[PerpPosition]:
        query = """
        query getPositions($trader: String!) {
            positions(where: {trader: $trader, isOpen: true}) {
                id
                trader
                baseToken
                side
                size
                collateral
                entryPrice
                markPrice
                unrealizedPnl
                realizedPnl
                margin
                leverage
                liquidationPrice
                timestamp
            }
        }
        """
        variables = {"trader": address.lower()}
        try:
            positions = []
            response = await self._query_thegraph('perp-protocol', query, variables)
            for pos in response.get('data', {}).get('positions', []):
                position = PerpPosition(
                    address=address,
                    exchange='perp_protocol',
                    symbol=pos.get('baseToken', ''),
                    side=pos.get('side', ''),
                    size=float(pos.get('size', 0)),
                    entry_price=float(pos.get('entryPrice', 0)),
                    current_price=float(pos.get('markPrice', 0)),
                    unrealized_pnl=float(pos.get('unrealizedPnl', 0)),
                    realized_pnl=float(pos.get('realizedPnl', 0)),
                    margin=float(pos.get('margin', 0)),
                    leverage=float(pos.get('leverage', 0)),
                    liquidation_price=float(pos.get('liquidationPrice', 0)),
                    timestamp=datetime.fromtimestamp(int(pos.get('timestamp', 0)))
                )
                positions.append(position)
            return positions
        except Exception as e:
            logger.error(f"Error fetching Perp Protocol positions: {e}")
            return []

    async def _get_mux_positions(self, address: str) -> List[PerpPosition]:
        base_url = "https://api.mux.network/v1"
        try:
            session = await self.get_session()
            async with session.get(f"{base_url}/positions/{address}") as response:
                if response.status == 200:
                    data = await response.json()
                    positions = []
                    for pos in data.get('positions', []):
                        if pos.get('isOpen', False):
                            position = PerpPosition(
                                address=address,
                                exchange='mux',
                                symbol=pos.get('symbol', ''),
                                side=pos.get('side', ''),
                                size=float(pos.get('size', 0)),
                                entry_price=float(pos.get('entryPrice', 0)),
                                current_price=float(pos.get('markPrice', 0)),
                                unrealized_pnl=float(pos.get('unrealizedPnl', 0)),
                                realized_pnl=float(pos.get('realizedPnl', 0)),
                                margin=float(pos.get('margin', 0)),
                                leverage=float(pos.get('leverage', 0)),
                                liquidation_price=float(pos.get('liquidationPrice', 0)),
                                timestamp=datetime.fromtimestamp(int(pos.get('timestamp', 0)))
                            )
                            positions.append(position)
                    return positions
        except Exception as e:
            logger.error(f"Error fetching MUX positions: {e}")
            return []

    async def track_liquidity_positions(self, address: str) -> List[LiquidityPosition]:
        positions = []
        positions.extend(await self._get_uniswap_v2_positions(address))
        positions.extend(await self._get_uniswap_v3_positions(address))
        positions.extend(await self._get_sushiswap_positions(address))
        positions.extend(await self._get_pancakeswap_positions(address))
        positions.extend(await self._get_curve_positions(address))
        return positions

    async def _get_uniswap_v2_positions(self, address: str) -> List[LiquidityPosition]:
        query = """
        query getLiquidityPositions($user: String!) {
            liquidityPositions(where: {user: $user, liquidityTokenBalance_gt: "0"}) {
                id
                user
                pair {
                    id
                    token0 {
                        symbol
                    }
                    token1 {
                        symbol
                    }
                    reserve0
                    reserve1
                    totalSupply
                }
                liquidityTokenBalance
                token0Deposited
                token1Deposited
                token0Withdrawn
                token1Withdrawn
                timestamp
            }
        }
        """
        variables = {"user": address.lower()}
        try:
            positions = []
            response = await self._query_thegraph('uniswap-v2', query, variables)
            for pos in response.get('data', {}).get('liquidityPositions', []):
                pair = pos.get('pair', {})
                position = LiquidityPosition(
                    address=address,
                    protocol='uniswap_v2',
                    pair=f"{pair.get('token0', {}).get('symbol', '')}/{pair.get('token1', {}).get('symbol', '')}",
                    token0=pair.get('token0', {}).get('symbol', ''),
                    token1=pair.get('token1', {}).get('symbol', ''),
                    amount0=float(pos.get('token0Deposited', 0)) - float(pos.get('token0Withdrawn', 0)),
                    amount1=float(pos.get('token1Deposited', 0)) - float(pos.get('token1Withdrawn', 0)),
                    shares=float(pos.get('liquidityTokenBalance', 0)),
                    apr=0.0,
                    fees_earned=0.0,
                    impermanent_loss=0.0,
                    timestamp=datetime.fromtimestamp(int(pos.get('timestamp', 0)))
                )
                positions.append(position)
            return positions
        except Exception as e:
            logger.error(f"Error fetching Uniswap V2 positions: {e}")
            return []

    async def _get_uniswap_v3_positions(self, address: str) -> List[LiquidityPosition]:
        query = """
        query getPositions($owner: String!) {
            positions(where: {owner: $owner, liquidity_gt: "0"}) {
                id
                owner
                pool {
                    token0 {
                        symbol
                    }
                    token1 {
                        symbol
                    }
                    feeTier
                }
                liquidity
                depositedToken0
                depositedToken1
                withdrawnToken0
                withdrawnToken1
                collectedFeesToken0
                collectedFeesToken1
                tickLower {
                    tickIdx
                }
                tickUpper {
                    tickIdx
                }
                timestamp
            }
        }
        """
        variables = {"owner": address.lower()}
        try:
            positions = []
            response = await self._query_thegraph('uniswap-v3', query, variables)
            for pos in response.get('data', {}).get('positions', []):
                pool = pos.get('pool', {})
                position = LiquidityPosition(
                    address=address,
                    protocol='uniswap_v3',
                    pair=f"{pool.get('token0', {}).get('symbol', '')}/{pool.get('token1', {}).get('symbol', '')}",
                    token0=pool.get('token0', {}).get('symbol', ''),
                    token1=pool.get('token1', {}).get('symbol', ''),
                    amount0=float(pos.get('depositedToken0', 0)) - float(pos.get('withdrawnToken0', 0)),
                    amount1=float(pos.get('depositedToken1', 0)) - float(pos.get('withdrawnToken1', 0)),
                    shares=float(pos.get('liquidity', 0)),
                    apr=0.0,
                    fees_earned=float(pos.get('collectedFeesToken0', 0)) + float(pos.get('collectedFeesToken1', 0)),
                    impermanent_loss=0.0,
                    timestamp=datetime.fromtimestamp(int(pos.get('timestamp', 0)))
                )
                positions.append(position)
            return positions
        except Exception as e:
            logger.error(f"Error fetching Uniswap V3 positions: {e}")
            return []

    async def analyze_wallet_profile(self, address: str) -> WalletProfile:
        transactions = await self.get_comprehensive_transactions(address)
        perp_positions = await self.track_perpetual_positions(address)
        liquidity_positions = await self.track_liquidity_positions(address)
        total_value_usd = await self._calculate_total_value(address, transactions, perp_positions, liquidity_positions)
        total_pnl = self._calculate_total_pnl(transactions, perp_positions)
        win_rate = self._calculate_win_rate(transactions)
        total_trades = len(transactions)
        avg_trade_size = sum(tx.amount * tx.price_usd for tx in transactions) / len(transactions) if transactions else 0
        risk_score = self._calculate_risk_score(transactions, perp_positions)
        activity_score = self._calculate_activity_score(transactions)
        top_tokens = self._get_top_tokens(transactions)
        preferred_dexes = self._get_preferred_dexes(transactions)
        trading_pattern = self._analyze_trading_pattern(transactions)
        profile = WalletProfile(
            address=address,
            total_value_usd=total_value_usd,
            total_pnl=total_pnl,
            win_rate=win_rate,
            total_trades=total_trades,
            avg_trade_size=avg_trade_size,
            risk_score=risk_score,
            activity_score=activity_score,
            top_tokens=top_tokens,
            preferred_dexes=preferred_dexes,
            trading_pattern=trading_pattern,
            last_activity=transactions[0].timestamp if transactions else datetime.now()
        )
        return profile

    def _calculate_total_pnl(self, transactions: List[AdvancedTransaction], perp_positions: List[PerpPosition]) -> float:
        realized_pnl = sum(tx.profit_loss for tx in transactions)
        unrealized_pnl = sum(pos.unrealized_pnl for pos in perp_positions)
        return realized_pnl + unrealized_pnl

    def _calculate_win_rate(self, transactions: List[AdvancedTransaction]) -> float:
        profitable_trades = sum(1 for tx in transactions if tx.profit_loss > 0)
        total_trades = len([tx for tx in transactions if tx.profit_loss != 0])
        return (profitable_trades / total_trades * 100) if total_trades > 0 else 0

    def _calculate_risk_score(self, transactions: List[AdvancedTransaction], perp_positions: List[PerpPosition]) -> float:
        risk_factors = []
        avg_leverage = sum(pos.leverage for pos in perp_positions) / len(perp_positions) if perp_positions else 1
        risk_factors.append(min(avg_leverage / 10, 1) * 30)
        mev_ratio = sum(1 for tx in transactions if tx.mev_detected) / len(transactions) if transactions else 0
        risk_factors.append(mev_ratio * 25)
        avg_position_size = sum(tx.amount * tx.price_usd for tx in transactions) / len(transactions) if transactions else 0
        if avg_position_size > 100000:
            risk_factors.append(25)
        daily_trades = len(transactions) / 30 if transactions else 0
        if daily_trades > 10:
            risk_factors.append(20)
        return min(sum(risk_factors), 100)

    def _calculate_activity_score(self, transactions: List[AdvancedTransaction]) -> float:
        if not transactions:
            return 0
        recent_txs = [tx for tx in transactions if tx.timestamp > datetime.now() - timedelta(days=7)]
        recent_activity = len(recent_txs) / 7 * 10
        unique_exchanges = len(set(tx.exchange for tx in transactions))
        diversity_score = min(unique_exchanges * 5, 30)
        total_volume = sum(tx.amount * tx.price_usd for tx in transactions)
        volume_score = min(total_volume / 10000, 40)
        return min(recent_activity + diversity_score + volume_score, 100)

    def _get_top_tokens(self, transactions: List[AdvancedTransaction]) -> List[str]:
        token_counts = defaultdict(int)
        for tx in transactions:
            token_counts[tx.token] += 1
        return sorted(token_counts.keys(), key=lambda x: token_counts[x], reverse=True)[:10]

    def _get_preferred_dexes(self, transactions: List[AdvancedTransaction]) -> List[str]:
        dex_counts = defaultdict(int)
        for tx in transactions:
            if tx.exchange and tx.exchange != 'unknown':
                dex_counts[tx.exchange] += 1
        return sorted(dex_counts.keys(), key=lambda x: dex_counts[x], reverse=True)[:5]

    def _analyze_trading_pattern(self, transactions: List[AdvancedTransaction]) -> str:
        if not transactions:
            return "inactive"
        if len(transactions) > 1:
            time_diffs = [(transactions[i].timestamp - transactions[i+1].timestamp).total_seconds() for i in range(len(transactions)-1)]
            avg_time_diff = sum(time_diffs) / len(time_diffs)
            if avg_time_diff < 3600:
                return "high_frequency"
            elif avg_time_diff < 86400:
                return "day_trader"
            elif avg_time_diff < 604800:
                return "active_trader"
            else:
                return "long_term_holder"
        return "single_trade"

    async def _query_thegraph(self, subgraph: str, query: str, variables: Dict | None = None) -> Dict:
        subgraph_urls = {
            'uniswap-v2': 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2',
            'uniswap-v3': 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3',
            'sushiswap': 'https://api.thegraph.com/subgraphs/name/sushiswap/exchange',
            'gmx': 'https://api.thegraph.com/subgraphs/name/gmx-io/gmx-stats',
            'perp-protocol': 'https://api.thegraph.com/subgraphs/name/perpetual-protocol/perp-v2'
        }
        if subgraph not in subgraph_urls:
            raise ValueError(f"Unknown subgraph: {subgraph}")
        session = await self.get_session()
        payload = {'query': query, 'variables': variables or {}}
        try:
            async with session.post(subgraph_urls[subgraph], json=payload) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"GraphQL query failed with status {response.status}")
                    return {}
        except Exception as e:
            logger.error(f"Error querying {subgraph}: {e}")
            return {}

    async def _get_coingecko_historical_price(self, token: str, timestamp: datetime) -> float:
        date_str = timestamp.strftime('%d-%m-%Y')
        token_mapping = {
            'ETH': 'ethereum',
            'BTC': 'bitcoin',
            'USDT': 'tether',
            'USDC': 'usd-coin',
            'BNB': 'binancecoin',
            'MATIC': 'matic-network',
            'AVAX': 'avalanche-2',
            'UNI': 'uniswap',
            'LINK': 'chainlink',
            'AAVE': 'aave'
        }
        coin_id = token_mapping.get(token.upper(), token.lower())
        session = await self.get_session()
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
            params = {'date': date_str}
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('market_data', {}).get('current_price', {}).get('usd', 0)
        except Exception as e:
            logger.error(f"Error fetching CoinGecko price for {token}: {e}")
        return 0.0

    async def _get_current_token_price(self, token: str) -> float:
        session = await self.get_session()
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {'ids': token.lower(), 'vs_currencies': 'usd'}
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return list(data.values())[0].get('usd', 0) if data else 0
        except Exception as e:
            logger.error(f"Error fetching current price for {token}: {e}")
        return 0.0

    async def track_wallet_ultra_comprehensive(self, address: str) -> Dict:
        logger.info(f"Starting ultra comprehensive tracking for {address}")
        tasks = [
            self.get_comprehensive_transactions(address),
            self.track_perpetual_positions(address),
            self.track_liquidity_positions(address),
            self.analyze_wallet_profile(address)
        ]
        try:
            transactions, perp_positions, liquidity_positions, profile = await asyncio.gather(*tasks)
            self.store_advanced_transactions(transactions)
            self.store_perp_positions(perp_positions)
            self.store_liquidity_positions(liquidity_positions)
            self.store_wallet_profile(profile)
            summary = {
                'address': address,
                'profile': profile,
                'transactions': {
                    'total': len(transactions),
                    'recent': len([tx for tx in transactions if tx.timestamp > datetime.now() - timedelta(days=7)]),
                    'by_type': self._categorize_transactions_summary(transactions)
                },
                'perpetual_positions': {
                    'total': len(perp_positions),
                    'open': len([pos for pos in perp_positions if pos.is_open]),
                    'total_pnl': sum(pos.unrealized_pnl + pos.realized_pnl for pos in perp_positions)
                },
                'liquidity_positions': {
                    'total': len(liquidity_positions),
                    'total_value': sum(pos.amount0 + pos.amount1 for pos in liquidity_positions),
                    'fees_earned': sum(pos.fees_earned for pos in liquidity_positions)
                },
                'insights': self._generate_insights(transactions, perp_positions, liquidity_positions, profile)
            }
            logger.info(f"Ultra comprehensive tracking completed for {address}")
            return summary
        except Exception as e:
            logger.error(f"Error in ultra comprehensive tracking: {e}")
            return {'error': str(e)}

    def _categorize_transactions_summary(self, transactions: List[AdvancedTransaction]) -> Dict:
        categories = defaultdict(int)
        for tx in transactions:
            categories[tx.tx_type.value] += 1
        return dict(categories)

    def _generate_insights(self, transactions: List[AdvancedTransaction], perp_positions: List[PerpPosition], liquidity_positions: List[LiquidityPosition], profile: WalletProfile) -> List[str]:
        insights = []
        if profile.activity_score > 80:
            insights.append("🔥 Highly active trader with frequent transactions")
        elif profile.activity_score < 20:
            insights.append("😴 Low activity - mostly holding positions")
        if profile.risk_score > 70:
            insights.append("⚠️ High risk profile - uses high leverage or MEV strategies")
        elif profile.risk_score < 30:
            insights.append("🛡️ Conservative trader with low risk exposure")
        if profile.win_rate > 70:
            insights.append("🎯 High win rate - skilled trader")
        elif profile.win_rate < 30:
            insights.append("📉 Low win rate - needs strategy improvement")
        if perp_positions:
            avg_leverage = sum(pos.leverage for pos in perp_positions) / len(perp_positions)
            if avg_leverage > 10:
                insights.append("⚡ High leverage perpetual trader")
            total_perp_pnl = sum(pos.unrealized_pnl + pos.realized_pnl for pos in perp_positions)
            if total_perp_pnl > 0:
                insights.append("💰 Profitable in perpetual trading")
        if liquidity_positions:
            insights.append("🏦 Provides liquidity to multiple protocols")
            total_fees = sum(pos.fees_earned for pos in liquidity_positions)
            if total_fees > 1000:
                insights.append("💸 Earning significant fees from liquidity provision")
        mev_transactions = [tx for tx in transactions if tx.mev_detected]
        if mev_transactions:
            insights.append("🤖 Involved in MEV activities")
        if profile.top_tokens:
            insights.append(f"🪙 Prefers trading: {', '.join(profile.top_tokens[:3])}")
        return insights

    def store_advanced_transactions(self, transactions: List[AdvancedTransaction]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for tx in transactions:
            cursor.execute('''
                INSERT OR REPLACE INTO advanced_transactions 
                (hash, from_address, to_address, amount, token, timestamp, chain, tx_type, 
                 gas_fee, block_number, exchange, price_usd, profit_loss, slippage, 
                 mev_detected, arbitrage_detected, tags, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx.hash, tx.from_address, tx.to_address, tx.amount, tx.token, tx.timestamp,
                tx.chain, tx.tx_type.value, tx.gas_fee, tx.block_number, tx.exchange,
                tx.price_usd, tx.profit_loss, tx.slippage, tx.mev_detected, tx.arbitrage_detected,
                ','.join(tx.tags), json.dumps(tx.raw_data)
            ))
        conn.commit()
        conn.close()

    def store_perp_positions(self, positions: List[PerpPosition]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for pos in positions:
            cursor.execute('''
                INSERT OR REPLACE INTO perp_positions 
                (address, exchange, symbol, side, size, entry_price, current_price, 
                 unrealized_pnl, realized_pnl, margin, leverage, liquidation_price, 
                 timestamp, is_open)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pos.address, pos.exchange, pos.symbol, pos.side, pos.size, pos.entry_price,
                pos.current_price, pos.unrealized_pnl, pos.realized_pnl, pos.margin,
                pos.leverage, pos.liquidation_price, pos.timestamp, pos.is_open
            ))
        conn.commit()
        conn.close()

    def store_liquidity_positions(self, positions: List[LiquidityPosition]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for pos in positions:
            cursor.execute('''
                INSERT OR REPLACE INTO liquidity_positions 
                (address, protocol, pair, token0, token1, amount0, amount1, shares, apr, fees_earned, impermanent_loss, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pos.address, pos.protocol, pos.pair, pos.token0, pos.token1, pos.amount0,
                pos.amount1, pos.shares, pos.apr, pos.fees_earned, pos.impermanent_loss, pos.timestamp
            ))
        conn.commit()
        conn.close()

    def store_wallet_profile(self, profile: WalletProfile):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO wallet_profiles 
            (address, total_value_usd, total_pnl, win_rate, total_trades, avg_trade_size, risk_score, activity_score, top_tokens, preferred_dexes, trading_pattern, last_activity, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            profile.address, profile.total_value_usd, profile.total_pnl, profile.win_rate,
            profile.total_trades, profile.avg_trade_size, profile.risk_score, profile.activity_score,
            ','.join(profile.top_tokens), ','.join(profile.preferred_dexes), profile.trading_pattern,
            profile.last_activity, ','.join(profile.tags)
        ))
        conn.commit()
        conn.close()
