from dotenv import dotenv_values, find_dotenv
import ast
from ratelimiter import RateLimiter

rate_limiter = RateLimiter(max_calls=5, period=1)

dot_env_path = find_dotenv(raise_error_if_not_found=True)
config = dotenv_values(dot_env_path)

WEB3_ALCHEMY_PROJECT_ID = config["WEB3_ALCHEMY_PROJECT_ID"]
WEB3_OPT_PROJECT_ID = config["WEB3_OPT_PROJECT_ID"]
WEB3_ARB_PROJECT_ID = config["WEB3_ARB_PROJECT_ID"]
WEB3_POLY_PROJECT_ID = config["WEB3_POLY_PROJECT_ID"]

# read the list of wallets from .env
# wallets are in a string dictionary
WALLETS = ast.literal_eval(config["WALLETS"])
for item in WALLETS.items():
    WALLETS[item[0]] = item[1].lower()
WALLET_LIST = list(WALLETS.values())
wallet_address_to_name = {wallet: name for name, wallet in WALLETS.items()}

# get block explorer api keys
ETHERSCAN_TOKEN = config["ETHERSCAN_TOKEN"]
ARBISCAN_TOKEN = config["ARBISCAN_TOKEN"]
SNOWTRACE_TOKEN = config["SNOWTRACE_TOKEN"]
POLYGONSCAN_TOKEN = config["POLYGONSCAN_TOKEN"]
OPTIMISTIC_TOKEN = config["OPTIMISTIC_TOKEN"]
AURORASCAN_TOKEN = config["AURORASCAN_TOKEN"]
FTMSCAN_TOKEN = config["FTMSCAN_TOKEN"]
GNOSISSCAN_TOKEN = config["GNOSISSCAN_TOKEN"]
BSCSCAN_TOKEN = config["BSCSCAN_TOKEN"]

# info for acccessing data on each chain
CHAINS = {
    "mainnet": {
        "chain_id": 1,
        "explorer_url": "https://api.etherscan.io/api",
        "explorer_token": ETHERSCAN_TOKEN,
        "v2_pool": "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9",
        "pools": ["0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9"],
        "base_token_name": "Ether",
        "base_token_symbol": "ETH",
        "base_token_decimals": 18,
    },
    "polygon": {
        "chain_id": 137,
        "explorer_url": "https://api.polygonscan.com/api",
        "explorer_token": POLYGONSCAN_TOKEN,
        "v3_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "v2_pool": "0x8dFf5E27EA6b7AC08EbFdf9eB090F32ee9a30fcf",
        "pools": [
            "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            "0x8dFf5E27EA6b7AC08EbFdf9eB090F32ee9a30fcf",
        ],
        "amUSDT": "0x60D55F02A771d515e077c9C2403a1ef324885CeC",
        "base_token_name": "Polygon",
        "base_token_symbol": "MATIC",
        "base_token_decimals": 18,
    },
    "avalanche": {
        "chain_id": 43114,
        "explorer_token": SNOWTRACE_TOKEN,
        "explorer_url": "https://api.snowtrace.io/api",
        "v3_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "v2_pool": "0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C",
        "pools": [
            "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            "0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C",
        ],
        "base_token_name": "Avalanche",
        "base_token_symbol": "AVAX",
        "base_token_decimals": 18,
    },
    "arbitrum": {
        "chain_id": 42161,
        "explorer_url": "https://api.arbiscan.io/api",
        "explorer_token": ARBISCAN_TOKEN,
        "v3_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pools": ["0x794a61358D6845594F94dc1DB02A252b5b4814aD"],
        "base_token_name": "Ether",
        "base_token_symbol": "ETH",
        "base_token_decimals": 18,
    },
    "optimism": {
        "chain_id": 10,
        "explorer_url": "https://api-optimistic.etherscan.io/api",
        "explorer_token": OPTIMISTIC_TOKEN,
        "v3_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pools": ["0x794a61358D6845594F94dc1DB02A252b5b4814aD"],
        "base_token_name": "Optimism",
        "base_token_symbol": "OP",
        "base_token_decimals": 18,
    },
    "fantom": {
        "chain_id": 250,
        "explorer_url": "https://api.ftmscan.com/api",
        "explorer_token": FTMSCAN_TOKEN,
        "v3_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pools": ["0x794a61358D6845594F94dc1DB02A252b5b4814aD"],
        "base_token_name": "Fantom",
        "base_token_symbol": "FTM",
        "base_token_decimals": 18,
    },
    # harmony has a different block explorer so it needs a custom treatment
    "harmony": {
        "chain_id": 1666600000,
        "explorer_url": None,
        "explorer_token": None,
        "v3_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "pools": ["0x794a61358D6845594F94dc1DB02A252b5b4814aD"],
        "base_token_name": "Harmony One",
        "base_token_symbol": "ONE",
        "base_token_decimals": 18,
    },
    "binance": {
        "chain_id": 56,
        "explorer_url": "https://api.bscscan.com/api",
        "explorer_token": BSCSCAN_TOKEN,
        "pools": [],
        "base_token_name": "Binance Coin",
        "base_token_symbol": "BNB",
        "base_token_decimals": 18,
    },
    "gnosis": {
        "chain_id": 100,
        "explorer_url": "https://api.gnosisscan.io/api",
        "explorer_token": GNOSISSCAN_TOKEN,
        "base_token_name": "Gnosis",
        "base_token_symbol": "xDai",
        "base_token_decimals": 18,
        "pools": [],
    },
    "aurora": {
        "chain_id": 1313161554,
        "explorer_url": "https://api.aurorascan.dev/api",
        "explorer_token": AURORASCAN_TOKEN,
        "base_token_name": "Ether",
        "base_token_symbol": "ETH",
        "base_token_decimals": 18,
        "pools": [],
    },
}
chain_id_to_name = {
    1: "mainnet",
    137: "polygon",
    43114: "avalanche",
    42161: "arbitrum",
    10: "optimism",
    250: "fantom",
    1666600000: "harmony",
    56: "binance",
    100: "gnosis",
    1313161554: "aurora",
}
CHAIN_LIST = list(CHAINS.keys())
CHAIN_LIST.remove("harmony")


POOL_LIST = []
for chain in CHAIN_LIST:
    try:
        pool = CHAINS[chain]["v2_pool"]
        POOL_LIST.append(pool)
    except KeyError:
        pass
    try:
        pool = CHAINS[chain]["v3_pool"]
        POOL_LIST.append(pool)
    except KeyError:
        pass
POOL_LIST = [pool.lower() for pool in POOL_LIST]

TEST_WALLET = WALLET_LIST[11]
TEST_POOL = CHAINS["polygon"]["v2_pool"]
TEST_CHAIN = "polygon"
