import requests
import pandas as pd

try:
    from scripts.utils import *
except ModuleNotFoundError:
    from ..utils import *


# get all "normal" transactions for a particular address on a given chain
def get_normal_transactions_by_address(
    address, chain="mainnet", start_block=0, end_block=99999999
):
    address = address.lower()
    base_url = CHAINS[chain]["explorer_url"]
    api_key = CHAINS[chain]["explorer_token"]

    url = (
        "{}?module=account&action=txlist&address={}&startblock={}&endblock={}&apikey={}"
    )
    url = url.format(base_url, address, start_block, end_block, api_key)

    response = requests.get(url).json()
    if response['result']:
        return pd.DataFrame(response["result"])
    return pd.DataFrame(columns=['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash',
       'transactionIndex', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError',
       'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed',
       'gasUsed', 'confirmations', 'methodId', 'functionName'])


# get normal transactions across multiple chains
def get_normal_transactions_by_address_on_all_chains(
    address, chains, start_block=0, end_block=99999999
):
    normal_transactions = pd.DataFrame()

    for chain in chains:
        normal_transactions = pd.concat(
            [
                normal_transactions,
                get_normal_transactions_by_address(
                    address, chain, start_block, end_block
                ),
            ]
        )

    return normal_transactions


# get normal transactions for all wallets and chains
def get_normal_transactions_for_all_wallets_on_all_chains(
    addresses, chains, start_block=0, end_block=9999999
):
    normal_transactions = pd.DataFrame()

    for address in addresses:
        address = address.lower()
        for chain in chains:
            normal_transactions = pd.concat(
                [
                    normal_transactions,
                    get_normal_transactions_by_address(
                        address, chain, start_block, end_block
                    ),
                ]
            )

    return normal_transactions


# filter normal transactions for interactions with a particular address
def filter_normal_transactions_by_address_to_address(normal_transactions, to_address):
    normal_transactions_to_address = normal_transactions[
        normal_transactions["to"] == to_address.lower()
    ]

    return normal_transactions_to_address


# get all erc20 sends for a given wallet
def get_token_txs_by_wallet(
    address, token=None, chain="mainnet", start_block=0, end_block=99999999
):
    base_url = CHAINS[chain]["explorer_url"]
    api_key = CHAINS[chain]["explorer_token"]

    if token:
        url = "{}?module=account&action=tokentx&contractaddress={}&address={}&startblock={}&endblock={}&apikey={}"
        url = url.format(base_url, token, address, start_block, end_block, api_key)
    else:
        url = "{}?module=account&action=tokentx&address={}&startblock={}&endblock={}&apikey={}"
        url = url.format(base_url, address, start_block, end_block, api_key)

    response = requests.get(url).json()
    if response['result']:
        return pd.DataFrame(response["result"])
    # ensure no errors if nothing is found
    return pd.DataFame(columns=['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from',
       'contractAddress', 'to', 'value', 'tokenName', 'tokenSymbol',
       'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed',
       'cumulativeGasUsed', 'input', 'confirmations'])



# get all erc20 tokens interacted with
def get_all_erc20_tokens_interacted_with(
    address, chain="mainnet", start_block=0, end_block=99999999
):
    token_txs = get_token_txs_by_wallet(
        address, token=None, chain=chain, start_block=start_block, end_block=end_block
    )
    tokens = token_txs.drop_duplicates(
        subset=["contractAddress", "tokenSymbol", "tokenName"]
    )
    tokens.reset_index(drop=True, inplace=True)

    return tokens[["tokenSymbol", "tokenName", "contractAddress"]]
