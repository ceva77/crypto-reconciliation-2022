from modulefinder import Module
import pandas as pd
import requests

try:
    from scripts.utils import *
except ModuleNotFoundError:
    from utils import *


# get transfers on a given token for a given wallet
def get_transfers_by_token(token, wallet):
    url = "https://api.etherscan.io/api?module=account&action=tokentx&contractaddress={}&address={}&startblock={}&endblock={}&apikey={}"

    api_key = ETHERSCAN_TOKEN
    start_block = 0
    end_block = 99999999

    url_format = url.format(token, wallet, start_block, end_block, api_key)

    response = requests.get(url_format)
    df = pd.DataFrame(response.json()["result"])

    return df


# check if a given transaction is internal, given a list of wallets
# note: tx must have "to" and "from" fields
def is_internal_tx(tx, wallet_list):
    if tx["from"] in wallet_list and tx["to"] in wallet_list:
        return True
    return False


# check for internal transactions
# transfers should be a pandas DataFrame of erc20 transfers (see above function)
# wallet_list should be a list of internal wallets
# returns a list of internal transfers by transaction ID
def find_internal_txs(transfers, wallet_list):
    assert isinstance(transfers, pd.DataFrame), "Transfers must be a Pandas DataFrame"

    wallet_list = [wallet.lower() for wallet in wallet_list]
    internal_txs = set()

    for _, transfer in transfers.iterrows():
        if is_internal_tx(transfer, wallet_list):
            internal_txs.add(transfer["hash"])

    return list(internal_txs)


def get_transfers_for_all_wallets(
    wallet_list, token=None, chain_id=1, method="alchemy"
):
    if method == "alchemy":
        out = pd.DataFrame()
        for wallet in wallet_list:
            out = pd.concat([out, get_asset_transfers_from(wallet, chain_id=chain_id)])

        return out

    if method == "etherscan":
        out = pd.DataFrame()
        for wallet in wallet_list:
            out = pd.concat([out, get_transfers_by_token(token, wallet)])

        return out

    return "Error: unrecognized method"


def get_transfers_for_all_wallets_and_tokens(wallet_list, token_list=None):
    out = pd.DataFrame()

    for token in token_list:
        out = pd.concat(
            [out, get_transfers_for_all_wallets(wallet_list, token, method="etherscan")]
        )

    return out


def get_transfers_for_all_wallets_and_chains(
    wallet_list, token=None, chain_ids=None, method="alchemy"
):
    if method == "alchemy":
        out = pd.DataFrame()
        for chain in chain_ids:
            out = pd.concat(
                [
                    out,
                    get_transfers_for_all_wallets(
                        wallet_list, chain_id=chain, method="alchemy"
                    ),
                ]
            )

        return out
    elif method == "etherscan":
        return "Not implemented"

    return "Error: unrecognized method, try 'alchemy'"


# returns a list of hashes for internal transactions across a given list of tokens and wallets
# only returns unique hashes
def find_internal_transactions_for_all_wallets_and_tokens(wallet_list, token_list):
    transfers = get_transfers_for_all_wallets_and_tokens(wallet_list, token_list)

    internal_txs = set()

    for _, transfer in transfers.iterrows():
        if is_internal_tx(transfer, wallet_list):
            internal_txs.add(transfer["hash"])

    return internal_txs


# marks transactions within a dataframe as internal (T/F) according to a list of internal wallets
# adds "internal" column to dataframe to identify a given tx as internal
def mark_internal_transactions_in_dataframe(transfers, wallet_list):
    assert isinstance(transfers, pd.DataFrame), "Transfers must be a pandas DataFrame"

    # make sure wallets are all lowercase
    wallet_list = [wallet.lower() for wallet in wallet_list]

    # check if each tx is internal, if yes add to the list
    internal_txs = []
    for _, transfer in transfers.iterrows():
        if is_internal_tx(transfer, wallet_list):
            internal_txs += [True]
        else:
            internal_txs += [False]

    # add the internal transfer bools as a column to the dataframe
    transfers["internal"] = internal_txs

    return transfers


# use the alchemy api to get all asset transfers from a given wallet address
def get_asset_transfers_from(
    addr,
    chain_id=1,
    from_block=0,
    to_block="latest",
    max_count=1000,
    categories=["external", "erc20"],
):
    # handle converting from and to block params to hexadecimal
    # the alchemy api requires hex values for these params
    if isinstance(from_block, int):
        from_block = hex(from_block)
    if isinstance(to_block, int):
        to_block = hex(to_block)
    if isinstance(max_count, int):
        max_count = hex(max_count)

    api_key = CHAINS[chain_id_to_name[chain_id]]["api-key"]
    url_prefix = CHAINS[chain_id_to_name[chain_id]]["url-prefix"]
    url = f"https://{url_prefix}/{api_key}"

    payload = {
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromAddress": addr,
                "fromBlock": from_block,
                "toBlock": to_block,
                "withMetadata": False,
                "excludeZeroValue": True,
                "maxCount": max_count,
                "category": categories,
            }
        ],
    }

    headers = {"accept": "application/json", "content-type": "application/json"}

    # make the request and convert result to pandas dataframe
    response = requests.post(url, json=payload, headers=headers)
    df = pd.DataFrame(response.json()["result"]["transfers"])

    df["chain"] = chain_id_to_name[chain_id]
    df["chain_id"] = chain_id

    df["wallet"] = wallet_address_to_name[addr]

    return df


# use the alchemy api to get all asset transfers to a given wallet address
def get_asset_transfers_to(
    addr,
    chain_id=1,
    from_block=0,
    to_block="latest",
    max_count=1000,
    categories=["external", "erc20"],
):
    # handle converting from and to block params to hexadecimal
    # the alchemy api requires hex values for these params
    if isinstance(from_block, int):
        from_block = hex(from_block)
    if isinstance(to_block, int):
        to_block = hex(to_block)
    if isinstance(max_count, int):
        max_count = hex(max_count)

    api_key = WEB3_ALCHEMY_PROJECT_ID
    url = f"https://eth-mainnet.alchemyapi.io/v2/{api_key}"

    payload = {
        "id": chain_id,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "toAddress": addr,
                "fromBlock": from_block,
                "toBlock": to_block,
                "withMetadata": False,
                "excludeZeroValue": True,
                "maxCount": max_count,
                "category": categories,
            }
        ],
    }

    headers = {"accept": "application/json", "content-type": "application/json"}

    # make the request and convert result to pandas dataframe
    response = requests.post(url, json=payload, headers=headers)
    df = pd.DataFrame(response.json()["result"]["transfers"])

    df["chain"] = chain_id_to_name[chain_id]
    df["chain_id"] = chain_id

    df["wallet"] = wallet_address_to_name[addr]

    return df


# get the transfers dataframe with "internal" column and the list of internal tx's (by hash)
def main():
    wallet_list = [wallet.lower() for wallet in WALLET_LIST]

    transfers = get_transfers_for_all_wallets_and_chains(
        wallet_list, chain_ids=CHAIN_IDS
    )
    transfers = mark_internal_transactions_in_dataframe(transfers, wallet_list)

    internal_txs = find_internal_txs(transfers, wallet_list)

    transfers.to_csv("output_files/internal_transfers/transfers_summary.csv")

    return transfers, internal_txs


if __name__ == "__main__":
    # print(WEB3_ALCHEMY_PROJECT_ID)
    main()
