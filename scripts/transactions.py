import requests
import pandas as pd

try:
    from scripts.utils import *
except ModuleNotFoundError:
    from .utils import *


# get all "normal" transactions for a particular address on a given chain
def get_normal_transactions_by_wallet(
    wallet, chain="mainnet", start_block=0, end_block=99999999
):
    wallet = wallet.lower()
    base_url = CHAINS[chain]["explorer_url"]
    api_key = CHAINS[chain]["explorer_token"]

    url = (
        "{}?module=account&action=txlist&address={}&startblock={}&endblock={}&apikey={}"
    )
    url = url.format(base_url, wallet, start_block, end_block, api_key)

    response = requests.get(url).json()
    if response["status"] == "1":
        df = pd.DataFrame(response["result"])
        columns_str_to_int = [
            "blockNumber",
            "timeStamp",
            "nonce",
            "transactionIndex",
            "gas",
            "gasPrice",
            "isError",
            "txreceipt_status",
            "cumulativeGasUsed",
            "gasUsed",
            "confirmations",
        ]
        for column in columns_str_to_int:
            df[column] = df[column].astype(int)

        df["wallet"] = wallet.lower()
        df["wallet_name"] = wallet_address_to_name[wallet]
        df["chain"] = chain
        df.loc[:, ("to")] = [to.lower() for to in df["to"]]

        print(f"{len(df)} transaction(s) found")
        return df
    else:
        print(response["message"])
    df = pd.DataFrame(
        columns=[
            "blockNumber",
            "timeStamp",
            "hash",
            "nonce",
            "blockHash",
            "transactionIndex",
            "from",
            "to",
            "value",
            "gas",
            "gasPrice",
            "isError",
            "txreceipt_status",
            "input",
            "contractAddress",
            "cumulativeGasUsed",
            "gasUsed",
            "confirmations",
            "methodId",
            "functionName",
            "wallet",
            "wallet_name",
            "chain",
        ]
    )
    return df


# get all erc20 sends for a given wallet
def get_token_transfers_by_wallet(
    wallet, token=None, chain="mainnet", start_block=0, end_block=99999999
):
    base_url = CHAINS[chain]["explorer_url"]
    api_key = CHAINS[chain]["explorer_token"]

    if token:
        url = "{}?module=account&action=tokentx&contractaddress={}&address={}&startblock={}&endblock={}&apikey={}"
        url = url.format(base_url, token, wallet, start_block, end_block, api_key)
    else:
        url = "{}?module=account&action=tokentx&address={}&startblock={}&endblock={}&apikey={}"
        url = url.format(base_url, wallet, start_block, end_block, api_key)

    response = requests.get(url).json()
    if response["status"] == "1":
        df = pd.DataFrame(response["result"])
        columns_str_to_int = [
            "blockNumber",
            "timeStamp",
            "nonce",
            "tokenDecimal",
            "transactionIndex",
            "gas",
            "gasPrice",
            "gasUsed",
            "cumulativeGasUsed",
            "confirmations",
        ]
        for column in columns_str_to_int:
            df[column] = pd.to_numeric(df[column])

        df["wallet"] = wallet
        df["wallet_name"] = wallet_address_to_name[wallet]
        df["chain"] = chain

        print(f"{len(df)} token transfers found")
        return df
    else:
        print(response["message"])

    # ensure no errors if nothing is found
    return pd.DataFrame(
        columns=[
            "blockNumber",
            "timeStamp",
            "hash",
            "nonce",
            "blockHash",
            "from",
            "contractAddress",
            "to",
            "value",
            "tokenName",
            "tokenSymbol",
            "tokenDecimal",
            "transactionIndex",
            "gas",
            "gasPrice",
            "gasUsed",
            "cumulativeGasUsed",
            "input",
            "confirmations",
            "wallet",
            "wallet_name",
            "chain",
        ]
    )


def get_normal_transactions(wallets, chains):
    if isinstance(wallets, str):
        wallets = [wallets]
    if isinstance(chains, str):
        chains = [chains]
    normal_transactions = pd.DataFrame()

    for wallet in wallets:
        for chain in chains:
            print(f"{wallet_address_to_name[wallet]}, {chain}")
            normal_transactions = pd.concat(
                [normal_transactions, get_normal_transactions_by_wallet(wallet, chain)]
            )

    return normal_transactions


def filter_normal_transactions(normal_transactions, addresses):
    if isinstance(addresses, str):
        addresses = [addresses]
    addresses = [address.lower() for address in addresses]
    normal_transactions_filtered = normal_transactions[
        normal_transactions["to"].isin(addresses)
    ]

    return normal_transactions_filtered.copy()


def get_token_transfers(wallets, chains):
    token_transfers = pd.DataFrame()
    for wallet in wallets:
        for chain in chains:
            print(f"{wallet_address_to_name[wallet]}, {chain}")
            temp_transfers = get_token_transfers_by_wallet(wallet, None, chain)
            token_transfers = pd.concat([token_transfers, temp_transfers])

    token_transfers.reset_index(drop=True, inplace=True)
    return token_transfers


# get all erc20 tokens interacted with
def get_all_tokens_interacted_with(wallets, chains):
    token_transfers = get_token_transfers(wallets, chains)
    tokens = token_transfers.drop_duplicates(
        subset=["contractAddress", "tokenSymbol", "tokenName"]
    )
    tokens.reset_index(drop=True, inplace=True)

    return tokens[["tokenSymbol", "tokenName", "contractAddress"]]


def main(verbose=False):
    wallets = WALLET_LIST
    chains = CHAIN_LIST

    print("Getting normal transactions")
    normal_transactions = get_normal_transactions(wallets, chains)

    print("Getting token transfers")
    token_transfers = get_token_transfers(wallets, chains)

    if verbose:
        print(normal_transactions)
        print(token_transfers)

    return normal_transactions, token_transfers


if __name__ == "__main__":
    normal_transactions, token_transfers = main(verbose=True)
    with pd.ExcelWriter("output_files/transactions.xlsx") as writer:
        normal_transactions.to_excel(
            writer, sheet_name="normal_transactions", index=False
        )
        token_transfers.to_excel(writer, sheet_name="token_transfers", index=False)
