from datetime import datetime
import pandas as pd
import requests

from src.utils import CHAINS, CHAIN_LIST, WALLET_LIST, wallet_address_to_name


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
        df = df[df['isError'] != '1']
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
        
        # remove reverted transactions
        df.reset_index(drop=True, inplace=True)

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


# get all internal transactions by address
def get_internal_transactions_by_wallet(
    wallet, chain="mainnet", start_block=0, end_block=99999999
):
    base_url = CHAINS[chain]["explorer_url"]
    api_key = CHAINS[chain]["explorer_token"]

    url = (
        "{}?module=account&action=txlistinternal&address={}&startblock={}&endblock={}&apikey={}"
    )
    url = url.format(base_url, wallet, start_block, end_block, api_key)

    response = requests.get(url).json()

    if response["status"] == "1":
        df = pd.DataFrame(response["result"])
        df = df[df['isError'] != '1']
        df['wallet'] = wallet.lower()
        df['wallet_name'] = wallet_address_to_name[wallet]
        df['chain'] = chain

        empty_columns_to_add = ['nonce','blockHash','transactionIndex','gasPrice','txreceipt_status','cumulativeGasUsed','confirmations','methodId','functionName']
        for column in empty_columns_to_add:
            df[column] = ''

        columns_str_to_int = [
            "blockNumber",
            "timeStamp",
            "gas",
            "isError",
            "gasUsed",
        ]
        for column in columns_str_to_int:
            df[column] = df[column].astype(int)

        df.loc[:, ("to")] = [to.lower() for to in df["to"]]

        del df['type']
        del df['traceId']
        del df['errCode']

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
        df['value'] = [int(value) for value in df.value]

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


def get_normal_transactions(_wallets, _chains):
    if isinstance(_wallets, str):
        _wallets = [_wallets]
    if isinstance(_chains, str):
        _chains = [_chains]
    normal_transactions = pd.DataFrame()

    for wallet in _wallets:
        for chain in _chains:
            print(f"{wallet_address_to_name[wallet]}, {chain}")
            normal_transactions = pd.concat(
                [normal_transactions, get_normal_transactions_by_wallet(wallet, chain)]
            )

    normal_transactions.reset_index(drop=True, inplace=True)
    return normal_transactions.copy()


def filter_normal_transactions(_normal_transactions, _addresses):
    if isinstance(_addresses, str):
        _addresses = [_addresses]
    addresses = [address.lower() for address in _addresses]
    normal_transactions_filtered = _normal_transactions[
        _normal_transactions["to"].isin(addresses)
    ]

    normal_transactions_filtered.reset_index(drop=True, inplace=True)
    return normal_transactions_filtered.copy()


def get_token_transfers(wallets, chains):
    token_transfers = pd.DataFrame()
    for wallet in wallets:
        for chain in chains:
            print(f"{wallet_address_to_name[wallet]}, {chain}")
            temp_transfers = get_token_transfers_by_wallet(wallet, None, chain)
            token_transfers = pd.concat([token_transfers, temp_transfers])

    token_transfers.reset_index(drop=True, inplace=True)
    return token_transfers.copy()


def get_internal_transactions(_wallets, _chains):
    internal_transactions = pd.DataFrame()

    for wallet in _wallets:
        for chain in _chains:
            print(f"{wallet_address_to_name[wallet]}, {chain}")
            temp_transactions = get_internal_transactions_by_wallet(wallet, chain)
            internal_transactions = pd.concat([internal_transactions, temp_transactions])

    internal_transactions.reset_index(drop=True, inplace=True)
    return internal_transactions.copy()


# get all erc20 tokens interacted with
def get_all_tokens_interacted_with(wallets, chains):
    token_transfers = get_token_transfers(wallets, chains)
    tokens = token_transfers.drop_duplicates(
        subset=["contractAddress", "tokenSymbol", "tokenName"]
    )
    tokens.reset_index(drop=True, inplace=True)

    return tokens[["tokenSymbol", "tokenName", "contractAddress"]]


# format transactions where raw eth was exchanged into same format as erc20 transfers
def get_raw_transfers(_normal_transactions, _internal_transactions):
    combined_transactions = pd.concat([_normal_transactions, _internal_transactions])
    combined_transactions.loc[:,('value')] = [int(value) for value in combined_transactions['value']]
    plus_value_transactions = combined_transactions[combined_transactions['value'] > 0].copy() 

    plus_value_transactions.reset_index(drop=True, inplace=True)
    plus_value_chains = plus_value_transactions.loc[:,("chain")]
    plus_value_transactions.loc[:,('tokenName')] = [CHAINS[chain]["base_token_name"] for chain in plus_value_chains] 
    plus_value_transactions.loc[:,('tokenSymbol')] = [CHAINS[chain]["base_token_symbol"] for chain in plus_value_chains]
    plus_value_transactions.loc[:,('tokenDecimal')] = [CHAINS[chain]["base_token_decimals"] for chain in plus_value_chains]
    
    plus_value_transactions = plus_value_transactions.loc[:,
        ("blockNumber","timeStamp","hash","nonce","blockHash","from","contractAddress","to","value","tokenName","tokenSymbol","tokenDecimal","transactionIndex","gas","gasPrice","gasUsed","cumulativeGasUsed","input","confirmations","wallet","wallet_name","chain")
    ]

    return plus_value_transactions


# merge transactions with all token and raw eth transfers
# reformat columns to remove duplicate columns
def merge_transactions_and_token_transfers(_normal_transactions, _token_transfers, _internal_transactions):
    raw_transfers = get_raw_transfers(_normal_transactions, _internal_transactions)
    token_transfers = pd.concat([_token_transfers, raw_transfers])
    token_transfers.reset_index(drop=True, inplace=True)

    merged_transfers = token_transfers.merge(
        _normal_transactions, how="left", on=["hash","wallet"], suffixes=(None, "_tx")
    )

    merged_transfers = merged_transfers[
        [
            "blockNumber",
            "timeStamp",
            "hash",
            "nonce",
            "blockHash",
            "from",
            "from_tx",
            "contractAddress",
            "to",
            "to_tx",
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
            "value_tx",
            "isError",
            "txreceipt_status",
            "input_tx",
            "methodId",
            "functionName",
            "wallet",
            "wallet_name",
            "chain",
        ]
    ]

    merged_transfers.columns = [
        "blockNumber",
        "timeStamp",
        "hash",
        "nonce",
        "blockHash",
        "transferFrom",
        "from",
        "contractAddress",
        "transferTo",
        "to",
        "amount",
        "tokenName",
        "tokenSymbol",
        "tokenDecimal",
        "transactionIndex",
        "gas",
        "gasPrice",
        "gasUsed",
        "cumulativeGasUsed",
        "input_deprecated",
        "confirmations",
        "value",
        "isError",
        "txreceipt_status",
        "input",
        "methodId",
        "functionName",
        "wallet",
        "wallet_name",
        "chain",
    ]
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
        merged_transfers[column] = pd.to_numeric(merged_transfers[column])
    merged_transfers['amount'] = [int(amount) for amount in merged_transfers.amount]

    # add additional columns for use later
    merged_transfers['datetime'] = [datetime.fromtimestamp(timestamp) for timestamp in merged_transfers.timeStamp]
    merged_transfers['amount_fixed'] = merged_transfers.amount / 10**merged_transfers.tokenDecimal 
    merged_transfers['functionName'] = merged_transfers.functionName.astype(str)
    merged_transfers['action'] = [name.split("(")[0] for name in merged_transfers['functionName']]

    return merged_transfers


def main():
    wallets = WALLET_LIST
    chains = CHAIN_LIST

    print("Getting normal transactions")
    normal_transactions = get_normal_transactions(wallets, chains)

    print("Getting token transfers")
    token_transfers = get_token_transfers(wallets, chains)

    print("Getting internal transactions")
    internal_transactions = get_internal_transactions(wallets, chains)

    print("Getting all transfers")
    all_transfers = merge_transactions_and_token_transfers(normal_transactions, token_transfers, internal_transactions)

    return normal_transactions, token_transfers, internal_transactions, all_transfers


if __name__ == "__main__":
    normal_transactions, token_transfers, internal_transactions, all_transfers = main()

    normal_transactions.to_csv("output_files/normal_transactions.csv", index=False)
    token_transfers.to_csv("output_files/token_transfers.csv", index=False)
    internal_transactions.to_csv("output_files/internal_transactions.csv", index=False)
    all_transfers.to_csv("output_files/all_transfers.csv", index=False)



normal_transactions = pd.read_csv("output_files/normal_transactions.csv")
internal_transactions = pd.read_csv("output_files/internal_transactions.csv")
token_transfers = pd.read_csv("output_files/token_transfers.csv")
all_transfers = merge_transactions_and_token_transfers(normal_transactions, token_transfers, internal_transactions)