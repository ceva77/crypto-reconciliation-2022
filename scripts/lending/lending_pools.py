import pandas as pd
from datetime import datetime

try:
    from scripts.lending.transactions import *
except ModuleNotFoundError:
    from transactions import *


chain = "polygon"
pool = CHAINS["polygon"]["v2_pool"]
wallet = WALLET_LIST[11]

pools = POOL_LIST
chains = CHAIN_LIST
wallets = WALLET_LIST

# get transactions with lending pool
def get_transactions_with_lending_pool(from_address, pool_address, chain="polygon"):
    normal_transactions = get_normal_transactions_by_address(from_address, chain)
    lending_pool_txs = filter_normal_transactions_by_address_to_address(
        normal_transactions, pool_address
    )
    lending_pool_txs.reset_index(drop=True, inplace=True)
    lending_pool_txs["wallet"] = from_address
    lending_pool_txs["pool"] = pool_address
    lending_pool_txs["chain"] = chain
    return lending_pool_txs


def get_transactions_with_all_lending_pools(wallets, pools, chains):
    txs = pd.DataFrame()
    for chain in chains:
        for pool in pools:
            for wallet in wallets:
                with rate_limiter:
                    temp_txs = get_transactions_with_lending_pool(wallet, pool, chain)

                txs = pd.concat([txs, temp_txs])

    return txs


def merge_raw_txs_and_token_txs(raw_txs, token_txs):
    # find all the erc20 transactions that have hashes in the lending pool txs
    hashes = raw_txs["hash"].unique()  # this only works if .unique() is used
    ind_list = []
    for i, tx in token_txs.iterrows():
        if tx["hash"] in hashes:
            ind_list.append(i)

    merged_transfers = token_txs.iloc[ind_list]
    merged_transfers.reset_index(drop=True, inplace=True)

    # merge token transfers with raw txs
    merged_transfers = merged_transfers.merge(raw_txs, on="hash", how="left")

    # filter and change column names
    merged_transfers = merged_transfers[
        [
            "blockNumber_x",
            "timeStamp_x",
            "hash",
            "nonce_x",
            "blockHash_x",
            "from_x",
            "contractAddress_x",
            "to_x",
            "value_x",
            "tokenName",
            "tokenSymbol",
            "tokenDecimal",
            "transactionIndex_x",
            "gas_x",
            "gasPrice_x",
            "gasUsed_x",
            "cumulativeGasUsed_x",
            "input_x",
            "confirmations_x",
            "value_y",
            "isError",
            "txreceipt_status",
            "input_y",
            "methodId",
            "functionName",
            "wallet",
            "pool",
            "chain",
        ]
    ]
    merged_transfers.columns = [
        "blockNumber",
        "timeStamp",
        "hash",
        "nonce",
        "blockHash",
        "from",
        "contractAddress",
        "transferTo",
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
        "pool",
        "chain",
    ]

    # format columns
    merged_transfers["datetime"] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in merged_transfers["timeStamp"]
    ]
    merged_transfers["amount"] = [int(amount) for amount in merged_transfers["amount"]]
    merged_transfers["tokenDecimal"] = [
        int(decimal) for decimal in merged_transfers["tokenDecimal"]
    ]
    merged_transfers["amount_fixed"] = (
        merged_transfers["amount"] / 10 ** merged_transfers["tokenDecimal"]
    )

    return merged_transfers


def get_token_transfers_with_lending_pool(from_address, pool_address, chain="polygon"):
    lending_pool_txs = get_transactions_with_lending_pool(
        from_address, pool_address, chain
    )

    # get erc20 transfers
    token_txs = get_token_txs_by_wallet(address=from_address, token=None, chain=chain)

    # find all the erc20 transactions that have hashes in the lending pool txs
    hashes = lending_pool_txs["hash"].unique()  # this only works if .unique() is used
    ind_list = []
    for i, tx in token_txs.iterrows():
        if tx["hash"] in hashes:
            ind_list.append(i)

    lending_pool_transfers = token_txs.iloc[ind_list]
    lending_pool_transfers.reset_index(drop=True, inplace=True)

    # merge token transfers with raw txs
    lending_pool_transfers = lending_pool_transfers.merge(
        lending_pool_txs, on="hash", how="left"
    )

    # filter and change column names
    lending_pool_transfers = lending_pool_transfers[
        [
            "blockNumber_x",
            "timeStamp_x",
            "hash",
            "nonce_x",
            "blockHash_x",
            "from_x",
            "contractAddress_x",
            "to_x",
            "value_x",
            "tokenName",
            "tokenSymbol",
            "tokenDecimal",
            "transactionIndex_x",
            "gas_x",
            "gasPrice_x",
            "gasUsed_x",
            "cumulativeGasUsed_x",
            "input_x",
            "confirmations_x",
            "value_y",
            "isError",
            "txreceipt_status",
            "input_y",
            "methodId",
            "functionName",
        ]
    ]
    lending_pool_transfers.columns = [
        "blockNumber",
        "timeStamp",
        "hash",
        "nonce",
        "blockHash",
        "from",
        "contractAddress",
        "transferTo",
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
    ]

    # format columns
    lending_pool_transfers["datetime"] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in lending_pool_transfers["timeStamp"]
    ]
    lending_pool_transfers["amount"] = [
        int(amount) for amount in lending_pool_transfers["amount"]
    ]
    lending_pool_transfers["tokenDecimal"] = [
        int(decimal) for decimal in lending_pool_transfers["tokenDecimal"]
    ]
    lending_pool_transfers["amount_fixed"] = (
        lending_pool_transfers["amount"] / 10 ** lending_pool_transfers["tokenDecimal"]
    )

    lending_pool_transfers["wallet"] = from_address
    lending_pool_transfers["pool"] = pool_address
    lending_pool_transfers["chain"] = chain

    return lending_pool_transfers


# get erc20 transactions associated with lending pool transations
def get_token_transfers_with_all_lending_pools(wallets, pools, chains):
    lending_pool_txs = get_transactions_with_all_lending_pools(wallets, pools, chains)

    token_txs = get_token_transfers(wallets, chains)

    return merge_raw_txs_and_token_txs(lending_pool_txs, token_txs)


def get_raw_transfers_with_all_lending_pools(wallets, pools, chains):
    lending_pool_txs = get_transactions_with_all_lending_pools(wallets, pools, chains)

    # simply find all the txs where the msg.value was greater than 0
    lending_pool_txs["value"] = [int(value) for value in lending_pool_txs["value"]]
    lending_pool_txs_with_raw_eth = lending_pool_txs[lending_pool_txs["value"] > 0]

    # fill out the missing columns
    lending_pool_txs_with_raw_eth["transferTo"] = lending_pool_txs_with_raw_eth["to"]
    lending_pool_txs_with_raw_eth["amount"] = [
        int(value) for value in lending_pool_txs_with_raw_eth["value"]
    ]
    lending_pool_txs_with_raw_eth["tokenName"] = [
        CHAINS[chain]["base_token_name"]
        for chain in lending_pool_txs_with_raw_eth["chain"]
    ]
    lending_pool_txs_with_raw_eth["tokenSymbol"] = [
        CHAINS[chain]["base_token_symbol"]
        for chain in lending_pool_txs_with_raw_eth["chain"]
    ]
    lending_pool_txs_with_raw_eth["tokenDecimal"] = [
        int(CHAINS[chain]["base_token_decimals"])
        for chain in lending_pool_txs_with_raw_eth["chain"]
    ]
    lending_pool_txs_with_raw_eth["amount_fixed"] = (
        lending_pool_txs_with_raw_eth["amount"]
        / 10 ** lending_pool_txs_with_raw_eth["tokenDecimal"]
    )
    lending_pool_txs_with_raw_eth["input_deprecated"] = lending_pool_txs_with_raw_eth[
        "input"
    ]
    lending_pool_txs_with_raw_eth["datetime"] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in lending_pool_txs_with_raw_eth["timeStamp"]
    ]

    return lending_pool_txs_with_raw_eth


# get raw transactions with a given lending pool
def get_raw_transfers_with_lending_pool(from_address, pool_address, chain="polygon"):
    lending_pool_txs = get_transactions_with_lending_pool(
        from_address, pool_address, chain
    )

    # simply find all the txs where the msg.value was greater than 0
    lending_pool_txs["value"] = [int(value) for value in lending_pool_txs["value"]]
    lending_pool_txs_with_raw_eth = lending_pool_txs[lending_pool_txs["value"] > 0]

    # fill out the missing columns
    lending_pool_txs_with_raw_eth["transferTo"] = lending_pool_txs_with_raw_eth["to"]
    lending_pool_txs_with_raw_eth["amount"] = [
        int(value) for value in lending_pool_txs_with_raw_eth["value"]
    ]
    lending_pool_txs_with_raw_eth["tokenName"] = CHAINS[chain]["base_token_name"]
    lending_pool_txs_with_raw_eth["tokenSymbol"] = CHAINS[chain]["base_token_symbol"]
    lending_pool_txs_with_raw_eth["tokenDecimal"] = int(
        CHAINS[chain]["base_token_decimals"]
    )
    lending_pool_txs_with_raw_eth["amount_fixed"] = (
        lending_pool_txs_with_raw_eth["amount"]
        / 10 ** lending_pool_txs_with_raw_eth["tokenDecimal"]
    )
    lending_pool_txs_with_raw_eth["input_deprecated"] = lending_pool_txs_with_raw_eth[
        "input"
    ]
    lending_pool_txs_with_raw_eth["datetime"] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in lending_pool_txs_with_raw_eth["timeStamp"]
    ]

    # lending_pool_txs_with_raw_eth["wallet"] = from_address
    # lending_pool_txs_with_raw_eth["pool"] = pool_address
    # lending_pool_txs_with_raw_eth["chain"] = chain

    return lending_pool_txs_with_raw_eth


# get combined erc20 and raw transfers with lending pool
def get_all_transfers_with_lending_pool(from_address, pool_address, chain="polygon"):
    raw_transfers = get_raw_transfers_with_lending_pool(
        from_address, pool_address, chain
    )
    token_transfers = get_token_transfers_with_lending_pool(
        from_address, pool_address, chain
    )

    all_transfers = pd.concat([raw_transfers, token_transfers])

    all_transfers["wallet"] = from_address
    all_transfers["wallet_name"] = wallet_address_to_name[from_address]
    all_transfers["pool"] = pool_address
    all_transfers["chain"] = chain

    return all_transfers


def get_lending_pool_transfers(wallets, pools, chains):
    return pd.concat(
        [
            get_token_transfers_with_all_lending_pools(wallets, pools, chains),
            get_raw_transfers_with_all_lending_pools(wallets, pools, chains),
        ]
    )


def get_lending_pool_transfers_for_all_pools(
    from_address, pool_addresses, chain="polygon"
):
    lending_pool_transfers = pd.DataFrame()

    for pool in pool_addresses:
        temp = get_all_transfers_with_lending_pool(from_address, pool, chain)

        lending_pool_transfers = pd.concat([lending_pool_transfers, temp])

    return lending_pool_transfers


def get_lending_pool_transfers_for_all_pools_and_chains(
    from_address, pool_addresses, chains
):
    lending_pool_transfers = pd.DataFrame()

    for chain in chains:
        for pool in pool_addresses:
            temp = get_all_transfers_with_lending_pool(from_address, pool, chain)

            lending_pool_transfers = pd.concat([lending_pool_transfers, temp])

    return lending_pool_transfers


def get_lending_pool_transfers_for_all(wallets, chains):
    lending_pool_transfers = pd.DataFrame()

    for wallet in wallets:
        for chain in chains:
            pools = CHAINS[chain]["pools"]
            for pool in pools:
                print(f"{wallet_address_to_name[wallet]}, {pool}, {chain}")
                with rate_limiter:
                    temp = get_all_transfers_with_lending_pool(wallet, pool, chain)
                    lending_pool_transfers = pd.concat([lending_pool_transfers, temp])


    return lending_pool_transfers


def main(verbose=False):
    lending_pool_transfers = get_lending_pool_transfers(wallets, pools, chains)

    if verbose:
        print(lending_pool_transfers)

    return lending_pool_transfers


if __name__ == "__main__":
    lending_pool_transfers = main(verbose=True)
    lending_pool_transfers.to_csv(
        "output_files/lending/lending_pool_transfers.csv", index=False
    )
