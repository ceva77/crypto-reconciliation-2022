import pandas as pd
from datetime import datetime

try:
    from scripts.lending.transactions import *
except ModuleNotFoundError:
    from transactions import *


chain = "polygon"
pool = CHAINS["polygon"]["v2_pool"]
wallet = WALLET_LIST[11]


# get transactions with lending pool
def get_transactions_with_lending_pool(from_address, pool_address, chain="polygon"):
    normal_transactions = get_normal_transactions_by_address(from_address, chain)
    lending_pool_txs = filter_normal_transactions_by_address_to_address(
        normal_transactions, pool_address
    )
    lending_pool_txs.reset_index(drop=True, inplace=True)
    return lending_pool_txs


# get erc20 transactions associated with lending pool transations
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
    lending_pool_transfers = lending_pool_transfers.merge(
        lending_pool_txs, on="hash", how="left"
    )
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

    lending_pool_transfers["datetime"] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in lending_pool_transfers["timeStamp"]
    ]
    return lending_pool_transfers


# get raw transactions with a given lending pool
def get_raw_transfers_with_lending_pool(from_address, pool_address, chain="polygon"):
    lending_pool_txs = get_transactions_with_lending_pool(
        from_address, pool_address, chain
    )

    # simply find all the txs where the msg.value was greater than 0
    lending_pool_txs['value'] = [int(value) for value in lending_pool_txs['value']]
    lending_pool_txs_with_raw_eth = lending_pool_txs[lending_pool_txs['value'] > 0]

    # fill out the missing columns
    lending_pool_txs_with_raw_eth['transferTo'] = lending_pool_txs_with_raw_eth['to']
    lending_pool_txs_with_raw_eth['amount'] = lending_pool_txs_with_raw_eth['value']
    lending_pool_txs_with_raw_eth['tokenName'] = CHAINS[chain]['base_token_name']
    lending_pool_txs_with_raw_eth['tokenSymbol'] = CHAINS[chain]['base_token_symbol']
    lending_pool_txs_with_raw_eth['tokenDecimal'] = CHAINS[chain]['base_token_decimals']
    lending_pool_txs_with_raw_eth['input_deprecated'] = lending_pool_txs_with_raw_eth['input'] 
    lending_pool_txs_with_raw_eth['datetime'] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in lending_pool_txs_with_raw_eth['timeStamp']
    ]

    return lending_pool_txs_with_raw_eth


# get combined erc20 and raw transfers with lending pool
def get_all_transfers_with_lending_pool(from_address, pool_address, chain="polygon"):
    raw_transfers = get_raw_transfers_with_lending_pool(from_address, pool_address, chain)
    token_transfers = get_token_transfers_with_lending_pool(from_address, pool_address, chain)

    all_transfers = pd.concat([raw_transfers, token_transfers])

    return all_transfers


def main(verbose=False):
    lending_pool_transfers = get_token_transfers_with_lending_pool(wallet, pool, chain)

    if verbose:
        print(lending_pool_transfers)

    return lending_pool_transfers


if __name__ == "__main__":
    lending_pool_transfers = main(verbose=True)
    lending_pool_transfers.to_csv(
        "output_files/lending_pool_transfers.csv", index=False
    )
