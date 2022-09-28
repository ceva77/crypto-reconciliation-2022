import pandas as pd
from datetime import datetime

from scripts.transactions import *


pools = POOL_LIST
chains = CHAIN_LIST
wallets = WALLET_LIST

try:
    normal_transactions = pd.read_excel(
        "output_files/transactions.xlsx", sheet_name="normal_transactions"
    )
    token_transfers = pd.read_excel(
        "output_files/transactions.xlsx", sheet_name="token_transfers"
    )
except FileNotFoundError:
    pass


def get_transactions_with_lending_pools(_normal_transactions, _pools):
    _lending_pool_txs = filter_normal_transactions(_normal_transactions, _pools)
    _lending_pool_txs.loc[:, ("pool")] = _lending_pool_txs["to"]
    _lending_pool_txs.reset_index(drop=True, inplace=True)

    return _lending_pool_txs.copy()


def get_token_transfers_with_lending_pools(_lending_pool_txs, _token_transfers):
    hashes = _lending_pool_txs["hash"].unique()  # get all tx hashes
    ind_list = []
    for i, transfer in _token_transfers.iterrows():
        # find all transfers with matching hashes to the raw transactions
        if transfer["hash"] in hashes:
            ind_list.append(i)

    _lending_pool_transfers = _token_transfers.iloc[ind_list]
    _lending_pool_transfers.reset_index(drop=True, inplace=True)

    # merge transfers and raw txs
    _lending_pool_transfers = _lending_pool_transfers.merge(
        _lending_pool_txs, on="hash", how="left"
    )

    # filter and change column names
    _lending_pool_transfers = _lending_pool_transfers[
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
            "wallet_x",
            "wallet_name_x",
            "pool",
            "chain_x",
        ]
    ]
    _lending_pool_transfers.columns = [
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
        "wallet_name",
        "pool",
        "chain",
    ]

    # format columns
    _lending_pool_transfers.loc[:, ("datetime")] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in _lending_pool_transfers["timeStamp"]
    ]
    _lending_pool_transfers.loc[:, ("amount")] = [
        int(amount) for amount in _lending_pool_transfers["amount"]
    ]
    _lending_pool_transfers.loc[:, ("tokenDecimal")] = [
        int(decimal) for decimal in _lending_pool_transfers["tokenDecimal"]
    ]
    _lending_pool_transfers.loc[:, ("amount_fixed")] = (
        _lending_pool_transfers["amount"]
        / 10 ** _lending_pool_transfers["tokenDecimal"]
    )

    return _lending_pool_transfers


def get_raw_transfers_with_lending_pools(_lending_pool_txs):
    # simply find all the txs where the msg.value was greater than 0
    _lending_pool_txs.loc[:, ("value")] = [
        int(value) for value in _lending_pool_txs["value"]
    ]
    _lending_pool_txs_with_raw_eth = _lending_pool_txs[_lending_pool_txs["value"] > 0]

    # fill out the missing columns
    _lending_pool_txs_with_raw_eth = _lending_pool_txs_with_raw_eth.rename(
        columns={"to": "transferTo"}
    )
    _lending_pool_txs_with_raw_eth.loc[:, ("amount")] = [
        int(value) for value in _lending_pool_txs_with_raw_eth["value"]
    ]
    _lending_pool_txs_with_raw_eth.loc[:, ("tokenName")] = [
        CHAINS[chain]["base_token_name"]
        for chain in _lending_pool_txs_with_raw_eth["chain"]
    ]
    _lending_pool_txs_with_raw_eth.loc[:, ("tokenSymbol")] = [
        CHAINS[chain]["base_token_symbol"]
        for chain in _lending_pool_txs_with_raw_eth["chain"]
    ]
    _lending_pool_txs_with_raw_eth.loc[:, ("tokenDecimal")] = [
        int(CHAINS[chain]["base_token_decimals"])
        for chain in _lending_pool_txs_with_raw_eth["chain"]
    ]
    _lending_pool_txs_with_raw_eth.loc[:, ("amount_fixed")] = (
        _lending_pool_txs_with_raw_eth["amount"]
        / 10 ** _lending_pool_txs_with_raw_eth["tokenDecimal"]
    )
    _lending_pool_txs_with_raw_eth.loc[
        :, ("input_deprecated")
    ] = _lending_pool_txs_with_raw_eth["input"]
    _lending_pool_txs_with_raw_eth.loc[:, ("datetime")] = [
        datetime.fromtimestamp(int(timestamp))
        for timestamp in _lending_pool_txs_with_raw_eth["timeStamp"]
    ]

    return _lending_pool_txs_with_raw_eth


# get combined erc20 and raw transfers with lending pool
def merge_token_with_raw_transfers(_token_transfers, _raw_transfers):
    _all_transfers = pd.concat([_token_transfers, _raw_transfers])

    return _all_transfers


def main(verbose=False):
    lending_pool_txs = get_transactions_with_lending_pools(normal_transactions, pools)
    lending_pool_transfers = get_token_transfers_with_lending_pools(
        lending_pool_txs, token_transfers
    )
    lending_pool_raw_transfers = get_raw_transfers_with_lending_pools(lending_pool_txs)
    all_lending_pool_transfers = merge_token_with_raw_transfers(
        lending_pool_transfers, lending_pool_raw_transfers
    )

    if verbose:
        print("Token Transfers")
        print(lending_pool_transfers)
        print("Raw transfers")
        print(lending_pool_raw_transfers)
        print("All transfers:")
        print(all_lending_pool_transfers)

    return (
        lending_pool_transfers,
        lending_pool_raw_transfers,
        all_lending_pool_transfers,
    )


if __name__ == "__main__":
    (
        lending_pool_transfers,
        lending_pool_raw_transfers,
        all_lending_pool_transfers,
    ) = main(verbose=True)
    path = "output_files/lending/lending_pool_transfers.xlsx"
    with pd.ExcelWriter(path) as writer:
        lending_pool_transfers.to_excel(
            writer, sheet_name="token_transfers", index=False
        )
        lending_pool_raw_transfers.to_excel(
            writer, sheet_name="raw_transfers", index=False
        )
        all_lending_pool_transfers.to_excel(
            writer, sheet_name="all_transfers", index=False
        )
