try:
    from scripts.internal_transfers.get_transactions import *
except ModuleNotFoundError:
    from get_transactions import *

import pandas as pd


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


# get all transactions and mark as internal or not
# send output to a .csv file
def main(verbose=False):
    df = get_transactions_for_all_wallets_and_chains()

    df = mark_internal_transactions_in_dataframe(df, WALLET_LIST).drop_duplicates(
        subset=["hash"]
    )

    if verbose:
        # print all internal tx hashes
        print(find_internal_txs(df, WALLET_LIST))

    df.to_csv("output_files/internal_transfers/transactions_summary.csv", index=False)

    return df


if __name__ == "__main__":
    _ = main(verbose=True)
