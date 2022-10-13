from src.transactions import get_normal_transactions
from src.utils import WALLET_LIST, CHAIN_LIST

import pandas as pd


wallets = WALLET_LIST
chains = CHAIN_LIST
normal_transactions = pd.read_excel(
    "output_files/transactions.xlsx", sheet_name="normal_transactions"
)


# check if a given transaction is internal, given a list of wallets
# note: tx must have "to" and "from" fields
def is_internal_tx(_tx, _wallets):
    if _tx["from"] in _wallets and _tx["to"] in _wallets:
        return True
    return False


# check for internal transactions
# transfers should be a pandas DataFrame of erc20 transfers (see above function)
# wallet_list should be a list of internal wallets
# returns a list of internal transfers by transaction ID
def find_internal_txs(_transfers, _wallets):
    assert isinstance(_transfers, pd.DataFrame), "Transfers must be a Pandas DataFrame"

    _wallets = [wallet.lower() for wallet in _wallets]
    internal_txs = set()

    for _, transfer in _transfers.iterrows():
        if is_internal_tx(transfer, _wallets):
            internal_txs.add(transfer["hash"])

    return list(internal_txs)


# marks transactions within a dataframe as internal (T/F) according to a list of internal wallets
# adds "internal" column to dataframe to identify a given tx as internal
def mark_internal_transactions_in_dataframe(_transfers, _wallets):
    assert isinstance(_transfers, pd.DataFrame), "Transfers must be a pandas DataFrame"

    # make sure wallets are all lowercase
    _wallets = [wallet.lower() for wallet in _wallets]

    # check if each tx is internal, if yes add to the list
    internal_txs = []
    for _, transfer in _transfers.iterrows():
        if is_internal_tx(transfer, _wallets):
            internal_txs += [True]
        else:
            internal_txs += [False]

    # add the internal transfer bools as a column to the dataframe
    _transfers["internal"] = internal_txs

    return _transfers.copy()


# get all transactions and mark as internal or not
# send output to a .csv file
def main(verbose=False):
    normal_transactions_marked = mark_internal_transactions_in_dataframe(
        normal_transactions, wallets
    ).drop_duplicates(subset=["hash"])

    if verbose:
        # print all internal tx hashes
        print(find_internal_txs(normal_transactions, wallets))

    normal_transactions_marked.to_csv(
        "output_files/internal_transfers/internal_transaction.csv", index=False
    )

    return normal_transactions_marked


if __name__ == "__main__":
    _ = main(verbose=True)
