import pandas as pd
from datetime import date

from src.transactions import (
    get_normal_transactions, 
    get_token_transfers, 
    get_internal_transactions, 
    merge_transactions_and_token_transfers
)
from src.lending.split import get_deposits_and_borrows
from src.lending.filter import filter_deposits_and_withdrawals, filter_borrows_and_repayments, filter_split_txs
from src.utils import CHAIN_LIST, WALLET_LIST, POOL_LIST


def build_lending_excel_sheet(_wallets, _pools, _chains, verbose=False):
    if verbose:
        print("Downloading normal transactions")
    normal_transactions = get_normal_transactions(_wallets, _chains)

    if verbose:
        print("Getting token transfers")
    token_transfers = get_token_transfers(_wallets, _chains)

    if verbose:
        print("Getting internal transactions")
    internal_transactions = get_internal_transactions(_wallets, _chains)

    print(f"Retrieved {len(normal_transactions)} normal transactons")
    print(f"Retrieved {len(token_transfers)} erc20 transfers")

    # merge normal and token transfers
    all_transfers = merge_transactions_and_token_transfers(
        normal_transactions, 
        token_transfers, 
        internal_transactions
    )

    # get all the relevant dataframes
    if verbose:
        print("Getting collateral and borrow history")
    deposits_and_borrows = get_deposits_and_borrows(all_transfers, POOL_LIST)

    if verbose:
        print("Filtering collateral and borrowing transactions")
    deposits_and_withdrawals = filter_deposits_and_withdrawals(
        deposits_and_borrows
    )
    borrows_and_repayments = filter_borrows_and_repayments(deposits_and_borrows)

    split_txs = filter_split_txs(deposits_and_borrows) 

    # put everything to excel
    path = f"output_files/lending/aave_lending_{date.today()}.xlsx"
    with pd.ExcelWriter(path) as writer:
        if verbose:
            print("Writing to excel")

        normal_transactions.to_excel(
            writer, sheet_name="normal_transactions", index=False
        )
        all_transfers.to_excel(writer, sheet_name="all_transfers")
        deposits_and_withdrawals.to_excel(
            writer, sheet_name="deposits_and_withdrawals", index=False
        )
        borrows_and_repayments.to_excel(
            writer, sheet_name="borrows_and_repayments", index=False
        )
        split_txs.to_excel(
            writer, sheet_name="split_txs", index=False
        )
        if verbose:
            print("Done!")

    return True


def main(verbose=False):
    wallets = WALLET_LIST
    pools = POOL_LIST
    chains = CHAIN_LIST

    build_lending_excel_sheet(wallets, pools, chains, verbose)


if __name__ == "__main__":
    main(verbose=True)
