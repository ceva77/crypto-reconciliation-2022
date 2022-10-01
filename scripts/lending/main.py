import pandas as pd

from scripts.transactions import get_normal_transactions, get_token_transfers, merge_transactions_and_token_transfers
from scripts.lending.lending_pools import get_pool_transfers
from scripts.lending.collateral import (
    get_deposits_and_withdrawals,
    get_split_interest_txs_collateral,
)
from scripts.lending.borrowing import (
    get_borrows_and_repayments,
    get_split_interest_txs_borrows,
)
from scripts.utils import CHAIN_LIST, WALLET_LIST, POOL_LIST


def build_lending_excel_sheet(_wallets, _pools, _chains, verbose=False):
    if verbose:
        print("Downloading normal transactions")
    normal_transactions = get_normal_transactions(_wallets, _chains)

    if verbose:
        print("Getting token transfers")
    token_transfers = get_token_transfers(_wallets, _chains)

    print(f"Retrieved {len(normal_transactions)} normal transactons")
    print(f"Retrieved {len(token_transfers)} erc20 transfers")

    # merge normal and token transfers
    all_transfers = merge_transactions_and_token_transfers(normal_transactions, token_transfers)

    # get all the relevant dataframes
    if verbose:
        print("Getting lending pool transfers")
    pool_transfers = get_pool_transfers(all_transfers, _pools)
    print(f"Found {len(pool_transfers)} lending pool transfers")

    if verbose:
        print("Getting collateral and borrow history")
    deposits_and_withdrawals = get_deposits_and_withdrawals(pool_transfers)
    print(f"{len(deposits_and_withdrawals)} deposits and withdrawals")
    borrows_and_repayments = get_borrows_and_repayments(pool_transfers)
    print(f"{len(borrows_and_repayments)} borrows and repayments")

    if verbose:
        print("Splitting principal and interest transactions")
    split_interest_txs_collateral = get_split_interest_txs_collateral(
        deposits_and_withdrawals
    )
    split_interest_txs_borrows = get_split_interest_txs_borrows(borrows_and_repayments)
    print(
        f"{len(split_interest_txs_borrows)+len(split_interest_txs_collateral)} total split principal and interest payments"
    )

    # put everything to excel
    path = f"output_files/lending/aave_lending.xlsx"
    with pd.ExcelWriter(path) as writer:
        if verbose:
            print("Writing to excel")

        normal_transactions.to_excel(
            writer, sheet_name="normal_transactions", index=False
        )
        token_transfers.to_excel(writer, sheet_name="token_transfers")
        pool_transfers.to_excel(
            writer, sheet_name="lending_pool_transfers", index=False
        )
        deposits_and_withdrawals.to_excel(
            writer, sheet_name="deposits_and_withdrawals", index=False
        )
        split_interest_txs_collateral.to_excel(
            writer, sheet_name="split_interest_txs_collateral", index=False
        )
        borrows_and_repayments.to_excel(
            writer, sheet_name="borrows_and_repayments", index=False
        )
        split_interest_txs_borrows.to_excel(
            writer, sheet_name="split_interest_txs_borrows", index=False
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
