import pandas as pd

try:
    from lending_pools import get_all_transfers_with_lending_pool
    from collateral import (
        get_deposits_and_withdrawals,
        get_split_interest_txs_collateral,
    )
    from borrowing import get_borrows_and_repayments, get_split_interest_txs_borrows
    from ..utils import *
except ModuleNotFoundError:
    from scripts.lending.lending_pools import get_all_transfers_with_lending_pool
    from scripts.lending.collateral import (
        get_deposits_and_withdrawals,
        get_split_interest_txs_collateral,
    )
    from scripts.lending.borrowing import (
        get_borrows_and_repayments,
        get_split_interest_txs_borrows,
    )
    from scripts.utils import *


def build_lending_excel_sheet(wallet, pool, chain="polygon", verbose=False):
    wallet_name = wallet_address_to_name[wallet]

    # get all the relevant dataframes
    if verbose: print("Getting lending pool transfers")
    lending_pool_transfers = get_all_transfers_with_lending_pool(wallet, pool, chain)

    if verbose: print("Getting collateral and borrow history")
    deposits_and_withdrawals = get_deposits_and_withdrawals(lending_pool_transfers)
    borrows_and_repayments = get_borrows_and_repayments(lending_pool_transfers)

    if verbose: print("Splitting principal and interest transactions")
    split_interest_txs_collateral = get_split_interest_txs_collateral(
        deposits_and_withdrawals
    )
    split_interest_txs_borrows = get_split_interest_txs_borrows(borrows_and_repayments)

    # put everything to excel
    path = f"output_files/lending/{wallet_name}_lending_{chain}.xlsx"
    with pd.ExcelWriter(path) as writer:
        if verbose: print("Writing to excel")

        lending_pool_transfers.to_excel(writer, sheet_name="lending_pool_transfers", index=False)

        deposits_and_withdrawals.to_excel(writer, sheet_name="deposits_and_withdrawals", index=False)
        split_interest_txs_collateral.to_excel(
            writer, sheet_name="split_interest_txs_collateral", index=False
        )

        borrows_and_repayments.to_excel(writer, sheet_name="borrows_and_repayments", index=False)
        split_interest_txs_borrows.to_excel(
            writer, sheet_name="split_interest_txs_borrows", index=False
        )
        if verbose: print("Done!")


def main(verbose=False):
    wallet = TEST_WALLET
    pool = TEST_POOL
    chain = TEST_CHAIN

    build_lending_excel_sheet(wallet, pool, chain, verbose)



if __name__ == "__main__":
    main(verbose=True)
