"""
This script is used for calculating interest rewards on lonas taken from AAVE
The goal is to take a dataframe of transactions for a given wallet with a given pool / chain,
and calculate the interest paid for each token. 

We want to output a list of withdrawals partitioned between interest and principal repayments.
I.e. $1M USDC is loaned. Then assume $1.05M USDC is repaid.
We provide functions to convert these initial two transactions into 3: 
1) $1M borrow
2) $1M repaid
3) $0.05M interest paid
"""

import pandas as pd

from scripts.lending.lending_pools import get_pool_transfers
from scripts.utils import POOL_LIST, CHAINS
from scripts.transactions import all_transfers

pools = POOL_LIST
pool_transfers = get_pool_transfers(all_transfers, pools)


def get_borrows_and_repayments(pool_transfers):
    borrows_and_repayments = pool_transfers[pool_transfers.action.isin(['borrow','borrowETH','repay','repayETH'])].copy()
    borrows_and_repayments['pool'] = borrows_and_repayments.to
    tokens = borrows_and_repayments.tokenSymbol.unique()
    wallets = borrows_and_repayments.wallet.unique()
    pools = borrows_and_repayments.to.unique()

    balances = pd.DataFrame()
    for wallet in wallets:
        for pool in pools:
            for token in tokens:
                # get the transactions for this token, pool, and wallet
                temp_txs = borrows_and_repayments[
                    (borrows_and_repayments.tokenSymbol == token)
                    & (borrows_and_repayments.to == pool)
                    & (borrows_and_repayments.wallet == wallet)
                ].sort_values(by="datetime")

                # for each transaction compute the amount in the pool and the interest withdrawn
                temp_borrows = 0
                temp_repayments = 0
                # loop over all the transactions for this token
                for _, tx in temp_txs.iterrows():
                    # increment borrows, withdrawals, and accrued interest
                    amount = tx.amount_fixed
                    if "ETH" in tx.action and tx.tokenSymbol == CHAINS[tx.chain]['base_token_symbol']:
                        continue
                    if tx.action in ["borrowETH", "borrow"]:
                        temp_borrows += amount
                    elif tx.action in ["repayETH", "repay"]:
                        temp_repayments += amount
                    temp_interest = max(temp_repayments - temp_borrows, 0)
                    row = pd.DataFrame(
                        [
                            {
                                "tokenSymbol": tx.tokenSymbol,
                                "hash": tx.hash,
                                "datetime": tx.datetime,
                                "action": tx.action,
                                "amount": amount,
                                "total_borrows": temp_borrows,
                                "total_repayments": temp_repayments,
                                "total_interest_paid": temp_interest,
                                "wallet": tx.wallet,
                                "wallet_name": tx.wallet_name,
                                "pool": tx.pool,
                                "chain": tx.chain,
                            }
                        ]
                    )
                    if tx.action in ["borrowETH","repayETH"]:
                        # etherscan API cant find internal raw ETH transfers so we need to 
                        # manuallly add it based on the values from the variable debt token
                        row_raw = pd.DataFrame(
                            [
                                {
                                    "tokenSymbol": CHAINS[tx.chain]['base_token_symbol'],
                                    "hash": tx.hash,
                                    "datetime": tx.datetime,
                                    "action": tx.action,
                                    "amount": amount,
                                    "total_borrows": temp_borrows,
                                    "total_repayments": temp_repayments,
                                    "total_interest_paid": temp_interest,
                                    "wallet": tx.wallet,
                                    "wallet_name": tx.wallet_name,
                                    "pool": tx.pool,
                                    "chain": tx.chain
                                }
                            ]
                        )
                        row = pd.concat([row, row_raw])
                    balances = pd.concat([balances, row])

    return balances.sort_values(by=['wallet','chain','pool','tokenSymbol','datetime'])


# given a dataframe of running totals for borrows and repayments
# split out repayments that paid interest into multiple transactions
def get_split_interest_txs_borrows(borrows_and_repayments):
    # look at repayments only
    repayments = borrows_and_repayments[
        borrows_and_repayments.action.isin(["repay", "repayETH"])
    ].copy()
    tokens = repayments.tokenSymbol.unique()  # get list of tokensj
    wallets = repayments.wallet.unique()
    pools = repayments.pool.unique()

    split_txs = pd.DataFrame()

    for wallet in wallets:
        for pool in pools:
            for token in tokens:
                # get repayments for this wallet, pool, and token only
                temp_repayments = repayments[
                    (repayments.tokenSymbol == token)
                    & (repayments.pool == pool)
                    & (repayments.wallet == wallet)
                ].sort_values(by="datetime")

                prev_interest = 0
                # loop over repayments for this token
                for _, repayment in temp_repayments.iterrows():
                    # if interest was gained in this tx...
                    if repayment.total_interest_paid > prev_interest:
                        # calculate interest and principal repayment
                        this_interest = repayment.total_interest_paid - prev_interest
                        this_principal = repayment.amount - this_interest
                        prev_interest = repayment.total_interest_paid

                        # construct two tx's for interest and principal repayment
                        txs = pd.DataFrame(
                            [
                                # principal
                                {
                                    "tokenSymbol": repayment.tokenSymbol,
                                    "hash": repayment.hash,
                                    "datetime": repayment.datetime,
                                    "action": "repay_principal",
                                    "amount": this_principal,
                                    "total_borrows": repayment.total_borrows,
                                    "total_repayments": repayment.total_repayments,
                                    "total_interest_paid": repayment.total_interest_paid
                                    ,
                                    "wallet": repayment.wallet,
                                    "wallet_name": repayment.wallet_name,
                                    "pool": repayment.pool,
                                    "chain": repayment.chain,
                                },
                                # interest
                                {
                                    "tokenSymbol": repayment.tokenSymbol,
                                    "hash": repayment.hash,
                                    "datetime": repayment.datetime,
                                    "action": "repay_interest",
                                    "amount": this_interest,
                                    "total_borrows": repayment.total_borrows,
                                    "total_repayments": repayment.total_repayments,
                                    "total_interest_paid": repayment.total_interest_paid
                                    ,
                                    "wallet": repayment.wallet,
                                    "wallet_name": repayment.wallet_name,
                                    "pool": repayment.pool,
                                    "chain": repayment.chain,
                                },
                            ]
                        )

                        # concatenate to working dataframe
                        split_txs = pd.concat([split_txs, txs])
                    else:  # if only principal was repaid, concat just one tx
                        this_principal = repayment.amount
                        tx = pd.DataFrame(
                            [
                                {
                                    "tokenSymbol": repayment.tokenSymbol,
                                    "hash": repayment.hash,
                                    "datetime": repayment.datetime,
                                    "action": "repay_principal",
                                    "amount": this_principal,
                                    "total_borrows": repayment.total_borrows,
                                    "total_repayments": repayment.total_repayments,
                                    "total_interest_paid": repayment.total_interest_paid,
                                    "wallet": repayment.wallet,
                                    "wallet_name": repayment.wallet_name,
                                    "pool": repayment.pool,
                                    "chain": repayment.chain,
                                }
                            ]
                        )
                        split_txs = pd.concat([split_txs, tx])

    return split_txs


# get split interest transactions for the sample pool and wallet on polygon
# if running from console, output results to a .csv file
def main(verbose=False):
    borrows_and_repayments = get_borrows_and_repayments(pool_transfers)

    split_txs = get_split_interest_txs_borrows(borrows_and_repayments)

    if verbose == True:
        print(borrows_and_repayments)
        print(split_txs)

    return borrows_and_repayments, split_txs


if __name__ == "__main__":
    borrows_and_repayments, split_txs = main(verbose=True)
    with pd.ExcelWriter("output_files/lending/borrowing.xlsx") as writer:
        borrows_and_repayments.to_excel(
            writer, sheet_name="borrows_and_repayments", index=False
        )
        split_txs.to_excel(writer, sheet_name="split_interest_txs", index=False)
