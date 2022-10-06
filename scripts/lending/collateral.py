"""
This script is used for calculating interest rewards on collateral deposited to AAVE
The goal is to take a dataframe of transactions for a given wallet with a given pool / chain,
and calculate the interest rewards in each currency. 

We want to output a list of withdrawals partitioned between interest and principal withdrawals
I.e. $1M USDC is deposited. Then assume $1.05M USDC is withdrawn.
We provide functions to convert these initial two transactions into 3: 
1) $1M deposit
2) $1M withdrawn
3) $0.05M interest received
"""


import pandas as pd

from scripts.lending.lending_pools import get_pool_transfers
from scripts.transactions import merge_transactions_and_token_transfers
from scripts.utils import CHAINS, POOL_LIST

normal_transactions = pd.read_csv("output_files/normal_transactions.csv")
token_transfers = pd.read_csv("output_files/token_transfers.csv")
internal_transactions = pd.read_csv("output_files/internal_transactions.csv")
all_transfers = merge_transactions_and_token_transfers(normal_transactions, token_transfers, internal_transactions)
pool_transfers = get_pool_transfers(all_transfers, POOL_LIST)


# get deposits and withdrawals from the list of all transfers and the transfers with lending pools
def get_deposits_and_withdrawals(_pool_transfers):
    deposits_and_withdraws = _pool_transfers[_pool_transfers.action.isin(['deposit','depositETH','withdraw','withdrawETH'])].copy()
    deposits_and_withdraws['pool'] = deposits_and_withdraws.to
    tokens = deposits_and_withdraws.tokenSymbol.unique()
    wallets = deposits_and_withdraws.wallet.unique()
    pools = deposits_and_withdraws.to.unique()

    balances = pd.DataFrame()
    for wallet in wallets:
        for pool in pools:
            for token in tokens:
                # get the transactions for this token
                temp_txs = deposits_and_withdraws[
                    (deposits_and_withdraws.tokenSymbol == token)
                    & (deposits_and_withdraws.to == pool)
                    & (deposits_and_withdraws.wallet == wallet)
                ].sort_values(by="datetime")

                # for each transaction compute the amount in the pool and the interest withdrawn
                temp_deposits = 0
                temp_withdraws = 0
                # loop over all the transactions for this token
                for _, tx in temp_txs.iterrows():
                    # increment deposits, withdrawals, and accrued interest
                    amount = tx.amount_fixed
                    if "ETH" in tx.action and tx.tokenSymbol == CHAINS[tx.chain]['base_token_symbol']:
                        continue
                    if tx.action in ["supply","depositETH","deposit"]:
                        temp_deposits += amount
                    elif tx.action in ["withdrawETH","withdraw"]:
                        temp_withdraws += amount
                    temp_interest = max(temp_withdraws - temp_deposits, 0)
                    row = pd.DataFrame(
                        [
                            {
                                "tokenSymbol": tx.tokenSymbol,
                                "hash": tx.hash,
                                "datetime": tx.datetime,
                                "action": tx.action,
                                "amount": amount,
                                "total_deposits": temp_deposits,
                                "total_withdraws": temp_withdraws,
                                "total_interest_received": temp_interest,
                                "wallet": tx.wallet,
                                "wallet_name": tx.wallet_name,
                                "pool": tx.pool,
                                "chain": tx.chain,
                            }
                        ]
                    )
                    if tx.action in ["withdrawETH","depositETH"]: 
                        # etherscan API can't find the internal raw ETH transfers so we need to 
                        # manually add them based on the values from the amWETH token
                        row_raw = pd.DataFrame(
                            [
                                {
                                    "tokenSymbol": CHAINS[tx.chain]['base_token_symbol'],
                                    "hash": tx.hash,
                                    "datetime": tx.datetime,
                                    "action": tx.action,
                                    "amount": amount,
                                    "total_deposits": temp_deposits,
                                    "total_withdraws": temp_withdraws,
                                    "total_interest_received": temp_interest,
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


# given a dataframe of running totals for deposits and withdrawals
# split out withdraws that received interest into multiple transactions
def get_split_interest_txs_collateral(_deposits_and_withdrawals):
    # look at withdrawals only
    withdrawals = _deposits_and_withdrawals[
        _deposits_and_withdrawals.action.isin(["withdraw","withdrawETH"])
    ].copy()
    tokens = withdrawals.tokenSymbol.unique()  # get list of tokensj
    wallets = withdrawals.wallet.unique()
    pools = withdrawals.pool.unique()

    split_txs = pd.DataFrame()
    for wallet in wallets:
        for pool in pools:
            for token in tokens:
                # get withdraws for this token only
                temp_withdraws = withdrawals[
                    (withdrawals.tokenSymbol == token)
                    & (withdrawals.pool == pool)
                    & (withdrawals.wallet == wallet)
                ].sort_values(by="datetime")

                prev_interest = 0
                # loop over withdraws for this token
                for _, withdraw in temp_withdraws.iterrows():
                    # if interest was gained in this tx...
                    if withdraw.total_interest_received > prev_interest:
                        # calculate interest and principal withdraw
                        this_interest = (
                            withdraw.total_interest_received - prev_interest
                        )
                        this_principal = withdraw.amount - this_interest
                        prev_interest = withdraw.total_interest_received

                        # construct two tx's for interest and principal withdrawals
                        txs = pd.DataFrame(
                            [
                                {
                                    "tokenSymbol": withdraw.tokenSymbol,
                                    "hash": withdraw.hash,
                                    "datetime": withdraw.datetime,
                                    "action": "withdraw_principal",
                                    "amount": this_principal,
                                    "total_deposits": withdraw.total_deposits,
                                    "total_withdraws": withdraw.total_withdraws,
                                    "total_interest_received": withdraw.total_interest_received,
                                    "wallet": withdraw.wallet,
                                    "wallet_name": withdraw.wallet_name,
                                    "pool": withdraw.pool,
                                    "chain": withdraw.chain,
                                },
                                {
                                    "tokenSymbol": withdraw.tokenSymbol,
                                    "hash": withdraw.hash,
                                    "datetime": withdraw.datetime,
                                    "action": "withdraw_interest",
                                    "amount": this_interest,
                                    "total_deposits": withdraw.total_deposits,
                                    "total_withdraws": withdraw.total_withdraws,
                                    "total_interest_received": withdraw.total_interest_received,
                                    "wallet": withdraw.wallet,
                                    "wallet_name": withdraw.wallet_name,
                                    "pool": withdraw.pool,
                                    "chain": withdraw.chain,
                                },
                            ]
                        )

                        # concatenate to working dataframe
                        split_txs = pd.concat([split_txs, txs])
                    else:  # if only principal was withdrawn, concat just one tx
                        this_principal = withdraw.amount
                        tx = pd.DataFrame(
                            [
                                {
                                    "tokenSymbol": withdraw.tokenSymbol,
                                    "hash": withdraw.hash,
                                    "datetime": withdraw.datetime,
                                    "action": "withdraw_principal",
                                    "amount": this_principal,
                                    "total_deposits": withdraw.total_deposits,
                                    "total_withdraws": withdraw.total_withdraws,
                                    "total_interest_received": withdraw.total_interest_received,
                                    "wallet": withdraw.wallet,
                                    "wallet_name": withdraw.wallet_name,
                                    "pool": withdraw.pool,
                                    "chain": withdraw.chain,
                                }
                            ]
                        )
                        split_txs = pd.concat([split_txs, tx])

    split_txs.reset_index(drop=True, inplace=True)
    return split_txs


# get split interest transactions for the sample pool and wallet on polygon
# if running from console, output results to a .csv file
def main(verbose=False):
    deposits_and_withdrawals = get_deposits_and_withdrawals(pool_transfers)

    split_txs = get_split_interest_txs_collateral(deposits_and_withdrawals)

    if verbose == True:
        print(split_txs)

    return deposits_and_withdrawals, split_txs


if __name__ == "__main__":
    deposits_and_withdrawals, split_txs = main(verbose=True)

    split_txs.to_csv("output_files/split_txs/split_txs_collateral.csv", index=False)
    deposits_and_withdrawals.to_csv("output_files/lending/deposits_and_withdrawals.csv", index=False)
