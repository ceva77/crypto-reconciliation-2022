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

from scripts.lending.lending_pools import *


lending_pool_transfers = pd.read_excel(
    "output_files/lending/lending_pool_transfers.xlsx",
    sheet_name="all_transfers",
)


# sift token transfers from the lending pool. Filter for "deposit" and "withdraw" only
def get_deposits_and_withdrawals(_lending_pool_transfers):
    # fix function names
    _lending_pool_transfers["action"] = [
        name.split("(")[0] for name in _lending_pool_transfers["functionName"]
    ]

    # get deposits and withdraws only
    deposits_and_withdraws = _lending_pool_transfers[
        _lending_pool_transfers["action"].isin(
            ["supply", "deposit", "withdraw", "depositETH", "withdrawETH"]
        )
    ]

    # loop over all the tokens and calculate the running balances at each transaction
    tokens = deposits_and_withdraws["tokenSymbol"].unique()
    wallets = deposits_and_withdraws["wallet"].unique()
    pools = deposits_and_withdraws["pool"].unique()
    balances = pd.DataFrame()
    for wallet in wallets:
        for pool in pools:
            for token in tokens:
                # get the transactions for this token
                temp_txs = deposits_and_withdraws[
                    (deposits_and_withdraws["tokenSymbol"] == token)
                    & (deposits_and_withdraws["pool"] == pool)
                    & (deposits_and_withdraws["wallet"] == wallet)
                ]

                # for each transaction compute the amount in the pool and the interest withdrawn
                temp_deposits = 0
                temp_withdraws = 0
                # loop over all the transactions for this token
                for _, tx in temp_txs.iterrows():
                    # increment deposits, withdrawals, and accrued interest
                    amount = tx["amount_fixed"]
                    if tx["action"] in ["supply","depositETH","deposit"]:
                        temp_deposits += amount
                    elif tx["action"] in ["withdrawETH","withdraw"]:
                        temp_withdraws += amount
                    temp_interest = max(temp_withdraws - temp_deposits, 0)
                    row = pd.DataFrame(
                        [
                            {
                                "tokenSymbol": tx["tokenSymbol"],
                                "hash": tx["hash"],
                                "datetime": tx["datetime"],
                                "action": tx["action"],
                                "amount": amount,
                                "total_deposits": temp_deposits,
                                "total_withdraws": temp_withdraws,
                                "total_interest_received": temp_interest,
                                "wallet": tx["wallet"],
                                "wallet_name": tx["wallet_name"],
                                "pool": tx["pool"],
                                "chain": tx["chain"],
                            }
                        ]
                    )
                    balances = pd.concat([balances, row])

    return balances


# given a dataframe of running totals for deposits and withdrawals
# split out withdraws that received interest into multiple transactions
def get_split_interest_txs_collateral(deposits_and_withdrawals):
    # look at withdrawals only
    withdrawals = deposits_and_withdrawals[
        deposits_and_withdrawals["action"].isin(["withdraw","withdrawETH"])
    ]
    tokens = withdrawals["tokenSymbol"].unique()  # get list of tokensj
    wallets = withdrawals["wallet"].unique()
    pools = withdrawals["pool"].unique()

    split_txs = pd.DataFrame()
    for wallet in wallets:
        for pool in pools:
            for token in tokens:
                # get withdraws for this token only
                temp_withdraws = withdrawals[
                    (withdrawals["tokenSymbol"] == token)
                    & (withdrawals["pool"] == pool)
                    & (withdrawals["wallet"] == wallet)
                ]

                prev_interest = 0
                # loop over withdraws for this token
                for _, withdraw in temp_withdraws.iterrows():
                    # if interest was gained in this tx...
                    if withdraw["total_interest_received"] > prev_interest:
                        # calculate interest and principal withdraw
                        this_interest = (
                            withdraw["total_interest_received"] - prev_interest
                        )
                        this_principal = withdraw["amount"] - this_interest
                        prev_interest = withdraw["total_interest_received"]

                        # construct two tx's for interest and principal withdrawals
                        txs = pd.DataFrame(
                            [
                                {
                                    "tokenSymbol": withdraw["tokenSymbol"],
                                    "hash": withdraw["hash"],
                                    "datetime": withdraw["datetime"],
                                    "action": "withdraw_principal",
                                    "amount": this_principal,
                                    "total_deposits": withdraw["total_deposits"],
                                    "total_withdraws": withdraw["total_withdraws"],
                                    "total_interest_received": withdraw[
                                        "total_interest_received"
                                    ],
                                    "wallet": withdraw["wallet"],
                                    "wallet_name": withdraw["wallet_name"],
                                    "pool": withdraw["pool"],
                                    "chain": withdraw["chain"],
                                },
                                {
                                    "tokenSymbol": withdraw["tokenSymbol"],
                                    "hash": withdraw["hash"],
                                    "datetime": withdraw["datetime"],
                                    "action": "withdraw_interest",
                                    "amount": this_interest,
                                    "total_deposits": withdraw["total_deposits"],
                                    "total_withdraws": withdraw["total_withdraws"],
                                    "total_interest_received": withdraw[
                                        "total_interest_received"
                                    ],
                                    "wallet": withdraw["wallet"],
                                    "wallet_name": withdraw["wallet_name"],
                                    "pool": withdraw["pool"],
                                    "chain": withdraw["chain"],
                                },
                            ]
                        )

                        # concatenate to working dataframe
                        split_txs = pd.concat([split_txs, txs])
                    else:  # if only principal was withdrawn, concat just one tx
                        this_principal = withdraw["amount"]
                        tx = pd.DataFrame(
                            [
                                {
                                    "tokenSymbol": withdraw["tokenSymbol"],
                                    "hash": withdraw["hash"],
                                    "datetime": withdraw["datetime"],
                                    "action": "withdraw_principal",
                                    "amount": this_principal,
                                    "total_deposits": withdraw["total_deposits"],
                                    "total_withdraws": withdraw["total_withdraws"],
                                    "total_interest_received": withdraw[
                                        "total_interest_received"
                                    ],
                                    "wallet": withdraw["wallet"],
                                    "wallet_name": withdraw["wallet_name"],
                                    "pool": withdraw["pool"],
                                    "chain": withdraw["chain"],
                                }
                            ]
                        )
                        split_txs = pd.concat([split_txs, tx])

    return split_txs


# get split interest transactions for the sample pool and wallet on polygon
# if running from console, output results to a .csv file
def main(verbose=False):
    deposits_and_withdrawals = get_deposits_and_withdrawals(lending_pool_transfers)

    split_txs = get_split_interest_txs_collateral(deposits_and_withdrawals)

    if verbose == True:
        print(split_txs)

    return deposits_and_withdrawals, split_txs


if __name__ == "__main__":
    deposits_and_withdrawals, split_txs = main(verbose=True)
    with pd.ExcelWriter("output_files/lending/collateral.xlsx") as writer:
        deposits_and_withdrawals.to_excel(
            writer, sheet_name="deposits_and_withdrawals", index=False
        )
        split_txs.to_excel(writer, sheet_name="split_interest_txs", index=False)
