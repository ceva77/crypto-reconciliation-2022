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

try:
    from scripts.lending.lending_pools import *
except ModuleNotFoundError:
    from lending_pools import *

import pandas as pd


chain = "polygon"
pool = CHAINS["polygon"]["v2_pool"]
wallet = WALLET_LIST[11]


# sift token transfers from the lending pool. Filter for "deposit" and "withdraw" only
def get_deposits_and_withdrawals(lending_pool_transfers):
    # fix function names
    lending_pool_transfers["action"] = [
        name.split("(")[0] for name in lending_pool_transfers["functionName"]
    ]

    # get deposits and withdraws only
    deposits_and_withdraws = lending_pool_transfers[
        lending_pool_transfers["action"].isin(["deposit", "withdraw"])
    ]

    # loop over all the tokens and calculate the running balances at each transaction
    tokens = deposits_and_withdraws["tokenSymbol"].unique()
    balances = pd.DataFrame()
    for token in tokens:
        # get the transactions for this token
        temp_txs = deposits_and_withdraws[
            deposits_and_withdraws["tokenSymbol"] == token
        ]

        # for each transaction compute the amount in the pool and the interest withdrawn
        temp_balances = pd.DataFrame(
            columns=[
                "tokenSymbol",
                "hash",
                "datetime",
                "action",
                "amount",
                "total_deposits",
                "total_withdraws",
                "total_interest_received",
            ]
        )
        temp_deposits = 0
        temp_withdraws = 0
        # loop over all the transactions for this token
        for _, tx in temp_txs.iterrows():
            # increment deposits, withdrawals, and accrued interest
            amount = tx["amount"]
            if tx["action"] == "deposit":
                temp_deposits += int(amount)
            elif tx["action"] == "withdraw":
                temp_withdraws += int(amount)
            temp_interest = max(temp_withdraws - temp_deposits, 0)
            row = pd.DataFrame(
                [
                    {
                        "tokenSymbol": tx["tokenSymbol"],
                        "hash": tx["hash"],
                        "datetime": tx["datetime"],
                        "action": tx["action"],
                        "amount": tx["amount"],
                        "total_deposits": temp_deposits,
                        "total_withdraws": temp_withdraws,
                        "total_interest_received": temp_interest,
                    }
                ]
            )
            balances = pd.concat([balances, row])

    return balances


# given a dataframe of running totals for deposits and withdrawals
# split out withdraws that received interest into multiple transactions
def split_interest_transactions(deposits_and_withdrawals):
    # look at withdrawals only
    withdrawals = deposits_and_withdrawals[
        deposits_and_withdrawals["action"] == "withdraw"
    ]
    tokens = withdrawals["tokenSymbol"].unique()  # get list of tokensj

    split_txs = pd.DataFrame()
    for token in tokens:
        # get withdraws for this token only
        temp_withdraws = withdrawals[withdrawals["tokenSymbol"] == token]

        prev_interest = 0
        # loop over withdraws for this token
        for _, withdraw in temp_withdraws.iterrows():
            # if interest was gained in this tx...
            if withdraw["total_interest_received"] > prev_interest:
                # calculate interest and principal withdraw
                this_interest = int(withdraw["total_interest_received"]) - prev_interest
                this_principal = int(withdraw["amount"]) - this_interest
                prev_interest = int(withdraw["total_interest_received"])

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
                        }
                    ]
                )
                split_txs = pd.concat([split_txs, tx])

    return split_txs


# get split interest transactions for the sample pool and wallet on polygon
# if running from console, output results to a .csv file
def main(verbose=False):
    wallet = WALLET_LIST[11]
    pool = CHAINS["polygon"]["v2_pool"]
    chain = "polygon"

    lending_pool_transfers = get_token_transfers_with_lending_pool(wallet, pool, chain)

    deposits_and_withdrawals = get_deposits_and_withdrawals(lending_pool_transfers)

    split_txs = split_interest_transactions(deposits_and_withdrawals)

    if verbose == True:
        print(split_txs)

    return deposits_and_withdrawals, split_txs


if __name__ == "__main__":
    deposits_and_withdrawals, split_txs = main(verbose=True)
    deposits_and_withdrawals.to_csv(
        "output_files/deposits_and_withdrawals.csv", index=False
    )
    split_txs.to_csv("output_files/split_interest_transactions.csv", index=False)
