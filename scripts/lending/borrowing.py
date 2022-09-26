
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

try:
    from scripts.lending.lending_pools import *
except ModuleNotFoundError:
    from lending_pools import *

import pandas as pd


chain = "polygon"
pool = CHAINS["polygon"]["v2_pool"]
wallet = WALLET_LIST[11]


# sift token transfers from the lending pool. Filter for "deposit" and "withdraw" only
def get_borrows_and_repayments(lending_pool_transfers):
    # fix function names
    lending_pool_transfers["action"] = [
        name.split("(")[0] for name in lending_pool_transfers["functionName"]
    ]

    # get borrows and repayments only
    borrows_and_repayments = lending_pool_transfers[
        lending_pool_transfers["action"].isin(["borrow", "borrowETH", "repay","repayETH"])
    ]

    # loop over all the tokens and calculate the running balances at each transaction
    tokens = borrows_and_repayments["tokenSymbol"].unique()
    balances = pd.DataFrame()
    for token in tokens:
        # get the transactions for this token
        temp_txs = borrows_and_repayments[
            borrows_and_repayments["tokenSymbol"] == token
        ]

        # for each transaction compute the amount in the pool and the interest withdrawn
        temp_borrows = 0
        temp_repayments = 0
        # loop over all the transactions for this token
        for _, tx in temp_txs.iterrows():
            # increment borrows, withdrawals, and accrued interest
            amount = tx["amount"]
            if tx["action"] == "borrow":
                temp_borrows += int(amount)
            elif tx["action"] == "repay":
                temp_repayments += int(amount)
            temp_interest = max(temp_repayments - temp_borrows, 0)
            row = pd.DataFrame(
                [
                    {
                        "tokenSymbol": tx["tokenSymbol"],
                        "hash": tx["hash"],
                        "datetime": tx["datetime"],
                        "action": tx["action"],
                        "amount": tx["amount"],
                        "total_borrows": temp_borrows,
                        "total_repayments": temp_repayments,
                        "total_interest_paid": temp_interest,
                    }
                ]
            )
            balances = pd.concat([balances, row])

    return balances


# given a dataframe of running totals for borrows and repayments
# split out repayments that paid interest into multiple transactions
def split_interest_transactions(borrows_and_repayments):
    # look at repayments only
    repayments = borrows_and_repayments[
        borrows_and_repayments["action"] == "repay"
    ]
    tokens = repayments["tokenSymbol"].unique()  # get list of tokensj

    split_txs = pd.DataFrame()
    for token in tokens:
        # get repayments for this token only
        temp_repayments = repayments[repayments["tokenSymbol"] == token]

        prev_interest = 0
        # loop over repayments for this token
        for _, repayment in temp_repayments.iterrows():
            # if interest was gained in this tx...
            if repayment["total_interest_paid"] > prev_interest:
                # calculate interest and principal repayment
                this_interest = int(repayment["total_interest_paid"]) - prev_interest
                this_principal = int(repayment["amount"]) - this_interest
                prev_interest = int(repayment["total_interest_paid"])

                # construct two tx's for interest and principal repayment
                txs = pd.DataFrame(
                    [
                        # principal
                        {
                            "tokenSymbol": repayment["tokenSymbol"],
                            "hash": repayment["hash"],
                            "datetime": repayment["datetime"],
                            "action": "repay_principal",
                            "amount": this_principal,
                            "total_borrows": repayment["total_borrows"],
                            "total_repayments": repayment["total_repayments"],
                            "total_interest_paid": repayment[
                                "total_interest_paid"
                            ],
                        },
                        # interest
                        {
                            "tokenSymbol": repayment["tokenSymbol"],
                            "hash": repayment["hash"],
                            "datetime": repayment["datetime"],
                            "action": "repay_interest",
                            "amount": this_interest,
                            "total_borrows": repayment["total_borrows"],
                            "total_repayments": repayment["total_repayments"],
                            "total_interest_paid": repayment[
                                "total_interest_paid"
                            ],
                        },
                    ]
                )

                # concatenate to working dataframe
                split_txs = pd.concat([split_txs, txs])
            else:  # if only principal was repaid, concat just one tx
                this_principal = repayment["amount"]
                tx = pd.DataFrame(
                    [
                        {
                            "tokenSymbol": repayment["tokenSymbol"],
                            "hash": repayment["hash"],
                            "datetime": repayment["datetime"],
                            "action": "repay_principal",
                            "amount": this_principal,
                            "total_borrows": repayment["total_borrows"],
                            "total_repayments": repayment["total_repayments"],
                            "total_interest_paid": repayment[
                                "total_interest_paid"
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

    borrows_and_repayments = get_borrows_and_repayments(lending_pool_transfers)

    split_txs = split_interest_transactions(borrows_and_repayments)

    if verbose == True:
        print(borrows_and_repayments)
        print(split_txs)

    return borrows_and_repayments, split_txs


if __name__ == "__main__":
    borrows_and_repayments, split_txs = main(verbose=True)
    borrows_and_repayments.to_csv(
        "output_files/lending/borrows_and_repayments.csv", index=False
    )
    split_txs.to_csv("output_files/lending/split_interest_transactions_borrows.csv", index=False)