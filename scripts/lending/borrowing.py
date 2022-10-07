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
from scripts.transactions import merge_transactions_and_token_transfers
from scripts.utils import CHAINS, POOL_LIST

normal_transactions = pd.read_csv("output_files/normal_transactions.csv")
token_transfers = pd.read_csv("output_files/token_transfers.csv")
internal_transactions = pd.read_csv("output_files/internal_transactions.csv")
all_transfers = merge_transactions_and_token_transfers(normal_transactions, token_transfers, internal_transactions)
pool_transfers = get_pool_transfers(all_transfers, POOL_LIST)

deposit_tokens = [
    "bAVAX",
    "gFTM",
    "amAAVE",
    "amDAI",
    "amUSDC",
    "amUSDT",
    "amWBTC",
    "amWMATIC",
    "aCRV",
    "aLINK",
    "aUSDC",
    "aUSDT",
    "aXSUSHI",
    "aYFI",
    "amWETH",
]

# redo borrowing logic
# borrow -> add to total_borrows
# repay -> if amount + total_repayments > total_borrows, then there is interest
# interest = amount + total_repayments - total_borrows
# make repay amounts negative if it is to us, and borrows if they are from us
def handle_tx(_tx, _temp_borrows, _temp_repayments, _temp_interest):
    if _tx.action in ['repay','repayETH']:
        row = handle_repay(_tx, _temp_borrows, _temp_repayments, _temp_interest)
    if _tx.action in ['borrow','borrowETH']:
        row = handle_borrow(_tx, _temp_borrows, _temp_repayments, _temp_interest)

    return row


# handle a borrow
def handle_borrow(_tx, _temp_borrows, _temp_repayments, _temp_interest):
    # check if amount is to us or from us
    amount = _tx.amount_fixed
    if _tx.transferTo == _tx.wallet:
        amount = amount
    elif _tx['from'] == _tx.wallet:
        # if the amount is from us, then there was an overpayment
        amount = -amount
    else:
        raise Exception(f"transfer is neither to nor from one of our wallets: {_tx.hash}")

    new_borrows = _temp_borrows + amount
    row = pd.DataFrame(
        [
            {
                "tokenSymbol": _tx.tokenSymbol,
                "hash": _tx.hash,
                "datetime": _tx.datetime,
                "action": "borrow",
                "transfer_from": _tx['from'],
                "transfer_to": _tx.transferTo,
                "amount": amount,
                "total_borrows": new_borrows,
                "total_repayments": _temp_repayments,
                "total_interest": _temp_interest,
                "wallet": _tx.wallet,
                "wallet_name": _tx.wallet_name,
                "pool": _tx.pool,
                "chain": _tx.chain,
            },
        ]
    )

    return row, new_borrows, _temp_repayments, _temp_interest


# handle a repayment
def handle_repay(_tx, _temp_borrows, _temp_repayments, _temp_interest):
    # check if amount is to us or from us
    amount = _tx.amount_fixed
    if _tx['from'] == _tx.wallet:
        amount = amount
    elif _tx.transferTo == _tx.wallet:
        # if the amount is to us, then there was an overpayment -> make amount negative
        amount = -amount
    else:
        raise Exception(f"transfer is neither to nor from one of our wallets: {_tx.hash}")

    if _temp_repayments < _temp_borrows: # there is principal
        if _temp_repayments + amount > _temp_borrows + _temp_interest: # there is also interest
            principal_amount = _temp_borrows + _temp_interest - _temp_repayments  # principal amount is amount up to total borrows
            interest_amount = amount - principal_amount # interest amount is the rest
            
            row = pd.DataFrame(
                [
                    # principal
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "repay_principal",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount,
                        "total_interest": _temp_interest,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                    # interest
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "repay_interest",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount + interest_amount,
                        "total_interest": _temp_interest + interest_amount,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )
        else:
            principal_amount = amount
            interest_amount = 0
            row = pd.DataFrame(
                [
                    # principal
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "repay_principal",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount,
                        "total_interest": _temp_interest,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )
    else:
        if _temp_repayments + amount > _temp_borrows: # only interest
            interest_amount = amount
            row = pd.DataFrame(
                [
                    # interest
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "repay_interest",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + interest_amount,
                        "total_interest": _temp_interest + interest_amount,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )
        else: # interest and principal
            interest_amount = _temp_borrows - _temp_repayments # interest amount is amount down to total borrows
            principal_amount = amount - interest_amount # principal amount is the rest
            
            row = pd.DataFrame(
                [
                    # principal
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "repay_principal",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount,
                        "total_interest": _temp_interest,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                    # interest
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "repay_interest",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + interest_amount,
                        "total_interest": _temp_interest + interest_amount,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )

    return row, _temp_borrows, _temp_repayments + amount, _temp_interest + interest_amount


def get_borrows_and_repayments(_pool_transfers):
    borrows_and_repayments = _pool_transfers[_pool_transfers.action.isin(['borrow','borrowETH','repay','repayETH'])].copy()
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
                temp_interest = 0
                # loop over all the transactions for this token
                for _, tx in temp_txs.iterrows():
                    row, temp_borrows, temp_repayments, temp_interest = handle_tx(tx, temp_borrows, temp_repayments, temp_interest)

                    balances = pd.concat([balances, row])

    return balances.sort_values(by=['wallet','chain','pool','tokenSymbol','datetime'])


# given a dataframe of running totals for borrows and repayments
# split out repayments that paid interest into multiple transactions
# TODO: Fix for updated borrow and repayment logic
def get_split_interest_txs_borrows(_borrows_and_repayments):
    split_txs = _borrows_and_repayments[_borrows_and_repayments.action.isin(['repay','repayETH'])].copy()
    split_txs.reset_index(drop=True, inplace=True)
    return split_txs


# combine borrowing and deposit split transactions together
def merge_split_txs(split_txs_borrows, split_txs_deposits):
    split_txs = pd.concat([split_txs_borrows, split_txs_deposits]).copy()
    split_txs.reset_index(drop=True, inplace=True)

    variable_debt_txs = split_txs[
        (split_txs['tokenSymbol'].str.contains("variableDebt"))&
        (split_txs['action'] == 'repay_interest')
    ].copy()
    variable_debt_txs.reset_index(drop=True, inplace=True)
    variable_debt_txs['action'] = 'dummy_income'

    a_txs = split_txs[
        (split_txs['tokenSymbol'].isin(deposit_tokens))&
        (split_txs['action'] == 'withdraw_interest')
    ].copy()
    a_txs.reset_index(drop=True, inplace=True)
    a_txs['action'] = "dummy_income"

    split_txs = pd.concat([split_txs, variable_debt_txs, a_txs]).copy()
    split_txs.reset_index(drop=True, inplace=True)
    return split_txs


# split txs by accounts between wallets and chain to match lukka system for accounts
def split_txs_by_account(split_txs):
    wallets = split_txs.wallet_name.unique()
    chains = split_txs.chain.unique()
    total_txs_check = 0
    total_txs = len(split_txs)

    for wallet_name in wallets:
        for chain in chains:
            these_txs = split_txs[(split_txs['wallet_name'] == wallet_name) & (split_txs['chain'] == chain)]
            if not these_txs.empty:
                these_txs.to_csv(f'output_files/split_txs/{wallet_name}_{chain}.csv', index=False)
                total_txs_check += len(these_txs)

    assert total_txs_check == total_txs


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

    borrows_and_repayments.to_csv("output_files/lending/borrows_and_repayments.csv", index=False)
    split_txs.to_csv("output_files/split_txs/split_txs_borrows.csv", index=False)

