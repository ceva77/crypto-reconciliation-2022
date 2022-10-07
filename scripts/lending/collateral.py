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

pool_transfers = pd.read_csv("output_files/pool_transfers.csv") 
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


def _handle_tx(_tx, _temp_deposits, _temp_withdraws, _temp_interest):
    if _tx.action in ['deposit','depositETH','supply', 'supplyETH']:
        row = _handle_deposit(_tx, _temp_deposits, _temp_withdraws, _temp_interest)
    elif _tx.action in ['withdraw','withdrawETH']:
        row = _handle_withdraw(_tx, _temp_deposits, _temp_withdraws, _temp_interest)

    return row


def _handle_deposit(_tx, _temp_deposits, _temp_withdraws, _temp_interest):
    # check if amount is to us or from us
    amount = _tx.amount_fixed
    if _tx['from'] == _tx.wallet:
        amount = amount
    elif _tx.transferTo == _tx.wallet:
        amount = -amount
    else:
        raise Exception(f"transfer is neither to nor from one of our wallets")
    if _tx.tokenSymbol in deposit_tokens:
        amount = -amount

    new_deposits = _temp_deposits + amount
    row = pd.DataFrame(
        [
            {
                "tokenSymbol": _tx.tokenSymbol,
                "hash": _tx.hash,
                "datetime": _tx.datetime,
                "action": "deposit",
                "transfer_from": _tx['from'],
                "transfer_to": _tx.transferTo,
                "amount": amount,
                "total_deposits": new_deposits,
                "total_withdraws": _temp_withdraws,
                "total_interest": _temp_interest,
                "wallet": _tx.wallet,
                "wallet_name": _tx.wallet_name,
                "pool": _tx.pool,
                "chain": _tx.chain,
            },
        ]
    )

    return row, new_deposits, _temp_withdraws, _temp_interest


def _handle_withdraw(_tx, _temp_deposits, _temp_withdraws, _temp_interest):
    amount = _tx.amount_fixed

    # TODO: we need a way to deal with a tokens since they are moving in 
    # the opposite direction as the main tokens
    if _tx.transferTo == _tx.wallet:
        amount = amount
    elif _tx['from'] == _tx.wallet:
        amount = -amount 
    else:
        raise Exception(f"transfer is neither to nor from one of our wallets {_tx.hash}")
    if _tx.tokenSymbol in deposit_tokens:
        amount = -amount

    if _temp_withdraws < _temp_deposits: # there is principal
        if _temp_withdraws + amount > _temp_deposits + _temp_interest: # also interest
            principal_amount = _temp_deposits + _temp_interest - _temp_withdraws
            interest_amount = amount - principal_amount
            
            row = pd.DataFrame(
                [
                    # principal
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "withdraw_principal",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount,
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
                        "action": "withdraw_interest",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount + interest_amount,
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
                        "action": "withdraw_principal",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount,
                        "total_interest": _temp_interest,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )
    else:
        if _temp_withdraws + amount > _temp_deposits: # only interest
            interest_amount = amount
            row = pd.DataFrame(
                [
                    # interest
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "withdraw_interest",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + interest_amount,
                        "total_interest": _temp_interest + interest_amount,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )
        else: # interest and principal
            interest_amount = _temp_deposits - _temp_withdraws # interest amount is amount down to total borrows
            principal_amount = amount - interest_amount # principal amount is the rest
            
            row = pd.DataFrame(
                [
                    # principal
                    {
                        "tokenSymbol": _tx.tokenSymbol,
                        "hash": _tx.hash,
                        "datetime": _tx.datetime,
                        "action": "withdraw_principal",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount,
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
                        "action": "withdraw_interest",
                        "transfer_from": _tx['from'],
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + interest_amount,
                        "total_interest": _temp_interest + interest_amount,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )

    return row, _temp_deposits, _temp_withdraws + amount, _temp_interest + interest_amount

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
                temp_interest = 0
                # loop over all the transactions for this token
                for _, tx in temp_txs.iterrows():
                    # increment deposits, withdrawals, and accrued interest
                    row, temp_deposits, temp_withdraws, temp_interest = _handle_tx(tx, temp_deposits, temp_withdraws, temp_interest)
                    balances = pd.concat([balances, row])

    return balances.sort_values(by=['wallet','chain','pool','tokenSymbol','datetime'])


# given a dataframe of running totals for deposits and withdrawals
# split out withdraws that received interest into multiple transactions
def get_split_interest_txs_collateral(_deposits_and_withdrawals):
    split_txs = _deposits_and_withdrawals[_deposits_and_withdrawals.action.isin(['withdraw','withdrawETH'])]
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
