import pandas as pd

from src.utils import POOL_LIST, CHAINS

all_transfers = pd.read_csv("output_files/all_transfers.csv")
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
action_categories = {
    "deposit": 0,
    "withdraw_principal": 0,
    "withdraw_interest": 0,
    "borrow": 1,
    "repay_principal": 1,
    "repay_interest": 1,
    "dummy_income": 2,
    "gas_fee": 2,
}


def _is_deposit(_tx):
    return _tx.action in ['deposit','depositETH','supply','supplyETH']


def _is_withdraw(_tx):
    return _tx.action in ['withdraw','withdrawETH']


def _is_borrow(_tx):
    return _tx.action in ['borrow','borrowETH']


def _is_repay(_tx):
    return _tx.action in ['repay','repayETH']


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
        return _handle_borrow(_tx, _temp_deposits, _temp_withdraws, _temp_interest)

    new_deposits = _temp_deposits + amount
    row = pd.DataFrame(
        [
            {
                "tokenSymbol": _tx.tokenSymbol,
                "hash": _tx.hash,
                "datetime": _tx.datetime,
                "action": "deposit",
                "from": _tx['from'],
                "transfer_from": _tx.transferFrom,
                "transfer_to": _tx.transferTo,
                "amount": amount,
                "total_deposits": new_deposits,
                "total_withdraws": _temp_withdraws,
                "total_deposit_interest": _temp_interest,
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
        return _handle_repay(_tx, _temp_deposits, _temp_withdraws, _temp_interest)

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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount,
                        "total_deposit_interest": _temp_interest,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount + interest_amount,
                        "total_deposit_interest": _temp_interest + interest_amount,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount,
                        "total_deposit_interest": _temp_interest,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + interest_amount,
                        "total_deposit_interest": _temp_interest + interest_amount,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + principal_amount,
                        "total_deposit_interest": _temp_interest,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_deposits": _temp_deposits,
                        "total_withdraws": _temp_withdraws + interest_amount,
                        "total_deposit_interest": _temp_interest + interest_amount,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )

    return row, _temp_deposits, _temp_withdraws + amount, _temp_interest + interest_amount


# handle a borrow
def _handle_borrow(_tx, _temp_borrows, _temp_repayments, _temp_interest):
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
                "from": _tx['from'],
                "transfer_from": _tx.transferFrom,
                "transfer_to": _tx.transferTo,
                "amount": amount,
                "total_borrows": new_borrows,
                "total_repayments": _temp_repayments,
                "total_borrow_interest": _temp_interest,
                "wallet": _tx.wallet,
                "wallet_name": _tx.wallet_name,
                "pool": _tx.pool,
                "chain": _tx.chain,
            },
        ]
    )

    return row, new_borrows, _temp_repayments, _temp_interest


# handle a repayment
def _handle_repay(_tx, _temp_borrows, _temp_repayments, _temp_interest):
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount,
                        "total_borrow_interest": _temp_interest,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount + interest_amount,
                        "total_borrow_interest": _temp_interest + interest_amount,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount,
                        "total_borrow_interest": _temp_interest,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + interest_amount,
                        "total_borrow_interest": _temp_interest + interest_amount,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": principal_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + principal_amount,
                        "total_borrow_interest": _temp_interest,
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
                        "from": _tx['from'],
                        "transfer_from": _tx.transferFrom,
                        "transfer_to": _tx.transferTo,
                        "amount": interest_amount,
                        "total_borrows": _temp_borrows,
                        "total_repayments": _temp_repayments + interest_amount,
                        "total_borrow_interest": _temp_interest + interest_amount,
                        "wallet": _tx.wallet,
                        "wallet_name": _tx.wallet_name,
                        "pool": _tx.pool,
                        "chain": _tx.chain,
                    },
                ]
            )

    return row, _temp_borrows, _temp_repayments + amount, _temp_interest + interest_amount


def _add_dummy_transactions(_deposits_and_borrows):
    variable_debt_txs = _deposits_and_borrows[
        (_deposits_and_borrows['tokenSymbol'].str.contains("variableDebt"))&
        (_deposits_and_borrows['action'] == 'repay_interest')
    ].copy()
    variable_debt_txs.reset_index(drop=True, inplace=True)
    variable_debt_txs['action'] = 'dummy_income'
    temp = variable_debt_txs['transfer_to']
    variable_debt_txs['transfer_to'] = variable_debt_txs['transfer_from']
    variable_debt_txs['transfer_from'] = temp
    variable_debt_txs['hash'] = [f"{hash}-dummy" for hash in variable_debt_txs.hash]

    a_txs = _deposits_and_borrows[
        (_deposits_and_borrows['tokenSymbol'].isin(deposit_tokens))&
        (_deposits_and_borrows['action'] == 'repay_interest')
    ].copy()
    a_txs.reset_index(drop=True, inplace=True)
    a_txs['action'] = "dummy_income"
    temp = a_txs['transfer_to']
    a_txs['transfer_to'] = a_txs['transfer_from']
    a_txs['transfer_from'] = temp
    a_txs['hash'] = [f"{hash}-dummy" for hash in a_txs.hash]

    _deposits_and_borrows = pd.concat([_deposits_and_borrows, variable_debt_txs, a_txs])
    _deposits_and_borrows.reset_index(drop=True, inplace=True)

    return _deposits_and_borrows


# add tx's for the gas fees
def calc_gas_fees(_pool_transfers):
    # only use one gas fee per transaction hash
    _pool_transfers = _pool_transfers.drop_duplicates(subset=['hash'])
    gas_fees = pd.DataFrame()
    for _, _tx in _pool_transfers.iterrows():
        chain = _tx.chain

        # the gas paid is the price times gas used, adjusted by decimals
        fee_amount = _tx.gasPrice * _tx.gasUsed / (10**CHAINS[chain]['base_token_decimals'])
        row = pd.DataFrame(
            [
                {
                    "tokenSymbol": CHAINS[chain]['base_token_symbol'],
                    "hash": _tx.hash,
                    "datetime": _tx.datetime,
                    "action": "gas_fee",
                    "transfer_from": _tx['from'],
                    "transfer_to": "0x0000000000000000000000000000000000000000", # always send gas to zero address
                    "amount": fee_amount,
                    "from": _tx['from'],
                    "wallet": _tx.wallet,
                    "wallet_name": _tx.wallet_name,
                    "pool": _tx.to,
                    "chain": _tx.chain,
                }
            ]
        )
        gas_fees = pd.concat([gas_fees, row])
        
    return gas_fees



# get transfers with tx going to these pools
def get_pool_transfers(_transfers, _pools):
    pool_transfers = _transfers[_transfers.to.str.lower().isin(_pools)].copy()
    pool_transfers.reset_index(drop=True, inplace=True)

    return pool_transfers


# find, format and calculate for all split transacitons from all transfers for a list of pools 
def get_deposits_and_borrows(_transfers, _pools):
    _pool_transfers = get_pool_transfers(_transfers, _pools)
    gas_fees = calc_gas_fees(_pool_transfers)

    _pool_transfers['pool'] = _pool_transfers.to
    tokens = _pool_transfers.tokenSymbol.unique()
    wallets = _pool_transfers.wallet.unique()
    pools = _pool_transfers.to.unique()

    deposits_and_borrows = gas_fees.copy()
    for wallet in wallets:
        for pool in pools:
            for token in tokens:
                # get the transactions for this token, pool, and wallet
                temp_txs = _pool_transfers[
                    (_pool_transfers.tokenSymbol == token)
                    & (_pool_transfers.to == pool)
                    & (_pool_transfers.wallet == wallet)
                ].sort_values(by="datetime")

                # for each transaction compute the amount in the pool and the interest withdrawn
                temp_deposits = 0
                temp_withdraws = 0
                temp_deposit_interest = 0

                temp_borrows = 0
                temp_repayments = 0
                temp_borrow_interest = 0

                # loop over all the transactions for this token
                for _, tx in temp_txs.iterrows():
                    if _is_deposit(tx):
                        row, temp_deposits, temp_withdraws, temp_deposit_interest = _handle_deposit(
                            tx, 
                            temp_deposits, 
                            temp_withdraws, 
                            temp_deposit_interest
                        )
                    elif _is_borrow(tx):
                        row, temp_borrows, temp_repayments, temp_borrow_interest = _handle_borrow(
                            tx, 
                            temp_borrows, 
                            temp_repayments, 
                            temp_borrow_interest
                        )
                    elif _is_withdraw(tx):
                        row, temp_deposits, temp_withdraws, temp_deposit_interest = _handle_withdraw(
                            tx, 
                            temp_deposits, 
                            temp_withdraws, 
                            temp_deposit_interest
                        )
                    elif _is_repay(tx):
                        row, temp_borrows, temp_repayments, temp_borrow_interest = _handle_repay(
                            tx, 
                            temp_borrows, 
                            temp_repayments, 
                            temp_borrow_interest
                        )


                    deposits_and_borrows = pd.concat([deposits_and_borrows, row])

    # add variable debt transactions and a transactions
    deposits_and_borrows = _add_dummy_transactions(deposits_and_borrows)

    deposits_and_borrows['category'] = [action_categories[action] for action in deposits_and_borrows.action]

    # shift columns to the back
    columns_to_shift = ['wallet','wallet_name','pool','chain','category']
    columns = deposits_and_borrows.columns.tolist()
    for column in columns_to_shift:
        columns.insert(len(columns), columns.pop(columns.index(column)))

    deposits_and_borrows = deposits_and_borrows[columns]


    return deposits_and_borrows.sort_values(by=['wallet','chain','pool','tokenSymbol','category','datetime'])


# get split interest transactions for the sample pool and wallet on polygon
# if running from console, output results to a .csv file
def main(verbose=False):
    deposits_and_borrows = get_deposits_and_borrows(all_transfers, POOL_LIST)

    if verbose == True:
        print(deposits_and_borrows)

    return deposits_and_borrows


if __name__ == "__main__":
    deposits_and_borrows = main(verbose=True)
    deposits_and_borrows.to_csv("output_files/lending/deposits_and_borrows.csv", index=False)
