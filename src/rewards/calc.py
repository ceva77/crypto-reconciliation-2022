from src.utils import *
from src.lending.split import get_pool_transfers, action_categories

all_transfers = pd.read_csv("output_files/all_transfers.csv")

deposit_actions = ['stake','deposit']
withdraw_actions = ['withdraw']

spooky_contracts = ['0x37cf490255082ee50845ea4ff783eb9b6d1622ce']
lithium_contracts = ['0xfcd73006121333c92d770662745146338e419556']
arcadium_contracts = ['0x9dd1fe32aff4060c12e2b42961548876053187c6']
sushiswap_masterchef = ['0xef0881ec094552b2e128cf945ef17a6752b4ec5d']
fiat_dao = ['0x4645d1cF3f4cE59b06008642E74E60e8F80c8b58']

def _is_tx_in(_tx):
    return _tx.transferTo == _tx.wallet


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
                    "original_action": _tx.action,
                }
            ]
        )
        gas_fees = pd.concat([gas_fees, row])
        
    return gas_fees


def _handle_deposit(_tx, _temp_deposits, _temp_withdraws, _temp_interest):
    amount = _tx.amount_fixed

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
                "original_action": _tx.action
            },
        ]
    )

    return row, new_deposits, _temp_withdraws, _temp_interest


def _handle_withdraw(_tx, _temp_deposits, _temp_withdraws, _temp_interest):
    amount = _tx.amount_fixed

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
                        "original_action": _tx.action
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
                        "original_action": _tx.action
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
                        "original_action": _tx.action
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
                        "original_action": _tx.action
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
                        "original_action": _tx.action
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
                        "original_action": _tx.action
                    },
                ]
            )

    return row, _temp_deposits, _temp_withdraws + amount, _temp_interest + interest_amount


def get_contract_transfers(_transfers, _contracts):
    return get_pool_transfers(_transfers, _contracts)


def calc_rewards(_transfers, _contracts):
    contract_transfers = get_contract_transfers(_transfers, _contracts)
    gas_fees = calc_gas_fees(contract_transfers)

    contract_transfers['pool'] = contract_transfers.to
    tokens = contract_transfers.tokenSymbol.unique()
    wallets = contract_transfers.wallet.unique()
    contracts = contract_transfers.to.unique()

    rewards = gas_fees.copy()
    for wallet in wallets:
        for contract in contracts:
            for token in tokens:
                temp_txs = contract_transfers[
                    (contract_transfers.tokenSymbol == token)
                    & (contract_transfers.to == contract)
                    & (contract_transfers.wallet == wallet)
                ].sort_values(by="datetime")

                temp_deposits = 0
                temp_withdraws = 0
                temp_deposit_interest = 0

                for _, tx in temp_txs.iterrows():
                    # first case is if the flow of the token is to our wallet
                    if _is_tx_in(tx): 
                        row, temp_deposits, temp_withdraws, temp_deposit_interest = _handle_withdraw(
                            tx,
                            temp_deposits,
                            temp_withdraws,
                            temp_deposit_interest
                        )
                    elif not _is_tx_in(tx): 
                        row, temp_deposits, temp_withdraws, temp_deposit_interest = _handle_deposit(
                            tx,
                            temp_deposits,
                            temp_withdraws,
                            temp_deposit_interest
                        )
                    else:
                        print(f"Skipping irrelevant action type: {tx.action}")
                        continue

                    rewards = pd.concat([rewards, row])

    rewards['category'] = [action_categories[action] for action in rewards.action]

    columns_to_shift = ['wallet','wallet_name','pool','chain']
    columns = rewards.columns.tolist()
    for column in columns_to_shift:
        columns.insert(len(columns), columns.pop(columns.index(column)))

    rewards = rewards[columns]

    return rewards.sort_values(by=['wallet','chain','pool','tokenSymbol','category','datetime'])


def main(verbose=False):
    print("Hasn't been implemented yet!")


if __name__ == "__main__":
    main()