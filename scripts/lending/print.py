import pandas as pd

deposits_and_borrows = pd.read_csv("output_files/lending/deposits_and_borrows.csv")
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


# return deposits and withdrawals only from the deposits and borrows
def filter_deposits_and_withdrawals(_deposits_and_borrows):
    tx_types = ['deposit','withdraw_interest','withdraw_principal']

    deposits_and_withdrawals = _deposits_and_borrows[_deposits_and_borrows.action.isin(tx_types)].copy()
    deposits_and_withdrawals.reset_index(drop=True, inplace=True)

    return deposits_and_withdrawals

    
# return borrows and repayments only from the deposits and borrows
def filter_borrows_and_repayments(_deposits_and_borrows):
    tx_types = ['borrow','repay_interest','repay_principal']

    borrows_and_repayments = _deposits_and_borrows[_deposits_and_borrows.action.isin(tx_types)].copy()
    borrows_and_repayments.reset_index(drop=True, inplace=True)

    return borrows_and_repayments


def filter_split_txs(_deposits_and_borrows):
    # only transactions with these actions need to be edited
    tx_types = [
        'repay_principal','repay_interest','dummy_income','withdraw_principal','withdraw_interest'
    ]
    split_txs = _deposits_and_borrows[_deposits_and_borrows.action.isin[tx_types]].copy()

    return split_txs

# split txs by accounts between wallets and chain to match lukka system for accounts
def print_txs_by_account(_deposits_and_borrows):
    split_txs = filter_split_txs(_deposits_and_borrows)

    # get all wallets and chains
    wallets = split_txs.wallet_name.unique()
    chains = split_txs.chain.unique()

    total_txs_check = 0
    total_txs = len(split_txs)
    for wallet_name in wallets:
        for chain in chains:
            # filter frame for this wallet and chain only
            these_txs = split_txs[
                (split_txs['wallet_name'] == wallet_name) & 
                (split_txs['chain'] == chain)
            ]
            # output txs to separate csv if the filter is non-empty
            if not these_txs.empty:
                these_txs.to_csv(f'output_files/finals/{wallet_name}_{chain}.csv', index=False)
                total_txs_check += len(these_txs)

    # transactions added must equal total transactions in starting dataframe
    assert total_txs_check == total_txs


def main():
    print_txs_by_account(deposits_and_borrows)



