import pandas as pd

from src.lending.formatting import format_split_txs 

deposits_and_borrows = pd.read_csv("output_files/lending/deposits_and_borrows.csv")


# helper function to extract out all the gas fees for the filtered transactions 
def _find_gas_fees(_deposits_and_borrows, _filtered_df):
    gas_fees = _deposits_and_borrows[
        (_deposits_and_borrows.action == 'gas_fee') &
        (_deposits_and_borrows.hash.isin(_filtered_df.hash.unique().tolist()))
    ].copy()

    return gas_fees
    

# return deposits and withdrawals only from the deposits and borrows
def filter_deposits_and_withdrawals(_deposits_and_borrows):
    tx_types = ['deposit','withdraw_interest','withdraw_principal']

    deposits_and_withdrawals = _deposits_and_borrows[_deposits_and_borrows.action.isin(tx_types)].copy()
    deposits_and_withdrawals.reset_index(drop=True, inplace=True)

    gas_fees = _find_gas_fees(_deposits_and_borrows, deposits_and_withdrawals)
    deposits_and_withdrawals = pd.concat([deposits_and_withdrawals, gas_fees])

    return deposits_and_withdrawals

    
# return borrows and repayments only from the deposits and borrows
def filter_borrows_and_repayments(_deposits_and_borrows):
    tx_types = ['borrow','repay_interest','repay_principal']

    borrows_and_repayments = _deposits_and_borrows[_deposits_and_borrows.action.isin(tx_types)].copy()
    borrows_and_repayments.reset_index(drop=True, inplace=True)

    gas_fees = _find_gas_fees(_deposits_and_borrows, borrows_and_repayments)
    borrows_and_repayments = pd.concat([borrows_and_repayments, gas_fees])

    return borrows_and_repayments


def filter_split_txs(_deposits_and_borrows):
    # only transactions with these actions need to be edited
    tx_types = [
        'repay_principal','repay_interest','dummy_income','withdraw_principal','withdraw_interest'
    ]
    split_txs = _deposits_and_borrows[_deposits_and_borrows.action.isin(tx_types)].copy()

    # need to be able to find the gas fees for just these txs
    gas_fees = _find_gas_fees(_deposits_and_borrows, split_txs)
    split_txs = pd.concat([split_txs, gas_fees])

    return split_txs


# split txs by accounts between wallets and chain to match lukka system for accounts
def print_txs_by_account(_deposits_and_borrows):
    split_txs = filter_split_txs(_deposits_and_borrows)

    # get all wallets and chains
    wallets = split_txs.wallet_name.unique()
    chains = split_txs.chain.unique()

    total_txs_check = 0
    total_txs = len(split_txs)

    hash_list = pd.DataFrame()
    for wallet_name in wallets:
        for chain in chains:
            # filter frame for this wallet and chain only
            these_txs = split_txs[
                (split_txs['wallet_name'] == wallet_name) & 
                (split_txs['chain'] == chain)
            ]
            # output txs to separate csv if the filter is non-empty
            if not these_txs.empty:
                these_hashes = these_txs[~these_txs.hash.str.contains("-dummy")].hash.unique().tolist()
                i = 0
                group_hashes = []
                while i < len(these_hashes):
                    group_hashes += [",".join(these_hashes[i:i+20])]
                    i += 20

                temp_hash_list = pd.DataFrame({f"{wallet_name} {chain}": group_hashes})
                hash_list = pd.concat([hash_list, temp_hash_list])

                these_txs = format_split_txs(these_txs)
                these_txs.to_csv(f'output_files/split_txs/{wallet_name}_{chain}.csv', index=False)
                total_txs_check += len(these_txs)

    hash_list.to_csv("output_files/split_txs/hash_list.csv", index=False)

    # transactions added must equal total transactions in starting dataframe
    assert total_txs_check == total_txs


def main():
    print_txs_by_account(deposits_and_borrows)


if __name__ == "__main__":
    main()
