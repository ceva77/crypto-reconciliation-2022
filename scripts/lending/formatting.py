import pandas as pd

from scripts.transactions import merge_transactions_and_token_transfers
from scripts.utils import CHAINS, POOL_LIST

normal_transactions = pd.read_csv("output_files/normal_transactions.csv")
token_transfers = pd.read_csv("output_files/token_transfers.csv")
all_transfers = merge_transactions_and_token_transfers(normal_transactions, token_transfers)


map_action_to_type = {
    "withdraw_principal": "Deposit",
    "withdraw_interest": "Income",
    "repay_principal": "Withdraw",
    "repay_interest": "Expense",
    "dummy_income": "Income",
    "gas_fee": "Expense"
}

map_asset_to_assetcode = {
    "MATIC": "MATIC1",
    "ETH": "ETH",
    
}



def _handle_repay_interest(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Expense",
                "sub_type": "Interest",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": "",
                "transaction_id": "",
                "order_id": "",
                "from_address": _tx.from_address,
                "to_address": _tx.to_address,
                "contract_address": "",
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": "_repay_interest",
                "tags": "",
            }

        ]
    )

    return row


def _handle_repay_principal(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Withdraw",
                "sub_type": "Crypto Loan Out",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": "",
                "transaction_id": "",
                "order_id": "",
                "from_address": _tx.from_address,
                "to_address": _tx.to_address,
                "contract_address": _tx.pool,
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": "_repay_principal",
                "tags": "",
            }

        ]
    )

    return row


def _handle_withdraw_interest(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Income",
                "sub_type": "Interest",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": "",
                "transaction_id": "",
                "order_id": "",
                "from_address": _tx.from_address,
                "to_address": _tx.to_address,
                "contract_address": _tx.pool,
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": "_withdraw_interest",
                "tags": "",
            }

        ]
    )

    return row


def _handle_withdraw_principal(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Deposit",
                "sub_type": "Crypto Loan In",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": "",
                "transaction_id": "",
                "order_id": "",
                "from_address": _tx.from_address,
                "to_address": _tx.to_address,
                "contract_address": _tx.pool,
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": "_withdraw_principal",
                "tags": "",
            }

        ]
    )

    return row


def _handle_dummy_income(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Income",
                "sub_type": "",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": "",
                "transaction_id": "",
                "order_id": "",
                "from_address": _tx.from_address,
                "to_address": _tx.to_address,
                "contract_address": _tx.pool,
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": "_dummy_income",
                "tags": "",
            }

        ]
    )

    return row


def _handle_gas(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Expense",
                "sub_type": "",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.tokenSymbol],
                "amount": 0,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": map_asset_to_assetcode[_tx.tokenSymbol],
                "fee_asset_amount": _tx.amount,
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": "",
                "transaction_id": "",
                "order_id": "",
                "from_address": _tx.from_address,
                "to_address": _tx.to_address,
                "contract_address": _tx.pool,
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": "_repay_interest",
                "tags": "",
            }

        ]
    )

    return row


# format the split transactions dataframe to have the right columns for uploading to Lukka
def format_split_txs(split_txs):
    split_txs_columns = [
        "tokenSymbol", 
        "hash", 
        "datetime", 
        "action", 
        "amount", 
        "total_borrows", 
        "total_repayments", 
        "total_interest_paid", 
        "wallet", 
        "wallet_name", 
        "pool", 
        "chain",
    ]    

    formatted_txs = pd.DataFrame()

    for _, tx in split_txs.iterrows():
        if tx.action == 'repay_interest':
            row = _handle_repay_interest(tx)
        elif tx.action == 'repay_principal':
            row = _handle_repay_principal(tx)
        elif tx.action == 'withdraw_interest':
            row = _handle_withdraw_interest(tx)
        elif tx.action == 'withdraw_principal':
            row = _handle_withdraw_principal(tx)
        elif tx.action == 'dummy_income':
            row = _handle_dummy_income(tx)
        elif tx.action == 'gas_fee':
            row = _handle_gas(tx)
        else:
            raise Exception(f"Transaction action {tx.action} not recognized")

    lukka_columns = [
        "type", 
        # idk what to do with this
        "sub_type", 
        "ref_data_exchange", 
        # need asset code mapping
        "asset", 
        # done
        "amount",
        # n/aj
        "counter_asset_code", 
        "counter_asset_amount", 
        # 0 for everything except for gas fees
        "fee_asset_code", 
        "fee_asset_amount", 
        # n/a
        "rebate_asset_code", 
        "rebate_asset_amount",
        # not surej
        "rate",
        "txn_complete_ts",
        "transaction_id",
        "order_id",
        # from
        "from_address",
        # transfer_to
        "to_address",
        # pool
        "contract_address",
        # hash
        "blockchain_transaction_id",
        # wallet
        "blockchain_address",
        # n/a
        "counterparty",
        # action
        "notes",
        # none
        "tags",
    ]

    # need to convert the columns from split_txs to the columns for lukka
    # but don't have a way to figure out what to put in the columns for lukka
