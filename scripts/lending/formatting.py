import pandas as pd

from scripts.transactions import merge_transactions_and_token_transfers
from scripts.utils import CHAINS, POOL_LIST

normal_transactions = pd.read_csv("output_files/normal_transactions.csv")
token_transfers = pd.read_csv("output_files/token_transfers.csv")
all_transfers = merge_transactions_and_token_transfers(normal_transactions, token_transfers)


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

    lukka_columns = [
        "type", 
        "sub_type", 
        "ref_data_exchange", 
        "asset", 
        "amount",
        "counter_asset_code", 
        "counter_asset_amount", 
        "fee_asset_code", 
        "fee_asset_amount", 
        "rebate_asset_code", 
        "rebate_asset_amount",
        "rate",
        "txn_complete_ts",
        "transaction_id",
        "order_id",
        "from_address",
        "to_address",
        "contract_address",
        "blockchain_transaction_id",
        "blockchain_address",
        "counterparty",
        "notes",
        "tags",
    ]

    # need to convert the columns from split_txs to the columns for lukka
    # but don't have a way to figure out what to put in the columns for lukka


    


