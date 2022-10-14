import pandas as pd
import random

from src.utils import WALLET_LIST


map_action_to_type = {
    "withdraw_principal": "Deposit",
    "withdraw_interest": "Income",
    "repay_principal": "Withdrawal",
    "repay_interest": "Expense",
    "dummy_income": "Income",
    "gas_fee": "Expense"
}

map_asset_to_assetcode = {
    'polygon': 
        {
            'AAVE': 'AAVE1', 
            'DAI': 'DAI2', 
            'MATIC': 'MATIC1', 
            'SUSHI': 'SUSHI3', 
            'USDC': 'USDC1', 
            'USDT': 'USDT1', 
            'WBTC': 'WBTC2', 
            'WETH': 'WETH2', 
            'aPolSUSHI': 'APOLSUSHI', 
            'amAAVE': 'AMAAVE', 
            'amDAI': 'AMDAI', 
            'amUSDC': 'AMUSDC', 
            'amUSDT': 'AMUSDT', 
            'amWBTC': 'AMWBTC', 
            'amWETH': 'AMWETH', 
            'amWMATIC': 'AMWMATIC', 
            'variableDebtmUSDC': 'VARIABLEDEBTMUSDC', 
            'variableDebtmUSDT': 'VARIABLEDEBTMUSDT', 
            'variableDebtmWMATIC': 'VARIABLEDEBTMWMATIC'
        }, 
    'avalanche': 
        {
            'AVAX': 'AVAX', 
            'bAVAX': 'BAVAX', 
            'variableDebtgAVAX': 'VARIABLEDEBTGAVAX'
        },
    'mainnet': 
        {
            'CRV': 'CRV', 
            'CVX': 'CVX', 
            'ETH': 'ETH', 
            'LINK': 'LINK', 
            'USDC': 'USDC', 
            'USDT': 'USDT', 
            'YFI': 'YFI', 
            'aCRV': 'ACRV', 
            'aCVX': 'ACVX', 
            'aLINK': 'ALINK1', 
            'aUSDC': 'AUSDC1', 
            'aUSDT': 'AUSDT1', 
            'aXSUSHI': 'AXSUSHI', 
            'aYFI': 'AYFI1', 
            'sUSD': 'NUSD', 
            'variableDebtCRV': 'VARIABLEDEBTCRV', 
            'variableDebtSUSD': 'VARIABLEDEBTSUSD', 
            'variableDebtUSDT': 'VARIABLEDEBTUSDT', 
            'xSUSHI': 'XSUSHI'
        }, 
    'fantom': 
        {
            'FTM': 'FTM1', 
            'gFTM': 'GFTM'
        }
}


def _is_tx_in(_tx) -> bool:
    return _tx.transfer_to in WALLET_LIST


def _make_row(_tx, _type, _sub_type, _notes):
    
    row = pd.DataFrame(
        [
            {
                "type": _type,
                "sub_type": _sub_type,
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.chain][_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": _tx.datetime.replace(" ", "T"),
                "transaction_id": f"{_tx.hash}-{random.random()}",
                "order_id": "",
                "from_address": _tx.transfer_from,
                "to_address": _tx.transfer_to,
                "contract_address": _tx.pool,
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": _notes,
                "tags": "",
            }
        ]
    )

    return row



def _handle_borrow(_tx):
    type_ = "Deposit" if _is_tx_in(_tx) else "Withdrawal"
    sub_type = "Crypto Loan In" if _is_tx_in(_tx) else "Crypto Loan Out"
    notes = "_borrow"

    return _make_row(_tx, type_, sub_type, notes)


def _handle_deposit(_tx):
    type_ = "Deposit" if _is_tx_in(_tx) else "Withdrawal"
    sub_type = "Crypto Loan In" if _is_tx_in(_tx) else "Crypto Loan Out"
    notes = "_lending_deposit"
    
    return _make_row(_tx, type_, sub_type, notes)


def _handle_repay_interest(_tx):
    type_ = "Income" if _is_tx_in(_tx) else "Expense"
    sub_type = "Interest"
    notes = "_repay_interest"

    return _make_row(_tx, type_, sub_type, notes)


def _handle_repay_principal(_tx):
    if _is_tx_in(_tx):
        type_ = "Deposit"
        sub_type = "Crypto Loan In"
    else:
        type_ = "Withdrawal"
        sub_type = "Crypto Loan Out"
    notes = "_repay_principal"

    return _make_row(_tx, type_, sub_type, notes)


def _handle_withdraw_interest(_tx):
    type_ = "Income" if _is_tx_in(_tx) else "Expense"
    sub_type = "Interest"
    notes = "_withdraw_interest"

    return _make_row(_tx, type_, sub_type, notes)


def _handle_withdraw_principal(_tx):
    if _is_tx_in(_tx):
        type_ = "Deposit"
        sub_type = "Crypto Loan In"
    else:
        type_ = "Withdrawal"
        sub_type = "Crypto Loan Out"
    notes = "_withdraw_principal"

    return _make_row(_tx, type_, sub_type, notes)


def _handle_dummy_income(_tx):
    type_ = "Income"
    sub_type = "Interest"
    notes = "_dummy_income"

    return _make_row(_tx, type_, sub_type, notes)


def _handle_gas(_tx):
    return pd.DataFrame(
        [
            {
                "type": "Expense",
                "sub_type": "Other",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.chain][_tx.tokenSymbol],
                "amount": 0,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": map_asset_to_assetcode[_tx.chain][_tx.tokenSymbol],
                "fee_asset_amount": _tx.amount,
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": _tx.datetime.replace(" ", "T"),
                "transaction_id": f"{_tx.hash}-{random.random()}",
                "order_id": "",
                "from_address": _tx.transfer_from,
                "to_address": _tx.transfer_to,
                "contract_address": _tx.pool,
                "blockchain_transaction_id": _tx.hash,
                "blockchain_address": _tx.wallet,
                "counterparty": "",
                "notes": "gas",
                "tags": "",
            }
        ]
    )


def _handle_tx(_tx):
    if _tx.action == 'deposit':
        row = _handle_deposit(_tx)
    elif _tx.action == 'borrow':
        row = _handle_borrow(_tx)
    elif _tx.action == 'repay_interest':
        row = _handle_repay_interest(_tx)
    elif _tx.action == 'repay_principal':
        row = _handle_repay_principal(_tx)
    elif _tx.action == 'withdraw_interest':
        row = _handle_withdraw_interest(_tx)
    elif _tx.action == 'withdraw_principal':
        row = _handle_withdraw_principal(_tx)
    elif _tx.action == 'dummy_income':
        row = _handle_dummy_income(_tx)
    elif _tx.action == 'gas_fee':
        row = _handle_gas(_tx)
    else:
        raise Exception(f"Transaction action {_tx.action} not recognized")

    return row


# format the split transactions dataframe to have the right columns for uploading to Lukka
def format_split_txs(split_txs):
    formatted_txs = pd.DataFrame()

    for _, tx in split_txs.iterrows(): 
        row = _handle_tx(tx)
        
        formatted_txs = pd.concat([formatted_txs, row])

    formatted_txs.reset_index(drop=True, inplace=True)
    return formatted_txs.copy()
