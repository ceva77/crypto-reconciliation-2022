import pandas as pd


map_action_to_type = {
    "withdraw_principal": "Deposit",
    "withdraw_interest": "Income",
    "repay_principal": "Withdraw",
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


def _handle_repay_interest(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Expense",
                "sub_type": "Interest",
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
                "transaction_id": _tx.hash,
                "order_id": "",
                "from_address": _tx.transfer_from,
                "to_address": _tx.transfer_to,
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


def _handle_repay_principal(_tx):
    row = pd.DataFrame(
        [
            {
                "type": "Withdraw",
                "sub_type": "Crypto Loan Out",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.chain][_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": _tx.datetime.replace(" ", "T"),
                "transaction_id": _tx.hash,
                "order_id": "",
                "from_address": _tx.transfer_from,
                "to_address": _tx.transfer_to,
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
                "asset": map_asset_to_assetcode[_tx.chain][_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": _tx.datetime.replace(" ", "T"),
                "transaction_id": _tx.hash,
                "order_id": "",
                "from_address": _tx.transfer_from,
                "to_address": _tx.transfer_to,
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
                "asset": map_asset_to_assetcode[_tx.chain][_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": _tx.datetime.replace(" ", "T"),
                "transaction_id": _tx.hash,
                "order_id": "",
                "from_address": _tx.transfer_from,
                "to_address": _tx.transfer_to,
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
                "sub_type": "Interest",
                "ref_data_exchange": "",
                "asset": map_asset_to_assetcode[_tx.chain][_tx.tokenSymbol],
                "amount": _tx.amount,
                "counter_asset_code": "",
                "counter_asset_amount": "",
                "fee_asset_code": "",
                "fee_asset_amount": "", 
                "rebate_asset_code": "",
                "rebate_asset_amount": "",
                "rate": "",
                "txn_complete_ts": _tx.datetime.replace(" ", "T"),
                "transaction_id": _tx.hash,
                "order_id": "",
                "from_address": _tx.transfer_from,
                "to_address": _tx.transfer_to,
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
                "transaction_id": _tx.hash,
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

    return row


# format the split transactions dataframe to have the right columns for uploading to Lukka
def format_split_txs(split_txs):
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
        
        formatted_txs = pd.concat([formatted_txs, row])

    formatted_txs.reset_index(drop=True, inplace=True)
    return formatted_txs.copy()
