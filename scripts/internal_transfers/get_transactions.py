import pandas as pd
import requests

try:
    from scripts.utils import *
except ModuleNotFoundError:
    from utils import *


# download transactions from block explorer api for a given wallet and chain
def get_transactions(wallet, chain="mainnet", start_block=0, end_block=99999999):
    if chain == "harmony":
        return pd.DataFrame()

    explorer_url = CHAINS[chain]["explorer_url"]
    explorer_token = CHAINS[chain]["explorer_token"]

    url = (
        "{}?module=account&action=txlist&address={}&startblock={}&endblock={}&apikey={}"
    )
    url = url.format(explorer_url, wallet, start_block, end_block, explorer_token)

    response = requests.get(url).json()

    return pd.DataFrame(response["result"])


# download transactions from block explorer api for a given wallet on all chains
def get_transactions_for_all_chains(
    wallet, chains=CHAIN_LIST, start_block=0, end_block=99999999
):
    df = pd.DataFrame()

    # loop over all chains and concatenate results
    for chain in chains:
        temp = get_transactions(
            wallet, chain=chain, start_block=start_block, end_block=end_block
        )
        temp["chain"] = chain

        df = pd.concat([df, temp])

    return df


def get_transactions_for_all_wallets_and_chains(
    wallets=WALLET_LIST, chains=CHAIN_LIST, start_block=0, end_block=99999999
):
    df = pd.DataFrame()

    # loop over all wallets and concatenate results
    for wallet in wallets:
        temp = get_transactions_for_all_chains(
            wallet, chains, start_block=start_block, end_block=end_block
        )
        temp["wallet"] = wallet
        temp["wallet_name"] = wallet_address_to_name[wallet.lower()]

        df = pd.concat([df, temp])

    return df


# get all transactions for all wallets and chains
def main(verbose=False):
    wallets = WALLET_LIST
    chains = CHAIN_LIST
    start_block = 0
    end_block = 99999999

    df = get_transactions_for_all_wallets_and_chains(
        wallets, chains, start_block, end_block
    ).drop_duplicates(subset=["hash"])

    if verbose:
        print(df)
    return df


if __name__ == "__main__":
    _ = main(verbose=True)
