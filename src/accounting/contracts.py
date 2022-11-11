# keep track of all contracts and their labels through this contract.
import pandas as pd


TRANSFERS = pd.read_csv("output_files/all_transfers.csv")


# return the count of unique contract addresses
def count_contracts(transfers):
    unique_transfers = transfers.contractAddress.unique()

    return len(unique_transfers)


# get information for all the unique contracts
def fetch_contracts(transfers):
    transfers = transfers[['contractAddress', 'chain']].copy()

    transfers = transfers.drop_duplicates(subset=['contractAddress', 'chain'])

    return transfers


# main
def main():
    transfer_count = count_contracts(TRANSFERS)

    print(transfer_count)


if __name__ == "__main__":
    main()
