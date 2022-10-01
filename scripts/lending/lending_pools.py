from scripts.transactions import all_transfers  
from scripts.utils import POOL_LIST


pools = POOL_LIST

# get transfers with tx going to these pools
def get_pool_transfers(_transfers, _pools):
    pool_transfers = _transfers[_transfers.to.str.lower().isin(_pools)].copy()
    pool_transfers.reset_index(drop=True, inplace=True)

    return pool_transfers


def main(verbose=False):
    pool_transfers = get_pool_transfers(all_transfers, pools)

    if verbose:
        print("All transfers:")
        print(pool_transfers)

    return pool_transfers


if __name__ == "__main__":
    main(verbose=True).to_csv("output_files/lending/pool_transfers.csv")


pool_transfers = get_pool_transfers(all_transfers, pools)