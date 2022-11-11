# Establishing and identifying a cost-basis

Once we have identified tax-treatments for all token transfers, we are prepared to identify cost-bases and establish lots for all token acquisitions. 

The difficulty is that we need to keep a running list of existing lots at each point in time in order to determine the gain/loss reported on the disposal of an asset. 

We need the following information about any acquired asset:

- Asset id (some combination of token symbol, contract address, chain id)
- Timestamp of acquisition (easily known from transfer)
- Price (to establish the cost basis)
- Quantity

Choosing the right data-structure to maintain this information and allow us to perform operations on it efficiently is difficult. 

We need to be able to:

- Quickly time-sort lots for any asset (in order to do FIFO, LIFO style accounting)
- Quickly price-sort lots for any asset (HIFO and spec-id):    

Probably the simplest solution is a table:

Columns are [asset_id, timestamp, amount, cost_basis, amount_disposed]

The amount_disposed simply counts the amount of the asset that has been disposed of already. This way we can easily see when a given lot has been fully disposed (i.e. amount == amount_disposed)



