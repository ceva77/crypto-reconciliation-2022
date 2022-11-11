# Crypto reconciliation 2022

The overall project is to speed up the reconciliation process of our past transactions to the Lukka accounting system. We want to add tax treatments for all of our past treatments, many of which are not being successfully labeled and dealt within the Lukka system. This solution is to use a combination of block explorer scraping with some Python scripts to ease our process for some difficult cases.

Notably, this project deals with lending, particularly handling the separation of interest and principal both for depositing collateral and on loans taken on, as well as tracking firm internal transactions, that once identified, can be easily labeled as non-taxable events.

## Lending

This folder is used for tracking loan repayments on DeFi protocols for different wallets

We have an existing problem which is that it is very hard to determine if a given loan repayment on Etherscan is for interest, principal or both.

The hope of this project is to be able to parse our transactions programmatically to determine how much we've borrowed, how much we've repaid, and then determine how much interest expense we have vs. how much principal pay down we've done.

We use the Etherscan API to programmatically inspect, filter, and sort our transaction history. Importantly, we've developed functions to track all ERC20 transfers in and out of our wallets, which allows us to look for all sorts of transactions across EVM chains. We can then filter these by contracts that we've interacted with, making it possible to see all transactions with a given lending pool.

## Internal Transfers

We used the Etherscan API (and the like for other EVM-chains) in order to track and label internal transactions (raw ETH sends and ERC20 transfers between our various wallets). Largely, this process is complete for now as we've successfully identified and labeled within the Lukka system all of our internal transfers. This was an issue as the Lukka system does not automatically to / from fields for an arbitrary transaction, and therefore was unable to mark when we sent funds between our own wallets.

