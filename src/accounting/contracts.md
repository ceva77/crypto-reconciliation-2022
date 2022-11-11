# Contract style Accounting

Proposal - Our idea is to try accounting DeFi transactions according to the following rough outline:

1. Gather and format all token transfers from etherscan-like APIs. 
2. Sort transfers by Contract
3. Process accounting treatments by different contract types 
    - Assign a contract "type" to a given Contract
    - Process all transfers for this contract accordingly
4. Establish cost-basis accounting based on the different treatments to the various transactions

## Issues with this approach

We have ~600 different contracts interacted with. Manually tracking all the categories for each contract address will be cumbersome and challenging. However, it will make it possible to automate repeated transactions with the same contracts over time

## Contract Categories

An important step in this process is determining a sufficiently flexible, yet specific set of contract types in order to make it possible to easily categorize transactions programmatically. We also need a way to manually track token flows in and out of corner-case contracts where perhaps we don't have a template

### LP Contract

Transaction Types:
- Create LP token (swap input tokens for LP) - includes disposal of sold assets and acquisition of LP
- Harvest rewards (income token (perhaps separate from the LP or input tokens, i.e. a DAO token))
- Splitting the LP token is also a trade (sell LP, buy underlying tokens proportionally)

### Rewards Gauge

Transaction types:
- Stake (simply a loan out)
- Harvest (income)
- Withdraw (income and loan in)

### Lending (Aave style)

Types:
- Deposit collateral (loan out)
- Withdraw collateral (loan out + interest income)
- Borrow (loan in)
- Repay (loan out + interest expense)

### ERC20 contract

Basically just direct token transfers - these need manual labeling since OTC activity means a given transaction could be virtually any activity. Though we can identify internal transactions and safely disregard these.


