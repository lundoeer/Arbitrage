# Market discrepancy - estimation and mitigation

# Last updated (27/02/2026)

This document describes the planned implementation of estimation and mitigation of market discrepancy and also documents the current implementation. Discrepancy in this context is a measure of how likely it is that the markets outcomes on the two markets resolves differently. Example: Polymarket resolves Up/yes and kalshi resolves No/down. This can happen because these markets have a different BTC price measures. Currently I have estimated that resolve difference happens around 6,5 pct of the time. The main purpose of this implementation is to identify and handle this risk for each market pair.

## Planned implementation

Market volitility handling will be split into these sections

- estimation of current market pair discrepancy
  - Market analysis
    - Current market pair targets and prices for BTC
    - Ask gaps
    - Book analysis maybe - this is low priority and last if at all

- mitigation of discrepancy
  - Minimize exposure
    - Hedging
    - Participation

### Estiamtion of discrepancy

Estimation of volitility will be specific market pair analyses, that each should help evaluate the discrepancy or how likely the current pair is to resolve different outcomes.

#### Current market pair targets and prices for BTC

Polymarket uses chain link. This is easy to get the current price, and it also seems easy to get target based on scripts implemented already
Kalshi - target invesitgate if the target is registered in the market information after market window has started. It is definitely possible to get target at some point see the compare_resolved script. Kalshi current prices requires more investigation - task

First step of the analysis is to invetigate how the price gap between the two markets evolve, what is a large gap, and how volatile is the gaps. Both venues measure BTC price so while not identical they are almost but this an analysis of this specific gap.

Key outputs on current data
Base rate: 7.4454% different (218/2928 usable rows).
Recommended abs-gap buckets:
low [0,5): 3.8352%
watch [5,10): 6.7029%
elevated [10,50): 7.6705%
high [50,inf): 17.4242%
Added directional signal:
near_parity [-10,10): 5.0955%
pm_above_50+: 15.6863%
kalshi_above_50+: 18.5185%
Best threshold sweep (from script summary):
abs_gap_usd >= 51.1449 (same as your finding)
abs_gap_bps >= 6.9084

what i want is a script that can run every minute and based on targets and prices can calculate the risk of a discrepant outcome. This should then be used for the tolerance of loss in hedging.

#### Ask gaps

The ask gaps between the markets is the source of arbitrage and also an indicator of how high the discrepancy is between the two markets

#### Book analysis

### Mitigation of discrepancy

#### Hedging

#### Participation

## Implementation

### Market analysis

Polymarket chain link: there is setup of api calls to get prices, these scripts use it:

- scripts\diagnostic\get_polymarket_chainlink_window_prices.py
- scripts\diagnostic\compare_resolved_15m_pairs.py

I do not think there are others but please check and add if any.

needs updates regarding current scripts

both targets and prices can be gotten from querying the websites. It is probably necessary for kalshi prices and targets during the market with polymarket chain link is probably the best way to go.

separate market monitoring script that updates resolved pair log, collects price data pr minute, maybe best_asks and best_asks_size. this should run in a separate repo and saved data into the arbitrage project for analysis.
