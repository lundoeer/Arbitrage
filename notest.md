chekc that bids match for slippage and threshold is correctly setup

Add task updating threshold changes based on time left of the market

I need to look more into the current order types and general kalshi trading

so kalshi selling is a little odd, because it switches side, so the above is actually selling yes, can you handle that, so kalshi selling side is switched. including it is using switched side price

I think the solution is to do a state and only buying. The stats is investment and then cover until the investment is covered, then back to investment. with a check for time left

cover is based on positions after investment. with the goal of

as markets near closed cover should become more important than profit.

kalshi and polymarket works differently read positions correctly polymarket you need to net, kalshi auto nets for you. the trigger from cover back to investment is a minimum net investment.

Additionally next step is to look for troubling markets and especially troubeling uncovered market endings.

25-02-26:2000 - 26.02.26:9000 test med sen markeds start 180 sekunder. worked very well trade errors and good prices. Look into if trades were
26.02.26:9000 - ? test med markeds start 60 sekunder - default 120 seconds depth of book may be an issue

also sizing with depth 2 should be tried out

and better logging of resolved trading prices

maybe create a new branch for full refactor

splitting the trade_log up into 2 files one for trades and another for market_outcomes. genrally improve post marketpair profit background and error analysis
