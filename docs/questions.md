## Questions from my review 23/02/26

Are these parts of the config used currently when running the enging:

"buy": {
"min_gross_edge_threshold": 0.04,
"max_spend_per_market_usd": 10.0,
"max_size_cap_per_leg": 20.0

}

"buy_execution": {
"api_retry": {
"enabled": true,
"max_attempts": 3,
"base_backoff_seconds": 0.5,
"jitter_ratio": 0.2,
"include_post": true

In buy Decistion in buying this is written:
Engine updates buy execution counters by result status.
I would like to know how this work and specifically what are the cases where

## Additional documentation

Please explain the BuyIdempotencyState() with examples in the buying.md by expanding it.
Please expand buying.md with an explanation of maybe_rearm(...), what does it check and how does it work.
Please explain the engine_loop. What is the function and how is it setup. As a specific part of the explanation what and why exactly is running in async? This should be done in the file: docs\run_scripts_overview.md

## general architecture questions:

Can core functions can be run from tests without argparse?
Is logging configuration called once at startup?
Do modules use logging in a form similar to logging.getLogger(**name**) and don’t configure handlers?
Is the CLI layer mostly doing parsing, validation, config assembly, and error handling?

##Kalshi changes to orders:
Is the current implementation ready for this change: https://docs.kalshi.com/getting_started/fixed_point_contracts

I do not like this solution suggestion from https://docs.kalshi.com/getting_started/fixed_point_contract: One way to prepare is to internally multiply the \_fp value by 100 and cast to an integer. For example, treating "1.55" as 155 units of 1c contracts allows continued use of integer arithmetic.

My thoughts. I expect to handle this differently avoiding multiplying by 100 and to just implement fractional contracts directly when this becomes available for the markets i am looking at
