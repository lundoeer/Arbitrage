# Future Markets Log Implementation

Last updated: 2026-03-04

## Objective

Implement task #42 by adding a per-run JSONL log that records current/next/second-next market top-of-book snapshots every 10 seconds during reverse strategy runtime.

## Scope

- Runtime: `scripts/limit_premarket/start_reverse_strategy.py`
- New module: `scripts/limit_premarket/future_markets_logger.py`
- New output file pattern: `data/limit_market/future_markets__<run_id>.jsonl`

## Design

1. Keep logging inside reverse strategy process (no separate command).
2. Use existing run `run_id` for per-run file naming.
3. Read BTC price from existing RTDS adapter (`rtds.read(...)`).
4. Read top-of-book prices and sizes for YES/NO tokens via CLOB orderbook fetch.
5. Write one row every `future_markets_log_interval_seconds` (default `10.0`).
6. If a market is missing or quote fetch fails, still write the row with `null` values and append diagnostics to `errors`.

## Row Contract

Flat keys:

- `current_market_age_seconds`
- `btc_price_usd`
- `current_market_*` best ask/bid price+size for yes/no
- `next_market_*` best ask/bid price+size for yes/no
- `second_next_market_*` best ask/bid price+size for yes/no
- `errors`

Trailing identity fields:

- `current_market_slug`
- `next_market_slug`
- `second_next_market_slug`
- `current_market_condition_id`

## Config Additions (`reverse_strategy`)

- `future_markets_log_enabled`
- `future_markets_log_interval_seconds`
- `future_markets_log_filename_template`

## Observability

Summary counters:

- `future_markets_rows_written`
- `future_markets_rows_failed`

Summary output additions:

- `output_files.future_markets`
- `future_markets_log` stats object

