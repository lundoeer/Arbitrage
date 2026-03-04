# Reverse Strategy Limit Logs

## Introduction
These three logs together describe one reverse-limit strategy run end-to-end:

- Decision/state telemetry (`reverse_strategy_events__<run_id>.jsonl`)
- Order lifecycle records (`reverse_strategy_orders__<run_id>.jsonl`)
- Future market snapshots (`future_markets__<run_id>.jsonl`)
- Per-market finalized rollup rows (`logs/log_reverse_strategy.jsonl`)

For the run `20260303T163341Z`, the files line up as:

- `data/limit_market/reverse_strategy_events__20260303T163341Z.jsonl`: 6412 rows
- `data/limit_market/reverse_strategy_orders__20260303T163341Z.jsonl`: 149 rows
- `logs/log_reverse_strategy.jsonl` filtered to `run_id=20260303T163341Z`: 63 rows

## Common Fields
Fields that appear in most rows:

- `ts`: Event timestamp in UTC ISO format.
- `recv_ms`: Receive timestamp in epoch milliseconds.
- `run_id`: Strategy run identifier (present in orders and summary rows, and strategy start in events).
- `kind`: Row type within a JSONL stream (events and orders files).

## `reverse_strategy_events__<run_id>.jsonl`
Purpose: high-frequency decision and runtime telemetry.

### `kind=strategy_start`
- `config_path`, `state_path`, `dry_run`
- `config`: Effective runtime config snapshot (thresholds, sizing, timing, logging flags).

### `kind=window_transition`
- `window_start_s`: Current 15m window start epoch seconds.
- `window_slug`: Polymarket slug for the current window.
- `window_start_utc`, `minute_bucket`, `is_quarter_hour`

### `kind=decision_poll`
- `window_start_s`, `candidate_kind`, `signal_window_start_s`
- `signal_market`: Market being resolved to infer direction.
  - Includes `slug`, `market_id`, `condition_id`, `window_end_s`, and observed/api/preclose yes prices.
- `target_market`: Next market to place the reverse order on.
  - Includes `slug`, `market_id`, `condition_id`.
- `rtds`: Chainlink RTDS snapshot (`price_usd`, `source_timestamp_ms`, `age_ms`, `is_stale`, etc.).
- `start_price`: Start reference price once captured.
- `last_trade_ws`, `user_fill_ws`: WS health/counters.
- `determination`: Current resolution decision state (`resolved`, `reason`, `seconds_to_end`, chosen price fields).

### `kind=start_price_captured`
- `window_start_s`, `slug`, `rtds`

### `kind=strategy_stop`
- `reason` (example: `keyboard_interrupt`)

## `reverse_strategy_orders__<run_id>.jsonl`
Purpose: order attempts and fills.

### `kind=order_attempt`
- Market context: `window_start_s`, `candidate_kind`, `signal_window_start_s`, `signal_slug`
- Target context: `target_market_key`, `target_slug`, `target_token`
- Decision context: `resolved_outcome`, `resolved_reason`, `reverse_side`
- Status: `ok`
- `result`:
  - `request`: normalized intent (`signal_id`, `client_order_id`, side/size/price, market identifiers)
  - `response`: venue payload
  - `normalized`: normalized order response (`order_id`, status, fill/remaining if available)

### `kind=order_fill`
- IDs: `order_id`, `client_order_id`, `signal_id`
- Fill source/status: `fill_source`, `source`, `status`
- Fill economics: `requested_size`, `fill_delta`, `filled_total`, `fill_price`
- Instrument context: `market` (condition id), `instrument_id`, `outcome_side`, `order_side`
- Some rows also carry attempt context (`target_slug`, `resolved_reason`, etc.)

## `future_markets__<run_id>.jsonl`
Purpose: periodic snapshots (default every 10s) of BTC and top-of-book on three
Polymarket windows: current, next, and second-next.

Core fields:

- `ts`, `recv_ms`, `run_id`
- `current_market_age_seconds`
- `btc_price_usd` (from RTDS adapter read in strategy loop)
- Current market flat quote fields:
  - `current_market_best_ask_price_yes`, `current_market_best_ask_size_yes`
  - `current_market_best_bid_price_yes`, `current_market_best_bid_size_yes`
  - `current_market_best_ask_price_no`, `current_market_best_ask_size_no`
  - `current_market_best_bid_price_no`, `current_market_best_bid_size_no`
- Next market flat quote fields:
  - `next_market_best_ask_price_yes`, `next_market_best_ask_size_yes`
  - `next_market_best_bid_price_yes`, `next_market_best_bid_size_yes`
  - `next_market_best_ask_price_no`, `next_market_best_ask_size_no`
  - `next_market_best_bid_price_no`, `next_market_best_bid_size_no`
- Second-next market flat quote fields:
  - `second_next_market_best_ask_price_yes`, `second_next_market_best_ask_size_yes`
  - `second_next_market_best_bid_price_yes`, `second_next_market_best_bid_size_yes`
  - `second_next_market_best_ask_price_no`, `second_next_market_best_ask_size_no`
  - `second_next_market_best_bid_price_no`, `second_next_market_best_bid_size_no`
- `errors` (non-fatal lookup/orderbook issues; row still written)
- Trailing market identity fields:
  - `current_market_slug`
  - `next_market_slug`
  - `second_next_market_slug`
  - `current_market_condition_id`

## `logs/log_reverse_strategy.jsonl`
Purpose: finalized per-market rollup rows across runs.

Core fields:

- `run_id`
- Market identity: `market_start`, `market_slug`, `market_id`, `condition_id`
- Decision summary: `previous_market_resolve`, `next_order_decided_by`, `order_side`
- Order summary: `order_price`, `order_size`, `orders_filled_when_first_seconds`, `orders_filled_when_full_seconds`, `orders_unfilled`
- PnL/account snapshot: `result_usd`, `account_balance`, `account_balance_method`
- Finalization: `finalized_at`, `finalization_status`

## Connections Between Logs

1. **Run-level link**
- Use `run_id` to scope the summary log to the same run-specific events/orders files.

2. **Market/window link**
- `events.decision_poll.signal_market.slug` matches `orders.signal_slug`.
- `events.decision_poll.target_market.slug` matches `orders.target_slug`.
- `orders.target_slug` matches `summary.market_slug` for that market’s final row.

3. **Decision reason propagation**
- Decision reasons flow from telemetry to execution summary:
  - `events.decision_poll.determination.reason`
  - `orders.resolved_reason`
  - `summary.next_order_decided_by`
- For run `20260303T163341Z`, `summary.next_order_decided_by` matched `orders.resolved_reason` for all comparable rows (`62/62`).

4. **Order lifecycle link**
- `order_attempt.result.request.signal_id` <-> `order_fill.signal_id`
- `order_attempt.result.request.client_order_id` <-> `order_fill.client_order_id`
- `order_attempt.result.normalized.order_id` <-> `order_fill.order_id`
- For run `20260303T163341Z`, 50 attempted orders had observed fills by these IDs.

5. **Shutdown/finalization link**
- `events.strategy_stop.reason=keyboard_interrupt` lines up with a `partial_on_shutdown` row in summary for the same run.
