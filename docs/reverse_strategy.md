# Reverse Premarket Strategy

Last updated: 2026-03-02

## Goal

Implement a simpler Polymarket-only strategy that places one reverse-side
limit order in the upcoming BTC 15m market based on the current market outcome.

## Runtime Entry Point

- `scripts/limit_premarket/start_reverse_strategy.py`

Run command:

```powershell
.venv\Scripts\python.exe -m scripts.limit_premarket.start_reverse_strategy --config config/run_config.json
```

## High-Level Flow

1. Identify current and next Polymarket BTC 15m markets by slug time bucket.
2. Capture RTDS BTC price at market boundary (`00/15/30/45`) as start price.
3. During final 2 minutes of current market, poll current market every 10s:
   - use live CLOB market websocket `last_trade_price` for YES/NO tokens
   - if YES price `>= 0.97` -> current outcome YES
   - if YES price `<= 0.03` -> current outcome NO
4. If threshold did not determine outcome:
   - after market end, wait `resolution_wait_after_end_seconds`
   - try official market resolution from `outcomePrices`
   - keep checking previous market for
     `previous_window_resolution_grace_seconds` after boundary rollover
5. If official resolution is unavailable:
   - use RTDS fallback (`end_price vs start_price`) only when start price was
     captured for that market.
6. When current outcome resolves, place one reverse-side GTC limit buy order in
   next market:
   - current YES -> buy next NO token
   - current NO -> buy next YES token
7. Never retry that next market in the same strategy state.

## Order Rules

- Venue: Polymarket only
- Time in force: `GTC`
- One order total per next market key (`market_id` fallback `slug`)
- Token routing: direct YES/NO token ids
- Default price/size:
  - `order_limit_price = 0.49`
  - `order_size = 10`

## Logging and State

Output directory:

- `data/limit_market`

Files:

- `reverse_strategy_events__<run_id>.jsonl`
- `reverse_strategy_orders__<run_id>.jsonl`
- `reverse_strategy_summary__<run_id>.json`
- `reverse_strategy_state.json` (persistent)

Required logging behavior implemented:

- order placement errors are always written (`kind=order_error`)
- filled/partially-filled responses are always written (`kind=order_fill`)
- extra logs can be toggled from config (or overridden by CLI flags)

## Configuration Section

Dedicated config section in `config/run_config.json`:

- `reverse_strategy.enabled`
- `reverse_strategy.poll_interval_seconds`
- `reverse_strategy.loop_sleep_seconds`
- `reverse_strategy.observation_window_seconds`
- `reverse_strategy.yes_threshold`
- `reverse_strategy.no_threshold`
- `reverse_strategy.order_limit_price`
- `reverse_strategy.order_size`
- `reverse_strategy.dry_run`
- `reverse_strategy.output_dir`
- `reverse_strategy.state_file`
- `reverse_strategy.start_price_capture_grace_seconds`
- `reverse_strategy.resolution_wait_after_end_seconds`
- `reverse_strategy.rtds_symbol`
- `reverse_strategy.rtds_max_age_ms`
- `reverse_strategy.rtds_resolution_epsilon_usd`
- `reverse_strategy.previous_window_resolution_grace_seconds`
- `reverse_strategy.log_events`
- `reverse_strategy.log_decision_polls`
- `reverse_strategy.log_order_attempts`

CLI overrides (optional, config remains default):

- `--log-events/--no-log-events`
- `--log-decision-polls/--no-log-decision-polls`
- `--log-order-attempts/--no-log-order-attempts`

## Notes

- First market after process start may not have a captured start price.
  RTDS fallback is therefore intentionally unavailable for that market.
- Strategy is a standalone command and is not integrated into
  `scripts/run/start_engine.py`.
