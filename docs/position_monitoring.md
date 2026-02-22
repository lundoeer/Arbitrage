# Position Monitoring Implementation Plan

Last updated: 2026-02-22  
Task: #12 (design)

## Objective

Build a position monitoring system that combines:

1. Order submit responses (`execute_cross_venue_buy` results) for immediate in-memory updates.
2. Polymarket user websocket channel for continuous order/trade lifecycle updates.
3. Polymarket positions REST endpoint for periodic reconciliation.
4. Kalshi `market_positions` websocket channel for continuous position updates.
5. Kalshi portfolio positions REST endpoint for periodic reconciliation.

This aligns with your proposed architecture: fast streaming updates + periodic authoritative checks.

## Confirmed Decisions (Resolved)

1. Polymarket user-channel subscription is scoped to the active Polymarket market for the current discovered pair.
2. Discovery output must include Polymarket `conditionId`.
3. Polymarket fill delta is applied only on `CONFIRMED` trade lifecycle state.
4. Startup is fail-closed for buy execution if initial position bootstrap fails, with automatic re-enable once later position polls succeed.
5. Monitoring scope is only the currently selected pair, not account-wide positions.
6. State remains in-memory only, with REST bootstrap/reconciliation (no persisted local state).

## Key Design Decision

Use a hybrid model:

- Event-driven updates for low latency.
- Periodic snapshot reconciliation for correctness.

No dedicated Polymarket position websocket appears to exist. The user channel provides `order` and `trade` events, so Polymarket position state must be inferred from fills and reconciled against REST snapshots.

## Source Roles and Trust Order

### Polymarket

- `Source A`: submit response (`buy_execution` result)  
  role: create pending order records quickly  
  trust: low for filled size

- `Source B`: user channel `trade` / `order`  
  role: near-real-time fill lifecycle  
  trust: medium (event-ordering and retries can happen)

- `Source C`: `GET /positions`  
  role: authoritative reconciliation  
  trust: high

### Kalshi

- `Source A`: submit response (`buy_execution` result)  
  role: create pending order records quickly  
  trust: low for final filled size

- `Source B`: `market_positions` websocket  
  role: near-real-time position updates  
  trust: high for current position

- `Source C`: `GET /portfolio/positions`  
  role: authoritative reconciliation and recovery  
  trust: high

## Proposed Runtime Architecture

### New modules

- `scripts/common/position_runtime.py`
- `scripts/common/position_polling.py`
- `scripts/common/position_monitoring.py` (small orchestration helpers)

### Extend existing modules

- `scripts/common/ws_collectors.py`
  add:
  - `PolymarketUserWsCollector`
  - `KalshiMarketPositionsWsCollector`

- `scripts/common/ws_normalization.py`
  add:
  - `normalize_polymarket_user_event(...)`
  - `normalize_kalshi_market_positions_event(...)`

- `scripts/common/run_config.py`
  add `PositionMonitoringConfig` loaders.

- `scripts/run/engine_cli.py`
  add CLI flags for position monitoring enablement and intervals.

- `scripts/run/arbitrage_engine.py`
  wire runtime + collectors + poll loop + logging + summary counters.

## Canonical In-Memory Model

### `PositionRuntime` state

- `positions_by_key`: canonical per-venue position state.
- `orders_by_client_order_id`: pending/filled/rejected order lifecycle.
- `event_dedupe`: recent event IDs/hashes to prevent double-apply.
- `last_reconcile_at_ms` per venue.
- `health`: stale/drift/error indicators.

### Canonical position key

- `venue`: `polymarket` or `kalshi`
- `instrument_id`: Polymarket token ID or Kalshi ticker
- `outcome_side`: `yes` or `no`

### Canonical position fields

- `net_contracts`
- `avg_entry_price`
- `realized_pnl` (if available from venue sources)
- `fees` (if available)
- `last_update_ms`
- `last_source` (`submit_ack`, `user_ws`, `positions_ws`, `positions_poll`)
- `is_authoritative` (true when set from trusted snapshot/ws position source)

## Update and Reconciliation Rules

### Rule 1: submit-response ingestion (source 1)

On `buy_execution` result:

- Record/refresh order state keyed by `client_order_id`.
- Store venue order ID if present in API response.
- Mark status as `submitted` or `pending`.
- Do not treat as filled unless explicit fill fields are present.

### Rule 2: Polymarket user channel ingestion (source 2)

Normalize `trade` and `order` events.

- `trade` events:
  - apply position/fill deltas only on `CONFIRMED`.
  - `MATCHED`/`MINED`/other pre-confirm states update order lifecycle only, not net position.
  - `FAILED` updates order status and does not affect position unless a prior `CONFIRMED` correction is required.
- `order` events:
  - update lifecycle status (`LIVE`, `DELAYED`, `MATCHED`, `UNMATCHED`, `MINED`, `CONFIRMED`, `RETRYING`, `FAILED`).
  - update cumulative filled size if provided.

Deduplicate using trade/order IDs + status + size hash.

### Rule 3: Kalshi market_positions ingestion (source 4)

Treat each `market_position` event as an absolute position update.

- Upsert the corresponding Kalshi position key.
- Mark as authoritative.
- Update order correlation if order identifiers are present.

### Rule 4: periodic REST reconciliation (sources 3 and 5)

Poll both venues on a timer.

- Polymarket:
  - pull current positions by user.
  - overwrite canonical Polymarket position values for tracked instruments.
- Kalshi:
  - pull portfolio positions (paginate with `cursor`).
  - overwrite canonical Kalshi position values.

After overwrite, clear drift flags for matching keys.

### Rule 5: drift handling

If stream-derived value differs materially from poll snapshot:

- mark `drift_detected=true`.
- emit warning log entry.
- trust poll snapshot as source of truth.

## Polling and Freshness Plan

### Startup bootstrap

Before enabling live buy execution in a segment:

1. Run one Polymarket positions poll.
2. Run one Kalshi positions poll.
3. Initialize `PositionRuntime` from those snapshots.

### Continuous cadence

- Polymarket positions poll every `10s` (configurable).
- Kalshi positions poll every `20s` (configurable).
- Immediate retry on failure with bounded backoff.

### Staleness thresholds

- warning if no authoritative update for a venue in `>60s` (configurable)
- hard stale state if `>180s` (configurable)

## Engine Integration Plan

### `scripts/run/arbitrage_engine.py`

1. Instantiate `PositionRuntime` at segment start.
2. Forward every `buy_execution` result into `PositionRuntime.apply_buy_execution_result(...)`.
3. Start additional tasks:
   - Polymarket user WS collector task.
   - Kalshi market_positions WS collector task.
   - positions poll loop task.
   - optional position snapshot writer loop task.
4. Include new summary counters:
   - position events ingested
   - poll successes/failures by venue
   - drift corrections
   - stale intervals
5. Keep buy decision loop independent; position monitoring failure should not crash the process.
6. Add a position health gate in the engine submit path:
   - `decision.can_trade` remains quote/market-data based.
   - final buy-submit eligibility becomes `decision.can_trade AND position_runtime.health.buy_execution_allowed`.
   - `buy_execution_allowed=false` at startup until bootstrap succeeds.
   - if bootstrap fails initially, block submits (fail-closed).
   - if later reconciliation polls succeed and health recovers, flip to `buy_execution_allowed=true` without restart.

## Config and CLI Additions

### `config/run_config.json` (new section)

`position_monitoring`:

- `enabled` (bool)
- `polymarket_user_ws_enabled` (bool)
- `kalshi_market_positions_ws_enabled` (bool)
- `polymarket_poll_seconds` (int)
- `kalshi_poll_seconds` (int)
- `drift_tolerance_contracts` (float)
- `stale_warning_seconds` (int)
- `stale_error_seconds` (int)
- `require_bootstrap_before_buy` (bool, default true)

### CLI flags

- `--enable-position-monitoring` / `--no-enable-position-monitoring`
- `--position-polymarket-poll-seconds`
- `--position-kalshi-poll-seconds`
- `--log-positions`

## Logging Outputs

New optional file:

- `data/position_monitoring_log__<run_id>.jsonl`

Recommended event kinds:

- `position_order_submit_ack`
- `position_polymarket_user_trade`
- `position_polymarket_user_order`
- `position_kalshi_market_position`
- `position_poll_snapshot`
- `position_drift_correction`
- `position_health`

## Phased Implementation Steps

1. Phase 1: Core runtime and tests.
   - Deliver `PositionRuntime` with unit tests for merges, dedupe, drift correction.
2. Phase 2: Venue ingestion adapters.
   - Implement user/market_positions collectors + normalizers.
3. Phase 3: REST poll clients and reconcile loop.
   - Implement polling clients with pagination and retries.
4. Phase 4: Engine wiring.
   - Integrate runtime and loops into `arbitrage_engine.py`.
5. Phase 5: Observability.
   - Add logs, counters, summary fields, and stale/drift alerts.

## Discovery and Selection Changes

`scripts/run/discover_active_btc_15m_markets.py` and `scripts/common/market_selection.py` should be extended so selected Polymarket payload includes:

- `condition_id` (from Polymarket `conditionId`)

Engine market context should carry this as:

- `polymarket_condition_id`

Polymarket user-channel collector should subscribe only to this selected condition/market scope, then apply local filtering as a safety check.

## Test Plan

Add:

- `tests/test_position_runtime.py`
- `tests/test_position_polling.py`
- `tests/test_position_ws_normalization.py`

Minimum cases:

- submit-response registers pending order
- polymarket trade dedupe and status transitions
- polymarket pre-confirm states do not mutate position
- kalshi market_positions absolute overwrite behavior
- poll snapshot reconciliation overwrites drifted state
- kalshi positions pagination merge
- stale health transitions

Additional test cases (resolved decisions):

- Polymarket trade events with `MATCHED` do not change position.
- Polymarket trade events with `CONFIRMED` do change position.
- buy execution is blocked until bootstrap success when `require_bootstrap_before_buy=true`.
- buy execution auto-unblocks after recovery poll success.
- only selected-pair instruments are tracked; unrelated instruments are ignored.

## External References

- Polymarket user channel: https://docs.polymarket.com/market-data/websocket/user-channel
- Polymarket get current positions: https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user
- Kalshi market positions websocket: https://docs.kalshi.com/websockets/market-positions
- Kalshi get positions: https://docs.kalshi.com/api-reference/portfolio/get-positions
