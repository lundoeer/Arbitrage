# Position Monitoring Architecture

Last updated: 2026-02-24  
Scope: Cross-venue Position and Order-State Reconciliation

## Objective

The monitoring system combines:

1. Submit responses (`buy_execution` results) for immediate in-memory order updates.
2. Polymarket user websocket channel for continuous order/trade lifecycle updates.
3. Kalshi `market_positions` websocket channel for continuous position updates.
4. Kalshi `user_orders` websocket channel for continuous order lifecycle updates.
5. Polymarket positions REST endpoint for authoritative periodic position reconciliation.
6. Kalshi positions REST endpoint for authoritative periodic position reconciliation.
7. Polymarket + Kalshi orders REST endpoints for authoritative periodic order reconciliation.

This hybrid model keeps fast streaming behavior while continuously correcting to authoritative snapshots.

## Design Constraints & Decisions

1. Monitoring scope is the active discovered market pair only (not portfolio-wide).
2. Polymarket user channel subscription is scoped to the active condition.
3. Kalshi user orders websocket is scoped to the active ticker.
4. Polymarket fill delta is applied only on `CONFIRMED` trade events.
5. Position bootstrap/staleness remains fail-closed for buy execution.
6. Order-state reconciliation affects exposure/order accounting only; it is not a separate hard global gate.
7. Order merge precedence is fixed: `submit_ack < websocket < REST poll snapshot`.
8. Closed orders are removed immediately from runtime (history lives in logs).
9. Global execution lock unlock uses positions poll success timestamps for required venues.

## Source Roles and Trust

### Polymarket

- Submit response: immediate order record seed, low trust.
- User websocket (`trade` / `order`): near-real-time lifecycle, medium trust.
- Positions REST: authoritative position snapshot, high trust.
  - Poll query uses `user`, `market=<active conditionId>`, `sizeThreshold=0`.
  - Normalization uses `initialValue` as position exposure (fallback `size * avgPrice`).
- Orders REST: authoritative order snapshot, high trust.

### Kalshi

- Submit response: immediate order record seed, low trust.
- `market_positions` websocket: near-real-time position updates, high trust for position.
- `user_orders` websocket: near-real-time order lifecycle, medium trust.
- Positions REST: authoritative position snapshot, high trust.
  - Normalization prefers `market_exposure_dollars` as position exposure.
- Orders REST: authoritative order snapshot, high trust.

## Module Layout

- `scripts/common/position_runtime.py`
  - canonical in-memory positions/orders
  - order/position update merge logic
  - health + exposure accounting
- `scripts/common/position_polling.py`
  - positions + orders poll clients
  - `PositionReconcileLoop` orchestration
- `scripts/common/ws_collectors.py`
  - `PolymarketUserWsCollector`
  - `KalshiMarketPositionsWsCollector`
  - `KalshiUserOrdersWsCollector`
- `scripts/common/ws_normalization.py`
  - user/position/order event normalization
- `scripts/run/arbitrage_engine.py`
  - wiring callbacks, collectors, runtime, and reconcile loop
- `scripts/run/engine_loop.py`
  - runs polling loop task + logging integration
  - forwards successful positions poll events to execution lock runtime

## Canonical Runtime Model

### Position model

Tracked by `(venue, instrument_id, outcome_side)` with:

- `net_contracts`
- `avg_entry_price`
- `position_exposure_usd` (authoritative when returned by REST normalization)
- `last_source`, `last_update_ms`
- `is_authoritative`

### Order model

Tracked by `client_order_id` with order-id back-reference when present:

- `client_order_id`, `order_id`
- `venue`, `instrument_id`, `outcome_side`, `action`
- `status` (normalized)
- `requested_size`, `filled_size`, `remaining_size`
- `limit_price`
- `last_source`, `source_rank`, `last_update_ms`

Normalized statuses:

- `open`
- `partially_filled`
- `filled`
- `canceled`
- `rejected`
- `expired`
- `failed`

Terminal statuses are removed from runtime immediately.

## Update Rules

### Rule 1: Submit-response ingestion

On `buy_execution` results:

- upsert order state from leg request/response
- apply explicit fill delta only when fill quantity is present in submit response

### Rule 2: Polymarket user channel

- `trade` events:
  - apply position delta only on `CONFIRMED`
- `order` events:
  - upsert order lifecycle fields (status/sizes/price identifiers)

### Rule 3: Kalshi market_positions websocket

- apply absolute yes/no position values for active ticker
- mark position updates as authoritative stream values

### Rule 4: Kalshi user_orders websocket

- upsert order lifecycle fields for active ticker
- close/remove order record immediately when status is terminal

### Rule 5: REST reconciliation (positions + orders)

On poll cadence:

- positions snapshots overwrite tracked position state per venue
- orders snapshots reconcile tracked orders per venue with highest precedence
- if positions REST payload is null/malformed, reconcile is skipped for that venue
  (runtime keeps prior position state; venue is marked reconcile failure)
- when an orders snapshot is marked complete for a venue, runtime prunes local
  venue order records that are missing from that snapshot (after a short grace
  window) to prevent stale open-order exposure from persisting

Order precedence is deterministic:

- submit ack (rank 1)
- websocket update (rank 2)
- poll snapshot (rank 3)

Order snapshot completeness:

- runtime only performs missing-order prune when snapshot is complete
  (`truncated_by_max_pages=false`)
- this avoids accidental drops when a paginated poll is truncated
- prune uses a 5-second grace window based on order `last_update_ms` to reduce
  race conditions between websocket and poll updates

## Polling and Freshness

### Startup behavior

Before active buy execution in a segment:

1. positions poll bootstrap (both venues)
2. orders poll bootstrap (both venues)
3. runtime health/exposure initialized from current snapshots

Execution-lock integration:

1. After execution result, lock runtime requires per-venue positions poll success at/after execution completion timestamp.
2. `_position_poll_loop` feeds each successful venue poll to lock runtime (`mark_positions_reconcile_success`).

### Staleness thresholds

- `warning_stale` when no authoritative position reconcile success for venue exceeds warning threshold.
- `hard_stale` when it exceeds error threshold; this blocks buy execution until recovery.

Order poll failures do not create a separate hard global gate; they impact exposure accuracy until next successful order reconcile.

## Max Exposure Gate

Exposure is computed per active market bucket:

- `exposure_by_market.polymarket`
- `exposure_by_market.kalshi`

Per bucket:

- `position_leg_exposure_usd = position_exposure_usd when present; else abs(net_contracts) * resolved_entry_price`
- `resolved_entry_price = avg_entry_price when valid; else 1.0 (conservative max payout fallback)`
- `gross_position_exposure_usd = sum(position_leg_exposure_usd)`
- `gross_open_order_exposure_usd = sum(open_buy_remaining_size * limit_price)`
- `gross_total_exposure_usd = gross_position + gross_open_order`

If `gross_total_exposure_usd > max_exposure_per_market_usd`, that market bucket sets `buy_blocked=true`.

Execution plan gating blocks all buy legs whenever one or more market buckets are blocked.
Candidate-order projection is not included.

## Config and CLI Control

### `position_monitoring` config keys

- `enabled`
- `require_bootstrap_before_buy`
- `polymarket_user_ws_enabled`
- `kalshi_market_positions_ws_enabled`
- `kalshi_user_orders_ws_enabled`
- `polymarket_poll_seconds`
- `kalshi_poll_seconds`
- `polymarket_orders_poll_seconds`
- `kalshi_orders_poll_seconds`
- `loop_sleep_seconds`
- `drift_tolerance_contracts`
- `max_exposure_per_market_usd`
- `include_pending_orders_in_exposure`
- `stale_warning_seconds`
- `stale_error_seconds`

### Relevant CLI overrides

- `--enable-position-monitoring`
- `--polymarket-user-ws-enabled/--no-polymarket-user-ws-enabled`
- `--kalshi-market-positions-ws-enabled/--no-kalshi-market-positions-ws-enabled`
- `--kalshi-user-orders-ws-enabled/--no-kalshi-user-orders-ws-enabled`
- `--position-polymarket-poll-seconds`
- `--position-kalshi-poll-seconds`
- `--position-polymarket-orders-poll-seconds`
- `--position-kalshi-orders-poll-seconds`

## Logging Outputs

With `--log-positions`, runtime emits `data/position_monitoring_log__*.jsonl` including:

- `kind=position_health_transition`
- `kind=position_poll_snapshot` (now includes order poll/reconcile payloads)
- `kind=position_polymarket_user_order`
- `kind=position_kalshi_user_order`
- `kind=position_kalshi_market_position`
- `kind=position_execution_lock_transition`

Each event includes the current `position_health` snapshot.

With `--log-raw-events`, per-venue REST raw payload logs are also emitted:

- `data/position_poll_raw_http_polymarket__*.jsonl`
  - includes `result.snapshot.raw_http_body` (Polymarket `/positions` response body)
- `data/position_poll_raw_http_kalshi__*.jsonl`
  - includes `result.snapshot.raw_http_pages` (Kalshi paginated `/portfolio/positions` bodies)

## External References

- Polymarket user channel: https://docs.polymarket.com/market-data/websocket/user-channel
- Polymarket positions API: https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user
- Polymarket user orders API: https://docs.polymarket.com/api-reference/trade/get-user-orders
- Kalshi market positions websocket: https://docs.kalshi.com/websockets/market-positions
- Kalshi user orders websocket: https://docs.kalshi.com/websockets/user-orders
- Kalshi positions API: https://docs.kalshi.com/api-reference/portfolio/get-positions
- Kalshi orders API: https://docs.kalshi.com/api-reference/orders/get-orders
