# Position Monitoring Architecture

Last updated: 2026-02-22  
Scope: Cross-venue Position Reconciliation & Tracking

## Objective

The position monitoring system combines:

1. Order submit responses (`execute_cross_venue_buy` results) for immediate in-memory updates.
2. Polymarket user websocket channel for continuous order/trade lifecycle updates.
3. Polymarket positions REST endpoint for periodic reconciliation.
4. Kalshi `market_positions` websocket channel for continuous position updates.
5. Kalshi portfolio positions REST endpoint for periodic reconciliation.

This hybrid model enables fast streaming updates gated by periodic authoritative checks.

## Design Constraints & Decisions

1. Polymarket user-channel subscription is scoped to the active Polymarket market for the current discovered pair.
2. Discovery output includes the Polymarket `conditionId` mapping.
3. Polymarket fill delta is applied only on `CONFIRMED` trade lifecycle state.
4. Startup is fail-closed for buy execution if initial position bootstrap fails, with automatic re-enable once later position polls succeed.
5. Monitoring scope is strictly the currently selected pair, not account-wide positions.
6. State remains in-memory only, with REST bootstrap/reconciliation acting as the recovery mechanism (no persisted local database state).

## Key Architecture

The hybrid model pairs low-latency event-driven updates with periodic snapshot reconciliation for correctness.

Since no dedicated Polymarket position websocket exists, the user channel's `order` and `trade` events are normalized, and Polymarket position state is inferred from confirmed fills and reconciled against REST snapshots.

## Source Roles and Trust Order

### Polymarket

- `Source A`: submit response (`buy_execution` result)  
  role: create pending order records quickly  
  trust: low for filled size

- `Source B`: user channel `trade` / `order`  
  role: near-real-time fill lifecycle tracking  
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

## Module Layout

- `scripts/common/position_runtime.py`: `PositionRuntime` maintains the canonical per-venue state, orders, and health metrics using strict `@dataclass` structures.
- `scripts/common/position_polling.py`: Contains API polling clients and the periodic `PositionReconcileLoop`.
- `scripts/common/ws_collectors.py`: Houses `PolymarketUserWsCollector` and `KalshiMarketPositionsWsCollector`.
- `scripts/run/arbitrage_engine.py`: Wires runtime instances, collectors, the polling loop, and health gates.
- `scripts/common/engine_setup.py`: Constructs position components based on configuration.

## Canonical In-Memory Model

### `PositionState` structure

The canonical position state tracked by `PositionRuntime`:

- `net_contracts`: Current net held quantity
- `avg_entry_price`: Computed VWAP of entry
- `realized_pnl`: (if available from venue sources)
- `fees`: (if available)
- `last_update_ms`: Timestamp
- `last_source`: (`submit_ack`, `user_ws`, `positions_ws`, `positions_poll`)
- `is_authoritative`: true when set from trusted snapshot/ws position source

### Canonical position key

- `venue`: `polymarket` or `kalshi`
- `instrument_id`: Polymarket token ID or Kalshi ticker
- `outcome_side`: `yes` or `no`

## Update and Reconciliation Rules

### Rule 1: Submit-response ingestion (source 1)

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

Deduplication occurs via trade/order IDs within `PositionRuntime`.

### Rule 3: Kalshi market_positions ingestion (source 4)

Treat each `market_position` event as an absolute position update.

- Upsert the corresponding Kalshi position key.
- Mark as authoritative.
- Update order correlation if order identifiers are present.

### Rule 4: Periodic REST reconciliation (sources 3 and 5)

Poll both venues on a configurable timer (e.g. 10s Poly, 20s Kalshi).

- Polymarket:
  - pull current positions by user.
  - overwrite canonical Polymarket position values for tracked instruments.
- Kalshi:
  - pull portfolio positions (paginate with `cursor`).
  - overwrite canonical Kalshi position values.

After overwrite, drift counters are incremented if the state was misaligned, and health flags are cleared.

### Rule 5: Drift handling

If a stream-derived value differs materially from a poll snapshot:

- Record drift in counters.
- Trust the poll snapshot as the absolute source of truth.

## Polling and Freshness Strategy

### Startup bootstrap

Before enabling live buy execution in a segment:

1. Run one Polymarket positions poll.
2. Run one Kalshi positions poll (via the polling loop).
3. Initialize `PositionRuntime` limits and set health to `buy_execution_allowed=True`.

### Staleness thresholds

- `warning_stale` triggers if no authoritative update for a venue occurs in `>60s` (configurable)
- `hard_stale` state if `>180s` (configurable), blocking execution until it recovers.

## Config and CLI Control

### `config/run_config.json` (Position section)

The `position_monitoring` config dictionary contains:

- `enabled` (bool)
- `polymarket_user_ws_enabled` (bool)
- `kalshi_market_positions_ws_enabled` (bool)
- `polymarket_poll_seconds` (int)
- `kalshi_poll_seconds` (int)
- `drift_tolerance_contracts` (float)
- `stale_warning_seconds` (int)
- `stale_error_seconds` (int)
- `require_bootstrap_before_buy` (bool, default true)

### Logging Outputs

`run_core_loop` optionally writes out periodic `PositionRuntime.snapshot()` state to `data/` if the correct CLI flags are provided (e.g. `--log-positions`), allowing observability of open positions, pending orders, error counts, and drift metrics across the full engine lifecycle.

## External References

- Polymarket user channel: https://docs.polymarket.com/market-data/websocket/user-channel
- Polymarket get current positions: https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user
- Kalshi market positions websocket: https://docs.kalshi.com/websockets/market-positions
- Kalshi get positions: https://docs.kalshi.com/api-reference/portfolio/get-positions
