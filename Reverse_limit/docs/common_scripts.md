# Common Scripts Implementation

Last updated: 2026-03-04

This document describes the kept `scripts/common/*` modules used by
`scripts/limit_premarket/start_reverse_strategy.py`.

It is implementation-specific and focuses on data contracts and runtime behavior.

## Scope and Components

Runtime imports from `scripts/common/`:

- `api_transport.py`
- `buy_execution.py`
- `engine_setup.py`
- `polymarket_resolution.py`
- `position_polling.py`
- `run_config.py`
- `utils.py`
- `ws_normalization.py`
- `ws_transport.py`

## End-to-End Pipelines

### 1) Config -> Retry Transport -> Order Client

1. `run_config.load_buy_execution_runtime_config_from_run_config(...)` reads
   `buy_execution.*` from `config/run_config.json`.
2. `engine_setup.build_buy_execution_transport(...)` maps retry config into
   `ApiTransport(retry_config=RetryConfig(...))`.
3. `buy_execution.build_polymarket_api_buy_client_from_env(...)` builds
   a `PolymarketApiBuyClient` with:
   - authenticated `py-clob-client` context
   - on-chain funder validation (signature type 1/2)
   - nonce manager seeded from exchange `nonces(address)`

Concrete config example in current repo:

```json
{
  "buy_execution": {
    "enabled": true,
    "api_retry": {
      "enabled": true,
      "max_attempts": 3,
      "base_backoff_seconds": 0.5,
      "jitter_ratio": 0.2,
      "include_post": true
    }
  }
}
```

Resolved transport behavior from that config:

- `max_attempts=3`
- `retry_methods={GET,HEAD,OPTIONS,POST}`
- transient retry status set includes `408,425,429,500,502,503,504`

### 2) Limit Order Submit Path (Polymarket)

`buy_execution.PolymarketApiBuyClient.place_buy_order(...)` flow:

1. Build signed order payload (`_build_polymarket_signed_order`).
2. Submit through `clob_client.post_order(...)`.
3. Retry transient submit failures in `_post_order_with_retry(...)`.
4. If error text includes `invalid nonce`, refresh nonce and retry once.
5. Return normalized envelope:
   - `ok`, `venue`, `status_code`
   - `request` (signed order payload)
   - `response` (exchange response payload)

Client order id generation:

- `build_client_order_id(...)` is deterministic from:
  `signal_id + venue + instrument_id + side + seed`.
- Example observed in production logs:
  - `arb-pm-5571381380-ecc9ba57b369ae452f8e`

Observed order-attempt example (from
`data/limit_market/reverse_strategy_orders__20260303T163341Z.jsonl`):

```json
{
  "kind": "order_attempt",
  "target_slug": "btc-updown-15m-1772556300",
  "resolved_reason": "last_trade_threshold_lower",
  "result": {
    "request": {
      "signal_id": "reverse-premarket-1772555400-1483937",
      "venue": "polymarket",
      "side": "yes",
      "instrument_id": "55713813804142270500445196993359201616144459307396863549709319905402600058822",
      "order_kind": "limit",
      "time_in_force": "gtc",
      "size": 10,
      "limit_price": 0.49,
      "client_order_id": "arb-pm-5571381380-ecc9ba57b369ae452f8e"
    },
    "response": {
      "status_code": 200,
      "response": {
        "orderID": "0x1d26dfe57e2fd0ed9932b532f13c61a384ee10742f3451c4647ff5ee0b48e6a4",
        "status": "live",
        "success": true
      }
    }
  }
}
```

### 3) Account Balance Poll Path

`position_polling.PolymarketAccountPollClient.fetch_balance_allowance()`:

1. Uses authenticated clob client.
2. Calls `get_balance_allowance(...)` for collateral asset.
3. Returns:
   - `{"venue":"polymarket","balance_allowance":{...}}`

Strategy-level balance extraction (outside `common`) reads
`balance_allowance.balance` and converts micro-units to USD by `/ 1_000_000`.

Observed finalized market-log row (from `logs/log_reverse_strategy.jsonl`):

```json
{
  "market_start": "2026-03-03T15:45:00Z",
  "account_balance": 209.86844,
  "account_balance_method": "cash_only",
  "finalization_status": "final"
}
```

### 4) Closed-Positions Resolution Fallback

`polymarket_resolution.fetch_closed_positions_resolution_map(...)`:

1. Calls `GET /closed-positions` on Polymarket data API with pagination:
   - `user`, `limit`, `offset`
2. Parses rows with `parse_closed_positions_resolution_rows(...)`.
3. Keeps the latest timestamp per `conditionId`.
4. Returns map:
   - `condition_id -> polymarket_market_resolution-like object`

Representative parse example:

Raw rows:

```json
[
  {"conditionId":"0x2933...aa80e","outcome":"yes","timestamp":1772554500},
  {"conditionId":"0x2933...aa80e","outcome":"no","timestamp":1772554600}
]
```

Parsed output (latest wins):

```json
{
  "0x2933...aa80e": {
    "kind": "polymarket_market_resolution",
    "condition_id": "0x2933...aa80e",
    "outcome": "no",
    "source": "closed_positions",
    "source_timestamp_ms": 1772554600000
  }
}
```

### 5) User/Resolution WebSocket Normalization

Relevant parts from broader websocket design are still used here:

- raw vendor message -> normalized event
- normalized event includes `source_timestamp_ms` and `lag_ms`
- strategy logic consumes normalized events instead of raw payload shape

Implemented normalizers:

- `normalize_polymarket_user_event(...)`
- `normalize_polymarket_market_resolution_event(...)`

#### User Event Schemas

`kind="polymarket_user_order"`:

- `order_id`, `client_order_id`
- `market`, `asset_id`
- `outcome_side`, `order_side`
- `price`, `original_size`, `size_matched`, `remaining_size`
- `status`, `source_timestamp_ms`, `lag_ms`

`kind="polymarket_user_trade"`:

- `event_id`, `market`, `asset_id`
- `outcome_side`, `order_side`
- `price`, `size`
- `status`, `is_confirmed`
- `taker_order_id`, `maker_orders`
- `source_timestamp_ms`, `lag_ms`

`kind="control_or_unknown"`:

- passthrough fallback when message type is not `trade` or `order`

#### Market Resolution Schema

`kind="polymarket_market_resolution"`:

- `condition_id`
- `outcome` (`yes|no|null`)
- `is_resolved`
- `resolution_status`
- `source_event_type`
- `source_timestamp_ms`, `lag_ms`
- `raw`

#### Raw -> Normalized Examples

Example A: user `order` event:

```json
{
  "event_type": "order",
  "id": "0xcc5c...",
  "client_order_id": "arb-pm-1118398172-f88877f484e201c4e913",
  "market": "0xb7d1...",
  "asset_id": "1118398...",
  "outcome": "yes",
  "side": "buy",
  "price": "0.49",
  "original_size": "10",
  "size_matched": "10",
  "timestamp": "1772558026207"
}
```

Normalizes to:

```json
{
  "kind": "polymarket_user_order",
  "order_id": "0xcc5c...",
  "client_order_id": "arb-pm-1118398172-f88877f484e201c4e913",
  "outcome_side": "yes",
  "order_side": "buy",
  "price": 0.49,
  "original_size": 10.0,
  "size_matched": 10.0,
  "remaining_size": 0.0,
  "source_timestamp_ms": 1772558026207
}
```

Example B: strategy fill log produced from normalized user updates
(observed):

```json
{
  "kind": "order_fill",
  "fill_source": "user_ws",
  "source": "user_ws_order",
  "order_id": "0xcc5ccf57cde396058fb5f31aecf911bcb97c02971c29fd263f5749178cdb38c7",
  "fill_delta": 10.0,
  "filled_total": 10.0,
  "fill_price": 0.49
}
```

### 6) JSONL Persistence Path

`ws_transport.JsonlWriter` behavior:

1. Open file in append mode.
2. Write one compact JSON object per line.
3. Flush on every write.

Used by strategy log sinks for:

- `reverse_strategy_events__<run_id>.jsonl`
- `reverse_strategy_orders__<run_id>.jsonl`
- `logs/log_reverse_strategy.jsonl` (persistent market rollup)

## Script-by-Script Reference

### `api_transport.py`

Purpose:

- HTTP request transport with retry and JSON handling.

Key types:

- `RetryConfig`
- `ApiTransport`
- `ApiTransportError`, `ApiHTTPError`, `ApiResponseParseError`

Implementation highlights:

- Retries only for retryable methods.
- `allow_status` controls accepted HTTP status set.
- Non-JSON success payload raises `ApiResponseParseError`.
- Backoff: `base * 2^(attempt-1)` with optional jitter.

### `buy_execution.py`

Purpose:

- Build authenticated Polymarket order client and submit signed orders.

Key entry points:

- `build_client_order_id(...)`
- `build_polymarket_api_buy_client_from_env(...)`
- `PolymarketApiBuyClient.place_buy_order(...)`

Implementation highlights:

- Reads L1/L2 credentials from env.
- Validates/derives funder for signature type 1/2.
- Tracks exchange nonce and handles nonce mismatch recovery.
- Retries transient `post_order` failures.

### `engine_setup.py`

Purpose:

- Build retry-enabled `ApiTransport` from runtime config.

Key entry point:

- `build_buy_execution_transport(...)`

### `polymarket_resolution.py`

Purpose:

- Parse authoritative closed-position rows into resolution map.

Key entry points:

- `parse_closed_positions_resolution_rows(...)`
- `fetch_closed_positions_resolution_map(...)`

Implementation highlights:

- Outcome normalization supports `yes/no` and `up/down`.
- Timestamp normalization accepts seconds or milliseconds.
- Latest row wins per condition id.

### `position_polling.py`

Purpose:

- Poll Polymarket collateral balance/allowance via authenticated client.

Key entry point:

- `PolymarketAccountPollClient.fetch_balance_allowance()`

### `run_config.py`

Purpose:

- Parse only buy-execution runtime section needed by strategy.

Key entry point:

- `load_buy_execution_runtime_config_from_run_config(...)`

### `utils.py`

Purpose:

- Safe coercion helpers and UTC time helpers used across all modules.

Key helpers:

- `as_dict`, `as_float`, `as_int`, `as_non_empty_text`
- `now_ms`, `utc_now_iso`

### `ws_normalization.py`

Purpose:

- Normalize Polymarket user and resolution websocket payloads.

Key entry points:

- `normalize_polymarket_user_event(...)`
- `normalize_polymarket_market_resolution_event(...)`

### `ws_transport.py`

Purpose:

- Minimal JSONL writer abstraction.

Key types:

- `JsonlWriter`
- `NullWriter`

## Operational Constraints

- `buy_execution.py` and `position_polling.py` require `py-clob-client`.
- `api_transport.py` requires `requests`.
- WS adapters in strategy runtime require `websockets`; normalized events from
  `ws_normalization.py` are the expected interface.
