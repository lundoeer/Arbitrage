# Market Surveillance Data Flow (Current Implementation)

Last updated: 2026-02-27

## Goal

Explain exactly how the surveillance runtime turns Polymarket/Kalshi source data into per-tick BTC prices with source timestamps.

This document reflects the current implementation in:

1. `scripts/surveillance/market_surveillance.py`
2. `tests/test_market_surveillance_phase*_*.py`

## Runtime Overview

In `ws` mode the process runs these components:

1. Polymarket RTDS chainlink adapter (background websocket thread).
2. Quote runtime websocket collectors:
   - Polymarket CLOB market websocket
   - Kalshi trade websocket
3. Snapshot loop (default cadence `1.0s`) writing one JSONL row per tick.
4. Optional resolved-pairs maintenance loop (default enabled).

## Startup Sequence (Current)

1. Build adapters/selectors.
2. Start Polymarket RTDS adapter first.
3. Wait `--ws-start-stagger-seconds` (default `10.0`).
4. Select active pair.
5. Start Polymarket/Kalshi quote collectors.
6. Enter snapshot loop.

This ordering is intentional to warm RTDS before quote snapshots.

## Source 1: Kalshi Website BTC (`btc_current.json`)

Endpoint:

`https://kalshi-public-docs.s3.amazonaws.com/external/crypto/btc_current.json?allowRequestEvenIfPageIsHidden=true`

### Raw payload example

From `tests/fixtures/surveillance/kalshi_btc_current_sample.json`:

```json
{
  "maturity_ts_ms": 1772198705000,
  "timeseries": {
    "second": [
      66019.92033, 66020.01383, 66020.26517, 66020.711, 66021.1185, 66021.548
    ]
  }
}
```

### Timestamp translation logic

Given:

1. `A = maturity_ts_ms`
2. `N = len(timeseries.second)`
3. `i` is zero-based index

Mapped timestamp for element `i`:

`ts_i = A - (N - 1 - i) * 1000`

Using the example above (`A=1772198705000`, `N=6`):

1. `i=0` (`66019.92033`) -> `1772198700000`
2. `i=5` (`66021.548`) -> `1772198705000`

### Lookup for each snapshot tick

For snapshot receive time `recv_ms`:

1. `target_second_ms = floor(recv_ms / 1000) * 1000`
2. If exact second in cache, use it.
3. Else use latest cached second `<= target_second_ms`.
4. Else fallback to latest cached second overall.

Metadata on output includes:

1. `requested_second_ms`
2. `selected_second_ms`
3. `selected_second_exact`
4. `selected_second_offset_ms`
5. `series_anchor_ts_ms`
6. `timeseries_second_count`

## Source 2: Polymarket RTDS Chainlink BTC

Websocket endpoint:

`wss://ws-live-data.polymarket.com`

Topic:

`crypto_prices_chainlink` filtered by `symbol=btc/usd`

### Subscription payload

```json
{
  "action": "subscribe",
  "subscriptions": [
    {
      "topic": "crypto_prices_chainlink",
      "type": "*",
      "filters": "{\"symbol\":\"BTC/USD\"}"
    }
  ]
}
```

### RTDS keepalive and shutdown

1. Sends app-level `PING` every 5 seconds.
2. Replies `PONG` to incoming `PING`.
3. On shutdown sends explicit `unsubscribe` before close.

Unsubscribe payload:

```json
{
  "action": "unsubscribe",
  "subscriptions": [
    {
      "topic": "crypto_prices_chainlink",
      "type": "*",
      "filters": "{\"symbol\":\"BTC/USD\"}"
    }
  ]
}
```

### RTDS event parsing logic

Expected event shape:

```json
{
  "topic": "crypto_prices_chainlink",
  "payload": {
    "symbol": "BTC/USD",
    "timestamp": 1772211862123,
    "value": "65637.93364099108"
  }
}
```

Translated to cache:

1. `price_usd = float(payload.value)`
2. `source_ts_ms = int(payload.timestamp)`
3. `second_ts_ms = floor(source_ts_ms / 1000) * 1000`
4. store `cache[second_ts_ms] = price_usd`

Lookup at read time uses same rule as Kalshi:

1. exact second
2. latest prior second
3. latest available second

## Backoff and Retry Behavior

### RTDS websocket

1. Exponential reconnect for general errors.
2. Special-case slow retry for 429 (`>= 60s`) to avoid hammering.

### Pair rotation discovery

1. Boundary trigger at market boundary.
2. One retry after `--pair-boundary-retry-seconds` (default `20`).

### Resolved-pairs loop

1. Runs on configured cadence.
2. Failures are isolated and do not stop snapshot loop.

## Raw WS/HTTP Examples Seen During Runs

### RTDS handshake 429 example (observed)

```json
{
  "message": "Too Many Requests",
  "connectionId": "Zc5BVdBqLPECHVg=",
  "requestId": "Zc5BVFdWLPEEvsA="
}
```

### Polymarket market websocket fixture sample

From `tests/fixtures/surveillance/pair_a_polymarket.jsonl`:

```json
{
  "recv_ms": 1000,
  "message": {
    "event_type": "book",
    "asset_id": "pm_yes_a",
    "bids": [[0.39, 200.0]],
    "asks": [[0.41, 180.0]],
    "timestamp": "1000"
  }
}
```

### Kalshi market websocket fixture sample

From `tests/fixtures/surveillance/pair_a_kalshi.jsonl`:

```json
{
  "recv_ms": 1000,
  "message": {
    "type": "ticker",
    "msg": {
      "yes_bid_dollars": "0.4000",
      "yes_ask_dollars": "0.4200",
      "yes_bid_size_fp": "220.0",
      "yes_ask_size_fp": "210.0",
      "ts": "1"
    }
  }
}
```

## End-to-End Output Example

From `data/diagnostic/market_surveillance/btc_15m_per_second.jsonl` (`run_id=20260227T171416Z`):

```json
{
  "ts": "2026-02-27T17:14:30.060+00:00",
  "recv_ms": 1772212470060,
  "underlying_prices": {
    "polymarket_chainlink": {
      "price_usd": 65480.684733313814,
      "source_timestamp_ms": 1772212468000,
      "source_age_ms": 2060,
      "source": "polymarket_rtds_chainlink",
      "metadata": {
        "requested_second_ms": 1772212470000,
        "selected_second_ms": 1772212468000,
        "selected_second_exact": false,
        "selected_second_offset_ms": 2000
      }
    },
    "kalshi_site": {
      "price_usd": 65489.34983,
      "source_timestamp_ms": 1772212468000,
      "source_age_ms": 2060,
      "source": "kalshi_btc_current_json",
      "metadata": {
        "requested_second_ms": 1772212470000,
        "selected_second_ms": 1772212468000,
        "selected_second_exact": false,
        "selected_second_offset_ms": 2000
      }
    }
  }
}
```

Interpretation:

1. Snapshot was written at `recv_ms=1772212470060`.
2. Target second is `1772212470000`.
3. Neither source had exact `...70000` cached at that moment.
4. Both sources resolved to prior second `1772212468000`.

## Notes on Timing Semantics

1. Snapshot cadence is tick-driven; if loop jitter occurs, rows are not guaranteed exactly every wall-clock second.
2. Price timestamps are source-derived and can differ from row `recv_ms`.
3. `source_age_ms` and `is_stale` are the canonical freshness indicators for downstream logic.

## Current Limits

1. RTDS raw frames are not persisted to disk by this script.
2. RTDS availability can be constrained by edge rate limiting (429).
3. When source second is missing, adapter uses latest prior cached second to preserve continuity.

underlying_prices.polymarket_chainling.price_usd
underlying_prices.polymarket_chainling.source_timestamp_ms
underlying_prices.kalshi_site.price_usd
underlying_prices.kalshi_site.source_timestamp_ms
underlying_prices.kalshi_site.metadata.selected_timestamp_ms
ts rounded down to second
best_asks.polymarket_yes.price
best_asks.polymarket_yes.size
best_asks.polymarket_no.price
best_asks.polymarket_no.size
best_asks.kalshi_yes.price
best_asks.kalshi_yes.size
best_asks.kalshi_no.price
best_asks.kalshi_no.size
kalshi_ticker
health.errors
