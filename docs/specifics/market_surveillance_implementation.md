# Market Surveillance Implementation Plan (Task #33)

Last updated: 2026-02-27  
Scope: Build a standalone market surveillance workflow that can run in this repo now and be moved to a separate repo with minimal rewiring.

## Goal

Implement a surveillance process that continuously maintains:

1. `data/diagnostic/resolved_15m_pairs.log` (resolved-pair history)
2. A new per-second snapshot file with:
   - BTC-USD reference price for Polymarket (Chainlink source)
   - BTC-USD reference price for Kalshi (website source)
   - Best ask price and size for all four legs:
     - `polymarket_yes`
     - `polymarket_no`
     - `kalshi_yes`
     - `kalshi_no`

This process must be production-safe (idempotent writes, retries, health visibility) and portable to a new repo.

## Confirmed Inputs From Existing Work

1. Resolved-pair log writer already exists in `scripts/diagnostic/compare_resolved_15m_pairs.py`.
2. Existing runtime can already produce best bid/ask plus size for all four legs via `SharePriceRuntime` + WS collectors (`scripts/diagnostic/capture_btc_15m_ws_price_feeds.py`).
3. Polymarket Chainlink extraction patterns exist in:
   - `scripts/diagnostic/get_polymarket_chainlink_window_prices.py`
   - `scripts/diagnostic/chainlink_btc_15m_historic.py`
4. Kalshi website BTC feed research is documented in `docs/kalshi_targets_price.md` and points to:
   - `https://kalshi-public-docs.s3.amazonaws.com/external/crypto/btc_current.json?allowRequestEvenIfPageIsHidden=true`

## Proposed Runtime Architecture

One process, three concurrent loops, one shared state cache.

1. `resolved_pairs_loop` (low frequency, e.g. every 5-15 min)
   - Calls existing `compare_resolved_15m_pairs.run(...)`.
   - Appends only new rows to `resolved_15m_pairs.log`.
2. `market_pair_selection_loop` (boundary-triggered at `:00/:15/:30/:45`)
   - At each 15-minute boundary, refreshes active pair using existing discovery/cache logic.
   - If no valid pair is found at the boundary, performs one safety retry after 20 seconds.
   - Rotates subscriptions when pair changes.
3. `per_second_snapshot_loop` (1.0s cadence)
   - Reads latest quotes from WS runtime state.
   - Pulls Polymarket reference BTC price from Chainlink adapter.
   - Pulls Kalshi reference BTC price from Kalshi website adapter.
   - Emits one JSONL row per second.

## Proposed File Outputs

1. Existing:
   - `data/diagnostic/resolved_15m_pairs.log`
2. New primary output:
   - `data/diagnostic/market_surveillance/btc_15m_per_second.jsonl`
3. New optional health/status:
   - `data/diagnostic/market_surveillance/market_surveillance_status.json`
4. New optional run summary:
   - `data/diagnostic/market_surveillance/market_surveillance_summary__<run_id>.json`

## Snapshot Schema (Per-Second JSONL)

```json
{
  "ts": "2026-02-27T12:00:01.123456+00:00",
  "recv_ms": 1772232001123,
  "run_id": "20260227T120000Z",
  "pair": {
    "kalshi_ticker": "KXBTC15M-27FEB1200-00",
    "polymarket_slug": "btc-updown-15m-1772231100",
    "polymarket_market_id": "1234567",
    "polymarket_token_yes": "....",
    "polymarket_token_no": "....",
    "window_end": "2026-02-27T12:15:00+00:00"
  },
  "underlying_prices": {
    "polymarket_chainlink": {
      "price_usd": 66820.12,
      "source_timestamp_ms": 1772232000000,
      "source": "chainlink_btcusd"
    },
    "kalshi_site": {
      "price_usd": 66818.77,
      "source_timestamp_ms": 1772231999000,
      "source": "kalshi_btc_current_second_tail"
    }
  },
  "best_asks": {
    "polymarket_yes": { "price": 0.41, "size": 512.0 },
    "polymarket_no": { "price": 0.6, "size": 344.0 },
    "kalshi_yes": { "price": 0.42, "size": 280.0 },
    "kalshi_no": { "price": 0.59, "size": 265.0 }
  },
  "health": {
    "quotes_fresh": true,
    "underlying_fresh": true,
    "errors": []
  }
}
```

## Data Source Plan

### 1) Resolved pairs log

Reuse existing implementation directly:

- Module: `scripts/diagnostic/compare_resolved_15m_pairs.py`
- Interface: call `run(...)` from the surveillance process.

No reimplementation needed.

### 2) Best ask quotes (4 legs)

Reuse existing WS runtime path:

- `PolymarketWsCollector`
- `KalshiWsCollector`
- `SharePriceRuntime.snapshot(...).quotes.legs`

Reason:

1. Already normalized.
2. Already includes best price + size.
3. Already battle-tested in current diagnostics and engine runtime.

### 3) Polymarket BTC reference price (Chainlink)

Use Polymarket RTDS (`crypto_prices_chainlink`) as the source of Chainlink BTC feed values via a dedicated adapter (`PolymarketRtdsChainlinkPriceAdapter`) that:

1. Connects to `wss://ws-live-data.polymarket.com`.
2. Subscribes to topic `crypto_prices_chainlink` filtered by symbol `btc/usd`.
3. Caches per-second values from RTDS payloads and resolves each surveillance row to requested second.
4. Records source timestamp/freshness fields from RTDS event timestamp.

### 4) Kalshi BTC reference price (website)

Use documented public endpoint:

- `btc_current.json` from `kalshi-public-docs.s3.amazonaws.com`

Sampling logic:

1. Read latest element of `timeseries.second` as current price.
2. Derive `source_timestamp_ms` from response metadata plus series position.
3. Keep raw payload optional for diagnostics when `--log-raw` is enabled.

## Module and CLI Layout

Add new package `scripts/surveillance/`:

1. `market_surveillance.py`
   - CLI entrypoint and loop orchestration.
2. `sources.py`
   - `ChainlinkPriceAdapter`
   - `KalshiWebsitePriceAdapter`
3. `pair_runtime.py`
   - boundary-based pair refresh (`:00/:15/:30/:45`), retry-after-20s, and collector rotation.
4. `writers.py`
   - JSONL/status/summaries with atomic flush strategy.
5. `schemas.py` (optional)
   - typed payload builders.

Planned CLI:

```powershell
.venv\Scripts\python.exe -m scripts.surveillance.market_surveillance `
  --config config/run_config.json `
  --duration-seconds 0 `
  --snapshot-seconds 1.0 `
  --pair-boundary-retry-seconds 20 `
  --resolved-refresh-seconds 600 `
  --output-jsonl data/diagnostic/market_surveillance/btc_15m_per_second.jsonl
```

`duration-seconds=0` means run continuously.

## Reliability and Safety Rules

1. Never block the 1-second snapshot loop on long refresh operations.
2. Use bounded retry/backoff for all HTTP requests.
3. Emit sample rows even if one source fails, with explicit error fields.
4. Keep strict typed parsing for prices and sizes; fail row fields, not whole process.
5. Rotate WS collectors only on confirmed pair change.
6. Preserve existing `resolved_15m_pairs.log` format for compatibility.

## Portability Strategy (Separate Repo Readiness)

Make portability an explicit constraint from phase 1:

1. New surveillance code must not depend on engine-loop internals.
2. Limit reused imports to stable shared modules:
   - `scripts.common.ws_collectors`
   - `scripts.common.decision_runtime` (runtime book model only)
   - `scripts.common.market_selection`
   - `scripts.common.api_transport`
3. Keep all paths configurable (no hardcoded project-root assumptions in core logic).
4. Wrap repo-specific defaults at CLI layer only.

Migration path:

1. Copy `scripts/surveillance/*` and minimal required `scripts/common/*`.
2. Keep file schemas unchanged.
3. Point output path to new repo storage root.

## Implementation Phases

## Phase 1: Skeleton + output contract

1. Create `scripts/surveillance/market_surveillance.py` entrypoint.
2. Implement JSONL writer and status writer.
3. Implement minimal tick loop with mocked source adapters.
4. Add tests for schema shape and writer behavior.

## Phase 2: Integrate existing quote runtime

1. Wire active pair discovery and WS collectors.
2. Populate best ask price/size for all four legs.
3. Add boundary-based pair rotation handling (`:00/:15/:30/:45`) and one retry-after-20s path.
4. Add integration test with captured WS fixtures.

## Phase 3: Underlying BTC price adapters

1. Implement `KalshiWebsitePriceAdapter` (`btc_current.json`).
2. Implement Polymarket RTDS Chainlink adapter (`crypto_prices_chainlink`, symbol `btc/usd`).
3. Add freshness/staleness checks and source timestamp fields.
4. Add adapter unit tests with recorded payloads.

## Phase 4: Resolved-pair maintenance integration

1. Add `resolved_pairs_loop` calling `compare_resolved_15m_pairs.run(...)`.
2. Add cadence controls and per-run summary counters.
3. Add failure isolation so resolved sync errors do not stop snapshot loop.

## Phase 5: Hardening + handoff

1. Add structured run summary JSON.
2. Add watchdog-friendly status file updates.
3. Add docs and runbook commands.
4. Verify portability by running from a copied minimal folder.

## Test Plan

1. Unit tests:
   - adapter parsing (`btc_current.json`, chainlink response parsing)
   - snapshot row builder with missing inputs
   - log append idempotency expectations
2. Integration tests:
   - 60-second dry run with live WS + HTTP adapters
   - pair rotation around 15m boundary
   - degraded mode when one source fails
3. Regression checks:
   - `resolved_15m_pairs.log` parser compatibility with existing enrichment scripts
   - snapshot JSONL backward-compatible field naming

## Acceptance Criteria

1. Process writes one snapshot row per second during runtime.
2. Each row contains:
   - Polymarket Chainlink BTC price
   - Kalshi website BTC price
   - best ask price + size for 4 legs
3. `resolved_15m_pairs.log` is updated on configured cadence.
4. Process survives transient source failures without stopping.
5. Output paths and runtime can be moved to a separate repo with only config/path changes.

## Risks and Open Decisions

1. If Polymarket RTDS chainlink topic degrades, fallback strategy is still an open design item.
2. Timestamp semantics for Kalshi website seconds series need a fixed rule.
3. If strict portability is immediate priority, a small extraction of shared WS runtime modules may be needed sooner.

## Recommended Next Step

Start with Phase 1 and Phase 2 together, so the new script can immediately produce the required best-ask surveillance rows while adapters for both underlying BTC sources are added in Phase 3.
