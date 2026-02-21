# context.md

Last updated: 2026-02-21

Purpose: Capture the current project snapshot so contributors can quickly understand where things stand right now.

How this file is structured:

- `Current State`: What is implemented and running today.
- `In Progress`: Work currently underway.
- `Known Gaps`: Missing capabilities, risks, or unknowns.
- `References`: Pointers to key files, docs, and dashboards.

## Current State

### Architecture

- Active market discovery for Polymarket and Kalshi with strict fail-fast validation.
- Discovery uses identifier prefixes plus window-end alignment for 15m market selection.
- Decision loop polls at 0.2s (configurable via `--decision-poll-seconds`), evaluating four legs: polymarket_yes, polymarket_no, kalshi_yes, kalshi_no.
- Health gating uses recv-based heartbeat timing (transport + market data freshness) plus source timestamp lag.
- Buy execution uses FSM (`BuyFsmRuntime`) with states: IDLE → SUBMITTING → AWAITING_RESULT → COOLDOWN → IDLE.
- Cross-venue edge detection compares `total_ask = yes_ask + no_ask` against $1.00 payout.

### Module Layout

| Module                | Location          | Responsibility                                                                                         |
| --------------------- | ----------------- | ------------------------------------------------------------------------------------------------------ |
| `arbitrage_engine.py` | `scripts/run/`    | Main entry point — segment loop, WS lifecycle, decision loop                                           |
| `engine_cli.py`       | `scripts/run/`    | CLI argument parser (`build_parser`)                                                                   |
| `buy_fsm.py`          | `scripts/common/` | `BuyFsmRuntime` and `BuyFsmState` — FSM for buy execution                                              |
| `edge_snapshots.py`   | `scripts/common/` | `build_edge_snapshot`, `cross_venue_edge`, `extract_leg_quote`                                         |
| `utils.py`            | `scripts/common/` | Shared helpers: `now_ms`, `as_dict`, `as_float`, `as_int`, `as_non_empty_text`, `normalize_kalshi_pem` |
| `ws_transport.py`     | `scripts/common/` | `BaseWsCollector` — WebSocket connection lifecycle with reconnect                                      |
| `ws_collectors.py`    | `scripts/common/` | `KalshiWsCollector`, `PolymarketWsCollector`                                                           |
| `ws_normalization.py` | `scripts/common/` | Raw message → normalized event transformation                                                          |
| `buy_execution.py`    | `scripts/common/` | `KalshiApiBuyClient`, `PolymarketApiBuyClient`, execution pipeline                                     |
| `decision_runtime.py` | `scripts/common/` | `DecisionRuntime` — evaluates trading signals from book state                                          |
| `kalshi_auth.py`      | `scripts/common/` | Kalshi WebSocket auth header generation                                                                |
| `api_transport.py`    | `scripts/common/` | HTTP transport with retry/backoff and JSON parsing                                                     |
| `run_config.py`       | `scripts/common/` | Config parsing from `config/run_config.json`                                                           |

### Running the Engine

```bash
# Always use venv Python (system Python lacks websockets and other deps)
.venv\Scripts\python.exe -m scripts.run.arbitrage_engine --duration-seconds 300 --no-enable-buy-execution --log-summary

# Key flags:
#   --skip-discovery          Use cached pair (only if pair cache is fresh)
#   --no-enable-buy-execution Disable trading
#   --log-runtime-memory      1s snapshots of full book state
#   --log-raw-events          Write raw WS messages to data/websocket_*/
#   --log-decisions           Write decision samples to data/decision_log__*.jsonl
#   --log-edge-snapshots      Write edge snapshots to data/gross_edge_snapshot__*.jsonl
```

**Important**: `--skip-discovery` requires a fresh pair cache (`data/market_pair_cache.json`). If the cache contains expired market windows, the engine will find no active segments and exit with 0 messages.

### WebSocket Architecture

**Polymarket**: Splits real-time data across CLOB WebSocket (order book depth, trades, fills) and RTDS (fast crypto price feeds). Both sockets are kept open.

**Kalshi**: Single WebSocket endpoint with explicit channel subscriptions (`ticker`, `orderbook_delta`). Uses `headers_factory` pattern so auth signatures are regenerated on each reconnect.

**Normalization**: Polymarket raw messages often contain multiple price updates (YES + NO tokens) per frame, so `polymarket_normalized_events > polymarket_raw_messages` (~1.7x) is expected. Kalshi is 1:1.

## In Progress

- No active work items. See `tasks.md` for the priority queue.

## Known Gaps

- Lag metric accuracy is limited by Kalshi's second-precision timestamps and clock skew (see `docs/Improve_lag.md`).
- No position monitoring or P&L tracking.
- No persisted idempotency — restart during cooldown can cause duplicate submissions.
- Upstream API schema changes can break strict checks; this should fail clearly and trigger intentional updates.

## References

- `docs/ClaudeOpusSuggestions.md` — Prioritized improvement plan
- `docs/ClaudeOpusReviewComments.md` — Architecture review
- `docs/Improve_lag.md` — Health metric accuracy improvements
- `config/run_config.json` — Runtime configuration
- `data/` — Engine output (logs, snapshots, summaries)
