# decisions.md

Last updated: 2026-02-21

Purpose: Record important decisions with rationale so future changes can build on clear historical context.

How this file is structured:

- `Decision Log`: Reverse-chronological list of decisions.
- `Entry Template`: Standard fields for each decision.

## Decision Log

### 2026-02-21 - Module Extraction Strategy for arbitrage_engine.py

- Status: Accepted
- Context: `arbitrage_engine.py` had grown to 1333 lines, mixing CLI parsing, FSM logic, edge computation, and shared helpers with the core engine loop. This made the file hard to navigate and test.
- Decision: Extract four focused modules: `buy_fsm.py` (FSM runtime), `edge_snapshots.py` (edge computation), `engine_cli.py` (CLI parser), and `utils.py` (shared helpers). Each module has a single clear responsibility.
- Consequences: Engine reduced from 1333 to ~1013 lines (24% reduction). Modules are independently testable. Shared helpers are no longer duplicated across 9+ files.
- References: scripts/common/buy_fsm.py, scripts/common/edge_snapshots.py, scripts/run/engine_cli.py, scripts/common/utils.py

### 2026-02-21 - headers_factory Pattern for Kalshi WebSocket Auth

- Status: Accepted
- Context: Kalshi WebSocket headers include a timestamped signature that expires. When the engine reconnects after a disconnect, it reused stale pre-computed headers, causing auth failures.
- Decision: Pass a `headers_factory: Callable[[], Dict[str, str]]` to `BaseWsCollector` instead of static headers. The factory is called before each `websockets.connect()`, generating fresh signatures.
- Consequences: Reconnections use valid auth. Backwards compatible — `headers=` parameter still works for static headers. Diagnostic scripts continue using static headers (acceptable for short-lived captures).
- References: scripts/common/ws_transport.py, scripts/common/ws_collectors.py

### 2026-02-21 - Polymarket post_order Retry Wrapper

- Status: Accepted
- Context: `py-clob-client`'s `post_order` makes HTTP calls internally, bypassing the `ApiTransport` retry layer. Transient failures (timeouts, 502/503/504) caused immediate order rejection.
- Decision: Wrap `post_order` in `_post_order_with_retry()` using the same `RetryConfig` from `ApiTransport`, rather than reimplementing the signing/submission flow with `request_json`. This preserves SDK compatibility.
- Consequences: Transient HTTP errors are retried with exponential backoff. Non-transient errors (auth, validation) propagate immediately.
- References: scripts/common/buy_execution.py, scripts/common/api_transport.py

### 2026-02-18 - Strict Fail-Fast Market Identity Validation

- Status: Accepted
- Context: Loose text-based matching can hide data quality issues and select wrong contracts without obvious failures.
- Decision: Use strict identifier and time-window validation for discovery/matching, and raise explicit errors when expectations are not met.
- Consequences: Fewer silent mismatches, clearer debugging, and safer arbitrage logic; requires deliberate config/schema updates when upstream formats change.
- References: scripts/run/discover_active_btc_15m_markets.py, scripts/diagnostic/compare_resolved_15m_pairs.py

## Entry Template

- Date:
- Title:
- Status: Proposed | Accepted | Deprecated
- Context:
- Decision:
- Consequences:
- References:
