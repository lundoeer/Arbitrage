# Architecture & Code Review — Arbitrage Engine

**Date:** 2026-02-21  
**Reviewer:** Claude (Opus-class)  
**Scope:** Full repository review excluding `_oldproject/` and `_deprecated/`. `docs/Improve_lag.md` noted but excluded (not implemented).

---

## 1. Executive Summary

This repository implements a **cross-venue BTC 15-minute binary options arbitrage engine** operating between **Polymarket** (CLOB) and **Kalshi** (exchange). It discovers matching market pairs, connects to both venues via WebSocket, maintains real-time in-memory order books, evaluates arbitrage opportunities per tick, and submits cross-venue buy orders when a positive gross edge exceeds a configurable threshold.

**Overall assessment:** The codebase is well-structured for a solo/small-team project in active development. The core pipeline — from websocket ingestion through normalization, book management, decision gating, to order execution — is implemented with care and appropriate defensive coding. The architecture is sound and its layering is clean, though there are several areas where the system could be hardened for production-grade reliability. The documentation is notably thorough for a project of this size.

---

## 2. Architecture Overview

```
┌─────────────────────┐     ┌──────────────────────┐
│  Market Discovery   │     │   config/run_config   │
│  (discover_active_  │     │       .json           │
│   btc_15m_markets)  │     └──────────┬───────────┘
└─────────┬───────────┘                │
          │                            │
          ▼                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    arbitrage_engine.py                       │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ Polymarket   │  │   Kalshi     │  │   Decision Loop   │  │
│  │ WsCollector  │  │ WsCollector  │  │  (0.2s poll)      │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬──────────┘  │
│         │                 │                   │              │
│         └────────┬────────┘                   │              │
│                  ▼                            │              │
│  ┌──────────────────────────┐                 │              │
│  │  SharePriceRuntime       │◄────────────────┘              │
│  │  ├─NormalizedBookRuntime │                                │
│  │  └─4 OutcomeBooks        │                                │
│  └──────────────────────────┘                                │
│                  │                                           │
│                  ▼                                           │
│  ┌──────────────────────────┐   ┌──────────────────────┐     │
│  │  DecisionRuntime         │──▶│   BuyFsmRuntime      │     │
│  │  .evaluate()             │   │   IDLE→SUBMIT→       │     │
│  │   ├─health gate          │   │    AWAIT→COOLDOWN    │     │
│  │   ├─quote sanity         │   └──────────┬───────────┘     │
│  │   ├─execution gate       │              │                 │
│  │   ├─buy signal gate      │              ▼                 │
│  │   └─execution plan       │   ┌──────────────────────┐     │
│  └──────────────────────────┘   │  execute_cross_      │     │
│                                 │  venue_buy()          │     │
│                                 │   ├─KalshiApiBuyClient│     │
│                                 │   └─PolymarketApiBuy  │     │
│                                 │     Client             │     │
│                                 └──────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

### Module Responsibilities

| Module                               | LOC    | Responsibility                                                            |
| ------------------------------------ | ------ | ------------------------------------------------------------------------- |
| `arbitrage_engine.py`                | 1333   | Main entrypoint, CLI, engine loop, FSM, segment orchestration             |
| `decision_runtime.py`                | 677    | Quote sanity, candidate evaluation, execution plan building               |
| `normalized_books.py`                | 802    | In-memory order books (4 legs), event-driven updates, quote derivation    |
| `buy_execution.py`                   | 1152   | Cross-venue order submission, idempotency, Kalshi/Polymarket clients      |
| `ws_transport.py`                    | 359    | WebSocket base collector, reconnect logic, health tracking, JSONL writers |
| `ws_normalization.py`                | 261    | Polymarket/Kalshi raw-to-normalized event mapping                         |
| `ws_collectors.py`                   | 89     | Venue-specific WebSocket collector subclasses                             |
| `run_config.py`                      | 266    | Config loading, dataclass definitions for health/decision/buy configs     |
| `api_transport.py`                   | 123    | HTTP transport with retry/backoff                                         |
| `kalshi_auth.py`                     | 82     | Kalshi WebSocket authentication header generation                         |
| `market_selection.py`                | 86     | Market discovery output consumption and validation                        |
| `discover_active_btc_15m_markets.py` | ~700\* | Market discovery for Polymarket + Kalshi BTC 15m contracts                |

---

## 3. Strengths

### 3.1 Clean Layered Architecture

The pipeline follows a clean data flow: **collect → normalize → apply to books → derive quotes → evaluate decision → execute**. Each layer has clear boundaries. The `NormalizedBookRuntime` is the single source of truth for in-memory book state, and the `DecisionRuntime` is a pure function (static `evaluate`). This separation makes the system predictable and testable.

### 3.2 Defensive Coding Throughout

Helper functions like `_as_dict`, `_as_float_number`, `_as_non_empty_text`, `_to_float`, `_price_to_prob` are used pervasively. Virtually every external value is validated, coerced, or safely defaulted before use. This is especially important given the system processes untrusted venue data in real time with real money at stake.

### 3.3 Excellent Documentation

The `buying.md` and `websocket.md` documents are unusually thorough — they document not just the schema but the actual data flow, with real examples from production runs. These serve both as specification and as onboarding material.

### 3.4 Multi-Layered Safeguards for Live Trading

The buy path has 5+ independent safety checks before any order reaches an API:

1. `health_can_trade` (both streams alive and healthy)
2. `decision_ready` (all 4 leg quotes present and valid)
3. `execution_gate` (price bounds + time window checks)
4. `buy_signal_gate` (gross edge ≥ threshold)
5. `execution_plan` build validation
6. FSM gate (must be `IDLE`)
7. Max attempts per run
8. `--enable-buy-execution` CLI flag
9. Signal-level idempotency

This defense-in-depth approach is exactly right for a system that moves money.

### 3.5 Configurable and Observable

The system is highly configurable via `run_config.json` and CLI overrides. The optional logging tiers (`--log-raw-events`, `--log-decisions`, `--log-buy-decisions`, `--log-buy-execution`, `--log-edge-snapshots`, `--log-runtime-memory`) enable detailed post-run analysis without impacting production performance by default.

### 3.6 Monotonicity Guards on Book Updates

`_is_newest_or_tied_event` in `NormalizedBookRuntime` prevents out-of-order venue events from corrupting book state — a subtle but critical correctness detail for websocket-driven systems.

### 3.7 Deterministic Signal IDs

The `signal_id` is computed as a deterministic SHA-256-based hash over market identity + candidate + window end. This makes idempotency reliable and observable (the same market conditions will always produce the same signal ID).

---

## 4. Concerns and Recommendations

### 4.1 ⚠️ Critical: Sequential Two-Leg Execution (Execution Risk)

**Current:** `execute_cross_venue_buy` iterates legs sequentially in a `for` loop.

**Risk:** If leg 1 (e.g., Kalshi YES) succeeds but leg 2 (e.g., Polymarket NO) fails, you have a **naked one-sided position** with no hedge. The execution plan says `execution_mode: "two_leg_parallel"` but the actual execution is sequential.

**Impact:** In a fast-moving 15-minute market, this is a material financial risk. A network error, API rate limit, or transient failure on the second venue after the first succeeds leaves you exposed.

**Recommendation:**

- Consider true parallel submission (e.g., `asyncio.gather` for both legs).
- Implement an immediate unwind/cancel attempt if one leg fails and the other succeeds.
- At minimum, flag `partially_submitted` results with an alert mechanism (not just a counter).
- This is explicitly noted as out of scope in `buying.md` ("Hedge/unwind strategy… still out of scope"), but deserves prioritization before running with meaningful position sizes.

### 4.2 ⚠️ Critical: No Position Tracking or P&L Monitoring

The system has no awareness of existing positions, portfolio state, or cumulative exposure. Each decision cycle evaluates whether to enter a new trade independently, without knowing:

- What positions are currently open
- Whether previous orders were filled (vs. just submitted)
- Cumulative exposure across multiple 15-minute windows

**Recommendation:**

- Add a position tracker that polls or caches order fill status after submission.
- Enforce a portfolio-level max exposure limit.
- Track P&L per window to detect if the strategy is actually profitable.

### 4.3 ⚠️ High: `arbitrage_engine.py` is Too Large (1333 Lines)

This single file contains:

- The `BuyFsmRuntime` class and `BuyFsmState` enum
- Edge snapshot computation
- CLI parser (100+ lines)
- Main function with segment loop (530+ lines)
- The async engine runner
- Multiple helper functions

**Recommendation:** Extract into separate modules:

- `buy_fsm.py` — FSM state machine
- `edge_snapshots.py` — edge computation
- `engine_cli.py` — argument parsing
- Keep `arbitrage_engine.py` as the orchestrator

### 4.4 ⚠️ High: In-Memory Idempotency Only

`BuyIdempotencyState` is explicitly process-local. If the engine crashes and restarts, all idempotency state is lost. Given that the engine runs in a continuous segment loop, a crash mid-cooldown could result in duplicate submissions.

**Recommendation:**

- Persist idempotency state to a local file (e.g., `.json` or `sqlite`).
- On startup, load prior state and skip any `in_flight` signals that weren't confirmed.

### 4.5 ⚠️ High: No Fill Confirmation After Order Submission

Orders are submitted and the result status is recorded (`submitted`, `partially_submitted`, `rejected`), but there is no follow-up to confirm whether the orders actually **filled**. An order with status `submitted` only means the API accepted the request — it does not mean shares were acquired.

**Recommendation:**

- Poll order status after submission using the Kalshi and Polymarket order status APIs.
- Track fill status as part of the execution result.
- Consider this essential for any position tracking.

### 4.6 Medium: Synchronous Buy Execution Blocks the Decision Loop

Order submission (`execute_cross_venue_buy`) is called inline within the async `_decision_loop`. The Kalshi client uses `requests` (synchronous HTTP) within an `asyncio` context. This blocks the decision loop for the entire duration of the order submission + API roundtrip.

**Impact:** During a slow API response or retry, the decision loop is paused, meaning health snapshots are not refreshed and quotes become stale.

**Recommendation:**

- Run order execution in a separate thread via `asyncio.to_thread(...)` or an executor.
- Alternatively, switch the buy clients to an async HTTP library.

### 4.7 Medium: Health Thresholds Are Very Tight

Default health config:

```json
"transport_heartbeat_stale_seconds": 1.5,
"market_data_stale_seconds": 1.5,
"max_lag_ms": 2000,
"max_source_age_ms": 2500
```

The 1.5-second stale thresholds are quite aggressive. Run summaries show `health_can_trade=false` is a frequent blocker. While conservative is safer than permissive, overly tight thresholds reduce opportunity capture.

**Recommendation:**

- Analyze historical health data to find the right balance.
- Consider differentiated thresholds per venue (Kalshi typically has higher latency than Polymarket).
- Make the thresholds adjustable via CLI in addition to config.

### 4.8 Medium: `Polymarket` Client Uses `py-clob-client` `post_order` Directly

The `PolymarketApiBuyClient.place_buy_order` calls `self.clob_client.post_order(...)` which bypasses the custom `ApiTransport` retry logic entirely. The `ApiTransport` instance on the client is never used for order submission.

**Impact:** Polymarket order submissions do not get retry/backoff protection despite `api_retry.include_post=true` being configured.

**Recommendation:**

- Either route Polymarket order submission through `ApiTransport.request_json` (with the signed payload), or
- Wrap `clob_client.post_order` in a retry wrapper that respects the configured retry policy.

### 4.9 Medium: No Structured Logging

All console output uses raw `print()` statements. There is no log-level control (debug/info/warning/error), no structured log format, and no way to redirect engine logs to a file handler.

**Recommendation:**

- Adopt Python's `logging` module with structured formatters.
- Add log levels so diagnostic output can be separated from operational messages.
- Consider JSON-structured logging for machine-parseable output.

### 4.10 Medium: Kalshi Auth Signature Is Computed at Connection Time Only

`resolve_kalshi_ws_headers()` generates a single timestamp-signed header set. If the WebSocket reconnects (which the retry logic supports), the headers may contain an expired timestamp. Whether Kalshi validates timestamp freshness on reconnect is unclear, but stale auth headers could cause silent reconnection failures.

**Recommendation:**

- Re-compute auth headers on each reconnection attempt, not just once at startup.
- The `KalshiWsCollector.__init__` currently calls `resolve_kalshi_ws_headers()` once. Instead, move header generation into the reconnect path.

### 4.11 Low: Duplicate Helper Functions Across Modules

There are near-identical utility functions duplicated across modules:

- `_as_dict`, `_as_float`, `_to_float` appear in 4+ files with minor variations.
- `now_ms()` is defined in both `ws_transport.py` and `buy_execution.py`.
- `_normalize_kalshi_pem` exists in both `kalshi_auth.py` and `buy_execution.py`.

**Recommendation:**

- Create a shared `utils.py` module for common type coercions and time utilities.

### 4.12 Low: Test Coverage is Minimal

Only 3 test files exist:

- `test_buy_execution_idempotency.py` (2 tests)
- `test_buy_fsm_runtime.py` (2 tests)
- `test_run_config_buy_execution_retry.py` (2 tests)

**Missing test coverage for:**

- `NormalizedBookRuntime` (the most complex module — 800 lines of book manipulation logic)
- `DecisionRuntime.evaluate` (the core decision pipeline)
- `ws_normalization` (Polymarket/Kalshi event normalization)
- `build_quote_sanity_and_canonical` (candidate scoring)
- Edge cases in `_evaluate_time_window` and `_evaluate_price_bounds`

**Recommendation:**

- Prioritize tests for `NormalizedBookRuntime` — order book logic bugs are the most dangerous class of error in this system.
- Add parameterized tests for the decision pipeline with various health/quote/time states.

### 4.13 Low: `.env` File Is Present in Repository Root

A `.env` file (2348 bytes) exists at the project root. While `*.env` is in `.gitignore`, the file itself contains API keys and private keys for live trading venues.

**Recommendation:**

- Verify this file is not tracked in git history.
- Consider using a secret manager or at minimum document the expected env vars in the README rather than shipping an `.env` template.

### 4.14 Low: `data/` Directory Contains Large Runtime Artifacts

The `data/` directory contains ~60+ files totaling significant disk usage (some JSONL files are 5-18 MB). While `data/` is gitignored, there's no rotation or cleanup mechanism.

**Recommendation:**

- Add a configurable data retention policy or a cleanup script.
- Consider compressing archived run data.

### 4.15 Low: No `__init__.py` Files in Package Directories

`scripts/`, `scripts/common/`, `scripts/run/`, `scripts/diagnostic/`, and `tests/` lack `__init__.py` files. The codebase relies on `sys.path` manipulation (in `conftest.py`) and Ruff's `src = ["scripts"]` config. This works but is fragile and non-standard.

**Recommendation:**

- Add `__init__.py` files to all package directories.
- Consider packaging with a proper `pyproject.toml` `[project]` section.

---

## 5. Code Quality Assessment

| Aspect              | Rating | Notes                                                                                                                                                        |
| ------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Readability**     | ★★★★☆  | Clean naming conventions; generous docstrings in key classes. Some functions are long but well-commented.                                                    |
| **Correctness**     | ★★★★☆  | Book update logic is carefully implemented. Decision gating is thorough. The sequential execution risk (§4.1) is the main concern.                           |
| **Maintainability** | ★★★☆☆  | `arbitrage_engine.py` is too monolithic. Duplicate helpers add maintenance burden. Lack of tests makes refactoring risky.                                    |
| **Reliability**     | ★★★☆☆  | Good reconnect logic and health monitoring. But in-memory-only idempotency, no fill confirmation, and blocking sync calls in async context are risk factors. |
| **Security**        | ★★★★☆  | Proper PSS signature handling for Kalshi. HMAC for Polymarket L2. Funder verification against on-chain state. Some concern over `.env` management.           |
| **Observability**   | ★★★★☆  | Excellent logging options and run summaries. Missing structured logging and real-time alerting.                                                              |

---

## 6. Data Flow Correctness Review

### Quote Derivation Path

Verified that the quote derivation (`executable_price_feed`) correctly reads from in-memory book state through `OutcomeBook.top()`, which sorts bid levels descending and ask levels ascending to find best prices. This is correct.

### Kalshi Ask Reconstruction

Kalshi's API provides only bid ladders per side. The reconstruction formula `ask_yes = 1 - bid_no` and `ask_no = 1 - bid_yes` is mathematically correct for a binary market where YES + NO probabilities sum to 1.

### Gross Edge Calculation

`gross_edge = 1.0 - (yes_ask + no_ask)` where the YES and NO legs span different venues. When this is positive, buying both sides costs less than the guaranteed $1 payout. This is the core arbitrage signal and it's implemented correctly.

### Sizing Policy

`size = floor(min(max_size_cap_per_leg, max_spend / total_ask))` — conservative and correct. Both legs use the same size, which is correct for binary market arbitrage.

---

## 7. Security Observations

- **Kalshi auth** uses RSA-PSS with SHA-256, matching Kalshi's documented API requirements. ✅
- **Polymarket auth** uses HMAC-SHA256 with L2 credentials. ✅
- **On-chain funder verification** derives the proxy/safe funder wallet from the exchange contract and compares against the configured value. This prevents trading from the wrong wallet. ✅
- **PEM key normalization** handles various edge cases (quoted, escaped newlines, whitespace). ✅
- **No secrets in source code** — all credentials come from env vars. ✅

---

## 8. Summary of Priority Actions

| Priority | Action                                                        | Section    |
| -------- | ------------------------------------------------------------- | ---------- |
| **P0**   | Address sequential execution risk / implement unwind strategy | §4.1       |
| **P0**   | Add position tracking and fill confirmation                   | §4.2, §4.5 |
| **P1**   | Persist idempotency state to disk                             | §4.4       |
| **P1**   | Fix Polymarket client bypassing retry transport               | §4.8       |
| **P1**   | Add test coverage for `NormalizedBookRuntime`                 | §4.12      |
| **P2**   | Extract `arbitrage_engine.py` into smaller modules            | §4.3       |
| **P2**   | Move buy execution off the decision loop thread               | §4.6       |
| **P2**   | Add structured logging                                        | §4.9       |
| **P2**   | Re-compute Kalshi auth on reconnect                           | §4.10      |
| **P3**   | Consolidate duplicate helpers                                 | §4.11      |
| **P3**   | Add data retention / cleanup                                  | §4.14      |
| **P3**   | Add package `__init__.py` files                               | §4.15      |

---

_This review is based on static analysis of the codebase at the stated date. No code changes were made._
