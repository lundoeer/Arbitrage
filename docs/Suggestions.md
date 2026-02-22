# Suggestions & Responses to Review Comments

**Date:** 2026-02-21  
**In response to:** `docs/ClaudeOpusReviewComments.md`

---

## Section 2 — Decision Loop & Quote Sanity

### Decision Loop (0.2s poll) — Is this the decision heartbeat? Should it be configurable? Should it be faster?

Yes, this is the decision heartbeat. It's defined in `_decision_loop` inside `arbitrage_engine.py` (line 425):

```python
poll_s = max(0.05, float(decision_poll_seconds))
```

It is already configurable via the CLI flag `--decision-poll-seconds` (default `0.2`). The hard floor is `0.05` (50ms).

**Should it be faster?** It depends on what's bottlenecking you:

- **If health gates are the main blocker** (which your run data suggests — `can_trade_false` significantly outnumbers `can_trade_true`), polling faster won't help. The bottleneck is data freshness, not poll frequency.
- **If edges appear and vanish within 200ms**, then yes, faster polling would capture more opportunities. But in BTC 15-minute markets, price movements are generally slower than 200ms.
- **The real constraint:** `DecisionRuntime.evaluate()` is a pure function that takes the current snapshot. Between polls, the WebSocket callbacks (`on_event`) are continuously updating the in-memory books. The poll just reads the latest state. So **50ms vs 200ms is the difference between catching an edge 150ms sooner**, which matters only if edges are extremely transient.

**My recommendation:** Keep 0.2s as default. If you observe from logged edge snapshots that profitable edges consistently last < 500ms, try 0.1s. Going below 0.1s is unlikely to help and will increase CPU usage with no benefit.

### Quote Sanity — How does this work exactly?

Quote sanity is the pipeline that validates all four order book legs before any decision can be made. Here's the exact flow:

**Step 1: `validate_leg_quote()`** — Called for each of the 4 legs (polymarket_yes, polymarket_no, kalshi_yes, kalshi_no). Checks:

- `best_bid` and `best_ask` exist and are not None
- `best_bid_size` and `best_ask_size` exist and are positive
- Prices are within [0.0, 1.0] (valid probabilities)
- Quote is not crossed (bid ≤ ask)
- If **any** leg fails, `decision_ready = false` and the specific failure reason is recorded

**Step 2: Cross-venue candidate scoring** — Two candidates are evaluated:

1. **Buy Polymarket YES + Buy Kalshi NO** → `total_ask = pm_yes_ask + kx_no_ask`
2. **Buy Kalshi YES + Buy Polymarket NO** → `total_ask = kx_yes_ask + pm_no_ask`

For each: `gross_edge = 1.0 - total_ask`. If positive, buying both sides costs less than the $1.00 payout.

**Step 3: Best candidate selection** — The candidate with the highest `gross_edge` is selected as the `best_cross_venue_buy_candidate`.

**What "decision_ready" means:** All 4 legs have valid bid/ask prices with positive sizes, and no quote is crossed. This is a prerequisite for any further evaluation — if this fails, execution gate and buy signal gate are never even evaluated.

**Key thing to understand:** Quote sanity does **not** check health. A leg can have a valid cached quote in memory but the underlying stream could be stale. That's why `health_can_trade` is checked separately and independently — it looks at transport heartbeat age and data freshness. Both must pass for `can_trade = true`.

---

## 4.1 — Sequential Two-Leg Execution

Your approach makes sense: accept that partial fills and one-leg executions will happen, trade as fast as possible, and build mitigation around positions rather than blocking execution. That's a pragmatic production stance.

One concrete action for the short term: when `execute_cross_venue_buy` returns `partially_submitted`, the current engine logs it as a counter but takes no further action. Consider adding an immediate console alert (print to stderr or a distinct log line) so you notice one-sided fills in real time during monitored runs. This is a trivial change but gives you visibility until the position monitoring system exists.

---

## 4.2 — Position Tracking & P&L

Agreed this is the next major feature. I'll include this in the plan below.

---

## 4.3 — Extracting `arbitrage_engine.py`

Agreed. This is in the "do today" section of the plan below.

---

## 4.4 — In-Memory Idempotency: Position Monitoring vs. Persisted State

**My opinion:** Position monitoring is the right long-term solution, but it serves a different purpose than idempotency. They solve different failure modes:

| Failure                                                      | Position monitoring fixes it?                             | Persisted idempotency fixes it?      |
| ------------------------------------------------------------ | --------------------------------------------------------- | ------------------------------------ |
| Engine crashes mid-cooldown, restarts, resubmits same signal | ❌ If order isn't filled yet, monitoring sees no position | ✅ Sees signal was already attempted |
| Engine submits, crashes before cooldown starts               | ❌ Same as above                                          | ✅ Same as above                     |
| Two leg fills diverge (partial hedge)                        | ✅ Sees net position, can hedge                           | ❌ Not its job                       |
| P&L tracking across windows                                  | ✅                                                        | ❌                                   |

The risk scenario is: you submit a Kalshi order, the engine crashes, restarts, the signal conditions still hold (same 15-minute window, same edge), and the engine submits again. Now you have 2× the intended position on one leg. Position monitoring _can_ catch this **if** the first order has already filled by the time the engine restarts — but in a fast restart (< 1 second), the first order might still be in-flight on the exchange.

**My recommendation:** You don't need a complex persistence layer. A single JSON file written atomically on each FSM transition (5 lines of code) would handle the restart scenario. But if you're confident you won't be running with meaningful size before position monitoring is built, then it's fine to defer.

---

## 4.5 — Fill Confirmation

Agreed, bundled with position monitoring.

---

## 4.6 — Synchronous Buy Execution Blocking the Decision Loop

**My opinion:** Your instinct is sound, and I'd adjust my recommendation. Here's my reasoning:

**Arguments for keeping blocking execution (your position):**

- During order submission (typically 200-500ms), you don't want the decision loop to fire again and potentially queue another trade while the first is still in-flight
- The FSM gate already prevents this (`can_accept_new_signal()` returns false during `SUBMITTING` state), so even if the loop ran, it would be a no-op
- Simpler to reason about — no concurrency hazards

**The remaining concern (minor):** While the decision loop is blocked, health snapshots don't refresh. If the API call takes 3+ seconds (timeout, retry), the next decision loop iteration will use a health snapshot that's 3+ seconds stale. But the health check itself will catch this — `transport_heartbeat_stale_seconds` would trigger `heartbeat_timeout`, blocking the next trade.

**Verdict:** Keep it synchronous. The FSM gate already handles re-entrancy, and the health checks self-correct for staleness. The only scenario where async execution would matter is if you wanted to run diagnostics or update health metrics _during_ order submission, which isn't worth the complexity.

---

## 4.7 — Health Metric Accuracy (Referencing `Improve_lag.md`)

I've read both the `Improve_lag.md` suggestions and the current implementation in `ws_transport.py` and `ws_normalization.py`. Here's my assessment of **current accuracy**:

### Current Health Metrics — Accuracy Analysis

**1. Transport heartbeat age (`last_message_recv_ms`):**

- **Accuracy: HIGH.** This uses `now_ms() - last_message_recv_ms` where `last_message_recv_ms` is stamped at the moment `ws.recv()` returns. This is reliable and doesn't depend on venue clocks.
- **No issues here.**

**2. Market data heartbeat age (`last_data_recv_ms`):**

- **Accuracy: HIGH.** Same mechanism as transport, but filtered to only market-data events (excludes heartbeats/control messages). Also reliable.

**3. Lag (`lag_ms = recv_ms - source_timestamp_ms`):**

- **Accuracy: PROBLEMATIC.** This is where `Improve_lag.md` is spot-on. The problems:
  - **Kalshi timestamp source:** `to_epoch_ms(data.get("ts") or data.get("timestamp") or data.get("time"))` — Kalshi's `ts` field is often **second-precision** (e.g., `1708000000` vs `1708000000123`). A second-precision source timestamp means the lag measurement has up to ±999ms of noise. With a 2000ms `max_lag_ms` threshold, this means a true lag of 1100ms could appear as 100ms or 2100ms depending on where in the second the message arrived.

  - **Clock skew:** `recv_ms` is your local clock. `source_timestamp_ms` is the venue's clock. Any clock skew between your machine and the venue server directly inflates or deflates the lag number. Even 200ms of clock drift (common) would systematically bias the metric.

  - **Polymarket timestamp:** `to_float(message.get("timestamp"))` — Polymarket's timestamp field appears to be millisecond-precision, which is better. But still subject to clock skew.

**4. `health_snapshot.decision_ok` — the actual decision gate:**

- **Accuracy: GOOD for the purpose.** Because `decision_ok` is the AND of `transport_ok` and `market_data_ok`, and both of those rely on the **accurate** recv-based heartbeat metrics, the overall gate works well. The lag metric is an additional filter but not the primary one.

### What I'd Prioritize from `Improve_lag.md`

Your `Improve_lag.md` has 8 suggestions. Here's my prioritized subset:

1. **Suggestion 1 (Timestamp precision) — Do first.** The biggest accuracy win. For Kalshi, check if `data.get("ts")` vs `data.get("timestamp")` differ in precision. If `ts` is seconds and `timestamp` is millis, swap the fallback order. Also emit `source_ts_precision: "s"|"ms"` so you can filter during analysis.

2. **Suggestion 3 (Rolling lag stats) — Do second.** Replace `last_lag_ms` with a rolling window (last 20 samples). Report `p50`/`p90` instead of the last value. This smooths out the second-precision noise and gives a much better signal. This is ~30 lines of code.

3. **Suggestion 6 (Timestamp quality flags) — Do alongside 1.** Mark events where source timestamp is missing, future, or low-precision. This costs almost nothing to implement and makes your logged data much more analyzable.

4. **Suggestions 2, 4, 5, 7, 8 — Defer.** Clock offset estimation, stage-separated lag, per-event-type tracking, and sequence integrity are all valuable but are refinements rather than accuracy fixes. Build them when you have more production data to analyze.

**Bottom line for your concern:** Your health gate is actually **more accurate than the lag metric** because it relies on recv-based heartbeat timing (which is purely local clock). Lag is a useful diagnostic signal but its accuracy is limited by venue timestamp precision and clock skew. The `Improve_lag.md` suggestions are well-structured — prioritize #1 and #3 for the biggest gains.

---

## 4.8 — Polymarket Client Bypassing Retry Transport

**Which approach is best:** I recommend **wrapping `clob_client.post_order` in a retry wrapper** rather than rewriting to use `ApiTransport.request_json` directly. Here's why:

- `py-clob-client`'s `post_order` handles internal signing, serialization, and submission as a single call. Reimplementing this with `ApiTransport.request_json` means you'd need to manually construct the signed payload, set the auth headers, and submit — essentially reimplementing `post_order` yourself. That's fragile and will break when `py-clob-client` updates.

- A retry wrapper is ~15 lines:

```python
def _retry_post_order(clob_client, signed_order, *, order_type, post_only, retry_config):
    last_exc = None
    for attempt in range(retry_config.max_attempts):
        try:
            return clob_client.post_order(signed_order, orderType=order_type, post_only=post_only)
        except Exception as exc:
            last_exc = exc
            if attempt + 1 < retry_config.max_attempts:
                delay = retry_config.base_backoff_seconds * (2 ** attempt)
                jitter = delay * retry_config.jitter_ratio * random.random()
                time.sleep(delay + jitter)
    raise last_exc
```

This gives you the same retry behavior as `ApiTransport` without coupling to its HTTP internals. You already have `RetryConfig` available from `api_transport.py`.

---

## 4.10 — Kalshi Auth Reconnect — Size of Change

**Size: Small (< 20 lines changed).** Here's exactly what to change:

Currently, `resolve_kalshi_ws_headers()` is called once in the segment loop (line 1042 of `arbitrage_engine.py`), and the result is passed to `KalshiWsCollector.__init__` as `headers`. The collector passes these to `BaseWsCollector.__init__`, which stores them as `self.headers` and uses them on every `websockets.connect()` call.

**The fix:** Instead of passing pre-computed headers, pass a callable that generates fresh headers on each connection:

1. In `BaseWsCollector.__init__`, accept `headers_factory: Optional[Callable[[], Dict[str, str]]]` alongside (or instead of) `headers`.
2. In `BaseWsCollector.run()`, at line 249, call `current_headers = self.headers_factory() if self.headers_factory else self.headers` before `websockets.connect()`.
3. In `arbitrage_engine.py`, pass `headers_factory=resolve_kalshi_ws_headers` instead of `headers=segment_kalshi_headers`.

That's ~10 lines changed across 2 files. Low risk, low effort. Can be done today.

---

## 4.11 — Duplicate Helpers

In the plan below as a quick win for today.

---

## 4.12 — Test Coverage

I'd prioritize tests for `NormalizedBookRuntime` specifically. The key scenarios to test:

1. **Polymarket book_snapshot replaces existing state** (not accumulates)
2. **Polymarket book_top_update removes stale prices above new best bid**
3. **Kalshi orderbook_delta without prior snapshot is ignored** (line 595-596)
4. **Kalshi ask reconstruction** (`ask_yes = 1 - bid_no`) produces correct results
5. **Out-of-order event rejection** via `_is_newest_or_tied_event`
6. **Depth pruning** doesn't discard better prices

These are the most dangerous code paths and should be prioritized. This is a medium-sized task (probably 1-2 hours), and I'd suggest doing it after the module extraction (§4.3).

---

## 4.15 — `__init__.py` Files

In the plan below as a quick win for today.

---

## High-Level Plan — Ordered by Priority

### Today — Small Changes (1-2 hours)

| #   | Change                                                                                                                                | Size   | Risk                              |
| --- | ------------------------------------------------------------------------------------------------------------------------------------- | ------ | --------------------------------- |
| 1   | Add `__init__.py` files to `scripts/`, `scripts/common/`, `scripts/run/`, `scripts/diagnostic/`, `tests/`                             | 5 min  | None                              |
| 2   | Create `scripts/common/utils.py` and consolidate duplicate helpers (`now_ms`, `_as_dict`, `_to_float`, `_normalize_kalshi_pem`, etc.) | 30 min | Low — mechanical, run tests after |
| 3   | Extract `BuyFsmRuntime` + `BuyFsmState` → `scripts/common/buy_fsm.py`                                                                 | 20 min | Low — move + update imports       |
| 4   | Extract edge snapshot computation → `scripts/common/edge_snapshots.py`                                                                | 15 min | Low — same                        |
| 5   | Extract CLI argument parsing → `scripts/run/engine_cli.py`                                                                            | 15 min | Low — same                        |
| 6   | Kalshi auth reconnect fix (headers_factory pattern)                                                                                   | 15 min | Low                               |
| 7   | Add retry wrapper for Polymarket `post_order`                                                                                         | 15 min | Low                               |

### This Week — Medium Changes

| #   | Change                                                               | Size    | Dependency |
| --- | -------------------------------------------------------------------- | ------- | ---------- |
| 8   | Write `NormalizedBookRuntime` test suite (6+ test cases)             | 1-2 hrs | After #2-5 |
| 9   | Implement lag timestamp precision detection (Improve_lag.md #1 + #6) | 1 hr    | None       |
| 10  | Add rolling lag stats to health snapshots (Improve_lag.md #3)        | 1 hr    | After #9   |
| 11  | Add partial-submit alert (print to stderr on `partially_submitted`)  | 10 min  | None       |

### Next Sprint — Position Monitoring (Large)

| #   | Change                                                                                            |
| --- | ------------------------------------------------------------------------------------------------- |
| 12  | Design position monitoring system (order status polling, fill tracking, net position computation) |
| 13  | Implement Kalshi order status polling client                                                      |
| 14  | Implement Polymarket order status polling client                                                  |
| 15  | Build `PositionRuntime` — tracks open positions, fills, and net exposure                          |
| 16  | Integrate position monitoring into the engine loop                                                |
| 17  | Add portfolio-level max exposure limit                                                            |

I recommend starting items 12-17 with a separate design document before implementation.
