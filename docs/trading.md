# Trading Execution Design (Buy + Sell + Global Lock) - Implementation (24/02/2026)

This document describes the current trading path (buy + sell) implemented in:

- `scripts/common/decision_runtime.py`
- `scripts/common/buy_execution.py`
- `scripts/common/position_runtime.py`
- `scripts/common/execution_lock.py`
- `scripts/common/buy_fsm.py`
- `scripts/run/engine_loop.py`
- `scripts/run/arbitrage_engine.py`
- `scripts/common/run_config.py`

## Disclaimer (Current Architecture)

This is not yet a unified buy/sell/hedge FSM.

- Submit lifecycle is still handled by `BuyFsmRuntime` (cooldown + one active submission state machine).
- Cross-action serialization is handled by `ExecutionLockRuntime`.
- Sell and buy share the same submission executor (`execute_cross_venue_buy(...)`) but are action-aware (`buy` vs `sell`).

## Implemented Trading Capabilities

- Two-leg cross-venue buy execution.
- Two-leg cross-venue sell execution.
- Action priority in each tick: `sell` first, then `buy`.
- Global execution lock across actions.
- Strict fail-closed gating on position health / reconcile path.
- Action-aware execution dispatch to venue clients.
- In-memory idempotency keyed by `signal_id`.

## Trading Loop Overview

Per decision tick in `engine_loop`:

1. Refresh FSM cooldown state.
2. Refresh lock terminal status from open-order state.
3. Build quotes snapshot and position snapshot.
4. Evaluate decision runtime:
   - buy signal and buy plan
   - sell signal and sell plan
   - `selected_action_hint` (`sell` if valid, else `buy` if valid)
5. Choose executable action (sell preferred if enabled and valid).
6. Run gates in order:
   - action enablement
   - position health
   - execution lock
   - FSM state
   - per-plan position gate (`PositionRuntime.evaluate_execution_plan`)
7. Submit two legs in parallel if accepted.
8. Record result into position runtime and execution lock.
9. Move FSM to cooldown/idle.

## Buy Path (Summary)

Signal and sizing:

- Candidates:
  - `buy_polymarket_yes_and_kalshi_no`
  - `buy_kalshi_yes_and_polymarket_no`
- Edge rule: `1.0 - total_ask >= buy.min_gross_edge_threshold`
- Shared size uses min of:
  - spend cap (`max_spend_per_market_usd / total_ask`)
  - config cap (`max_size_cap_per_leg`)
  - top-of-book ask cap (`min(best_ask_size) * best_ask_size_safety_factor`)
- Rounded down to whole contracts.
- Optional min size and min notional gates.

Order policy:

- Planner emits `action="buy"`, `order_kind="market"`, `time_in_force="fak"`.
- Market intent is emulated with aggressive limit pricing.
- Buy market emulation limit is:
  - `limit_price = min(0.99, best_ask + buy.market_emulation_slippage)`

## Sell Path (Summary)

Signal and sizing:

- Candidates:
  - `sell_polymarket_yes_and_kalshi_no`
  - `sell_kalshi_yes_and_polymarket_no`
- Edge rule: `total_bid - 1.0 >= sell.min_gross_edge_threshold`
- Shared size uses min of:
  - available position cap (per leg, authoritative runtime state)
  - top-of-book bid cap (`min(best_bid_size) * best_bid_size_safety_factor`; `0` disables by treating as `1.0`)
  - optional config cap (`sell.max_size_cap_per_leg`, disabled at `0`)
- Rounded down to whole contracts.
- Optional min size and min notional gates (`0` disables).

Sell order policy:

- Planner emits `action="sell"`, `order_kind="market"`, `time_in_force="fak"`.
- Sell market emulation limit is:
  - `limit_price = max(0.01, best_bid - sell.market_emulation_slippage)`

## Venue Sell Schema (Implemented)

Kalshi (`POST /trade-api/v2/portfolio/orders`):

- Uses `action: "sell"` (distinct from buy).
- Uses `side: "yes" | "no"` for outcome side.
- Uses `yes_price_dollars` or `no_price_dollars` based on side.
- Uses `type: "limit"` + aggressive price for market emulation.

Polymarket (py-clob-client signed order + post_order):

- Sell signing uses `side="SELL"` (not `BUY`).
- Market emulation for sell uses signed limit-style order with low crossing price.
- Submission includes order type derived from `fak`.

## Critical Safeguard: Prevent Unintended Kalshi Sell Without Position

### Problem

If a venue were to accept a sell with no inventory (or internally translate behavior in a way that creates opposite exposure), strategy safeguards could be bypassed if we submitted that order.

### Last Safeguard (Pre-Submit) - Step by Step

This is the final gate before any API call:

1. Engine picks an action/plan candidate.
2. Engine calls `PositionRuntime.evaluate_execution_plan(execution_plan=...)`.
3. For every sell leg, runtime reads tracked net position for the exact key:
   - `venue|instrument_id|outcome_side`
4. Runtime computes:
   - `available_contracts = max(0, net_contracts)`
   - `required_contracts = leg.size`
5. If `available_contracts < required_contracts`, the leg is blocked with:
   - `reason = "insufficient_available_position_for_sell"`
6. If any sell leg is blocked, `allowed=false` for the plan.
7. Engine does not call the venue API for that sell action.

Outcome:

- A sell with zero position is blocked locally before Kalshi submission.
- No sell request is sent, so venue-side interpretation cannot create unintended opposite exposure from that blocked leg.

### Earlier Independent Safeguard (Plan Build)

Before pre-submit gating, sell plan construction also caps size from current position snapshot:

- `cap_by_position = min(available_position_leg1, available_position_leg2)`
- If either available position is `0`, plan build fails and sell is not executable.

### Why Two Checks

Two independent checks reduce failure risk:

- Decision-time check prevents creating a sell plan that exceeds position.
- Pre-submit check re-validates right before submission using latest runtime state.

## Execution Lock and Reconcile Behavior

- Global lock is acquired when submit begins (`action_type` includes buy/sell).
- Lock remains active until:
  - required submitted venues reach terminal order state, and
  - required venue position reconcile success occurs after execution completion.
- This prevents chained submits on stale or unresolved state.

## Position Runtime Notes for Sell

- Submit-ack/order state stores `action` per leg.
- If submit response includes immediate fill size:
  - buy fill applies positive delta to position
  - sell fill applies negative delta to position
- Exposure reservation logic for pending orders remains buy-only.

## Config (Trading-Relevant)

Decision config:

- `buy.*` for buy thresholds/sizing.
- `sell.*` for sell thresholds/sizing/slippage.

Execution runtime config:

- `buy_execution.*`
  - enable/disable buy
  - cooldown
  - max attempts per run (buy-only)
  - parallel leg timeout
  - API retry config
- `sell_execution.*`
  - enable/disable sell
  - parallel leg timeout
  - API retry config

## Logging

Current sinks are preserved:

- `decision_log__*.jsonl`
- `buy_decision_log__*.jsonl`
- `buy_execution_log__*.jsonl`

Execution payload now includes action-aware context:

- `execution.action` (`buy` or `sell`)
- `selected_action`
- both buy and sell plan fields in decision/execution logs

## Known Limits

- Idempotency is process-local (not persisted across restart).
- Architecture is not yet unified buy/sell/hedge FSM.
- Strategy scope is current active discovered pair segment (not full portfolio orchestration).

## Future Work

- Add one-leg hedge execution path.
- Unify FSM across buy/sell/hedge.
- Optional persistent idempotency and stronger restart recovery.
