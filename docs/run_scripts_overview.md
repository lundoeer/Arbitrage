# Run Scripts Overview

Last updated: 2026-02-23

## Purpose

This file explains how the `scripts/run/` entry points fit together, with special focus on `engine_loop.py` and why the runtime is asynchronous.

## Main Run Path

For the production engine, the call chain is:

1. `scripts/run/start_engine.py`
2. `scripts/run/arbitrage_engine.py`
3. `scripts/run/engine_loop.py`

High-level flow:

1. `start_engine.py` parses CLI args, loads config, applies CLI overrides, and initializes `EngineLogger`.
2. It builds an `ArbitrageEngine` instance and repeatedly discovers/rotates active 15m market segments.
3. For each segment, `ArbitrageEngine.run_segment(...)` builds collectors/runtimes and calls `asyncio.run(run_core_loop(...))`.
4. `run_core_loop(...)` owns all per-segment concurrent loops and returns per-segment stats.

## `scripts/run/` Responsibilities

- `start_engine.py`
  - process entry point (`main()`).
  - configuration loading and override assembly.
  - top-level segment lifecycle orchestration.
- `arbitrage_engine.py`
  - per-segment setup/wiring.
  - runtime/component creation (collectors, buy clients, position monitoring wiring).
  - segment-level and run-level aggregation.
- `engine_loop.py`
  - async core loop runtime for one market segment.
  - runs websocket ingestion and periodic decision/observability loops concurrently.
- `engine_cli.py`
  - argparse definition only (`build_parser()`).
- `discover_active_btc_15m_markets.py`
  - discovery utility used by the engine and diagnostics.

## `run_core_loop(...)` in `engine_loop.py`

`run_core_loop(...)` is the segment runtime coordinator. It:

- initializes a buy FSM and stats accumulator.
- starts websocket collectors.
- starts periodic loops (decision, runtime-memory snapshots, edge snapshots, position polling).
- keeps all loops alive until duration expires (or external stop).
- sets a shared stop event, waits for task shutdown, and returns final stats.

### Concurrent Tasks Started

The loop creates one `asyncio.Task` for each active component:

- `kalshi_collector.run(stop_event)`
- `polymarket_collector.run(stop_event)`
- `_decision_loop(stop_event)`
- `_runtime_memory_loop(stop_event)`
- `_edge_snapshot_loop(stop_event)`
- `_position_poll_loop(stop_event)`
- optional:
  - `polymarket_user_collector.run(stop_event)`
  - `kalshi_market_positions_collector.run(stop_event)`

All tasks watch the same `stop_event` for coordinated shutdown.

## What Runs in Async and Why

### Async components

- websocket collectors (`*.run(...)`)
  - network receive loops; naturally asynchronous IO.
- periodic loops (`_decision_loop`, `_runtime_memory_loop`, `_edge_snapshot_loop`, `_position_poll_loop`)
  - each uses `await asyncio.sleep(...)` to run on cadence without blocking other tasks.

### Why async here

The engine needs to do several things at once within a segment:

1. Consume Kalshi and Polymarket websockets continuously.
2. Evaluate trading decisions on a frequent heartbeat (default 0.2s).
3. Emit observability logs on separate cadences.
4. Optionally track positions from extra websocket + polling channels.

`asyncio` provides cooperative concurrency in one event loop, avoiding thread coordination overhead and allowing one shared stop signal.

### Important practical behavior

- Buy submission (`execute_cross_venue_buy(...)`) is currently synchronous inside `_decision_loop`.
- This means a slow submit can temporarily delay decision ticks.
- Current design accepts this to keep submit sequencing and FSM behavior simple.

## Decision Loop Summary

Each `_decision_loop` tick does:

1. `buy_fsm.maybe_rearm(...)`
2. refresh position health (if enabled)
3. fetch websocket health snapshots
4. fetch runtime quotes
5. call `DecisionRuntime.evaluate(...)`
6. if `can_trade` is true, enforce execution safeguards in order:
   - max attempts
   - execution enabled
   - position health gate
   - FSM gate
   - execution plan/signal id presence
7. execute buy path and update counters/FSM snapshots
8. write decision/buy-execution logs
9. sleep until next decision tick

## Testing and Argparse Boundary

Core runtime functions are callable directly (without argparse), because parser usage is isolated to `start_engine.py` and `engine_cli.py`.

This is why tests can directly import and run core units (`DecisionRuntime`, buy execution/idempotency, FSM, position runtime) without CLI setup.
