# tasks.md

Last updated: 2026-02-21

Purpose: Maintain the active execution queue with clear priorities, ownership, and status.

How this file is structured:

- `Done`: Recently completed tasks.
- `Now`: Immediate tasks that are actively being worked.
- `Next`: Ready tasks waiting for available capacity.
- `Later`: Lower-priority tasks or deferred work.
- `Blocked`: Tasks that need external input or dependency resolution.

## Done

- [x] Add `__init__.py` files to package dirs
- [x] Consolidate duplicate helpers → `scripts/common/utils.py`
- [x] Extract `BuyFsmRuntime` → `scripts/common/buy_fsm.py`
- [x] Extract edge snapshots → `scripts/common/edge_snapshots.py`
- [x] Extract CLI parser → `scripts/run/engine_cli.py`
- [x] Kalshi auth reconnect fix — `headers_factory` pattern
- [x] Polymarket `post_order` retry wrapper
- [x] Live integration test — 2 segments, 13K+ Kalshi / 27K+ Polymarket messages
- [x] #12 — Design position monitoring system (order status polling, fill tracking, net position)
- [x] #15 — Build `PositionRuntime` — tracks open positions, fills, and net exposure
- [x] #16 — Integrate position monitoring into engine loop

## Now

- [/] #18 — Refactor Engine Architecture
  - Create `scripts/common/engine_logger.py` for logging setup
  - Create `scripts/common/engine_setup.py` for WS/REST build steps
  - Create `scripts/run/start_engine.py` as new CLI entry point
  - Simplify `scripts/run/arbitrage_engine.py` to a core class

## Next

- [ ] #19 — Refactor `PositionRuntime` to use `@dataclass` for state management
- [ ] #20 — Clean up `position_polling.py` utility redundancies
- [ ] #21 — Polish `docs/position_monitoring.md` documentation
- [ ] #8 — Write `NormalizedBookRuntime` test suite (6+ cases) — 1-2 hrs
- [ ] #9 — Lag timestamp precision detection (`Improve_lag.md` #1 + #6) — 1 hr
- [ ] #10 — Rolling lag stats in health snapshots (`Improve_lag.md` #3) — 1 hr, depends on #9
- [ ] #11 — Partial-submit alert (stderr on `partially_submitted`) — 10 min

## Later

- [ ] #13 — Implement Kalshi order status polling client
- [ ] #14 — Implement Polymarket order status polling client
- [ ] #17 — Add portfolio-level max exposure limit

## Blocked

- None
