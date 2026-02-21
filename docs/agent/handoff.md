# handoff.md

Last updated: 2026-02-21

Purpose: Provide a fast transfer note for the next contributor with recent changes, current status, and immediate next actions.

How this file is structured:

- `What Changed`: Key updates since the previous handoff.
- `Current Status`: What is complete, in progress, and blocked.
- `Next Actions`: Ordered checklist for the next session.
- `Watchouts`: Risks, caveats, and sensitive areas.

## What Changed

Seven refactoring/fix items completed (see `docs/ClaudeOpusSuggestions.md` items 1-7):

1. Added `__init__.py` to `scripts/`, `scripts/common/`, `scripts/run/`, `scripts/diagnostic/`, `tests/`
2. Consolidated 7 duplicate helpers into `scripts/common/utils.py`, updated imports across 9 files
3. Extracted `BuyFsmRuntime` + `BuyFsmState` → `scripts/common/buy_fsm.py`
4. Extracted edge snapshot functions → `scripts/common/edge_snapshots.py`
5. Extracted CLI parser → `scripts/run/engine_cli.py`
6. Kalshi WS auth: `headers_factory` pattern ensures fresh signatures on reconnect
7. Polymarket `post_order`: retry wrapper with exponential backoff for transient errors

**Net impact**: `arbitrage_engine.py` reduced from 1333 → ~1013 lines. All 6 existing tests pass. Live 5-minute engine run verified with 2 market segments, 13K+ Kalshi and 27K+ Polymarket messages.

## Current Status

- Complete: Items 1-7 (today's small changes), live integration test
- In progress: Nothing
- Blocked: Nothing

## Next Actions

- [ ] Write `NormalizedBookRuntime` test suite — 6+ test cases covering snapshot replacement, top-of-book updates, delta handling, ask reconstruction, out-of-order rejection, depth pruning (#8)
- [ ] Implement lag timestamp precision detection — `Improve_lag.md` suggestions #1 + #6 (#9)
- [ ] Add rolling lag stats to health snapshots — `Improve_lag.md` suggestion #3 (#10)
- [ ] Add partial-submit alert — print to stderr on `partially_submitted` status (#11)
- [ ] Design position monitoring system (#12)

## Watchouts

- **Use venv Python.** System Python lacks `websockets` and other deps. Always run with `.venv\Scripts\python.exe`.

- **`--skip-discovery` requires fresh pair cache.** If `data/market_pair_cache.json` contains expired market windows, the engine will find no active segments. Run without `--skip-discovery` to force fresh discovery.

- Do not reintroduce heuristic market matching or silent fallback behavior without explicit approval.

- If strict checks fail due to upstream format changes, update rules intentionally instead of adding guess-based recovery.
