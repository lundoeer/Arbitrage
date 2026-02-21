# memory.md

Last updated: 2026-02-21

Purpose: Store durable project knowledge that should remain true over time. Keep this file short, stable, and practical.

How this file is structured:

- `Core Facts`: Stable truths about architecture, domain, and constraints.
- `Standards`: Naming, coding, and operational conventions.
- `Environments`: Trusted details about local/dev/prod setup.
- `Do Not Forget`: High-value reminders that are still long-lived.

## Core Facts

- This project builds an arbitrage system between Polymarket and Kalshi for BTC 15-minute directional (up/down) markets.

- This is a full rebuild of an old project. The old project lives in `\_oldproject` and is kept as reference only — nothing is reused.

- `scripts/run/arbitrage_engine.py` is the main entry point. It runs a segment loop (one per 15-minute market window) with concurrent WebSocket collectors and a decision polling loop.

- Contract discovery and matching is strict and deterministic (identifier/prefix/time alignment), not heuristic.

- Module layout: `scripts/common/` for shared code, `scripts/run/` for executables, `scripts/diagnostic/` for analysis tools, `tests/` for pytest tests.

## Standards

- Prefer simple strict code that fails loudly over loose code that guesses silently.

- Do not add fallback heuristics unless explicitly requested and documented with purpose.

- Validate market identity with hard rules (ticker/slug prefixes and exact window alignment).

- Shared helpers live in `scripts/common/utils.py` — do not duplicate helpers across files. If a helper is used in more than one file, move it to `utils.py`.

- Prefer extracting focused modules over growing monolith files. Each module should have a single clear responsibility.

- When extracting functions from a file, make them public (no underscore prefix) in the new module, since they are now part of a shared API.

## Environments

- Python: Use `.venv\Scripts\python.exe` (Windows). System Python lacks `websockets` and other required dependencies.

- Config: `config/run_config.json` — runtime parameters (health thresholds, buy config, execution bounds).

- Data output: `data/` — engine summaries, decision logs, edge snapshots, raw WebSocket captures.

- Pair cache: `data/market_pair_cache.json` — cached market pair from last discovery. Expires when market window ends.

## Do Not Forget

- Polymarket normalized event count is intentionally higher than raw message count (~1.7x). Each `price_change` raw message contains updates for multiple tokens (YES + NO).

- `--skip-discovery` will silently produce 0 messages if the pair cache is stale. Always verify cache freshness or run without this flag.

- Kalshi timestamps can be second-precision, which adds up to ±999ms of noise to lag measurements. See `docs/Improve_lag.md`.
