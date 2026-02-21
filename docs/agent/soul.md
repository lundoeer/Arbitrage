# soul.md

Last updated: 2026-02-21

Purpose: Define the project's values and style guardrails so implementation decisions stay coherent.

How this file is structured:

- `Values`: Non-negotiable principles for product and engineering choices.
- `Experience Goals`: What the user should consistently feel.
- `Code Feel`: Desired characteristics of the codebase.
- `Anti-Patterns`: Behaviors and patterns to avoid.

## Values

- Clarity over convenience: if data does not meet strict expectations, fail with a clear error.

- Determinism over heuristics: avoid ambiguous matching logic for markets and contracts.

- Correctness over speed: do not sacrifice correctness to save development time.

## Experience Goals

- Engine runs should be self-documenting via structured JSON summaries, decision logs, and edge snapshots.

- Failures should be obvious and actionable, never silent.

## Code Feel

- Keep code small, explicit, and easy to audit.

- Prefer hard validation boundaries over permissive interpretation.

- Prefer focused single-responsibility modules over large multi-purpose files.

- Each extracted module should be independently understandable and testable.

## Anti-Patterns

- Creating fallback guesses without explicit instruction and purpose.

- Silent recovery that hides data mismatches or contract-identity errors.

- Duplicating helper functions across files instead of sharing from `utils.py`.

- Growing `arbitrage_engine.py` — extract new functionality into dedicated modules.
