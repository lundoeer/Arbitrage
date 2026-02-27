#!/usr/bin/env python3
"""
Run full resolved-pair diagnostics refresh in one command.

Pipeline:
1) Refresh/append resolved Kalshi<->Polymarket pairs and baseline summary.
2) Refresh Polymarket Chainlink target/end validation in batch mode.
3) Enrich pair log lines with Kalshi + Polymarket target/end values.
4) Rewrite resolution summary from the fully updated pair log.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.diagnostic.compare_resolved_15m_pairs import (
    _read_pair_log,
    run as run_compare,
)
from scripts.diagnostic.enrich_resolved_pairs_with_prices import run as run_enrich
from scripts.diagnostic.get_polymarket_chainlink_window_prices import run_batch as run_chainlink_batch


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _rewrite_summary_from_log(
    *,
    previous_summary: Dict[str, Any],
    pair_log_path: Path,
    summary_path: Path,
    chainlink_outputs: Dict[str, str],
    enrich_outputs: Dict[str, Any],
) -> Dict[str, Any]:
    rows = _read_pair_log(pair_log_path)
    same_count = sum(1 for row in rows if row.get("comparison") == "same")
    different_count = sum(1 for row in rows if row.get("comparison") == "different")

    totals = dict(previous_summary.get("totals", {}))
    totals["pairs_in_log_total"] = len(rows)
    totals["resolved_same"] = same_count
    totals["resolved_different"] = different_count

    summary = {
        **previous_summary,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": totals,
        "output_files": {
            **dict(previous_summary.get("output_files", {})),
            "pair_log": str(pair_log_path),
            "summary_json": str(summary_path),
            "polymarket_chainlink_validation_log": chainlink_outputs["validation_log"],
            "polymarket_chainlink_validation_summary_json": chainlink_outputs["summary_json"],
            "polymarket_chainlink_per_slug_dir": chainlink_outputs["per_slug_dir"],
        },
        "different_examples": [row for row in rows if row.get("comparison") == "different"][:50],
        "enrichment": {
            "rows_updated": int(enrich_outputs.get("totals", {}).get("rows_updated", 0)),
            "rows_selected": int(enrich_outputs.get("totals", {}).get("rows_selected", 0)),
            "rows_processed": int(enrich_outputs.get("totals", {}).get("rows_processed", 0)),
            "unique_kalshi_markets_fetched": int(enrich_outputs.get("unique_kalshi_markets_fetched", 0)),
        },
    }
    _write_json(summary_path, summary)
    return summary


def run(
    *,
    config_path: Path,
    pair_log_path: Path,
    summary_path: Path,
    page_size: int,
    max_pages: int | None,
    max_markets: int | None,
    validation_log_path: Path,
    validation_summary_path: Path,
    per_slug_dir: Path,
    chainlink_only_missing: bool = True,
    chainlink_max_slugs: int | None = None,
    chainlink_dry_run: bool = False,
    chainlink_progress_every: int = 25,
    chainlink_sleep_ms: int = 0,
    chainlink_max_consecutive_overload: int = 5,
    chainlink_state_path: Path | None = None,
    chainlink_resume_from_state: bool = True,
    chainlink_max_attempts: int = 3,
    chainlink_base_backoff_seconds: float = 1.0,
    chainlink_jitter_ratio: float = 0.2,
    enrich_only_missing: bool = True,
    enrich_max_lines: int | None = None,
    enrich_dry_run: bool = False,
    enrich_progress_every: int = 200,
    enrich_sleep_ms: int = 0,
    enrich_max_consecutive_overload: int = 5,
) -> Dict[str, Any]:
    compare_summary = run_compare(
        config_path=config_path,
        pair_log_path=pair_log_path,
        summary_path=summary_path,
        page_size=page_size,
        max_pages=max_pages,
        max_markets=max_markets,
    )

    chainlink_summary = run_chainlink_batch(
        resolved_log_path=pair_log_path,
        validation_log_path=validation_log_path,
        summary_path=validation_summary_path,
        per_slug_dir=per_slug_dir,
        only_missing=chainlink_only_missing,
        max_slugs=chainlink_max_slugs,
        dry_run=chainlink_dry_run,
        progress_every=chainlink_progress_every,
        sleep_ms=chainlink_sleep_ms,
        max_consecutive_overload=chainlink_max_consecutive_overload,
        state_path=chainlink_state_path,
        resume_from_state=chainlink_resume_from_state,
        chainlink_max_attempts=chainlink_max_attempts,
        chainlink_base_backoff_seconds=chainlink_base_backoff_seconds,
        chainlink_jitter_ratio=chainlink_jitter_ratio,
    )

    enrich_summary = run_enrich(
        config_path=config_path,
        pair_log_path=pair_log_path,
        polymarket_validation_log_path=validation_log_path,
        only_missing=enrich_only_missing,
        max_lines=enrich_max_lines,
        dry_run=enrich_dry_run,
        progress_every=enrich_progress_every,
        sleep_ms=enrich_sleep_ms,
        max_consecutive_overload=enrich_max_consecutive_overload,
    )

    final_summary = _rewrite_summary_from_log(
        previous_summary=compare_summary,
        pair_log_path=pair_log_path,
        summary_path=summary_path,
        chainlink_outputs={
            "validation_log": str(validation_log_path),
            "summary_json": str(validation_summary_path),
            "per_slug_dir": str(per_slug_dir),
        },
        enrich_outputs=enrich_summary,
    )

    return {
        "final_resolution_summary": final_summary,
        "chainlink_batch_summary": chainlink_summary,
        "enrichment_summary": enrich_summary,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Full refresh for resolved BTC 15m pair diagnostics."
    )
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--pair-log", default="data/diagnostic/resolved_15m_pairs.log")
    parser.add_argument("--summary", default="data/diagnostic/resolution_comparison_summary.json")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-markets", type=int, default=None)
    parser.add_argument(
        "--validation-log",
        default="data/diagnostic/polymarket_chainlink_price_validation.log",
    )
    parser.add_argument(
        "--validation-summary",
        default="data/diagnostic/polymarket_chainlink_price_validation_summary.json",
    )
    parser.add_argument(
        "--validation-per-slug-dir",
        default="data/diagnostic/chainlink_window_prices_batch",
    )
    parser.add_argument(
        "--chainlink-only-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--chainlink-max-slugs", type=int, default=None)
    parser.add_argument("--chainlink-dry-run", action="store_true")
    parser.add_argument("--chainlink-progress-every", type=int, default=25)
    parser.add_argument("--chainlink-sleep-ms", type=int, default=0)
    parser.add_argument("--chainlink-max-consecutive-overload", type=int, default=5)
    parser.add_argument(
        "--chainlink-state-file",
        default="data/diagnostic/chainlink_window_prices_batch_state.json",
    )
    parser.add_argument("--chainlink-no-resume", action="store_true")
    parser.add_argument("--chainlink-max-attempts", type=int, default=3)
    parser.add_argument("--chainlink-base-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--chainlink-jitter-ratio", type=float, default=0.2)
    parser.add_argument(
        "--enrich-only-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--enrich-max-lines", type=int, default=None)
    parser.add_argument("--enrich-dry-run", action="store_true")
    parser.add_argument("--enrich-progress-every", type=int, default=200)
    parser.add_argument("--enrich-sleep-ms", type=int, default=0)
    parser.add_argument("--enrich-max-consecutive-overload", type=int, default=5)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = run(
        config_path=Path(args.config),
        pair_log_path=Path(args.pair_log),
        summary_path=Path(args.summary),
        page_size=args.page_size,
        max_pages=args.max_pages,
        max_markets=args.max_markets,
        validation_log_path=Path(args.validation_log),
        validation_summary_path=Path(args.validation_summary),
        per_slug_dir=Path(args.validation_per_slug_dir),
        chainlink_only_missing=bool(args.chainlink_only_missing),
        chainlink_max_slugs=args.chainlink_max_slugs,
        chainlink_dry_run=bool(args.chainlink_dry_run),
        chainlink_progress_every=args.chainlink_progress_every,
        chainlink_sleep_ms=args.chainlink_sleep_ms,
        chainlink_max_consecutive_overload=args.chainlink_max_consecutive_overload,
        chainlink_state_path=Path(args.chainlink_state_file) if args.chainlink_state_file else None,
        chainlink_resume_from_state=not bool(args.chainlink_no_resume),
        chainlink_max_attempts=args.chainlink_max_attempts,
        chainlink_base_backoff_seconds=args.chainlink_base_backoff_seconds,
        chainlink_jitter_ratio=args.chainlink_jitter_ratio,
        enrich_only_missing=bool(args.enrich_only_missing),
        enrich_max_lines=args.enrich_max_lines,
        enrich_dry_run=bool(args.enrich_dry_run),
        enrich_progress_every=args.enrich_progress_every,
        enrich_sleep_ms=args.enrich_sleep_ms,
        enrich_max_consecutive_overload=args.enrich_max_consecutive_overload,
    )

    totals = result["final_resolution_summary"]["totals"]
    print("Full resolved-pair diagnostics refresh complete")
    print(f"Pairs in log: {totals.get('pairs_in_log_total')}")
    print(f"Resolved same: {totals.get('resolved_same')}")
    print(f"Resolved different: {totals.get('resolved_different')}")
    print(f"Pair log: {args.pair_log}")
    print(f"Summary: {args.summary}")
    print(f"Validation log: {args.validation_log}")
    print(f"Validation summary: {args.validation_summary}")


if __name__ == "__main__":
    main()
