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
            "lines_updated": int(enrich_outputs.get("lines_updated", 0)),
            "unique_kalshi_markets": int(enrich_outputs.get("unique_kalshi_markets", 0)),
            "unique_polymarket_markets": int(enrich_outputs.get("unique_polymarket_markets", 0)),
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
    )

    enrich_summary = run_enrich(
        config_path=config_path,
        pair_log_path=pair_log_path,
        polymarket_validation_log_path=validation_log_path,
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
