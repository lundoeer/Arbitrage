#!/usr/bin/env python3
"""
Build target-only risk buckets from resolved BTC 15m pair logs.

The script reads `resolved_15m_pairs.log`, keeps rows with:
- comparison in {same, different}
- kalshi_target and polymarket_target present

It writes:
- CSV bucket table with empirical and smoothed P(different)
- JSON summary with dataset stats and best threshold sweeps
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.diagnostic.compare_resolved_15m_pairs import _read_pair_log


@dataclass(frozen=True)
class BucketSpec:
    name: str
    lower: Optional[float] = None
    upper: Optional[float] = None


@dataclass(frozen=True)
class BucketFamily:
    family_name: str
    feature_name: str
    buckets: Tuple[BucketSpec, ...]


ABS_GAP_RECOMMENDED = (
    BucketSpec("low", 0.0, 5.0),
    BucketSpec("watch", 5.0, 10.0),
    BucketSpec("elevated", 10.0, 50.0),
    BucketSpec("high", 50.0, None),
)

ABS_GAP_DETAIL = (
    BucketSpec("0-5", 0.0, 5.0),
    BucketSpec("5-10", 5.0, 10.0),
    BucketSpec("10-25", 10.0, 25.0),
    BucketSpec("25-50", 25.0, 50.0),
    BucketSpec("50-75", 50.0, 75.0),
    BucketSpec("75-100", 75.0, 100.0),
    BucketSpec("100+", 100.0, None),
)

BPS_GAP_BUCKETS = (
    BucketSpec("0-1", 0.0, 1.0),
    BucketSpec("1-2", 1.0, 2.0),
    BucketSpec("2-4", 2.0, 4.0),
    BucketSpec("4-6", 4.0, 6.0),
    BucketSpec("6-8", 6.0, 8.0),
    BucketSpec("8+", 8.0, None),
)

SIGNED_GAP_BUCKETS = (
    BucketSpec("pm_above_50+", None, -50.0),
    BucketSpec("pm_above_10_50", -50.0, -10.0),
    BucketSpec("near_parity_-10_10", -10.0, 10.0),
    BucketSpec("kalshi_above_10_50", 10.0, 50.0),
    BucketSpec("kalshi_above_50+", 50.0, None),
)

BUCKET_FAMILIES = (
    BucketFamily("abs_gap_usd_recommended", "abs_gap_usd", ABS_GAP_RECOMMENDED),
    BucketFamily("abs_gap_usd_detail", "abs_gap_usd", ABS_GAP_DETAIL),
    BucketFamily("abs_gap_bps", "abs_gap_bps", BPS_GAP_BUCKETS),
    BucketFamily("signed_gap_usd", "signed_gap_usd", SIGNED_GAP_BUCKETS),
)


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _row_key(row: Dict[str, Any]) -> str:
    return "|".join(
        (
            str(row.get("window_end", "")),
            str(row.get("kalshi_ticker", "")),
            str(row.get("polymarket_slug", "")),
        )
    )


def _bucket_contains(value: float, bucket: BucketSpec) -> bool:
    if bucket.lower is not None and value < bucket.lower:
        return False
    if bucket.upper is not None and value >= bucket.upper:
        return False
    return True


def _format_bound(value: Optional[float], *, is_lower: bool) -> str:
    if value is None:
        return "-inf" if is_lower else "inf"
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _wilson_95(successes: int, trials: int) -> Tuple[float, float]:
    if trials <= 0:
        return (0.0, 0.0)
    z = 1.959963984540054
    z2 = z * z
    phat = successes / trials
    denom = 1.0 + z2 / trials
    center = (phat + z2 / (2.0 * trials)) / denom
    margin = z * math.sqrt((phat * (1.0 - phat) + z2 / (4.0 * trials)) / trials) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def _load_samples(pair_log_path: Path, *, dedupe: bool) -> Dict[str, Any]:
    rows = _read_pair_log(pair_log_path)
    rows_total = len(rows)

    parsed_rows: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    for row in rows:
        key = _row_key(row)
        if dedupe and key in seen_keys:
            continue
        seen_keys.add(key)
        parsed_rows.append(row)

    rows_labeled = 0
    rows_with_targets = 0
    samples: List[Dict[str, Any]] = []

    for row in parsed_rows:
        comparison = str(row.get("comparison", "")).strip().lower()
        if comparison not in {"same", "different"}:
            continue
        rows_labeled += 1

        kalshi_target = _safe_float(row.get("kalshi_target"))
        polymarket_target = _safe_float(row.get("polymarket_target"))
        if kalshi_target is None or polymarket_target is None:
            continue

        rows_with_targets += 1
        signed_gap = kalshi_target - polymarket_target
        abs_gap = abs(signed_gap)
        mid = (abs(kalshi_target) + abs(polymarket_target)) / 2.0
        if mid <= 0:
            continue

        abs_gap_bps = abs_gap / mid * 10_000.0
        samples.append(
            {
                "window_end": str(row.get("window_end", "")),
                "is_different": 1 if comparison == "different" else 0,
                "abs_gap_usd": abs_gap,
                "abs_gap_bps": abs_gap_bps,
                "signed_gap_usd": signed_gap,
            }
        )

    different_total = sum(int(sample["is_different"]) for sample in samples)
    usable_total = len(samples)
    base_rate = (different_total / usable_total) if usable_total else 0.0

    return {
        "rows_total_in_log": rows_total,
        "rows_after_dedupe": len(parsed_rows),
        "rows_with_same_or_different_label": rows_labeled,
        "rows_with_both_targets": rows_with_targets,
        "samples": samples,
        "usable_rows": usable_total,
        "different_rows": different_total,
        "base_rate_different": base_rate,
    }


def _build_bucket_rows(
    *,
    samples: Iterable[Dict[str, Any]],
    families: Iterable[BucketFamily],
    base_rate: float,
    prior_strength: float,
    min_count: int,
) -> List[Dict[str, Any]]:
    sample_list = list(samples)
    sample_total = len(sample_list)
    prior_successes = prior_strength * base_rate

    output_rows: List[Dict[str, Any]] = []
    for family in families:
        for bucket in family.buckets:
            bucket_rows = [row for row in sample_list if _bucket_contains(float(row[family.feature_name]), bucket)]
            count = len(bucket_rows)
            if count < min_count:
                continue

            different_count = sum(int(row["is_different"]) for row in bucket_rows)
            same_count = count - different_count
            empirical_rate = different_count / count if count else 0.0
            smoothed_rate = (
                (different_count + prior_successes) / (count + prior_strength)
                if (count + prior_strength) > 0
                else 0.0
            )
            lift = (empirical_rate / base_rate) if base_rate > 0 else 0.0
            ci_low, ci_high = _wilson_95(different_count, count)

            output_rows.append(
                {
                    "bucket_family": family.family_name,
                    "feature_name": family.feature_name,
                    "bucket_name": bucket.name,
                    "lower_inclusive": _format_bound(bucket.lower, is_lower=True),
                    "upper_exclusive": _format_bound(bucket.upper, is_lower=False),
                    "rows": count,
                    "different_rows": different_count,
                    "same_rows": same_count,
                    "row_share_pct": f"{(100.0 * count / sample_total) if sample_total else 0.0:.4f}",
                    "different_rate_pct": f"{100.0 * empirical_rate:.4f}",
                    "different_rate_smoothed_pct": f"{100.0 * smoothed_rate:.4f}",
                    "risk_lift_vs_base": f"{lift:.4f}",
                    "wilson95_low_pct": f"{100.0 * ci_low:.4f}",
                    "wilson95_high_pct": f"{100.0 * ci_high:.4f}",
                }
            )
    return output_rows


def _best_threshold(samples: Iterable[Dict[str, Any]], *, feature_name: str) -> Dict[str, Any]:
    values = [(float(row[feature_name]), int(row["is_different"])) for row in samples]
    if not values:
        return {
            "feature_name": feature_name,
            "best_threshold_ge": None,
            "f1": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "flag_rate": 0.0,
            "flagged_rows": 0,
        }

    unique_thresholds = sorted({score for score, _ in values})
    total = len(values)
    positives = sum(label for _, label in values)
    best = {
        "feature_name": feature_name,
        "best_threshold_ge": None,
        "f1": -1.0,
        "precision": 0.0,
        "recall": 0.0,
        "flag_rate": 0.0,
        "flagged_rows": 0,
    }

    for threshold in unique_thresholds:
        tp = fp = 0
        flagged = 0
        for score, label in values:
            if score >= threshold:
                flagged += 1
                if label == 1:
                    tp += 1
                else:
                    fp += 1

        fn = positives - tp
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

        if f1 > float(best["f1"]):
            best = {
                "feature_name": feature_name,
                "best_threshold_ge": threshold,
                "f1": f1,
                "precision": precision,
                "recall": recall,
                "flag_rate": flagged / total if total > 0 else 0.0,
                "flagged_rows": flagged,
            }

    return best


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    columns = [
        "bucket_family",
        "feature_name",
        "bucket_name",
        "lower_inclusive",
        "upper_exclusive",
        "rows",
        "different_rows",
        "same_rows",
        "row_share_pct",
        "different_rate_pct",
        "different_rate_smoothed_pct",
        "risk_lift_vs_base",
        "wilson95_low_pct",
        "wilson95_high_pct",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def run(
    *,
    pair_log_path: Path,
    output_csv_path: Path,
    output_summary_path: Path,
    prior_strength: float,
    min_count: int,
    dedupe: bool,
) -> Dict[str, Any]:
    loaded = _load_samples(pair_log_path, dedupe=dedupe)
    samples = loaded["samples"]

    bucket_rows = _build_bucket_rows(
        samples=samples,
        families=BUCKET_FAMILIES,
        base_rate=float(loaded["base_rate_different"]),
        prior_strength=prior_strength,
        min_count=min_count,
    )
    _write_csv(output_csv_path, bucket_rows)

    best_abs_threshold = _best_threshold(samples, feature_name="abs_gap_usd")
    best_bps_threshold = _best_threshold(samples, feature_name="abs_gap_bps")
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_pair_log": str(pair_log_path),
        "output_bucket_csv": str(output_csv_path),
        "output_summary_json": str(output_summary_path),
        "config": {
            "prior_strength": prior_strength,
            "min_count": min_count,
            "dedupe": dedupe,
        },
        "dataset": {
            "rows_total_in_log": loaded["rows_total_in_log"],
            "rows_after_dedupe": loaded["rows_after_dedupe"],
            "rows_with_same_or_different_label": loaded["rows_with_same_or_different_label"],
            "rows_with_both_targets": loaded["rows_with_both_targets"],
            "usable_rows": loaded["usable_rows"],
            "different_rows": loaded["different_rows"],
            "base_rate_different": loaded["base_rate_different"],
        },
        "threshold_sweep": {
            "abs_gap_usd": best_abs_threshold,
            "abs_gap_bps": best_bps_threshold,
        },
        "bucket_families": [
            {
                "bucket_family": family.family_name,
                "feature_name": family.feature_name,
                "bucket_count": len(family.buckets),
            }
            for family in BUCKET_FAMILIES
        ],
    }
    _write_json(output_summary_path, summary)
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build target-gap risk buckets from resolved pair log.")
    parser.add_argument("--pair-log", default="data/diagnostic/resolved_15m_pairs.log")
    parser.add_argument("--output-csv", default="data/diagnostic/target_gap_risk_buckets.csv")
    parser.add_argument("--output-summary", default="data/diagnostic/target_gap_risk_summary.json")
    parser.add_argument("--prior-strength", type=float, default=20.0)
    parser.add_argument("--min-count", type=int, default=1)
    parser.add_argument(
        "--dedupe",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Deduplicate rows by (window_end, kalshi_ticker, polymarket_slug).",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        pair_log_path=Path(args.pair_log),
        output_csv_path=Path(args.output_csv),
        output_summary_path=Path(args.output_summary),
        prior_strength=args.prior_strength,
        min_count=args.min_count,
        dedupe=bool(args.dedupe),
    )
    dataset = summary["dataset"]
    print("Target-gap risk buckets complete")
    print(f"Source log: {summary['source_pair_log']}")
    print(f"Usable rows: {dataset['usable_rows']}")
    print(f"Different rows: {dataset['different_rows']}")
    print(f"Base rate different: {100.0 * float(dataset['base_rate_different']):.4f}%")
    print(f"Bucket CSV: {summary['output_bucket_csv']}")
    print(f"Summary JSON: {summary['output_summary_json']}")


if __name__ == "__main__":
    main()
