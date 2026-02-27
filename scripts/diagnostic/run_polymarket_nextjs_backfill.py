#!/usr/bin/env python3
"""
Run Polymarket Next.js window price collection day-by-day in reverse UTC order.

Stops immediately on the first failed day.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.diagnostic.get_polymarket_nextjs_window_prices import run as run_day


def _parse_day_token(value: str) -> date:
    token = value.strip().lower()
    if token == "yesterday":
        return (datetime.now(timezone.utc) - timedelta(days=1)).date()
    if token == "today":
        return datetime.now(timezone.utc).date()

    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        if "T" in raw:
            parsed_dt = datetime.fromisoformat(raw)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return parsed_dt.astimezone(timezone.utc).date()
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise RuntimeError(f"invalid day token: {value}") from exc


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")


def _eta_seconds(*, processed: int, total: int, elapsed_seconds: float) -> Optional[float]:
    if processed <= 0:
        return None
    remaining = max(0, total - processed)
    return (remaining / float(processed)) * max(0.0, elapsed_seconds)


def _format_eta(seconds: Optional[float]) -> str:
    if seconds is None:
        return "unknown"
    safe = max(0, int(round(seconds)))
    mins, secs = divmod(safe, 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        return f"{hours:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


def run(
    *,
    resolved_log_path: Path,
    output_jsonl_path: Path,
    run_summary_jsonl_path: Path,
    start_day_token: str,
    end_day_token: str,
    requests_per_second: float,
    max_attempts: int,
    base_backoff_seconds: float,
    jitter_ratio: float,
    progress_every: int,
    retry_errors: bool,
    max_slugs_per_day: Optional[int],
    sleep_between_days_seconds: float,
) -> int:
    start_day = _parse_day_token(start_day_token)
    end_day = _parse_day_token(end_day_token)
    if start_day < end_day:
        raise RuntimeError(f"start day {start_day.isoformat()} is earlier than end day {end_day.isoformat()}")

    total_days = (start_day - end_day).days + 1
    processed_days = 0
    success_days = 0
    failed_days = 0
    total_rows_success = 0
    total_rows_error = 0
    started = time.time()

    current_day = start_day
    while current_day >= end_day:
        processed_days += 1
        day_str = current_day.isoformat()
        print(f"[day {processed_days}/{total_days}] running {day_str}")
        day_started = time.time()

        try:
            summary = run_day(
                resolved_log_path=resolved_log_path,
                output_jsonl_path=output_jsonl_path,
                day_utc=day_str,
                max_slugs=max_slugs_per_day,
                requests_per_second=requests_per_second,
                max_attempts=max_attempts,
                base_backoff_seconds=base_backoff_seconds,
                jitter_ratio=jitter_ratio,
                progress_every=progress_every,
                retry_errors=retry_errors,
            )
        except Exception as exc:
            failed_days += 1
            elapsed_day = time.time() - day_started
            record = {
                "day_utc": day_str,
                "status": "failed_exception",
                "error": str(exc),
                "elapsed_seconds": round(elapsed_day, 3),
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            }
            _append_jsonl(run_summary_jsonl_path, record)
            print(f"Stopping on failure day {day_str}: {exc}")
            break

        written_success = int(summary.get("written_success", 0))
        written_error = int(summary.get("written_error", 0))
        selected = int(summary.get("selected", 0))
        total_rows_success += written_success
        total_rows_error += written_error
        elapsed_day = time.time() - day_started

        day_status = "ok" if written_error == 0 else "failed_day_errors"
        record = {
            "day_utc": day_str,
            "status": day_status,
            "selected": selected,
            "written_success": written_success,
            "written_error": written_error,
            "requests_sent": int(summary.get("requests_sent", 0)),
            "build_id_refreshes": int(summary.get("build_id_refreshes", 0)),
            "elapsed_seconds": round(elapsed_day, 3),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        _append_jsonl(run_summary_jsonl_path, record)

        if written_error > 0:
            failed_days += 1
            print(f"Stopping on failure day {day_str}: written_error={written_error}")
            break

        success_days += 1
        elapsed_total = time.time() - started
        eta = _eta_seconds(processed=processed_days, total=total_days, elapsed_seconds=elapsed_total)
        print(
            f"[day {processed_days}/{total_days}] complete {day_str} "
            f"success_rows={written_success} elapsed={int(elapsed_day)}s eta={_format_eta(eta)}"
        )

        if sleep_between_days_seconds > 0 and current_day > end_day:
            time.sleep(sleep_between_days_seconds)

        current_day = current_day - timedelta(days=1)

    print("Backward run finished")
    print(f"Start day: {start_day.isoformat()}")
    print(f"End day:   {end_day.isoformat()}")
    print(f"Processed days: {processed_days}")
    print(f"Successful days: {success_days}")
    print(f"Failed days: {failed_days}")
    print(f"Total success rows: {total_rows_success}")
    print(f"Total error rows: {total_rows_error}")
    print(f"Output JSONL: {output_jsonl_path}")
    print(f"Run summary JSONL: {run_summary_jsonl_path}")
    return 0 if failed_days == 0 else 1


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backward Polymarket Next.js day runner (stop on first failure).")
    p.add_argument("--resolved-log", default="data/diagnostic/resolved_15m_pairs.log")
    p.add_argument(
        "--out-jsonl",
        default="data/diagnostic/polymarket_nextjs_window_price_validation_backfill.jsonl",
    )
    p.add_argument(
        "--run-summary-jsonl",
        default="data/diagnostic/polymarket_nextjs_window_price_backfill_run_summary.jsonl",
    )
    p.add_argument("--start-day", default="yesterday", help="UTC day token: yesterday|today|YYYY-MM-DD|ISO datetime")
    p.add_argument("--end-day", required=True, help="UTC day token: YYYY-MM-DD or ISO datetime")
    p.add_argument("--requests-per-second", type=float, default=2.0)
    p.add_argument("--max-attempts", type=int, default=4)
    p.add_argument("--base-backoff-seconds", type=float, default=1.0)
    p.add_argument("--jitter-ratio", type=float, default=0.2)
    p.add_argument("--progress-every", type=int, default=20)
    p.add_argument("--retry-errors", action="store_true")
    p.add_argument("--max-slugs-per-day", type=int, default=None)
    p.add_argument("--sleep-between-days-seconds", type=float, default=0.0)
    return p


def main() -> None:
    args = _build_parser().parse_args()
    exit_code = run(
        resolved_log_path=Path(args.resolved_log),
        output_jsonl_path=Path(args.out_jsonl),
        run_summary_jsonl_path=Path(args.run_summary_jsonl),
        start_day_token=str(args.start_day).strip(),
        end_day_token=str(args.end_day).strip(),
        requests_per_second=float(args.requests_per_second),
        max_attempts=int(args.max_attempts),
        base_backoff_seconds=float(args.base_backoff_seconds),
        jitter_ratio=float(args.jitter_ratio),
        progress_every=int(args.progress_every),
        retry_errors=bool(args.retry_errors),
        max_slugs_per_day=args.max_slugs_per_day,
        sleep_between_days_seconds=float(args.sleep_between_days_seconds),
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
