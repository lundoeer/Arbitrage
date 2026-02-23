#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read account_portfolio_snapshot_log JSONL files and emit only rows where "
            "Kalshi/Polymarket/total USD balances changed."
        )
    )
    parser.add_argument(
        "--input-glob",
        default="data/account_portfolio_snapshot_log__*.jsonl",
        help="Glob pattern for account snapshot logs.",
    )
    parser.add_argument(
        "--kalshi-divisor",
        type=float,
        default=100.0,
        help="Raw Kalshi balance divisor to convert to USD (default: 100 for cents).",
    )
    parser.add_argument(
        "--polymarket-divisor",
        type=float,
        default=1_000_000.0,
        help=(
            "Raw Polymarket balance divisor to convert to USD "
            "(default: 1,000,000; set to 100000 if your feed is millicents)."
        ),
    )
    parser.add_argument(
        "--output",
        default="logs/account_balance_changes.jsonl",
        help="Output JSONL path for incremental append (default: logs/account_balance_changes.jsonl).",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=6,
        help="Decimal places for USD fields (default: 6).",
    )
    return parser


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _ts_to_epoch_ms(ts: str) -> Optional[int]:
    raw = str(ts or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _extract_balances_usd(
    *,
    payload: Dict[str, Any],
    kalshi_divisor: float,
    polymarket_divisor: float,
) -> Optional[Tuple[float, float]]:
    snapshot = _as_dict(payload.get("snapshot"))
    venues = _as_dict(snapshot.get("venues"))

    kalshi = _as_dict(venues.get("kalshi"))
    kalshi_balance = _as_dict(_as_dict(kalshi.get("balance")).get("balance"))
    kalshi_raw = _to_float(kalshi_balance.get("balance"))

    polymarket = _as_dict(venues.get("polymarket"))
    polymarket_allowance = _as_dict(
        _as_dict(_as_dict(polymarket.get("balance_allowance")).get("data")).get("balance_allowance")
    )
    polymarket_raw = _to_float(polymarket_allowance.get("balance"))

    if kalshi_raw is None or polymarket_raw is None:
        return None
    if kalshi_divisor <= 0.0 or polymarket_divisor <= 0.0:
        raise RuntimeError("Divisors must be > 0")
    return float(kalshi_raw / kalshi_divisor), float(polymarket_raw / polymarket_divisor)


def _collect_rows(
    *,
    input_glob: str,
    kalshi_divisor: float,
    polymarket_divisor: float,
) -> List[Dict[str, Any]]:
    files = sorted(Path().glob(input_glob))
    rows: List[Dict[str, Any]] = []
    for path in files:
        with path.open("r", encoding="utf-8") as src:
            for line_no, line in enumerate(src, start=1):
                raw = line.strip()
                if not raw:
                    continue
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                if str(payload.get("kind") or "") != "account_portfolio_snapshot":
                    continue
                ts = str(payload.get("ts") or "").strip()
                recv_ms = _to_float(payload.get("recv_ms"))
                balances = _extract_balances_usd(
                    payload=payload,
                    kalshi_divisor=kalshi_divisor,
                    polymarket_divisor=polymarket_divisor,
                )
                if balances is None:
                    continue
                kalshi_usd, polymarket_usd = balances
                epoch_ms = _ts_to_epoch_ms(ts)
                if epoch_ms is None and recv_ms is not None:
                    epoch_ms = int(recv_ms)
                if epoch_ms is None:
                    continue
                rows.append(
                    {
                        "time": ts if ts else str(epoch_ms),
                        "kalshi_usd": float(kalshi_usd),
                        "polymarket_usd": float(polymarket_usd),
                        "total_usd": float(kalshi_usd + polymarket_usd),
                        "_sort": (int(epoch_ms), str(path), int(line_no)),
                    }
                )
    rows.sort(key=lambda item: item["_sort"])
    return rows


def _emit_changes(rows: List[Dict[str, Any]], *, precision: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    last: Optional[Tuple[float, float, float]] = None
    for row in rows:
        k = round(float(row["kalshi_usd"]), int(precision))
        p = round(float(row["polymarket_usd"]), int(precision))
        t = round(float(row["total_usd"]), int(precision))
        current = (k, p, t)
        if last is not None and current == last:
            continue
        out.append(
            {
                "time": row["time"],
                "kalshi_usd": k,
                "polymarket_usd": p,
                "total_usd": t,
            }
        )
        last = current
    return out


def _row_key(row: Dict[str, Any], *, precision: int) -> Tuple[str, float, float, float]:
    return (
        str(row.get("time") or "").strip(),
        round(float(row.get("kalshi_usd") or 0.0), int(precision)),
        round(float(row.get("polymarket_usd") or 0.0), int(precision)),
        round(float(row.get("total_usd") or 0.0), int(precision)),
    )


def _load_existing_keys(path: Path, *, precision: int) -> set[Tuple[str, float, float, float]]:
    keys: set[Tuple[str, float, float, float]] = set()
    if not path.exists():
        return keys
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            keys.add(_row_key(row, precision=precision))
    return keys


def main() -> None:
    args = _build_parser().parse_args()
    rows = _collect_rows(
        input_glob=str(args.input_glob),
        kalshi_divisor=float(args.kalshi_divisor),
        polymarket_divisor=float(args.polymarket_divisor),
    )
    changes = _emit_changes(rows, precision=int(args.precision))
    output_path = Path(str(args.output).strip() or "logs/account_balance_changes.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    precision = int(args.precision)
    existing_keys = _load_existing_keys(output_path, precision=precision)
    new_rows: List[Dict[str, Any]] = []
    for row in changes:
        key = _row_key(row, precision=precision)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append(row)

    if not new_rows:
        print(f"No new balance-change rows to append ({output_path})")
        return

    with output_path.open("a", encoding="utf-8") as dst:
        for row in new_rows:
            dst.write(json.dumps(row, ensure_ascii=True) + "\n")
    print(f"Appended {len(new_rows)} new rows to {output_path}")


if __name__ == "__main__":
    main()
