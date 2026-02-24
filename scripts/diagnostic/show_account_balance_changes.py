#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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
    parser.add_argument(
        "--update-from-api",
        action="store_true",
        help=(
            "Fetch one live account portfolio snapshot from venue APIs and write a new "
            "data/account_portfolio_snapshot_log__*.jsonl file before processing."
        ),
    )
    parser.add_argument(
        "--snapshot-output",
        default="",
        help=(
            "Optional output path for the live snapshot JSONL row. If omitted, "
            "writes data/account_portfolio_snapshot_log__<utc-run-id>.jsonl."
        ),
    )
    parser.add_argument(
        "--snapshot-scope",
        default="manual_update",
        help="Scope label for live snapshot rows (default: manual_update).",
    )
    parser.add_argument(
        "--market-context-file",
        default="data/market_discovery_latest.json",
        help=(
            "Preferred file for market context auto-resolution (default: "
            "data/market_discovery_latest.json)."
        ),
    )
    parser.add_argument("--polymarket-event-slug", default="", help="Optional Polymarket event slug override.")
    parser.add_argument("--polymarket-market-id", default="", help="Optional Polymarket market id override.")
    parser.add_argument("--polymarket-condition-id", default="", help="Optional Polymarket condition id override.")
    parser.add_argument("--polymarket-token-yes", default="", help="Optional Polymarket YES token id override.")
    parser.add_argument("--polymarket-token-no", default="", help="Optional Polymarket NO token id override.")
    parser.add_argument("--kalshi-ticker", default="", help="Optional Kalshi ticker override.")
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _clean_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text if text else None


def _merge_context(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    for key in (
        "polymarket_event_slug",
        "polymarket_market_id",
        "polymarket_condition_id",
        "polymarket_token_yes",
        "polymarket_token_no",
        "kalshi_ticker",
        "market_window_end_epoch_ms",
    ):
        value = source.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if key not in target or target.get(key) in (None, ""):
            target[key] = value


def _context_from_market_discovery_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    polymarket = _as_dict(payload.get("polymarket"))
    kalshi = _as_dict(payload.get("kalshi"))
    pm_selected = _as_dict(polymarket.get("selected_contract"))
    kx_selected = _as_dict(kalshi.get("selected_contract"))

    out["polymarket_event_slug"] = _clean_text(pm_selected.get("event_slug"))
    out["polymarket_market_id"] = _clean_text(pm_selected.get("market_id"))
    out["polymarket_condition_id"] = _clean_text(pm_selected.get("condition_id"))
    out["polymarket_token_yes"] = _clean_text(pm_selected.get("token_yes"))
    out["polymarket_token_no"] = _clean_text(pm_selected.get("token_no"))
    out["kalshi_ticker"] = _clean_text(kx_selected.get("ticker"))
    window_end = _clean_text(pm_selected.get("window_end")) or _clean_text(kx_selected.get("close_time"))
    out["market_window_end_epoch_ms"] = _ts_to_epoch_ms(window_end or "")
    return out


def _context_from_pair_cache_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    pairs = payload.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        return out
    first = _as_dict(pairs[0])
    out["polymarket_event_slug"] = _clean_text(first.get("polymarket_event_slug"))
    out["polymarket_market_id"] = _clean_text(first.get("polymarket_market_id"))
    out["kalshi_ticker"] = _clean_text(first.get("kalshi_ticker"))
    out["market_window_end_epoch_ms"] = _ts_to_epoch_ms(str(first.get("window_end") or ""))
    return out


def _read_market_context_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    body = _as_dict(payload)
    if not body:
        return {}
    if "polymarket" in body or "kalshi" in body:
        return _context_from_market_discovery_payload(body)
    if "pairs" in body:
        return _context_from_pair_cache_payload(body)
    return {}


def _resolve_market_context(args: argparse.Namespace) -> Dict[str, Any]:
    context: Dict[str, Any] = {}

    preferred_raw = str(getattr(args, "market_context_file", "") or "").strip()
    candidates: List[Path] = []
    if preferred_raw:
        preferred = Path(preferred_raw)
        if not preferred.is_absolute():
            preferred = PROJECT_ROOT / preferred
        candidates.append(preferred)
    candidates.append(PROJECT_ROOT / "data/market_pair_cache.json")

    for file_path in candidates:
        discovered = _read_market_context_file(file_path)
        _merge_context(context, discovered)

    manual = {
        "polymarket_event_slug": _clean_text(args.polymarket_event_slug),
        "polymarket_market_id": _clean_text(args.polymarket_market_id),
        "polymarket_condition_id": _clean_text(args.polymarket_condition_id),
        "polymarket_token_yes": _clean_text(args.polymarket_token_yes),
        "polymarket_token_no": _clean_text(args.polymarket_token_no),
        "kalshi_ticker": _clean_text(args.kalshi_ticker),
    }
    _merge_context(context, manual)
    return context


def _write_live_account_snapshot(args: argparse.Namespace) -> Optional[Path]:
    from dotenv import load_dotenv

    from scripts.common.engine_setup import resolve_polymarket_position_user_address_from_env
    from scripts.common.position_polling import (
        KalshiPositionsPollClient,
        PolymarketAccountPollClient,
        PolymarketPositionsPollClient,
        PositionPollClientConfig,
        capture_account_portfolio_snapshot,
    )

    load_dotenv(PROJECT_ROOT / ".env")
    market_context = _resolve_market_context(args)
    update_errors: List[str] = []

    pm_poll_client = None
    pm_account_client = None
    kx_poll_client = None

    pm_user_address = _clean_text(resolve_polymarket_position_user_address_from_env())
    pm_condition_id = _clean_text(market_context.get("polymarket_condition_id"))
    pm_token_yes = _clean_text(market_context.get("polymarket_token_yes"))
    pm_token_no = _clean_text(market_context.get("polymarket_token_no"))
    kx_ticker = _clean_text(market_context.get("kalshi_ticker"))

    if pm_user_address and pm_condition_id and pm_token_yes and pm_token_no:
        try:
            pm_poll_client = PolymarketPositionsPollClient(
                user_address=pm_user_address,
                condition_id=pm_condition_id,
                token_yes=pm_token_yes,
                token_no=pm_token_no,
                config=PositionPollClientConfig(),
            )
        except Exception as exc:
            update_errors.append(f"polymarket_positions_poll_client_init_failed:{type(exc).__name__}:{exc}")
    else:
        update_errors.append(
            "polymarket_positions_poll_client_not_configured:requires_user_address+condition_id+token_yes+token_no"
        )

    try:
        pm_account_client = PolymarketAccountPollClient()
    except Exception as exc:
        update_errors.append(f"polymarket_account_poll_client_init_failed:{type(exc).__name__}:{exc}")

    if kx_ticker:
        try:
            kx_poll_client = KalshiPositionsPollClient(
                market_ticker=kx_ticker,
                config=PositionPollClientConfig(),
            )
        except Exception as exc:
            update_errors.append(f"kalshi_positions_poll_client_init_failed:{type(exc).__name__}:{exc}")
    else:
        update_errors.append("kalshi_positions_poll_client_not_configured:missing_kalshi_ticker")

    snapshot = capture_account_portfolio_snapshot(
        polymarket_client=pm_poll_client,
        polymarket_account_client=pm_account_client,
        kalshi_client=kx_poll_client,
    )

    output_raw = str(getattr(args, "snapshot_output", "") or "").strip()
    if output_raw:
        output_path = Path(output_raw)
        if not output_path.is_absolute():
            output_path = PROJECT_ROOT / output_path
    else:
        output_path = PROJECT_ROOT / "data" / f"account_portfolio_snapshot_log__{_utc_run_id()}.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    recv_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    row: Dict[str, Any] = {
        "ts": _utc_now_iso(),
        "recv_ms": recv_ms,
        "kind": "account_portfolio_snapshot",
        "scope": _clean_text(getattr(args, "snapshot_scope", "")) or "manual_update",
        "market_context": {
            "polymarket_event_slug": _clean_text(market_context.get("polymarket_event_slug")),
            "polymarket_market_id": _clean_text(market_context.get("polymarket_market_id")),
            "polymarket_condition_id": pm_condition_id,
            "polymarket_token_yes": pm_token_yes,
            "polymarket_token_no": pm_token_no,
            "kalshi_ticker": kx_ticker,
            "market_window_end_epoch_ms": market_context.get("market_window_end_epoch_ms"),
        },
        "snapshot": snapshot,
    }
    if update_errors:
        row["update_errors"] = update_errors

    with output_path.open("a", encoding="utf-8") as dst:
        dst.write(json.dumps(row, ensure_ascii=True) + "\n")
    print(f"Wrote live account snapshot row to {output_path}")
    if update_errors:
        print("Live snapshot warnings:")
        for item in update_errors:
            print(f"- {item}")
    return output_path


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
    root = PROJECT_ROOT
    pattern = str(input_glob or "").strip()
    if not pattern:
        return []
    files = sorted(root.glob(pattern))
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
    if bool(args.update_from_api):
        _write_live_account_snapshot(args)
    rows = _collect_rows(
        input_glob=str(args.input_glob),
        kalshi_divisor=float(args.kalshi_divisor),
        polymarket_divisor=float(args.polymarket_divisor),
    )
    changes = _emit_changes(rows, precision=int(args.precision))
    output_raw = str(args.output).strip() or "logs/account_balance_changes.jsonl"
    output_path = Path(output_raw)
    if not output_path.is_absolute():
        output_path = PROJECT_ROOT / output_path
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
