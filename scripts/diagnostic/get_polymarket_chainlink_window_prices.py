#!/usr/bin/env python3
"""
Get Chainlink BTC start/end prices for a Polymarket BTC up/down 15m market slug.

Usage:
  python scripts/diagnostic/get_polymarket_chainlink_window_prices.py --slug btc-updown-15m-1771370100
  python scripts/diagnostic/get_polymarket_chainlink_window_prices.py --batch-from-log data/diagnostic/resolved_15m_pairs.log
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport


POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
CHAINLINK_BASE = "https://data.chain.link"
WINDOW_SECONDS = 900


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _extract_next_data_json(html: str) -> Dict[str, Any]:
    marker = '<script id="__NEXT_DATA__" type="application/json">'
    start = html.find(marker)
    if start < 0:
        raise RuntimeError("Could not find __NEXT_DATA__ on Chainlink stream page.")
    end = html.find("</script>", start)
    if end < 0:
        raise RuntimeError("Could not parse __NEXT_DATA__ script block.")
    blob = html[start + len(marker) : end]
    try:
        return json.loads(blob)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse Chainlink __NEXT_DATA__ JSON: {exc}") from exc


def _to_decimal_price(raw_value: Optional[str], divisor: float) -> Optional[float]:
    if raw_value is None:
        return None
    try:
        return float(raw_value) / divisor
    except Exception:
        return None


def _normalize_snapshot(node: Dict[str, Any], divisor: float) -> Dict[str, Any]:
    return {
        "time_bucket": str(node.get("timeBucket", "")),
        "open_raw": node.get("open"),
        "mid_raw": node.get("mid"),
        "bid_raw": node.get("bid"),
        "ask_raw": node.get("ask"),
        "open": _to_decimal_price(node.get("open"), divisor),
        "mid": _to_decimal_price(node.get("mid"), divisor),
        "bid": _to_decimal_price(node.get("bid"), divisor),
        "ask": _to_decimal_price(node.get("ask"), divisor),
    }


def _select_boundary_price(snapshot: Dict[str, Any]) -> Optional[float]:
    # For 15m markers, "open" is the price at that exact bucket boundary.
    if snapshot.get("open") is not None:
        return float(snapshot["open"])
    if snapshot.get("mid") is not None:
        return float(snapshot["mid"])
    return None


def _fetch_polymarket_market_by_slug(transport: ApiTransport, slug: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    _, events = transport.request_json("GET", f"{POLYMARKET_GAMMA_API}/events", params={"slug": slug})
    if not isinstance(events, list) or not events:
        raise RuntimeError(f"No Polymarket event found for slug {slug}")

    event = events[0]
    if not isinstance(event, dict):
        raise RuntimeError(f"Unexpected event payload for slug {slug}")

    event_slug = str(event.get("slug", "")).strip()
    if event_slug != slug:
        raise RuntimeError(f"Polymarket returned unexpected event slug {event_slug} for requested slug {slug}")

    markets = event.get("markets", [])
    if not isinstance(markets, list) or not markets:
        raise RuntimeError(f"No market objects found inside event {slug}")

    market_match: Optional[Dict[str, Any]] = None
    for market in markets:
        if str(market.get("slug", "")).strip() == slug:
            market_match = market
            break
    if market_match is None:
        raise RuntimeError(f"Could not find market with slug {slug} in event payload.")

    return event, market_match


def _resolve_chainlink_stream_page_url(resolution_source: str) -> str:
    if not resolution_source:
        raise RuntimeError("Polymarket market/event has no resolutionSource URL.")
    if "data.chain.link/streams/" not in resolution_source:
        raise RuntimeError(f"Unexpected resolutionSource (not data.chain.link streams): {resolution_source}")

    # Follow redirect from /streams/btc-usd to concrete stream page slug.
    response = requests.get(resolution_source, allow_redirects=True, timeout=20)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to load Chainlink stream page: {resolution_source} ({response.status_code})")
    return response.url


def _fetch_chainlink_markers(feed_id: str, time_range: str) -> List[Dict[str, Any]]:
    response = requests.get(
        f"{CHAINLINK_BASE}/api/historical-timescale-stream-data",
        params={"feedId": feed_id, "timeRange": time_range},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Chainlink timescale endpoint failed ({response.status_code}) for feedId={feed_id} timeRange={time_range}"
        )
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected Chainlink timescale payload.")

    data = payload.get("data", {})
    if not isinstance(data, dict):
        raise RuntimeError("Chainlink timescale payload missing data object.")

    # Prefer 15m markers when present.
    candidates = ["mercuryHistory15MinMarkers", "mercuryHistory1Month"]
    for field in candidates:
        block = data.get(field)
        if isinstance(block, dict) and isinstance(block.get("nodes"), list):
            nodes = block["nodes"]
            if nodes:
                return nodes
    return []


def _find_node_by_timestamp(nodes: List[Dict[str, Any]], target_dt: datetime) -> Optional[Dict[str, Any]]:
    target_iso = target_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "+00:00")
    for node in nodes:
        bucket = _parse_iso_datetime(str(node.get("timeBucket", "")))
        if bucket is None:
            continue
        if bucket == target_dt:
            return node
    # fallback exact string compare in case parser normalization differs
    for node in nodes:
        if str(node.get("timeBucket", "")).strip() == target_iso:
            return node
    return None


def run(slug: str, output_path: Optional[Path]) -> Dict[str, Any]:
    polymarket = ApiTransport(
        default_headers={
            "User-Agent": "Arbitrage-Chainlink-Window-Price/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )

    event, market = _fetch_polymarket_market_by_slug(polymarket, slug)

    window_end = _parse_iso_datetime(str(market.get("endDate") or event.get("endDate") or ""))
    if window_end is None:
        raise RuntimeError(f"Could not parse market endDate for slug {slug}")
    window_start = window_end - timedelta(seconds=WINDOW_SECONDS)

    resolution_source = str(market.get("resolutionSource") or event.get("resolutionSource") or "").strip()
    stream_page_url = _resolve_chainlink_stream_page_url(resolution_source)

    html = requests.get(stream_page_url, timeout=30).text
    next_data = _extract_next_data_json(html)
    stream_data = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("streamData", {})
    )
    stream_metadata = stream_data.get("streamMetadata", {}) if isinstance(stream_data, dict) else {}
    feed_id = str(stream_metadata.get("feedId") or "").strip()
    if not feed_id:
        raise RuntimeError("Could not extract Chainlink feedId from stream page metadata.")

    divisor = float(stream_metadata.get("multiply") or 1e18)
    if divisor <= 0:
        raise RuntimeError(f"Invalid Chainlink multiply value: {divisor}")

    # 15-minute exact markers are reliably available in 1D window.
    markers = _fetch_chainlink_markers(feed_id, "1D")
    if not markers:
        raise RuntimeError("Chainlink returned no historical marker nodes for timeRange=1D.")

    start_node = _find_node_by_timestamp(markers, window_start)
    end_node = _find_node_by_timestamp(markers, window_end)
    if start_node is None or end_node is None:
        raise RuntimeError(
            "Could not find exact Chainlink 15m markers for market boundaries. "
            f"required_start={window_start.isoformat()} required_end={window_end.isoformat()} "
            "This usually means the market is outside the endpoint's available exact marker window."
        )

    start_snapshot = _normalize_snapshot(start_node, divisor)
    end_snapshot = _normalize_snapshot(end_node, divisor)

    target_price = _select_boundary_price(start_snapshot)
    end_price = _select_boundary_price(end_snapshot)
    if target_price is None or end_price is None:
        raise RuntimeError("Could not derive numeric target/end price from Chainlink marker snapshots.")

    implied_result = "UP_OR_YES" if end_price >= target_price else "DOWN_OR_NO"

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "polymarket": {
            "slug": slug,
            "event_id": str(event.get("id", "")),
            "market_id": str(market.get("id", "")),
            "question": str(market.get("question", "")),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "resolution_source": resolution_source,
        },
        "chainlink": {
            "stream_page_url": stream_page_url,
            "feed_id": feed_id,
            "multiply": divisor,
            "time_range_used": "1D",
            "start_snapshot": start_snapshot,
            "end_snapshot": end_snapshot,
            "target_price": target_price,
            "end_price": end_price,
            "delta": end_price - target_price,
            "implied_outcome": implied_result,
        },
    }

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    return payload


def _parse_resolved_pairs_log(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Resolved pairs log not found: {path}")

    entries: List[Dict[str, str]] = []
    seen_slugs: set[str] = set()

    # Example line:
    # 2026-02-17T23:30:00+00:00 | kalshi=... | polymarket=btc-updown-15m-1771370100:Down(NO) | different
    pattern = re.compile(
        r"^(?P<window_end>[^|]+)\s+\|\s+kalshi=.*\|\s+polymarket=(?P<slug>[^:]+):(?P<raw>[^()]+)\((?P<norm>YES|NO)\)\s+\|\s+.*$"
    )

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = pattern.match(line)
        if not match:
            continue
        slug = match.group("slug").strip()
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        entries.append(
            {
                "window_end": match.group("window_end").strip(),
                "slug": slug,
                "registered_raw": match.group("raw").strip(),
                "registered_norm": match.group("norm").strip(),
            }
        )

    if not entries:
        raise RuntimeError(f"No parseable polymarket entries found in {path}")
    return entries


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _run_batch(
    *,
    resolved_log_path: Path,
    validation_log_path: Path,
    summary_path: Path,
    per_slug_dir: Path,
) -> Dict[str, Any]:
    entries = _parse_resolved_pairs_log(resolved_log_path)

    per_slug_dir.mkdir(parents=True, exist_ok=True)
    validation_log_path.parent.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, Any]] = []
    log_lines: List[str] = []

    for entry in entries:
        slug = entry["slug"]
        window_end = entry["window_end"]
        registered_raw = entry["registered_raw"]
        registered_norm = entry["registered_norm"]
        out_path = per_slug_dir / f"{_safe_name(slug)}.json"

        try:
            payload = run(slug=slug, output_path=out_path)
            target_price = float(payload["chainlink"]["target_price"])
            end_price = float(payload["chainlink"]["end_price"])
            delta = float(payload["chainlink"]["delta"])
            implied = str(payload["chainlink"]["implied_outcome"])
            computed_norm = "YES" if implied == "UP_OR_YES" else "NO"
            match = computed_norm == registered_norm

            result = {
                "slug": slug,
                "window_end": window_end,
                "registered_raw": registered_raw,
                "registered_norm": registered_norm,
                "target_price": target_price,
                "end_price": end_price,
                "delta": delta,
                "computed_norm": computed_norm,
                "computed_implied_outcome": implied,
                "match": match,
                "per_slug_json": str(out_path),
            }
            results.append(result)
            log_lines.append(
                f"{slug} | window_end={window_end} | target={target_price:.8f} | end={end_price:.8f} | "
                f"delta={delta:.8f} | computed={computed_norm} | registered={registered_raw}({registered_norm}) | "
                f"match={'yes' if match else 'no'}"
            )
        except Exception as exc:
            error = str(exc)
            results.append(
                {
                    "slug": slug,
                    "window_end": window_end,
                    "registered_raw": registered_raw,
                    "registered_norm": registered_norm,
                    "error": error,
                    "match": None,
                    "per_slug_json": str(out_path),
                }
            )
            log_lines.append(
                f"{slug} | window_end={window_end} | ERROR={error} | "
                f"registered={registered_raw}({registered_norm})"
            )

    validation_log_path.write_text("\n".join(log_lines) + ("\n" if log_lines else ""), encoding="utf-8")

    successful = [r for r in results if "error" not in r]
    failures = [r for r in results if "error" in r]
    matches = [r for r in successful if r["match"] is True]
    mismatches = [r for r in successful if r["match"] is False]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "resolved_log_path": str(resolved_log_path),
            "slug_count": len(entries),
        },
        "outputs": {
            "validation_log_path": str(validation_log_path),
            "summary_path": str(summary_path),
            "per_slug_dir": str(per_slug_dir),
        },
        "totals": {
            "processed": len(entries),
            "successful": len(successful),
            "failed": len(failures),
            "matches": len(matches),
            "mismatches": len(mismatches),
        },
        "mismatch_examples": mismatches[:50],
        "failure_examples": failures[:50],
    }
    _write_json(summary_path, summary)
    return summary


def run_batch(
    *,
    resolved_log_path: Path,
    validation_log_path: Path,
    summary_path: Path,
    per_slug_dir: Path,
) -> Dict[str, Any]:
    return _run_batch(
        resolved_log_path=resolved_log_path,
        validation_log_path=validation_log_path,
        summary_path=summary_path,
        per_slug_dir=per_slug_dir,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Get Chainlink target/end BTC prices for a Polymarket BTC 15m market slug."
    )
    parser.add_argument("--slug", default=None, help="Polymarket market slug, e.g. btc-updown-15m-1771370100")
    parser.add_argument(
        "--batch-from-log",
        default=None,
        help="Process all Polymarket slugs from resolved_15m_pairs.log",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional output JSON path. Default: data/diagnostic/chainlink_window_prices_<slug>.json",
    )
    parser.add_argument(
        "--batch-validation-log",
        default="data/diagnostic/polymarket_chainlink_price_validation.log",
        help="Output log path for batch validation results.",
    )
    parser.add_argument(
        "--batch-summary",
        default="data/diagnostic/polymarket_chainlink_price_validation_summary.json",
        help="Output JSON summary path for batch validation.",
    )
    parser.add_argument(
        "--batch-per-slug-dir",
        default="data/diagnostic/chainlink_window_prices_batch",
        help="Directory for per-slug JSON artifacts in batch mode.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    if bool(args.slug) == bool(args.batch_from_log):
        raise RuntimeError("Provide exactly one of --slug or --batch-from-log")

    if args.slug:
        default_out = Path(f"data/diagnostic/chainlink_window_prices_{args.slug}.json")
        out_path = Path(args.out) if args.out else default_out

        payload = run(slug=args.slug, output_path=out_path)
        print("Chainlink boundary price lookup complete")
        print(f"Slug: {payload['polymarket']['slug']}")
        print(f"Window start: {payload['polymarket']['window_start']}")
        print(f"Window end:   {payload['polymarket']['window_end']}")
        print(f"Target price: {payload['chainlink']['target_price']}")
        print(f"End price:    {payload['chainlink']['end_price']}")
        print(f"Delta:        {payload['chainlink']['delta']}")
        print(f"Implied:      {payload['chainlink']['implied_outcome']}")
        print(f"Saved:        {out_path}")
        return

    summary = _run_batch(
        resolved_log_path=Path(args.batch_from_log),
        validation_log_path=Path(args.batch_validation_log),
        summary_path=Path(args.batch_summary),
        per_slug_dir=Path(args.batch_per_slug_dir),
    )
    totals = summary["totals"]
    print("Batch Chainlink boundary price validation complete")
    print(f"Processed:  {totals['processed']}")
    print(f"Successful: {totals['successful']}")
    print(f"Failed:     {totals['failed']}")
    print(f"Matches:    {totals['matches']}")
    print(f"Mismatches: {totals['mismatches']}")
    print(f"Validation log: {summary['outputs']['validation_log_path']}")
    print(f"Summary JSON:   {summary['outputs']['summary_path']}")
    print(f"Per-slug JSON dir: {summary['outputs']['per_slug_dir']}")


if __name__ == "__main__":
    main()
