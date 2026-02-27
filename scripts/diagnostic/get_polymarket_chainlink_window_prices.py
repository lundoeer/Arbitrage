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
import math
import random
import re
import sys
import time
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
OVERLOAD_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504, 520, 522, 524, 529})
VALIDATION_LOG_PATTERN = re.compile(r"^(?P<slug>[^|]+)\s+\|")


class ChainlinkHttpError(RuntimeError):
    def __init__(self, url: str, status_code: int, body_preview: str):
        super().__init__(f"HTTP {status_code} from {url}: {body_preview}")
        self.url = url
        self.status_code = status_code
        self.body_preview = body_preview


def _format_eta(seconds: float) -> str:
    safe_seconds = max(0, int(round(seconds)))
    minutes, secs = divmod(safe_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _sleep_backoff(*, attempt: int, base_backoff_seconds: float, jitter_ratio: float) -> None:
    wait = max(0.0, float(base_backoff_seconds)) * (2 ** max(0, attempt - 1))
    safe_jitter = max(0.0, float(jitter_ratio))
    if safe_jitter > 0 and wait > 0:
        wait += random.uniform(0, wait * safe_jitter)
    if wait > 0:
        time.sleep(wait)


def _http_get_with_retry(
    *,
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]],
    timeout_seconds: int,
    allow_redirects: bool,
    max_attempts: int,
    base_backoff_seconds: float,
    jitter_ratio: float,
) -> requests.Response:
    safe_max_attempts = max(1, int(max_attempts))
    last_exception: Optional[Exception] = None
    for attempt in range(1, safe_max_attempts + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=timeout_seconds,
                allow_redirects=allow_redirects,
            )
        except requests.RequestException as exc:
            last_exception = exc
            if attempt >= safe_max_attempts:
                raise RuntimeError(f"Request failed for {url}: {exc}") from exc
            _sleep_backoff(
                attempt=attempt,
                base_backoff_seconds=base_backoff_seconds,
                jitter_ratio=jitter_ratio,
            )
            continue

        if response.status_code == 200:
            return response

        is_overload = response.status_code in OVERLOAD_HTTP_STATUS_CODES
        if is_overload and attempt < safe_max_attempts:
            retry_after = response.headers.get("Retry-After", "").strip()
            if retry_after:
                try:
                    wait_seconds = float(retry_after)
                except ValueError:
                    wait_seconds = 0.0
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                    continue
            _sleep_backoff(
                attempt=attempt,
                base_backoff_seconds=base_backoff_seconds,
                jitter_ratio=jitter_ratio,
            )
            continue

        raise ChainlinkHttpError(url, response.status_code, response.text[:500])

    if last_exception is not None:
        raise RuntimeError(f"Request failed for {url}: {last_exception}") from last_exception
    raise RuntimeError(f"Request failed for {url}: exhausted retries")


def _is_overload_error(exc: Exception) -> Tuple[bool, Optional[int]]:
    status_code: Optional[int] = None
    if isinstance(exc, ChainlinkHttpError):
        status_code = exc.status_code
    else:
        match = re.search(r"\bHTTP\s+(?P<status>\d{3})\b", str(exc))
        if match:
            status_code = int(match.group("status"))
    if status_code is None:
        return False, None
    return status_code in OVERLOAD_HTTP_STATUS_CODES, status_code


def _read_validation_log_map(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    result: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = VALIDATION_LOG_PATTERN.match(line)
        if not match:
            continue
        result[match.group("slug").strip()] = line
    return result


def _save_validation_log(
    *,
    path: Path,
    entries: List[Dict[str, str]],
    validation_log_map: Dict[str, str],
) -> int:
    rendered_lines: List[str] = []
    for entry in entries:
        line = validation_log_map.get(entry["slug"])
        if line:
            rendered_lines.append(line)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(rendered_lines) + ("\n" if rendered_lines else ""), encoding="utf-8")
    return len(rendered_lines)


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_state(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _entry_window_end(entry: Dict[str, str]) -> Optional[datetime]:
    return _parse_iso_datetime(entry.get("window_end"))


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


def _resolve_chainlink_stream_page_url(
    resolution_source: str,
    *,
    session: requests.Session,
    max_attempts: int,
    base_backoff_seconds: float,
    jitter_ratio: float,
) -> str:
    if not resolution_source:
        raise RuntimeError("Polymarket market/event has no resolutionSource URL.")
    if "data.chain.link/streams/" not in resolution_source:
        raise RuntimeError(f"Unexpected resolutionSource (not data.chain.link streams): {resolution_source}")

    # Follow redirect from /streams/btc-usd to concrete stream page slug.
    response = _http_get_with_retry(
        session=session,
        url=resolution_source,
        params=None,
        timeout_seconds=20,
        allow_redirects=True,
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
        jitter_ratio=jitter_ratio,
    )
    return response.url


def _fetch_chainlink_markers(
    feed_id: str,
    time_range: str,
    *,
    session: requests.Session,
    max_attempts: int,
    base_backoff_seconds: float,
    jitter_ratio: float,
) -> List[Dict[str, Any]]:
    response = _http_get_with_retry(
        session=session,
        url=f"{CHAINLINK_BASE}/api/historical-timescale-stream-data",
        params={"feedId": feed_id, "timeRange": time_range},
        timeout_seconds=30,
        allow_redirects=False,
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
        jitter_ratio=jitter_ratio,
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


def run(
    slug: str,
    output_path: Optional[Path],
    *,
    polymarket_transport: Optional[ApiTransport] = None,
    chainlink_session: Optional[requests.Session] = None,
    chainlink_max_attempts: int = 3,
    chainlink_base_backoff_seconds: float = 1.0,
    chainlink_jitter_ratio: float = 0.2,
) -> Dict[str, Any]:
    owns_polymarket_transport = polymarket_transport is None
    owns_chainlink_session = chainlink_session is None
    if polymarket_transport is None:
        polymarket_transport = ApiTransport(
            default_headers={
                "User-Agent": "Arbitrage-Chainlink-Window-Price/1.0",
                "Accept": "application/json, text/plain, */*",
            }
        )
    if chainlink_session is None:
        chainlink_session = requests.Session()

    try:
        event, market = _fetch_polymarket_market_by_slug(polymarket_transport, slug)

        window_end = _parse_iso_datetime(str(market.get("endDate") or event.get("endDate") or ""))
        if window_end is None:
            raise RuntimeError(f"Could not parse market endDate for slug {slug}")
        window_start = window_end - timedelta(seconds=WINDOW_SECONDS)

        resolution_source = str(market.get("resolutionSource") or event.get("resolutionSource") or "").strip()
        stream_page_url = _resolve_chainlink_stream_page_url(
            resolution_source,
            session=chainlink_session,
            max_attempts=chainlink_max_attempts,
            base_backoff_seconds=chainlink_base_backoff_seconds,
            jitter_ratio=chainlink_jitter_ratio,
        )

        html = _http_get_with_retry(
            session=chainlink_session,
            url=stream_page_url,
            params=None,
            timeout_seconds=30,
            allow_redirects=False,
            max_attempts=chainlink_max_attempts,
            base_backoff_seconds=chainlink_base_backoff_seconds,
            jitter_ratio=chainlink_jitter_ratio,
        ).text
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

        markers = _fetch_chainlink_markers(
            feed_id,
            "1D",
            session=chainlink_session,
            max_attempts=chainlink_max_attempts,
            base_backoff_seconds=chainlink_base_backoff_seconds,
            jitter_ratio=chainlink_jitter_ratio,
        )
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
    finally:
        if owns_polymarket_transport:
            try:
                polymarket_transport.session.close()
            except Exception:
                pass
        if owns_chainlink_session:
            try:
                chainlink_session.close()
            except Exception:
                pass


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
    only_missing: bool = False,
    max_slugs: Optional[int] = None,
    dry_run: bool = False,
    progress_every: int = 25,
    sleep_ms: int = 0,
    max_consecutive_overload: int = 5,
    state_path: Optional[Path] = None,
    resume_from_state: bool = True,
    retry_errors: bool = False,
    max_chainlink_15m_age_hours: int = 26,
    chainlink_max_attempts: int = 3,
    chainlink_base_backoff_seconds: float = 1.0,
    chainlink_jitter_ratio: float = 0.2,
) -> Dict[str, Any]:
    entries = _parse_resolved_pairs_log(resolved_log_path)
    per_slug_dir.mkdir(parents=True, exist_ok=True)
    validation_log_path.parent.mkdir(parents=True, exist_ok=True)
    existing_validation_log_map = _read_validation_log_map(validation_log_path)
    selected_entries: List[Dict[str, str]] = []
    for entry in entries:
        slug = entry["slug"]
        out_path = per_slug_dir / f"{_safe_name(slug)}.json"
        has_json = out_path.exists()
        existing_validation_line = existing_validation_log_map.get(slug)
        if only_missing:
            if existing_validation_line and not retry_errors:
                continue
            if existing_validation_line and retry_errors and " | ERROR=" not in existing_validation_line:
                continue
            if has_json and existing_validation_line and " | ERROR=" not in existing_validation_line:
                continue
        selected_entries.append(entry)

    if max_slugs is not None and max_slugs > 0:
        selected_entries = selected_entries[: max_slugs]

    now_utc = datetime.now(timezone.utc)
    max_age_seconds = max_chainlink_15m_age_hours * 3600 if max_chainlink_15m_age_hours > 0 else 0
    estimated_skipped_by_age = 0
    if max_age_seconds > 0:
        cutoff = now_utc - timedelta(seconds=max_age_seconds)
        for entry in selected_entries:
            window_end = _entry_window_end(entry)
            if window_end is not None and window_end < cutoff:
                estimated_skipped_by_age += 1

    estimated_http_calls_per_slug = 4
    if dry_run:
        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": {
                "dry_run": True,
                "only_missing": only_missing,
                "max_slugs": max_slugs,
                "retry_errors": retry_errors,
                "max_chainlink_15m_age_hours": max_chainlink_15m_age_hours,
            },
            "inputs": {
                "resolved_log_path": str(resolved_log_path),
                "slug_count_total": len(entries),
                "slug_count_selected": len(selected_entries),
                "slug_count_already_complete": len(entries) - len(selected_entries),
            },
            "estimates": {
                "estimated_http_calls_per_slug": estimated_http_calls_per_slug,
                "estimated_http_calls_total": estimated_http_calls_per_slug * len(selected_entries),
                "estimated_skipped_by_age_before_requests": estimated_skipped_by_age,
            },
            "outputs": {
                "validation_log_path": str(validation_log_path),
                "summary_path": str(summary_path),
                "per_slug_dir": str(per_slug_dir),
                "state_path": str(state_path) if state_path else None,
            },
            "totals": {
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "matches": 0,
                "mismatches": 0,
            },
            "mismatch_examples": [],
            "failure_examples": [],
        }
        _write_json(summary_path, summary)
        return summary

    next_index = 0
    state_loaded = False
    if state_path is not None and resume_from_state:
        state = _load_state(state_path)
        same_mode = bool(state.get("only_missing", False)) == bool(only_missing)
        same_source = str(state.get("resolved_log_path", "")) == str(resolved_log_path)
        same_selected_count = int(state.get("selected_slug_count", -1)) == len(selected_entries)
        candidate_next = state.get("next_index")
        if same_mode and same_source and same_selected_count and isinstance(candidate_next, int):
            if 0 <= candidate_next <= len(selected_entries):
                next_index = candidate_next
                state_loaded = True

    results: List[Dict[str, Any]] = []
    validation_log_map = dict(existing_validation_log_map)
    progress_every_safe = max(1, int(progress_every))
    sleep_seconds = max(0.0, float(sleep_ms) / 1000.0)
    started_at = time.time()
    processed_in_run = 0
    terminated_early = False
    termination_reason: Optional[str] = None
    overload_termination_status_code: Optional[int] = None
    consecutive_overload_errors = 0
    skipped_by_age = 0

    polymarket_transport = ApiTransport(
        default_headers={
            "User-Agent": "Arbitrage-Chainlink-Window-Price/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )
    chainlink_session = requests.Session()
    try:
        for idx in range(next_index, len(selected_entries)):
            entry = selected_entries[idx]
            slug = entry["slug"]
            window_end = entry["window_end"]
            registered_raw = entry["registered_raw"]
            registered_norm = entry["registered_norm"]
            out_path = per_slug_dir / f"{_safe_name(slug)}.json"
            next_index = idx + 1
            processed_in_run += 1
            skipped_due_age = False

            if max_chainlink_15m_age_hours > 0:
                window_end_dt = _entry_window_end(entry)
                if window_end_dt is not None:
                    cutoff = now_utc - timedelta(hours=max_chainlink_15m_age_hours)
                    if window_end_dt < cutoff:
                        skipped_due_age = True
                        skipped_by_age += 1
                        validation_log_map[slug] = (
                            f"{slug} | window_end={window_end} | "
                            f"SKIPPED=outside_chainlink_15m_window(max_age_hours={max_chainlink_15m_age_hours}) | "
                            f"registered={registered_raw}({registered_norm})"
                        )
                        result = {
                            "slug": slug,
                            "window_end": window_end,
                            "registered_raw": registered_raw,
                            "registered_norm": registered_norm,
                            "skipped": True,
                            "skip_reason": "outside_chainlink_15m_window",
                            "per_slug_json": str(out_path),
                        }
                        results.append(result)

            if not skipped_due_age:
                try:
                    payload = run(
                        slug=slug,
                        output_path=out_path,
                        polymarket_transport=polymarket_transport,
                        chainlink_session=chainlink_session,
                        chainlink_max_attempts=chainlink_max_attempts,
                        chainlink_base_backoff_seconds=chainlink_base_backoff_seconds,
                        chainlink_jitter_ratio=chainlink_jitter_ratio,
                    )
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
                    validation_log_map[slug] = (
                        f"{slug} | window_end={window_end} | target={target_price:.8f} | end={end_price:.8f} | "
                        f"delta={delta:.8f} | computed={computed_norm} | registered={registered_raw}({registered_norm}) | "
                        f"match={'yes' if match else 'no'}"
                    )
                    consecutive_overload_errors = 0
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
                    validation_log_map[slug] = (
                        f"{slug} | window_end={window_end} | ERROR={error} | "
                        f"registered={registered_raw}({registered_norm})"
                    )
                    is_overload, overload_status = _is_overload_error(exc)
                    if is_overload:
                        consecutive_overload_errors += 1
                    else:
                        consecutive_overload_errors = 0

                    if max_consecutive_overload > 0 and consecutive_overload_errors >= max_consecutive_overload:
                        terminated_early = True
                        overload_termination_status_code = overload_status
                        termination_reason = f"consecutive_overload_errors_reached_{max_consecutive_overload}"
                        print(
                            f"Stopping early due to overload responses (consecutive={consecutive_overload_errors}, "
                            f"status={overload_status})."
                        )

            if state_path is not None:
                _save_state(
                    state_path,
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "resolved_log_path": str(resolved_log_path),
                        "only_missing": bool(only_missing),
                        "selected_slug_count": len(selected_entries),
                        "next_index": next_index,
                        "processed_in_current_run": processed_in_run,
                    },
                )

            should_print_progress = (
                processed_in_run == 1
                or processed_in_run % progress_every_safe == 0
                or next_index >= len(selected_entries)
                or terminated_early
            )
            if should_print_progress:
                elapsed = max(0.001, time.time() - started_at)
                rate_per_min = (processed_in_run / elapsed) * 60.0
                remaining = max(0, len(selected_entries) - next_index)
                eta_seconds = (remaining / processed_in_run) * elapsed if processed_in_run > 0 else math.inf
                print(
                    f"[{next_index}/{len(selected_entries)}] processed={processed_in_run} "
                    f"rate={rate_per_min:.2f}/min eta={_format_eta(eta_seconds)} slug={slug}"
                )

            if terminated_early:
                break

            if sleep_seconds > 0 and next_index < len(selected_entries):
                time.sleep(sleep_seconds)
    finally:
        try:
            polymarket_transport.session.close()
        except Exception:
            pass
        try:
            chainlink_session.close()
        except Exception:
            pass

    validation_lines_after_run = _save_validation_log(
        path=validation_log_path,
        entries=entries,
        validation_log_map=validation_log_map,
    )

    successful = [r for r in results if "error" not in r]
    failures = [r for r in results if "error" in r]
    successful_non_skipped = [r for r in successful if not r.get("skipped")]
    matches = [r for r in successful_non_skipped if r["match"] is True]
    mismatches = [r for r in successful_non_skipped if r["match"] is False]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": {
            "dry_run": False,
            "only_missing": only_missing,
            "max_slugs": max_slugs,
            "retry_errors": retry_errors,
            "max_chainlink_15m_age_hours": max_chainlink_15m_age_hours,
            "progress_every": progress_every_safe,
            "sleep_ms": int(max(0, sleep_ms)),
            "resume_from_state": resume_from_state,
            "resumed_from_state": state_loaded,
        },
        "inputs": {
            "resolved_log_path": str(resolved_log_path),
            "slug_count_total": len(entries),
            "slug_count_selected": len(selected_entries),
            "slug_count_already_complete": len(entries) - len(selected_entries),
            "estimated_skipped_by_age_before_requests": estimated_skipped_by_age,
        },
        "outputs": {
            "validation_log_path": str(validation_log_path),
            "summary_path": str(summary_path),
            "per_slug_dir": str(per_slug_dir),
            "state_path": str(state_path) if state_path else None,
        },
        "totals": {
            "processed": processed_in_run,
            "selected_total": len(selected_entries),
            "next_index": next_index,
            "successful": len(successful),
            "successful_non_skipped": len(successful_non_skipped),
            "failed": len(failures),
            "matches": len(matches),
            "mismatches": len(mismatches),
            "skipped_by_age": skipped_by_age,
            "validation_lines_after_run": validation_lines_after_run,
            "terminated_early": terminated_early,
            "overload_termination_status_code": overload_termination_status_code,
        },
        "termination": {
            "terminated_early": terminated_early,
            "reason": termination_reason,
            "overload_status_code": overload_termination_status_code,
        },
        "estimates": {
            "estimated_http_calls_per_slug": estimated_http_calls_per_slug,
            "estimated_http_calls_selected_total": estimated_http_calls_per_slug * len(selected_entries),
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
    only_missing: bool = False,
    max_slugs: Optional[int] = None,
    dry_run: bool = False,
    progress_every: int = 25,
    sleep_ms: int = 0,
    max_consecutive_overload: int = 5,
    state_path: Optional[Path] = None,
    resume_from_state: bool = True,
    retry_errors: bool = False,
    max_chainlink_15m_age_hours: int = 26,
    chainlink_max_attempts: int = 3,
    chainlink_base_backoff_seconds: float = 1.0,
    chainlink_jitter_ratio: float = 0.2,
) -> Dict[str, Any]:
    return _run_batch(
        resolved_log_path=resolved_log_path,
        validation_log_path=validation_log_path,
        summary_path=summary_path,
        per_slug_dir=per_slug_dir,
        only_missing=only_missing,
        max_slugs=max_slugs,
        dry_run=dry_run,
        progress_every=progress_every,
        sleep_ms=sleep_ms,
        max_consecutive_overload=max_consecutive_overload,
        state_path=state_path,
        resume_from_state=resume_from_state,
        retry_errors=retry_errors,
        max_chainlink_15m_age_hours=max_chainlink_15m_age_hours,
        chainlink_max_attempts=chainlink_max_attempts,
        chainlink_base_backoff_seconds=chainlink_base_backoff_seconds,
        chainlink_jitter_ratio=chainlink_jitter_ratio,
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
    parser.add_argument(
        "--only-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="In batch mode, process only slugs missing per-slug JSON and/or validation log lines.",
    )
    parser.add_argument(
        "--max-slugs",
        type=int,
        default=None,
        help="In batch mode, process at most this many selected slugs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="In batch mode, estimate work only and do not call remote APIs.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Print progress every N processed slugs in batch mode.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Optional sleep between slugs in batch mode (milliseconds).",
    )
    parser.add_argument(
        "--max-consecutive-overload",
        type=int,
        default=5,
        help="Terminate early after this many consecutive overload HTTP errors (0 disables).",
    )
    parser.add_argument(
        "--state-file",
        default="data/diagnostic/chainlink_window_prices_batch_state.json",
        help="Batch checkpoint state JSON file.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoint state and start selected batch from the beginning.",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="When --only-missing is used, retry slugs that already have ERROR lines in validation log.",
    )
    parser.add_argument(
        "--max-chainlink-15m-age-hours",
        type=int,
        default=26,
        help="Skip API calls for entries older than this age because Chainlink 15m markers are unavailable (0 disables).",
    )
    parser.add_argument(
        "--chainlink-max-attempts",
        type=int,
        default=3,
        help="Max attempts per Chainlink HTTP request.",
    )
    parser.add_argument(
        "--chainlink-base-backoff-seconds",
        type=float,
        default=1.0,
        help="Base backoff seconds for Chainlink retries.",
    )
    parser.add_argument(
        "--chainlink-jitter-ratio",
        type=float,
        default=0.2,
        help="Jitter ratio for Chainlink retries.",
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
        only_missing=bool(args.only_missing),
        max_slugs=args.max_slugs,
        dry_run=bool(args.dry_run),
        progress_every=args.progress_every,
        sleep_ms=args.sleep_ms,
        max_consecutive_overload=args.max_consecutive_overload,
        state_path=Path(args.state_file) if args.state_file else None,
        resume_from_state=not bool(args.no_resume),
        retry_errors=bool(args.retry_errors),
        max_chainlink_15m_age_hours=args.max_chainlink_15m_age_hours,
        chainlink_max_attempts=args.chainlink_max_attempts,
        chainlink_base_backoff_seconds=args.chainlink_base_backoff_seconds,
        chainlink_jitter_ratio=args.chainlink_jitter_ratio,
    )
    totals = summary["totals"]
    print("Batch Chainlink boundary price validation complete")
    print(f"Processed:  {totals['processed']}")
    print(f"Successful: {totals['successful']}")
    print(f"Failed:     {totals['failed']}")
    print(f"Skipped by age: {totals.get('skipped_by_age')}")
    print(f"Matches:    {totals['matches']}")
    print(f"Mismatches: {totals['mismatches']}")
    if totals.get("terminated_early"):
        print(f"Terminated early: yes ({summary.get('termination', {}).get('reason')})")
    print(f"Selected slugs: {summary.get('inputs', {}).get('slug_count_selected')}")
    print(f"Validation lines now: {totals.get('validation_lines_after_run')}")
    print(f"Validation log: {summary['outputs']['validation_log_path']}")
    print(f"Summary JSON:   {summary['outputs']['summary_path']}")
    print(f"Per-slug JSON dir: {summary['outputs']['per_slug_dir']}")


if __name__ == "__main__":
    main()
