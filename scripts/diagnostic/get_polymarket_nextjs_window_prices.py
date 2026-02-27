#!/usr/bin/env python3
"""
Fetch Polymarket BTC 15m open/close window prices from Next.js payloads.

Writes incrementally to JSONL so runs are inspectable and resumable.
This script does NOT modify resolved_15m_pairs.log.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

OVERLOAD_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504, 520, 522, 524, 529})

BASE_LINE_PATTERN = re.compile(
    r"^(?P<window_end>[^|]+)\s+\|\s+"
    r"kalshi=(?P<kalshi_ticker>[^:]+):(?P<kalshi_raw>[^()]+)\((?P<kalshi_norm>[^()]*)\)\s+\|\s+"
    r"polymarket=(?P<slug>[^:]+):(?P<pm_raw>[^()]+)\((?P<pm_norm>[^()]*)\)\s+\|\s+"
    r"(?P<comparison>[^|]+)(?P<extras>.*)$"
)

NEXT_DATA_SCRIPT_PATTERN = re.compile(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(?P<data>.*?)</script>', re.DOTALL)
BUILD_ID_FALLBACK_PATTERN = re.compile(r"/_next/data/(?P<build_id>[^/]+)/")
BUILD_ID_JSON_PATTERN = re.compile(r'"buildId":"(?P<build_id>[^"]+)"')
BUILD_ID_MANIFEST_PATTERN = re.compile(r"/_next/static/(?P<build_id>[^/]+)/_buildManifest\.js")


def _parse_iso_utc(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _sleep_backoff(*, attempt: int, base_backoff_seconds: float, jitter_ratio: float) -> None:
    wait = max(0.0, float(base_backoff_seconds)) * (2 ** max(0, attempt - 1))
    safe_jitter = max(0.0, float(jitter_ratio))
    if safe_jitter > 0 and wait > 0:
        wait += random.uniform(0.0, wait * safe_jitter)
    if wait > 0:
        time.sleep(wait)


class RateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        safe_rps = max(0.1, float(requests_per_second))
        self.min_interval = 1.0 / safe_rps
        self.last_call = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.monotonic()


def _http_get_with_retry(
    *,
    session: requests.Session,
    rate_limiter: RateLimiter,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    allow_redirects: bool = False,
    timeout_seconds: int = 25,
    max_attempts: int = 4,
    base_backoff_seconds: float = 1.0,
    jitter_ratio: float = 0.2,
) -> requests.Response:
    safe_attempts = max(1, int(max_attempts))
    last_exc: Optional[Exception] = None
    for attempt in range(1, safe_attempts + 1):
        rate_limiter.wait()
        try:
            response = session.get(
                url,
                params=params,
                timeout=timeout_seconds,
                allow_redirects=allow_redirects,
            )
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= safe_attempts:
                raise RuntimeError(f"request_failed url={url} error={exc}") from exc
            _sleep_backoff(
                attempt=attempt,
                base_backoff_seconds=base_backoff_seconds,
                jitter_ratio=jitter_ratio,
            )
            continue

        if response.status_code == 200:
            return response

        if response.status_code in OVERLOAD_HTTP_STATUS_CODES and attempt < safe_attempts:
            retry_after = response.headers.get("Retry-After", "").strip()
            if retry_after:
                try:
                    retry_wait = float(retry_after)
                except ValueError:
                    retry_wait = 0.0
                if retry_wait > 0:
                    time.sleep(retry_wait)
                    continue
            _sleep_backoff(
                attempt=attempt,
                base_backoff_seconds=base_backoff_seconds,
                jitter_ratio=jitter_ratio,
            )
            continue

        raise RuntimeError(f"http_status={response.status_code} url={url} body={response.text[:250]}")

    if last_exc is not None:
        raise RuntimeError(f"request_failed url={url} error={last_exc}") from last_exc
    raise RuntimeError(f"request_failed url={url} reason=exhausted")


def _extract_build_id_from_html(html: str) -> str:
    match = NEXT_DATA_SCRIPT_PATTERN.search(html)
    if match:
        blob = match.group("data")
        try:
            payload = json.loads(blob)
            build_id = str(payload.get("buildId") or "").strip()
            if build_id:
                return build_id
        except Exception:
            pass

    fallback = BUILD_ID_FALLBACK_PATTERN.search(html)
    if fallback:
        build_id = str(fallback.group("build_id") or "").strip()
        if build_id:
            return build_id

    fallback_json = BUILD_ID_JSON_PATTERN.search(html)
    if fallback_json:
        build_id = str(fallback_json.group("build_id") or "").strip()
        if build_id:
            return build_id

    fallback_manifest = BUILD_ID_MANIFEST_PATTERN.search(html)
    if fallback_manifest:
        build_id = str(fallback_manifest.group("build_id") or "").strip()
        if build_id:
            return build_id

    raise RuntimeError("could_not_extract_build_id")


def _fetch_build_id(
    *,
    session: requests.Session,
    rate_limiter: RateLimiter,
    slug: str,
    max_attempts: int,
    base_backoff_seconds: float,
    jitter_ratio: float,
) -> str:
    url = f"https://polymarket.com/en/event/{slug}"
    response = _http_get_with_retry(
        session=session,
        rate_limiter=rate_limiter,
        url=url,
        allow_redirects=True,
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
        jitter_ratio=jitter_ratio,
    )
    return _extract_build_id_from_html(response.text)


def _extract_candidate_query(
    *,
    page_payload: Dict[str, Any],
    expected_window_end: datetime,
) -> Tuple[Dict[str, Any], str, str, float, float]:
    queries = (
        page_payload.get("pageProps", {})
        .get("dehydratedState", {})
        .get("queries", [])
    )
    if not isinstance(queries, list) or not queries:
        raise RuntimeError("missing_dehydrated_queries")

    best: Optional[Tuple[int, Dict[str, Any], str, str, float, float]] = None
    expected_end_iso = _to_iso_utc(expected_window_end)

    for query in queries:
        if not isinstance(query, dict):
            continue
        state = query.get("state", {})
        if not isinstance(state, dict):
            continue
        data = state.get("data", {})
        if not isinstance(data, dict):
            continue
        if "openPrice" not in data or "closePrice" not in data:
            continue
        open_raw = data.get("openPrice")
        close_raw = data.get("closePrice")
        try:
            open_price = float(open_raw)
            close_price = float(close_raw)
        except Exception:
            continue

        query_key = query.get("queryKey", [])
        if not isinstance(query_key, list):
            query_key = []
        window_start = str(query_key[3]).strip() if len(query_key) > 3 else ""
        window_end = str(query_key[5]).strip() if len(query_key) > 5 else ""
        score = 0
        if any(str(part).lower() == "crypto-prices" for part in query_key):
            score += 1
        if any(str(part).upper() == "BTC" for part in query_key):
            score += 1
        if any(str(part).lower() == "fifteen" for part in query_key):
            score += 1
        if window_end:
            try:
                parsed_end = _parse_iso_utc(window_end)
                if _to_iso_utc(parsed_end) == expected_end_iso:
                    score += 3
            except Exception:
                pass

        candidate = (score, query, window_start, window_end, open_price, close_price)
        if best is None or candidate[0] > best[0]:
            best = candidate

    if best is None:
        raise RuntimeError("missing_open_close_query")
    _, selected_query, window_start, window_end, open_price, close_price = best
    if not window_start:
        window_start = _to_iso_utc(expected_window_end - timedelta(minutes=15))
    if not window_end:
        window_end = _to_iso_utc(expected_window_end)
    return selected_query, window_start, window_end, open_price, close_price


def _load_slug_status_from_jsonl(path: Path) -> Dict[str, str]:
    status_by_slug: Dict[str, str] = {}
    if not path.exists():
        return status_by_slug
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        slug = str(payload.get("slug") or "").strip()
        if not slug:
            continue
        status = str(payload.get("status") or "").strip() or "unknown"
        status_by_slug[slug] = status
    return status_by_slug


def _iter_entries_for_day(resolved_log_path: Path, day_utc: str) -> List[Dict[str, Any]]:
    if not resolved_log_path.exists():
        raise FileNotFoundError(f"missing resolved log: {resolved_log_path}")
    entries: List[Dict[str, Any]] = []
    seen_slugs: set[str] = set()
    for raw in resolved_log_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        match = BASE_LINE_PATTERN.match(line)
        if not match:
            continue
        window_end = _parse_iso_utc(match.group("window_end"))
        if window_end.strftime("%Y-%m-%d") != day_utc:
            continue
        slug = match.group("slug").strip()
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        entries.append(
            {
                "slug": slug,
                "window_end_utc": _to_iso_utc(window_end),
                "registered_outcome": match.group("pm_norm").strip(),
            }
        )
    return entries


def _write_jsonl_line(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")


def run(
    *,
    resolved_log_path: Path,
    output_jsonl_path: Path,
    day_utc: str,
    max_slugs: Optional[int],
    requests_per_second: float,
    max_attempts: int,
    base_backoff_seconds: float,
    jitter_ratio: float,
    progress_every: int,
    retry_errors: bool,
) -> Dict[str, Any]:
    entries = _iter_entries_for_day(resolved_log_path, day_utc)
    if not entries:
        raise RuntimeError(f"no entries found for UTC day {day_utc}")

    previous_status = _load_slug_status_from_jsonl(output_jsonl_path)
    selected: List[Dict[str, Any]] = []
    for item in entries:
        previous = previous_status.get(item["slug"])
        if previous == "success":
            continue
        if previous and previous != "success" and not retry_errors:
            continue
        selected.append(item)

    if max_slugs is not None and max_slugs > 0:
        selected = selected[: max_slugs]

    if not selected:
        return {
            "day_utc": day_utc,
            "entries_total_for_day": len(entries),
            "selected": 0,
            "written_success": 0,
            "written_error": 0,
            "build_id_refreshes": 0,
            "requests_sent": 0,
        }

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
        }
    )
    limiter = RateLimiter(requests_per_second=requests_per_second)

    build_id = _fetch_build_id(
        session=session,
        rate_limiter=limiter,
        slug=selected[0]["slug"],
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
        jitter_ratio=jitter_ratio,
    )
    build_refreshes = 1
    requests_sent = 1

    written_success = 0
    written_error = 0
    progress_every_safe = max(1, int(progress_every))
    started = time.time()

    try:
        for idx, entry in enumerate(selected, start=1):
            slug = str(entry["slug"])
            expected_end = _parse_iso_utc(str(entry["window_end_utc"]))
            url = f"https://polymarket.com/_next/data/{build_id}/en/event/{slug}.json"
            fetch_error: Optional[str] = None
            payload_json: Optional[Dict[str, Any]] = None

            for pass_idx in (1, 2):
                try:
                    response = _http_get_with_retry(
                        session=session,
                        rate_limiter=limiter,
                        url=url,
                        params={"slug": slug},
                        allow_redirects=False,
                        max_attempts=max_attempts,
                        base_backoff_seconds=base_backoff_seconds,
                        jitter_ratio=jitter_ratio,
                    )
                    requests_sent += 1
                    payload_json = response.json()
                    fetch_error = None
                    break
                except Exception as exc:
                    fetch_error = str(exc)
                    if "http_status=404" in fetch_error and pass_idx == 1:
                        try:
                            build_id = _fetch_build_id(
                                session=session,
                                rate_limiter=limiter,
                                slug=slug,
                                max_attempts=max_attempts,
                                base_backoff_seconds=base_backoff_seconds,
                                jitter_ratio=jitter_ratio,
                            )
                            build_refreshes += 1
                            requests_sent += 1
                            url = f"https://polymarket.com/_next/data/{build_id}/en/event/{slug}.json"
                            continue
                        except Exception as refresh_exc:
                            fetch_error = f"{fetch_error}; build_refresh_failed={refresh_exc}"
                    break

            record_base: Dict[str, Any] = {
                "slug": slug,
                "requested_window_end_utc": _to_iso_utc(expected_end),
                "registered_outcome": entry.get("registered_outcome"),
                "source": "polymarket_next_data",
                "source_build_id": build_id,
                "fetched_at_utc": _to_iso_utc(datetime.now(timezone.utc)),
            }

            if payload_json is None:
                record = dict(record_base)
                record["status"] = "error"
                record["error"] = fetch_error or "unknown_fetch_error"
                _write_jsonl_line(output_jsonl_path, record)
                written_error += 1
            else:
                try:
                    _, window_start, window_end, open_price, close_price = _extract_candidate_query(
                        page_payload=payload_json,
                        expected_window_end=expected_end,
                    )
                    computed = "YES" if close_price >= open_price else "NO"
                    record = dict(record_base)
                    record.update(
                        {
                            "status": "success",
                            "window_start_utc": window_start,
                            "window_end_utc": window_end,
                            "open_price": open_price,
                            "close_price": close_price,
                            "computed_outcome": computed,
                        }
                    )
                    _write_jsonl_line(output_jsonl_path, record)
                    written_success += 1
                except Exception as exc:
                    record = dict(record_base)
                    record["status"] = "error"
                    record["error"] = f"parse_error={exc}"
                    _write_jsonl_line(output_jsonl_path, record)
                    written_error += 1

            if idx == 1 or (idx % progress_every_safe == 0) or idx == len(selected):
                elapsed = max(0.001, time.time() - started)
                rate_per_min = (idx / elapsed) * 60.0
                remaining = len(selected) - idx
                eta_seconds = (remaining / max(1, idx)) * elapsed
                print(
                    f"[{idx}/{len(selected)}] success={written_success} error={written_error} "
                    f"rate={rate_per_min:.2f}/min eta={int(max(0.0, eta_seconds))}s"
                )
    finally:
        session.close()

    return {
        "day_utc": day_utc,
        "entries_total_for_day": len(entries),
        "selected": len(selected),
        "written_success": written_success,
        "written_error": written_error,
        "build_id_refreshes": build_refreshes,
        "requests_sent": requests_sent,
        "output_jsonl": str(output_jsonl_path),
    }


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch Polymarket Next.js BTC 15m open/close for one UTC day.")
    p.add_argument("--resolved-log", default="data/diagnostic/resolved_15m_pairs.log")
    p.add_argument(
        "--out-jsonl",
        default="data/diagnostic/polymarket_nextjs_window_price_validation.jsonl",
    )
    p.add_argument("--day-utc", required=True, help="UTC day, format YYYY-MM-DD")
    p.add_argument("--max-slugs", type=int, default=None, help="Optional cap for test batches.")
    p.add_argument("--requests-per-second", type=float, default=2.0)
    p.add_argument("--max-attempts", type=int, default=4)
    p.add_argument("--base-backoff-seconds", type=float, default=1.0)
    p.add_argument("--jitter-ratio", type=float, default=0.2)
    p.add_argument("--progress-every", type=int, default=10)
    p.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry slugs that already exist in output JSONL with non-success status.",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        resolved_log_path=Path(args.resolved_log),
        output_jsonl_path=Path(args.out_jsonl),
        day_utc=str(args.day_utc).strip(),
        max_slugs=args.max_slugs,
        requests_per_second=args.requests_per_second,
        max_attempts=args.max_attempts,
        base_backoff_seconds=args.base_backoff_seconds,
        jitter_ratio=args.jitter_ratio,
        progress_every=args.progress_every,
        retry_errors=bool(args.retry_errors),
    )
    print("Polymarket Next.js window price fetch complete")
    print(f"Day UTC: {summary['day_utc']}")
    print(f"Entries found: {summary['entries_total_for_day']}")
    print(f"Selected: {summary['selected']}")
    print(f"Wrote success: {summary['written_success']}")
    print(f"Wrote error: {summary['written_error']}")
    print(f"Build ID refreshes: {summary['build_id_refreshes']}")
    print(f"Requests sent: {summary['requests_sent']}")
    print(f"Output JSONL: {summary['output_jsonl']}")


if __name__ == "__main__":
    main()
