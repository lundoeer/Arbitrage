#!/usr/bin/env python3
"""
List active YES/NO markets from Polymarket and Kalshi for manual setup selection.

Outputs two JSONL files (one per venue) with minimal fields.
Supports topic filtering, including a strict League of Legends mode.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport
from scripts.common.utils import as_dict, as_float

logging.getLogger("dotenv.main").setLevel(logging.ERROR)

POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
KALSHI_DEFAULT_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

TOPIC_ALL = "all"
TOPIC_LEAGUE_OF_LEGENDS = "league-of-legends"


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
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
    return dt.astimezone(timezone.utc)


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _normalize_yes_no_label(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if text == "yes":
        return "yes"
    if text == "no":
        return "no"
    return None


def _polymarket_yes_no_detected(market: Dict[str, Any]) -> bool:
    outcomes = _parse_json_list(market.get("outcomes"))
    if len(outcomes) != 2:
        return False
    # Treat any clean two-outcome market as binary even when labels are team
    # names (for example esports match winner markets).
    labels = [str(item or "").strip() for item in outcomes]
    if not all(labels):
        return False
    if labels[0].lower() == labels[1].lower():
        return False
    token_ids = _parse_json_list(market.get("clobTokenIds"))
    return len(token_ids) == 2


def _kalshi_yes_no_detected(market: Dict[str, Any]) -> bool:
    market_type = str(market.get("market_type") or "").strip().lower()
    if market_type == "binary":
        return True
    has_yes_no_book_fields = (
        (market.get("yes_bid") is not None or market.get("yes_ask") is not None)
        and (market.get("no_bid") is not None or market.get("no_ask") is not None)
    )
    if has_yes_no_book_fields:
        return True
    has_yes_no_subtitles = bool(str(market.get("yes_sub_title") or "").strip()) and bool(
        str(market.get("no_sub_title") or "").strip()
    )
    return has_yes_no_subtitles


def _first_float(values: List[Any], default: float = 0.0) -> float:
    for value in values:
        parsed = as_float(value)
        if parsed is not None:
            return float(parsed)
    return float(default)


def _seconds_to_end(*, end_dt: datetime, now_utc: datetime) -> int:
    return int((end_dt - now_utc).total_seconds())


def _read_run_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")
    return as_dict(json.loads(config_path.read_text(encoding="utf-8")))


def _resolve_kalshi_base_url(config: Dict[str, Any]) -> str:
    api_cfg = as_dict(config.get("api"))
    return str(api_cfg.get("base_url") or KALSHI_DEFAULT_API_BASE).rstrip("/")


def _resolve_kalshi_api_key(config: Dict[str, Any]) -> str:
    env_candidates = ["KALSHI_READONLY_API_KEY", "KALSHI_API_KEY", "kalshiapi"]
    for env_key in env_candidates:
        value = str(os.getenv(env_key, "") or "").strip()
        if value:
            return value
    api_cfg = as_dict(config.get("api"))
    return str(api_cfg.get("readonly_api_key") or "").strip()


def _polymarket_market_row(*, market: Dict[str, Any], now_utc: datetime, window_seconds: int, active_only: bool) -> Optional[Dict[str, Any]]:
    is_active = bool(market.get("active", False))
    if active_only and not is_active:
        return None

    end_dt = _parse_iso_datetime(market.get("endDate") or market.get("endDateIso"))
    if end_dt is None:
        return None

    seconds_to_end = _seconds_to_end(end_dt=end_dt, now_utc=now_utc)
    if seconds_to_end <= 0 or seconds_to_end > window_seconds:
        return None

    yes_no_detected = _polymarket_yes_no_detected(market)
    if not yes_no_detected:
        return None

    market_id = str(market.get("id") or "").strip()
    if not market_id:
        return None

    condition_id = str(market.get("conditionId") or "").strip()
    setup_fragment: Dict[str, Any] = {"polymarket": {"market_id": market_id}}
    if condition_id:
        setup_fragment["polymarket"]["condition_id"] = condition_id

    event_slug = ""
    events = market.get("events")
    if isinstance(events, list) and events:
        first_event = as_dict(events[0])
        event_slug = str(first_event.get("slug") or "").strip()

    dollar_volume = _first_float(
        [
            market.get("dollar_volume"),
            market.get("volumeNum"),
            market.get("volume"),
        ],
        default=0.0,
    )

    return {
        "venue": "polymarket",
        "event_slug": event_slug,
        "end_time": end_dt.isoformat(),
        "title": str(market.get("question") or market.get("title") or "").strip(),
        "dollar_volume": float(dollar_volume),
        "yes_no_detected": True,
        "setup_fragment": setup_fragment,
    }


def _fetch_polymarket_rows_all(
    *,
    page_size: int,
    max_pages: int,
    now_utc: datetime,
    window_seconds: int,
    active_only: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    transport = ApiTransport(
        default_headers={
            "User-Agent": "Arbitrage-Market-Setup-List/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )

    rows: List[Dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    stats = {
        "pages": 0,
        "scanned": 0,
        "selected": 0,
        "skipped": 0,
    }

    offset = 0
    window_start_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    window_end_iso = (now_utc + timedelta(seconds=window_seconds)).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    for _ in range(max_pages):
        params = {
            "limit": max(1, int(page_size)),
            "offset": int(offset),
            "active": "true" if active_only else "false",
            "closed": "false",
            "archived": "false",
            "end_date_min": window_start_iso,
            "end_date_max": window_end_iso,
        }
        _, payload = transport.request_json("GET", f"{POLYMARKET_GAMMA_API}/markets", params=params, allow_status={200})
        batch = payload if isinstance(payload, list) else []
        stats["pages"] += 1
        if not batch:
            break

        for item in batch:
            market = as_dict(item)
            market_id = str(market.get("id") or "").strip()
            if not market_id or market_id in seen_market_ids:
                continue
            seen_market_ids.add(market_id)
            stats["scanned"] += 1

            row = _polymarket_market_row(
                market=market,
                now_utc=now_utc,
                window_seconds=window_seconds,
                active_only=active_only,
            )
            if row is None:
                stats["skipped"] += 1
                continue
            rows.append(row)
            stats["selected"] += 1

        offset += len(batch)
        if len(batch) < max(1, int(page_size)):
            break

    rows.sort(key=lambda row: (str(row.get("end_time") or ""), -float(row.get("dollar_volume") or 0.0)))
    return rows, stats


def _fetch_polymarket_rows_lol(
    *,
    page_size: int,
    max_pages: int,
    now_utc: datetime,
    window_seconds: int,
    active_only: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    transport = ApiTransport(
        default_headers={
            "User-Agent": "Arbitrage-Market-Setup-List/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )

    rows: List[Dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    stats = {
        "pages": 0,
        "events_scanned": 0,
        "markets_scanned": 0,
        "selected": 0,
        "skipped": 0,
    }

    offset = 0
    window_start_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    window_end_iso = (now_utc + timedelta(seconds=window_seconds)).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )

    for _ in range(max_pages):
        params = {
            "limit": max(1, int(page_size)),
            "offset": int(offset),
            "active": "true" if active_only else "false",
            "closed": "false",
            "tag_slug": TOPIC_LEAGUE_OF_LEGENDS,
            "end_date_min": window_start_iso,
            "end_date_max": window_end_iso,
        }
        _, payload = transport.request_json("GET", f"{POLYMARKET_GAMMA_API}/events", params=params, allow_status={200})
        events = payload if isinstance(payload, list) else []
        stats["pages"] += 1
        if not events:
            break

        for event_item in events:
            event = as_dict(event_item)
            stats["events_scanned"] += 1
            event_slug = str(event.get("slug") or "").strip()
            markets = event.get("markets") if isinstance(event.get("markets"), list) else []
            for market_item in markets:
                market = as_dict(market_item)
                market_id = str(market.get("id") or "").strip()
                if not market_id or market_id in seen_market_ids:
                    continue
                seen_market_ids.add(market_id)
                stats["markets_scanned"] += 1

                # Attach event slug when not present in nested market payload.
                if event_slug and not market.get("events"):
                    market["events"] = [{"slug": event_slug}]

                row = _polymarket_market_row(
                    market=market,
                    now_utc=now_utc,
                    window_seconds=window_seconds,
                    active_only=active_only,
                )
                if row is None:
                    stats["skipped"] += 1
                    continue
                rows.append(row)
                stats["selected"] += 1

        offset += len(events)
        if len(events) < max(1, int(page_size)):
            break

    rows.sort(key=lambda row: (str(row.get("end_time") or ""), -float(row.get("dollar_volume") or 0.0)))
    return rows, stats


def _is_lol_series(series: Dict[str, Any]) -> bool:
    title = str(series.get("title") or "").strip().lower()
    ticker = str(series.get("ticker") or "").strip().upper()
    if "league of legends" in title:
        return True
    if re.search(r"\blol\b", title):
        return True
    return ticker.startswith("KXLOL") or "LEAGUEOFLEGENDS" in ticker


def _fetch_kalshi_lol_series(
    *,
    transport: ApiTransport,
    base_url: str,
    page_size: int,
    max_pages: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    series_hits: List[Dict[str, Any]] = []
    stats = {
        "series_pages": 0,
        "series_scanned": 0,
        "series_selected": 0,
    }
    cursor: Optional[str] = None
    seen_series: set[str] = set()

    for _ in range(max_pages):
        params: Dict[str, Any] = {
            "limit": max(1, int(page_size)),
            "tags": "Esports",
        }
        if cursor:
            params["cursor"] = cursor

        _, payload = transport.request_json("GET", f"{base_url}/series", params=params, allow_status={200})
        body = as_dict(payload)
        series_list = body.get("series") if isinstance(body.get("series"), list) else []
        stats["series_pages"] += 1
        if not series_list:
            break

        for item in series_list:
            series = as_dict(item)
            ticker = str(series.get("ticker") or "").strip()
            if not ticker or ticker in seen_series:
                continue
            seen_series.add(ticker)
            stats["series_scanned"] += 1
            if not _is_lol_series(series):
                continue
            series_hits.append(series)
            stats["series_selected"] += 1

        cursor_raw = str(body.get("cursor") or "").strip()
        cursor = cursor_raw or None
        if cursor is None:
            break

    return series_hits, stats


def _kalshi_series_from_event_ticker(event_ticker: str) -> str:
    raw = str(event_ticker or "").strip()
    if not raw:
        return ""
    if "-" not in raw:
        return raw
    return raw.split("-", 1)[0]


def _parse_kalshi_event_ticker_date(event_ticker: str) -> Optional[datetime]:
    raw = str(event_ticker or "").strip().upper()
    if not raw:
        return None
    # Example: KXLOLGAME-26MAR02TESWB -> 26MAR02
    match = re.search(r"-([0-9]{2}[A-Z]{3}[0-9]{2})", raw)
    if not match:
        return None
    token = match.group(1)
    try:
        parsed_date = datetime.strptime(token, "%y%b%d")
    except ValueError:
        return None
    # Treat event-ticker date as end-of-day UTC for window inclusion.
    return datetime(
        year=parsed_date.year,
        month=parsed_date.month,
        day=parsed_date.day,
        hour=23,
        minute=59,
        second=59,
        tzinfo=timezone.utc,
    )


def _kalshi_market_row(
    *,
    market: Dict[str, Any],
    now_utc: datetime,
    window_seconds: int,
    active_only: bool,
    series_ticker_hint: str,
    use_event_ticker_date: bool,
) -> Optional[Dict[str, Any]]:
    ticker = str(market.get("ticker") or "").strip()
    if not ticker:
        return None

    status = str(market.get("status") or "").strip().lower()
    if active_only and status != "active":
        return None

    event_ticker = str(market.get("event_ticker") or "").strip()
    end_dt = _parse_iso_datetime(market.get("close_time") or market.get("expiration_time"))
    if use_event_ticker_date:
        event_date_end = _parse_kalshi_event_ticker_date(event_ticker)
        if event_date_end is not None:
            end_dt = event_date_end
    if end_dt is None:
        return None

    seconds_to_end = _seconds_to_end(end_dt=end_dt, now_utc=now_utc)
    if seconds_to_end <= 0 or seconds_to_end > window_seconds:
        return None

    yes_no_detected = _kalshi_yes_no_detected(market)
    if not yes_no_detected:
        return None

    series_ticker = series_ticker_hint or _kalshi_series_from_event_ticker(event_ticker)

    dollar_volume = _first_float(
        [
            market.get("dollar_volume"),
            market.get("dollarVolume"),
        ],
        default=0.0,
    )

    return {
        "venue": "kalshi",
        "series_ticker": series_ticker,
        "event_ticker": event_ticker,
        "end_time": end_dt.isoformat(),
        "title": str(market.get("title") or "").strip(),
        "dollar_volume": float(dollar_volume),
        "yes_no_detected": True,
        "setup_fragment": {
            "kalshi": {
                "ticker": ticker,
            }
        },
    }


def _fetch_kalshi_rows_all(
    *,
    base_url: str,
    api_key: str,
    page_size: int,
    max_pages: int,
    now_utc: datetime,
    window_seconds: int,
    active_only: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    headers = {
        "User-Agent": "Arbitrage-Market-Setup-List/1.0",
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    transport = ApiTransport(default_headers=headers)

    rows: List[Dict[str, Any]] = []
    seen_tickers: set[str] = set()
    stats = {
        "pages": 0,
        "markets_scanned": 0,
        "selected": 0,
        "skipped": 0,
    }

    cursor: Optional[str] = None
    min_close_ts = int(now_utc.timestamp())
    max_close_ts = int((now_utc + timedelta(seconds=window_seconds)).timestamp())

    for _ in range(max_pages):
        params: Dict[str, Any] = {
            "limit": max(1, int(page_size)),
            "min_close_ts": int(min_close_ts),
            "max_close_ts": int(max_close_ts),
        }
        if cursor:
            params["cursor"] = cursor

        _, payload = transport.request_json("GET", f"{base_url}/markets", params=params, allow_status={200})
        body = as_dict(payload)
        markets = body.get("markets") if isinstance(body.get("markets"), list) else []
        stats["pages"] += 1
        if not markets:
            break

        for item in markets:
            market = as_dict(item)
            ticker = str(market.get("ticker") or "").strip()
            if not ticker or ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)
            stats["markets_scanned"] += 1

            row = _kalshi_market_row(
                market=market,
                now_utc=now_utc,
                window_seconds=window_seconds,
                active_only=active_only,
                series_ticker_hint="",
                use_event_ticker_date=False,
            )
            if row is None:
                stats["skipped"] += 1
                continue
            rows.append(row)
            stats["selected"] += 1

        cursor_raw = str(body.get("cursor") or "").strip()
        cursor = cursor_raw or None
        if cursor is None:
            break

    rows.sort(key=lambda row: (str(row.get("end_time") or ""), -float(row.get("dollar_volume") or 0.0)))
    return rows, stats


def _fetch_kalshi_rows_lol(
    *,
    base_url: str,
    api_key: str,
    page_size: int,
    max_pages: int,
    now_utc: datetime,
    window_seconds: int,
    active_only: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    headers = {
        "User-Agent": "Arbitrage-Market-Setup-List/1.0",
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    transport = ApiTransport(default_headers=headers)

    series_hits, series_stats = _fetch_kalshi_lol_series(
        transport=transport,
        base_url=base_url,
        page_size=page_size,
        max_pages=max_pages,
    )

    rows: List[Dict[str, Any]] = []
    seen_tickers: set[str] = set()
    stats = {
        "series_pages": int(series_stats["series_pages"]),
        "series_scanned": int(series_stats["series_scanned"]),
        "series_selected": int(series_stats["series_selected"]),
        "market_pages": 0,
        "markets_scanned": 0,
        "selected": 0,
        "skipped": 0,
    }

    for series in series_hits:
        series_ticker = str(series.get("ticker") or "").strip()
        if not series_ticker:
            continue

        cursor: Optional[str] = None
        for _ in range(max_pages):
            params: Dict[str, Any] = {
                "series_ticker": series_ticker,
                "status": "open",
                "limit": max(1, int(page_size)),
            }
            if cursor:
                params["cursor"] = cursor

            _, payload = transport.request_json("GET", f"{base_url}/markets", params=params, allow_status={200})
            body = as_dict(payload)
            markets = body.get("markets") if isinstance(body.get("markets"), list) else []
            stats["market_pages"] += 1
            if not markets:
                break

            for item in markets:
                market = as_dict(item)
                ticker = str(market.get("ticker") or "").strip()
                if not ticker or ticker in seen_tickers:
                    continue
                seen_tickers.add(ticker)
                stats["markets_scanned"] += 1

                row = _kalshi_market_row(
                    market=market,
                    now_utc=now_utc,
                    window_seconds=window_seconds,
                    active_only=active_only,
                    series_ticker_hint=series_ticker,
                    use_event_ticker_date=True,
                )
                if row is None:
                    stats["skipped"] += 1
                    continue
                rows.append(row)
                stats["selected"] += 1

            cursor_raw = str(body.get("cursor") or "").strip()
            cursor = cursor_raw or None
            if cursor is None:
                break

    rows.sort(key=lambda row: (str(row.get("end_time") or ""), -float(row.get("dollar_volume") or 0.0)))
    return rows, stats


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")
    return int(len(rows))


def run(
    *,
    config_path: Path,
    output_polymarket: Path,
    output_kalshi: Path,
    window_hours: float,
    active_only: bool,
    max_pages: int,
    page_size: int,
    topic: str,
) -> Dict[str, Any]:
    load_dotenv(dotenv_path=".env", override=False)
    config = _read_run_config(config_path)
    kalshi_base_url = _resolve_kalshi_base_url(config)
    kalshi_api_key = _resolve_kalshi_api_key(config)

    now_utc = datetime.now(timezone.utc)
    window_seconds = int(max(1.0, float(window_hours)) * 3600)

    if topic == TOPIC_LEAGUE_OF_LEGENDS:
        polymarket_rows, polymarket_stats = _fetch_polymarket_rows_lol(
            page_size=page_size,
            max_pages=max_pages,
            now_utc=now_utc,
            window_seconds=window_seconds,
            active_only=active_only,
        )
        kalshi_rows, kalshi_stats = _fetch_kalshi_rows_lol(
            base_url=kalshi_base_url,
            api_key=kalshi_api_key,
            page_size=page_size,
            max_pages=max_pages,
            now_utc=now_utc,
            window_seconds=window_seconds,
            active_only=active_only,
        )
    else:
        polymarket_rows, polymarket_stats = _fetch_polymarket_rows_all(
            page_size=page_size,
            max_pages=max_pages,
            now_utc=now_utc,
            window_seconds=window_seconds,
            active_only=active_only,
        )
        kalshi_rows, kalshi_stats = _fetch_kalshi_rows_all(
            base_url=kalshi_base_url,
            api_key=kalshi_api_key,
            page_size=page_size,
            max_pages=max_pages,
            now_utc=now_utc,
            window_seconds=window_seconds,
            active_only=active_only,
        )

    polymarket_written = _write_jsonl(output_polymarket, polymarket_rows)
    kalshi_written = _write_jsonl(output_kalshi, kalshi_rows)

    return {
        "generated_at": now_utc.isoformat(),
        "window_hours": float(window_hours),
        "active_only": bool(active_only),
        "topic": str(topic),
        "outputs": {
            "polymarket": str(output_polymarket),
            "kalshi": str(output_kalshi),
        },
        "polymarket": {
            "rows_written": int(polymarket_written),
            **polymarket_stats,
        },
        "kalshi": {
            "rows_written": int(kalshi_written),
            **kalshi_stats,
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List active YES/NO markets for manual cross-venue setup.")
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument(
        "--output-polymarket",
        default="data/diagnostic/market_setup_candidates_polymarket.jsonl",
        help="Output JSONL path for Polymarket rows.",
    )
    parser.add_argument(
        "--output-kalshi",
        default="data/diagnostic/market_setup_candidates_kalshi.jsonl",
        help="Output JSONL path for Kalshi rows.",
    )
    parser.add_argument("--window-hours", type=float, default=24.0)
    parser.add_argument(
        "--active-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include only active markets (default: true).",
    )
    parser.add_argument(
        "--topic",
        choices=[TOPIC_ALL, TOPIC_LEAGUE_OF_LEGENDS],
        default=TOPIC_ALL,
        help="Optional topic narrowing. Use league-of-legends for strict LoL filtering.",
    )
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--page-size", type=int, default=200)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        config_path=Path(args.config),
        output_polymarket=Path(args.output_polymarket),
        output_kalshi=Path(args.output_kalshi),
        window_hours=float(args.window_hours),
        active_only=bool(args.active_only),
        max_pages=max(1, int(args.max_pages)),
        page_size=max(1, int(args.page_size)),
        topic=str(args.topic),
    )

    print("Market setup listing complete")
    print(f"Generated at: {summary['generated_at']}")
    print(f"Topic: {summary['topic']}")
    print(
        "Polymarket: "
        f"rows_written={summary['polymarket']['rows_written']}"
    )
    print(
        "Kalshi: "
        f"rows_written={summary['kalshi']['rows_written']}"
    )
    print(f"Output Polymarket: {summary['outputs']['polymarket']}")
    print(f"Output Kalshi:     {summary['outputs']['kalshi']}")


if __name__ == "__main__":
    main()
