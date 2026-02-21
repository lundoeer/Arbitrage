#!/usr/bin/env python3
"""
Endpoint research for one resolved pair that differed between Kalshi and Polymarket.

This script:
1. Selects one differing pair from resolution_comparison_summary.json (or from args).
2. Calls multiple candidate endpoints per venue for ended markets.
3. Writes raw response per API call into separate files under data/diagnostic/market_research.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
KALSHI_DEFAULT_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_SERIES = "KXBTC15M"

logging.getLogger("dotenv.main").setLevel(logging.ERROR)


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def _load_runtime_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_kalshi_api_key(config: Dict[str, Any]) -> str:
    env_candidates = ["KALSHI_READONLY_API_KEY", "KALSHI_API_KEY", "kalshiapi"]
    for env_key in env_candidates:
        value = os.getenv(env_key, "").strip()
        if value:
            return value
    return str(config.get("api", {}).get("readonly_api_key") or "").strip()


def _load_different_pair_from_summary(summary_path: Path) -> Dict[str, Any]:
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing summary file: {summary_path}")

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    diffs = summary.get("different_examples", [])
    if not isinstance(diffs, list) or not diffs:
        raise RuntimeError("No different_examples found in summary.")
    pair = diffs[0]
    required = ["window_end", "kalshi_ticker", "polymarket_slug"]
    for key in required:
        if not pair.get(key):
            raise RuntimeError(f"different_examples[0] missing required key: {key}")
    return pair


def _request_and_store(
    *,
    session: requests.Session,
    out_dir: Path,
    call_index: int,
    call_name: str,
    method: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 20,
) -> Dict[str, Any]:
    params = params or {}
    query = urlencode(params, doseq=True) if params else ""
    request_url = f"{url}?{query}" if query else url
    ts = datetime.now(timezone.utc).isoformat()

    try:
        response = session.request(method, url, params=params, timeout=timeout_seconds)
        status = response.status_code
        content_type = response.headers.get("Content-Type", "")
        text_body = response.text
    except Exception as exc:
        status = None
        content_type = ""
        text_body = f"REQUEST_ERROR: {exc}"

    out_dir.mkdir(parents=True, exist_ok=True)
    file_stem = f"{call_index:02d}_{_safe_name(call_name)}"
    metadata: Dict[str, Any] = {
        "call_index": call_index,
        "call_name": call_name,
        "method": method,
        "request_url": request_url,
        "request_params": params,
        "status_code": status,
        "content_type": content_type,
        "requested_at": ts,
    }

    parsed_json: Optional[Any] = None
    if isinstance(text_body, str) and status is not None:
        try:
            parsed_json = response.json()  # type: ignore[name-defined]
        except Exception:
            parsed_json = None

    if parsed_json is not None:
        body_path = out_dir / f"{file_stem}__status{status}.json"
        with open(body_path, "w", encoding="utf-8") as f:
            json.dump(parsed_json, f, indent=2)
    else:
        body_path = out_dir / f"{file_stem}__status{status if status is not None else 'ERR'}.txt"
        body_path.write_text(text_body, encoding="utf-8", errors="replace")

    metadata["response_file"] = str(body_path)
    return metadata


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def run(
    *,
    config_path: Path,
    summary_path: Path,
    out_root: Path,
    kalshi_ticker: Optional[str],
    polymarket_slug: Optional[str],
    window_end: Optional[str],
) -> Dict[str, Any]:
    load_dotenv(dotenv_path=".env", override=False)
    config = _load_runtime_config(config_path)
    kalshi_base_url = str(config.get("api", {}).get("base_url") or KALSHI_DEFAULT_API_BASE).rstrip("/")
    kalshi_api_key = _resolve_kalshi_api_key(config)
    if not kalshi_api_key:
        raise RuntimeError("Missing Kalshi API key in .env or config/run_config.json")

    if kalshi_ticker and polymarket_slug and window_end:
        selected = {
            "window_end": window_end,
            "kalshi_ticker": kalshi_ticker,
            "polymarket_slug": polymarket_slug,
        }
    else:
        selected = _load_different_pair_from_summary(summary_path)

    selected_window_end = str(selected["window_end"])
    selected_kalshi_ticker = str(selected["kalshi_ticker"])
    selected_polymarket_slug = str(selected["polymarket_slug"])

    pair_key = "__".join(
        [
            _safe_name(selected_window_end),
            _safe_name(selected_kalshi_ticker),
            _safe_name(selected_polymarket_slug),
        ]
    )
    pair_dir = out_root / pair_key
    kalshi_dir = pair_dir / f"kalshi_{_safe_name(selected_kalshi_ticker)}"
    polymarket_dir = pair_dir / f"polymarket_{_safe_name(selected_polymarket_slug)}"

    kalshi_session = requests.Session()
    kalshi_session.headers.update(
        {
            "Authorization": f"Bearer {kalshi_api_key}",
            "Content-Type": "application/json",
            "User-Agent": "Arbitrage-Market-Research/1.0",
        }
    )
    polymarket_session = requests.Session()
    polymarket_session.headers.update(
        {
            "User-Agent": "Arbitrage-Market-Research/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )

    kalshi_calls: List[Dict[str, Any]] = []
    polymarket_calls: List[Dict[str, Any]] = []

    # Kalshi endpoint search for ended/specific market information.
    kalshi_candidates: List[Tuple[str, str, str, Dict[str, Any]]] = [
        ("market_detail", "GET", f"{kalshi_base_url}/markets/{selected_kalshi_ticker}", {}),
        ("market_orderbook", "GET", f"{kalshi_base_url}/markets/{selected_kalshi_ticker}/orderbook", {}),
        ("market_history_candidate", "GET", f"{kalshi_base_url}/markets/{selected_kalshi_ticker}/history", {}),
        ("markets_by_series", "GET", f"{kalshi_base_url}/markets", {"series_ticker": KALSHI_SERIES, "limit": 50}),
        (
            "markets_by_series_finalized",
            "GET",
            f"{kalshi_base_url}/markets",
            {"series_ticker": KALSHI_SERIES, "status": "finalized", "limit": 50},
        ),
        ("market_by_ticker_query", "GET", f"{kalshi_base_url}/markets", {"ticker": selected_kalshi_ticker, "limit": 10}),
    ]

    kalshi_market_payload: Optional[Dict[str, Any]] = None
    for idx, (name, method, url, params) in enumerate(kalshi_candidates, start=1):
        meta = _request_and_store(
            session=kalshi_session,
            out_dir=kalshi_dir,
            call_index=idx,
            call_name=name,
            method=method,
            url=url,
            params=params,
        )
        kalshi_calls.append(meta)

        if name == "market_detail" and meta["status_code"] == 200:
            try:
                with open(meta["response_file"], "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if isinstance(payload, dict):
                    if isinstance(payload.get("market"), dict):
                        kalshi_market_payload = payload["market"]
                    elif payload.get("ticker"):
                        kalshi_market_payload = payload
            except Exception:
                pass

    if kalshi_market_payload:
        event_ticker = str(kalshi_market_payload.get("event_ticker") or "").strip()
        if event_ticker:
            extra = [
                ("event_detail_candidate", "GET", f"{kalshi_base_url}/events/{event_ticker}", {}),
                ("markets_by_event_query", "GET", f"{kalshi_base_url}/markets", {"event_ticker": event_ticker, "limit": 20}),
            ]
            start_idx = len(kalshi_calls) + 1
            for off, (name, method, url, params) in enumerate(extra):
                kalshi_calls.append(
                    _request_and_store(
                        session=kalshi_session,
                        out_dir=kalshi_dir,
                        call_index=start_idx + off,
                        call_name=name,
                        method=method,
                        url=url,
                        params=params,
                    )
                )

    # Polymarket endpoint search for ended/specific market information.
    polymarket_candidates: List[Tuple[str, str, str, Dict[str, Any]]] = [
        ("event_by_slug", "GET", f"{POLYMARKET_GAMMA_API}/events", {"slug": selected_polymarket_slug}),
        ("market_by_slug", "GET", f"{POLYMARKET_GAMMA_API}/markets", {"slug": selected_polymarket_slug}),
        ("events_closed_bitcoin_sample", "GET", f"{POLYMARKET_GAMMA_API}/events", {"closed": "true", "tag_slug": "bitcoin", "limit": 20, "offset": 0}),
        ("markets_closed_sample", "GET", f"{POLYMARKET_GAMMA_API}/markets", {"closed": "true", "limit": 20, "offset": 0}),
    ]

    polymarket_event_payload: Optional[Dict[str, Any]] = None
    polymarket_market_payload: Optional[Dict[str, Any]] = None

    for idx, (name, method, url, params) in enumerate(polymarket_candidates, start=1):
        meta = _request_and_store(
            session=polymarket_session,
            out_dir=polymarket_dir,
            call_index=idx,
            call_name=name,
            method=method,
            url=url,
            params=params,
        )
        polymarket_calls.append(meta)

        if name == "event_by_slug" and meta["status_code"] == 200:
            try:
                with open(meta["response_file"], "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                    polymarket_event_payload = payload[0]
            except Exception:
                pass
        if name == "market_by_slug" and meta["status_code"] == 200:
            try:
                with open(meta["response_file"], "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                    polymarket_market_payload = payload[0]
            except Exception:
                pass

    if polymarket_event_payload:
        event_id = str(polymarket_event_payload.get("id") or "").strip()
        if event_id:
            polymarket_calls.append(
                _request_and_store(
                    session=polymarket_session,
                    out_dir=polymarket_dir,
                    call_index=len(polymarket_calls) + 1,
                    call_name="event_by_id_query_candidate",
                    method="GET",
                    url=f"{POLYMARKET_GAMMA_API}/events",
                    params={"id": event_id},
                )
            )

    pm_market_id: Optional[str] = None
    if polymarket_market_payload:
        pm_market_id = str(polymarket_market_payload.get("id") or "").strip() or None
    elif polymarket_event_payload:
        markets = polymarket_event_payload.get("markets", [])
        if isinstance(markets, list):
            for m in markets:
                if str(m.get("slug", "")).strip() == selected_polymarket_slug:
                    pm_market_id = str(m.get("id") or "").strip() or None
                    break

    if pm_market_id:
        polymarket_calls.append(
            _request_and_store(
                session=polymarket_session,
                out_dir=polymarket_dir,
                call_index=len(polymarket_calls) + 1,
                call_name="market_by_id_query",
                method="GET",
                url=f"{POLYMARKET_GAMMA_API}/markets",
                params={"id": pm_market_id},
            )
        )

    kalshi_index = {
        "market": selected_kalshi_ticker,
        "calls": kalshi_calls,
        "successful_status_200": [c for c in kalshi_calls if c.get("status_code") == 200],
    }
    polymarket_index = {
        "market": selected_polymarket_slug,
        "calls": polymarket_calls,
        "successful_status_200": [c for c in polymarket_calls if c.get("status_code") == 200],
    }

    _write_json(kalshi_dir / "endpoint_index.json", kalshi_index)
    _write_json(polymarket_dir / "endpoint_index.json", polymarket_index)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selected_pair": {
            "window_end": selected_window_end,
            "kalshi_ticker": selected_kalshi_ticker,
            "polymarket_slug": selected_polymarket_slug,
        },
        "pair_research_dir": str(pair_dir),
        "kalshi_dir": str(kalshi_dir),
        "polymarket_dir": str(polymarket_dir),
        "notes": "Each API call is stored as its own raw file; endpoint_index.json summarizes statuses per platform.",
    }
    _write_json(pair_dir / "research_manifest.json", manifest)
    return manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect raw endpoint responses for one differing resolved 15m pair."
    )
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument(
        "--summary",
        default="data/diagnostic/resolution_comparison_summary.json",
        help="Summary file containing different_examples.",
    )
    parser.add_argument(
        "--out-root",
        default="data/diagnostic/market_research",
        help="Root directory for research artifacts.",
    )
    parser.add_argument("--kalshi-ticker", default=None)
    parser.add_argument("--polymarket-slug", default=None)
    parser.add_argument("--window-end", default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    manifest = run(
        config_path=Path(args.config),
        summary_path=Path(args.summary),
        out_root=Path(args.out_root),
        kalshi_ticker=args.kalshi_ticker,
        polymarket_slug=args.polymarket_slug,
        window_end=args.window_end,
    )
    print("Market endpoint research complete")
    print(f"Output directory: {manifest['pair_research_dir']}")
    print(f"Kalshi endpoint index: {manifest['kalshi_dir']}\\endpoint_index.json")
    print(f"Polymarket endpoint index: {manifest['polymarket_dir']}\\endpoint_index.json")


if __name__ == "__main__":
    main()
