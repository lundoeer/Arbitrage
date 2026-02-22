#!/usr/bin/env python3
"""
Discover active BTC 15m up/down markets across Polymarket and Kalshi.

Selection rule:
- Market must be active on venue.
- Market end/close time must be in the future.
- "Current window" means the contract closes within the next 15 minutes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport

logging.getLogger("dotenv.main").setLevel(logging.ERROR)

POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
KALSHI_DEFAULT_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
WINDOW_SECONDS = 900
POLYMARKET_15M_SLUG_PREFIX = "btc-updown-15m-"
KALSHI_15M_SERIES = "KXBTC15M"
KALSHI_15M_TICKER_PREFIX = "KXBTC15M-"


@dataclass
class VenueNormalization:
    venue: str
    venue_market_id: str
    underlying: str
    condition_type: str
    threshold: Optional[float]
    window_start: Optional[str]
    window_end: Optional[str]
    expiry: Optional[str]
    resolution_source: str
    timezone: str
    raw_title: str

    def signature(self) -> str:
        time_key = (self.window_end or self.expiry or "none")[:16]
        return f"{self.underlying}|{self.condition_type}|none|{self.timezone}|{time_key}"


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


def _next_window_end_utc(dt: datetime, bucket_seconds: int = WINDOW_SECONDS) -> datetime:
    epoch = int(dt.timestamp())
    remainder = epoch % bucket_seconds
    if remainder == 0:
        next_epoch = epoch + bucket_seconds
    else:
        next_epoch = epoch + (bucket_seconds - remainder)
    return datetime.fromtimestamp(next_epoch, tz=timezone.utc)


def _window_end_candidates(now_utc: datetime, back_buckets: int, forward_buckets: int) -> List[datetime]:
    base_end = _next_window_end_utc(now_utc)
    candidates: List[datetime] = []
    for offset in range(-back_buckets, forward_buckets + 1):
        candidates.append(base_end + timedelta(seconds=offset * WINDOW_SECONDS))
    return candidates


def _is_15m_bucket_aligned(dt: datetime, bucket_seconds: int = WINDOW_SECONDS) -> bool:
    return int(dt.timestamp()) % bucket_seconds == 0


def _polymarket_candidate_slugs(now_utc: datetime, back_buckets: int, forward_buckets: int) -> List[str]:
    slugs: List[str] = []
    seen: set[str] = set()
    for window_end in _window_end_candidates(now_utc, back_buckets, forward_buckets):
        # Polymarket slug timestamp encodes bucket start; derive it from stable window_end.
        window_start = window_end - timedelta(seconds=WINDOW_SECONDS)
        slug = f"btc-updown-15m-{int(window_start.timestamp())}"
        if slug in seen:
            continue
        seen.add(slug)
        slugs.append(slug)
    return slugs


def _kalshi_15m_ticker_from_close(close_et: datetime) -> str:
    minute_suffix = close_et.strftime("%M")
    return f"KXBTC15M-{close_et.strftime('%y%b%d%H%M').upper()}-{minute_suffix}"


def _kalshi_15m_candidate_tickers(now_utc: datetime, back_buckets: int, forward_buckets: int) -> List[str]:
    candidates: List[str] = []
    seen: set[str] = set()
    for window_end in _window_end_candidates(now_utc, back_buckets, forward_buckets):
        close_et = window_end.astimezone(ZoneInfo("America/New_York"))
        ticker = _kalshi_15m_ticker_from_close(close_et)
        if ticker in seen:
            continue
        seen.add(ticker)
        candidates.append(ticker)
    return candidates


def _parse_polymarket_token_ids(raw_ids: Any) -> List[str]:
    if isinstance(raw_ids, list):
        return [str(token_id) for token_id in raw_ids]
    if isinstance(raw_ids, str):
        try:
            parsed = json.loads(raw_ids)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [str(token_id) for token_id in parsed]
    return []


def _seconds_to_end(end_dt: datetime, now_utc: datetime) -> int:
    return int((end_dt - now_utc).total_seconds())


def _is_current_window(seconds_to_end: int) -> bool:
    return 0 < seconds_to_end <= WINDOW_SECONDS


def _build_polymarket_normalization(event: Dict[str, Any], market: Dict[str, Any]) -> VenueNormalization:
    return VenueNormalization(
        venue="polymarket",
        venue_market_id=str(market.get("id", "")),
        underlying="BTC",
        condition_type="directional_updown",
        threshold=None,
        window_start=market.get("startDate") or event.get("startDate"),
        window_end=market.get("endDate") or market.get("endDateIso") or event.get("endDate"),
        expiry=market.get("closedTime") or market.get("endDateIso") or event.get("endDate"),
        resolution_source=str(market.get("resolutionSource") or event.get("resolutionSource") or ""),
        timezone="ET",
        raw_title=str(market.get("question") or event.get("title") or ""),
    )


def _build_kalshi_normalization(market: Dict[str, Any]) -> VenueNormalization:
    return VenueNormalization(
        venue="kalshi",
        venue_market_id=str(market.get("ticker", "")),
        underlying="BTC",
        condition_type="directional_updown",
        threshold=None,
        window_start=market.get("open_time"),
        window_end=market.get("close_time"),
        expiry=market.get("expiration_time"),
        resolution_source="",
        timezone="ET",
        raw_title=str(market.get("title") or ""),
    )


class PolymarketDiscoveryClient:
    def __init__(self) -> None:
        self.transport = ApiTransport(
            default_headers={
                "User-Agent": "Arbitrage-Market-Discovery/1.0",
                "Accept": "application/json, text/plain, */*",
            }
        )

    def _fetch_event_by_slug(self, slug: str) -> Optional[Dict[str, Any]]:
        url = f"{POLYMARKET_GAMMA_API}/events?slug={slug}"
        _, payload = self.transport.request_json("GET", url)
        if not payload:
            return None
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected Polymarket payload for slug {slug}")
        return payload[0]

    def discover(self, now_utc: datetime, back_buckets: int, forward_buckets: int) -> List[Dict[str, Any]]:
        markets: List[Dict[str, Any]] = []
        seen_market_ids: set[str] = set()

        for slug in _polymarket_candidate_slugs(now_utc, back_buckets, forward_buckets):
            event = self._fetch_event_by_slug(slug)
            if not event:
                continue

            event_slug = str(event.get("slug", ""))
            if event_slug != slug:
                raise RuntimeError(
                    f"Polymarket returned unexpected event slug for query slug={slug}: got {event_slug}"
                )
            if not event_slug.startswith(POLYMARKET_15M_SLUG_PREFIX):
                raise RuntimeError(
                    f"Unexpected Polymarket event slug format: {event_slug} (expected prefix {POLYMARKET_15M_SLUG_PREFIX})"
                )
            event_title = str(event.get("title", ""))

            for market in event.get("markets", []):
                market_id = str(market.get("id", ""))
                if not market_id or market_id in seen_market_ids:
                    continue
                market_slug = str(market.get("slug", "")).strip()
                if market_slug != slug:
                    raise RuntimeError(
                        f"Unexpected Polymarket market slug in event {event_slug}: {market_slug} (expected {slug})"
                    )
                if not market_slug.startswith(POLYMARKET_15M_SLUG_PREFIX):
                    raise RuntimeError(
                        f"Unexpected Polymarket market slug format: {market_slug} (expected prefix {POLYMARKET_15M_SLUG_PREFIX})"
                    )
                if not bool(market.get("active", False)):
                    continue
                if bool(market.get("archived", False)):
                    continue

                end_dt = _parse_iso_datetime(
                    market.get("endDate") or market.get("endDateIso") or event.get("endDate")
                )
                if not end_dt:
                    continue
                if not _is_15m_bucket_aligned(end_dt):
                    continue
                seconds_to_end = _seconds_to_end(end_dt, now_utc)
                if seconds_to_end <= 0:
                    continue

                question = str(market.get("question", ""))

                token_ids = _parse_polymarket_token_ids(market.get("clobTokenIds", []))
                if len(token_ids) < 2:
                    continue

                normalization = _build_polymarket_normalization(event, market)
                markets.append(
                    {
                        "platform": "polymarket",
                        "market_id": market_id,
                        "condition_id": str(market.get("conditionId", "")),
                        "event_id": str(event.get("id", "")),
                        "event_slug": event_slug,
                        "event_title": event_title,
                        "market_question": question,
                        "token_yes": token_ids[0],
                        "token_no": token_ids[1],
                        "active": bool(market.get("active", False)),
                        "closed": bool(market.get("closed", False)),
                        "window_end": end_dt.isoformat(),
                        "seconds_to_end": seconds_to_end,
                        "is_current_window": _is_current_window(seconds_to_end),
                        "normalization": asdict(normalization),
                        "normalization_signature": normalization.signature(),
                    }
                )
                seen_market_ids.add(market_id)

        markets.sort(key=lambda m: m["window_end"])
        return markets


class KalshiDiscoveryClient:
    def __init__(self, config_path: Path) -> None:
        if not config_path.exists():
            raise FileNotFoundError(
                f"Missing config file: {config_path}. Create config/run_config.json"
            )

        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        api = config.get("api", {})
        markets = config.get("markets", {})
        self.base_url = str(api.get("base_url") or KALSHI_DEFAULT_API_BASE).rstrip("/")
        self.target_series = str(markets.get("target_series") or KALSHI_15M_SERIES).upper()
        if self.target_series != KALSHI_15M_SERIES:
            raise RuntimeError(
                f"Unsupported Kalshi series for this strict discovery: {self.target_series} "
                f"(expected {KALSHI_15M_SERIES})"
            )
        self.api_key = self._resolve_kalshi_api_key(str(api.get("readonly_api_key") or ""))
        if not self.api_key:
            raise RuntimeError(
                "Missing Kalshi API key. Set KALSHI_READONLY_API_KEY in .env "
                "or api.readonly_api_key in config/run_config.json."
            )

        self.transport = ApiTransport(
            default_headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "Arbitrage-Market-Discovery/1.0",
            }
        )

    @staticmethod
    def _resolve_kalshi_api_key(config_key: str) -> str:
        env_candidates = ["KALSHI_READONLY_API_KEY", "KALSHI_API_KEY", "kalshiapi"]
        for env_key in env_candidates:
            value = os.getenv(env_key, "").strip()
            if value:
                return value
        return config_key.strip()

    def _fetch_market_by_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/markets/{ticker}"
        status, payload = self.transport.request_json("GET", url, allow_status={200, 404})
        if status == 404:
            return None
        if not payload:
            return None
        if isinstance(payload, dict) and "market" in payload:
            market = payload["market"]
            if isinstance(market, dict):
                return market
        if isinstance(payload, dict) and payload.get("ticker"):
            return payload
        raise RuntimeError(f"Unexpected Kalshi market payload for ticker {ticker}")

    def discover(self, now_utc: datetime, back_buckets: int, forward_buckets: int) -> List[Dict[str, Any]]:
        raw_markets: List[Dict[str, Any]] = []
        for ticker in _kalshi_15m_candidate_tickers(now_utc, back_buckets, forward_buckets):
            market = self._fetch_market_by_ticker(ticker)
            if market:
                raw_markets.append(market)

        discovered: List[Dict[str, Any]] = []
        seen_tickers: set[str] = set()
        for market in raw_markets:
            ticker = str(market.get("ticker", "")).strip()
            if not ticker or ticker in seen_tickers:
                continue
            if not ticker.startswith(KALSHI_15M_TICKER_PREFIX):
                raise RuntimeError(
                    f"Unexpected Kalshi ticker format: {ticker} (expected prefix {KALSHI_15M_TICKER_PREFIX})"
                )
            seen_tickers.add(ticker)

            title = str(market.get("title", ""))
            if str(market.get("status", "")).lower() != "active":
                continue

            end_dt = _parse_iso_datetime(market.get("close_time") or market.get("expiration_time"))
            if not end_dt:
                continue
            if not _is_15m_bucket_aligned(end_dt):
                continue
            seconds_to_end = _seconds_to_end(end_dt, now_utc)
            if seconds_to_end <= 0:
                continue

            normalization = _build_kalshi_normalization(market)
            discovered.append(
                {
                    "platform": "kalshi",
                    "ticker": ticker,
                    "title": title,
                    "status": str(market.get("status", "")),
                    "window_end": end_dt.isoformat(),
                    "seconds_to_end": seconds_to_end,
                    "is_current_window": _is_current_window(seconds_to_end),
                    "normalization": asdict(normalization),
                    "normalization_signature": normalization.signature(),
                }
            )

        discovered.sort(key=lambda m: m["window_end"])
        return discovered


def _group_by_signature(markets: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for market in markets:
        signature = market.get("normalization_signature")
        if not signature:
            continue
        grouped.setdefault(signature, []).append(market)
    return grouped


def _infer_pairs(pm_markets: List[Dict[str, Any]], kalshi_markets: List[Dict[str, Any]], created_at: str) -> List[Dict[str, Any]]:
    pairs: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)
    pm_by_sig = _group_by_signature(pm_markets)
    kx_by_sig = _group_by_signature(kalshi_markets)
    signatures = sorted(set(pm_by_sig) & set(kx_by_sig))

    for signature in signatures:
        for pm in pm_by_sig[signature]:
            for kx in kx_by_sig[signature]:
                pm_end = _parse_iso_datetime(pm.get("normalization", {}).get("window_end"))
                kx_end = _parse_iso_datetime(kx.get("normalization", {}).get("window_end"))
                pair_end = pm_end or kx_end
                if not pair_end:
                    continue
                seconds_to_end = _seconds_to_end(pair_end, now_utc)
                pairs.append(
                    {
                        "normalization_signature": signature,
                        "polymarket_market_id": str(pm.get("market_id", "")),
                        "polymarket_event_slug": str(pm.get("event_slug", "")),
                        "polymarket_question": str(pm.get("market_question", "")),
                        "kalshi_ticker": str(kx.get("ticker", "")),
                        "polymarket_normalization": pm.get("normalization", {}),
                        "kalshi_normalization": kx.get("normalization", {}),
                        "window_end": pair_end.isoformat(),
                        "seconds_to_end": seconds_to_end,
                        "is_current_window": _is_current_window(seconds_to_end),
                        "created_at": created_at,
                    }
                )
    pairs.sort(key=lambda p: p.get("window_end", ""))
    return pairs


def _select_current_contract(candidates: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], str]:
    current = [c for c in candidates if bool(c.get("is_current_window"))]
    if current:
        current.sort(key=lambda c: c.get("window_end", ""))
        return current[0], "current_window"

    future = [c for c in candidates if int(c.get("seconds_to_end", 0)) > 0]
    if future:
        future.sort(key=lambda c: c.get("window_end", ""))
        return future[0], "next_window"
    return None, "no_open_market"


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def run_discovery(
    config_path: Path,
    output_path: Path,
    pair_cache_path: Path,
    pm_back_buckets: int,
    pm_forward_buckets: int,
    kalshi_back_buckets: int,
    kalshi_forward_buckets: int,
) -> Dict[str, Any]:
    load_dotenv(dotenv_path=".env", override=False)
    now_utc = datetime.now(timezone.utc)
    generated_at = now_utc.isoformat()

    pm_client = PolymarketDiscoveryClient()
    kalshi_client = KalshiDiscoveryClient(config_path=config_path)

    pm_markets = pm_client.discover(now_utc, pm_back_buckets, pm_forward_buckets)
    kalshi_markets = kalshi_client.discover(now_utc, kalshi_back_buckets, kalshi_forward_buckets)
    pairs = _infer_pairs(pm_markets, kalshi_markets, generated_at)

    selected_pm, selected_pm_reason = _select_current_contract(pm_markets)
    selected_kx, selected_kx_reason = _select_current_contract(kalshi_markets)
    selected_pair, selected_pair_reason = _select_current_contract(pairs)

    payload: Dict[str, Any] = {
        "generated_at": generated_at,
        "window_seconds": WINDOW_SECONDS,
        "polymarket": {
            "count": len(pm_markets),
            "selected_reason": selected_pm_reason,
            "selected_contract": selected_pm,
            "active_markets": pm_markets,
        },
        "kalshi": {
            "count": len(kalshi_markets),
            "selected_reason": selected_kx_reason,
            "selected_contract": selected_kx,
            "active_markets": kalshi_markets,
        },
        "pairs": {
            "count": len(pairs),
            "selected_reason": selected_pair_reason,
            "selected_pair": selected_pair,
            "matched_pairs": pairs,
        },
    }

    _write_json(output_path, payload)

    pair_cache = {
        "version": 1,
        "updated_at": generated_at,
        "last_inference_attempt_at": generated_at,
        "last_market_fingerprint": {
            "polymarket": sorted(
                f"{m.get('market_id')}|{m.get('normalization_signature')}" for m in pm_markets
            ),
            "kalshi": sorted(
                f"{m.get('ticker')}|{m.get('normalization_signature')}" for m in kalshi_markets
            ),
        },
        "pairs": pairs,
    }
    _write_json(pair_cache_path, pair_cache)
    return payload


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover active BTC 15m up/down markets.")
    parser.add_argument(
        "--config",
        default="config/run_config.json",
        help="Path to runtime config JSON.",
    )
    parser.add_argument(
        "--output",
        default="data/market_discovery_latest.json",
        help="Path for discovery output JSON.",
    )
    parser.add_argument(
        "--pair-cache",
        default="data/market_pair_cache.json",
        help="Path for inferred pair cache JSON.",
    )
    parser.add_argument("--pm-back-buckets", type=int, default=2)
    parser.add_argument("--pm-forward-buckets", type=int, default=8)
    parser.add_argument("--kalshi-back-buckets", type=int, default=2)
    parser.add_argument("--kalshi-forward-buckets", type=int, default=8)
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    payload = run_discovery(
        config_path=Path(args.config),
        output_path=Path(args.output),
        pair_cache_path=Path(args.pair_cache),
        pm_back_buckets=args.pm_back_buckets,
        pm_forward_buckets=args.pm_forward_buckets,
        kalshi_back_buckets=args.kalshi_back_buckets,
        kalshi_forward_buckets=args.kalshi_forward_buckets,
    )

    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))

    print("Market discovery complete")
    print(f"UTC now: {now_utc.isoformat()}")
    print(f"ET now:  {now_et.isoformat()}")
    print(f"Polymarket active markets: {payload['polymarket']['count']}")
    print(f"Kalshi active markets: {payload['kalshi']['count']}")
    print(f"Matched pairs: {payload['pairs']['count']}")

    selected_pair = payload["pairs"]["selected_pair"]
    if selected_pair:
        print(
            "Selected pair: "
            f"{selected_pair.get('polymarket_event_slug')} <-> {selected_pair.get('kalshi_ticker')} "
            f"({payload['pairs']['selected_reason']})"
        )
    else:
        print(f"No open pair selected ({payload['pairs']['selected_reason']})")


if __name__ == "__main__":
    main()
