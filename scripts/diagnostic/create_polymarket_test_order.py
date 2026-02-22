#!/usr/bin/env python3
"""
Submit a small Polymarket YES/NO buy order for manual testing.

Default behavior submits an order. Use --dry-run to preview only.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport, RetryConfig  # noqa: E402
from scripts.common.buy_execution import build_polymarket_api_buy_client_from_env  # noqa: E402
from scripts.common.market_selection import load_selected_markets  # noqa: E402
from scripts.common.run_config import load_buy_execution_runtime_config_from_run_config  # noqa: E402


def _default_client_order_id(*, side: str, token_id: str) -> str:
    ts = int(time.time() * 1000)
    token_short = "".join(ch for ch in str(token_id) if ch.isalnum())[:16] or "token"
    return f"manual-pm-{side}-{token_short}-{ts}"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create one Polymarket buy order (~$2 test)")
    parser.add_argument("--token-id", default="", help="Polymarket token id; omit to use selected market side token")
    parser.add_argument("--side", default="yes", choices=("yes", "no"), help="Outcome side token to buy")
    parser.add_argument(
        "--usd-amount",
        type=float,
        default=2.0,
        help="Target max USD notional for the order",
    )
    parser.add_argument(
        "--limit-price",
        type=float,
        default=None,
        help="Explicit limit price [0,1]. When omitted, uses best ask + offset",
    )
    parser.add_argument(
        "--price-offset",
        type=float,
        default=0.02,
        help="Offset added to best ask when limit price is not provided",
    )
    parser.add_argument(
        "--max-limit-price",
        type=float,
        default=0.99,
        help="Upper cap applied to computed limit price",
    )
    parser.add_argument(
        "--base-url",
        default="https://clob.polymarket.com",
        help="Polymarket CLOB API base URL",
    )
    parser.add_argument(
        "--order-kind",
        default="market",
        choices=("market", "limit"),
        help="Order kind sent to the buy client",
    )
    parser.add_argument(
        "--time-in-force",
        default="fak",
        choices=("fak", "ioc", "gtc"),
        help="Time in force",
    )
    parser.add_argument("--client-order-id", default="", help="Optional custom client order id")
    parser.add_argument("--config", default="config/run_config.json", help="Run config path")
    parser.add_argument("--discovery-output", default="data/market_discovery_latest.json")
    parser.add_argument("--pair-cache", default="data/market_pair_cache.json")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Print payload preview and do not submit")
    return parser


def _resolve_token_and_market(args: argparse.Namespace) -> Tuple[str, Dict[str, Any]]:
    token_id = str(args.token_id or "").strip()
    if token_id:
        return token_id, {}

    selection = load_selected_markets(
        config_path=Path(args.config),
        discovery_output=Path(args.discovery_output),
        pair_cache_path=Path(args.pair_cache),
        run_discovery_first=(not bool(args.skip_discovery)),
    )
    pm = dict(selection.get("polymarket") or {})
    token_yes = str(pm.get("token_yes") or "").strip()
    token_no = str(pm.get("token_no") or "").strip()
    token_id = token_yes if str(args.side).strip().lower() == "yes" else token_no
    if not token_id:
        raise RuntimeError("could_not_resolve_token_id_from_selected_market")
    return token_id, {
        "event_slug": pm.get("event_slug"),
        "market_id": pm.get("market_id"),
        "condition_id": pm.get("condition_id"),
        "token_yes": token_yes,
        "token_no": token_no,
        "window_end": pm.get("window_end"),
    }


def _fetch_reference_buy_price(*, base_url: str, token_id: str) -> Tuple[float, str]:
    try:
        from py_clob_client.client import ClobClient
    except Exception as exc:
        raise RuntimeError("Missing dependency py-clob-client for orderbook fetch") from exc

    client = ClobClient(str(base_url).rstrip("/"))
    book = client.get_order_book(str(token_id))
    asks = list(getattr(book, "asks", []) or [])
    if asks:
        first = asks[0]
        price = float(getattr(first, "price", 0.0))
        if price > 0.0:
            return float(price), "orderbook_best_ask"

    try:
        quote = client.get_price(str(token_id), side="BUY")
        quoted_price = float((quote or {}).get("price") or 0.0)
    except Exception as exc:
        raise RuntimeError("no_asks_in_orderbook_and_get_price_failed") from exc
    if quoted_price <= 0.0:
        raise RuntimeError("no_asks_in_orderbook_and_invalid_quote_price")
    return quoted_price, "quote_buy_price"


def _build_transport_from_config(config_path: Path) -> ApiTransport:
    cfg = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    api_retry = cfg.api_retry
    if not bool(api_retry.enabled):
        return ApiTransport(timeout_seconds=10)
    methods = {"GET", "HEAD", "OPTIONS"}
    if bool(api_retry.include_post):
        methods.add("POST")
    retry = RetryConfig(
        max_attempts=max(1, int(api_retry.max_attempts)),
        base_backoff_seconds=max(0.0, float(api_retry.base_backoff_seconds)),
        jitter_ratio=max(0.0, float(api_retry.jitter_ratio)),
        retry_methods=frozenset(methods),
    )
    return ApiTransport(timeout_seconds=10, retry_config=retry)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(dotenv_path=".env", override=False)
    parser = _build_parser()
    args = parser.parse_args(argv)

    if float(args.usd_amount) <= 0:
        parser.error("--usd-amount must be > 0")
    if args.limit_price is not None and not (0.0 < float(args.limit_price) <= 1.0):
        parser.error("--limit-price must be in (0,1]")
    if not (0.0 < float(args.max_limit_price) <= 1.0):
        parser.error("--max-limit-price must be in (0,1]")

    try:
        token_id, market = _resolve_token_and_market(args)
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "phase": "market_resolution", "error_type": type(exc).__name__, "error": str(exc)},
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    try:
        best_ask, price_source = _fetch_reference_buy_price(base_url=args.base_url, token_id=token_id)
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "phase": "orderbook_fetch", "error_type": type(exc).__name__, "error": str(exc)},
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    if args.limit_price is None:
        computed_limit = min(float(args.max_limit_price), best_ask + max(0.0, float(args.price_offset)))
    else:
        computed_limit = min(float(args.max_limit_price), float(args.limit_price))
    if computed_limit <= 0.0:
        print(
            json.dumps(
                {"ok": False, "phase": "input_validation", "error": "computed_limit_price_invalid"},
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    # Use limit price for sizing so max notional is capped by usd_amount.
    size = round(float(args.usd_amount) / float(computed_limit), 6)
    if size <= 0.0:
        print(
            json.dumps(
                {"ok": False, "phase": "sizing", "error": "computed_size_non_positive"},
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    client_order_id = str(args.client_order_id or "").strip() or _default_client_order_id(
        side=str(args.side).lower(),
        token_id=token_id,
    )

    cfg = load_buy_execution_runtime_config_from_run_config(config_path=Path(args.config))
    preview = {
        "ok": True,
        "mode": "dry_run" if bool(args.dry_run) else "submit",
        "market": market,
        "request_preview": {
            "base_url": str(args.base_url).rstrip("/"),
            "token_id": token_id,
            "side": str(args.side).lower(),
            "order_kind": str(args.order_kind).lower(),
            "time_in_force": str(args.time_in_force).lower(),
            "usd_amount": float(args.usd_amount),
            "best_ask": float(best_ask),
            "price_source": price_source,
            "limit_price": float(computed_limit),
            "size": float(size),
            "max_notional_usd": round(float(size) * float(computed_limit), 6),
            "client_order_id": client_order_id,
        },
    }
    print(json.dumps(preview, ensure_ascii=True, separators=(",", ":")))

    if bool(args.dry_run):
        return 0

    try:
        client = build_polymarket_api_buy_client_from_env(
            base_url=str(args.base_url).rstrip("/"),
            transport=_build_transport_from_config(Path(args.config)),
        )
        response = client.place_buy_order(
            instrument_id=token_id,
            side=str(args.side).lower(),
            size=float(size),
            order_kind=str(args.order_kind).lower(),
            limit_price=float(computed_limit),
            time_in_force=str(args.time_in_force).lower(),
            client_order_id=client_order_id,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "phase": "submit",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "client_order_id": client_order_id,
                    "token_id": token_id,
                },
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    print(json.dumps({"ok": True, "result": response}, ensure_ascii=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
