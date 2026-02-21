#!/usr/bin/env python3
"""
Build a Polymarket signed order payload and auth headers without submitting.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.buy_execution import build_polymarket_order_signing_providers_from_env  # noqa: E402
from scripts.common.market_selection import load_selected_markets  # noqa: E402


def _mask(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "<missing>"
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def _default_client_order_id(*, side: str) -> str:
    ts = int(time.time() * 1000)
    return f"manual-pm-{str(side).strip().lower()}-{ts}"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Polymarket signed order (no submit)")
    parser.add_argument("--token-id", default="", help="Polymarket token id; when omitted, resolve from current market")
    parser.add_argument("--side", default="yes", choices=("yes", "no"), help="Outcome side token to buy")
    parser.add_argument("--size", type=float, default=1.0, help="Order size")
    parser.add_argument("--price", type=float, default=0.55, help="Limit price for signed order")
    parser.add_argument("--order-kind", default="market", choices=("market", "limit"))
    parser.add_argument("--time-in-force", default="fak", choices=("fak", "ioc", "gtc"))
    parser.add_argument("--client-order-id", default="")
    parser.add_argument("--base-url", default="https://clob.polymarket.com")
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--discovery-output", default="data/market_discovery_latest.json")
    parser.add_argument("--pair-cache", default="data/market_pair_cache.json")
    parser.add_argument("--skip-discovery", action="store_true")
    return parser.parse_args()


def _resolve_token_id(args: argparse.Namespace) -> tuple[str, Dict[str, Any]]:
    token_id = str(args.token_id or "").strip()
    market: Dict[str, Any] = {}
    if token_id:
        return token_id, market

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
        raise RuntimeError("could_not_resolve_polymarket_token_id")
    market = {
        "event_slug": pm.get("event_slug"),
        "market_id": pm.get("market_id"),
        "token_yes": token_yes,
        "token_no": token_no,
        "window_end": pm.get("window_end"),
    }
    return token_id, market


def main() -> int:
    load_dotenv(dotenv_path=".env", override=False)
    args = _parse_args()

    if float(args.size) <= 0:
        print(json.dumps({"ok": False, "error": "size_must_be_positive"}, separators=(",", ":")), file=sys.stderr)
        return 1

    try:
        token_id, market = _resolve_token_id(args)
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "phase": "token_resolution", "error_type": type(exc).__name__, "error": str(exc)},
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    client_order_id = str(args.client_order_id or "").strip() or _default_client_order_id(side=args.side)

    try:
        signed_order_builder, auth_headers_provider = build_polymarket_order_signing_providers_from_env(
            base_url=str(args.base_url).rstrip("/"),
        )
        payload = signed_order_builder(
            instrument_id=token_id,
            side=str(args.side),
            size=float(args.size),
            order_kind=str(args.order_kind),
            limit_price=float(args.price),
            time_in_force=str(args.time_in_force),
            client_order_id=client_order_id,
        )
        headers = auth_headers_provider(
            request_path="/order",
            method="POST",
            body=payload,
        )
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "phase": "signing", "error_type": type(exc).__name__, "error": str(exc)},
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    order = dict(payload.get("order") or payload.get("signed_order") or {})
    preview = {
        "ok": True,
        "base_url": str(args.base_url).rstrip("/"),
        "market": market,
        "request_preview": {
            "token_id": token_id,
            "side": str(args.side),
            "order_kind": str(args.order_kind),
            "time_in_force": str(args.time_in_force),
            "price": float(args.price),
            "size": float(args.size),
            "client_order_id": client_order_id,
            "payload_keys": sorted(payload.keys()),
            "order_keys": sorted(order.keys()),
            "order_type": payload.get("orderType"),
            "owner_masked": _mask(payload.get("owner")),
        },
        "auth_headers_preview": {
            "POLY_ADDRESS": headers.get("POLY_ADDRESS"),
            "POLY_TIMESTAMP": headers.get("POLY_TIMESTAMP"),
            "POLY_API_KEY_masked": _mask(headers.get("POLY_API_KEY")),
            "POLY_PASSPHRASE_masked": _mask(headers.get("POLY_PASSPHRASE")),
            "POLY_SIGNATURE_prefix": str(headers.get("POLY_SIGNATURE") or "")[:16],
            "header_keys": sorted(headers.keys()),
        },
        "env_presence": {
            "POLYMARKET_L1_APIKEY": bool(str(os.getenv("POLYMARKET_L1_APIKEY", "")).strip()),
            "POLYMARKET_L2_API_KEY": bool(str(os.getenv("POLYMARKET_L2_API_KEY", "")).strip()),
            "POLYMARKET_L2_API_SECRET": bool(str(os.getenv("POLYMARKET_L2_API_SECRET", "")).strip()),
            "POLYMARKET_L2_API_PASSPHRASE": bool(str(os.getenv("POLYMARKET_L2_API_PASSPHRASE", "")).strip()),
        },
    }
    print(json.dumps(preview, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
