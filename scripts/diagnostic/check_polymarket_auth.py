#!/usr/bin/env python3
"""
Polymarket auth-only diagnostic check.

Validates:
- L2 API key/secret/passphrase can perform authenticated CLOB private read.
- Funder alignment for signature_type 1/2 (configured vs on-chain derived).

No orders are submitted.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.buy_execution import (  # noqa: E402
    derive_polymarket_funder_from_chain,
)


def _mask(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "<missing>"
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def _normalize_eth_address(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("empty_address")
    if not raw.startswith("0x"):
        raw = f"0x{raw}"
    lower = raw.lower()
    if not re.fullmatch(r"0x[a-f0-9]{40}", lower):
        raise RuntimeError(f"invalid_eth_address:{value!r}")
    return lower


def _parse_int_env(key: str, default: int) -> int:
    raw = str(os.getenv(key, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception as exc:
        raise RuntimeError(f"invalid_{key}:{raw!r}") from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Polymarket CLOB auth (no orders)")
    parser.add_argument("--base-url", default="https://clob.polymarket.com")
    parser.add_argument("--rpc-url", default="")
    parser.add_argument("--signature-type", type=int, choices=(0, 1, 2), default=None)
    parser.add_argument("--chain-id", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    load_dotenv(dotenv_path=".env", override=False)
    args = _parse_args()

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "phase": "imports",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                separators=(",", ":"),
            )
        )
        return 1

    l1_key = str(os.getenv("POLYMARKET_L1_APIKEY", "") or "").strip()
    l2_key = str(os.getenv("POLYMARKET_L2_API_KEY", "") or "").strip()
    l2_secret = str(os.getenv("POLYMARKET_L2_API_SECRET", "") or "").strip()
    l2_passphrase = str(os.getenv("POLYMARKET_L2_API_PASSPHRASE", "") or "").strip()
    configured_funder_raw = str(os.getenv("POLYMARKET_FUNDER", "") or "").strip()

    if not l1_key:
        print(json.dumps({"ok": False, "phase": "env", "error": "missing_POLYMARKET_L1_APIKEY"}, separators=(",", ":")))
        return 1
    missing_l2 = []
    if not l2_key:
        missing_l2.append("POLYMARKET_L2_API_KEY")
    if not l2_secret:
        missing_l2.append("POLYMARKET_L2_API_SECRET")
    if not l2_passphrase:
        missing_l2.append("POLYMARKET_L2_API_PASSPHRASE")
    if missing_l2:
        print(
            json.dumps(
                {"ok": False, "phase": "env", "error": f"missing_l2_credentials:{','.join(missing_l2)}"},
                separators=(",", ":"),
            )
        )
        return 1

    try:
        signature_type = int(args.signature_type) if args.signature_type is not None else _parse_int_env(
            "POLYMARKET_SIGNATURE_TYPE",
            1,
        )
        chain_id = int(args.chain_id) if args.chain_id is not None else _parse_int_env("POLYMARKET_CHAIN_ID", 137)
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "phase": "config", "error_type": type(exc).__name__, "error": str(exc)},
                separators=(",", ":"),
            )
        )
        return 1

    if signature_type not in {0, 1, 2}:
        print(
            json.dumps(
                {"ok": False, "phase": "config", "error": f"invalid_signature_type:{signature_type}"},
                separators=(",", ":"),
            )
        )
        return 1

    derived_funder: Optional[str] = None
    configured_funder: Optional[str] = None
    funder_match: Optional[bool] = None
    effective_funder: Optional[str] = None

    try:
        if configured_funder_raw:
            configured_funder = _normalize_eth_address(configured_funder_raw)

        if signature_type in {1, 2}:
            derived_funder = derive_polymarket_funder_from_chain(
                l1_key=l1_key,
                signature_type=int(signature_type),
                chain_id=int(chain_id),
                base_url=str(args.base_url),
                rpc_url=(str(args.rpc_url).strip() or None),
            )
            if configured_funder is not None:
                funder_match = bool(configured_funder == derived_funder)
                if not funder_match:
                    raise RuntimeError(
                        f"configured_funder_mismatch:{configured_funder}!={derived_funder}"
                    )
                effective_funder = configured_funder
            else:
                effective_funder = derived_funder
        else:
            effective_funder = configured_funder
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "phase": "funder_check",
                    "signature_type": int(signature_type),
                    "configured_funder": configured_funder,
                    "derived_funder": derived_funder,
                    "funder_match": funder_match,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                separators=(",", ":"),
            )
        )
        return 1

    try:
        client_kwargs: dict[str, Any] = {
            "host": str(args.base_url).rstrip("/"),
            "chain_id": int(chain_id),
            "key": l1_key,
            "signature_type": int(signature_type),
        }
        if effective_funder:
            client_kwargs["funder"] = effective_funder

        clob_client = ClobClient(**client_kwargs)
        signer_address = _normalize_eth_address(str(clob_client.signer.address()))
        clob_client.set_api_creds(
            ApiCreds(api_key=l2_key, api_secret=l2_secret, api_passphrase=l2_passphrase)
        )
        allowance = clob_client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=int(signature_type),
            )
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "phase": "private_read",
                    "signature_type": int(signature_type),
                    "chain_id": int(chain_id),
                    "l2_api_key_masked": _mask(l2_key),
                    "effective_funder": effective_funder,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                separators=(",", ":"),
            )
        )
        return 1

    print(
        json.dumps(
            {
                "ok": True,
                "signature_type": int(signature_type),
                "chain_id": int(chain_id),
                "signer_address": signer_address,
                "configured_funder": configured_funder,
                "derived_funder": derived_funder,
                "effective_funder": effective_funder,
                "funder_match": funder_match,
                "l2_api_key_masked": _mask(l2_key),
                "allowance_fields_present": (
                    sorted(list(allowance.keys())) if isinstance(allowance, dict) else []
                ),
                "allowance_balance_raw": (
                    str((allowance or {}).get("balance")) if isinstance(allowance, dict) else None
                ),
            },
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
