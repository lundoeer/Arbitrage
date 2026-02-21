#!/usr/bin/env python3
"""
Derive Polymarket funder wallet from on-chain exchange contract lookup.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.buy_execution import derive_polymarket_funder_from_chain  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve Polymarket funder from chain")
    parser.add_argument("--base-url", default="https://clob.polymarket.com")
    parser.add_argument(
        "--rpc-url",
        default="",
        help=(
            "Optional single RPC URL override. When omitted, uses default fallback list "
            "or POLYMARKET_FUNDER_RPC_URL (comma-separated)."
        ),
    )
    parser.add_argument("--signature-type", type=int, default=1, choices=(1, 2))
    parser.add_argument("--chain-id", type=int, default=137)
    return parser.parse_args()


def main() -> int:
    load_dotenv(dotenv_path=".env", override=False)
    args = _parse_args()

    l1_key = str(os.getenv("POLYMARKET_L1_APIKEY", "") or "").strip()
    if not l1_key:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "Missing POLYMARKET_L1_APIKEY in environment.",
                },
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    configured_funder = str(os.getenv("POLYMARKET_FUNDER", "") or "").strip() or None
    try:
        derived_funder = derive_polymarket_funder_from_chain(
            l1_key=l1_key,
            signature_type=int(args.signature_type),
            chain_id=int(args.chain_id),
            base_url=str(args.base_url),
            rpc_url=(str(args.rpc_url).strip() or None),
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    payload = {
        "ok": True,
        "signature_type": int(args.signature_type),
        "chain_id": int(args.chain_id),
        "derived_funder": derived_funder,
        "configured_funder": configured_funder,
        "matches_configured_funder": (
            None if configured_funder is None else (configured_funder.lower() == derived_funder.lower())
        ),
    }
    print(json.dumps(payload, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
