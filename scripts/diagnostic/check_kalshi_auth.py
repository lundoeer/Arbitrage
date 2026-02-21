#!/usr/bin/env python3
"""
Kalshi auth-only diagnostic check.

Validates API key + signing key by calling a signed private read endpoint:
- GET /trade-api/v2/portfolio/balance
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiHTTPError, ApiTransport  # noqa: E402
from scripts.common.buy_execution import (  # noqa: E402
    _load_kalshi_private_key_from_env,
    _resolve_kalshi_api_key_from_env,
    now_ms,
)


def _mask(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "<missing>"
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def _sign(*, private_key: Any, method: str, path: str, timestamp_ms: str) -> str:
    import base64

    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    message = f"{timestamp_ms}{method.upper()}{path}"
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Kalshi signed private API auth (no orders)")
    parser.add_argument("--base-host", default="https://api.elections.kalshi.com")
    return parser.parse_args()


def main() -> int:
    load_dotenv(dotenv_path=".env", override=False)
    args = _parse_args()

    try:
        api_key = _resolve_kalshi_api_key_from_env()
        if not api_key:
            raise RuntimeError("Missing Kalshi API key in env")
        private_key = _load_kalshi_private_key_from_env()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "phase": "credentials_load",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                separators=(",", ":"),
            )
        )
        return 1

    path = "/trade-api/v2/portfolio/balance"
    ts = str(now_ms())
    try:
        headers = {
            "KALSHI-ACCESS-KEY": api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": _sign(
                private_key=private_key,
                method="GET",
                path=path,
                timestamp_ms=ts,
            ),
            "Content-Type": "application/json",
        }
        transport = ApiTransport(timeout_seconds=15)
        status_code, payload = transport.request_json(
            "GET",
            f"{str(args.base_host).rstrip('/')}{path}",
            headers=headers,
            allow_status={200},
        )
    except ApiHTTPError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "phase": "private_read",
                    "api_key_masked": _mask(api_key),
                    "status_code": int(exc.status_code),
                    "error": str(exc),
                },
                separators=(",", ":"),
            )
        )
        return 1
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "phase": "private_read",
                    "api_key_masked": _mask(api_key),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                separators=(",", ":"),
            )
        )
        return 1

    balance_payload: Dict[str, Any] = payload if isinstance(payload, dict) else {}
    print(
        json.dumps(
            {
                "ok": True,
                "api_key_masked": _mask(api_key),
                "status_code": int(status_code),
                "endpoint": path,
                "balance_fields_present": sorted(list(balance_payload.keys())),
            },
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
