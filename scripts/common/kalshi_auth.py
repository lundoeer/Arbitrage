from __future__ import annotations

import base64
import os
import time
from pathlib import Path
from typing import Any, Dict, Protocol, cast

from scripts.common.utils import normalize_kalshi_pem as _normalize_pem


class _SigningKey(Protocol):
    def sign(self, data: bytes, pad: Any, algorithm: Any) -> bytes: ...


def resolve_kalshi_ws_headers() -> Dict[str, str]:
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except Exception as exc:
        raise RuntimeError(
            "Missing cryptography dependency. Install requirements.txt in your .venv."
        ) from exc

    api_key = ""
    for key in ("KALSHI_RW_API_KEY", "KALSHI_READONLY_API_KEY", "KALSHI_API_KEY", "kalshiapi"):
        value = os.getenv(key, "").strip()
        if value:
            api_key = value
            break

    pem = (os.getenv("KALSHI_PRIVATEKEY", "") or os.getenv("KALSHI_PRIVATE_KEY", "")).strip()
    pem_path = (os.getenv("KALSHI_PRIVATEKEY_PATH", "") or os.getenv("KALSHI_PRIVATE_KEY_PATH", "")).strip()
    if not pem and pem_path:
        path = Path(pem_path)
        if not path.exists():
            raise RuntimeError(f"KALSHI private key path does not exist: {path}")
        pem = path.read_text(encoding="utf-8")
    pem = _normalize_pem(pem)

    if not api_key or not pem:
        raise RuntimeError(
            "Missing Kalshi websocket auth credentials. "
            "Set KALSHI_READONLY_API_KEY (or KALSHI_RW_API_KEY) and KALSHI_PRIVATEKEY in .env."
        )

    try:
        private_key_obj = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
    except Exception as exc:
        raise RuntimeError(
            "Invalid KALSHI private key format. Use KALSHI_PRIVATEKEY as a valid PEM "
            "or provide KALSHI_PRIVATEKEY_PATH pointing to a PEM file."
        ) from exc
    private_key = cast(_SigningKey, private_key_obj)
    timestamp_ms = str(int(time.time() * 1000))
    message = f"{timestamp_ms}GET/trade-api/ws/v2"
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }
