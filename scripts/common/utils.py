"""Shared utility helpers used across the arbitrage engine."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional


def now_ms() -> int:
    """Current UTC time as epoch milliseconds."""
    return int(time.time() * 1000)


def utc_now_iso() -> str:
    """Current UTC time as ISO-8601 string (second precision)."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def as_dict(value: Any) -> Dict[str, Any]:
    """Return *value* if it is a dict, otherwise return an empty dict."""
    return value if isinstance(value, dict) else {}


def as_float(value: Any) -> Optional[float]:
    """Coerce int/float/str to float; return None for anything else or parsing error."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> Optional[int]:
    """Coerce int/float/str to int; return None for bool or anything else."""
    if isinstance(value, bool):
        return None
    try:
        return int(float(value)) if isinstance(value, str) else int(value)
    except (TypeError, ValueError):
        return None


def as_non_empty_text(value: Any) -> Optional[str]:
    """Return stripped string if non-empty, otherwise None."""
    text = str(value or "").strip()
    return text if text else None


def normalize_kalshi_pem(raw_value: str) -> str:
    """Normalize a Kalshi PEM private key from env-var formatting.

    Handles:
    - Surrounding quotes (single or double)
    - Escaped newlines (\\n, \\r)
    - Whitespace-collapsed bodies
    """
    text = str(raw_value or "").strip()
    if not text:
        return ""
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        text = text[1:-1].strip()
    text = text.replace("\\r", "\r").replace("\\n", "\n")
    if "-----BEGIN" not in text or "-----END" not in text:
        return text
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) >= 3 and lines[0].startswith("-----BEGIN ") and lines[-1].startswith("-----END "):
        body = "".join(lines[1:-1]).replace(" ", "")
        return f"{lines[0]}\n{body}\n{lines[-1]}\n"
    return text
