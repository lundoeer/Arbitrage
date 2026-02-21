from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("_")


def load_selected_markets(
    *,
    config_path: Path,
    discovery_output: Path,
    pair_cache_path: Path,
    run_discovery_first: bool,
) -> Dict[str, Any]:
    if run_discovery_first:
        # Delayed import keeps this module lightweight and avoids startup
        # dependency cycles when only static selection is needed.
        from scripts.run.discover_active_btc_15m_markets import run_discovery

        payload = run_discovery(
            config_path=config_path,
            output_path=discovery_output,
            pair_cache_path=pair_cache_path,
            pm_back_buckets=2,
            pm_forward_buckets=8,
            kalshi_back_buckets=2,
            kalshi_forward_buckets=8,
        )
    else:
        if not discovery_output.exists():
            raise FileNotFoundError(
                f"Missing discovery output file: {discovery_output}. Run discovery first or remove --skip-discovery."
            )
        payload = json.loads(discovery_output.read_text(encoding="utf-8"))

    selected_pm = payload.get("polymarket", {}).get("selected_contract")
    selected_kx = payload.get("kalshi", {}).get("selected_contract")
    if not isinstance(selected_pm, dict):
        raise RuntimeError("No selected Polymarket contract available from discovery.")
    if not isinstance(selected_kx, dict):
        raise RuntimeError("No selected Kalshi contract available from discovery.")

    pm_token_yes = str(selected_pm.get("token_yes") or "").strip()
    pm_token_no = str(selected_pm.get("token_no") or "").strip()
    pm_slug = str(selected_pm.get("event_slug") or "").strip()
    pm_market_id = str(selected_pm.get("market_id") or "").strip()
    kx_ticker = str(selected_kx.get("ticker") or "").strip()

    if not pm_token_yes or not pm_token_no:
        raise RuntimeError("Selected Polymarket contract is missing token_yes/token_no.")
    if not pm_slug:
        raise RuntimeError("Selected Polymarket contract is missing event_slug.")
    if not pm_market_id:
        raise RuntimeError("Selected Polymarket contract is missing market_id.")
    if not kx_ticker:
        raise RuntimeError("Selected Kalshi contract is missing ticker.")

    pm_sig = str(selected_pm.get("normalization_signature") or "")
    kx_sig = str(selected_kx.get("normalization_signature") or "")
    if pm_sig and kx_sig and pm_sig != kx_sig:
        raise RuntimeError(
            f"Selected venue signatures do not match. polymarket={pm_sig} kalshi={kx_sig}"
        )

    return {
        "discovery_generated_at": payload.get("generated_at"),
        "polymarket": {
            "event_slug": pm_slug,
            "market_id": pm_market_id,
            "token_yes": pm_token_yes,
            "token_no": pm_token_no,
            "window_end": selected_pm.get("window_end"),
            "normalization_signature": pm_sig,
        },
        "kalshi": {
            "ticker": kx_ticker,
            "window_end": selected_kx.get("window_end"),
            "normalization_signature": kx_sig,
        },
    }
