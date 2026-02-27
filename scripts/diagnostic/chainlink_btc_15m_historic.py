#!/usr/bin/env python3
"""
One-shot historical BTC/USD 15-minute series from Chainlink Data Feed (Ethereum mainnet).

- Pulls raw rounds from Chainlink BTC/USD proxy using getRoundData
- Keeps updates from --start (default 2025-12-01T00:00:00Z)
- Resamples to exact 15-minute timestamps using last-known price at or before each timestamp
- Outputs:
    1) chainlink_btcusd_updates.csv  (raw updates)
    2) chainlink_btcusd_15m.csv      (15-minute sampled series)

Notes:
- Chainlink feeds update on deviation/heartbeat, not exactly every 15 minutes.
  So the 15m series is a resample of irregular updates.
"""

import os
import sys
import time
import random
import argparse
from datetime import datetime, timezone

import pandas as pd
from web3 import Web3
from dotenv import load_dotenv


# Chainlink BTC/USD feed (Ethereum mainnet) standard proxy address:
# https://data.chain.link/feeds/ethereum/mainnet/btc-usd
FEED_BTC_USD_ETH_MAINNET = Web3.to_checksum_address("0xF4030086522a5beEA4988F8cA5B36dbC97BeE88c")

# Minimal ABI for AggregatorV3Interface + proxy-compatible methods.
# Chainlink docs: latestRoundData + getRoundData + decimals.
ABI = [
    {
        "name": "decimals",
        "outputs": [{"type": "uint8", "name": ""}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "latestRoundData",
        "outputs": [
            {"type": "uint80", "name": "roundId"},
            {"type": "int256", "name": "answer"},
            {"type": "uint256", "name": "startedAt"},
            {"type": "uint256", "name": "updatedAt"},
            {"type": "uint80", "name": "answeredInRound"},
        ],
        "inputs": [],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "getRoundData",
        "outputs": [
            {"type": "uint80", "name": "roundId"},
            {"type": "int256", "name": "answer"},
            {"type": "uint256", "name": "startedAt"},
            {"type": "uint256", "name": "updatedAt"},
            {"type": "uint80", "name": "answeredInRound"},
        ],
        "inputs": [{"type": "uint80", "name": "_roundId"}],
        "stateMutability": "view",
        "type": "function",
    },
]


def parse_utc(s: str) -> datetime:
    """
    Accepts:
      - '2025-12-01'
      - '2025-12-01T00:00:00Z'
      - '2025-12-01T00:00:00'
    Returns timezone-aware UTC datetime.
    """
    s = s.strip()
    if len(s) == 10:
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=timezone.utc)
    if s.endswith("Z"):
        s = s[:-1]
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def floor_to_interval(dt: datetime, seconds: int) -> datetime:
    ts = int(dt.timestamp())
    return datetime.fromtimestamp(ts - (ts % seconds), tz=timezone.utc)


def u80_phase_round(u80_round_id: int) -> tuple[int, int]:
    """
    Chainlink proxy roundId is commonly encoded as:
      (phaseId << 64) | aggregatorRoundId
    """
    phase = u80_round_id >> 64
    r = u80_round_id & ((1 << 64) - 1)
    return phase, r


def make_u80_round_id(phase: int, r: int) -> int:
    return (phase << 64) | r


def is_no_data_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "no data present" in text


def main() -> int:
    load_dotenv(".env", override=False)

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--rpc",
        default=os.getenv("ALCHEMY_RPC_URL", "").strip(),
        help="Ethereum mainnet RPC URL (or set env ALCHEMY_RPC_URL).",
    )
    ap.add_argument(
        "--feed",
        default=FEED_BTC_USD_ETH_MAINNET,
        help="Feed proxy address. Default is Chainlink BTC/USD on Ethereum mainnet.",
    )
    ap.add_argument(
        "--start", default="2025-12-01T00:00:00Z", help="Start datetime (UTC). Default 2025-12-01T00:00:00Z."
    )
    ap.add_argument(
        "--interval", type=int, default=15 * 60, help="Sampling interval in seconds. Default 900 (15 minutes)."
    )
    ap.add_argument("--updates_csv", default="chainlink_btcusd_updates.csv", help="Output CSV for raw updates.")
    ap.add_argument("--bars_csv", default="chainlink_btcusd_15m.csv", help="Output CSV for 15-minute sampled series.")
    ap.add_argument("--max_calls", type=int, default=0, help="Optional safety cap on getRoundData calls (0 = no cap).")
    ap.add_argument(
        "--sleep_every", type=int, default=250, help="Sleep briefly every N RPC calls to avoid rate limits."
    )
    ap.add_argument("--sleep_seconds", type=float, default=0.15, help="Sleep duration when sleeping.")
    ap.add_argument("--rpc_max_attempts", type=int, default=3, help="Max attempts per RPC call.")
    ap.add_argument(
        "--rpc_base_backoff_seconds",
        type=float,
        default=0.5,
        help="Base backoff seconds for RPC retries.",
    )
    ap.add_argument(
        "--rpc_jitter_ratio",
        type=float,
        default=0.2,
        help="Jitter ratio for RPC retries.",
    )
    ap.add_argument(
        "--progress_every",
        type=int,
        default=1000,
        help="Print progress every N rounds while iterating updates (0 disables).",
    )
    args = ap.parse_args()

    if not args.rpc:
        print("ERROR: Provide --rpc or set env ALCHEMY_RPC_URL", file=sys.stderr)
        return 2

    start_dt = parse_utc(args.start)
    start_ts = int(start_dt.timestamp())

    w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        print("ERROR: Could not connect to RPC", file=sys.stderr)
        return 2

    feed_addr = Web3.to_checksum_address(args.feed)
    c = w3.eth.contract(address=feed_addr, abi=ABI)

    decimals = int(c.functions.decimals().call())
    scale = 10**decimals

    latest = c.functions.latestRoundData().call()
    latest_u80 = int(latest[0])
    latest_phase, latest_r = u80_phase_round(latest_u80)

    print(f"Feed: {feed_addr}")
    print(f"Decimals: {decimals}")
    print(f"Latest roundId(u80): {latest_u80}  (phase={latest_phase}, r={latest_r})")
    print(f"Start cutoff (UTC): {start_dt.isoformat()}")

    # --- Helpers for RPC calls ---
    call_count = 0

    def maybe_sleep():
        nonlocal call_count
        if args.sleep_every > 0 and call_count > 0 and (call_count % args.sleep_every) == 0:
            time.sleep(args.sleep_seconds)

    def get_round(u80_round_id: int):
        """Return (answer_int, updatedAt_int) or None if incomplete/invalid."""
        nonlocal call_count
        max_attempts = max(1, int(args.rpc_max_attempts))
        base_backoff = max(0.0, float(args.rpc_base_backoff_seconds))
        jitter_ratio = max(0.0, float(args.rpc_jitter_ratio))

        for attempt in range(1, max_attempts + 1):
            if args.max_calls and call_count >= args.max_calls:
                raise RuntimeError("Reached --max_calls safety cap")
            call_count += 1
            maybe_sleep()
            try:
                r = c.functions.getRoundData(u80_round_id).call()
                updated_at = int(r[3])
                if updated_at == 0:
                    return None
                ans = int(r[1])
                return ans, updated_at
            except Exception as exc:
                if is_no_data_error(exc):
                    return None
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"RPC getRoundData failed for roundId={u80_round_id} after {max_attempts} attempts: {exc}"
                    ) from exc
                wait = base_backoff * (2 ** max(0, attempt - 1))
                if jitter_ratio > 0 and wait > 0:
                    wait += random.uniform(0, wait * jitter_ratio)
                if wait > 0:
                    time.sleep(wait)
        return None

    # --- Find earliest round within the latest phase (binary search) ---
    # We assume r indices in a phase start near 1, but we don't rely on it.
    # Find the first valid r in [1, latest_r].
    lo, hi = 1, latest_r
    first_valid_r = None
    while lo <= hi:
        mid = (lo + hi) // 2
        u80 = make_u80_round_id(latest_phase, mid)
        got = get_round(u80)
        if got is None:
            lo = mid + 1
        else:
            first_valid_r = mid
            hi = mid - 1

    if first_valid_r is None:
        print("ERROR: Could not find any valid round data in latest phase.", file=sys.stderr)
        return 2

    # Check if cutoff is earlier than earliest timestamp in this phase.
    earliest = get_round(make_u80_round_id(latest_phase, first_valid_r))
    assert earliest is not None
    earliest_ts = earliest[1]

    if start_ts < earliest_ts:
        # For Dec 2025 -> now, this *may* still be ok, but if the feed was upgraded,
        # cutoff could be in a previous phase. Robust multi-phase crawling requires
        # proxy internals (phaseAggregators). To keep this script "minimal setup",
        # we bail with an actionable message.
        print(
            "\nWARNING: Your requested start date is earlier than the earliest round in the current phase.\n"
            "This likely means the feed was upgraded and older rounds are in a previous phase.\n"
            "If you hit this, tell me and I’ll give you the multi-phase version that queries prior phase aggregators.\n",
            file=sys.stderr,
        )
        print(
            f"Earliest in latest phase: {datetime.fromtimestamp(earliest_ts, tz=timezone.utc).isoformat()}",
            file=sys.stderr,
        )
        return 3

    # --- Find the first round with updatedAt >= start_ts (binary search) ---
    lo, hi = first_valid_r, latest_r
    start_r = latest_r
    while lo <= hi:
        mid = (lo + hi) // 2
        got = get_round(make_u80_round_id(latest_phase, mid))
        if got is None:
            # treat as missing/incomplete -> search higher
            lo = mid + 1
            continue
        _, ts = got
        if ts >= start_ts:
            start_r = mid
            hi = mid - 1
        else:
            lo = mid + 1

    print(f"RPC calls used (so far): {call_count}")
    print(f"Starting from r={start_r} (phase={latest_phase})")

    # --- Iterate forward collecting all updates from start_r -> latest_r ---
    updates = []
    total_rounds = (latest_r - start_r) + 1
    progress_every = max(0, int(args.progress_every))
    for i, r in enumerate(range(start_r, latest_r + 1), start=1):
        got = get_round(make_u80_round_id(latest_phase, r))
        if got is None:
            if progress_every > 0 and (i % progress_every == 0 or i == total_rounds):
                print(f"[{i}/{total_rounds}] updates_collected={len(updates)} rpc_calls={call_count}")
            continue
        ans, ts = got
        if ts < start_ts:
            if progress_every > 0 and (i % progress_every == 0 or i == total_rounds):
                print(f"[{i}/{total_rounds}] updates_collected={len(updates)} rpc_calls={call_count}")
            continue
        updates.append((ts, ans))
        if progress_every > 0 and (i % progress_every == 0 or i == total_rounds):
            print(f"[{i}/{total_rounds}] updates_collected={len(updates)} rpc_calls={call_count}")

    if not updates:
        print("ERROR: No updates found after start date (unexpected).", file=sys.stderr)
        return 2

    # De-dup if multiple rounds share same updatedAt (rare, but can happen)
    updates.sort(key=lambda x: x[0])
    # keep last answer per timestamp
    dedup = {}
    for ts, ans in updates:
        dedup[ts] = ans
    updates = sorted(dedup.items(), key=lambda x: x[0])

    df_u = pd.DataFrame(updates, columns=["updatedAt", "answer_int"])
    df_u["datetime_utc"] = pd.to_datetime(df_u["updatedAt"], unit="s", utc=True)
    df_u["price"] = df_u["answer_int"] / scale
    df_u = df_u[["datetime_utc", "updatedAt", "price"]]

    df_u.to_csv(args.updates_csv, index=False)
    print(f"Wrote raw updates: {args.updates_csv}  (rows={len(df_u)})")

    # --- Resample to exact 15-minute timestamps using last update at or before t ---
    start_floor = floor_to_interval(start_dt, args.interval)
    end_dt = datetime.fromtimestamp(int(df_u["updatedAt"].iloc[-1]), tz=timezone.utc)
    end_floor = floor_to_interval(end_dt, args.interval)

    timeline = pd.date_range(start=start_floor, end=end_floor, freq=f"{args.interval}s", tz="UTC")
    df_t = pd.DataFrame({"datetime_utc": timeline})
    # Use timestamp() for robust epoch seconds across pandas backends (ns/us).
    df_t["t_ts"] = df_t["datetime_utc"].map(lambda dt: int(dt.timestamp())).astype("int64")

    # merge_asof requires sorted keys
    df_u2 = df_u.sort_values("updatedAt").rename(columns={"updatedAt": "u_ts"})
    df_t2 = df_t.sort_values("t_ts")

    df_b = pd.merge_asof(
        df_t2,
        df_u2[["u_ts", "price"]],
        left_on="t_ts",
        right_on="u_ts",
        direction="backward",
        allow_exact_matches=True,
    )

    # If the very first bucket predates the first update, price will be NaN; that’s expected.
    df_b = df_b[["datetime_utc", "price"]]
    df_b.to_csv(args.bars_csv, index=False)
    print(f"Wrote 15m series: {args.bars_csv}  (rows={len(df_b)})")

    print(f"Total RPC calls: {call_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
