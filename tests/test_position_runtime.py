from __future__ import annotations

from scripts.common.position_runtime import PositionRuntime, PositionRuntimeConfig


def _runtime(
    *,
    config: PositionRuntimeConfig | None = None,
    now_epoch_ms: int = 1_000,
) -> PositionRuntime:
    return PositionRuntime(
        polymarket_token_yes="pm_yes_token",
        polymarket_token_no="pm_no_token",
        kalshi_ticker="KXBTC15M-TEST",
        config=config,
        now_epoch_ms=now_epoch_ms,
    )


def test_apply_buy_execution_result_merges_submit_acks_without_fill_change() -> None:
    runtime = _runtime()

    payload = {
        "signal_id": "sig-1",
        "status": "submitted",
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "instrument_id": "pm_yes_token",
                "client_order_id": "cid-pm-1",
                "response_payload": {"response": {"id": "pm-order-1"}},
                "submitted": True,
                "error": None,
            },
            {
                "venue": "kalshi",
                "side": "no",
                "instrument_id": "KXBTC15M-TEST",
                "client_order_id": "cid-kx-1",
                "response_payload": {"response": {"order_id": "kx-order-1"}},
                "submitted": True,
                "error": None,
            },
        ],
    }
    accepted = runtime.apply_buy_execution_result(result_payload=payload, now_epoch_ms=1_500)
    assert accepted == 2
    assert len(runtime.orders_by_client_order_id) == 2
    assert runtime.orders_by_client_order_id["cid-pm-1"].order_id == "pm-order-1"
    assert runtime.orders_by_client_order_id["cid-kx-1"].order_id == "kx-order-1"
    assert runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")["net_contracts"] == 0.0
    assert runtime.position(venue="kalshi", instrument_id="KXBTC15M-TEST", outcome_side="no")["net_contracts"] == 0.0


def test_polymarket_confirmed_fill_is_deduped_by_event_id() -> None:
    runtime = _runtime()

    applied_first = runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-1",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=2.0,
        fill_price=0.44,
        now_epoch_ms=2_000,
    )
    applied_second = runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-1",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=2.0,
        fill_price=0.44,
        now_epoch_ms=2_100,
    )

    assert applied_first is True
    assert applied_second is False
    assert runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")["net_contracts"] == 2.0
    assert runtime.snapshot()["counters"]["deduped_events"] == 1


def test_reconcile_snapshot_corrects_drift_and_overwrites_position() -> None:
    runtime = _runtime()
    runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-2",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=3.0,
        fill_price=0.5,
        now_epoch_ms=3_000,
    )
    before = runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")
    assert before["net_contracts"] == 3.0

    result = runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 1.0, "avg_entry_price": 0.41},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0.0},
        ],
        snapshot_id="poll-1",
        now_epoch_ms=3_500,
    )

    assert result["applied"] == 2
    assert result["drift_corrections"] == 1
    after = runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")
    assert after["net_contracts"] == 1.0
    assert after["avg_entry_price"] == 0.41
    assert runtime.snapshot()["counters"]["drift_corrections"] == 1


def test_bootstrap_fail_closed_then_auto_reenable_on_successful_reconciliation() -> None:
    runtime = _runtime()
    assert runtime.snapshot(now_epoch_ms=1_000)["health"]["buy_execution_allowed"] is False

    runtime.mark_reconcile_failure(venue="polymarket", error="timeout", now_epoch_ms=4_000)
    assert runtime.snapshot(now_epoch_ms=4_000)["health"]["buy_execution_allowed"] is False

    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="poll-pm-success",
        now_epoch_ms=4_100,
    )
    assert runtime.snapshot(now_epoch_ms=4_100)["health"]["buy_execution_allowed"] is False

    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="poll-kx-success",
        now_epoch_ms=4_200,
    )
    assert runtime.snapshot(now_epoch_ms=4_200)["health"]["buy_execution_allowed"] is True


def test_hard_stale_blocks_buy_and_recovers_after_fresh_reconcile() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=True,
            stale_warning_seconds=1,
            stale_error_seconds=2,
        ),
        now_epoch_ms=10_000,
    )

    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="bootstrap-pm",
        now_epoch_ms=10_000,
    )
    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="bootstrap-kx",
        now_epoch_ms=10_000,
    )
    assert runtime.snapshot(now_epoch_ms=10_000)["health"]["buy_execution_allowed"] is True

    stale_health = runtime.snapshot(now_epoch_ms=12_200)["health"]
    assert stale_health["hard_stale"] is True
    assert stale_health["buy_execution_allowed"] is False

    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="recover-pm",
        now_epoch_ms=12_300,
    )
    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="recover-kx",
        now_epoch_ms=12_300,
    )
    recovered = runtime.snapshot(now_epoch_ms=12_300)["health"]
    assert recovered["hard_stale"] is False
    assert recovered["buy_execution_allowed"] is True
