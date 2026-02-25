from __future__ import annotations

from scripts.common.execution_lock import ExecutionLockRuntime


def test_execution_lock_full_flow_two_submitted_venues_unlocks_after_terminal_and_reconcile() -> None:
    runtime = ExecutionLockRuntime.initialize(now_epoch_ms=1_000)
    assert runtime.can_start_execution()["allowed"] is True

    begin = runtime.begin_execution(
        action_type="buy",
        attempted_venues=["polymarket", "kalshi"],
        now_epoch_ms=1_100,
    )
    assert begin["accepted"] is True
    assert runtime.can_start_execution()["allowed"] is False

    pending = runtime.refresh_terminal_status(
        has_open_orders_for_venue=lambda _venue: False,
        now_epoch_ms=1_150,
    )
    assert pending["reason"] == "execution_result_pending"

    record = runtime.record_execution_result(
        result_payload={
            "status": "submitted",
            "completed_at_ms": 1_200,
            "legs": [
                {"venue": "polymarket", "submitted": True},
                {"venue": "kalshi", "submitted": True},
            ],
        },
        now_epoch_ms=1_210,
    )
    assert record["accepted"] is True

    runtime.refresh_terminal_status(
        has_open_orders_for_venue=lambda _venue: True,
        now_epoch_ms=1_220,
    )
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_TERMINAL"

    runtime.refresh_terminal_status(
        has_open_orders_for_venue=lambda venue: venue != "kalshi",
        now_epoch_ms=1_230,
    )
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_TERMINAL"

    runtime.refresh_terminal_status(
        has_open_orders_for_venue=lambda _venue: False,
        now_epoch_ms=1_240,
    )
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_RECONCILE"

    runtime.mark_positions_reconcile_success(
        venue="polymarket",
        poll_at_ms=1_250,
        now_epoch_ms=1_251,
    )
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_RECONCILE"

    runtime.mark_positions_reconcile_success(
        venue="kalshi",
        poll_at_ms=1_260,
        now_epoch_ms=1_261,
    )
    assert runtime.snapshot()["state"] == "UNLOCKED"
    assert runtime.can_start_execution()["allowed"] is True


def test_execution_lock_partial_submit_requires_only_submitted_venue() -> None:
    runtime = ExecutionLockRuntime.initialize(now_epoch_ms=2_000)
    runtime.begin_execution(
        action_type="buy",
        attempted_venues=["polymarket", "kalshi"],
        now_epoch_ms=2_010,
    )
    runtime.record_execution_result(
        result_payload={
            "status": "partially_submitted",
            "completed_at_ms": 2_020,
            "legs": [
                {"venue": "polymarket", "submitted": True},
                {"venue": "kalshi", "submitted": False},
            ],
        },
        now_epoch_ms=2_021,
    )

    active = runtime.snapshot()["active_lock"]
    assert active is not None
    assert active["required_terminal_venues"] == ["polymarket"]
    assert active["required_reconcile_venues"] == ["polymarket"]

    runtime.refresh_terminal_status(
        has_open_orders_for_venue=lambda _venue: False,
        now_epoch_ms=2_030,
    )
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_RECONCILE"

    runtime.mark_positions_reconcile_success(
        venue="polymarket",
        poll_at_ms=2_040,
        now_epoch_ms=2_041,
    )
    assert runtime.snapshot()["state"] == "UNLOCKED"


def test_execution_lock_no_submit_requires_attempted_venues_and_reconcile_after_completion() -> None:
    runtime = ExecutionLockRuntime.initialize(now_epoch_ms=3_000)
    runtime.begin_execution(
        action_type="buy",
        attempted_venues=["polymarket", "kalshi"],
        now_epoch_ms=3_010,
    )
    runtime.record_execution_result(
        result_payload={
            "status": "rejected",
            "completed_at_ms": 3_020,
            "legs": [
                {"venue": "polymarket", "submitted": False},
                {"venue": "kalshi", "submitted": False},
            ],
        },
        now_epoch_ms=3_021,
    )

    active = runtime.snapshot()["active_lock"]
    assert active is not None
    assert active["required_terminal_venues"] == ["polymarket", "kalshi"]
    assert active["required_reconcile_venues"] == ["polymarket", "kalshi"]

    runtime.refresh_terminal_status(
        has_open_orders_for_venue=lambda _venue: False,
        now_epoch_ms=3_030,
    )
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_RECONCILE"

    before = runtime.mark_positions_reconcile_success(
        venue="polymarket",
        poll_at_ms=3_019,
        now_epoch_ms=3_031,
    )
    assert before["reason"] == "reconcile_before_execution_completed"
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_RECONCILE"

    runtime.mark_positions_reconcile_success(
        venue="polymarket",
        poll_at_ms=3_040,
        now_epoch_ms=3_041,
    )
    assert runtime.snapshot()["state"] == "LOCKED_AWAITING_RECONCILE"

    runtime.mark_positions_reconcile_success(
        venue="kalshi",
        poll_at_ms=3_042,
        now_epoch_ms=3_043,
    )
    assert runtime.snapshot()["state"] == "UNLOCKED"
