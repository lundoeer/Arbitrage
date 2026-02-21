from __future__ import annotations

from scripts.common.buy_fsm import BuyFsmRuntime, BuyFsmState


def test_buy_fsm_cooldown_blocks_and_then_rearms() -> None:
    fsm = BuyFsmRuntime.initialize(now_epoch_ms=1000)
    assert fsm.state == BuyFsmState.IDLE
    assert fsm.can_accept_new_signal()

    fsm.begin_submission(signal_id="sig-1", now_epoch_ms=1100, reason="decision_can_trade")
    assert fsm.state == BuyFsmState.SUBMITTING
    assert not fsm.can_accept_new_signal()
    assert fsm.in_flight_signal_id == "sig-1"

    fsm.complete_submission(
        result_payload={"signal_id": "sig-1", "status": "submitted"},
        now_epoch_ms=1200,
        cooldown_ms=250,
    )
    assert fsm.state == BuyFsmState.COOLDOWN
    assert not fsm.can_accept_new_signal()
    assert fsm.in_flight_signal_id is None
    assert fsm.cooldown_until_ms == 1450

    fsm.maybe_rearm(now_epoch_ms=1449)
    assert fsm.state == BuyFsmState.COOLDOWN
    assert not fsm.can_accept_new_signal()

    fsm.maybe_rearm(now_epoch_ms=1450)
    assert fsm.state == BuyFsmState.IDLE
    assert fsm.can_accept_new_signal()
    assert fsm.cooldown_until_ms is None


def test_buy_fsm_fail_submission_returns_to_idle() -> None:
    fsm = BuyFsmRuntime.initialize(now_epoch_ms=2000)
    fsm.begin_submission(signal_id="sig-err", now_epoch_ms=2100, reason="decision_can_trade")
    fsm.fail_submission(
        signal_id="sig-err",
        error_payload={"signal_id": "sig-err", "status": "error", "error": "boom"},
        now_epoch_ms=2150,
        reason="submit_exception",
    )
    assert fsm.state == BuyFsmState.IDLE
    assert fsm.can_accept_new_signal()
    assert fsm.in_flight_signal_id is None
    assert fsm.submitted_signals["sig-err"]["status"] == "error"
