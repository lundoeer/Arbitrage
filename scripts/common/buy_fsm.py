"""Buy execution Finite State Machine (FSM) for the arbitrage engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

from scripts.common.utils import as_dict, now_ms


class BuyFsmState(str, Enum):
    IDLE = "IDLE"
    SUBMITTING = "SUBMITTING"
    AWAITING_RESULT = "AWAITING_RESULT"
    COOLDOWN = "COOLDOWN"


@dataclass
class BuyFsmRuntime:
    state: BuyFsmState = BuyFsmState.IDLE
    in_flight_signal_id: Optional[str] = None
    in_flight_started_at_ms: Optional[int] = None
    cooldown_until_ms: Optional[int] = None
    last_transition_at_ms: int = 0
    last_transition_reason: str = "engine_start"
    last_execution_result: Optional[Dict[str, Any]] = None
    submitted_signals: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @staticmethod
    def initialize(now_epoch_ms: Optional[int] = None) -> "BuyFsmRuntime":
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        return BuyFsmRuntime(last_transition_at_ms=ts)

    def transition(self, *, state: BuyFsmState, now_epoch_ms: int, reason: str) -> None:
        self.state = state
        self.last_transition_at_ms = int(now_epoch_ms)
        self.last_transition_reason = str(reason)

    def _clear_in_flight(self) -> None:
        self.in_flight_signal_id = None
        self.in_flight_started_at_ms = None

    def begin_submission(self, *, signal_id: str, now_epoch_ms: int, reason: str) -> None:
        self.in_flight_signal_id = str(signal_id)
        self.in_flight_started_at_ms = int(now_epoch_ms)
        self.cooldown_until_ms = None
        self.transition(state=BuyFsmState.SUBMITTING, now_epoch_ms=now_epoch_ms, reason=reason)

    def complete_submission(
        self,
        *,
        result_payload: Dict[str, Any],
        now_epoch_ms: int,
        cooldown_ms: int,
    ) -> None:
        signal_id = str(
            self.in_flight_signal_id
            or as_dict(result_payload).get("signal_id")
            or ""
        ).strip()
        status = str(as_dict(result_payload).get("status") or "unknown")
        self.last_execution_result = dict(result_payload)
        if signal_id:
            self.submitted_signals[signal_id] = {
                "status": status,
                "updated_at_ms": int(now_epoch_ms),
            }
        self.transition(
            state=BuyFsmState.AWAITING_RESULT,
            now_epoch_ms=now_epoch_ms,
            reason=f"submit_completed:{status}",
        )
        cool_ms = max(0, int(cooldown_ms))
        self._clear_in_flight()
        if cool_ms > 0:
            self.cooldown_until_ms = int(now_epoch_ms + cool_ms)
            self.transition(
                state=BuyFsmState.COOLDOWN,
                now_epoch_ms=now_epoch_ms,
                reason=f"cooldown:{cool_ms}ms",
            )
            return
        self.cooldown_until_ms = None
        self.transition(state=BuyFsmState.IDLE, now_epoch_ms=now_epoch_ms, reason="rearmed_immediate")

    def fail_submission(
        self,
        *,
        signal_id: Optional[str],
        error_payload: Dict[str, Any],
        now_epoch_ms: int,
        reason: str,
    ) -> None:
        signal = str(signal_id or self.in_flight_signal_id or "").strip()
        self.last_execution_result = dict(error_payload)
        if signal:
            self.submitted_signals[signal] = {
                "status": "error",
                "updated_at_ms": int(now_epoch_ms),
            }
        self.cooldown_until_ms = None
        self._clear_in_flight()
        self.transition(state=BuyFsmState.IDLE, now_epoch_ms=now_epoch_ms, reason=reason)

    def maybe_rearm(self, *, now_epoch_ms: int) -> None:
        if self.state != BuyFsmState.COOLDOWN:
            return
        until = self.cooldown_until_ms
        if until is None:
            self.transition(state=BuyFsmState.IDLE, now_epoch_ms=now_epoch_ms, reason="cooldown_missing_until")
            return
        if int(now_epoch_ms) >= int(until):
            self.cooldown_until_ms = None
            self.transition(state=BuyFsmState.IDLE, now_epoch_ms=now_epoch_ms, reason="cooldown_elapsed")

    def can_accept_new_signal(self) -> bool:
        return self.state == BuyFsmState.IDLE

    def snapshot(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "can_accept_new_signal": self.can_accept_new_signal(),
            "in_flight_signal_id": self.in_flight_signal_id,
            "in_flight_started_at_ms": self.in_flight_started_at_ms,
            "cooldown_until_ms": self.cooldown_until_ms,
            "last_transition_at_ms": self.last_transition_at_ms,
            "last_transition_reason": self.last_transition_reason,
            "last_execution_result": self.last_execution_result,
            "submitted_signal_count": len(self.submitted_signals),
        }
