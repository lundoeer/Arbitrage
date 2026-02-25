from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from scripts.common.utils import as_float, as_int, now_ms


class ExecutionLockState(str, Enum):
    UNLOCKED = "UNLOCKED"
    LOCKED_AWAITING_TERMINAL = "LOCKED_AWAITING_TERMINAL"
    LOCKED_AWAITING_RECONCILE = "LOCKED_AWAITING_RECONCILE"


def _normalize_venue(value: Any) -> Optional[str]:
    venue = str(value or "").strip().lower()
    if venue in {"polymarket", "kalshi"}:
        return venue
    return None


def _normalize_venue_list(values: List[Any]) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for raw in values:
        venue = _normalize_venue(raw)
        if venue is None or venue in seen:
            continue
        normalized.append(venue)
        seen.add(venue)
    return normalized


@dataclass
class ExecutionLockContext:
    lock_id: str
    action_type: str
    created_at_ms: int
    execution_completed_at_ms: Optional[int] = None
    attempted_venues: List[str] = field(default_factory=list)
    submitted_venues: List[str] = field(default_factory=list)
    required_terminal_venues: List[str] = field(default_factory=list)
    required_reconcile_venues: List[str] = field(default_factory=list)
    terminal_satisfied_venues: Set[str] = field(default_factory=set)
    reconcile_satisfied_venues: Set[str] = field(default_factory=set)
    last_result_status: Optional[str] = None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "lock_id": self.lock_id,
            "action_type": self.action_type,
            "created_at_ms": int(self.created_at_ms),
            "execution_completed_at_ms": (
                None
                if self.execution_completed_at_ms is None
                else int(self.execution_completed_at_ms)
            ),
            "attempted_venues": list(self.attempted_venues),
            "submitted_venues": list(self.submitted_venues),
            "required_terminal_venues": list(self.required_terminal_venues),
            "required_reconcile_venues": list(self.required_reconcile_venues),
            "terminal_satisfied_venues": sorted(self.terminal_satisfied_venues),
            "reconcile_satisfied_venues": sorted(self.reconcile_satisfied_venues),
            "last_result_status": self.last_result_status,
        }


@dataclass
class ExecutionLockRuntime:
    state: ExecutionLockState = ExecutionLockState.UNLOCKED
    active_lock: Optional[ExecutionLockContext] = None
    transition_count: int = 0
    last_transition_at_ms: int = 0
    last_transition_reason: str = "engine_start"
    last_released_lock: Optional[Dict[str, Any]] = None

    @staticmethod
    def initialize(now_epoch_ms: Optional[int] = None) -> "ExecutionLockRuntime":
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        return ExecutionLockRuntime(last_transition_at_ms=ts)

    def _transition(self, *, state: ExecutionLockState, reason: str, now_epoch_ms: int) -> None:
        self.state = state
        self.last_transition_at_ms = int(now_epoch_ms)
        self.last_transition_reason = str(reason)
        self.transition_count += 1

    def _is_terminal_satisfied(self, lock: ExecutionLockContext) -> bool:
        required = set(lock.required_terminal_venues)
        return required.issubset(set(lock.terminal_satisfied_venues))

    def _is_reconcile_satisfied(self, lock: ExecutionLockContext) -> bool:
        required = set(lock.required_reconcile_venues)
        return required.issubset(set(lock.reconcile_satisfied_venues))

    def _unlock(self, *, reason: str, now_epoch_ms: int) -> Dict[str, Any]:
        released = None if self.active_lock is None else self.active_lock.snapshot()
        if released is not None:
            released["released_at_ms"] = int(now_epoch_ms)
        self.last_released_lock = released
        self.active_lock = None
        self._transition(
            state=ExecutionLockState.UNLOCKED,
            reason=reason,
            now_epoch_ms=now_epoch_ms,
        )
        return {
            "changed": True,
            "transitioned": True,
            "state": self.state.value,
            "reason": self.last_transition_reason,
        }

    def can_start_execution(self) -> Dict[str, Any]:
        if self.state == ExecutionLockState.UNLOCKED:
            return {"allowed": True, "state": self.state.value, "reason": "lock_open"}
        lock_id = None if self.active_lock is None else self.active_lock.lock_id
        return {
            "allowed": False,
            "state": self.state.value,
            "reason": f"execution_lock_active:{self.state.value}",
            "lock_id": lock_id,
        }

    def begin_execution(
        self,
        *,
        action_type: str,
        attempted_venues: List[Any],
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        gate = self.can_start_execution()
        if not bool(gate.get("allowed")):
            return {
                "accepted": False,
                "changed": False,
                "transitioned": False,
                "reason": str(gate.get("reason") or "lock_closed"),
                "state": self.state.value,
            }
        normalized_attempted = _normalize_venue_list(list(attempted_venues or []))
        if not normalized_attempted:
            return {
                "accepted": False,
                "changed": False,
                "transitioned": False,
                "reason": "execution_lock_missing_attempted_venues",
                "state": self.state.value,
            }
        lock_id = f"lock_{ts}_{self.transition_count + 1}"
        action = str(action_type or "").strip().lower() or "unknown"
        self.active_lock = ExecutionLockContext(
            lock_id=lock_id,
            action_type=action,
            created_at_ms=ts,
            attempted_venues=normalized_attempted,
        )
        self._transition(
            state=ExecutionLockState.LOCKED_AWAITING_TERMINAL,
            reason=f"begin_execution:{action}",
            now_epoch_ms=ts,
        )
        return {
            "accepted": True,
            "changed": True,
            "transitioned": True,
            "state": self.state.value,
            "reason": self.last_transition_reason,
            "lock_id": lock_id,
        }

    def record_execution_result(
        self,
        *,
        result_payload: Dict[str, Any],
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        if self.active_lock is None:
            return {
                "accepted": False,
                "changed": False,
                "transitioned": False,
                "reason": "execution_lock_no_active_lock",
                "state": self.state.value,
            }
        lock = self.active_lock
        legs_raw = result_payload.get("legs")
        legs = list(legs_raw) if isinstance(legs_raw, list) else []
        attempted = _normalize_venue_list([dict(leg or {}).get("venue") for leg in legs])
        if attempted:
            lock.attempted_venues = attempted
        submitted = _normalize_venue_list(
            [
                dict(leg or {}).get("venue")
                for leg in legs
                if bool(dict(leg or {}).get("submitted"))
            ]
        )
        lock.submitted_venues = submitted
        required = list(submitted) if submitted else list(lock.attempted_venues)
        lock.required_terminal_venues = list(required)
        lock.required_reconcile_venues = list(required)
        lock.terminal_satisfied_venues = set()
        lock.reconcile_satisfied_venues = set()
        completed_at = as_int(result_payload.get("completed_at_ms"))
        lock.execution_completed_at_ms = int(completed_at if completed_at is not None else ts)
        lock.last_result_status = str(result_payload.get("status") or "").strip().lower() or None

        transitioned = False
        if self.state != ExecutionLockState.LOCKED_AWAITING_TERMINAL:
            self._transition(
                state=ExecutionLockState.LOCKED_AWAITING_TERMINAL,
                reason="execution_result_recorded",
                now_epoch_ms=ts,
            )
            transitioned = True
        if not required:
            unlocked = self._unlock(reason="unlock_no_required_venues", now_epoch_ms=ts)
            return {
                "accepted": True,
                "changed": True,
                "transitioned": bool(unlocked.get("transitioned")),
                "state": self.state.value,
                "reason": str(unlocked.get("reason") or "unlock_no_required_venues"),
            }
        return {
            "accepted": True,
            "changed": True,
            "transitioned": transitioned,
            "state": self.state.value,
            "reason": self.last_transition_reason,
            "required_venues": list(required),
        }

    def refresh_terminal_status(
        self,
        *,
        has_open_orders_for_venue: Callable[[str], bool],
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        if self.active_lock is None:
            return {
                "changed": False,
                "transitioned": False,
                "state": self.state.value,
                "reason": "no_active_lock",
            }
        lock = self.active_lock
        if lock.execution_completed_at_ms is None or not lock.required_terminal_venues:
            return {
                "changed": False,
                "transitioned": False,
                "state": self.state.value,
                "reason": "execution_result_pending",
            }
        changed = False
        for venue in list(lock.required_terminal_venues):
            if venue in lock.terminal_satisfied_venues:
                continue
            if not bool(has_open_orders_for_venue(venue)):
                lock.terminal_satisfied_venues.add(venue)
                changed = True

        transitioned = False
        if self._is_terminal_satisfied(lock):
            if self.state != ExecutionLockState.LOCKED_AWAITING_RECONCILE:
                self._transition(
                    state=ExecutionLockState.LOCKED_AWAITING_RECONCILE,
                    reason="terminal_satisfied",
                    now_epoch_ms=ts,
                )
                transitioned = True
            if self._is_reconcile_satisfied(lock):
                unlocked = self._unlock(reason="reconcile_satisfied", now_epoch_ms=ts)
                transitioned = bool(unlocked.get("transitioned")) or transitioned
                changed = True
        return {
            "changed": bool(changed or transitioned),
            "transitioned": bool(transitioned),
            "state": self.state.value,
            "reason": self.last_transition_reason,
        }

    def mark_positions_reconcile_success(
        self,
        *,
        venue: str,
        poll_at_ms: int,
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        if self.active_lock is None:
            return {
                "changed": False,
                "transitioned": False,
                "state": self.state.value,
                "reason": "no_active_lock",
            }
        lock = self.active_lock
        venue_norm = _normalize_venue(venue)
        if venue_norm is None or venue_norm not in set(lock.required_reconcile_venues):
            return {
                "changed": False,
                "transitioned": False,
                "state": self.state.value,
                "reason": "venue_not_required",
            }
        completed_at_ms = lock.execution_completed_at_ms
        if completed_at_ms is None:
            return {
                "changed": False,
                "transitioned": False,
                "state": self.state.value,
                "reason": "execution_completion_unknown",
            }
        poll_ts = int(as_float(poll_at_ms) or 0)
        if poll_ts < int(completed_at_ms):
            return {
                "changed": False,
                "transitioned": False,
                "state": self.state.value,
                "reason": "reconcile_before_execution_completed",
            }
        if venue_norm in lock.reconcile_satisfied_venues:
            return {
                "changed": False,
                "transitioned": False,
                "state": self.state.value,
                "reason": "reconcile_already_recorded",
            }
        lock.reconcile_satisfied_venues.add(venue_norm)

        transitioned = False
        if (
            self.state == ExecutionLockState.LOCKED_AWAITING_RECONCILE
            and self._is_terminal_satisfied(lock)
            and self._is_reconcile_satisfied(lock)
        ):
            unlocked = self._unlock(reason="reconcile_satisfied", now_epoch_ms=ts)
            transitioned = bool(unlocked.get("transitioned"))
        return {
            "changed": True,
            "transitioned": transitioned,
            "state": self.state.value,
            "reason": self.last_transition_reason,
            "venue": venue_norm,
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "can_start_execution": bool(self.state == ExecutionLockState.UNLOCKED),
            "transition_count": int(self.transition_count),
            "last_transition_at_ms": int(self.last_transition_at_ms),
            "last_transition_reason": self.last_transition_reason,
            "active_lock": None if self.active_lock is None else self.active_lock.snapshot(),
            "last_released_lock": self.last_released_lock,
        }
