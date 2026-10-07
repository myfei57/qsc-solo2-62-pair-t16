"""Time window projections for capacity limited defrost scheduling.

Two policy choices are fixed here on purpose and recorded on every plan so the
audit trail can show what rules a revision was built under:

* rescheduling is *rolling*: committed slots inside a short freeze window are
  carried over untouched and only the tail of the plan is recomputed;
* adjacent defrosts of one room are *serial*: a room may not start again until
  its previous defrost and its warm back (recovery) interval are over.
"""

from __future__ import annotations

from dataclasses import dataclass

POLICY_RESCHEDULE = "rolling"
POLICY_ADJACENCY = "serial"

DEFAULT_CONCURRENCY = 2
DEFAULT_RECOVERY_SECONDS = 600
DEFAULT_RESPONSE_SECONDS = 3600
DEFAULT_FREEZE_SECONDS = 300
DEFAULT_TOLERANCE_SECONDS = 300
DEFAULT_HORIZON_SECONDS = 24 * 3600

CONCURRENCY_KEY = "schedule:cfg:concurrency"
RECOVERY_KEY = "schedule:cfg:recovery-seconds"
RESPONSE_KEY = "schedule:cfg:response-seconds"
FREEZE_KEY = "schedule:cfg:freeze-seconds"
TOLERANCE_KEY = "schedule:cfg:tolerance-seconds"
HORIZON_KEY = "schedule:cfg:horizon-seconds"

MAX_JOBS = 200
MAX_HISTORY = 50

REASON_WINDOW = "window-open"
REASON_CAPACITY = "capacity-wait"
REASON_RECOVERY = "room-recovery-wait"
REASON_FROZEN = "frozen-carried"

TRIGGER_INITIAL = "initial"
TRIGGER_MANUAL = "manual"
TRIGGER_TEMP = "rolling-temp"
TRIGGER_COMPLETE = "rolling-complete"
TRIGGER_INSERT = "rolling-insert"


@dataclass(frozen=True)
class DefrostJob:
    """One room waiting for a defrost slot inside its time window.

    ``ready_at`` opens the window (earliest start) and ``due_at`` is the latest
    acceptable start, derived from the time the room can wait before warming
    back is compromised.
    """

    room_id: str
    temp: float
    ready_at: int
    due_at: int
    defrost_seconds: int
    recovery_seconds: int

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "temp": self.temp,
            "ready_at": self.ready_at,
            "due_at": self.due_at,
            "defrost_seconds": self.defrost_seconds,
            "recovery_seconds": self.recovery_seconds,
        }


@dataclass(frozen=True)
class ScheduledSlot:
    """A defrost placed on the timeline with the constraint that bound it."""

    room_id: str
    ready_at: int
    due_at: int
    start_at: int
    end_at: int
    recovered_at: int
    defrost_seconds: int
    recovery_seconds: int
    late: bool
    frozen: bool
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "ready_at": self.ready_at,
            "due_at": self.due_at,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "recovered_at": self.recovered_at,
            "defrost_seconds": self.defrost_seconds,
            "recovery_seconds": self.recovery_seconds,
            "late": self.late,
            "frozen": self.frozen,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ScheduledSlot":
        return cls(
            room_id=str(payload["room_id"]),
            ready_at=int(payload["ready_at"]),
            due_at=int(payload["due_at"]),
            start_at=int(payload["start_at"]),
            end_at=int(payload["end_at"]),
            recovered_at=int(payload["recovered_at"]),
            defrost_seconds=int(payload["defrost_seconds"]),
            recovery_seconds=int(payload["recovery_seconds"]),
            late=bool(payload.get("late", False)),
            frozen=bool(payload.get("frozen", False)),
            reason=str(payload.get("reason", "")),
        )


@dataclass(frozen=True)
class TimelinePlan:
    """One immutable revision of the time windowed schedule."""

    revision: int
    generated_at: int
    trigger: str
    reason: str
    policy_reschedule: str
    policy_adjacency: str
    concurrency: int
    frozen_until: int
    horizon_end: int
    consistent: bool
    violations: list[str]
    slots: list[ScheduledSlot]

    def as_dict(self) -> dict[str, object]:
        return {
            "revision": self.revision,
            "generated_at": self.generated_at,
            "trigger": self.trigger,
            "reason": self.reason,
            "policy_reschedule": self.policy_reschedule,
            "policy_adjacency": self.policy_adjacency,
            "concurrency": self.concurrency,
            "frozen_until": self.frozen_until,
            "horizon_end": self.horizon_end,
            "consistent": self.consistent,
            "violations": list(self.violations),
            "slots": [slot.as_dict() for slot in self.slots],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "TimelinePlan":
        raw_slots = payload.get("slots", [])
        slots = [
            ScheduledSlot.from_dict(item)
            for item in raw_slots  # type: ignore[union-attr]
            if isinstance(item, dict)
        ]
        raw_violations = payload.get("violations", [])
        violations = [str(item) for item in raw_violations] if isinstance(raw_violations, list) else []
        return cls(
            revision=int(payload.get("revision", 0)),
            generated_at=int(payload.get("generated_at", 0)),
            trigger=str(payload.get("trigger", "")),
            reason=str(payload.get("reason", "")),
            policy_reschedule=str(payload.get("policy_reschedule", POLICY_RESCHEDULE)),
            policy_adjacency=str(payload.get("policy_adjacency", POLICY_ADJACENCY)),
            concurrency=int(payload.get("concurrency", DEFAULT_CONCURRENCY)),
            frozen_until=int(payload.get("frozen_until", 0)),
            horizon_end=int(payload.get("horizon_end", 0)),
            consistent=bool(payload.get("consistent", False)),
            violations=violations,
            slots=slots,
        )

    def slot_for(self, room_id: str) -> ScheduledSlot | None:
        for slot in self.slots:
            if slot.room_id == room_id:
                return slot
        return None


def validate_concurrency(value: int) -> None:
    """Reject concurrency settings the capacity lanes cannot represent."""

    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("concurrency must be an integer")
    if value < 1:
        raise ValueError("concurrency must be at least one")
    if value > 64:
        raise ValueError("concurrency must not exceed 64")


def _validate_seconds(name: str, value: int, *, allow_zero: bool = False) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer number of seconds")
    if value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"{name} must be positive seconds")
    if value > 7 * 24 * 3600:
        raise ValueError(f"{name} must not exceed one week")


def validate_recovery_seconds(value: int) -> None:
    """Warm back waiting may be disabled (zero) but must stay bounded."""

    _validate_seconds("recovery seconds", value, allow_zero=True)


def validate_response_seconds(value: int) -> None:
    _validate_seconds("response seconds", value)


def validate_freeze_seconds(value: int) -> None:
    _validate_seconds("freeze seconds", value, allow_zero=True)


def validate_tolerance_seconds(value: int) -> None:
    _validate_seconds("start tolerance seconds", value)


def validate_horizon_seconds(value: int) -> None:
    _validate_seconds("horizon seconds", value)


def validate_trigger(trigger: str) -> str:
    """Normalise the reason recorded for why a revision was generated."""

    text = (trigger or "").strip()
    if not text:
        raise ValueError("trigger is required")
    if len(text) > 40:
        raise ValueError("trigger must not exceed 40 characters")
    return text


def queue_order(job: DefrostJob) -> tuple[int, float, str]:
    """Earliest due (recovery) time first, warmest room as the tie breaker."""

    return job.due_at, -job.temp, job.room_id
