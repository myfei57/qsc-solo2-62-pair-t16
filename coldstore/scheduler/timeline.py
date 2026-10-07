"""Rolling timeline persistence and actual execution bookkeeping.

Every generation of the schedule produces a new, immutable :class:`TimelinePlan`
revision.  Revisions are append only and kept for replay in the audit trail and
CSV exports.  Executions recorded by operators are compared against the revision
they were dispatched from so a missed window or an over capacity start is logged
as a deviation instead of silently drifting.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from coldstore.defrost.schedule import now_unix
from coldstore.store.commands import append_command, load_commands
from coldstore.store.epoch import load_float, save_float
from coldstore.store.store import Store

from .solver import schedule_jobs, verify_plan
from .window import (
    CONCURRENCY_KEY,
    DEFAULT_CONCURRENCY,
    DEFAULT_FREEZE_SECONDS,
    DEFAULT_HORIZON_SECONDS,
    DEFAULT_RECOVERY_SECONDS,
    DEFAULT_RESPONSE_SECONDS,
    DEFAULT_TOLERANCE_SECONDS,
    FREEZE_KEY,
    HORIZON_KEY,
    MAX_HISTORY,
    POLICY_ADJACENCY,
    POLICY_RESCHEDULE,
    REASON_FROZEN,
    RECOVERY_KEY,
    RESPONSE_KEY,
    TOLERANCE_KEY,
    DefrostJob,
    ScheduledSlot,
    TimelinePlan,
    validate_concurrency,
    validate_freeze_seconds,
    validate_horizon_seconds,
    validate_recovery_seconds,
    validate_response_seconds,
    validate_tolerance_seconds,
    validate_trigger,
)

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from coldstore.audit import Auditor

REVISION_KEY = "schedule:revision"
LATEST_PLAN_KEY = "schedule:plan:latest"
PLAN_HISTORY_KEY = "schedule:plan:history"
EXECUTION_KEY = "schedule:execution"

STATUS_PLANNED = "planned"
STATUS_RUNNING = "running"
STATUS_FINISHED = "finished"


@dataclass(frozen=True)
class ExecutionRecord:
    """What actually happened to one dispatched defrost."""

    room_id: str
    revision: int
    planned_start: int
    planned_end: int
    actual_start: int
    actual_finish: int
    status: str
    late: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "revision": self.revision,
            "planned_start": self.planned_start,
            "planned_end": self.planned_end,
            "actual_start": self.actual_start,
            "actual_finish": self.actual_finish,
            "status": self.status,
            "late": self.late,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ExecutionRecord":
        return cls(
            room_id=str(payload.get("room_id", "")),
            revision=int(payload.get("revision", 0)),
            planned_start=int(payload.get("planned_start", 0)),
            planned_end=int(payload.get("planned_end", 0)),
            actual_start=int(payload.get("actual_start", 0)),
            actual_finish=int(payload.get("actual_finish", 0)),
            status=str(payload.get("status", STATUS_PLANNED)),
            late=bool(payload.get("late", False)),
        )


def _load_int(store: Store, key: str, default: int) -> int:
    value, present = load_float(store, key)
    return int(value) if present else default


class TimelinePlanner:
    """Stores capacity parameters and owns revisioned plan generation."""

    def __init__(self, store: Store, auditor: "Auditor | None" = None) -> None:
        self._store = store
        self._auditor = auditor

    # -- configuration -----------------------------------------------------

    def concurrency(self) -> int:
        return _load_int(self._store, CONCURRENCY_KEY, DEFAULT_CONCURRENCY)

    def recovery_seconds(self) -> int:
        return _load_int(self._store, RECOVERY_KEY, DEFAULT_RECOVERY_SECONDS)

    def response_seconds(self) -> int:
        return _load_int(self._store, RESPONSE_KEY, DEFAULT_RESPONSE_SECONDS)

    def freeze_seconds(self) -> int:
        return _load_int(self._store, FREEZE_KEY, DEFAULT_FREEZE_SECONDS)

    def tolerance_seconds(self) -> int:
        return _load_int(self._store, TOLERANCE_KEY, DEFAULT_TOLERANCE_SECONDS)

    def horizon_seconds(self) -> int:
        return _load_int(self._store, HORIZON_KEY, DEFAULT_HORIZON_SECONDS)

    def set_concurrency(self, value: int) -> int:
        validate_concurrency(value)
        save_float(self._store, CONCURRENCY_KEY, float(value))
        return value

    def _set_seconds(self, key: str, value: int, validator) -> int:
        validator(value)
        save_float(self._store, key, float(value))
        return value

    def set_recovery_seconds(self, value: int) -> int:
        return self._set_seconds(RECOVERY_KEY, value, validate_recovery_seconds)

    def set_response_seconds(self, value: int) -> int:
        return self._set_seconds(RESPONSE_KEY, value, validate_response_seconds)

    def set_freeze_seconds(self, value: int) -> int:
        return self._set_seconds(FREEZE_KEY, value, validate_freeze_seconds)

    def set_tolerance_seconds(self, value: int) -> int:
        return self._set_seconds(TOLERANCE_KEY, value, validate_tolerance_seconds)

    def set_horizon_seconds(self, value: int) -> int:
        return self._set_seconds(HORIZON_KEY, value, validate_horizon_seconds)

    def config(self) -> dict[str, int]:
        return {
            "concurrency": self.concurrency(),
            "recovery_seconds": self.recovery_seconds(),
            "response_seconds": self.response_seconds(),
            "freeze_seconds": self.freeze_seconds(),
            "tolerance_seconds": self.tolerance_seconds(),
            "horizon_seconds": self.horizon_seconds(),
        }

    # -- revision history --------------------------------------------------

    def revision(self) -> int:
        return _load_int(self._store, REVISION_KEY, 0)

    def latest(self) -> TimelinePlan | None:
        raw, present = self._store.get(LATEST_PLAN_KEY)
        if not present:
            return None
        try:
            return TimelinePlan.from_dict(json.loads(raw))
        except ValueError:
            return None

    def history(self) -> list[TimelinePlan]:
        plans: list[TimelinePlan] = []
        for raw in load_commands(self._store, PLAN_HISTORY_KEY):
            try:
                payload = json.loads(raw)
            except ValueError:
                continue
            if isinstance(payload, dict):
                plans.append(TimelinePlan.from_dict(payload))
        return plans

    def revision_plan(self, number: int) -> TimelinePlan | None:
        for plan in self.history():
            if plan.revision == number:
                return plan
        return None

    # -- rolling generation ------------------------------------------------

    def generate(
        self,
        jobs: list[DefrostJob],
        *,
        trigger: str,
        reason: str = "",
        now: int | None = None,
        concurrency: int | None = None,
        freeze_seconds: int | None = None,
        running: list[str] | None = None,
    ) -> TimelinePlan:
        """Roll the plan forward, freezing committed work and re-solving tail.

        Slots from the current revision that are in flight or start inside the
        freeze window are carried over verbatim.  Everything after the freeze
        boundary is re-queued by recovery time against the capacity lanes.
        """

        trigger = validate_trigger(trigger)
        capacity = self.concurrency() if concurrency is None else concurrency
        validate_concurrency(capacity)
        freeze = self.freeze_seconds() if freeze_seconds is None else freeze_seconds
        validate_freeze_seconds(freeze)
        horizon = self.horizon_seconds()
        at = now_unix() if now is None else int(now)
        boundary = at + freeze
        horizon_end = at + horizon

        frozen = self._frozen_slots(boundary, at, set(running or []))
        committed = {slot.room_id for slot in frozen}
        slots = schedule_jobs(jobs, concurrency=capacity, now=at, frozen=frozen)
        violations = verify_plan(
            slots,
            concurrency=capacity,
            horizon_end=horizon_end,
            frozen_until=boundary,
            policy_reschedule=POLICY_RESCHEDULE,
            policy_adjacency=POLICY_ADJACENCY,
            committed_rooms=committed,
        )

        number = self.revision() + 1
        plan = TimelinePlan(
            revision=number,
            generated_at=at,
            trigger=trigger,
            reason=reason or _describe_reason(trigger, len(jobs), capacity, len(frozen)),
            policy_reschedule=POLICY_RESCHEDULE,
            policy_adjacency=POLICY_ADJACENCY,
            concurrency=capacity,
            frozen_until=boundary,
            horizon_end=horizon_end,
            consistent=not violations,
            violations=violations,
            slots=slots,
        )
        self._persist(plan)
        if self._auditor is not None:
            detail = {
                "revision": number,
                "trigger": trigger,
                "slots": len(slots),
                "frozen": len(frozen),
                "late": sum(1 for slot in slots if slot.late),
                "consistent": plan.consistent,
                "violations": violations,
            }
            self._auditor.record("schedule-plan", json.dumps(detail, ensure_ascii=False))
        return plan

    def _frozen_slots(
        self, boundary: int, now: int, running_rooms: set[str]
    ) -> list[ScheduledSlot]:
        latest = self.latest()
        if latest is None:
            return []
        frozen: list[ScheduledSlot] = []
        for slot in latest.slots:
            # Only the execution tracker knows what is truly in progress; the
            # time window alone cannot distinguish "finished early" from
            # "never started".  Imminent un-started slots are frozen as well.
            in_flight = slot.room_id in running_rooms
            imminent = (
                slot.start_at < boundary
                and slot.end_at > now
                and slot.room_id not in running_rooms
                and slot.start_at > now
            )
            if in_flight or imminent:
                # A carried slot is re-stamped as frozen; its timing never moves.
                frozen.append(
                    ScheduledSlot(
                        room_id=slot.room_id,
                        ready_at=slot.ready_at,
                        due_at=slot.due_at,
                        start_at=slot.start_at,
                        end_at=slot.end_at,
                        recovered_at=slot.recovered_at,
                        defrost_seconds=slot.defrost_seconds,
                        recovery_seconds=slot.recovery_seconds,
                        late=slot.late,
                        frozen=True,
                        reason=REASON_FROZEN,
                    )
                )
        return frozen

    def _persist(self, plan: TimelinePlan) -> None:
        raw = json.dumps(plan.as_dict(), ensure_ascii=False)
        save_float(self._store, REVISION_KEY, float(plan.revision))
        self._store.put(LATEST_PLAN_KEY, raw)
        history = load_commands(self._store, PLAN_HISTORY_KEY)
        history.append(raw)
        if len(history) > MAX_HISTORY:
            history = history[-MAX_HISTORY:]
        self._store.put(PLAN_HISTORY_KEY, json.dumps(history, ensure_ascii=False))


def _describe_reason(trigger: str, jobs: int, capacity: int, frozen: int) -> str:
    return (
        f"{trigger}: queued={jobs} capacity={capacity} carried={frozen} "
        f"policy={POLICY_RESCHEDULE}/{POLICY_ADJACENCY}"
    )


class ExecutionTracker:
    """Records dispatched starts and finishes and flags plan deviations."""

    def __init__(self, store: Store, planner: TimelinePlanner, auditor: "Auditor | None" = None) -> None:
        self._store = store
        self._planner = planner
        self._auditor = auditor

    def records(self) -> list[ExecutionRecord]:
        raw, present = self._store.get(EXECUTION_KEY)
        if not present:
            return []
        try:
            payload = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(payload, dict):
            return []
        return [
            ExecutionRecord.from_dict(item)
            for item in payload.values()
            if isinstance(item, dict)
        ]

    def record(self, room_id: str) -> ExecutionRecord | None:
        for item in self.records():
            if item.room_id == room_id:
                return item
        return None

    def running_ids(self) -> list[str]:
        return [item.room_id for item in self.records() if item.status == STATUS_RUNNING]

    def start(self, room_id: str, *, at: int | None = None) -> ExecutionRecord:
        """Begin a defrost, rejecting work that is not dispatchable now."""

        plan = self._planner.latest()
        if plan is None:
            raise ValueError("no schedule revision has been generated")
        slot = plan.slot_for(room_id)
        if slot is None:
            raise ValueError(f"room {room_id} is not on the current schedule")
        started_at = now_unix() if at is None else int(at)
        tolerance = self._planner.tolerance_seconds()

        existing = self.record(room_id)
        if existing is not None and existing.status == STATUS_RUNNING:
            raise ValueError(f"room {room_id} is already defrosting")
        if started_at < slot.ready_at - tolerance:
            raise ValueError(
                f"room {room_id} window opens at {slot.ready_at}, too early at {started_at}"
            )

        running = self.running_ids()
        if room_id not in running and len(running) >= plan.concurrency:
            raise ValueError(
                f"defrost capacity {plan.concurrency} reached, {len(running)} in progress"
            )

        late = started_at > slot.due_at
        deviation = ""
        if started_at < slot.ready_at:
            deviation = f"started {slot.ready_at - started_at}s before window opened"
        elif late:
            deviation = f"started {started_at - slot.due_at}s after window closed"
        record = ExecutionRecord(
            room_id=room_id,
            revision=plan.revision,
            planned_start=slot.start_at,
            planned_end=slot.end_at,
            actual_start=started_at,
            actual_finish=0,
            status=STATUS_RUNNING,
            late=late,
        )
        self._save(record)
        self._audit("schedule-exec-start", record, deviation)
        return record

    def finish(self, room_id: str, *, at: int | None = None) -> ExecutionRecord:
        record = self.record(room_id)
        if record is None or record.status != STATUS_RUNNING:
            raise ValueError(f"room {room_id} has no defrost in progress")
        finished_at = now_unix() if at is None else int(at)
        record = ExecutionRecord(
            room_id=record.room_id,
            revision=record.revision,
            planned_start=record.planned_start,
            planned_end=record.planned_end,
            actual_start=record.actual_start,
            actual_finish=finished_at,
            status=STATUS_FINISHED,
            late=record.late,
        )
        self._save(record)
        deviation = ""
        if finished_at < record.actual_start:
            deviation = "finished before it started"
        elif finished_at < record.planned_end:
            deviation = f"finished {record.planned_end - finished_at}s early"
        self._audit("schedule-exec-finish", record, deviation)
        return record

    def _save(self, record: ExecutionRecord) -> None:
        records = {item.room_id: item.as_dict() for item in self.records()}
        records[record.room_id] = record.as_dict()
        self._store.put(EXECUTION_KEY, json.dumps(records, ensure_ascii=False))

    def _audit(self, kind: str, record: ExecutionRecord, deviation: str) -> None:
        if self._auditor is None:
            return
        detail = json.dumps(
            {
                "room_id": record.room_id,
                "revision": record.revision,
                "planned_start": record.planned_start,
                "actual_start": record.actual_start,
                "actual_finish": record.actual_finish,
                "status": record.status,
                "late": record.late,
                "deviation": deviation,
            },
            ensure_ascii=False,
        )
        self._auditor.record(kind, detail)
        if deviation:
            self._auditor.record(
                "schedule-deviation",
                json.dumps(
                    {"room_id": record.room_id, "revision": record.revision, "detail": deviation},
                    ensure_ascii=False,
                ),
            )
