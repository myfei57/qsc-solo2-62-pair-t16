"""Temperature driven defrost planner with a rolling time window layer.

The legacy ordering (warmest room first, a defrost duration per room) is kept
for ``/schedule`` and existing callers.  The time window layer queues the same
rooms by recovery time, limits concurrent defrosts, waits for warm back and
commits a freeze window on every rolling regeneration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from coldstore.room import RoomRegistry
from coldstore.store.epoch import load_float, save_float
from coldstore.store.store import Store

from .report import ScheduleEntry, ScheduleState
from .timeline import (
    ExecutionRecord,
    ExecutionTracker,
    STATUS_FINISHED,
    TimelinePlan,
    TimelinePlanner,
)
from .window import (
    TRIGGER_MANUAL,
    DefrostJob,
)

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from coldstore.audit import Auditor

THRESHOLD_KEY = "schedule:temp-threshold"
DEFAULT_THRESHOLD = -12.0
DEFROST_BASE_SECONDS = 120
DEFROST_SECONDS_PER_DEGREE = 20


def defrost_duration(temp: float) -> int:
    """Defrost seconds required at one room temperature."""

    return DEFROST_BASE_SECONDS + int(abs(temp) * DEFROST_SECONDS_PER_DEGREE)


class Scheduler:
    """Orders defrost work by temperature and owns the rolling timeline."""

    def __init__(self, store: Store, auditor: "Auditor | None" = None) -> None:
        self._store = store
        self.timeline = TimelinePlanner(store, auditor)
        self.executions = ExecutionTracker(store, self.timeline, auditor)

    # -- legacy temperature ordering --------------------------------------

    def threshold(self) -> float:
        value, present = load_float(self._store, THRESHOLD_KEY)
        return value if present else DEFAULT_THRESHOLD

    def set_threshold(self, value: float) -> float:
        save_float(self._store, THRESHOLD_KEY, value)
        return value

    def due(self, rooms: RoomRegistry) -> list[str]:
        """Rooms at or above the temperature threshold, warmest first."""

        threshold = self.threshold()
        loaded = [room for room in rooms.rooms() if room.temp >= threshold]
        loaded.sort(key=lambda room: (-room.temp, room.id))
        return [room.id for room in loaded]

    def plan(self, rooms: RoomRegistry) -> list[ScheduleEntry]:
        """Number every room by temperature so the warmest defrosts first."""

        ordered = sorted(rooms.rooms(), key=lambda room: (-room.temp, room.id))
        return [
            ScheduleEntry(
                room_id=room.id,
                temp=room.temp,
                priority=index + 1,
                defrost_seconds=defrost_duration(room.temp),
            )
            for index, room in enumerate(ordered)
        ]

    def state(self, rooms: RoomRegistry) -> ScheduleState:
        return ScheduleState(
            threshold=self.threshold(),
            due=self.due(rooms),
            entries=self.plan(rooms),
        )

    def describe(self, rooms: RoomRegistry) -> str:
        state = self.state(rooms)
        latest = self.timeline.latest()
        revision = latest.revision if latest else 0
        consistent = latest.consistent if latest else True
        return (
            f"schedule threshold={state.threshold:.4f} due={len(state.due)} "
            f"revision={revision} consistent={consistent} "
            f"policy=rolling/serial capacity={self.timeline.concurrency()}"
        )

    # -- rolling time window ----------------------------------------------

    def build_jobs(
        self,
        rooms: RoomRegistry,
        *,
        now: int,
        durations: dict[str, int] | None = None,
        recoveries: dict[str, int] | None = None,
        ready_at: dict[str, int] | None = None,
        due_at: dict[str, int] | None = None,
    ) -> list[DefrostJob]:
        """Turn rooms at or above the threshold into queued time window jobs.

        The serial adjacency policy is enforced against reality too: a room
        whose latest defrost actually finished has its window pushed to open
        only after its warm back recovery elapses.
        """

        duration_overrides = durations or {}
        recovery_overrides = recoveries or {}
        ready_overrides = ready_at or {}
        due_overrides = due_at or {}
        threshold = self.threshold()
        response = self.timeline.response_seconds()
        default_recovery = self.timeline.recovery_seconds()
        finished_at = {
            record.room_id: record.actual_finish
            for record in self.executions.records()
            if record.status == STATUS_FINISHED and record.actual_finish > 0
        }

        jobs: list[DefrostJob] = []
        for room in rooms.rooms():
            if room.temp < threshold:
                continue
            recovery = int(recovery_overrides.get(room.id, default_recovery))
            base_ready = int(ready_overrides.get(room.id, now))
            if room.id in finished_at:
                # Serial: adjacent defrost waits for warm back to complete.
                base_ready = max(base_ready, finished_at[room.id] + recovery)
            jobs.append(
                DefrostJob(
                    room_id=room.id,
                    temp=room.temp,
                    ready_at=base_ready,
                    due_at=int(due_overrides.get(room.id, now + response)),
                    defrost_seconds=int(
                        duration_overrides.get(room.id, defrost_duration(room.temp))
                    ),
                    recovery_seconds=recovery,
                )
            )
        return jobs

    def generate(
        self,
        rooms: RoomRegistry,
        *,
        trigger: str = TRIGGER_MANUAL,
        reason: str = "",
        now: int | None = None,
        durations: dict[str, int] | None = None,
        recoveries: dict[str, int] | None = None,
        ready_at: dict[str, int] | None = None,
        due_at: dict[str, int] | None = None,
        concurrency: int | None = None,
        freeze_seconds: int | None = None,
    ) -> TimelinePlan:
        """Generate the next rolling revision for every room that is due."""

        from coldstore.defrost.schedule import now_unix

        at = now_unix() if now is None else int(now)
        jobs = self.build_jobs(
            rooms,
            now=at,
            durations=durations,
            recoveries=recoveries,
            ready_at=ready_at,
            due_at=due_at,
        )
        return self.timeline.generate(
            jobs,
            trigger=trigger,
            reason=reason,
            now=at,
            concurrency=concurrency,
            freeze_seconds=freeze_seconds,
            running=self.executions.running_ids(),
        )

    def latest_plan(self) -> TimelinePlan | None:
        return self.timeline.latest()

    def plan_history(self) -> list[TimelinePlan]:
        return self.timeline.history()

    def revision_plan(self, number: int) -> TimelinePlan | None:
        return self.timeline.revision_plan(number)

    def execution_records(self) -> list[ExecutionRecord]:
        return self.executions.records()

    # -- configuration passthrough ----------------------------------------

    def concurrency(self) -> int:
        return self.timeline.concurrency()

    def recovery_seconds(self) -> int:
        return self.timeline.recovery_seconds()

    def response_seconds(self) -> int:
        return self.timeline.response_seconds()

    def freeze_seconds(self) -> int:
        return self.timeline.freeze_seconds()

    def tolerance_seconds(self) -> int:
        return self.timeline.tolerance_seconds()

    def horizon_seconds(self) -> int:
        return self.timeline.horizon_seconds()

    def set_concurrency(self, value: int) -> int:
        return self.timeline.set_concurrency(value)

    def set_recovery_seconds(self, value: int) -> int:
        return self.timeline.set_recovery_seconds(value)

    def set_response_seconds(self, value: int) -> int:
        return self.timeline.set_response_seconds(value)

    def set_freeze_seconds(self, value: int) -> int:
        return self.timeline.set_freeze_seconds(value)

    def set_tolerance_seconds(self, value: int) -> int:
        return self.timeline.set_tolerance_seconds(value)

    def set_horizon_seconds(self, value: int) -> int:
        return self.timeline.set_horizon_seconds(value)
