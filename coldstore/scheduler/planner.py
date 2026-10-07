"""Temperature driven defrost planner with a rolling window re-plan."""

from __future__ import annotations

import time
import uuid

from coldstore.room import RoomRegistry
from coldstore.store.epoch import load_float, save_float
from coldstore.store.store import Store

from . import ledger
from .ledger import ExecutionRecord
from .report import (
    ScheduleEntry,
    ScheduleState,
    validate_capacity,
    validate_horizon,
    validate_recovery,
)
from .windows import (
    DEFAULT_HORIZON_SECONDS,
    DEFAULT_MAX_CONCURRENT,
    DEFAULT_RECOVERY_BASE_SECONDS,
    DEFAULT_RECOVERY_PER_DEGREE_SECONDS,
    Demand,
    Window,
    WindowPlan,
    solve,
    validate_windows,
)

THRESHOLD_KEY = "schedule:temp-threshold"
DEFAULT_THRESHOLD = -12.0
DEFROST_BASE_SECONDS = 120
DEFROST_SECONDS_PER_DEGREE = 20
CAPACITY_KEY = "schedule:max-concurrent"
RECOVERY_BASE_KEY = "schedule:recovery-base"
RECOVERY_PER_DEGREE_KEY = "schedule:recovery-per-degree"


def _load_int(store: Store, key: str) -> tuple[int, bool]:
    raw, present = store.get(key)
    if not present:
        return 0, False
    try:
        return int(raw), True
    except ValueError:
        return 0, False


class Scheduler:
    """Orders defrost work from the warmest room down to the coldest.

    On top of the temperature ordering it rolls a time window plan: every
    re-plan anchors the rooms already defrosting, queues the rest by the
    end of their recovery tail and caps how many rooms defrost at once.
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    def threshold(self) -> float:
        value, present = load_float(self._store, THRESHOLD_KEY)
        return value if present else DEFAULT_THRESHOLD

    def set_threshold(self, value: float) -> float:
        save_float(self._store, THRESHOLD_KEY, value)
        return value

    def max_concurrent(self) -> int:
        value, present = _load_int(self._store, CAPACITY_KEY)
        return value if present else DEFAULT_MAX_CONCURRENT

    def set_max_concurrent(self, value: int) -> int:
        validate_capacity(value)
        self._store.put(CAPACITY_KEY, str(value))
        return value

    def recovery_base(self) -> int:
        value, present = _load_int(self._store, RECOVERY_BASE_KEY)
        return value if present else DEFAULT_RECOVERY_BASE_SECONDS

    def recovery_per_degree(self) -> int:
        value, present = _load_int(self._store, RECOVERY_PER_DEGREE_KEY)
        return value if present else DEFAULT_RECOVERY_PER_DEGREE_SECONDS

    def set_recovery(self, base: int, per_degree: int) -> tuple[int, int]:
        validate_recovery(base, per_degree)
        self._store.put(RECOVERY_BASE_KEY, str(base))
        self._store.put(RECOVERY_PER_DEGREE_KEY, str(per_degree))
        return base, per_degree

    def defrost_seconds_for(self, temp: float) -> int:
        return DEFROST_BASE_SECONDS + int(abs(temp) * DEFROST_SECONDS_PER_DEGREE)

    def recovery_seconds_for(self, temp: float) -> int:
        return self.recovery_base() + int(abs(temp) * self.recovery_per_degree())

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
                defrost_seconds=self.defrost_seconds_for(room.temp),
            )
            for index, room in enumerate(ordered)
        ]

    def replan(
        self,
        rooms: RoomRegistry,
        *,
        reason: str = "manual",
        now: int | None = None,
        horizon: int | None = None,
    ) -> WindowPlan:
        """Re-solve the defrost windows from ``now`` (rolling re-plan).

        Rooms already defrosting are anchored as committed windows and
        rooms still inside their recovery tail cannot restart before it
        ends. The result is validated against the recovery and
        concurrency constraints, then persisted so it can be reviewed.
        """

        at = int(now) if now else int(time.time())
        horizon_seconds = int(horizon) if horizon else DEFAULT_HORIZON_SECONDS
        validate_horizon(horizon_seconds)
        cap = self.max_concurrent()

        committed: list[Window] = []
        for room_id in rooms.defrosting_ids():
            temp, _ = rooms.temp(room_id)
            defrost = self.defrost_seconds_for(temp)
            committed.append(
                Window(
                    room_id=room_id,
                    start=at,
                    end=at + defrost,
                    recovery_end=at + defrost + self.recovery_seconds_for(temp),
                    reason="committed in-progress",
                )
            )

        active = set(rooms.defrosting_ids())
        demands: list[Demand] = []
        for room_id in self.due(rooms):
            if room_id in active:
                continue
            temp, _ = rooms.temp(room_id)
            demands.append(
                Demand(
                    room_id=room_id,
                    temp=temp,
                    defrost_seconds=self.defrost_seconds_for(temp),
                    recovery_seconds=self.recovery_seconds_for(temp),
                    available_at=max(ledger.recovery_until(self._store, room_id), at),
                )
            )

        windows, deferred, stats = solve(demands, committed, at, horizon_seconds, cap)
        violations = validate_windows([*committed, *windows], cap)
        plan = WindowPlan(
            plan_id=f"plan-{uuid.uuid4().hex[:12]}",
            created_at=at,
            reason=reason or "manual",
            horizon_seconds=horizon_seconds,
            max_concurrent=cap,
            committed=tuple(committed),
            windows=tuple(windows),
            deferred=tuple(deferred),
            stats=stats,
            violations=tuple(violations),
        )
        ledger.save_plan(self._store, plan.as_dict())
        return plan

    def plans(self) -> list[WindowPlan]:
        """Every retained plan, oldest first, for schedule review."""

        return [WindowPlan.from_dict(item) for item in ledger.load_plan_dicts(self._store)]

    def latest_plan(self) -> WindowPlan | None:
        plans = self.plans()
        if not plans:
            return None
        return plans[-1]

    def executions(self) -> list[ExecutionRecord]:
        """Every retained execution record, oldest first."""

        return [ExecutionRecord.from_dict(item) for item in ledger.load_execution_dicts(self._store)]

    def note_start(self, room_id: str, at: int) -> None:
        """Mark the actual start of a defrost, called by the controller."""

        ledger.note_exec_start(self._store, room_id, int(at))

    def note_finish(self, room_id: str, at: int, temp: float) -> ExecutionRecord | None:
        """Close a defrost execution and open the room's recovery tail.

        Returns None when no matching start was seen, so a bare finish
        (for example a cycle reset) does not count as an execution. The
        record flags whether the actual start respected the previous
        recovery tail.
        """

        started = ledger.take_exec_start(self._store, room_id)
        if started is None:
            return None
        at = int(at)
        respected = started >= ledger.recovery_until(self._store, room_id)
        until = at + self.recovery_seconds_for(temp)
        ledger.set_recovery_until(self._store, room_id, until)

        planned_start = 0
        planned_end = 0
        plan_id = ""
        plan = self.latest_plan()
        if plan is not None:
            for window in (*plan.committed, *plan.windows):
                if window.room_id == room_id:
                    planned_start = window.start
                    planned_end = window.end
                    plan_id = plan.plan_id
        record = ExecutionRecord(
            room_id=room_id,
            plan_id=plan_id,
            planned_start=planned_start,
            planned_end=planned_end,
            actual_start=started,
            actual_end=at,
            recovery_until=until,
            recovery_respected=respected,
        )
        ledger.append_execution(self._store, record.as_dict())
        return record

    def state(self, rooms: RoomRegistry) -> ScheduleState:
        latest = self.latest_plan()
        return ScheduleState(
            threshold=self.threshold(),
            due=self.due(rooms),
            entries=self.plan(rooms),
            max_concurrent=self.max_concurrent(),
            recovery_base=self.recovery_base(),
            recovery_per_degree=self.recovery_per_degree(),
            planned_windows=len(latest.windows) if latest else 0,
            latest_plan_id=latest.plan_id if latest else "",
        )

    def describe(self, rooms: RoomRegistry) -> str:
        state = self.state(rooms)
        return f"schedule threshold={state.threshold:.4f} due={len(state.due)}"
