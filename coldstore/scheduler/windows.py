"""Time window solver for defrost scheduling.

Re-planning is rolling, not fixed period: every re-plan starts from the
current time, anchors rooms already defrosting as committed windows and
re-solves only the remaining demands. Adjacent defrosts of one room are
strictly serial and never overlap; the next start must wait for the
previous recovery tail to end.

The solver is a bounded greedy interval scheduler. Demands queue by the
time their recovery from the previous defrost ends, warmest first on
ties, and each demand is placed at the earliest feasible start. There is
no backtracking: every feasibility probe is counted in
:class:`SolveStats` so the computation stays visibly polynomial.
"""

from __future__ import annotations

from dataclasses import dataclass

MAX_CANDIDATES = 64
DEFAULT_MAX_CONCURRENT = 2
MAX_CONCURRENT_LIMIT = 8
DEFAULT_RECOVERY_BASE_SECONDS = 300
DEFAULT_RECOVERY_PER_DEGREE_SECONDS = 10
DEFAULT_HORIZON_SECONDS = 3600
MIN_HORIZON_SECONDS = 60
MAX_HORIZON_SECONDS = 86400
MAX_RECOVERY_SECONDS = 86400


@dataclass(frozen=True)
class Demand:
    """One room asking for a defrost window inside the horizon."""

    room_id: str
    temp: float
    defrost_seconds: int
    recovery_seconds: int
    available_at: int

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "temp": self.temp,
            "defrost_seconds": self.defrost_seconds,
            "recovery_seconds": self.recovery_seconds,
            "available_at": self.available_at,
        }


@dataclass(frozen=True)
class Window:
    """A scheduled defrost with its recovery tail.

    ``start``..``end`` is the heating phase that counts against the
    concurrency cap; ``end``..``recovery_end`` is the pull-down tail
    during which the room must not defrost again.
    """

    room_id: str
    start: int
    end: int
    recovery_end: int
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "start": self.start,
            "end": self.end,
            "recovery_end": self.recovery_end,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "Window":
        return cls(
            room_id=str(payload.get("room_id", "")),
            start=int(payload.get("start", 0)),
            end=int(payload.get("end", 0)),
            recovery_end=int(payload.get("recovery_end", 0)),
            reason=str(payload.get("reason", "")),
        )


@dataclass(frozen=True)
class Deferred:
    """A demand the solver could not place inside the horizon."""

    room_id: str
    temp: float
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {"room_id": self.room_id, "temp": self.temp, "reason": self.reason}

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "Deferred":
        return cls(
            room_id=str(payload.get("room_id", "")),
            temp=float(payload.get("temp", 0.0)),
            reason=str(payload.get("reason", "")),
        )


@dataclass(frozen=True)
class SolveStats:
    """Bookkeeping that proves the solve stayed bounded."""

    candidates: int
    placed: int
    deferred: int
    checks: int
    capped: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "candidates": self.candidates,
            "placed": self.placed,
            "deferred": self.deferred,
            "checks": self.checks,
            "capped": self.capped,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "SolveStats":
        return cls(
            candidates=int(payload.get("candidates", 0)),
            placed=int(payload.get("placed", 0)),
            deferred=int(payload.get("deferred", 0)),
            checks=int(payload.get("checks", 0)),
            capped=bool(payload.get("capped", False)),
        )


@dataclass(frozen=True)
class WindowPlan:
    """The outcome of one rolling re-plan, persisted for review."""

    plan_id: str
    created_at: int
    reason: str
    horizon_seconds: int
    max_concurrent: int
    committed: tuple[Window, ...]
    windows: tuple[Window, ...]
    deferred: tuple[Deferred, ...]
    stats: SolveStats
    violations: tuple[str, ...]

    def consistent(self) -> bool:
        """True when the plan honours every recovery and concurrency rule."""

        return not self.violations

    def as_dict(self) -> dict[str, object]:
        return {
            "plan_id": self.plan_id,
            "created_at": self.created_at,
            "reason": self.reason,
            "horizon_seconds": self.horizon_seconds,
            "max_concurrent": self.max_concurrent,
            "committed": [window.as_dict() for window in self.committed],
            "windows": [window.as_dict() for window in self.windows],
            "deferred": [entry.as_dict() for entry in self.deferred],
            "stats": self.stats.as_dict(),
            "violations": list(self.violations),
            "consistent": self.consistent(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "WindowPlan":
        def _windows(key: str) -> tuple[Window, ...]:
            items = payload.get(key, [])
            if not isinstance(items, list):
                return ()
            return tuple(Window.from_dict(item) for item in items if isinstance(item, dict))

        deferred_raw = payload.get("deferred", [])
        deferred = (
            tuple(Deferred.from_dict(item) for item in deferred_raw if isinstance(item, dict))
            if isinstance(deferred_raw, list)
            else ()
        )
        stats_raw = payload.get("stats", {})
        violations_raw = payload.get("violations", [])
        return cls(
            plan_id=str(payload.get("plan_id", "")),
            created_at=int(payload.get("created_at", 0)),
            reason=str(payload.get("reason", "")),
            horizon_seconds=int(payload.get("horizon_seconds", 0)),
            max_concurrent=int(payload.get("max_concurrent", 0)),
            committed=_windows("committed"),
            windows=_windows("windows"),
            deferred=deferred,
            stats=SolveStats.from_dict(stats_raw if isinstance(stats_raw, dict) else {}),
            violations=tuple(str(item) for item in violations_raw)
            if isinstance(violations_raw, list)
            else (),
        )


def _conflict_jump(placed: list[Window], start: int, end: int, cap: int) -> int | None:
    """Where to jump when [start, end) breaches the cap, else None.

    Concurrency only increases at window starts, so probing the candidate
    start and every overlapping window start is sufficient. On a breach
    the earliest end among the active windows is the next feasible point.
    """

    points = {start}
    for window in placed:
        if window.end <= start or window.start >= end:
            continue
        points.add(max(window.start, start))
    for point in sorted(points):
        active = [window for window in placed if window.start <= point < window.end]
        if len(active) >= cap:
            return min(window.end for window in active)
    return None


def solve(
    demands: list[Demand],
    committed: list[Window],
    now: int,
    horizon_seconds: int,
    max_concurrent: int,
) -> tuple[list[Window], list[Deferred], SolveStats]:
    """Place every demand at its earliest feasible start inside the horizon.

    Committed windows are fixed. Each demand starts no earlier than its
    recovery tail allows and only where the concurrency cap has headroom;
    demands that do not fit are deferred with a reason. The probe count
    is bounded by ``n * (n + 1) / 2 + n`` for ``n`` candidates.
    """

    placed = list(committed)
    windows: list[Window] = []
    deferred: list[Deferred] = []
    checks = 0
    queue = sorted(demands, key=lambda demand: (demand.available_at, -demand.temp, demand.room_id))
    capped = len(queue) > MAX_CANDIDATES
    overflow = queue[MAX_CANDIDATES:]
    end_limit = now + horizon_seconds

    for demand in queue[:MAX_CANDIDATES]:
        start = max(now, demand.available_at)
        while True:
            checks += 1
            jump = _conflict_jump(placed, start, start + demand.defrost_seconds, max_concurrent)
            if jump is None:
                break
            start = max(jump, start + 1)
            if start + demand.defrost_seconds > end_limit:
                break
        if start + demand.defrost_seconds > end_limit:
            deferred.append(Deferred(demand.room_id, demand.temp, "horizon full"))
            continue
        window = Window(
            room_id=demand.room_id,
            start=start,
            end=start + demand.defrost_seconds,
            recovery_end=start + demand.defrost_seconds + demand.recovery_seconds,
            reason=f"due temp={demand.temp:.4f}",
        )
        placed.append(window)
        windows.append(window)

    for demand in overflow:
        deferred.append(Deferred(demand.room_id, demand.temp, "solver cap"))

    stats = SolveStats(
        candidates=len(demands),
        placed=len(windows),
        deferred=len(deferred),
        checks=checks,
        capped=capped,
    )
    return windows, deferred, stats


def validate_windows(windows: list[Window], max_concurrent: int) -> list[str]:
    """Constraint violations across the windows, empty when consistent.

    Two rules are checked: adjacent defrosts of one room stay serial with
    the recovery tail honoured, and the defrost phase concurrency never
    exceeds the cap.
    """

    violations: list[str] = []
    by_room: dict[str, list[Window]] = {}
    for window in windows:
        by_room.setdefault(window.room_id, []).append(window)
    for room_id, room_windows in sorted(by_room.items()):
        ordered = sorted(room_windows, key=lambda window: (window.start, window.end))
        for previous, current in zip(ordered, ordered[1:]):
            if current.start < previous.recovery_end:
                violations.append(
                    f"{room_id}: start {current.start} before previous "
                    f"recovery end {previous.recovery_end}"
                )

    events: list[tuple[int, int]] = []
    for window in windows:
        events.append((window.start, 1))
        events.append((window.end, -1))
    active = 0
    peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    if peak > max_concurrent:
        violations.append(f"concurrency peak {peak} exceeds cap {max_concurrent}")
    return violations
