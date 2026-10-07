"""Bounded greedy solver for time windowed defrost slots.

The solver is deliberately simple so the compute cost stays predictable:
``O(n log n)`` in the number of rooms, capped by :data:`MAX_JOBS`.  Jobs are
queued by recovery time (earliest ``due_at`` first, warmest room as the tie
breaker) and placed on the earliest capacity lane that stays clear of both the
job's time window and that room's serial recovery interval.  A full constraint
verification (:func:`verify_plan`) re-checks every produced plan so an emitted
schedule is never silently inconsistent.
"""

from __future__ import annotations

from .window import (
    MAX_JOBS,
    REASON_CAPACITY,
    REASON_RECOVERY,
    REASON_WINDOW,
    DefrostJob,
    ScheduledSlot,
)


def _slot_for(
    job: DefrostJob,
    start_at: int,
    *,
    frozen: bool,
    reason: str,
) -> ScheduledSlot:
    end_at = start_at + job.defrost_seconds
    recovered_at = end_at + job.recovery_seconds
    return ScheduledSlot(
        room_id=job.room_id,
        ready_at=job.ready_at,
        due_at=job.due_at,
        start_at=start_at,
        end_at=end_at,
        recovered_at=recovered_at,
        defrost_seconds=job.defrost_seconds,
        recovery_seconds=job.recovery_seconds,
        late=start_at > job.due_at,
        frozen=frozen,
        reason=reason,
    )


def schedule_jobs(
    jobs: list[DefrostJob],
    *,
    concurrency: int,
    now: int,
    frozen: list[ScheduledSlot] | None = None,
) -> list[ScheduledSlot]:
    """Place jobs on a fixed number of concurrent defrost lanes.

    Each lane holds the end time of its last defrost; each room has a busy
    until time covering its warm back interval, which makes adjacent defrosts
    of the same room serial.  Frozen slots (rolling commits already dispatched)
    seed both the lanes and the room busy times and are carried over verbatim.
    """

    if len(jobs) > MAX_JOBS:
        raise ValueError(f"cannot schedule more than {MAX_JOBS} jobs in one revision")

    frozen_slots = sorted(frozen or [], key=lambda slot: (slot.start_at, slot.room_id))
    carried: dict[str, ScheduledSlot] = {slot.room_id: slot for slot in frozen_slots}
    if len(carried) != len(frozen_slots):
        raise ValueError("frozen slots must contain each room at most once")

    lanes = [0] * concurrency
    busy_until: dict[str, int] = {}
    result: list[ScheduledSlot] = []
    for slot in frozen_slots:
        result.append(slot)
        earliest_lane = min(range(concurrency), key=lambda index: lanes[index])
        lanes[earliest_lane] = slot.end_at
        busy_until[slot.room_id] = max(
            busy_until.get(slot.room_id, 0), slot.recovered_at
        )

    queued = sorted(jobs, key=lambda job: (job.due_at, -job.temp, job.room_id))
    for job in queued:
        if job.room_id in carried:
            continue
        earliest_lane = min(range(concurrency), key=lambda index: lanes[index])
        capacity_at = lanes[earliest_lane]
        recovery_at = busy_until.get(job.room_id, now)
        start_at = max(job.ready_at, capacity_at, recovery_at, now)
        if start_at == max(job.ready_at, now):
            reason = REASON_WINDOW
        elif capacity_at >= recovery_at:
            reason = REASON_CAPACITY
        else:
            reason = REASON_RECOVERY
        slot = _slot_for(job, start_at, frozen=False, reason=reason)
        lanes[earliest_lane] = slot.end_at
        busy_until[job.room_id] = slot.recovered_at
        result.append(slot)

    return sorted(result, key=lambda slot: (slot.start_at, slot.room_id))


def verify_plan(
    slots: list[ScheduledSlot],
    *,
    concurrency: int,
    horizon_end: int,
    frozen_until: int,
    policy_reschedule: str,
    policy_adjacency: str,
    committed_rooms: set[str] | None = None,
) -> list[str]:
    """Return human readable reasons when a plan breaks its own constraints.

    An empty list means the plan is consistent: windows are respected, no more
    than ``concurrency`` defrosts overlap, and adjacent defrosts of one room
    stay serial across the warm back interval.  ``committed_rooms`` holds the
    rooms whose previous revision promised a start before the freeze boundary;
    every one of them must be carried over frozen rather than rescheduled.
    """

    committed = committed_rooms or set()
    violations: list[str] = []
    seen_rooms: set[str] = set()
    for slot in slots:
        if slot.start_at < slot.ready_at:
            violations.append(f"{slot.room_id} starts before its window opens")
        if slot.end_at != slot.start_at + slot.defrost_seconds:
            violations.append(f"{slot.room_id} end does not match defrost duration")
        if slot.recovered_at != slot.end_at + slot.recovery_seconds:
            violations.append(f"{slot.room_id} recovery end does not match wait duration")
        if slot.room_id in seen_rooms:
            violations.append(f"{slot.room_id} is scheduled more than once")
        seen_rooms.add(slot.room_id)
        if slot.room_id in committed and not slot.frozen:
            violations.append(f"{slot.room_id} was moved inside the freeze window")
        if slot.start_at > horizon_end:
            violations.append(f"{slot.room_id} starts after the planning horizon")

    # Count live overlaps at every distinct start/end boundary.
    boundaries = sorted(
        {boundary for slot in slots for boundary in (slot.start_at, slot.end_at)}
    )
    for boundary in boundaries:
        live = sum(1 for slot in slots if slot.start_at <= boundary < slot.end_at)
        if live > concurrency:
            violations.append(f"concurrency {live} exceeds limit {concurrency} at {boundary}")
            break

    # Serial adjacency: a room's next start may not precede the previous
    # recovery end, which also covers hard overlaps of one room.
    by_room: dict[str, list[ScheduledSlot]] = {}
    for slot in slots:
        by_room.setdefault(slot.room_id, []).append(slot)
    for room_id, room_slots in by_room.items():
        room_slots.sort(key=lambda slot: slot.start_at)
        for earlier, later in zip(room_slots, room_slots[1:]):
            if later.start_at < earlier.recovered_at:
                violations.append(
                    f"{room_id} restarts at {later.start_at} before recovery ends "
                    f"{earlier.recovered_at}"
                )

    if policy_reschedule != "rolling":
        violations.append(f"unsupported reschedule policy {policy_reschedule}")
    if policy_adjacency != "serial":
        violations.append(f"unsupported adjacency policy {policy_adjacency}")
    return violations
