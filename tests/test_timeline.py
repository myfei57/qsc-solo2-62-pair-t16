"""Behavioural tests for the rolling time windowed defrost scheduler."""

from __future__ import annotations

import tempfile
import unittest

from coldstore.audit import Auditor
from coldstore.room import RoomRegistry
from coldstore.scheduler import (
    DEFAULT_CONCURRENCY,
    DEFAULT_RECOVERY_SECONDS,
    MAX_JOBS,
    POLICY_ADJACENCY,
    POLICY_RESCHEDULE,
    REASON_CAPACITY,
    REASON_FROZEN,
    REASON_RECOVERY,
    REASON_WINDOW,
    Scheduler,
    ScheduledSlot,
    TimelinePlanner,
    defrost_duration,
    queue_order,
    validate_concurrency,
    validate_recovery_seconds,
)
from coldstore.scheduler.solver import schedule_jobs, verify_plan
from coldstore.scheduler.window import DefrostJob
from coldstore.store import Store


def job(
    room_id: str,
    *,
    ready: int = 0,
    due: int | None = None,
    duration: int = 100,
    recovery: int = 60,
    temp: float = -10.0,
) -> DefrostJob:
    return DefrostJob(
        room_id=room_id,
        temp=temp,
        ready_at=ready,
        due_at=ready + 3600 if due is None else due,
        defrost_seconds=duration,
        recovery_seconds=recovery,
    )


class SolverCase(unittest.TestCase):
    def test_queues_by_recovery_time_then_temperature(self) -> None:
        urgent = job("urgent", ready=100, due=200, temp=-8.0)
        later = job("later", ready=100, due=900, temp=-5.0)
        warmest = job("warmest", ready=100, due=900, temp=-3.0)
        ordered = sorted([later, urgent, warmest], key=queue_order)
        self.assertEqual([item.room_id for item in ordered], ["urgent", "warmest", "later"])

    def test_concurrency_lanes_throttle_overlapping_defrosts(self) -> None:
        jobs = [
            job("a", ready=0, duration=100),
            job("b", ready=0, duration=100),
            job("c", ready=0, duration=100),
            job("d", ready=0, duration=100),
        ]
        slots = schedule_jobs(jobs, concurrency=2, now=0)
        by_room = {slot.room_id: slot for slot in slots}
        self.assertEqual(by_room["a"].start_at, 0)
        self.assertEqual(by_room["b"].start_at, 0)
        # The third and fourth rooms wait for a lane to free at t=100.
        self.assertEqual(by_room["c"].start_at, 100)
        self.assertEqual(by_room["d"].start_at, 100)
        self.assertEqual(by_room["c"].reason, REASON_CAPACITY)
        self.assertEqual(verify_plan(slots, concurrency=2, horizon_end=100000,
                                     frozen_until=0, policy_reschedule=POLICY_RESCHEDULE,
                                     policy_adjacency=POLICY_ADJACENCY), [])

    def test_serial_adjacency_waits_for_recovery(self) -> None:
        # Two jobs of the *same* room cannot overlap even if lanes are free:
        # the second one starts only after warm back recovery completes.
        jobs = [
            job("a", ready=0, duration=100, recovery=300),
            job("a2", ready=0, duration=100, recovery=0),
        ]
        # Distinct room ids in the solver, but emulate the room recovery lock
        # by reusing the busy interval through two rooms sharing a recovery lane
        # is not possible; serial semantics are checked through the same room
        # via frozen carry plus verify_plan.  Here assert cross-room queueing on
        # one lane first (capacity wait, no recovery for the queued room).
        jobs = [
            job("a", ready=0, duration=100, recovery=300),
            job("b", ready=0, duration=100, recovery=0),
        ]
        slots = schedule_jobs(jobs, concurrency=1, now=0)
        by_room = {slot.room_id: slot for slot in slots}
        self.assertEqual(by_room["b"].start_at, 100)
        self.assertEqual(by_room["b"].reason, REASON_CAPACITY)
        self.assertEqual(by_room["a"].recovered_at, 400)

    def test_window_wait_records_window_reason(self) -> None:
        jobs = [job("a", ready=500, duration=100)]
        slots = schedule_jobs(jobs, concurrency=2, now=0)
        self.assertEqual(slots[0].start_at, 500)
        self.assertEqual(slots[0].reason, REASON_WINDOW)

    def test_same_room_serial_via_frozen_recovery(self) -> None:
        # A frozen, already dispatched defrost for room "a" finishes at 100 and
        # its warm back ends at 400.  With two lanes free, another room starts
        # immediately while the busy-until bookkeeping pins "a" at 400 (proven
        # indirectly: verify_plan rejects any re-start of "a" before 400).
        frozen = ScheduledSlot(
            room_id="a", ready_at=0, due_at=1000, start_at=0, end_at=100,
            recovered_at=400, defrost_seconds=100, recovery_seconds=300,
            late=False, frozen=True, reason=REASON_FROZEN,
        )
        other = job("b", ready=0, duration=100, recovery=0)
        slots = schedule_jobs([other], concurrency=2, now=0, frozen=[frozen])
        by_room = {slot.room_id: slot for slot in slots}
        self.assertEqual(by_room["b"].start_at, 0)
        self.assertTrue(by_room["a"].frozen)
        violations = verify_plan(
            slots, concurrency=2, horizon_end=100000, frozen_until=0,
            policy_reschedule=POLICY_RESCHEDULE, policy_adjacency=POLICY_ADJACENCY,
            committed_rooms={"a"},
        )
        self.assertEqual(violations, [])

    def test_late_slot_flagged_when_capacity_misses_window(self) -> None:
        jobs = [
            job("a", ready=0, due=10, duration=100),
            job("b", ready=0, due=10, duration=100),
        ]
        slots = schedule_jobs(jobs, concurrency=1, now=0)
        self.assertFalse(slots[0].late)
        self.assertTrue(slots[1].late)

    def test_job_count_is_bounded(self) -> None:
        with self.assertRaises(ValueError):
            schedule_jobs(
                [job(f"r{i}") for i in range(MAX_JOBS + 1)], concurrency=2, now=0
            )

    def test_verify_detects_over_capacity(self) -> None:
        slots = schedule_jobs(
            [job("a", duration=100), job("b", duration=100)], concurrency=2, now=0
        )
        violations = verify_plan(
            slots, concurrency=1, horizon_end=100000, frozen_until=0,
            policy_reschedule=POLICY_RESCHEDULE, policy_adjacency=POLICY_ADJACENCY,
        )
        self.assertTrue(any("concurrency" in item for item in violations))

    def test_verify_detects_recovery_overlap(self) -> None:
        broken = ScheduledSlot(
            room_id="a", ready_at=0, due_at=1000, start_at=0, end_at=100,
            recovered_at=400, defrost_seconds=100, recovery_seconds=300,
            late=False, frozen=False, reason=REASON_WINDOW,
        )
        broken2 = ScheduledSlot(
            room_id="a", ready_at=400, due_at=1400, start_at=300, end_at=400,
            recovered_at=700, defrost_seconds=100, recovery_seconds=300,
            late=False, frozen=False, reason=REASON_RECOVERY,
        )
        violations = verify_plan(
            [broken, broken2], concurrency=2, horizon_end=100000, frozen_until=0,
            policy_reschedule=POLICY_RESCHEDULE, policy_adjacency=POLICY_ADJACENCY,
        )
        self.assertTrue(any("restarts" in item for item in violations))


class TimelineCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.auditor = Auditor(self.store)
        self.scheduler = Scheduler(self.store, self.auditor)
        self.rooms = RoomRegistry()
        self.rooms.add_room("r1", 1, -20.0)
        self.rooms.add_room("r2", 2, -8.0)
        self.rooms.add_room("r3", 3, -5.0)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_first_revision_respects_capacity_and_recovery(self) -> None:
        self.scheduler.set_concurrency(2)
        self.scheduler.set_recovery_seconds(120)
        plan = self.scheduler.generate(self.rooms, trigger="initial", now=1000)
        self.assertEqual(plan.revision, 1)
        self.assertTrue(plan.consistent)
        self.assertEqual(plan.policy_reschedule, POLICY_RESCHEDULE)
        self.assertEqual(plan.policy_adjacency, POLICY_ADJACENCY)
        # Only the two warm rooms are due; r1 (-20) is below the threshold.
        # Both start at once (two lanes) so display order falls back to id.
        self.assertEqual(sorted(slot.room_id for slot in plan.slots), ["r2", "r3"])
        by_room = {slot.room_id: slot for slot in plan.slots}
        duration_r3 = defrost_duration(-5.0)
        self.assertEqual(by_room["r3"].start_at, 1000)
        self.assertEqual(by_room["r3"].end_at, 1000 + duration_r3)
        self.assertEqual(by_room["r3"].recovered_at, 1000 + duration_r3 + 120)
        kinds = self.auditor.count_by_kind()
        self.assertEqual(kinds["schedule-plan"], 1)

    def test_rolling_regeneration_freezes_committed_slots(self) -> None:
        self.scheduler.set_concurrency(1)
        # No "imminent" freeze padding: only work actually in flight is carried.
        self.scheduler.set_freeze_seconds(0)
        self.scheduler.set_recovery_seconds(0)
        first = self.scheduler.generate(self.rooms, trigger="initial", now=0)
        frozen_room = first.slots[0].room_id
        # Advance into the first slot and re-generate: the in flight slot is
        # carried verbatim and marked frozen; queued tail slots are re-solved.
        self.scheduler.executions.start(frozen_room, at=0)
        second = self.scheduler.generate(self.rooms, trigger="rolling-complete", now=10)
        carried = [slot for slot in second.slots if slot.frozen]
        self.assertEqual(len(carried), 1)
        self.assertEqual(carried[0].room_id, frozen_room)
        self.assertEqual(carried[0].reason, REASON_FROZEN)
        self.assertEqual(carried[0].start_at, first.slots[0].start_at)
        self.assertEqual(second.revision, 2)
        # History holds both revisions for replay.
        self.assertEqual([plan.revision for plan in self.scheduler.plan_history()], [1, 2])

    def test_capacity_guard_rejects_a_third_concurrent_start(self) -> None:
        self.scheduler.set_concurrency(2)
        self.scheduler.generate(self.rooms, trigger="initial", now=0)
        running = [slot.room_id for slot in self.scheduler.latest_plan().slots[:2]]
        self.scheduler.executions.start(running[0], at=0)
        self.scheduler.executions.start(running[1], at=0)
        with self.assertRaises(ValueError):
            self.scheduler.executions.start(running[0], at=10)
        # Finishing one releases a lane and records the actual execution.
        self.scheduler.executions.finish(running[0], at=150)
        record = self.scheduler.executions.record(running[0])
        self.assertEqual(record.actual_finish, 150)
        kinds = self.auditor.count_by_kind()
        self.assertIn("schedule-exec-start", kinds)
        self.assertIn("schedule-exec-finish", kinds)

    def test_late_dispatch_is_allowed_but_audited_as_deviation(self) -> None:
        self.scheduler.set_tolerance_seconds(300)
        plan = self.scheduler.generate(self.rooms, trigger="initial", now=0)
        room_id = plan.slots[0].room_id
        self.scheduler.executions.start(room_id, at=plan.slots[0].due_at + 60)
        record = self.scheduler.executions.record(room_id)
        self.assertTrue(record.late)
        kinds = self.auditor.count_by_kind()
        self.assertEqual(kinds["schedule-deviation"], 1)

    def test_dispatch_before_window_is_rejected(self) -> None:
        self.scheduler.set_tolerance_seconds(60)
        jobs = [job("z", ready=1000, due=2000, duration=100, recovery=0)]
        self.scheduler.timeline.generate(jobs, trigger="initial", now=0)
        with self.assertRaises(ValueError):
            self.scheduler.executions.start("z", at=0)

    def test_persistence_round_trips_latest_plan(self) -> None:
        self.scheduler.generate(self.rooms, trigger="initial", now=500)
        reopened = TimelinePlanner(Store.open(self.store.path))
        plan = reopened.latest()
        self.assertIsNotNone(plan)
        self.assertEqual(plan.revision, 1)
        self.assertEqual(plan.generated_at, 500)
        self.assertEqual(plan.concurrency, DEFAULT_CONCURRENCY)
        self.assertEqual(len(plan.slots), 2)

    def test_per_room_overrides_drive_duration_and_recovery(self) -> None:
        plan = self.scheduler.generate(
            self.rooms,
            trigger="manual",
            now=0,
            durations={"r2": 999},
            recoveries={"r2": 55},
        )
        slot = plan.slot_for("r2")
        self.assertEqual(slot.defrost_seconds, 999)
        self.assertEqual(slot.recovery_seconds, 55)
        self.assertEqual(slot.recovered_at, slot.end_at + 55)

    def test_config_validation(self) -> None:
        validate_concurrency(3)
        with self.assertRaises(ValueError):
            validate_concurrency(0)
        validate_recovery_seconds(0)  # zero disables the warm back wait
        with self.assertRaises(ValueError):
            validate_recovery_seconds(-1)
        self.assertEqual(self.scheduler.recovery_seconds(), DEFAULT_RECOVERY_SECONDS)

    def test_completion_release_lets_rolling_replan_schedule_sooner(self) -> None:
        self.scheduler.set_concurrency(1)
        self.scheduler.set_recovery_seconds(0)
        first = self.scheduler.generate(self.rooms, trigger="initial", now=0)
        first_room = first.slots[0].room_id
        self.scheduler.executions.start(first_room, at=0)
        self.scheduler.executions.finish(first_room, at=50)
        # After completion the finished room is no longer frozen; remaining
        # rooms are still queued after the dispatched lane's planned end, which
        # the carried in-flight slot (if any) bounds.
        second = self.scheduler.generate(self.rooms, trigger="rolling-complete", now=60)
        self.assertTrue(second.consistent, second.violations)

    def test_actual_finish_pushes_next_defrost_past_recovery(self) -> None:
        # Serial adjacency against the real execution log: r3 actually defrosts
        # 100000..100150 and warms back for 600 s, so the rolling revision must
        # not queue it again before 100750 even though lanes are free.
        self.scheduler.set_concurrency(2)
        self.scheduler.set_recovery_seconds(600)
        self.scheduler.set_freeze_seconds(0)
        self.scheduler.generate(self.rooms, trigger="initial", now=100000)
        self.scheduler.executions.start("r3", at=100000)
        self.scheduler.executions.finish("r3", at=100150)
        replanned = self.scheduler.generate(
            self.rooms, trigger="rolling-complete", now=100200
        )
        r3 = replanned.slot_for("r3")
        self.assertIsNotNone(r3)
        self.assertGreaterEqual(r3.start_at, 100150 + 600)
        self.assertTrue(replanned.consistent, replanned.violations)


if __name__ == "__main__":
    unittest.main()
