"""Tests for the rolling, windowed defrost scheduler."""

from __future__ import annotations

import json
import tempfile
import unittest

from coldstore.console.seed import seed_defaults
from coldstore.console.server import Server
from coldstore.room import RoomRegistry
from coldstore.scheduler import (
    MAX_CANDIDATES,
    Demand,
    Scheduler,
    Window,
    solve,
    validate_windows,
)
from coldstore.store import Store


def _demand(room_id: str, temp: float = -8.0, available_at: int = 0) -> Demand:
    return Demand(
        room_id=room_id,
        temp=temp,
        defrost_seconds=100,
        recovery_seconds=50,
        available_at=available_at,
    )


class WindowSolveCase(unittest.TestCase):
    def test_serial_placement_when_cap_is_one(self) -> None:
        demands = [_demand("r1"), _demand("r2"), _demand("r3")]
        windows, deferred, stats = solve(demands, [], 0, 1000, 1)
        self.assertEqual([window.start for window in windows], [0, 100, 200])
        self.assertEqual([window.recovery_end for window in windows], [150, 250, 350])
        self.assertEqual(deferred, [])
        self.assertFalse(stats.capped)
        self.assertEqual(validate_windows(windows, 1), [])

    def test_parallel_placement_up_to_the_cap(self) -> None:
        demands = [_demand("r1"), _demand("r2"), _demand("r3")]
        windows, _, _ = solve(demands, [], 0, 1000, 2)
        self.assertEqual([window.start for window in windows], [0, 0, 100])
        self.assertEqual(validate_windows(windows, 2), [])

    def test_queue_follows_recovery_availability_then_temp(self) -> None:
        demands = [_demand("r1", temp=-8.0, available_at=300), _demand("r2", temp=-6.0)]
        windows, _, _ = solve(demands, [], 0, 1000, 1)
        self.assertEqual([window.room_id for window in windows], ["r2", "r1"])
        self.assertEqual(windows[1].start, 300)

    def test_committed_windows_block_capacity(self) -> None:
        committed = [Window("r9", 0, 100, 150, "committed in-progress")]
        windows, _, _ = solve([_demand("r1")], committed, 0, 1000, 1)
        self.assertEqual(windows[0].start, 100)
        self.assertEqual(validate_windows([*committed, *windows], 1), [])

    def test_horizon_full_defers_with_reason(self) -> None:
        demands = [_demand("r1"), _demand("r2")]
        windows, deferred, _ = solve(demands, [], 0, 150, 1)
        self.assertEqual(len(windows), 1)
        self.assertEqual([entry.room_id for entry in deferred], ["r2"])
        self.assertEqual(deferred[0].reason, "horizon full")

    def test_solver_cap_truncates_and_checks_stay_bounded(self) -> None:
        demands = [_demand(f"r{index}") for index in range(MAX_CANDIDATES + 6)]
        windows, deferred, stats = solve(demands, [], 0, 10_000_000, 1)
        self.assertTrue(stats.capped)
        self.assertEqual(len(windows), MAX_CANDIDATES)
        self.assertEqual(len([entry for entry in deferred if entry.reason == "solver cap"]), 6)
        # Each demand probes at most placed + 1 times, so the total stays
        # below n * (n + 1) / 2 + n for n candidates.
        bound = MAX_CANDIDATES * (MAX_CANDIDATES + 1) // 2 + MAX_CANDIDATES
        self.assertLessEqual(stats.checks, bound)

    def test_validate_flags_recovery_overlap_per_room(self) -> None:
        windows = [
            Window("r1", 0, 100, 150, "a"),
            Window("r1", 120, 220, 270, "b"),
        ]
        violations = validate_windows(windows, 4)
        self.assertEqual(len(violations), 1)
        self.assertIn("recovery", violations[0])

    def test_validate_flags_concurrency_peak(self) -> None:
        windows = [
            Window("r1", 0, 100, 150, "a"),
            Window("r2", 0, 100, 150, "b"),
        ]
        self.assertEqual(validate_windows(windows, 2), [])
        violations = validate_windows(windows, 1)
        self.assertEqual(len(violations), 1)
        self.assertIn("concurrency", violations[0])

    def test_validate_allows_touching_recovery_boundary(self) -> None:
        windows = [
            Window("r1", 0, 100, 150, "a"),
            Window("r1", 150, 250, 300, "b"),
        ]
        self.assertEqual(validate_windows(windows, 1), [])


class SchedulerReplanCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.scheduler = Scheduler(self.store)
        self.rooms = RoomRegistry()

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def test_replan_persists_a_consistent_plan(self) -> None:
        self.rooms.add_room("r1", 1, -8.0)
        self.rooms.add_room("r2", 2, -9.0)
        self.rooms.add_room("r3", 3, -20.0)
        plan = self.scheduler.replan(self.rooms, reason="threshold", now=1000)
        self.assertTrue(plan.consistent())
        self.assertEqual(plan.reason, "threshold")
        self.assertEqual(sorted(window.room_id for window in plan.windows), ["r1", "r2"])
        self.assertTrue(all(window.start >= 1000 for window in plan.windows))
        self.assertEqual(plan.stats.placed, 2)

        latest = self.scheduler.latest_plan()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.plan_id, plan.plan_id)
        self.assertEqual(len(self.scheduler.plans()), 1)

        state = self.scheduler.state(self.rooms)
        self.assertEqual(state.planned_windows, 2)
        self.assertEqual(state.latest_plan_id, plan.plan_id)
        self.assertEqual(state.max_concurrent, 2)

    def test_defrosting_room_is_anchored_as_committed(self) -> None:
        self.rooms.add_room("r1", 1, -8.0)
        self.rooms.add_room("r2", 2, -9.0)
        self.rooms.set_defrosting("r1", True)
        self.scheduler.set_max_concurrent(1)
        plan = self.scheduler.replan(self.rooms, now=1000)
        self.assertEqual([window.room_id for window in plan.committed], ["r1"])
        self.assertEqual(plan.committed[0].start, 1000)
        self.assertEqual([window.room_id for window in plan.windows], ["r2"])
        self.assertEqual(plan.windows[0].start, plan.committed[0].end)
        self.assertTrue(plan.consistent())

    def test_recovery_tail_defers_the_next_window(self) -> None:
        self.rooms.add_room("r1", 1, -8.0)
        self.scheduler.note_start("r1", 100)
        record = self.scheduler.note_finish("r1", 200, -8.0)
        self.assertIsNotNone(record)
        until = 200 + self.scheduler.recovery_seconds_for(-8.0)
        self.assertEqual(record.recovery_until, until)

        plan = self.scheduler.replan(self.rooms, now=150)
        self.assertEqual(plan.windows[0].start, until)
        self.assertTrue(plan.consistent())

    def test_execution_record_flags_recovery_breach(self) -> None:
        self.scheduler.note_start("r1", 100)
        first = self.scheduler.note_finish("r1", 200, -8.0)
        self.assertTrue(first.recovery_respected)
        self.assertEqual(first.actual_start, 100)
        self.assertEqual(first.actual_end, 200)

        # Restarting inside the recovery tail is recorded as a breach.
        self.scheduler.note_start("r1", 250)
        second = self.scheduler.note_finish("r1", 300, -8.0)
        self.assertFalse(second.recovery_respected)

        # A bare finish without a start is not an execution.
        self.assertIsNone(self.scheduler.note_finish("r1", 400, -8.0))
        self.assertEqual(len(self.scheduler.executions()), 2)

    def test_execution_matches_the_latest_plan_window(self) -> None:
        self.rooms.add_room("r1", 1, -8.0)
        plan = self.scheduler.replan(self.rooms, now=1000)
        self.scheduler.note_start("r1", 1100)
        record = self.scheduler.note_finish("r1", 1200, -8.0)
        self.assertEqual(record.plan_id, plan.plan_id)
        self.assertEqual(record.planned_start, plan.windows[0].start)
        self.assertEqual(record.planned_end, plan.windows[0].end)

    def test_plan_history_is_capped(self) -> None:
        self.rooms.add_room("r1", 1, -8.0)
        for index in range(55):
            self.scheduler.replan(self.rooms, reason=f"r{index}", now=1000 + index)
        plans = self.scheduler.plans()
        self.assertEqual(len(plans), 50)
        self.assertEqual(plans[-1].reason, "r54")
        self.assertEqual(plans[0].reason, "r5")

    def test_capacity_and_recovery_settings_round_trip(self) -> None:
        self.assertEqual(self.scheduler.max_concurrent(), 2)
        self.scheduler.set_max_concurrent(3)
        self.assertEqual(self.scheduler.max_concurrent(), 3)
        with self.assertRaises(ValueError):
            self.scheduler.set_max_concurrent(0)

        self.scheduler.set_recovery(120, 5)
        self.assertEqual(self.scheduler.recovery_seconds_for(-10.0), 170)
        with self.assertRaises(ValueError):
            self.scheduler.set_recovery(-1, 5)
        with self.assertRaises(ValueError):
            self.scheduler.replan(self.rooms, now=1000, horizon=10)


class ConsoleWindowCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        seed_defaults(self.store)
        self.server = Server(self.store)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def call(self, method: str, path: str, payload: dict | None = None):
        response = self.server.dispatch(method, path, payload)
        body = response.body.decode("utf-8")
        parsed = json.loads(body) if response.content_type.startswith("application/json") else body
        return response.status, parsed

    def add_room(self, room_id: str, zone: int, temp: float) -> None:
        status, _ = self.call("POST", "/room/add", {"id": room_id, "zone": zone, "temp": temp})
        self.assertEqual(status, 200)

    def test_replan_windows_history_and_audit(self) -> None:
        self.add_room("r1", 1, -8.0)
        self.add_room("r2", 2, -9.0)

        status, body = self.call("POST", "/schedule/replan", {"reason": "manual", "now": 1000})
        self.assertEqual(status, 200)
        self.assertTrue(body["consistent"])
        plan_id = body["plan"]["plan_id"]
        self.assertEqual(len(body["plan"]["windows"]), 2)
        self.assertEqual(body["plan"]["stats"]["placed"], 2)

        status, body = self.call("GET", "/schedule/windows")
        self.assertEqual(body["plan"]["plan_id"], plan_id)

        status, body = self.call("GET", "/schedule/history")
        self.assertEqual(body["count"], 1)
        self.assertEqual(body["plans"][0]["plan_id"], plan_id)

        status, body = self.call("POST", "/audit/filter", {"kind": "schedule"})
        self.assertEqual(len(body["entries"]), 1)
        detail = json.loads(body["entries"][0]["detail"])
        self.assertEqual(detail["plan"], plan_id)
        self.assertEqual(detail["reason"], "manual")
        self.assertTrue(detail["consistent"])

        status, body = self.call("GET", "/schedule")
        self.assertEqual(body["planned_windows"], 2)
        self.assertEqual(body["latest_plan_id"], plan_id)

    def test_capacity_and_recovery_routes_validate(self) -> None:
        status, body = self.call("POST", "/schedule/capacity", {"capacity": 3})
        self.assertEqual(body["max_concurrent"], 3)
        status, error = self.call("POST", "/schedule/capacity", {"capacity": 0})
        self.assertEqual(status, 400)
        self.assertIn("capacity", error["error"])
        status, error = self.call("POST", "/schedule/capacity", {"capacity": 99})
        self.assertEqual(status, 400)

        status, body = self.call("POST", "/schedule/recovery", {"base": 120, "per_degree": 5})
        self.assertEqual(body["recovery_base"], 120)
        status, error = self.call("POST", "/schedule/recovery", {"base": -1, "per_degree": 5})
        self.assertEqual(status, 400)
        self.assertIn("recovery", error["error"])

        status, error = self.call("POST", "/schedule/replan", {"horizon": 10})
        self.assertEqual(status, 400)
        self.assertIn("horizon", error["error"])

    def test_execution_flow_records_actuals_and_exports(self) -> None:
        self.add_room("r1", 1, -8.0)
        self.call("POST", "/fan/add", {"id": "f1", "role": "primary"})
        _, body = self.call("POST", "/schedule/replan", {"reason": "manual", "now": 1000})
        plan_id = body["plan"]["plan_id"]

        status, _ = self.call("POST", "/defrost/start", {"id": "r1"})
        self.assertEqual(status, 200)
        status, _ = self.call("POST", "/defrost/finish", {"id": "r1"})
        self.assertEqual(status, 200)

        status, body = self.call("GET", "/schedule/executions")
        self.assertEqual(body["count"], 1)
        record = body["executions"][0]
        self.assertEqual(record["room_id"], "r1")
        self.assertEqual(record["plan_id"], plan_id)
        self.assertTrue(record["recovery_respected"])
        self.assertGreater(record["actual_end"], record["actual_start"] - 1)

        status, body = self.call("POST", "/audit/filter", {"kind": "defrost-start"})
        self.assertEqual(len(body["entries"]), 1)
        status, body = self.call("POST", "/audit/filter", {"kind": "defrost-finish"})
        self.assertEqual(len(body["entries"]), 1)
        detail = json.loads(body["entries"][0]["detail"])
        self.assertEqual(detail["plan"], plan_id)

        status, text = self.call("GET", "/export/schedule")
        self.assertEqual(status, 200)
        self.assertIn("plan_id,created_at,reason,room_id", text)
        self.assertIn("r1", text)
        status, text = self.call("GET", "/export/defrost")
        self.assertIn("room_id,plan_id,planned_start", text)
        self.assertIn(plan_id, text)

    def test_history_and_executions_survive_reopen(self) -> None:
        self.add_room("r1", 1, -8.0)
        self.call("POST", "/schedule/replan", {"reason": "manual", "now": 1000})
        self.call("POST", "/defrost/start", {"id": "r1"})
        self.call("POST", "/defrost/finish", {"id": "r1"})
        self.store.close()

        reopened = Store.open(f"{self._tmp.name}/state.json")
        server = Server(reopened)
        response = server.dispatch("GET", "/schedule/history")
        body = json.loads(response.body.decode("utf-8"))
        self.assertEqual(body["count"], 1)
        response = server.dispatch("GET", "/schedule/executions")
        body = json.loads(response.body.decode("utf-8"))
        self.assertEqual(body["count"], 1)
        response = server.dispatch("GET", "/export/schedule")
        self.assertIn("plan-", response.body.decode("utf-8"))
        reopened.close()


if __name__ == "__main__":
    unittest.main()
