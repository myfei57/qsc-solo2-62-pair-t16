"""Append only, bounded persistence for defrost plans and executions.

Plans and execution records are retained so the schedule and its exports
can be reviewed after the fact. Both lists are capped: the oldest entries
drop off once the cap is reached, which keeps the store document bounded.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from coldstore.store.commands import load_commands
from coldstore.store.store import Store

PLANS_KEY = "schedule:plans"
EXECUTIONS_KEY = "schedule:executions"
RECOVERY_KEY_PREFIX = "schedule:recovery:"
EXEC_START_PREFIX = "schedule:exec-start:"
MAX_STORED_PLANS = 50
MAX_STORED_EXECUTIONS = 200


@dataclass(frozen=True)
class ExecutionRecord:
    """One completed defrost execution matched against the latest plan."""

    room_id: str
    plan_id: str
    planned_start: int
    planned_end: int
    actual_start: int
    actual_end: int
    recovery_until: int
    recovery_respected: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "plan_id": self.plan_id,
            "planned_start": self.planned_start,
            "planned_end": self.planned_end,
            "actual_start": self.actual_start,
            "actual_end": self.actual_end,
            "recovery_until": self.recovery_until,
            "recovery_respected": self.recovery_respected,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ExecutionRecord":
        return cls(
            room_id=str(payload.get("room_id", "")),
            plan_id=str(payload.get("plan_id", "")),
            planned_start=int(payload.get("planned_start", 0)),
            planned_end=int(payload.get("planned_end", 0)),
            actual_start=int(payload.get("actual_start", 0)),
            actual_end=int(payload.get("actual_end", 0)),
            recovery_until=int(payload.get("recovery_until", 0)),
            recovery_respected=bool(payload.get("recovery_respected", False)),
        )


def _append_capped(store: Store, key: str, payload: dict[str, object], cap: int) -> None:
    items = load_commands(store, key)
    items.append(json.dumps(payload, ensure_ascii=False))
    if len(items) > cap:
        items = items[len(items) - cap :]
    store.put(key, json.dumps(items, ensure_ascii=False))


def _load_dicts(store: Store, key: str) -> list[dict[str, object]]:
    documents: list[dict[str, object]] = []
    for item in load_commands(store, key):
        try:
            payload = json.loads(item)
        except ValueError:
            continue
        if isinstance(payload, dict):
            documents.append(payload)
    return documents


def save_plan(store: Store, plan: dict[str, object]) -> None:
    _append_capped(store, PLANS_KEY, plan, MAX_STORED_PLANS)


def load_plan_dicts(store: Store) -> list[dict[str, object]]:
    return _load_dicts(store, PLANS_KEY)


def append_execution(store: Store, record: dict[str, object]) -> None:
    _append_capped(store, EXECUTIONS_KEY, record, MAX_STORED_EXECUTIONS)


def load_execution_dicts(store: Store) -> list[dict[str, object]]:
    return _load_dicts(store, EXECUTIONS_KEY)


def recovery_until(store: Store, room_id: str) -> int:
    """End of the room's recovery tail, zero when it may defrost now."""

    raw, present = store.get(f"{RECOVERY_KEY_PREFIX}{room_id}")
    if not present:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def set_recovery_until(store: Store, room_id: str, at: int) -> None:
    store.put(f"{RECOVERY_KEY_PREFIX}{room_id}", str(at))


def note_exec_start(store: Store, room_id: str, at: int) -> None:
    """Mark the actual start of a defrost so the finish can be matched."""

    store.put(f"{EXEC_START_PREFIX}{room_id}", str(at))


def take_exec_start(store: Store, room_id: str) -> int | None:
    """Read and clear the in-flight start marker, None when absent."""

    raw, present = store.get(f"{EXEC_START_PREFIX}{room_id}")
    if not present:
        return None
    store.delete(f"{EXEC_START_PREFIX}{room_id}")
    try:
        return int(raw)
    except ValueError:
        return None
