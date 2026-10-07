"""CSV rendering for operator exports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from coldstore.audit import Auditor
    from coldstore.console.telemetry import Telemetry
    from coldstore.scheduler import TimelinePlan

QUOTE_TRIGGERS = (",", '"', "\n")


def _escape(value: object) -> str:
    text = "" if value is None else str(value)
    if any(trigger in text for trigger in QUOTE_TRIGGERS):
        return '"' + text.replace('"', '""') + '"'
    return text


def to_csv(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Render a header row and body rows as comma separated text."""

    lines = [",".join(_escape(column) for column in columns)]
    for row in rows:
        lines.append(",".join(_escape(value) for value in row))
    return "\n".join(lines) + "\n"


def audit_export(auditor: Auditor) -> tuple[list[str], list[list[str]]]:
    """Columns and rows for the runtime audit trail."""

    columns = ["time", "kind", "detail", "id"]
    rows = [[entry.time, entry.kind, entry.detail, entry.id] for entry in auditor.entries()]
    return columns, rows


def telemetry_export(telemetry: Telemetry) -> tuple[list[str], list[list[object]]]:
    """Columns and rows for the telemetry counters."""

    return ["metric", "value"], [[key, value] for key, value in telemetry.as_dict().items()]


SCHEDULE_COLUMNS = [
    "revision",
    "room_id",
    "ready_at",
    "due_at",
    "start_at",
    "end_at",
    "recovered_at",
    "defrost_seconds",
    "recovery_seconds",
    "late",
    "frozen",
    "reason",
]


def schedule_export(plan: TimelinePlan) -> tuple[list[str], list[list[object]]]:
    """Columns and rows for one revisioned time windowed schedule.

    The policy header and revision are repeated on every row so an exported CSV
    remains self describing when it is mailed or archived on its own.
    """

    rows: list[list[object]] = []
    for slot in plan.slots:
        rows.append(
            [
                plan.revision,
                slot.room_id,
                slot.ready_at,
                slot.due_at,
                slot.start_at,
                slot.end_at,
                slot.recovered_at,
                slot.defrost_seconds,
                slot.recovery_seconds,
                str(slot.late).lower(),
                str(slot.frozen).lower(),
                slot.reason,
            ]
        )
    return list(SCHEDULE_COLUMNS), rows


def schedule_plan_meta(plan: TimelinePlan) -> list[list[str]]:
    """Header rows that carry the revision policy alongside the CSV body."""

    return [
        ["revision", str(plan.revision)],
        ["generated_at", str(plan.generated_at)],
        ["trigger", plan.trigger],
        ["reason", plan.reason],
        ["policy_reschedule", plan.policy_reschedule],
        ["policy_adjacency", plan.policy_adjacency],
        ["concurrency", str(plan.concurrency)],
        ["frozen_until", str(plan.frozen_until)],
        ["horizon_end", str(plan.horizon_end)],
        ["consistent", str(plan.consistent).lower()],
        ["violations", "; ".join(plan.violations)],
    ]
