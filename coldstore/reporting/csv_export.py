"""CSV rendering for operator exports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from coldstore.audit import Auditor
    from coldstore.console.telemetry import Telemetry
    from coldstore.scheduler import Scheduler

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


def schedule_export(scheduler: Scheduler) -> tuple[list[str], list[list[object]]]:
    """Columns and rows for every retained defrost plan window."""

    columns = [
        "plan_id",
        "created_at",
        "reason",
        "room_id",
        "start",
        "end",
        "recovery_end",
        "window_reason",
        "committed",
    ]
    rows: list[list[object]] = []
    for plan in scheduler.plans():
        for window in plan.committed:
            rows.append(
                [
                    plan.plan_id,
                    plan.created_at,
                    plan.reason,
                    window.room_id,
                    window.start,
                    window.end,
                    window.recovery_end,
                    window.reason,
                    "yes",
                ]
            )
        for window in plan.windows:
            rows.append(
                [
                    plan.plan_id,
                    plan.created_at,
                    plan.reason,
                    window.room_id,
                    window.start,
                    window.end,
                    window.recovery_end,
                    window.reason,
                    "no",
                ]
            )
    return columns, rows


def defrost_export(scheduler: Scheduler) -> tuple[list[str], list[list[object]]]:
    """Columns and rows for the recorded defrost executions."""

    columns = [
        "room_id",
        "plan_id",
        "planned_start",
        "planned_end",
        "actual_start",
        "actual_end",
        "recovery_until",
        "recovery_respected",
    ]
    rows: list[list[object]] = [
        [
            record.room_id,
            record.plan_id,
            record.planned_start,
            record.planned_end,
            record.actual_start,
            record.actual_end,
            record.recovery_until,
            record.recovery_respected,
        ]
        for record in scheduler.executions()
    ]
    return columns, rows
