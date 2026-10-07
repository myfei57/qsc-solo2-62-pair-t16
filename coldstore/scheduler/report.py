"""Scheduling projections and validation rules."""

from __future__ import annotations

from dataclasses import dataclass

from .windows import (
    MAX_CONCURRENT_LIMIT,
    MAX_HORIZON_SECONDS,
    MAX_RECOVERY_SECONDS,
    MIN_HORIZON_SECONDS,
)


@dataclass(frozen=True)
class ScheduleEntry:
    """One planned defrost with its position and duration."""

    room_id: str
    temp: float
    priority: int
    defrost_seconds: int

    def as_dict(self) -> dict[str, object]:
        return {
            "room_id": self.room_id,
            "temp": self.temp,
            "priority": self.priority,
            "defrost_seconds": self.defrost_seconds,
        }


@dataclass(frozen=True)
class ScheduleState:
    """Threshold, due rooms, the ordered plan and the windowed re-plan."""

    threshold: float
    due: list[str]
    entries: list[ScheduleEntry]
    max_concurrent: int = 0
    recovery_base: int = 0
    recovery_per_degree: int = 0
    planned_windows: int = 0
    latest_plan_id: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "threshold": self.threshold,
            "due": self.due,
            "entries": [entry.as_dict() for entry in self.entries],
            "max_concurrent": self.max_concurrent,
            "recovery_base": self.recovery_base,
            "recovery_per_degree": self.recovery_per_degree,
            "planned_windows": self.planned_windows,
            "latest_plan_id": self.latest_plan_id,
        }


def validate_threshold(value: float) -> None:
    """Reject thresholds outside the supported temperature range."""

    if value < -100:
        raise ValueError("threshold is below the supported range")
    if value > 100:
        raise ValueError("threshold exceeds the supported range")


def validate_capacity(value: int) -> None:
    """Reject concurrency caps outside the supported limit."""

    if value < 1:
        raise ValueError("capacity must be at least one")
    if value > MAX_CONCURRENT_LIMIT:
        raise ValueError("capacity exceeds the supported limit")


def validate_recovery(base: int, per_degree: int) -> None:
    """Reject recovery settings outside the supported range."""

    if base < 0 or per_degree < 0:
        raise ValueError("recovery settings must be non-negative")
    if base > MAX_RECOVERY_SECONDS or per_degree > MAX_RECOVERY_SECONDS:
        raise ValueError("recovery setting exceeds the supported range")


def validate_horizon(value: int) -> None:
    """Reject re-plan horizons outside the supported range."""

    if value < MIN_HORIZON_SECONDS:
        raise ValueError("horizon is below the supported range")
    if value > MAX_HORIZON_SECONDS:
        raise ValueError("horizon exceeds the supported range")
