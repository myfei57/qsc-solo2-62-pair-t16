"""Defrost scheduling derived from room temperature."""

from .ledger import ExecutionRecord
from .planner import DEFAULT_THRESHOLD, THRESHOLD_KEY, Scheduler
from .report import (
    ScheduleEntry,
    ScheduleState,
    validate_capacity,
    validate_horizon,
    validate_recovery,
    validate_threshold,
)
from .windows import (
    DEFAULT_HORIZON_SECONDS,
    DEFAULT_MAX_CONCURRENT,
    DEFAULT_RECOVERY_BASE_SECONDS,
    DEFAULT_RECOVERY_PER_DEGREE_SECONDS,
    MAX_CANDIDATES,
    Demand,
    Deferred,
    SolveStats,
    Window,
    WindowPlan,
    solve,
    validate_windows,
)

__all__ = [
    "DEFAULT_HORIZON_SECONDS",
    "DEFAULT_MAX_CONCURRENT",
    "DEFAULT_RECOVERY_BASE_SECONDS",
    "DEFAULT_RECOVERY_PER_DEGREE_SECONDS",
    "DEFAULT_THRESHOLD",
    "MAX_CANDIDATES",
    "THRESHOLD_KEY",
    "Demand",
    "Deferred",
    "ExecutionRecord",
    "ScheduleEntry",
    "ScheduleState",
    "Scheduler",
    "SolveStats",
    "Window",
    "WindowPlan",
    "solve",
    "validate_capacity",
    "validate_horizon",
    "validate_recovery",
    "validate_threshold",
    "validate_windows",
]
