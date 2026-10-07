"""Default values written on first boot."""

from __future__ import annotations

from coldstore.ammonia import Monitor
from coldstore.ammonia.monitor import THRESHOLD_KEY as AMMONIA_THRESHOLD_KEY
from coldstore.evap import Condenser
from coldstore.evap.condenser import BASELINE_KEY as EVAP_BASELINE_KEY
from coldstore.evap.condenser import DEFAULT_BASELINE as EVAP_BASELINE
from coldstore.fan import FanSet
from coldstore.load import DEFAULT_BASELINE, LoadCalculator
from coldstore.load.calc import BASELINE_KEY as LOAD_BASELINE_KEY
from coldstore.load.calc import COEFFICIENT_KEY, DEFAULT_COEFFICIENT
from coldstore.quota import Accumulator
from coldstore.scheduler import Scheduler
from coldstore.scheduler.planner import THRESHOLD_KEY as SCHEDULE_THRESHOLD_KEY
from coldstore.scheduler.planner import DEFAULT_THRESHOLD as SCHEDULE_THRESHOLD
from coldstore.scheduler.window import (
    CONCURRENCY_KEY,
    DEFAULT_CONCURRENCY,
    DEFAULT_FREEZE_SECONDS,
    DEFAULT_HORIZON_SECONDS,
    DEFAULT_RECOVERY_SECONDS,
    DEFAULT_RESPONSE_SECONDS,
    DEFAULT_TOLERANCE_SECONDS,
    FREEZE_KEY,
    HORIZON_KEY,
    RECOVERY_KEY,
    RESPONSE_KEY,
    TOLERANCE_KEY,
)
from coldstore.store import Store, append_event, has_key
from coldstore.temp import Collector
from coldstore.temp.collector import WINDOW_KEY
from coldstore.temp.collector import DEFAULT_WINDOW as TEMP_WINDOW

from .history import EVENT_KEY


def seed_defaults(store: Store) -> None:
    """Make sure each mutable component has a usable starting value."""

    collector = Collector(store)
    if not has_key(store, WINDOW_KEY):
        collector.set_window(TEMP_WINDOW)

    monitor = Monitor(store)
    if not has_key(store, AMMONIA_THRESHOLD_KEY):
        monitor.set_threshold(monitor.threshold())

    calculator = LoadCalculator(store)
    if not has_key(store, LOAD_BASELINE_KEY):
        calculator.set_baseline(DEFAULT_BASELINE)
    if not has_key(store, COEFFICIENT_KEY):
        calculator.set_coefficient(DEFAULT_COEFFICIENT)

    condenser = Condenser(store, FanSet())
    if not has_key(store, EVAP_BASELINE_KEY):
        condenser.set_baseline(EVAP_BASELINE)

    accumulator = Accumulator(store)
    if accumulator.value() < 0:
        accumulator.reset()

    scheduler = Scheduler(store)
    if not has_key(store, SCHEDULE_THRESHOLD_KEY):
        scheduler.set_threshold(SCHEDULE_THRESHOLD)

    timeline_defaults = (
        (CONCURRENCY_KEY, DEFAULT_CONCURRENCY, scheduler.set_concurrency),
        (RECOVERY_KEY, DEFAULT_RECOVERY_SECONDS, scheduler.set_recovery_seconds),
        (RESPONSE_KEY, DEFAULT_RESPONSE_SECONDS, scheduler.set_response_seconds),
        (FREEZE_KEY, DEFAULT_FREEZE_SECONDS, scheduler.set_freeze_seconds),
        (TOLERANCE_KEY, DEFAULT_TOLERANCE_SECONDS, scheduler.set_tolerance_seconds),
        (HORIZON_KEY, DEFAULT_HORIZON_SECONDS, scheduler.set_horizon_seconds),
    )
    for key, default, apply_default in timeline_defaults:
        if not has_key(store, key):
            apply_default(default)

    append_event(store, EVENT_KEY, "seed", "defaults")
