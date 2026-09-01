"""Time source.

Two modes:
  * VirtualClock - deterministic, advances by a fixed step per tick. Chaos tests and replays
    use this so "waits" and "timeouts" cost microseconds and results are reproducible.
  * WallClock    - real time, used when the runtime runs continuously between Arena turns.

The kernel never calls time.time() directly; injectability is what makes replay equality
(journal.fold == live state) testable, which is what makes sandbox-recycle recovery provable.
"""
from __future__ import annotations

import abc
import itertools
import time
from dataclasses import dataclass, field


class Clock(abc.ABC):
    """Monotonic, single-threaded virtual or real time."""

    @abc.abstractmethod
    def now(self) -> float: ...

    def tick(self) -> float:  # pragma: no cover - default is a no-op for wall clocks
        return self.now()

    def advance(self, seconds: float) -> float:  # pragma: no cover
        raise NotImplementedError(f"{type(self).__name__} cannot be advanced")


@dataclass
class WallClock(Clock):
    origin: float = field(default_factory=time.time)

    def now(self) -> float:
        return time.time() - self.origin


@dataclass
class VirtualClock(Clock):
    """now() == ticks * step. Advancing is free and deterministic."""

    step: float = 0.01
    _ticks: itertools.count = field(default_factory=lambda: itertools.count())
    _current: float = 0.0
    real_origin: float = field(default_factory=time.time)

    def now(self) -> float:
        return self._current

    def tick(self) -> float:
        next(self._ticks)
        self._current += self.step
        return self._current

    def advance(self, seconds: float) -> float:
        self._current += seconds
        return self._current

    @property
    def elapsed_real(self) -> float:
        return time.time() - self.real_origin


def make_clock(mode: str = "virtual", step: float = 0.01) -> Clock:
    if mode == "virtual":
        return VirtualClock(step=step)
    if mode == "wall":
        return WallClock()
    raise ValueError(f"unknown clock mode {mode!r}")
