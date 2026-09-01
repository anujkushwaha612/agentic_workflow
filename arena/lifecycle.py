"""Agent lifecycle FSM (your §11) as a *closed* transition system.

Why an explicit table: without it, "an agent resumes itself from COMPLETED" is a silent logic bug.
Here it is a rejected event, journaled as ILLEGAL_TRANSITION, which is exactly what chaos test #7
checks. Illegal transitions never mutate state - the guard is at the boundary, not inside policies.
"""
from __future__ import annotations

import enum
from collections.abc import Iterable
from dataclasses import dataclass, field


class AgentState(enum.StrEnum):
    CREATED = "CREATED"
    INITIALIZING = "INITIALIZING"
    IDLE = "IDLE"
    WORKING = "WORKING"
    WAITING_FOR_DEPENDENCY = "WAITING_FOR_DEPENDENCY"
    BLOCKED = "BLOCKED"
    ESCALATED = "ESCALATED"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    TERMINATED = "TERMINATED"


#: states from which an agent holds no worker slot (scheduler may run someone else)
SLEEPING = frozenset({
    AgentState.WAITING_FOR_DEPENDENCY, AgentState.BLOCKED,
    AgentState.ESCALATED, AgentState.PAUSED,
    AgentState.COMPLETED, AgentState.TERMINATED, AgentState.CREATED,
})

#: states in which an agent is eligible to be given work / to receive control-plane messages
ADMITTABLE = frozenset({AgentState.IDLE, AgentState.CREATED, AgentState.INITIALIZING})

TRANSITIONS: dict[AgentState, frozenset[AgentState]] = {
    AgentState.CREATED: frozenset({AgentState.INITIALIZING, AgentState.TERMINATED}),
    AgentState.INITIALIZING: frozenset({AgentState.IDLE, AgentState.WORKING, AgentState.TERMINATED}),
    AgentState.IDLE: frozenset({AgentState.WORKING, AgentState.WAITING_FOR_DEPENDENCY,
                                 AgentState.BLOCKED, AgentState.ESCALATED, AgentState.PAUSED,
                                 AgentState.TERMINATED}),
    AgentState.WORKING: frozenset({AgentState.IDLE, AgentState.WAITING_FOR_DEPENDENCY,
                                   AgentState.BLOCKED, AgentState.ESCALATED, AgentState.PAUSED,
                                   AgentState.COMPLETED, AgentState.TERMINATED}),
    AgentState.WAITING_FOR_DEPENDENCY: frozenset({AgentState.WORKING, AgentState.IDLE,
                                                   AgentState.BLOCKED, AgentState.ESCALATED,
                                                   AgentState.TERMINATED}),
    AgentState.BLOCKED: frozenset({AgentState.WORKING, AgentState.IDLE, AgentState.ESCALATED,
                                   AgentState.TERMINATED}),
    AgentState.ESCALATED: frozenset({AgentState.WORKING, AgentState.IDLE,
                                     AgentState.WAITING_FOR_DEPENDENCY, AgentState.BLOCKED,
                                     AgentState.TERMINATED}),
    AgentState.PAUSED: frozenset({AgentState.IDLE, AgentState.WORKING,
                                  AgentState.WAITING_FOR_DEPENDENCY, AgentState.TERMINATED}),
    # COMPLETED is a drain state: an agent may only be reaped (TERMINATED) or re-initialised
    # explicitly. It can never jump straight back to WORKING - that would hide duplicate work.
    AgentState.COMPLETED: frozenset({AgentState.TERMINATED, AgentState.INITIALIZING}),
    AgentState.TERMINATED: frozenset(),
}


def can_transition(frm: AgentState | str, to: AgentState | str) -> bool:
    return AgentState(to) in TRANSITIONS[AgentState(frm)]


def assert_transition(frm: AgentState | str, to: AgentState | str) -> None:
    if not can_transition(frm, to):
        raise IllegalTransition(AgentState(frm), AgentState(to))


class IllegalTransition(Exception):
    def __init__(self, frm: AgentState, to: AgentState) -> None:
        self.frm, self.to = frm, to
        super().__init__(f"illegal lifecycle transition {frm} -> {to} "
                         f"(legal: {sorted(s.value for s in TRANSITIONS[frm]) or 'none - terminal'})")


@dataclass(slots=True)
class TransitionRecord:
    """Result of a requested transition - so callers can journal both accepted and rejected."""

    frm: AgentState
    to: AgentState
    ok: bool
    reason: str = ""

    @property
    def rejected(self) -> bool:
        return not self.ok


@dataclass
class Lifecycle:
    """State holder with a monotonic per-state clock (needed by idle-TTL reaping)."""

    state: AgentState = AgentState.CREATED
    since: float = 0.0
    history: list[tuple[float, AgentState, AgentState, str]] = field(default_factory=list)

    def request(self, to: AgentState | str, reason: str = "", *, now: float) -> TransitionRecord:
        # `now` is keyword-only on purpose: an earlier draft had `request(S, "reason")` passing a
        # string into the clock slot, which only surfaced as `float - str` five modules away.
        to = AgentState(to)
        if can_transition(self.state, to):
            prev = self.state
            self.state = to
            self.since = now
            self.history.append((now, prev, to, reason))
            return TransitionRecord(prev, to, True, reason)
        return TransitionRecord(self.state, to, False,
                                f"{self.state} -> {to} is not a legal edge")

    def time_in_state(self, now: float) -> float:
        return now - self.since

    def snapshot(self) -> dict:
        return {"state": str(self.state), "since": round(self.since, 4),
                "transitions": len(self.history)}


def states_reachable_from(states: Iterable[AgentState]) -> set[AgentState]:
    out: set[AgentState] = set()
    for s in states:
        out |= TRANSITIONS[AgentState(s)]
    return out
