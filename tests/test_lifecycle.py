"""Lifecycle FSM: the guard is a closed system, not a convention."""
import pytest

from arena.lifecycle import TRANSITIONS, AgentState, IllegalTransition, Lifecycle, can_transition
from arena.registry import AgentRegistry, SpawnBudget


def test_every_state_is_declared():
    assert set(TRANSITIONS) == set(AgentState), "an agent state with no transition rule"


def test_terminated_is_terminal():
    assert TRANSITIONS[AgentState.TERMINATED] == frozenset()


def test_completed_cannot_resume_directly():
    assert not can_transition(AgentState.COMPLETED, AgentState.WORKING)
    assert can_transition(AgentState.COMPLETED, AgentState.TERMINATED)
    assert can_transition(AgentState.COMPLETED, AgentState.INITIALIZING)


def test_happy_path_through_the_waiting_states():
    lc = Lifecycle()
    for to in (AgentState.INITIALIZING, AgentState.IDLE, AgentState.WORKING,
               AgentState.WAITING_FOR_DEPENDENCY, AgentState.WORKING, AgentState.COMPLETED,
               AgentState.TERMINATED):
        assert lc.request(to, f"via {to}", now=0.0).ok, to
    assert lc.state is AgentState.TERMINATED
    assert len(lc.history) == 7


def test_rejection_does_not_mutate():
    lc = Lifecycle(state=AgentState.TERMINATED, since=3.0)
    res = lc.request(AgentState.WORKING, "impossible", now=9.0)
    assert res.rejected and lc.state is AgentState.TERMINATED and lc.since == 3.0
    assert "legal" in res.reason or "->" in res.reason


def test_assert_transition_raises():
    with pytest.raises(IllegalTransition):
        from arena.lifecycle import assert_transition
        assert_transition(AgentState.TERMINATED, AgentState.IDLE)


def test_no_self_loops_except_idempotent_idle():
    # A self-transition is only legal if declared; it must never be implied.
    for st, allowed in TRANSITIONS.items():
        assert st not in allowed, f"{st} may transition to itself - that hides no-op churn"


def test_since_tracks_the_last_accepted_edge():
    lc = Lifecycle()
    lc.request(AgentState.INITIALIZING, "boot", now=1.5)
    assert lc.since == 1.5
    assert lc.time_in_state(2.0) == pytest.approx(0.5)


def test_registry_uses_kernel_transition_only():
    reg = AgentRegistry(budget=SpawnBudget(max_active_agents=1))
    rec = reg.register(agent_id="a", role="r")
    assert rec.state == "CREATED"
    ok = rec.lifecycle.request(AgentState.WORKING, "skip init", now=0.0)
    assert ok.rejected, "registry allowed CREATED -> WORKING behind the kernel"


def test_states_reachable_helper():
    from arena.lifecycle import states_reachable_from
    assert AgentState.TERMINATED in states_reachable_from([AgentState.WORKING])
