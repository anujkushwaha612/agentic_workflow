"""Registry: coverage must not be substring luck, and lineage/limits must hold."""
import pytest

from arena.graph import DependencyGraph, TaskSpec
from arena.lifecycle import AgentState, Lifecycle
from arena.registry import AgentRegistry, SpawnBudget


@pytest.fixture
def reg():
    g = DependencyGraph()
    g.add(TaskSpec("big", "lots of work", "backend", est_work=10.0))
    r = AgentRegistry(graph=g, budget=SpawnBudget(max_active_agents=3, max_spawn_epoch=2,
                                                   min_share_of_remaining=0.25, idle_ttl=1.0))
    return r, g


def _add(r, aid, role, skills=(), epoch=0, state=AgentState.IDLE, since=0.0):
    return r.register(agent_id=aid, role=role, skills=list(skills), epoch=epoch,
                      lifecycle=Lifecycle(state=state, since=since))


def test_ids_are_stable_and_slugged(reg):
    r, _ = reg
    assert r.next_id("Frontend Engineer") == "frontend_engineer_01"
    assert r.next_id("Frontend Engineer") == "frontend_engineer_02"
    assert r.next_id("ml engineer") == "ml_engineer_01"


def test_duplicate_registration_is_rejected(reg):
    r, _ = reg
    _add(r, "a1", "backend")
    with pytest.raises(ValueError):
        _add(r, "a1", "backend")


def test_cover_needs_real_skill_overlap_or_a_real_role_match(reg):
    r, _ = reg
    _add(r, "be", "backend", ["api", "http"])
    assert [a.agent_id for a in r.cover("backend")] == ["be"]  # role-token match
    assert [a.agent_id for a in r.cover("api design", ["api"])] == ["be"]
    # a nameless / tiny role must NOT be treated as covering everything
    assert r.cover("crew_0", []) == [], "unskilled role silently covered everything"
    assert r.cover("", []) == []
    assert r.cover("payments", ["stripe"]) == []


def test_cover_ranks_best_match_first(reg):
    r, _ = reg
    _add(r, "generalist", "backend", ["api"])
    _add(r, "specialist", "auth engineer", ["auth", "oauth", "jwt"])
    best = r.cover("oauth auth flow", ["auth", "oauth"])
    assert best[0].agent_id == "specialist"


def test_terminated_agents_stop_counting(reg):
    r, _ = reg
    a = _add(r, "a1", "backend", ["api"])
    assert len(r.active()) == 1
    a.lifecycle.state = AgentState.TERMINATED
    assert r.active() == [] and r.cover("backend") == []


def test_load_and_pending_work(reg):
    r, g = reg
    a = _add(r, "a1", "backend")
    a.task_id, a.task_queue = "big", ["extra"]
    assert a.load == 2 and a.pending_work == ["big", "extra"]


def test_overloaded_needs_actual_progress(reg):
    r, g = reg
    a = _add(r, "be", "backend", ["api"])
    g.tasks["big"].owner = "be"
    a.task_id = "big"
    assert a.work_done == 0.0
    assert r.overloaded("be"), "10 units of open work vs no progress must read as overloaded"
    a.work_done = 9.0
    assert not r.overloaded("be"), "nearly done is not overloaded"


def test_idle_ttl_only_touches_sleepers(reg):
    r, _ = reg
    _add(r, "idle", "x", state=AgentState.IDLE, since=0.0)
    _add(r, "busy", "y", state=AgentState.WORKING, since=0.0)
    assert [a.agent_id for a in r.idle_overdue(0.5)] == []
    assert [a.agent_id for a in r.idle_overdue(1.5)] == ["idle"]


def test_terminate_bumps_version_for_replay_checks(reg):
    r, _ = reg
    _add(r, "a1", "x")
    v0 = r.version
    r.terminate("a1", "done")
    assert r.version > v0 and r.get("a1") is None


def test_wait_for_edges_uses_pending_waits(reg):
    r, g = reg
    g.add(TaskSpec("t2", "other", "b", owner="b1"))
    _add(r, "a1", "a", state=AgentState.WAITING_FOR_DEPENDENCY)
    _add(r, "b1", "b", state=AgentState.WAITING_FOR_DEPENDENCY)
    r.agents["a1"].pending_waits = [{"task_id": "t2"}]
    r.agents["b1"].pending_waits = [{"task_id": "big"}]
    g.tasks["big"].owner, g.tasks["t2"].owner = "a1", "b1"
    assert r.wait_for_edges() == {"a1": {"b1"}, "b1": {"a1"}}, r.wait_for_edges()


def test_status_lines_are_sorted_by_epoch(reg):
    r, _ = reg
    _add(r, "zz_01", "z", epoch=1)
    _add(r, "aa_01", "a", epoch=0)
    lines = r.status_lines()
    assert lines[0].startswith("aa_01"), lines
    assert "IDLE" in lines[0] and "o" in lines[0][27:]  # idle glyph


def test_snapshot_is_json_safe(reg):
    import json
    r, _ = reg
    _add(r, "a1", "x", ["y"])
    json.dumps(r.snapshot())


def test_spawn_budget_defaults_are_finite():
    b = SpawnBudget()
    assert 0 < b.max_active_agents < 64 and b.max_concurrent_workers >= 1
    assert b.max_spawn_epoch <= 3, "unbounded spawn depth invites recursion"
