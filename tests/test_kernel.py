"""End-to-end kernel behaviour + the dynamic-shaping property the whole design rests on."""
import json
import pathlib

import pytest

from arena.graph import CycleError
from arena.graph import TaskSpec
from arena.kernel import Kernel
from arena.lifecycle import AgentState
from arena.message import MessageType
from arena.policy import EscalateOnComplexity, SimulatedWork, WaitForArtifacts
from arena.registry import SpawnBudget

SAAS = ("Build a SaaS application with authentication, dashboard, API backend, PostgreSQL "
        "database, and cloud deployment")
ML = "Train a churn model: data pipeline, feature engineering, model optimization, evaluation"
TINY = "rename the landing page title"


@pytest.fixture
def k(tmp_path):
    return Kernel(root=str(tmp_path), budget=SpawnBudget(max_active_agents=6,
                                                          max_concurrent_workers=2,
                                                          idle_ttl=1e9),
                  journal_path=tmp_path / "j.db")


def _run(k, ticks=400):
    return k.run(ticks=ticks)


def test_saas_plan_is_not_hardcoded(k):
    out = k.submit(SAAS)
    roles = {a.role for a in k.registry.agents.values()}
    assert {"database", "backend", "frontend", "cloud", "auth"} <= roles, roles
    assert out["tasks"] == 7
    assert out["parallelism"][0] >= 3, "first wave should be wide, not a single root task"


def test_ml_task_produces_a_completely_different_org(k):
    k.submit(ML)
    roles = {a.role for a in k.registry.agents.values()}
    assert {"ml-engineer", "evaluation", "data", "testing"} & roles, roles
    assert "database" not in roles and "frontend" not in roles, "SaaS roles leaked into an ML job"


def test_trivial_task_refuses_to_spawn_an_org(k):
    out = k.submit(TINY)
    assert out["tasks"] == 0
    assert not k.registry.agents, "the Parent invented engineers for a one-line edit"


def test_full_run_completes_and_publishes_artifacts(k):
    k.submit(SAAS)
    res = _run(k)
    assert res["done"], f"graph left open: {res['open_tasks']}"
    assert not res["stalled"]
    assert res["polls"] == 0
    assert {"contracts/api.json", "database/schema.sql", "backend/src"} <= set(k.artifacts)
    assert res["chain_ok"]


def test_dependency_order_is_observed(k):
    """frontend integration may not publish before the API contract exists."""
    k.submit(SAAS)
    order = []
    real = k.publish_artifact

    def spy(artifact, **kw):
        order.append(artifact)
        return real(artifact, **kw)

    k.publish_artifact = spy
    _run(k)
    assert order.index("contracts/api.json") < order.index("frontend/src"), order
    assert order.index("database/schema.sql") < order.index("contracts/api.json"), order


def test_consumers_wait_instead_of_failing(k):
    k.submit(SAAS)
    k.run(ticks=1)                      # one tick: consumers should already be parked, not errored
    parked = [a for a, r in k.registry.agents.items()
              if r.state == str(AgentState.WAITING_FOR_DEPENDENCY)]
    # nothing has completed yet on tick 1, so either parked or progressing is legitimate;
    # what is NOT legitimate is an error or a blocked agent
    assert not [a for a, r in k.registry.agents.items() if r.state == str(AgentState.BLOCKED)], \
        "an agent blocked instead of waiting"


def test_durable_waits_are_journaled_and_resolved(k):
    k.submit(SAAS)
    _run(k)
    armed = k.journal.events(etype=MessageType.WAIT_REGISTERED)
    resolved = k.journal.events(etype=MessageType.WAIT_RESOLVED)
    assert armed, "an agent waited without arming a durable wait"
    assert len(resolved) == len(armed), (len(armed), len(resolved))


def test_snapshot_and_replay_agree(k):
    k.submit(SAAS)
    _run(k)
    live = k.snapshot()
    r = Kernel.from_journal(k.journal.path, root=str(k.root), budget=SpawnBudget(idle_ttl=1e9))
    rp = r.snapshot()
    for key in ("agents", "tasks", "claims", "artifacts"):
        assert rp[key] == live[key], f"{key} diverged after replay"
    assert r.status()["counts"] is not None


def test_replay_reblocks_nobody(k):
    k.submit(SAAS)
    _run(k)
    r = Kernel.from_journal(k.journal.path, root=str(k.root), budget=SpawnBudget(idle_ttl=1e9))
    assert r.graph.known_artifacts == set(k.artifacts)
    for aid, rec in r.registry.agents.items():
        if rec.state == str(AgentState.WAITING_FOR_DEPENDENCY):
            assert rec.pending_waits, "a replayed waiter lost its wait"
    assert r.snapshot()["tasks"] == k.snapshot()["tasks"]


def test_pause_resume_terminate_are_journaled(k):
    k.submit(SAAS)
    aid = sorted(k.registry.agents)[0]
    assert k.parent.pause(aid) is True
    assert k.registry.agents[aid].state == str(AgentState.PAUSED)
    assert aid not in k._eligible(), "a paused agent was still eligible for a worker slot"
    assert k.parent.resume(aid) is True
    assert k.registry.agents[aid].state != str(AgentState.PAUSED)
    assert k.parent.terminate_agent(aid, "chaos: manual reap") is True
    assert aid not in k.registry.agents
    etypes = {r["etype"] for r in k.journal.events()}
    assert {"AGENT_PAUSED", "AGENT_RESUMED", "AGENT_TERMINATED"} <= etypes


def test_paused_state_survives_replay(k):
    k.submit(SAAS)
    aid = sorted(k.registry.agents)[0]
    k.parent.pause(aid)
    r = Kernel.from_journal(k.journal.path, root=str(k.root), budget=SpawnBudget(idle_ttl=1e9))
    assert r.registry.agents[aid].state == str(AgentState.PAUSED)


def test_spawn_request_end_to_end(k):
    k.role_policies = {"backend": "escalate"}
    k.role_overrides = {"backend": {"role": "security engineer", "skills": ["threat-model"],
                                   "reason": "authz needs a specialist", "work_estimate": 4.0}}
    k.submit(SAAS)
    _run(k, ticks=300)
    reqs = k.journal.events(etype=MessageType.SPAWN_AGENT_REQUEST)
    assert reqs, "the backend agent never asked for help"
    dec = k.parent.decisions
    # Phase 2 renamed the rules to the spec's vocabulary; the invariant (a named decision, one of
    # the documented vetoes) is unchanged.
    assert dec and dec[0]["rule"] in {"APPROVE", "REJECT_DUPLICATE_CAPABILITY",
                                     "REJECT_NOT_WORTH_IT", "REJECT_CAP",
                                     "REJECT_REQUESTER_OVERLOADED", "REJECT_SPAWN_DEPTH",
                                     "REJECT_UNSUPPORTED", "ESCALATE"}, dec
    if dec[0]["ok"]:
        assert any("security" in a.role for a in k.registry.agents.values())


def test_parent_rejects_tiny_spawns_with_a_reason(k):
    k.budget.min_share_of_remaining = 0.4
    k.submit(SAAS)
    aid = sorted(k.registry.agents)[0]
    d = k.parent.evaluate_spawn({"from": aid, "requested_role": "errand runner",
                                 "skills": ["nothing-matching"], "work_estimate": 0.1})
    assert not d.ok and d.rule in {"REJECT_NOT_WORTH_IT", "REJECT_REQUESTER_OVERLOADED"}, d


def test_plan_with_a_derived_cycle_is_refused(tmp_path):
    from arena.parent import RuleBasedPlanner

    class BadPlanner:
        name = "bad"

        def plan(self, text):
            from arena.graph import TaskSpec as TS
            return ([TS("t1", "a", "alpha", produces=["p1"], consumes=["p2"]),
                     TS("t2", "b", "alpha", produces=["p2"], consumes=["p1"])], {})

    k = Kernel(root=str(tmp_path), planner=BadPlanner(), budget=SpawnBudget(idle_ttl=1e9))
    with pytest.raises(CycleError):
        k.submit("anything")
    assert not k.registry.agents, "an org was built for an unschedulable plan"


def test_report_and_status_render(k):
    k.submit(SAAS)
    _run(k)
    text = k.report()
    assert "PARENT ARENA" in text and "t_db_schema" in text
    st = k.status()
    assert set(st) >= {"agents", "tasks", "counts", "bus", "polls", "critical_path"}
    assert json.dumps(st, default=str)
    why = k.why("backend_01")
    assert "state" in why and "unmet_deps" in why


def test_trace_follows_a_correlation(k):
    k.submit(SAAS)
    _run(k)
    cids = {r["correlation_id"] for r in k.journal.iterate() if r["correlation_id"]}
    assert cids
    for cid in list(cids)[:5]:
        tr = k.trace(cid)
        assert tr and all(e["correlation_id"] == cid for e in tr)


def test_idle_agents_are_reaped(tmp_path):
    k = Kernel(root=str(tmp_path), clock_step=0.1,
               budget=SpawnBudget(max_active_agents=4, idle_ttl=0.3))
    from arena.graph import TaskSpec
    k.graph.add(TaskSpec("one", "x", "alpha", produces=["a"], est_work=0.2))
    k.registry.register(agent_id="alpha_01", role="alpha", skills=["alpha"])
    k.registry.register(agent_id="tourist_01", role="tourist", skills=["t"])
    k.make_actor("alpha_01")
    k.parent.assign("one", "alpha_01")
    k.run(ticks=30)
    assert "tourist_01" not in k.registry.agents, "a never-useful agent stayed on the payroll"


def test_virtual_clock_makes_timeouts_free(k):
    assert k.now == 0.0
    k.run(ticks=5)
    assert k.now == pytest.approx(0.05)


def test_summary_counts_match_status(k):
    k.submit(SAAS)
    _run(k)
    s, st = k.summary(), k.status()
    assert s["agents"] == len(st["agents"])
    assert s["events"] == st["events"] == k.journal.count()


def test_two_kernels_do_not_share_state(tmp_path):
    a = Kernel(root=str(tmp_path / "a"), budget=SpawnBudget(idle_ttl=1e9))
    b = Kernel(root=str(tmp_path / "b"), budget=SpawnBudget(idle_ttl=1e9))
    a.submit(SAAS)
    b.submit(ML)
    assert {x.role for x in a.registry.agents.values()} != \
        {x.role for x in b.registry.agents.values()}


def test_policies_are_swappable_without_touching_the_kernel(k):
    k.submit(SAAS)
    aid = "database_01"
    k.actors[aid].policy = SimulatedWork(steps=1)
    out = k.actors[aid].run_step()
    assert out["action"] in {"PUBLISH", "PROCEED", "COMPLETE"}, out
    k.actors["backend_01"].policy = WaitForArtifacts(steps=1)
    assert k.actors["backend_01"].run_step()["action"] in {
        "PUBLISH", "PROCEED", "COMPLETE", "WAIT"}


def test_llm_adapter_seam_is_honest(k):
    """NullLLM must refuse loudly rather than fake intelligence."""
    from arena.policy import HybridPolicy, NullLLM  # noqa: F401
    with pytest.raises(RuntimeError, match="No LLM adapter configured"):
        NullLLM().decide({"x": 1})
    hp = HybridPolicy(heuristic=SimulatedWork(steps=1))
    assert hp.heuristic is not None and hp.llm is not None


def test_escalations_land_in_the_inbox_for_the_arena_cortex(k):
    """tier C of FEASIBILITY §4.3: judgment calls are written out for me, not swallowed."""
    from arena.actor import ActorContext
    k.submit(SAAS)
    aid = sorted(k.registry.agents)[0]
    actor = k.actors[aid]

    class Boom:
        name = "boom"

        def step(self, ctx, msg):
            from arena.policy import Action
            return Action.escalate("this needs a human call", decision="who owns payments?")

    actor.policy = Boom()
    k.run(ticks=4)
    assert k.parent.inbox, "escalation never reached the Parent"
    inbox_file = pathlib.Path(k.root) / k.parent.inbox_path
    assert inbox_file.exists() and inbox_file.read_text().strip(), "inbox was not persisted"
    row = json.loads(inbox_file.read_text().splitlines()[-1])
    assert row["agent"] == aid and "human call" in row["reason"]


def test_arena_decision_file_is_applied(k):
    """And the reverse direction: my written answer gets consumed on the next run()."""
    k.submit(SAAS)
    aid = sorted(k.registry.agents)[-1]
    dec = pathlib.Path(k.root) / k.parent.decision_path
    dec.parent.mkdir(parents=True, exist_ok=True)
    dec.write_text(json.dumps({"kind": "force_state", "agent_id": aid, "state": "PAUSED"}) + "\n")
    k.run(ticks=1)
    assert k.registry.agents[aid].state == str(AgentState.PAUSED)
    assert dec.read_text().strip() == "", "consumed decision must be cleared"
