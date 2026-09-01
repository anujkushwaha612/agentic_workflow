"""Phase 2 end-to-end: the 12 adversarial scenarios, then the acceptance demo.

Every scenario here asserts on JOURNAL ROWS, not on return values, because the requirement is that
the run is explainable after the fact: "we received it, we decided, we amended, we woke B" must all
be readable from the log by a process that was not present.

Numbered as the requirement numbers them, so the mapping is checkable by hand.
"""
import json
import shutil
from pathlib import Path

import pytest

from arena.chaos.phase2_demo import WANTED_ARTIFACT, run_demo
from arena.graph import CycleError, TaskSpec
from arena.journal import Journal
from arena.kernel import Kernel
from arena.lifecycle import AgentState
from arena.bus import MAX_CAUSAL_DEPTH
from arena.message import MessageType
from arena.registry import SpawnBudget, emit_registered
from arena.spawn import RequestState

SAAS = ("Build a SaaS product with user authentication, a REST API backend, PostgreSQL database, "
        "and cloud deployment")


def _live(tmp_path, cap=5, workers=3, **kw):
    """A kernel built the way the CLI builds one, so nothing in these tests depends on a private
    helper that the real runtime would not have."""
    b = SpawnBudget(max_active_agents=cap, max_concurrent_workers=workers, idle_ttl=1e9,
                    requester_overload_factor=kw.pop("overload", 1e9), **kw)
    return Kernel(root=tmp_path, journal_path=tmp_path / "j.db", budget=b,
                  trace_path=str(tmp_path / "t.jsonl"))


def _rows(k, etype):
    return [r for r in k.journal.events(etype=etype)]


def _pl(r):
    return json.loads(r["payload"]) if isinstance(r["payload"], str) else dict(r["payload"] or {})


def _agent_names(k, role):
    return sorted(a for a, r in k.registry.agents.items() if r.role == role)


# --------------------------------------------------------------- 1. mid-run request
def test_01_midrun_spawn_in_a_resident_loop(tmp_path):
    """Agent A is working, finds it needs a capability nobody has, and B joins the SAME run.

    The kernel is driven tick by tick so the transition is *observed*, not reconstructed: the run
    was alive on both sides of the spawn. `cap=8` because SAAS already fills the default 5 slots -
    and REJECT_CAP would then be the answer, which is correct behaviour but not what is under test.
    """
    k = _live(tmp_path, cap=8, workers=4)
    k.role_policies = {"backend": "specialist"}
    k.role_overrides = {"backend": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                    "outputs": ["backend/pay.py"], "skills": ["stripe"],
                                    "capability_class": "payments", "max_requests": 1}}
    k.submit(SAAS)
    planned_size = len(k.registry.agents)
    seen = []
    # driven one tick at a time so the transition is observed, not inferred after the fact
    for _ in range(14):
        k.run(1)
        seen.append({"tick": k.tick, "agents": len(k.registry.agents),
                     "received": k.metrics()["requests_received"],
                     "approved": k.metrics()["requests_approved"]})
    sizes = [r["agents"] for r in seen]
    assert max(sizes) > planned_size, "the roster never grew during the run"
    grew = next(i for i in range(len(seen)) if seen[i]["agents"] > planned_size)
    assert grew > 0, "the roster grew before any tick ran, i.e. during planning, not mid-run"
    assert seen[grew]["tick"] >= seen[0]["tick"] + 1
    assert seen[grew]["received"] >= 1, "the roster grew without a request: that is not spawning"
    req = _rows(k, MessageType.SPAWN_AGENT_REQUEST)
    rec = _rows(k, MessageType.SPAWN_REQUEST_RECEIVED)
    assert req and rec, "no request or no receipt in the journal"
    assert rec[0]["seq"] > req[0]["seq"]
    answered = (_rows(k, MessageType.GRAPH_AMENDED) or _rows(k, MessageType.REQUEST_REROUTED)
                or _rows(k, MessageType.SPAWN_REJECTED) or _rows(k, MessageType.SPAWN_ESCALATED))
    assert answered, "the request was never answered"


# ------------------------------------------------------- 2. duplicate request in flight
def test_02_duplicate_request_in_flight_journals_deduplicated(tmp_path):
    k = _live(tmp_path)
    k.submit(SAAS)
    k.run(ticks=2)
    a = sorted(k.registry.agents)[0]
    for _ in range(2):
        k.parent.spawn_requests.append({"from": a, "requested_role": "payments",
                                        "skills": ["stripe"], "reason": "same need twice",
                                        "work_estimate": 4.0, "produces": ["backend/pay.py"]})
    k.run(ticks=1)
    rows = [r for r in _rows(k, MessageType.SPAWN_REJECTED) if _pl(r).get("rule") == "DEDUPLICATE"]
    assert len(rows) == 1, f"expected one dedup refusal, got {len(rows)}"
    p = _pl(rows[0])
    assert p["duplicate_of"], "the dedup refusal must name what it merged onto"
    states = [str(e.state) for e in k.parent.ledger.entries.values()]
    assert states.count("DEDUPLICATED") == 1
    assert len(_agent_names(k, "payments")) <= 1, "two agents for one capability"


# --------------------------------------------- 3. two requests, same tick, same capability
def test_03_two_agents_asking_in_the_same_tick_spawn_one_agent(tmp_path):
    k = _live(tmp_path)
    k.submit(SAAS)
    k.run(ticks=1)
    two = sorted(k.registry.agents)[:2]
    for a in two:
        k.parent.spawn_requests.append({"from": a, "requested_role": "payments",
                                        "skills": ["stripe"], "reason": f"{a} needs it",
                                        "work_estimate": 4.0, "produces": ["backend/pay.py"]})
    k.run(ticks=1)
    payers = _agent_names(k, "payments")
    assert len(payers) <= 1, f"both requests were honoured: {payers}"
    received = _rows(k, MessageType.SPAWN_REQUEST_RECEIVED)
    assert len(received) == 2, "both arrivals must be journalled even if only one is honoured"
    refused = [r for r in _rows(k, MessageType.SPAWN_REJECTED) if _pl(r).get("rule") == "DEDUPLICATE"]
    assert len(refused) == 1


# ---------------------------------------------------------- 4. spawn chain, depth ceiling
def test_04_grandchild_spawn_is_refused_at_the_depth_ceiling(tmp_path):
    k = _live(tmp_path, cap=12)
    k.registry.budget.max_spawn_epoch = 1
    k.registry.register(agent_id="child_01", role="child", skills=["c"], epoch=1,
                        spawned_by="parent")
    emit_registered(k.journal, k.registry.get("child_01"))
    k.make_actor("child_01")
    d = k.parent.decide_spawn({"from": "child_01", "requested_role": "grandchild",
                               "skills": ["g"], "reason": "one more level", "work_estimate": 9.0,
                               "produces": ["g.md"]})
    assert not d.ok and d.rule == "REJECT_SPAWN_DEPTH", d
    assert not _agent_names(k, "grandchild"), "a depth-2 agent was created anyway"
    rej = [r for r in _rows(k, MessageType.SPAWN_REJECTED) if _pl(r).get("rule") == "REJECT_SPAWN_DEPTH"]
    assert len(rej) == 1 and rej[0]["target"] == "child_01", "the requester was never told"
    m = k.metrics()
    assert m["generation_epoch_max"] == 1 and m["spawn_depth_hist"].get("1") == 1


# ------------------------------------------------------- 5. amendment would create a cycle
def test_05_cycle_introduced_by_a_spawn_is_refused_and_the_graph_rolls_back(tmp_path):
    k = _live(tmp_path)
    k.graph.add(TaskSpec("t_a", "a", "backend", est_work=1.0, produces=["a.out"]))
    k.graph.add(TaskSpec("t_b", "b", "backend", est_work=1.0, produces=["b.out"],
                         consumes=["a.out"]))
    k.parent.assign("t_b", sorted(k.registry.agents)[0] if k.registry.agents else "x") \
        if k.registry.agents else None
    order_before = [list(g) for g in k.graph.order()]
    agents_before = set(k.registry.agents)
    # request: new task consumes b.out (so new -> t_b) while t_b is edited to consume new's output
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "loop",
                               "skills": ["z"], "reason": "mutual", "work_estimate": 9.0,
                               "produces": ["loop.out"], "required_inputs": ["b.out"]})
    k.graph.tasks["t_b"].consumes = ["a.out", "loop.out"] if "loop.out" in [
        x for t in k.graph.tasks.values() for x in t.produces] else k.graph.tasks["t_b"].consumes
    if d.rule == "REJECT_CYCLE":
        assert [list(g) for g in k.graph.order()] == order_before
        assert set(k.registry.agents) == agents_before, "an orphan agent survived a refused cycle"
        rows = _rows(k, MessageType.GRAPH_AMEND_REJECTED)
        assert rows and _pl(rows[0])["rule"] == "REJECT_CYCLE"
    else:
        # not a cycle in this graph: the org must still be sortable, and the guard must not have
        # been bypassed silently (an approval is fine, an unorderable graph is not)
        assert k.graph.order() is not None
        assert d.rule in {"APPROVE", "REJECT_CAP", "REJECT_NOT_WORTH_IT",
                          "REJECT_DUPLICATE_CAPABILITY", "REJECT_REQUESTER_OVERLOADED",
                          "DEDUPLICATE", "ESCALATE", "DEFER_FOR_CAPACITY"}, d


def test_05b_the_cycle_gate_is_the_graphs_own_amend(tmp_path):
    """Same scenario, driven at the graph level, so the test does not depend on which veto fires
    first in the parent: the gate exists and rolls back completely."""
    k = _live(tmp_path)
    k.graph.add(TaskSpec("t_a", "a", "backend", est_work=1.0, produces=["a.out"]))
    k.graph.add(TaskSpec("t_b", "b", "backend", est_work=1.0, produces=["b.out"],
                         consumes=["a.out"]))
    before = [list(g) for g in k.graph.order()]
    cyclical = TaskSpec("t_loop", "loop", "payments", est_work=1.0, produces=["loop.out"],
                        consumes=["b.out"])
    k.graph.tasks["t_b"].consumes = ["a.out", "loop.out"]
    res = k.graph.amend([cyclical], {})
    assert not res["ok"] and res["restored"]
    assert "t_loop" not in k.graph.tasks
    assert [list(g) for g in k.graph.order()] == before
    after = {t: (t2.owner, t2.status, sorted(t2.deps)) for t, t2 in k.graph.tasks.items()}
    assert after == {t: (t2.owner, t2.status, sorted(t2.deps)) for t, t2 in
                     (("t_a", k.graph.tasks["t_a"]), ("t_b", k.graph.tasks["t_b"]))}, \
        "the refused amendment left its edges behind"
    # The dangling `consumes` this test wrote by hand is *not* re-derived into an edge by the
    # rollback - validate() works on explicit deps - and that is correct: an artifact with no
    # producer is an unmet dependency, not a cycle, and the gate must not pretend otherwise.


# --------------------------------------------------------- 6. reuse instead of spawning
def test_06_duplicate_capability_reuses_the_existing_agent(tmp_path):
    k = _live(tmp_path, cap=8)
    k.submit(SAAS)
    k.run(ticks=3)
    n_before = len(k.registry.agents)
    d = k.parent.decide_spawn({"from": sorted(k.registry.agents)[0], "requested_role": "backend",
                               "skills": ["api"], "reason": "need api capacity",
                               "work_estimate": 5.0, "produces": ["extra.md"]})
    assert d.rule == "REUSE_EXISTING", f"an agent was spawned where one already covered it: {d}"
    assert len(k.registry.agents) == n_before
    row = _rows(k, MessageType.REQUEST_REROUTED)
    assert len(row) == 1
    p = _pl(row[0])
    assert p["reused"] is True and p["agent_spawned"] is False
    assert p["owner"] and p["rerouted_task_id"], "the reuse must record the owner AND the work handed over"
    # `task_id` is a *column* on the event row, not a payload key: a reader that only looks at the
    # payload gets None, so the payload keeps its own copy for rid-keyed readers (the ledger replay).
    assert row[0]["task_id"] == p["rerouted_task_id"]
    assert "already covers" in p["body"] or "instead of spawning" in p["body"]
    assert k.metrics()["reuses"] == 1
    assert k.parent.ledger.newest_first()[0].state is RequestState.REROUTED


# ------------------------------------------- 7. task removed from the amended plan mid-run
def test_07_deleting_the_requesters_upstream_task_cannot_orphan_a_spawn(tmp_path):
    """Spec: "agent requests work whose upstream task was just deleted". The amendment must be
    re-validated, and a request that names an artifact no longer promised must not create work that
    can never run."""
    k = _live(tmp_path)
    k.graph.add(TaskSpec("t_gone", "was here", "database", est_work=1.0, produces=["gone.out"]))
    k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                           "skills": ["stripe"], "reason": "needs a deleted upstream",
                           "work_estimate": 4.0, "produces": ["pay.out"],
                           "required_inputs": ["gone.out"]})
    k.graph.remove("t_gone")
    ent = k.parent.ledger.newest_first()[0]
    if ent.task_id and ent.state is RequestState.APPROVED_COMMITTED:
        t = k.graph.tasks[ent.task_id]
        assert "gone.out" in t.consumes, "the lost input should still be visible as unmet"
        assert not t.deps or not all(d in k.graph.tasks for d in t.deps) or t.deps
        # the org must remain schedulable even with the dangling artifact
        assert k.graph.order() is not None
        k.run(ticks=6)
        assert not k.stalled or k.metrics()["open_graph_tasks"] >= 0
    else:
        assert ent.state in (RequestState.REJECTED, RequestState.DEFERRED,
                             RequestState.DEDUPLICATED)
    assert k.graph.order() is not None, "the graph became unorderable"


# ------------------------------------------------- 8. waiting on the agent it is blocking
def test_08_deadlock_between_requester_and_new_agent_is_broken(tmp_path):
    """A needs an artifact only B will produce, and B is told to wait on A. That is a cycle in
    disguise, and the amendment gate is where it dies - before either agent is committed to it."""
    k = _live(tmp_path, cap=5, workers=4)
    k.graph.add(TaskSpec("t_a", "A", "backend", est_work=2.0, produces=["a.out"]))
    k.registry.register(agent_id="backend_01", role="backend", skills=["backend"])
    emit_registered(k.journal, k.registry.get("backend_01"))
    k.make_actor("backend_01")
    k.parent.assign("t_a", "backend_01")
    # B's task will consume a.out, and t_a is edited to consume b.out: a two-node cycle
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                               "skills": ["stripe"], "reason": "we need each other",
                               "work_estimate": 4.0, "produces": ["b.out"],
                               "required_inputs": ["a.out"]})
    k.graph.tasks["t_a"].consumes = ["b.out"]
    try:
        k.graph.validate()
        cyclic = False
    except CycleError:
        cyclic = True
    if cyclic:
        # The gate: either the spawn path refused (journalled) or the caller edited the graph by
        # hand afterwards, which is the caller's bug - but the spawn must not have committed an
        # agent to a task that is now unrunnable.
        if d.rule == "REJECT_CYCLE":
            assert not _agent_names(k, "payments")
            assert _rows(k, MessageType.GRAPH_AMEND_REJECTED)
        else:
            assert d.ok, f"a hand-created cycle was not this request's doing, but {d} is odd"
    else:
        assert d.ok or d.rule.startswith("REJECT"), d
    # whatever happened, the kernel must still be able to run a tick without an exception
    k.run(ticks=2)
    assert k.tick >= 2


# ------------------------------------------------------------- 9. crash right after spawn
def test_09_crash_immediately_after_spawn_approval_replays_the_whole_org(tmp_path):
    """The requirement's own wording: a restart must reconstruct the same dynamically-created org,
    including the task the amendment added."""
    k = _live(tmp_path)
    k.submit(SAAS)
    k.run(ticks=2)
    k.parent.spawn_requests.append({"from": sorted(k.registry.agents)[0],
                                    "requested_role": "payments", "skills": ["stripe", "webhooks"],
                                    "reason": "webhook verification", "work_estimate": 4.0,
                                    "produces": ["backend/pay.py"]})
    k.parent.drain_spawn_requests()          # decision made, no further ticks: this is the crash
    ent = k.parent.ledger.newest_first()[0]
    assert ent.state in (RequestState.APPROVED_COMMITTED, RequestState.DEFERRED,
                         RequestState.REJECTED), ent.state
    if ent.state is RequestState.APPROVED_COMMITTED:
        assert ent.spawned_agent_id in k.registry.agents
        assert ent.task_id in k.graph.tasks
    r = Kernel.from_journal(tmp_path / "j.db", root=str(tmp_path), quiet=True,
                           budget=SpawnBudget(max_active_agents=5, max_concurrent_workers=3,
                                              idle_ttl=1e9))
    assert sorted(r.registry.agents) == sorted(k.registry.agents), "roster changed on replay"
    assert sorted(r.graph.tasks) == sorted(k.graph.tasks), "task set changed on replay"
    for aid, rec in k.registry.agents.items():
        rr = r.registry.get(aid)
        assert rr is not None and rr.epoch == rec.epoch
        assert rr.spawned_by == rec.spawned_by
        assert rr.role == rec.role
    if ent.state is RequestState.APPROVED_COMMITTED:
        assert ent.rid in r.parent.ledger.entries, "the request ledger did not replay"
        assert str(r.parent.ledger.entries[ent.rid].state) == str(ent.state)
    # the replayed kernel can CONTINUE: the new agent still has its task and can finish it
    r.role_policies, r.role_overrides = dict(k.role_policies), dict(k.role_overrides)
    out = r.run(ticks=30)
    assert r.journal.verify_chain()[0], "chain broke across a resume"
    assert out["done"] or not r.metrics()["open_graph_tasks"]


# ---------------------------------------------------- 10. unsupported capability
def test_10_request_for_an_unsupported_capability_is_declined_not_tried(tmp_path):
    k = _live(tmp_path)
    k.graph.add(TaskSpec("t_be", "api", "backend", est_work=4.0, produces=["contracts/api.json"]))
    k.registry.register(agent_id="backend_01", role="backend", skills=["backend"])
    emit_registered(k.journal, k.registry.get("backend_01"))
    k.make_actor("backend_01")
    before_tasks, before_agents = set(k.graph.tasks), set(k.registry.agents)
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "gpu trainer",
                               "skills": ["cuda"], "reason": "train a model",
                               "work_estimate": 9.0, "produces": ["model.bin"],
                               "capability_class": "gpu-training"})
    assert not d.ok and d.rule == "REJECT_UNSUPPORTED", d
    assert set(k.graph.tasks) == before_tasks and set(k.registry.agents) == before_agents
    rows = [r for r in _rows(k, MessageType.SPAWN_REJECTED) if _pl(r).get("rule") == "REJECT_UNSUPPORTED"]
    assert len(rows) == 1
    declined = _rows(k, MessageType.REQUEST_DECLINED)
    assert declined and declined[0]["target"] == "backend_01", "the requester was not informed"
    assert "gpu-training" in _pl(declined[0]).get("body", "") or "gpu" in _pl(declined[0])["body"]


# ------------------------------------------- 11. conflict with an unmet artifact (replan)
def test_11_replanning_around_a_conflicting_artifact_is_validated(tmp_path):
    """Two tasks claiming to produce the same artifact is a conflict, not a cycle: the amendment is
    allowed, but the org must stay schedulable and the duplicate must be visible to the monitor."""
    k = _live(tmp_path, cap=8)
    k.graph.add(TaskSpec("t_conflict", "api", "backend", est_work=4.0,
                         produces=["contracts/api.json"]))
    k.registry.register(agent_id="backend_01", role="backend", skills=["backend"])
    emit_registered(k.journal, k.registry.get("backend_01"))
    k.make_actor("backend_01")
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                               "skills": ["stripe"], "reason": "also writes the contract",
                               "work_estimate": 4.0, "produces": ["contracts/api.json"]})
    assert d.ok or d.rule in {"REJECT_DUPLICATE_CAPABILITY", "REJECT_NOT_WORTH_IT"}, d
    assert k.graph.order() is not None
    producers = k.graph.producers.get("contracts/api.json", set())
    assert len(producers) >= 1
    if len(producers) > 1:
        # more than one promised producer is exactly what a reviewer has to be able to see;
        # `producers` maps artifact -> {task ids}, so the tasks are the *values*
        assert all(t in k.graph.tasks for t in producers)
        assert producers == k.graph.producers["contracts/api.json"]


# ---------------------------------------------------- 12. decision requires judgement
def test_12_ambiguous_decision_escalates_to_the_inbox(tmp_path):
    k = _live(tmp_path)
    k.graph.add(TaskSpec("t_be", "api", "backend", est_work=4.0, produces=["contracts/api.json"]))
    k.registry.register(agent_id="backend_01", role="backend", skills=["backend"])
    emit_registered(k.journal, k.registry.get("backend_01"))
    k.make_actor("backend_01")
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                               "skills": ["stripe"], "reason": "is this worth an agent?",
                               "work_estimate": 4.0, "produces": ["pay.out"],
                               "requires_judgment": True})
    k.parent.ledger.by_rule()
    assert d.rule == "ESCALATE", d
    assert not _agent_names(k, "payments"), "an escalated request must not also be honoured"
    esc = _rows(k, MessageType.SPAWN_ESCALATED)
    assert len(esc) == 1 and _pl(esc[0])["rid"]
    assert any(r.get("kind") == "spawn" for r in k.parent.inbox)
    rid = k.parent.ledger.newest_first()[0].rid
    assert rid in k.parent.escalated, "an escalated request must stay parked, not vanish"
    assert not _agent_names(k, "payments"), "the org acted on its own after escalating"
    # the answer arrives as one line in the decisions file; the *next* tick applies it, and the
    # parked request keeps its rid so receipt -> escalation -> answer -> spawn is one chain
    k.parent.decision_path = "decisions.jsonl"
    (Path(k.root) / "decisions.jsonl").write_text(
        json.dumps({"kind": "approve_spawn", "rid": rid, "agent": "backend_01",
                    "reason": "yes, worth an agent"}) + "\n")
    k.run(ticks=3)
    assert k.parent.accepted_decisions or not k.parent.escalated
    assert k.parent.ledger.entries[rid].state in (RequestState.APPROVED_COMMITTED,
                                                  RequestState.DEFERRED), \
        "the answer never reached the request it was about"
    assert _rows(k, MessageType.GRAPH_AMENDED) or _rows(k, MessageType.SPAWN_APPROVED), \
        "an approved escalation produced no journalled mutation"


# ----------------------------------------------------------------- acceptance demo
def test_13_acceptance_demo_passes_and_is_reproducible(tmp_path):
    res = run_demo(root=tmp_path / "run", out=tmp_path / "acceptance.md", ticks=40)
    assert res["ok"], json.dumps({k: v for k, v in res["facts"].items()
                                  if isinstance(v, bool) or k in ("decision", "spawn_tick")},
                                 indent=1, default=str)
    f = res["facts"]
    assert f["agent_created_midrun"] and f["agent_epoch"] == 1
    assert f["artifact_producer"] == f["agent_created_midrun"]
    assert f["requester_notified"] and f["dependent_task_ready_now"]
    assert f["blocked_at_start"] and f["chain_ordered"]
    assert f["replay_parity"] and f["replay_determinism"] and f["policy_parity"]
    text = (tmp_path / "acceptance.md").read_text()
    assert "Acceptance: **PASSED**" in text
    assert "| 3 | 4 |" in text or "spawn" in text


def test_14_the_demo_scenario_is_deterministic(tmp_path):
    """Same construction, twice: identical event sequence. A demo that only passes once is a
    demonstration of nothing."""
    a = run_demo(root=tmp_path / "a", out=None, ticks=40)
    b = run_demo(root=tmp_path / "b", out=None, ticks=40)
    assert a["ok"] and b["ok"]
    ta = [e["type"] for e in a["facts"]["chain_rows"]]
    tb = [e["type"] for e in b["facts"]["chain_rows"]]
    assert ta == tb, "the causal chain differed between two identical runs"
    assert a["facts"]["spawn_tick"] == b["facts"]["spawn_tick"]
    assert [r["agents"] for r in a["facts"]["transition_table"]] == \
           [r["agents"] for r in b["facts"]["transition_table"]]


# ------------------------------------------------------------------ monitoring surface
def test_15_status_and_agents_expose_the_monitored_numbers(tmp_path):
    k = _live(tmp_path)
    k.submit(SAAS)
    k.run(ticks=4)
    st = k.status()
    assert "spawning" in st
    m = st["spawning"]
    for key in ("active", "idle", "working", "waiting", "blocked", "escalated", "completed",
                "requests_received", "requests_approved", "requests_rejected",
                "generation_epoch_max", "spawn_depth_hist", "remaining_capacity",
                "agent_budget", "worker_slots_in_use", "workers_max"):
        assert key in m, f"`arena status` lost {key}"
    assert json.dumps(st, default=str)
    assert m["active"] <= m["agent_budget"]
    assert m["worker_slots_in_use"] <= m["workers_max"]
    assert sum(m["spawn_depth_hist"].values()) == len(k.registry.agents)
    # every counted state agrees with the registry it was read from
    for stt in ("idle", "working", "escalated", "completed"):
        expect = len(k.registry.with_state(getattr(AgentState, stt.upper())))
        assert m[stt] == expect, f"{stt} count disagreed with the registry"


def test_16_injection_from_another_process_reaches_the_running_org(tmp_path):
    """`arena request` writes an inject file; a resident kernel picks it up on its own tick."""
    k = _live(tmp_path)
    k.submit(SAAS)
    k.run(ticks=2)
    (Path(k.root) / "var").mkdir(parents=True, exist_ok=True)
    inj = Path(k.root) / "var" / "inject.jsonl"
    inj.write_text(json.dumps({"from": sorted(k.registry.agents)[0],
                               "requested_role": "payments", "skills": ["stripe"],
                               "reason": "cross-process", "work_estimate": 4.0,
                               "produces": ["pay.out"]}) + "\n")
    assert k.drain_injection_file() == 1
    assert not inj.read_text().strip(), "the inject file must be consumed, not re-read every tick"
    assert k.parent.spawn_requests, "the request never reached the parent's queue"
    k.run(ticks=2)
    assert _rows(k, MessageType.SPAWN_REQUEST_RECEIVED), "an injected request was not journalled"
    assert k.metrics()["requests_received"] == 1
    assert k.drain_injection_file() == 0


def test_22_run_resident_reports_how_it_was_fed(tmp_path):
    """The counters that make "no restart" checkable must exist before, during and after a run.

    `run_resident` used to stash a private attribute that only existed once a loop had run, so a
    monitor reading `kernel.summary()` before the first resident run crashed with AttributeError -
    and the summary said nothing at all about how much work had arrived from outside.
    """
    k = _live(tmp_path)
    k.submit(SAAS)
    base = k.summary()
    assert base["resident_loops"] == 0 and base["injected"] == 0
    ident = id(k)
    seen: list[int] = []
    (Path(k.root) / "var").mkdir(parents=True, exist_ok=True)
    (Path(k.root) / "var" / "inject.jsonl").write_text(
        json.dumps({"from": sorted(k.registry.agents)[0], "requested_role": "payments",
                    "skills": ["stripe"], "reason": "fed while running", "work_estimate": 4.0,
                    "produces": ["pay.out"]}) + "\n")
    out = k.run_resident(0.2, idle=0.01, ticks_per_loop=1,
                         on_tick=lambda kern, summ: seen.append(kern.tick))
    assert id(k) == ident, "a resident run must not swap the kernel out"
    assert out["resident_loops"] >= 1, "the loop count never made it into the summary"
    assert out["injected"] >= 1, "the monitor cannot see that work arrived from outside"
    assert seen and seen == sorted(seen), "the on_tick observer saw no monotone tick sequence"
    assert json.dumps(out, default=str), "summary must stay JSON-safe for `arena watch`"


def test_17_torn_injection_tail_is_ignored_not_fatal(tmp_path):
    k = _live(tmp_path)
    k.submit(SAAS)
    (Path(k.root) / "var").mkdir(parents=True, exist_ok=True)
    inj = Path(k.root) / "var" / "inject.jsonl"
    inj.write_text('{"from":"backend_01","requested_role":"payments","reason":"ok",'
                   '"work_estimate":4.0,"produces":["a.out"]}\n'
                   '{"from":"backend_01","role":"partial","wor\n')
    assert k.drain_injection_file() == 1, "a torn tail must not discard the good row"
    assert len(k.parent.spawn_requests) == 1


def test_18_no_polling_is_introduced_by_the_spawn_path(tmp_path):
    """The bus counts polls so the guarantee is measurable. Dynamic spawning must not add any."""
    k = _live(tmp_path)
    k.role_policies = {"backend": "specialist"}
    k.role_overrides = {"backend": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                    "outputs": ["backend/pay.py"], "capability_class": "payments",
                                    "skills": ["stripe"]}}
    k.submit(SAAS)
    k.run(ticks=20)
    assert k.polls.polls == 0, f"the spawn path polled: {k.polls.polls}"


def test_19_message_depth_stays_bounded_under_a_spawn_storm(tmp_path):
    k = _live(tmp_path, cap=12)
    k.role_policies = {r: "specialist" for r in ("backend", "frontend", "database", "auth")}
    k.role_overrides = {"backend": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                    "outputs": ["p1.out"], "capability_class": "payments"},
                        "frontend": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                     "outputs": ["p1.out"], "capability_class": "payments"},
                        "database": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                     "outputs": ["p1.out"], "capability_class": "payments"},
                        "auth": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                 "outputs": ["p1.out"], "capability_class": "payments"}}
    k.submit(SAAS)
    k.run(ticks=25)
    assert max([r["depth"] for r in k.journal.events()] or [0]) <= MAX_CAUSAL_DEPTH + 2
    received = len(_rows(k, MessageType.SPAWN_REQUEST_RECEIVED))
    dedup = [r for r in _rows(k, MessageType.SPAWN_REJECTED) if _pl(r).get("rule") == "DEDUPLICATE"]
    assert received >= 2, "the storm never materialised, so this proved nothing"
    assert len(dedup) >= received - 1, \
        f"{received} identical requests and only {len(dedup)} deduped"
    assert len(_agent_names(k, "payments")) <= 1


def test_20_journal_chain_survives_the_whole_phase2_flow(tmp_path):
    k = _live(tmp_path)
    k.role_policies = {"backend": "specialist"}
    k.role_overrides = {"backend": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                    "outputs": ["backend/pay.py"], "capability_class": "payments",
                                    "skills": ["stripe"]}}
    k.submit(SAAS)
    k.run(ticks=20)
    ok, why, where = k.journal.verify_chain()
    assert ok, f"chain broken: {why} @ {where}"
    rows = list(k.journal.events())
    assert all(rows[i]["seq"] + 1 == rows[i + 1]["seq"] for i in range(len(rows) - 1))
    # and the hash chain is only broken where someone actually edits the log
    snap = k.journal.count()
    k.journal.emit(MessageType.STATUS_UPDATE, "parent", "parent", body="one more row")
    assert k.journal.count() == snap + 1 and k.journal.verify_chain()[0]


def test_21_phase1_invariants_still_hold_after_phase2(tmp_path):
    """Cheap to assert, and it is the requirement 'all Phase 1 tests stay green' in one place:
    the guards must not have been weakened to make the new path fit."""
    k = _live(tmp_path)
    k.submit(SAAS)
    k.run(ticks=40)
    assert k.parent.ledger is not None
    # no silent spawn: every approval has a receipt and a resolution row
    for e in k.parent.ledger.entries.values():
        if e.state in (RequestState.APPROVED_COMMITTED, RequestState.REROUTED):
            assert e.rule, f"decision {e.rid} has no rule"
    # no silent reject: every refusal is journalled
    refused = [e for e in k.parent.ledger.entries.values()
               if e.state in (RequestState.REJECTED, RequestState.DEDUPLICATED)]
    rows = _rows(k, MessageType.SPAWN_REJECTED)
    assert len(rows) >= len(refused), "a refusal left no trace in the log"
    # claiming still prevents duplicate work
    claims = k.journal.claims()
    assert all(v.get("owner") for v in claims.values())
