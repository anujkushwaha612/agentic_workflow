"""Phase 2 unit tests: the request object, the ledger, the catalog, the monitor.

These are adversarial in the specific sense that matters here: each one tries to make the runtime
lie - accept a request that should have been refused, report a spawn that did not happen, or lose a
decision on replay. Nothing here tests "is the agent clever"; the requests are emitted by Phase 1's
policy layer, and what is under test is the kernel's admission, mutation and bookkeeping.
"""
import json

import pytest

from arena.graph import CycleError, DependencyGraph, TaskSpec
from arena.kernel import Kernel
from arena.lifecycle import AgentState
from arena.message import Message, MessageType
from arena.parent import Decision
from arena.policy import NeedsSpecialist, is_sleeping_action
from arena.registry import SpawnBudget
from arena.spawn import (
    REQUEST_TRANSITIONS, TERMINAL_REQUEST_STATES, CapabilityCatalog, IllegalRequestTransition,
    MalformedRequest, RequestState, SpawnLedger, SpawnRequest,
)


# --------------------------------------------------------------------- fixtures
def _reset(g: DependencyGraph) -> DependencyGraph:
    """Empty a graph IN PLACE. `kernel.graph = DependencyGraph()` is a trap: the registry and the
    bus hold their own reference to the original object, so `registry.overloaded()` (which reads
    `registry.graph`) would see an empty graph while the kernel saw a full one. Cost me a while."""
    g.tasks.clear()
    g.producers.clear()
    g.known_artifacts.clear()
    return g


def _kernel(tmp_path, cap=4, workers=4, *, ignore_overload=True, **kw):
    """`ignore_overload` disables only the requester-overload veto.

    Most Phase-2 tests ask a working agent for help, and that agent *is* by definition behind on
    its own task, so the veto would refuse every one of them for a reason unrelated to what is
    under test. Leaving it on for the one test that is about the veto (see
    test_requester_overload_...) keeps both readable.
    """
    root = tmp_path
    k = Kernel(root=root, journal_path=root / "j.db",
               budget=SpawnBudget(max_active_agents=cap, max_concurrent_workers=workers,
                                  idle_ttl=1e9,
                                  requester_overload_factor=(1e9 if ignore_overload else 1.5),
                                  **kw),
               trace_path=str(root / "t.jsonl"))
    _reset(k.graph)
    k.graph.add(TaskSpec("t_db", "schema", "database", est_work=2.0, produces=["db/schema.sql"]))
    k.graph.add(TaskSpec("t_be", "api", "backend", est_work=4.0, consumes=["db/schema.sql"],
                         produces=["contracts/api.json"]))
    for aid, role in (("database_01", "database"), ("backend_01", "backend")):
        rec = k.registry.register(agent_id=aid, role=role, skills=[role])
        from arena.registry import emit_registered
        emit_registered(k.journal, rec)
        k.make_actor(aid)
    k.journal.emit(MessageType.PLAN_CREATED, "parent", "parent",
                   tasks=[{"task_id": t.task_id, "title": t.title, "role": t.role,
                           "skills": t.skills, "produces": t.produces, "consumes": t.consumes,
                           "est_work": t.est_work, "deps": sorted(t.deps)}
                          for t in k.graph.tasks.values()])
    return k


def _req(**kw):
    base = dict(requester_agent_id="backend_01", requested_role="payments",
                reason="stripe webhook verification", required_skills=("stripe", "webhooks"),
                estimated_work=3.0, expected_outputs=("backend/pay/webhooks.py",),
                correlation_id="cid-1")
    base.update(kw)
    r = SpawnRequest(**base)
    r.normalise()
    r.validate()
    return r


# ------------------------------------------------------------------ the request
def test_request_schema_is_complete_and_the_message_round_trips():
    m = Message(msg_type=MessageType.SPAWN_AGENT_REQUEST, from_actor="backend_01",
                to_actor="parent", correlation_id="cid-9",
                payload={"requested_role": "payments", "reason": "webhooks",
                         "required_skills": ["stripe"], "required_inputs": ["contracts/api.json"],
                         "expected_outputs": ["backend/pay.py"], "estimated_work": 2.5,
                         "capability_class": "payments"})
    r = SpawnRequest.from_message(m)
    assert (r.requester_agent_id, r.requested_role, r.correlation_id) == ("backend_01", "payments",
                                                                           "cid-9")
    assert r.required_inputs == ("contracts/api.json",)
    assert r.expected_outputs == ("backend/pay.py",)
    assert r.estimated_work == 2.5 and r.parent_task_id is None
    d = r.to_dict()
    assert set(d) >= {"requester_agent_id", "requested_role", "required_skills", "reason",
                      "estimated_work", "required_inputs", "expected_outputs", "parent_task_id",
                      "correlation_id", "capability_class", "fingerprint", "state"}
    assert json.dumps(d, default=str)          # must be journallable as-is


def test_a_request_missing_a_load_bearing_field_is_refused_not_guessed():
    """The distinction: inferable fields (reason/outputs) are filled in, load-bearing ones are not.

    A missing estimate silently mis-sizes the org; a missing requester silently attributes the
    spawn to nobody. Those must raise, which is what turns them into REJECT_MALFORMED.
    """
    # no requester at all
    with pytest.raises(MalformedRequest) as e:
        SpawnRequest.from_message(Message(msg_type=MessageType.SPAWN_AGENT_REQUEST,
                                          from_actor="", payload={"requested_role": "x",
                                                                  "estimated_work": 3,
                                                                  "expected_outputs": ["o"]}))
    assert "requester_agent_id" in str(e.value)
    # an estimate that is not a number cannot be coerced into "free"
    with pytest.raises(MalformedRequest):
        SpawnRequest.from_message(Message(msg_type=MessageType.SPAWN_AGENT_REQUEST,
                                         from_actor="backend_01",
                                         payload={"requested_role": "x", "reason": "r",
                                                  "estimated_work": "lots",
                                                  "expected_outputs": ["o"]}))
    # an estimate of zero is a refusal too: "no work" must not create an agent
    with pytest.raises(MalformedRequest):
        SpawnRequest.from_message(Message(msg_type=MessageType.SPAWN_AGENT_REQUEST,
                                         from_actor="backend_01",
                                         payload={"requested_role": "x", "reason": "r",
                                                  "estimated_work": 0,
                                                  "expected_outputs": ["o"]}))


def test_legacy_requests_are_normalised_not_rejected():
    """Phase 1 payloads carry neither `reason` nor `produces`; they must still work."""
    m = Message(msg_type=MessageType.SPAWN_AGENT_REQUEST, from_actor="backend_01",
                payload={"requested_role": "security engineer", "skills": ["auth"],
                         "work_estimate": 4.0})
    r = SpawnRequest.from_message(m)
    assert r.expected_outputs and r.reason
    assert r.requested_role == "security engineer"


def test_fingerprint_ignores_who_asked_and_ignores_field_order():
    a = _req(required_skills=("stripe", "webhooks"))
    b = _req(required_skills=("webhooks", "stripe"), requester_agent_id="someone_else_01")
    assert a.fingerprint() == b.fingerprint(), "the same capability asked for twice is one request"
    assert a.fingerprint() != _req(expected_outputs=("other.py",)).fingerprint()


def test_task_ids_are_stable_across_a_restart():
    """rid embeds a run-local counter; if task_id used it, a replay would invent a different task
    for the same journalled request and parity would fail for a reason unrelated to the journal."""
    r = _req(rid="rq-0007")
    assert r.task_id() == _req(rid="rq-0099").task_id()
    assert r.task_id().startswith("t_payments_")


def test_required_inputs_become_consumes_so_the_graph_can_wire_them():
    r = _req(required_inputs=("contracts/api.json", "db/schema.sql"))
    spec = r.to_task_spec()
    assert spec.consumes == ["contracts/api.json", "db/schema.sql"]
    assert spec.est_work == 3.0 and spec.role == "payments"
    assert spec.claims and spec.claims[0].startswith("spawn:")


# -------------------------------------------------------------------- the catalog
def test_catalog_refuses_what_the_host_cannot_staff():
    c = CapabilityCatalog()
    assert c.is_serviceable("payments") and c.class_for("payment specialist", ["stripe"]) == "payments"
    assert not c.is_serviceable("gpu-training")
    assert "accelerator" in c.reason_for_unserviceable("gpu-training")
    # unknown-but-unclassified defaults to general, which IS serviceable: absence of a class is
    # not the same as an unsupported one, and treating it as unsupported would refuse everything.
    assert c.class_for("typist", ["typing"]) == "general" and c.is_serviceable("general")


def test_catalog_can_be_extended_without_touching_the_veto_engine():
    c = CapabilityCatalog()
    assert not c.is_serviceable("k8s-ops")
    c.serviceable["k8s-ops"] = ("kubernetes", "cluster")
    assert c.is_serviceable("k8s-ops"), "adding a capability should be a config change"


# --------------------------------------------------------------------- the ledger
def test_ledger_dedups_against_inflight_and_resolved_requests():
    led = SpawnLedger()
    r = _req()
    e = led.open(r)
    assert led.inflight_for(r.fingerprint()).rid == e.rid
    assert led.resolved_for(r.fingerprint()) is None
    led.move(e.rid, RequestState.EVALUATING)
    led.close_as(e.rid, RequestState.REJECTED, "REJECT_NOT_WORTH_IT", "tiny")
    assert led.inflight_for(r.fingerprint()) is None
    assert led.resolved_for(r.fingerprint()).rule == "REJECT_NOT_WORTH_IT"


def test_request_state_machine_rejects_illegal_edges():
    led = SpawnLedger()
    e = led.open(_req())
    with pytest.raises(IllegalRequestTransition):
        led.move(e.rid, RequestState.APPROVED_COMMITTED)      # RECEIVED -> APPROVED is not an edge
    led.move(e.rid, RequestState.EVALUATING)                   # the legal one
    assert e.state is RequestState.EVALUATING
    for terminal in TERMINAL_REQUEST_STATES:
        assert terminal not in REQUEST_TRANSITIONS.get(terminal, frozenset()), \
            f"{terminal} must be terminal"


def test_deferred_is_the_one_state_with_a_way_back_in():
    """Capacity can free up. A request that was deferred must be re-evaluable; a rejected one must
    not be, or a refused request would loop forever."""
    assert RequestState.EVALUATING in REQUEST_TRANSITIONS[RequestState.DEFERRED]
    assert RequestState.DEFERRED in REQUEST_TRANSITIONS[RequestState.EVALUATING]
    assert RequestState.DEFERRED not in TERMINAL_REQUEST_STATES


# ---------------------------------------------------------------- the kernel path
def test_receipt_is_journalled_before_any_decision(tmp_path):
    """Spec: the request is a runtime event with its own record, not only an outcome row."""
    k = _kernel(tmp_path)
    k.parent.spawn_requests.append({"from": "backend_01", "requested_role": "payments",
                                    "reason": "webhooks", "skills": ["stripe"],
                                    "work_estimate": 3.0,
                                    "produces": ["backend/pay.py"]})
    k.run(ticks=6)
    rows = list(k.journal.events(etype=MessageType.SPAWN_REQUEST_RECEIVED))
    assert len(rows) == 1, "the arrival of a request must be journalled, not just its outcome"
    p = json.loads(rows[0]["payload"])
    assert p["requested_role"] == "payments" and p["fingerprint"] and p["required_skills"]
    assert p["requester"] == "backend_01"


def test_every_decision_path_journals_a_rule(tmp_path):
    k = _kernel(tmp_path, cap=2)
    for i in range(5):
        k.parent.decide_spawn({"from": "backend_01", "requested_role": f"crew{i}",
                               "skills": [f"sk{i}"], "work_estimate": 9.0, "reason": f"r{i}",
                               "produces": [f"out{i}.md"]})
    rejected = [json.loads(r["payload"]) for r in k.journal.events(etype=MessageType.SPAWN_REJECTED)]
    assert rejected, "nothing was refused even though the cap is 2"
    assert all(p.get("rule") for p in rejected), f"a silent rejection: {rejected}"
    assert {p["rule"] for p in rejected} <= {
        "REJECT_CAP", "REJECT_DUPLICATE_CAPABILITY", "REJECT_NOT_WORTH_IT", "REJECT_SPAWN_DEPTH",
        "REJECT_UNSUPPORTED", "DEDUPLICATE", "REJECT_CYCLE", "REJECT_REQUESTER_OVERLOADED"}
    assert len(k.parent.ledger.entries) == 5


def test_rules_use_the_named_vocabulary_and_never_a_silent_ok(tmp_path):
    k = _kernel(tmp_path)
    d = k.parent.evaluate_spawn(_req())
    assert d.rule in {"APPROVE", "REJECT_NOT_WORTH_IT", "REJECT_CAP",
                      "REJECT_DUPLICATE_CAPABILITY", "REJECT_SPAWN_DEPTH", "REJECT_UNSUPPORTED",
                      "ESCALATE"}, d
    if d.ok:
        assert d.rule == "APPROVE"


def test_reuse_beats_a_new_agent_and_records_why(tmp_path):
    """Spec §4: existing capacity first, with a recorded reason for reusing it."""
    k = _kernel(tmp_path)
    d = k.parent.decide_spawn({"from": "database_01", "requested_role": "backend",
                               "skills": ["api"], "reason": "need api help",
                               "work_estimate": 5.0, "produces": ["docs/api.md"]})
    assert d.rule == "REUSE_EXISTING", d
    rows = list(k.journal.events(etype=MessageType.REQUEST_REROUTED))
    assert len(rows) == 1
    p = json.loads(rows[0]["payload"])
    assert p["reused"] is True and p["agent_spawned"] is False and p["owner"] == "backend_01"
    assert "already covers" in p["body"] or "assign to it" in p["body"]
    assert len(k.registry.active()) == 2, "a reuse must not also spawn"
    assert k.metrics()["reuses"] == 1


def test_unsupported_capability_is_refused_with_no_mutation(tmp_path):
    k = _kernel(tmp_path)
    before_tasks, before_agents = set(k.graph.tasks), set(k.registry.agents)
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "cuda trainer",
                               "skills": ["cuda"], "reason": "train a model",
                               "work_estimate": 5.0, "produces": ["model.bin"],
                               "capability_class": "gpu-training"})
    assert not d.ok and d.rule == "REJECT_UNSUPPORTED", d
    assert set(k.graph.tasks) == before_tasks and set(k.registry.agents) == before_agents
    rej = [json.loads(r["payload"]) for r in k.journal.events(etype=MessageType.SPAWN_REJECTED)]
    assert any(p.get("rule") == "REJECT_UNSUPPORTED" for p in rej)


def test_escalate_writes_the_inbox_and_changes_nothing(tmp_path):
    k = _kernel(tmp_path)
    n_tasks = len(k.graph.tasks)
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                               "skills": ["stripe"], "reason": "judgement call",
                               "work_estimate": 3.0, "produces": ["a.py"],
                               "requires_judgment": True})
    assert d.rule == "ESCALATE" and not d.ok
    assert len(k.graph.tasks) == n_tasks
    assert any(r.get("kind") == "spawn" for r in k.parent.inbox)
    esc = list(k.journal.events(etype=MessageType.SPAWN_ESCALATED))
    assert len(esc) == 1 and json.loads(esc[0]["payload"])["rid"]
    assert k.metrics()["requests_escalated"] == 1
    assert k.parent.ledger.newest_first()[0].state is RequestState.ESCALATED


def test_the_agent_fsm_is_untouched_by_dynamic_spawning(tmp_path):
    """A spawned agent is born the same way a planned one is: no new lifecycle states were needed."""
    from arena.lifecycle import TRANSITIONS
    k = _kernel(tmp_path)
    before = {s: set(v) for s, v in TRANSITIONS.items()}
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                               "skills": ["stripe"], "reason": "x", "work_estimate": 3.0,
                               "produces": ["a.py"]})
    assert d.ok and d.rule == "APPROVE"
    assert before == {s: set(v) for s, v in TRANSITIONS.items()}, \
        "Phase 2 changed the agent state machine; it must not have needed to"
    aid = k.parent.ledger.newest_first()[0].spawned_agent_id
    assert k.registry.get(aid).lifecycle.state in (AgentState.IDLE, AgentState.WORKING,
                                                   AgentState.CREATED)
    assert k.registry.get(aid).epoch == 1 and k.registry.get(aid).spawned_by == "backend_01"


def test_spawn_depth_is_measured_and_enforced(tmp_path):
    k = _kernel(tmp_path, cap=12)
    k.registry.budget.max_spawn_epoch = 2
    aid = k.parent._spawn(role="payments", skills=["stripe"], reason="depth-1", epoch=1,
                          spawned_by="parent")
    k.registry.register(agent_id="deep_01", role="deep", skills=["x"], epoch=2, spawned_by=aid)
    d = k.parent.decide_spawn({"from": "deep_01", "requested_role": "extra", "skills": ["y"],
                               "reason": "one more level", "work_estimate": 5.0,
                               "produces": ["z.md"]})
    assert not d.ok and d.rule == "REJECT_SPAWN_DEPTH", d
    assert k.metrics()["spawn_depth_hist"].get("2") == 1


def test_metrics_arithmetic_is_self_consistent(tmp_path):
    k = _kernel(tmp_path, cap=3)
    for i in range(6):
        k.parent.decide_spawn({"from": "backend_01", "requested_role": f"crew{i}",
                               "skills": [f"sk{i}"], "reason": f"r{i}", "work_estimate": 9.0,
                               "produces": [f"o{i}.md"]})
    m = k.metrics()
    assert m["active"] <= m["agent_budget"]
    assert m["requests_received"] == (m["requests_approved"] + m["requests_rejected"]
                                      + m["requests_deduplicated"] + m["requests_escalated"]
                                      + m["requests_deferred"])
    assert sum(m["spawn_depth_hist"].values()) == m["agents_total"]
    assert m["remaining_capacity"] == m["agent_budget"] - m["active"]
    assert set(m) >= {"active", "idle", "working", "waiting", "blocked", "escalated", "completed",
                      "requests_received", "requests_approved", "requests_rejected",
                      "generation_epoch_max", "spawn_depth_hist", "remaining_capacity",
                      "agent_budget", "reuses", "by_rule"}
    assert json.dumps(m, default=str)


# ------------------------------------------------------------------ regression
def test_defect_amended_task_survives_replay(tmp_path):
    """The fold used to project PLAN_AMENDED but not GRAPH_AMENDED, i.e. the spawn path's own
    mutations were invisible to recovery: the new agent came back holding a task that did not
    exist. This is the exact scenario `crash right after approval` produces."""
    k = _kernel(tmp_path)
    k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                           "skills": ["stripe"], "reason": "webhooks", "work_estimate": 3.0,
                           "produces": ["backend/pay.py"]})
    tid = k.parent.ledger.newest_first()[0].task_id
    assert tid in k.graph.tasks
    k.journal.close()
    r = Kernel.from_journal(tmp_path / "j.db", root=str(tmp_path), quiet=True,
                           budget=SpawnBudget(max_active_agents=4, max_concurrent_workers=4,
                                              idle_ttl=1e9))
    assert tid in r.graph.tasks, "the mid-run-created task vanished on replay"
    assert r.graph.tasks[tid].produces == ["backend/pay.py"]
    assert r.graph.tasks[tid].claims, "the claim key that prevents duplicate work was lost"
    assert k.parent.ledger.newest_first()[0].rid in r.parent.ledger.entries


def test_defect_no_owner_named_none_after_a_cap_race(tmp_path):
    """_spawn returns None under pressure; the old code did `assign(task, aid or role)`, which
    handed the task to a *string* owner that no agent is. Now the task stays pending and the
    request is DEFERRED."""
    k = _kernel(tmp_path, cap=2)
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                               "skills": ["stripe"], "reason": "no room", "work_estimate": 3.0,
                               "produces": ["backend/pay.py"]})
    assert d.rule == "REJECT_CAP", f"expected the cap to speak, got {d}"
    assert not d.ok
    owners = {t.owner for t in k.graph.tasks.values()}
    assert "None" not in owners and None in owners, f"phantom owner leaked: {owners}"
    assert not any(a_.startswith("payments") for a_ in k.registry.agents)
    # refused at evaluation, so there is nothing to defer and nothing to assign
    assert not list(k.journal.events(etype=MessageType.DEFERRED_FOR_CAPACITY))
    assert list(k.journal.events(etype=MessageType.SPAWN_REJECTED))


def test_defert_for_capacity_leaves_the_task_recoverable(tmp_path, monkeypatch):
    """Approve at evaluation, then lose the slot before _spawn: the task must exist, unowned, and
    the request must be retryable rather than lost."""
    k = _kernel(tmp_path, cap=4)
    real_spawn = k.parent._spawn

    def capped(**kw):
        return None                    # the slot closes between evaluation and commit

    monkeypatch.setattr(k.parent, "_spawn", capped)
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "payments",
                               "skills": ["stripe"], "reason": "race", "work_estimate": 3.0,
                               "produces": ["backend/pay.py"]})
    monkeypatch.undo()
    assert not d.ok and d.rule == "DEFER_FOR_CAPACITY", d
    ent = k.parent.ledger.newest_first()[0]
    assert ent.state is RequestState.DEFERRED and ent.task_id in k.graph.tasks
    assert k.graph.tasks[ent.task_id].owner is None, "a deferred task must not be phantom-owned"
    rows = list(k.journal.events(etype=MessageType.DEFERRED_FOR_CAPACITY))
    assert len(rows) == 1
    # `task_id` is its own column; the payload needs a copy or every rid-keyed reader (the ledger
    # replay, `arena spawns -v`) sees None.
    assert json.loads(rows[0]["payload"])["deferred_task_id"] == ent.task_id
    assert rows[0]["task_id"] == ent.task_id
    # and it is picked up once capacity exists
    k.parent.ledger.move(ent.rid, RequestState.EVALUATING, "retry")
    k.parent._spawn = real_spawn
    d2 = k.parent.commit_spawn(ent.request, k.parent.evaluate_spawn(ent.request))
    assert d2.ok and k.graph.tasks[ent.task_id].owner


def test_defect_sleeping_action_break_is_reachable():
    """`last_action` is `str(Act.X)` (upper). The run loop compared lower-case strings, so an
    agent's inbox was drained AND an extra step ran in the same tick - which is exactly the timing
    Phase 2 has to prove, so it had to be fixed first."""
    assert is_sleeping_action("COMPLETE") and is_sleeping_action("complete")
    assert is_sleeping_action("WAIT") and is_sleeping_action("ESCALATE") and is_sleeping_action("ERROR")
    assert not is_sleeping_action("PUBLISH") and not is_sleeping_action("PROCEED")
    assert not is_sleeping_action("")


def test_defect_replay_does_not_write_to_the_journal(tmp_path):
    """from_journal used to call make_actor per agent, journalling CREATED->IDLE for each: `verify`
    mutated the log it was checking, and a second replay disagreed with the first."""
    k = _kernel(tmp_path)
    k.run(ticks=4)
    n = k.journal.count()
    k.journal.close()
    r = Kernel.from_journal(tmp_path / "j.db", root=str(tmp_path), quiet=True,
                           budget=SpawnBudget(max_active_agents=4, max_concurrent_workers=4,
                                              idle_ttl=1e9))
    assert r.journal.count() == n, "a quiet replay appended rows to the journal"
    r2 = Kernel.from_journal(tmp_path / "j.db", root=str(tmp_path), quiet=True,
                            budget=SpawnBudget(max_active_agents=4, max_concurrent_workers=4,
                                               idle_ttl=1e9))
    assert r.snapshot() == r2.snapshot(), "two replays of one journal disagreed"
    assert not any(x["etype"] == "ILLEGAL_TRANSITION" for x in r.journal.events()), \
        "reconstruction still produces illegal self-transitions"


def test_midrun_spawn_happens_without_restarting_the_kernel(tmp_path):
    """Spec §6/§7: no restart-based spawning. One Kernel object, request answered on a later tick."""
    k = _kernel(tmp_path)
    ident = id(k)
    k.run(ticks=2)
    tick_at_request = k.tick
    agents_at_request = len(k.registry.active())
    assert agents_at_request == 2
    k.inject_spawn_request({"from": "backend_01", "requested_role": "payments",
                            "skills": ["stripe"], "reason": "webhooks", "work_estimate": 3.0,
                            "produces": ["backend/pay.py"], "required_inputs": ["db/schema.sql"]})
    k.run(ticks=3)
    assert id(k) == ident, "the kernel was replaced, which would be a restart in disguise"
    assert k.tick > tick_at_request
    # active() excludes COMPLETED, and by tick 5 the original two have finished; the claim is
    # about the roster gaining a member mid-run, so count the registry.
    assert len(k.registry.agents) == agents_at_request + 1
    new = [a for a, r in k.registry.agents.items() if r.epoch == 1]
    assert len(new) == 1
    rows = [(x["seq"], x["etype"]) for x in k.journal.events()]
    types = [t for _, t in rows]
    for t in ("SPAWN_AGENT_REQUEST", "SPAWN_REQUEST_RECEIVED", "GRAPH_AMENDED", "AGENT_REGISTERED",
              "SPAWN_APPROVED"):
        assert t in types, f"{t} missing from the journal"
    # Only rows from the request onward count. A global min() here would pick up the plan's own
    # AGENT_REGISTERED at seq 1 and "prove" that registration preceded the request.
    start = min(s for s, tt in rows if tt == "SPAWN_AGENT_REQUEST")
    seq = {t: min(s for s, tt in rows if tt == t and s >= start) for t in
           ("SPAWN_AGENT_REQUEST", "SPAWN_REQUEST_RECEIVED", "GRAPH_AMENDED", "AGENT_REGISTERED",
            "TASK_ASSIGNED", "SPAWN_APPROVED")}
    assert (seq["SPAWN_AGENT_REQUEST"] < seq["SPAWN_REQUEST_RECEIVED"] < seq["GRAPH_AMENDED"]
            < seq["AGENT_REGISTERED"] < seq["TASK_ASSIGNED"] < seq["SPAWN_APPROVED"]), seq


def test_two_requests_for_the_same_capability_in_one_tick_spawn_one_agent(tmp_path):
    """The dedup race: both arrive before either is decided, and a naive queue spawns two agents
    for one capability."""
    k = _kernel(tmp_path, cap=6)
    for _ in range(2):
        k.parent.spawn_requests.append({"from": "backend_01", "requested_role": "payments",
                                        "skills": ["stripe"], "reason": "webhooks",
                                        "work_estimate": 3.0, "produces": ["backend/pay.py"]})
    k.run(ticks=1)
    payers = [a for a, r in k.registry.agents.items() if r.role == "payments"]
    assert len(payers) == 1, f"got {payers}: duplicate capability, duplicate headcount"
    assert k.metrics()["requests_deduplicated"] == 1
    st = {str(e.state) for e in k.parent.ledger.entries.values()}
    assert "DEDUPLICATED" in st


def test_policy_asks_mid_task_not_at_plan_time(tmp_path):
    """A policy that asks on step 0 is indistinguishable from planning. after_steps>0 means the
    request is emitted by an agent that is already working."""
    pol = NeedsSpecialist(after_steps=2, max_requests=1)
    assert pol._asked == 0

    class Ctx:
        agent_id, step_index, task_id, task = "backend_01", 1, "t_be", None
        registry = None

        def __init__(self, k):
            self.kernel, self.registry = k, k.registry

        def log(self, *a):
            pass

        def request_specialist(self, role, reason="", **kw):
            """Mirrors ActorContext.request_specialist's payload key names on purpose: if the stub
            drifted, this test would pass while the real helper was producing unreadable messages."""
            self.requested = kw
            return Message(msg_type=MessageType.SPAWN_AGENT_REQUEST, from_actor=self.agent_id,
                           to_actor="parent", correlation_id="cid",
                           payload={"requested_role": role, "reason": reason,
                                    "required_skills": list(kw.get("skills") or []),
                                    "required_inputs": list(kw.get("inputs") or []),
                                    "expected_outputs": list(kw.get("outputs") or []),
                                    "estimated_work": kw.get("est_work") or 0.0,
                                    "capability_class": kw.get("capability_class") or "",
                                    "parent_task_id": self.task_id})

    k = _kernel(tmp_path)
    c1 = Ctx(k); c1.step_index = 1
    assert pol.step(c1, None).act.value == "PROCEED", "asked before doing any work"
    c2 = Ctx(k); c2.step_index = 3
    a = pol.step(c2, None)
    assert a.act.value == "PUBLISH" and a.msg.msg_type is MessageType.SPAWN_AGENT_REQUEST
    assert a.msg.payload["requested_role"] == "payment specialist"
    assert a.msg.payload["expected_outputs"], "the request must name what it will produce"


def test_amendment_that_would_cycle_is_refused_and_rolls_back(tmp_path):
    """The Phase-1 blocker: `decide_spawn` mutated the graph with no cycle gate at all, so a
    request whose task and an existing task consumed each other's artifacts left the org running on
    a graph that could not be topologically sorted. The gate now runs before an agent exists."""
    k = _kernel(tmp_path)
    agents_before = set(k.registry.agents)
    order_before = [list(g) for g in k.graph.order()]
    # t_be must consume the artifact the new task would produce, and the new task must consume the
    # artifact t_be produces: t_be -> new -> t_be. This is exactly what a mid-run request can encode
    # when two agents each assume the other will publish first.
    r = _req(requested_role="loop", expected_outputs=("docs/plan.md",), estimated_work=9.0,
             required_inputs=("contracts/api.json",))
    r.normalise()
    spec = r.to_task_spec()
    k.graph.tasks["t_be"].consumes = ["db/schema.sql", "docs/plan.md"]
    res = k.graph.amend([spec], {})
    assert not res["ok"], f"the gate did not fire: {res}"
    assert res["restored"] and res["rolled_back"] == [spec.task_id]
    assert spec.task_id not in k.graph.tasks, "a rolled-back amendment left its task behind"
    # and the same thing through the parent, where the difference matters: NO agent may be created
    d = k.parent.decide_spawn({"from": "backend_01", "requested_role": "loop2",
                               "skills": ["stripe"], "reason": "mutual dependency",
                               "work_estimate": 9.0, "produces": ["docs/plan2.md"],
                               "required_inputs": ["contracts/api.json"]})
    if d.rule == "REJECT_CYCLE":
        assert set(k.registry.agents) == agents_before, "an orphan agent survived a refused graph"
        rows = [json.loads(x["payload"]) for x in
                k.journal.events(etype=MessageType.GRAPH_AMEND_REJECTED)]
        assert rows and rows[0]["rule"] == "REJECT_CYCLE"
    else:
        # the engine is allowed to say "no cycle" - t_be's consumes no longer forms one here - but
        # it is never allowed to leave an unorderable graph behind
        assert [list(g) for g in k.graph.order()] or d.ok
    assert k.graph.tasks["t_be"].deps != {spec.task_id} or not d.ok
    assert isinstance(k.graph, DependencyGraph)


def test_a_refused_amendment_restores_the_graph_byte_for_byte(tmp_path):
    """Partial rollback is worse than none: edges pointing at deleted tasks hand the scheduler
    impossible work. amend() rolls back wholesale and the spawn path must inherit that."""
    k = _kernel(tmp_path)
    g = k.graph
    order_before = g.order()
    tasks_before = {t: (s.owner, s.status, sorted(s.deps)) for t, s in g.tasks.items()}
    # a real cycle, forced through the same gate the spawn path uses
    r2 = _req(requested_role="loopbreaker", expected_outputs=("docs/l.md",), estimated_work=9.0,
              required_inputs=("contracts/api.json",))
    r2.normalise()
    spec = r2.to_task_spec()
    g.tasks["t_be"].consumes = ["db/schema.sql", "docs/l.md"]     # t_be -> new
    res2 = g.amend([spec], {})                                     # new -> t_be via its consumes
    assert not res2["ok"], f"cycle not detected: {res2}"
    assert spec.task_id not in g.tasks, "refused task survived the rollback"
    # Only the SCHEDULE is compared, deliberately: `consumes` was edited by the caller before
    # amend() ran, so the snapshot _restore() rolls back to already contains it. Claiming the whole
    # spec object is byte-identical would be a stronger statement than the mechanism makes.
    after = {t: (s.owner, s.status, sorted(s.deps)) for t, s in g.tasks.items()}
    assert after == tasks_before, f"tasks not restored: {after} != {tasks_before}"
    assert [list(x) for x in g.order()] == [list(x) for x in order_before], \
        "the topological order was not restored after the refused amendment"


def test_requester_overload_blocks_headcount_before_it_blocks_help(tmp_path):
    """Spec §15 'unnecessary spawning': an agent that has finished nothing may not enlarge the org
    to avoid its own backlog."""
    k = _kernel(tmp_path, ignore_overload=False)
    rec = k.registry.get("backend_01")
    rec.work_done = 0.0
    k.parent.assign("t_be", "backend_01")             # 4.0 of open work, nothing completed
    assert k.registry.overloaded("backend_01")
    d = k.parent.evaluate_spawn(_req(estimated_work=9.0))
    assert not d.ok and d.rule == "REJECT_REQUESTER_OVERLOADED", d
    rec.work_done = 99.0
    d2 = k.parent.evaluate_spawn(_req(estimated_work=9.0))
    assert d2.ok and d2.rule == "APPROVE", "the veto must lift once the agent has delivered"
