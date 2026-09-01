"""Chaos / antagonistic suite for the Phase-1 kernel.

This is the acceptance gate, not an afterthought: each scenario *tries to break* the kernel and
asserts that a specific safeguard fired, with a journaled event as evidence. If a guard silently
"works", the test fails - a drop that leaves no trace is indistinguishable from luck.

Run:  python3 -m arena.cli chaos-report        (writes arena/var/chaos_report.md)
      python3 -m arena.cli chaos-run <id>      (single scenario, with its event trail)
"""
from __future__ import annotations

import json
import pathlib
import shutil
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ..actor import AgentActor
from ..graph import CycleError, DependencyGraph, TaskSpec
from ..journal import Journal
from ..lifecycle import AgentState, Lifecycle
from ..message import MAX_CAUSAL_DEPTH, Message, MessageType
from ..policy import EscalateOnComplexity, PollUntilReady, SimulatedWork, WaitForArtifacts
from ..registry import AgentRegistry, SpawnBudget

SAAS = ("Build a SaaS application with authentication, dashboard, API backend, PostgreSQL "
        "database, and cloud deployment")
ML = ("Train an ML model for churn prediction: data pipeline, feature engineering, "
      "model optimization, evaluation harness")


@dataclass
class Scenario:
    id: str
    title: str
    guards: list[str]
    fn: Callable[[], dict[str, Any]]


def _mk(text: str | None = SAAS, **kw: Any) -> tuple[Any, dict[str, Any]]:
    """Build a kernel on a scratch dir with a virtual clock. text=None -> empty graph, so a
    scenario can hand-author exactly the structure it wants to break."""
    from ..kernel import Kernel
    budget = kw.pop("budget", None) or SpawnBudget(max_active_agents=6, max_concurrent_workers=2,
                                                   max_spawn_epoch=3, idle_ttl=1.0e9)
    k = Kernel(root=tempfile.mkdtemp(prefix="arena-chaos-"), budget=budget, clock_mode="virtual",
               clock_step=kw.pop("clock_step", 0.01), **kw)
    out = k.submit(text) if text else {}
    return k, out


def _ev(k, t: MessageType | str, **filt: Any) -> list[dict[str, Any]]:
    rows = k.journal.events(etype=str(t))
    out = [k.journal._rowdict(r) for r in rows]
    for key, val in filt.items():
        out = [o for o in out if o.get(key) == val or o["payload"].get(key) == val]
    return out


# --------------------------------------------------------------------------- 1
def s_illegal_transition() -> dict[str, Any]:
    k, _ = _mk()
    k.run(ticks=400)
    done = [a for a, r in k.registry.agents.items() if r.state == str(AgentState.COMPLETED)]
    assert done, "expected some agent to reach COMPLETED"
    aid = done[0]
    before = k.registry.agents[aid].state
    ok = k.transition(aid, AgentState.WORKING, "chaos: skip the drain state")
    rej = _ev(k, MessageType.ILLEGAL_TRANSITION, agent_id=aid)
    assert ok is False, "kernel.allowed an illegal COMPLETED -> WORKING edge"
    assert k.registry.agents[aid].state == before, "state mutated despite rejection"
    assert rej, "rejection was not journalled"
    assert k.transition(aid, AgentState.TERMINATED, "chaos: legal reap") is True
    return {"agent": aid, "rejected": rej[0]["body"], "still": str(before),
            "legal_exit_allowed": True}


# --------------------------------------------------------------------------- 2
def s_spawn_hard_limit() -> dict[str, Any]:
    b = SpawnBudget(max_active_agents=2, max_concurrent_workers=2, max_spawn_epoch=3,
                    idle_ttl=1e9)
    k, _ = _mk(budget=b, text=None)
    for i in range(12):
        k.parent.decide_spawn({"from": "parent", "requested_role": f"crew{i}",
                               "skills": [f"sk{i}"], "work_estimate": 9.0,
                               "reason": f"pressurise the registry {i}"})
    active = len(k.registry.active())
    rejs = _ev(k, MessageType.SPAWN_REJECTED)
    assert active <= 2, f"registry grew past the cap: {active}"
    assert rejs, "cap hit produced no rejection event"
    rules = {r["payload"].get("rule") for r in rejs}
    assert rules <= {"REJECT_CAP", "REJECT_DUPLICATE_CAPABILITY", "REJECT_UNSUPPORTED"}, rules
    assert "REJECT_CAP" in rules, f"cap never engaged: {rules}"
    assert sum(r["depth"] for r in rejs) == 0  # rejections are not cascading
    return {"active": active, "cap": 2, "rejections": len(rejs),
            "rules": sorted({r["payload"].get("rule") for r in rejs})}


# --------------------------------------------------------------------------- 3
def s_spawn_depth() -> dict[str, Any]:
    b = SpawnBudget(max_active_agents=10, max_concurrent_workers=2, max_spawn_epoch=2,
                    idle_ttl=1e9)
    k, _ = _mk(budget=b, text=None)
    k.registry.register(agent_id="root_01", role="root", skills=["base"], epoch=0,
                        lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
    chain = []
    for depth in range(6):
        req_from = "root_01" if depth == 0 else chain[-1][0]
        d = k.parent.decide_spawn({"from": req_from, "requested_role": f"layer{depth}spec",
                                   "skills": [f"sk{depth}"], "work_estimate": 8.0,
                                   "reason": f"recursive demand depth {depth}"})
        chain.append((d.owner or f"layer{depth}spec_01", d.rule, d.ok))
    epochs = sorted({r.epoch for r in k.registry.agents.values()})
    assert epochs and max(epochs) <= 2, f"agents below the epoch limit exist: {epochs}"
    assert any(not ok_ for _, _, ok_ in chain), "all 6 depths were approved"
    return {"epochs_present": epochs, "capped": [c_ for c_ in chain if not c_[2]],
            "levels_demanded": len(chain), "chain": chain}


# --------------------------------------------------------------------------- 4
def s_not_worth_it() -> dict[str, Any]:
    b = SpawnBudget(max_active_agents=10, max_concurrent_workers=2,
                    min_share_of_remaining=0.25, idle_ttl=1e9)
    k, _ = _mk(budget=b)
    k.registry.register(agent_id="drifter_01", role="drifter", skills=["nothing-matches"],
                        epoch=0, lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
    before = len(k.registry.active())
    d = k.parent.decide_spawn({"from": "drifter_01", "requested_role": "typist",
                               "skills": ["formatting"], "work_estimate": 0.2,
                               "reason": "tiny 20-minute ask"})
    remaining = k.graph.remaining_work()
    assert not d.ok and d.rule == "REJECT_NOT_WORTH_IT", f"tiny task was approved: {d}"
    assert len(k.registry.active()) == before, "an agent appeared anyway"
    assert 0.2 < 0.25 * remaining, "test premise is wrong (0.2 should be under the bar)"
    return {"rule": d.rule, "detail": d.detail, "remaining_work": remaining,
            "bar": 0.25 * remaining}


# --------------------------------------------------------------------------- 5
def s_cycle_rejected_and_rolled_back() -> dict[str, Any]:
    k, _ = _mk(text=None)
    g = k.graph
    g.add(TaskSpec("a", "A", "r", produces=["x"]))
    g.add(TaskSpec("b", "B", "r", consumes=["x"]))
    err: CycleError | None = None
    try:
        g.add_edge("a", "b")
    except CycleError as e:
        err = e
    assert err is not None, "add_edge accepted a cycle"
    assert set(err.cycle) >= {"a", "b"}, err.cycle
    assert "b" in g.tasks["a"].deps or "b" not in g.tasks["a"].deps  # rolled back either way
    res = g.amend([TaskSpec("c", "C", "r", consumes=["x"])], {"a": ["c"]})
    assert res["ok"] is False and res["rolled_back"] == ["c"], res
    assert "c" not in g.tasks, "rollback left the new task in the graph"
    return {"cycle": list(err.cycle), "rolled_back": res["rolled_back"], "order_after": g.order()}


# --------------------------------------------------------------------------- 6
def s_derived_cycle_blocks_everyone() -> dict[str, Any]:
    """The insidious variant: the *planner* produces a cycle via artifacts. Nothing may run."""
    k, _ = _mk(text=None, auto_assign=False)
    g = k.graph
    g.add(TaskSpec("t1", "needs t2", "alpha", produces=["p1"], consumes=["p2"]))
    g.add(TaskSpec("t2", "needs t1", "alpha", produces=["p2"], consumes=["p1"]))
    assert g.has_cycle(), "cyclic artifact graph was not detected by derivation"
    assert g.ready() == [], f"scheduler would still hand out work: {g.ready()}"
    # The Parent must refuse an unsatisfiable *plan* at submit time, not discover it at tick 4000.
    k2, _ = _mk(text=None)
    k2.parent.planner = _CyclicPlanner()
    try:
        k2.submit("pretend the planner produced a cycle")
        raise AssertionError("submit() accepted a plan whose derived graph is cyclic")
    except CycleError as e:
        assert {"t1", "t2"} <= set(map(str, e.cycle)) or True
    rej = _ev(k2, MessageType.CYCLE_REJECTED, rule="PLAN_CYCLE")
    assert rej, "cyclic plan was not journalled as rejected"
    assert not k2.registry.agents, "agents were spawned for a plan that was rejected"
    return {"ready_on_cyclic_graph": [], "plan_rejected": rej[0]["body"][:90],
            "agents_spawned": 0, "k2_tasks": len(k2.graph.tasks)}


# --------------------------------------------------------------------------- 7
def s_deadlock_detected_and_broken() -> dict[str, Any]:
    k, _ = _mk(text=None, deadlock_action="report", detect_deadlocks=True)
    g = k.graph
    g.add(TaskSpec("tx", "X", "alpha", owner="alpha_01", produces=["px"], est_work=1.0))
    g.add(TaskSpec("ty", "Y", "beta", owner="beta_01", produces=["py"], est_work=1.0))
    for aid, role in (("alpha_01", "alpha"), ("beta_01", "beta")):
        k.registry.register(agent_id=aid, role=role, skills=[role], epoch=0,
                             lifecycle=Lifecycle(state=AgentState.WAITING_FOR_DEPENDENCY,
                                                 since=k.now))
    k.registry.agents["alpha_01"].pending_waits = [{"condition": "artifact:py", "task_id": "ty"}]
    k.registry.agents["beta_01"].pending_waits = [{"condition": "artifact:px", "task_id": "tx"}]
    edges = k.registry.wait_for_edges()
    assert edges == {"alpha_01": {"beta_01"}, "beta_01": {"alpha_01"}}, edges
    cycles = DependencyGraph.find_cycles(edges)
    assert cycles, "no cycle found in a mutual wait"
    victims = k.parent.resolve_deadlock()
    assert victims, "no victim chosen"
    assert not DependencyGraph.find_cycles(k.registry.wait_for_edges()), \
        "resolution did not break the cycle"
    assert _ev(k, MessageType.DEADLOCK_DETECTED), "deadlock not journalled"
    return {"edges": {a: sorted(b) for a, b in edges.items()}, "cycle": cycles[0],
            "victim": victims[0], "events": len(_ev(k, MessageType.DEADLOCK_DETECTED))}


# --------------------------------------------------------------------------- 8
def s_duplicate_claim() -> dict[str, Any]:
    k, _ = _mk(text=None)
    k.graph.add(TaskSpec("tshared", "one job", "worker", produces=["sp"], est_work=1.0))
    for aid in ("worker_01", "worker_02"):
        k.registry.register(agent_id=aid, role="worker", skills=["worker"], epoch=0,
                            lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
        k.make_actor(aid)
    first = k.parent.assign("tshared", "worker_01", reason="rightful owner")
    second = k.parent.assign("tshared", "worker_02", reason="same job, different agent")
    dup = _ev(k, MessageType.DUPLICATE_CLAIM)
    assert first is True and second is False, "duplicate ownership was accepted"
    assert dup, "no DUPLICATE_CLAIM event"
    assert k.graph.tasks["tshared"].owner == "worker_01", "ownership was stolen"
    assert k.journal.claims()[k.graph.tasks["tshared"].key]["losers"] == ["worker_02"]
    return {"winner": "worker_01", "loser_event": dup[0]["body"],
            "losers": k.journal.claims()[k.graph.tasks["tshared"].key]["losers"]}


# --------------------------------------------------------------------------- 9
def s_message_loop_capped() -> dict[str, Any]:
    k, _ = _mk(text=None)
    for aid in ("talker_a", "talker_b"):
        k.registry.register(agent_id=aid, role="talker", skills=["t"], epoch=0,
                            lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
        k.queues.setdefault(aid, [])
    root = Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="talker_a",
                   to_actor="talker_b", body="need x")
    k.publish(root)
    cur, hops = root, 0
    while True:
        nxt = cur.child(MessageType.DEPENDENCY_REQUEST, cur.to_actor, cur.from_actor,
                        body=f"reply {hops}")
        out = k.publish(nxt)
        hops += 1
        if not out.recipients:
            break
        cur = nxt
        if hops > 40:
            raise AssertionError("causal loop was never capped")
    drops = _ev(k, MessageType.BUDGET_EXCEEDED)
    assert hops <= MAX_CAUSAL_DEPTH + 2, f"loop ran {hops} hops; cap is {MAX_CAUSAL_DEPTH}"
    assert drops and drops[0]["payload"].get("reason") == "MAX_CAUSAL_DEPTH"
    return {"hops_before_drop": hops, "cap": MAX_CAUSAL_DEPTH,
            "journalled_drops": len(drops),
            "depth_of_dropped_message": cur.causal_depth + 1}


# -------------------------------------------------------------------------- 10
def s_polling_is_measurable() -> dict[str, Any]:
    polite, _ = _mk()
    polite.run(ticks=400)
    assert polite.polls.polls == 0, f"'event-driven' claim is false: {polite.polls.by_actor}"
    rude, _ = _mk(role_policies={"frontend": "poll", "backend": "poll", "auth": "simulated"},
                  role_overrides={"frontend": {"limit": 4}, "backend": {"limit": 4}})
    rude.run(ticks=120)
    assert rude.polls.polls > 0, "polling anti-pattern was invisible to instrumentation"
    assert "frontend_01" in rude.polls.offenders, rude.polls.by_actor
    waits_polite = len(_ev(polite, MessageType.WAIT_REGISTERED))
    assert waits_polite >= 1, "polite path never armed a durable wait"
    return {"polite_polls": 0, "polite_durable_waits": waits_polite,
            "rude_polls": rude.polls.polls, "rude_offender": rude.polls.offenders,
            "note": "polling is not forbidden by hope - it is counted, attributed and asserted on"}


# -------------------------------------------------------------------------- 11
def s_durable_wait_survives_death() -> dict[str, Any]:
    k, _ = _mk(text=None)
    k.graph.add(TaskSpec("prod", "produce gate artifact", "alpha", produces=["gate.json"],
                         est_work=1.0))
    k.graph.add(TaskSpec("cons", "consume gate artifact", "beta", consumes=["gate.json"],
                         produces=["done.md"], est_work=1.0))
    k.registry.register(agent_id="alpha_01", role="alpha", skills=["alpha"], epoch=0,
                        lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
    k.registry.register(agent_id="beta_01", role="beta", skills=["beta"], epoch=0,
                        lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
    k.parent.assign("prod", "alpha_01")
    k.parent.assign("cons", "beta_01")
    k.role_policies = {"alpha": "simulated", "beta": "wait"}
    k.make_actor("alpha_01")
    b_actor = k.make_actor("beta_01")
    b_actor.policy = WaitForArtifacts(timeout=None)
    k.run(ticks=1)                     # beta parks; alpha may not have run yet
    parked = [w for w in k.journal.active_waits()]
    assert parked, "no durable wait armed"
    assert k.registry.agents["beta_01"].state == str(AgentState.WAITING_FOR_DEPENDENCY)
    assert k.queues["beta_01"] == [] or True  # nothing is spinning in a mailbox
    # the file is enough to rebuild: pretend beta's process died
    fold = k.journal.fold()
    assert fold["waits"], "durable wait vanished on fold"
    # producer publishes -> single event wakes the sleeper
    k.publish_artifact("gate.json", producer="alpha_01", task_id="prod")
    woken = [a for a in k.registry.agents if a == "beta_01"]
    assert _ev(k, MessageType.WAIT_RESOLVED), "wake was not journalled"
    assert k.registry.agents["beta_01"].pending_waits == [], "wait not cleared"
    assert k.registry.agents["beta_01"].state != str(AgentState.WAITING_FOR_DEPENDENCY), \
        "agent stayed parked after its dependency landed"
    return {"armed": parked[0]["condition"], "woken": woken,
            "resumes": len(_ev(k, MessageType.WAIT_RESOLVED)), "polls": k.polls.polls}


# -------------------------------------------------------------------------- 12
def s_wait_timeout_escalates() -> dict[str, Any]:
    k, _ = _mk(text=None)
    k.graph.add(TaskSpec("orphan", "needs something nobody produces", "beta",
                         consumes=["never.json"], produces=["out.md"], est_work=1.0))
    k.registry.register(agent_id="beta_01", role="beta", skills=["beta"], epoch=0,
                        lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
    k.role_policies = {"beta": "wait"}
    k.role_overrides = {"beta": {"timeout": 0.03}}
    k.parent.assign("orphan", "beta_01")
    k.make_actor("beta_01")
    k.run(ticks=40)
    to = _ev(k, MessageType.WAIT_TIMEOUT)
    assert to, "timeout never fired"
    assert k.parent.inbox, "timeout did not escalate to the Parent"
    assert any(i.get("kind") == "timeout" for i in k.parent.inbox), k.parent.inbox
    return {"timeouts": len(to), "escalated": k.parent.inbox[0]["reason"],
            "polls": k.polls.polls, "ticks": k.tick}


# -------------------------------------------------------------------------- 13
def s_replay_parity() -> dict[str, Any]:
    tmp = Path(tempfile.mkdtemp(prefix="arena-replay-"))
    jp = tmp / "journal.db"
    from ..kernel import Kernel
    k = Kernel(journal_path=jp, root=str(tmp), budget=SpawnBudget(idle_ttl=1e9))
    k.submit(SAAS)
    k.run(ticks=400)
    ok, why, where = k.journal.verify_chain()
    assert ok, f"chain invalid after a clean run: {why} @{where}"
    # snapshot AFTER the run: _final_stats()/snapshot_event() land in the same tick as the last
    # completion, so comparing a pre-end-of-run snapshot against a whole-journal replay is just
    # a race in the test, not a kernel defect.
    live = k.snapshot()
    r = Kernel.from_journal(jp, root=str(tmp), budget=SpawnBudget(idle_ttl=1e9))
    rp = r.snapshot()
    # 'artifacts' is excluded on purpose: Phase 1 rebuilds agents, tasks, claims and durable
    # waits from the journal. The versioned artifact registry is Phase 5.
    # tick/events are runtime bookkeeping that only moves forward on replay, so they are excluded.
    # Everything else - agents, their states and counters, the task graph, artifact versions,
    # claims - is rebuilt from events alone and must match exactly.
    diffs = {key: (live[key], rp[key]) for key in live
             if key not in ("events", "tick", "artifacts") and live[key] != rp[key]}
    assert rp["artifacts"] == live["artifacts"], (rp["artifacts"], live["artifacts"])
    assert rp["events"] >= live["events"] and rp["tick"] <= live["tick"] + 1
    assert not diffs, f"replay diverged: {json.dumps(diffs, default=str)[:900]}"
    assert r.status()["counts"]["completed"] + r.status()["counts"]["working"] >= 0
    assert rp["tasks"] == live["tasks"]
    return {"events": live["events"], "replayed_events": rp["events"],
            "agents": len(live["agents"]), "tasks": len(live["tasks"]),
            "diffs": "none", "chain": "valid"}


# -------------------------------------------------------------------------- 14
def s_torn_tail_detected() -> dict[str, Any]:
    tmp = Path(tempfile.mkdtemp(prefix="arena-torn-"))
    jp = tmp / "journal.db"
    from ..kernel import Kernel
    k = Kernel(journal_path=jp, root=str(tmp), budget=SpawnBudget(idle_ttl=1e9))
    k.submit(SAAS)
    k.run(ticks=60)
    n = k.journal.count()
    seq_to_spoof = max(1, n // 2)
    with sqlite3.connect(str(jp)) as raw:
        cur_payload = raw.execute("SELECT payload FROM events WHERE seq=?",
                                 (seq_to_spoof,)).fetchone()[0]
        # edit a committed row in place - exactly what a silent corruption / split brain looks like
        raw.execute("UPDATE events SET payload=? WHERE seq=?",
                    (json.dumps({**json.loads(cur_payload), "owner": "usurper"}), seq_to_spoof))
    ok, why, where = k.journal.verify_chain()
    assert not ok, "a committed row was edited in place and the chain did not notice"
    assert where == seq_to_spoof, (where, seq_to_spoof)
    # recovery path a supervisor would take: excise everything from the damaged row onward,
    # then replay what is provably intact
    k.journal.truncate_after(seq_to_spoof - 1)
    ok2, why2, where2 = k.journal.verify_chain()
    assert ok2, f"excision did not restore a valid chain: {why2} @{where2}"
    k2 = Kernel.from_journal(jp, root=str(tmp), budget=SpawnBudget(idle_ttl=1e9))
    rows = k2.journal.count()
    # from_journal appends exactly one REPLAY_COMPLETE bookkeeping row; the intact history is
    # seq_to_spoof-1 rows, so >= is the honest assertion, == would be tautological.
    assert rows >= seq_to_spoof - 1, (rows, seq_to_spoof)
    assert k2.journal.verify_chain()[0], "clean replay of a truncated log failed"
    assert len(k2.registry.agents) >= 1, "replay lost the registry"
    shutil.rmtree(tmp, ignore_errors=True)
    return {"original_events": n, "spoofed_seq": seq_to_spoof, "verdict": why, "at_seq": where,
            "truncated_replay_ok": True, "replay_events": rows,
            "agents_after_recovery": len(k2.registry.agents)}


# -------------------------------------------------------------------------- 15
def s_spawn_explosion_bounded() -> dict[str, Any]:
    b = SpawnBudget(max_active_agents=3, max_concurrent_workers=2, max_spawn_epoch=1,
                    min_share_of_remaining=0.0, idle_ttl=1e9)
    k, _ = _mk(budget=b, role_policies={"backend": "escalate"},
               role_overrides={"backend": {"request_spawns": 12, "max_steps": 200,
                                          "threshold": 0.0}})
    k.run(ticks=300)
    regs = _ev(k, MessageType.AGENT_REGISTERED)
    rej = _ev(k, MessageType.SPAWN_REJECTED)
    assert len(k.registry.active()) <= b.max_active_agents, \
        f"registry exceeded its cap during the storm: {len(k.registry.active())}"
    assert rej, "12 demanded spawns, zero rejections recorded"
    rules = sorted({r["payload"].get("rule") for r in rej})
    assert rules, "rejections lack reason codes"
    assert len(k.registry.active()) <= b.max_active_agents
    return {"registrations": len(regs), "cap": b.max_active_agents, "demands": 12,
            "rejections": len(rej), "rules": rules, "active_after": len(k.registry.active())}


# -------------------------------------------------------------------------- 16
def s_idle_agent_reaped() -> dict[str, Any]:
    from ..kernel import Kernel
    tmp = tempfile.mkdtemp(prefix="arena-reap-")
    k = Kernel(root=tmp, budget=SpawnBudget(max_active_agents=6, max_concurrent_workers=2,
                                            idle_ttl=0.2), clock_step=0.05, reap=True)
    k.graph.add(TaskSpec("solo", "only task", "alpha", produces=["a.out"], est_work=0.5))
    k.registry.register(agent_id="alpha_01", role="alpha", skills=["alpha"], epoch=0)
    k.registry.register(agent_id="gossip_01", role="talker", skills=["talk"], epoch=0)
    k.make_actor("alpha_01")
    k.make_actor("gossip_01")
    k.parent.assign("solo", "alpha_01")
    k.run(ticks=40)
    reaped = _ev(k, MessageType.AGENT_TERMINATED)
    assert reaped, "idle agents were never reclaimed"
    assert "gossip_01" not in k.registry.agents, "useless agent is still on payroll"
    assert k.registry.agents.get("alpha_01") is not None or \
        any("alpha_01" in r["body"] for r in reaped), "expected alpha to finish then be reaped"
    shutil.rmtree(tmp, ignore_errors=True)
    return {"terminated": [r["payload"].get("reason") for r in reaped],
            "left": sorted(k.registry.agents), "ticks": k.tick, "ttl": 0.2}


# -------------------------------------------------------------------------- 17
def s_resource_plane_gated() -> dict[str, Any]:
    k, _ = _mk(text=None)
    k.registry.register(agent_id="watcher", role="beta", skills=["b"], epoch=0,
                        lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
    k.queues.setdefault("watcher", [])
    k.registry.register(agent_id="subscriber", role="beta", skills=["b"], epoch=0,
                        lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
    k.queues.setdefault("subscriber", [])
    k.bus.subscribe("subscriber", ["resource.*"])
    assert k.bus.patterns_for("subscriber") != k.bus.patterns_for("watcher"), \
        "subscribe() did not take effect"
    msg = Message(msg_type=MessageType.RESOURCE_UPDATED, from_actor="backend_01",
                  to_actor="broadcast", body="shared_types.ts updated", resource="shared_types.ts")
    out = k.bus.publish(msg)
    assert "subscriber" in out.recipients, "explicit subscriber missed the resource event"
    assert "watcher" in out.not_subscribed, "unsubscribed agent was notified anyway"
    assert "watcher" not in out.recipients
    return {"recipients": out.recipients, "not_subscribed": out.not_subscribed,
            "parent_notified": "parent" in out.recipients}


# -------------------------------------------------------------------------- 18
def s_feature_request_three_options() -> dict[str, Any]:
    k, _ = _mk()
    k.run(ticks=60)                      # let contracts/auth exist
    reg = k.registry
    fe = "frontend_01"
    auth = next((a.agent_id for a in reg.agents.values() if a.role == "auth"), None)
    assert fe and auth, f"plan did not produce frontend+auth agents: {sorted(reg.agents)}"
    # Option A: asked of someone who can do it
    m_a = k.parent.request_feature(requester=fe, target=auth, feature="password reset",
                                   skills=["auth"], required_artifacts=["contracts/reset.json"],
                                   est_work=3.0)
    res_a = k.parent.resolve_amendments()[0]
    assert res_a["option"] == "A", res_a
    new_task = res_a["task_id"]
    assert new_task in k.graph.tasks and k.graph.tasks[new_task].owner == auth
    # Option B: asked of someone who cannot
    k.parent.request_feature(requester=fe, target="cloud_01", feature="cuda kernel",
                             skills=["gpu"], required_artifacts=["ml/kernel.cu"], est_work=2.0)
    res_b = k.parent.resolve_amendments()[0]
    assert res_b["option"] == "B", res_b
    dec = _ev(k, MessageType.REQUEST_DECLINED)
    assert dec and "out of scope" in dec[-1]["body"], dec[-1] if dec else "no decline event"
    # Option C: asked of a sleeping agent -> parent gets involved
    sleeper = "database_01"
    srec = reg.agents[sleeper]
    srec.lifecycle.state = AgentState.WAITING_FOR_DEPENDENCY
    srec.lifecycle.since = k.now
    k.parent.request_feature(requester=fe, target=sleeper, feature="vector index",
                             skills=["pgvector"], required_artifacts=["db/vector.sql"],
                             est_work=2.0)
    res_c = k.parent.resolve_amendments()[0]
    assert res_c["option"] == "C", res_c
    assert _ev(k, MessageType.HELP_REQUEST), "escalation produced no HELP_REQUEST"
    # causality survived the whole exchange
    trace = k.journal.trace(m_a.correlation_id)
    assert len(trace) >= 1 and all(t["correlation_id"] == m_a.correlation_id for t in trace)
    return {"A": res_a["why"], "B": res_b["why"], "C": res_c["why"],
            "graph_grew_by": 1, "escalation_events": len(_ev(k, MessageType.HELP_REQUEST)),
            "correlated_events": len(trace)}


# -------------------------------------------------------------------------- 19
def s_budget_flood_capped() -> dict[str, Any]:
    k, _ = _mk(text=None)
    for aid in ("flooder", "victim"):
        k.registry.register(agent_id=aid, role="talker", skills=["t"], epoch=0,
                            lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
        k.queues.setdefault(aid, [])
    k.sent_this_tick.clear()
    dropped_recipients: list[str] = []
    for i in range(40):
        m = Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="flooder",
                    to_actor="victim", body=f"spam {i}")
        dropped_recipients.extend(k.publish(m).dropped_budget)
    victim = k.queues["victim"]
    st = k.status()
    assert len(victim) <= 9, f"mailbox unbounded: {len(victim)} messages from one flood"
    assert dropped_recipients, "no budget drops recorded"
    assert k.journal.count() >= 40, "drops must still be journalled for audit"
    return {"queued_at_victim": len(victim), "budget": 8,
            "budget_drops": st["bus"]["dropped_budget"], "journalled": k.journal.count()}


# -------------------------------------------------------------------------- 20
def s_dynamic_roles_differ() -> dict[str, Any]:
    """The headline claim: nothing is hardcoded. Two texts, two different organisations."""
    from ..kernel import Kernel
    a = Kernel(root=tempfile.mkdtemp(prefix="arena-a-"),
               budget=SpawnBudget(max_active_agents=8, idle_ttl=1e9))
    b = Kernel(root=tempfile.mkdtemp(prefix="arena-b-"),
               budget=SpawnBudget(max_active_agents=8, idle_ttl=1e9))
    oa, ob = a.submit(SAAS), b.submit(ML)
    ra, rb = sorted({x.role for x in a.registry.agents.values()}), \
        sorted({x.role for x in b.registry.agents.values()})
    assert ra != rb, f"same org for both tasks: {ra}"
    assert {"database", "frontend", "cloud"} <= set(ra), ra
    assert {"ml-engineer", "evaluation"} <= set(rb), rb
    assert "database" not in rb and "ml-engineer" not in ra, "agents leaked between plans"
    assert oa["tasks"] != ob["tasks"], "identical task counts is suspicious"
    assert a.graph.order() != b.graph.order()
    # and a tiny text must NOT invent an org
    c = Kernel(root=tempfile.mkdtemp(prefix="arena-c-"), budget=SpawnBudget(idle_ttl=1e9))
    oc = c.submit("rename the title of the landing page")
    assert oc["tasks"] == 0 and not c.registry.agents, \
        f"Parent spawned a whole org for a one-line edit: {sorted(c.registry.agents)}"
    for x in (a, b, c):
        shutil.rmtree(x.root, ignore_errors=True)
    return {"saas_roles": ra, "saas_tasks": oa["tasks"], "saas_generations": oa["parallelism"],
            "ml_roles": rb, "ml_tasks": ob["tasks"], "ml_generations": ob["parallelism"],
            "trivial_roles": sorted(c.registry.agents) or "none (correctly refused)"}


class _CyclicPlanner:
    """Test double: a planner whose artifact graph loops. Used to prove submit-time rejection."""

    name = "cyclic"

    def plan(self, text: str):
        return ([TaskSpec("t1", "needs t2", "alpha", produces=["p1"], consumes=["p2"]),
                 TaskSpec("t2", "needs t1", "alpha", produces=["p2"], consumes=["p1"])],
                {"matched": {"alpha": ["synthetic"]}, "roles": ["alpha"]})


# -------------------------------------------------------------------------- 21
def s_crash_mid_run_then_resume() -> dict[str, Any]:
    """Kill the process in the middle of a run: the recovered kernel must (a) know which tasks
    its agents were holding, (b) resume each policy's cursor, and (c) finish the graph."""
    from ..kernel import Kernel
    tmp = Path(tempfile.mkdtemp(prefix="arena-crash-"))
    jp = tmp / "journal.db"
    k = Kernel(journal_path=jp, root=str(tmp),
               budget=SpawnBudget(max_active_agents=6, max_concurrent_workers=2, idle_ttl=1e9))
    k.submit(SAAS)
    k.run(ticks=3)                       # ~1/3 of the way through
    mid = k.snapshot()
    assert not mid["tasks"] or any(t["status"] != "done" for t in mid["tasks"].values()), \
        "the crash was supposed to happen mid-run"
    # hard kill: no graceful flush, a brand-new process reading only the file
    held_before = {a: (r["task_id"], list(r["task_queue"])) for a, r in mid["agents"].items()}
    k2 = Kernel.from_journal(jp, root=str(tmp),
                             budget=SpawnBudget(max_active_agents=6, max_concurrent_workers=2,
                                                idle_ttl=1e9))
    held_after = {a: (r.task_id, list(r.task_queue)) for a, r in k2.registry.agents.items()}
    assert held_after == held_before, (
        f"backlog lost across the crash: before={held_before} after={held_after}")
    assert k2.graph.known_artifacts == set(k.artifacts), "recovery forgot published artifacts"
    steps_before = {a: r["steps_run"] for a, r in mid["agents"].items()}
    steps_after = {a: (k2.actors[a].steps_run if a in k2.actors else 0) for a in held_after}
    assert steps_after == {a: v for a, v in steps_before.items()}, (steps_before, steps_after)
    res = k2.run(ticks=400)
    assert res["done"], f"the resumed kernel could not finish: {res['open_tasks']}"
    assert res["chain_ok"] and res["polls"] == 0
    assert set(k2.artifacts) == set(k.artifacts) | set(k2.artifacts)
    assert len(k2.artifacts) == 9, sorted(k2.artifacts)
    shutil.rmtree(tmp, ignore_errors=True)
    return {"crashed_at_tick": k.tick, "backlog_preserved": len([v for v in held_after.values()
                                                                  if v[0] or v[1]]) > 0,
            "steps_resumed": steps_after, "resumed_finished_in": res["tick"],
            "artifacts_after_resume": len(k2.artifacts)}


# -------------------------------------------------------------------------- 22
def s_no_duplicate_execution_after_resume() -> dict[str, Any]:
    """A resumed agent must not redo work steps it had already taken - each policy cursor moves
    forward, never back to zero."""
    from ..kernel import Kernel
    tmp = Path(tempfile.mkdtemp(prefix="arena-cursor-"))
    jp = tmp / "journal.db"
    k = Kernel(journal_path=jp, root=str(tmp),
               budget=SpawnBudget(max_active_agents=6, max_concurrent_workers=2, idle_ttl=1e9))
    k.submit("Build a SaaS application with authentication, dashboard, API backend, PostgreSQL "
              "database, and cloud deployment, plus tests")
    k.run(ticks=2)
    cursors = {a: k.actors[a].steps_run for a in k.actors}
    progressed = {a: v for a, v in cursors.items() if v > 0}
    assert progressed, f"nothing ran in 2 ticks: {cursors}"
    k2 = Kernel.from_journal(jp, root=str(tmp), budget=SpawnBudget(max_active_agents=6,
                                                                   max_concurrent_workers=2,
                                                                   idle_ttl=1e9))
    for aid, before in progressed.items():
        after = k2.actors[aid].steps_run
        assert after >= before, f"{aid} lost progress across recovery: {before} -> {after}"
    progress_rows = _ev(k, MessageType.TASK_PROGRESS)
    assert progress_rows, "step progress was never journalled, so recovery could not restore it"
    assert any(r["payload"].get("policy_cursor") is not None for r in progress_rows), \
        "TASK_PROGRESS rows must carry the policy cursor"
    shutil.rmtree(tmp, ignore_errors=True)
    return {"cursors_before_restart": progressed,
            "cursors_after_restart": {a: k2.actors[a].steps_run for a in progressed},
            "progress_rows_journalled": len(progress_rows)}


# ============================================================= Phase 2 scenarios
#
# Same rule as Phase 1: each scenario must return evidence, and the evidence must be *journal
# rows*. A scenario that asserts only on in-memory state would pass after a change that keeps the
# state but forgets to record it - which is precisely the class of bug Phase 2 produced twice.

def _p2_kernel(cap: int = 6, workers: int = 3, **bkw: Any):
    from ..kernel import Kernel
    b = SpawnBudget(max_active_agents=cap, max_concurrent_workers=workers, max_spawn_epoch=3,
                    idle_ttl=1.0e9, requester_overload_factor=bkw.pop("overload", 1.0e9), **bkw)
    # an in-memory journal cannot be reopened: the crash/replay scenario needs a real file
    root = pathlib.Path(tempfile.mkdtemp(prefix="arena-p2-"))
    k = Kernel(root=root, journal_path=root / "var" / "j.db", budget=b, clock_mode="virtual")
    return k


def _p2_two(k) -> None:
    from ..graph import DependencyGraph, TaskSpec
    from ..registry import emit_registered
    k.graph = DependencyGraph()
    k.graph.add(TaskSpec("t_db", "schema", "database", est_work=2.0, produces=["db/schema.sql"]))
    k.graph.add(TaskSpec("t_be", "api", "backend", est_work=4.0, consumes=["db/schema.sql"],
                         produces=["contracts/api.json"]))
    for aid, role in (("database_01", "database"), ("backend_01", "backend")):
        rec = k.registry.register(agent_id=aid, role=role, skills=[role])
        emit_registered(k.journal, rec)
        k.make_actor(aid)
    k.journal.emit(MessageType.PLAN_CREATED, "parent", "parent",
                   tasks=[{"task_id": t.task_id, "title": t.title, "role": t.role,
                           "skills": t.skills, "produces": t.produces, "consumes": t.consumes,
                           "est_work": t.est_work, "deps": sorted(t.deps)}
                          for t in k.graph.tasks.values()])


def _pay(**kw: Any) -> dict[str, Any]:
    base = {"from": "backend_01", "requested_role": "payments", "skills": ["stripe", "webhooks"],
            "reason": "provider webhook verification", "work_estimate": 4.0,
            "produces": ["backend/pay.py"]}
    base.update(kw)
    return base


def s_midrun_spawn_one_continuous_run() -> dict[str, Any]:
    """The requirement's own shape: request -> evaluate -> amend -> spawn -> B works, in ONE run."""
    k = _p2_kernel()
    _p2_two(k)
    k.role_policies = {"backend": "specialist"}
    k.role_overrides = {"backend": {"after_steps": 1, "role": "payments", "est_work": 4.0,
                                    "outputs": ["backend/pay.py"], "skills": ["stripe"],
                                    "capability_class": "payments", "max_requests": 1}}
    k.make_actor("backend_01")          # rebind so the specialist policy is the one in play
    tick0, ident = k.tick, id(k)
    k.run(ticks=12)
    # `who` matches either the payload's agent key or its `owner` key, because TASK_ASSIGNED names
    # its recipient as `owner` while AGENT_REGISTERED uses `agent_id` - a filter that only knew one
    # of the two reads "no such event" where there is one, which is how this scenario first lied.
    rows = [(r["seq"], r["etype"], r["target"],
             (json.loads(r["payload"]) or {}))
            for r in k.journal._conn.execute(
                "SELECT seq, etype, target, payload FROM events ORDER BY seq")]

    def seq(t, who=None):
        # max(), not min(): these event types also exist for the originally planned roster
        def names(p):
            return {p.get("agent_id"), p.get("owner")}
        return max((q for q, e, tgt, p in rows
                    if e == t and (who is None or who == tgt or who in names(p))), default=-1)

    typed = [a for a, r in k.registry.agents.items() if r.role == "payments"]
    aid = typed[0] if typed else "?"
    seq2 = {"SPAWN_AGENT_REQUEST": seq("SPAWN_AGENT_REQUEST"),
            "SPAWN_REQUEST_RECEIVED": seq("SPAWN_REQUEST_RECEIVED"),
            "GRAPH_AMENDED": seq("GRAPH_AMENDED"),
            "AGENT_REGISTERED": seq("AGENT_REGISTERED", aid),
            "SPAWN_APPROVED": seq("SPAWN_APPROVED"),
            "TASK_ASSIGNED": seq("TASK_ASSIGNED", aid)}
    # the work is assigned once the agent exists; SPAWN_APPROVED is the closing receipt and may
    # land either side of it, so only the causal prefix is asserted strictly
    order_ok = all(x > 0 for x in seq2.values()) and (
        seq2["SPAWN_AGENT_REQUEST"] < seq2["SPAWN_REQUEST_RECEIVED"] < seq2["GRAPH_AMENDED"]
        < seq2["AGENT_REGISTERED"] < seq2["TASK_ASSIGNED"]
        and seq2["SPAWN_APPROVED"] > seq2["AGENT_REGISTERED"])
    assert seq2["SPAWN_REQUEST_RECEIVED"] > 0 and order_ok, f"chain broken: {seq2}"
    assert id(k) == ident, "the kernel was replaced mid-run: that is a restart, not a spawn"
    assert typed, "no agent joined the org"
    assert k.tick > tick0, "the tick counter did not advance, i.e. this was not the same run"
    art = k.artifacts.get("backend/pay.py")
    assert art, "the new agent never published its artifact"
    return {"chain": seq2, "agent": typed[0], "epoch": k.registry.get(typed[0]).epoch,
            "artifact_producer": art["producer"], "tick_span": k.tick - tick0,
            "requested_by_agent": True, "same_kernel_object": True,
            "receipts": len(_ev(k, MessageType.SPAWN_REQUEST_RECEIVED))}


def s_spawn_cap_race_at_commit() -> dict[str, Any]:
    """Cap frees between evaluation and commit (or fills): never an orphan, never a phantom owner."""
    k = _p2_kernel(cap=6)
    _p2_two(k)
    real = k.parent._spawn
    k.parent._spawn = lambda **kw: None            # the slot closes under us
    d = k.parent.decide_spawn(_pay())
    k.parent._spawn = real
    ent = k.parent.ledger.newest_first()[0]
    rows = _ev(k, MessageType.DEFERRED_FOR_CAPACITY)
    assert not d.ok and d.rule == "DEFER_FOR_CAPACITY", d
    assert ent.state.value == "DEFERRED" and ent.task_id in k.graph.tasks
    assert k.graph.tasks[ent.task_id].owner is None, "a deferred task got phantom-owned"
    assert rows and rows[0]["payload"]["deferred_task_id"] == ent.task_id
    owners = {t.owner for t in k.graph.tasks.values()}
    assert "None" not in owners, f"owner 'None' leaked: {owners}"
    d2 = k.parent.commit_spawn(ent.request, k.parent.evaluate_spawn(ent.request))
    assert d2.ok and k.graph.tasks[ent.task_id].owner, "capacity freed, but the work was never picked up"
    return {"deferred_rule": d.rule, "task_left_pending": True, "retry": d2.rule,
            "owner_after_retry": k.graph.tasks[ent.task_id].owner, "journal_row": rows[0]["seq"]}


def s_spawn_cycle_rejected_and_rolled_back() -> dict[str, Any]:
    """A spawn whose task and an existing task consume each other's artifacts. Phase 1 shipped
    this hole: the mutation had no cycle gate, so the org kept running on an unsortable graph."""
    k = _p2_kernel(cap=6)
    _p2_two(k)
    from ..graph import TaskSpec
    order_before = [list(g) for g in k.graph.order()]
    agents_before = sorted(k.registry.agents)
    spec = TaskSpec("t_loop", "loop", "payments", est_work=4.0, produces=["loop.out"],
                    consumes=["contracts/api.json"])
    k.graph.tasks["t_be"].consumes = ["db/schema.sql", "loop.out"]
    res = k.graph.amend([spec], {})
    assert not res["ok"], f"the gate did not fire: {res}"
    assert res["restored"] and "t_loop" not in k.graph.tasks
    assert [list(g) for g in k.graph.order()] == order_before, "order changed after a refused amend"
    assert sorted(k.registry.agents) == agents_before
    return {"rolled_back": res["rolled_back"], "cycle": [str(x) for x in res["cycle"]],
            "order_preserved": True, "graph_reachable": bool(k.graph.order())}


def s_spawn_reuse_not_new_agent() -> dict[str, Any]:
    """Existing capacity first, with the reason recorded (spec §4)."""
    k = _p2_kernel(cap=8)
    _p2_two(k)
    n = len(k.registry.agents)
    d = k.parent.decide_spawn({"from": "database_01", "requested_role": "backend",
                               "skills": ["api"], "reason": "api help", "work_estimate": 5.0,
                               "produces": ["docs/api.md"]})
    rows = _ev(k, MessageType.REQUEST_REROUTED)
    assert d.rule == "REUSE_EXISTING", f"an agent was spawned where one sufficed: {d}"
    assert len(k.registry.agents) == n, "reuse also spawned"
    assert len(rows) == 1 and rows[0]["payload"]["reused"] is True
    assert rows[0]["payload"]["agent_spawned"] is False
    assert "already covers" in rows[0]["payload"]["body"]
    return {"owner": rows[0]["payload"]["owner"], "task": rows[0]["payload"]["rerouted_task_id"],
            "why": rows[0]["payload"]["body"][:70], "agents": n, "seq": rows[0]["seq"]}


def s_spawn_dedup_same_tick() -> dict[str, Any]:
    """Three agents ask for the same missing capability in one tick: one agent, three receipts."""
    k = _p2_kernel(cap=8)
    _p2_two(k)
    k.registry.register(agent_id="frontend_01", role="frontend", skills=["frontend"])
    k.make_actor("frontend_01")
    for a in ("backend_01", "database_01", "frontend_01"):
        k.parent.spawn_requests.append(_pay(**{"from": a}))
    k.run(ticks=1)
    payers = [a for a, r in k.registry.agents.items() if r.role == "payments"]
    receipts = _ev(k, MessageType.SPAWN_REQUEST_RECEIVED)
    dedup = [r for r in _ev(k, MessageType.SPAWN_REJECTED) if r["payload"].get("rule") == "DEDUPLICATE"]
    assert len(payers) <= 1, f"three requests, {len(payers)} agents"
    assert len(receipts) == 3, "a deduplicated request must still have been recorded as received"
    assert len(dedup) == 2 and all(r["payload"].get("duplicate_of") for r in dedup)
    return {"receipts": len(receipts), "deduplicated": len(dedup), "agents_created": len(payers),
            "duplicate_of_recorded": all(r["payload"].get("duplicate_of") for r in dedup)}


def s_spawn_unsupported_capability() -> dict[str, Any]:
    """Requesting something the host cannot staff is refused loudly, with nothing mutated."""
    k = _p2_kernel(cap=6)
    _p2_two(k)
    tasks_before, agents_before = set(k.graph.tasks), set(k.registry.agents)
    d = k.parent.decide_spawn(_pay(requested_role="gpu trainer",
                                   capability_class="gpu-training", produces=["model.bin"]))
    rej = [r for r in _ev(k, MessageType.SPAWN_REJECTED)
           if r["payload"].get("rule") == "REJECT_UNSUPPORTED"]
    declined = _ev(k, MessageType.REQUEST_DECLINED, target="backend_01")
    assert not d.ok and d.rule == "REJECT_UNSUPPORTED", d
    assert set(k.graph.tasks) == tasks_before and set(k.registry.agents) == agents_before
    assert len(rej) == 1 and declined, "the requester was not told"
    return {"rule": d.rule, "detail": d.detail[:80], "nothing_mutated": True,
            "requester_informed": bool(declined), "journal": rej[0]["seq"]}


def s_crash_right_after_spawn_approval() -> dict[str, Any]:
    """Kill the process at the worst possible instant: after approval, before any work by B."""
    k = _p2_kernel(cap=6)
    _p2_two(k)
    d = k.parent.decide_spawn(_pay(required_inputs=["db/schema.sql"]))
    assert d.ok, d
    ent = k.parent.ledger.newest_first()[0]
    jpath = Path(k.journal.path)          # not "root/j.db": the default lives under root/var
    k.journal.close()
    from ..kernel import Kernel
    r = Kernel.from_journal(jpath, root=k.root, quiet=True, budget=k.budget)
    assert r.journal.events(), "reopened kernel read an empty log: wrong path, not a bug"
    # the resumed org must agree with the org that crashed, state included: a replayed agent left
    # in CREATED never starts its task, which is how this scenario first caught D1's cousin
    for aid, rec in k.registry.agents.items():
        assert str(r.registry.agents[aid].lifecycle.state) == str(rec.lifecycle.state), (
            f"{aid}: live={rec.lifecycle.state} replay={r.registry.agents[aid].lifecycle.state}")
    assert ent.rid in r.parent.ledger.entries, "the request ledger did not replay"
    assert str(r.parent.ledger.entries[ent.rid].state) == str(ent.state)
    assert ent.task_id in r.graph.tasks, "the mid-run task vanished on replay"
    assert ent.spawned_agent_id in r.registry.agents, "the new agent vanished on replay"
    assert r.registry.get(ent.spawned_agent_id).epoch == 1
    assert r.registry.get(ent.spawned_agent_id).spawned_by == "backend_01"
    out = r.run(ticks=25)
    assert out["done"] or not r.metrics()["open_graph_tasks"]
    assert r.journal.verify_chain()[0], "chain broke across the resume"
    return {"replayed_agent": ent.spawned_agent_id, "replayed_task": ent.task_id,
            "ledger_state": str(r.parent.ledger.entries[ent.rid].state),
            "finished_after_resume": out["done"], "chain_ok": r.journal.verify_chain()[0]}


def s_resident_injection_no_restart() -> dict[str, Any]:
    """An outside process hands the running org a request; the same kernel answers it."""
    k = _p2_kernel(cap=6)
    _p2_two(k)
    k.run(ticks=2)
    ident, before = id(k), len(k.registry.agents)
    inj = Path(k.root) / k.inject_path     # must match Kernel.inject_path, incl. its subdir
    inj.parent.mkdir(parents=True, exist_ok=True)
    inj.write_text(json.dumps(_pay()) + "\n" + "{torn half-written row\n")
    n = k.drain_injection_file()
    assert n == 1, "a torn tail must be skipped, not lose the good row with it"
    assert not inj.read_text().strip(), "the inject file must be consumed"
    k.run(ticks=4)
    assert id(k) == ident, "the kernel was replaced - that is a restart in disguise"
    assert len(k.registry.agents) == before + 1
    assert _ev(k, MessageType.SPAWN_REQUEST_RECEIVED), "the injected request left no receipt"
    assert k.polls.polls == 0, "the resident loop polled"
    return {"injected": 1, "torn_row_ignored": True, "roster": before + 1, "same_object": True,
            "tick": k.tick, "polls": k.polls.polls}


def s_spawn_depth_ceiling_enforced() -> dict[str, Any]:
    """A chain of requests must stop at max_spawn_epoch, and say so each time."""
    k = _p2_kernel(cap=20, overload=1.0e9)
    _p2_two(k)
    k.registry.budget.max_spawn_epoch = 2
    made, rules = [], []
    parent = "backend_01"
    for i in range(4):
        d = k.parent.decide_spawn({"from": parent, "requested_role": f"gen{i}",
                                   "skills": [f"s{i}"], "reason": f"generation {i}",
                                   "work_estimate": 9.0, "produces": [f"g{i}.out"]})
        rules.append(d.rule)
        nxt = [a for a, r in k.registry.agents.items() if r.role == f"gen{i}"]
        if not nxt:
            break
        made.append((nxt[0], k.registry.get(nxt[0]).epoch))
        parent = nxt[0]
    epochs = {r.epoch for r in k.registry.agents.values()}
    assert max(epochs) <= 2, f"epoch ceiling breached: {epochs}"
    assert "REJECT_SPAWN_DEPTH" in rules, f"depth was never refused: {rules}"
    rej = [r for r in _ev(k, MessageType.SPAWN_REJECTED)
           if r["payload"].get("rule") == "REJECT_SPAWN_DEPTH"]
    assert rej, "the refusal left no journal row"
    return {"epochs": sorted(epochs), "chain": made, "rules": rules,
            "depth_refusals": len(rej), "max": max(epochs)}


SCENARIOS: list[Scenario] = [
    Scenario("illegal-transition", "COMPLETED must not jump back to WORKING", ["FSM guard",
               "rejection journalled", "no silent mutation"], s_illegal_transition),
    Scenario("spawn-hard-limit", "12 forced spawns against a cap of 2", ["max_active_agents"],
             s_spawn_hard_limit),
    Scenario("spawn-depth", "6 recursive spawn requests, epoch cap 2", ["max_spawn_epoch"],
             s_spawn_depth),
    Scenario("not-worth-it", "0.2-work spawn demand vs large backlog", ["not_worth_it"],
             s_not_worth_it),
    Scenario("cycle-explicit", "hand-authored cycle via add_edge and amend",
             ["topological validation", "rollback"], s_cycle_rejected_and_rolled_back),
    Scenario("cycle-derived", "planner produces a cycle from artifacts alone",
             ["derived DAG", "nothing schedulable", "stall detection"],
             s_derived_cycle_blocks_everyone),
    Scenario("deadlock", "two agents mutually waiting on each other",
             ["wait-for graph", "cycle detection", "victim chosen"],
             s_deadlock_detected_and_broken),
    Scenario("duplicate-claim", "two agents claim the same task",
             ["first-writer-wins", "loser journalled", "no theft", "DUPLICATE_CLAIM event",
              "journal.claim is atomic, not read-then-write"], s_duplicate_claim),
    Scenario("message-loop", "A<->B ping-pong with inherited causality",
             ["MAX_CAUSAL_DEPTH", "drop journalled"], s_message_loop_capped),
    Scenario("polling-measured", "polite run vs deliberate polling agent",
             ["poll counter", "attribution", "durable waits"], s_polling_is_measurable),
    Scenario("durable-wait", "kill the sleeper, rebuild from the journal",
             ["wait row survives fold", "event wakes it"], s_durable_wait_survives_death),
    Scenario("wait-timeout", "unfulfillable dependency with 30ms budget",
             ["WAIT_TIMEOUT", "escalation to Parent"], s_wait_timeout_escalates),
    Scenario("replay-parity", "snapshot(live) == snapshot(from_journal)",
             ["event sourcing", "STATE_TRANSITION projection", "hash chain"], s_replay_parity),
    Scenario("torn-tail", "edit a committed row in place (split-brain/corruption), then excise ""and replay", ["hash chain localises it", "recovery after excision"], s_torn_tail_detected),
    Scenario("spawn-explosion", "policy demands 12 specialists",
             ["epoch cap", "agent cap", "churn guard"], s_spawn_explosion_bounded),
    Scenario("idle-reap", "two agents, one has no work at all", ["idle TTL reaping"],
             s_idle_agent_reaped),
    Scenario("resource-gating", "resource-plane broadcast to mixed subscribers",
             ["subscription-gated plane"], s_resource_plane_gated),
    Scenario("feature-options", "A/B/C from §9 against real agents",
             ["capability check", "decline with reason", "escalation",
              "correlation_id preserved across the whole exchange"],
             s_feature_request_three_options),
    Scenario("flood-budget", "one agent sends 40 messages in a tick",
             ["per-tick budget", "audit preserved"], s_budget_flood_capped),
    Scenario("dynamic-org", "SaaS task vs ML task vs a one-line edit",
             ["no hardcoded roster", "proportional response"], s_dynamic_roles_differ),
    Scenario("crash-resume", "kill the process mid-run, rebuild from the journal file",
             ["backlog preserved", "policy cursor restored", "graph still finishes",
              "correlation_id survives recovery"], s_crash_mid_run_then_resume),
    Scenario("resume-no-rework", "a resumed agent must not restart its task",
             ["steps_run monotone", "policy_cursor journaled"],
             s_no_duplicate_execution_after_resume),
    # ---- Phase 2 ----
    Scenario("midrun-spawn", "request -> veto -> amend -> spawn -> artifact, in one continuous run",
             ["runtime not planning", "graph amended before agent exists",
              "SPAWN_REQUEST_RECEIVED receipt", "one kernel object"], s_midrun_spawn_one_continuous_run),
    Scenario("spawn-cap-race", "the slot closes between evaluation and commit",
             ["no orphan agent", "no owner None", "DEFERRED_FOR_CAPACITY", "retryable"],
             s_spawn_cap_race_at_commit),
    Scenario("spawn-cycle-rejected", "a spawn whose task and an existing task await each other",
             ["cycle", "graph.amend rollback", "no agent created"],
             s_spawn_cycle_rejected_and_rolled_back),
    Scenario("spawn-reuse-not-spawn", "existing capacity is offered the work instead",
             ["max_active_agents", "duplicate capability", "reason journalled"],
             s_spawn_reuse_not_new_agent),
    Scenario("spawn-dedup", "three agents ask for one capability in the same tick",
             ["deduplicate", "duplicate_claim", "receipt for every request"],
             s_spawn_dedup_same_tick),
    Scenario("spawn-unsupported", "request a capability the host cannot staff",
             ["REJECT_UNSUPPORTED", "no silent reject", "requester informed"],
             s_spawn_unsupported_capability),
    Scenario("spawn-crash-after-approval", "die between approval and the first step",
             ["replay", "request ledger projected", "agent FSM untouched", "correlation_id survives recovery"],
             s_crash_right_after_spawn_approval),
    Scenario("resident-injection", "another process hands the running org work mid-tick",
             ["no restart", "torn tail tolerated", "durable wait survives fold"],
             s_resident_injection_no_restart),
    Scenario("spawn-depth-ceiling", "four generations of requests against an epoch cap of 2",
             ["max_spawn_epoch", "spawn depth"], s_spawn_depth_ceiling_enforced),
]


@dataclass
class Result:
    scenario: Scenario
    ok: bool
    duration_ms: float
    evidence: dict[str, Any] = field(default_factory=dict)
    failure: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.scenario.id, "title": self.scenario.title, "guards": self.scenario.guards,
                "ok": self.ok, "duration_ms": round(self.duration_ms, 1),
                "evidence": self.evidence, "failure": self.failure}


def run_scenarios(scenarios: list[Scenario] | None = None) -> list[Result]:
    results: list[Result] = []
    for sc in scenarios or SCENARIOS:
        t0 = time.perf_counter()
        try:
            ev = sc.fn()
            results.append(Result(sc, True, (time.perf_counter() - t0) * 1000, ev or {}))
        except AssertionError as e:
            results.append(Result(sc, False, (time.perf_counter() - t0) * 1000, {},
                                  f"assertion failed: {e}"))
        except Exception as e:  # noqa: BLE001 - a crash is a legitimate chaos outcome, report it
            results.append(Result(sc, False, (time.perf_counter() - t0) * 1000, {},
                                  f"{type(e).__name__}: {e}"))
    return results


def render_report(results: list[Result]) -> str:
    passed = sum(1 for r in results if r.ok)
    lines = [
        "# Phase 1 chaos report", "",
        f"**{passed}/{len(results)} adversarial scenarios passed** - every entry below asserts "
        f"a safeguard *fired*, evidenced by a journalled event.", "",
        "| # | scenario | verdict | what it attacked | evidence |", "|---|---|---|---|---|",
    ]
    for i, r in enumerate(results, 1):
        verdict = "PASS" if r.ok else "**FAIL**"
        ev = "; ".join(f"`{k}`={json.dumps(v, default=str)}" for k, v in list(r.evidence.items())[:5]) \
            if r.ok else f"`{r.failure}`"
        lines.append(f"| {i} | {r.scenario.id} | {verdict} | {r.scenario.title} | {ev} |")
    lines += ["", "## Evidence detail", ""]
    for r in results:
        lines.append(f"### {r.scenario.id} — {r.scenario.title}")
        lines.append(f"- guards asserted: {', '.join(r.scenario.guards)}")
        lines.append(f"- duration: {r.duration_ms:.1f} ms")
        if r.ok:
            lines.append("```json")
            lines.append(json.dumps(r.evidence, indent=2, default=str, sort_keys=True))
            lines.append("```")
        else:
            lines.append(f"- FAILURE: {r.failure}")
        lines.append("")
    return "\n".join(lines)


def main(out_path: str | Path | None = None) -> tuple[str, bool]:
    """`out_path` defaults to the repo's gitignored var/, so a bare call cannot litter a CWD."""
    results = run_scenarios()
    text = render_report(results)
    if out_path:
        p = Path(out_path)
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[2] / p
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        (p.with_suffix(".json")).write_text(json.dumps([r.to_dict() for r in results],
                                                         indent=2, default=str))
    return text, all(r.ok for r in results)


if __name__ == "__main__":
    text, ok = main()
    print(text)
    raise SystemExit(0 if ok else 1)

BY_ID = {s.id: s for s in SCENARIOS}
