"""Policy golden capture: drives all six built-in policies through a REAL
Python ActorContext (kernel-backed) across a scripted scenario, recording
every decision. The Rust port (`tests/policy_parity.rs`) replicates the same
facts in its test context and must produce identical decisions.

Volatile fields (message ids, correlation ids, journal hashes) are stripped;
compared values are the act/reason/condition/timeout/extra plus the semantic
message shape (type/to/body/task/depth/payload).

Regenerate: PYTHONPATH=. python3 tests/parity/policy_capture.py
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from arena.actor import ActorContext
from arena.graph import TaskSpec
from arena.kernel import Kernel
from arena.lifecycle import AgentState, Lifecycle
from arena.message import Message, MessageType
from arena.policy import (EscalateOnComplexity, HybridPolicy, NeedsSpecialist,
                          PollUntilReady, SimulatedWork, WaitForArtifacts,
                          is_sleeping_action, make_policy)
from arena.registry import SpawnBudget

FIX = Path(__file__).parent / "fixtures" / "policy_golden.json"


def mdict(m: Message | None) -> dict | None:
    if m is None:
        return None
    return {"type": str(m.msg_type), "from": m.from_actor, "to": m.to_actor,
            "body": m.body, "task_id": m.task_id, "depth": m.causal_depth,
            "payload": {k: v for k, v in m.payload.items()
                        if k not in ("correlation_id", "caused_by")}}


def adict(a) -> dict:
    return {"act": str(a.act), "reason": a.reason, "condition": a.condition,
            "timeout": a.timeout, "msg": mdict(a.msg),
            "extra": {k: v for k, v in (a.extra or {}).items()}}


def main() -> int:
    k = Kernel(root=tempfile.mkdtemp(), budget=SpawnBudget(idle_ttl=1e9),
               detect_deadlocks=False, auto_assign=False, reap=False)
    k.graph.add(TaskSpec("t_be", "backend API", "backend", est_work=2.0,
                         produces=["out/api.md"], consumes=["in/schema.sql"]))
    k.graph.add(TaskSpec("t_small", "small job", "backend", est_work=1.0,
                         produces=["out/small.md"], consumes=[]))
    k.graph.add(TaskSpec("t_schema", "schema", "data", est_work=1.0,
                         produces=["in/schema.sql"], consumes=[]))
    k.registry.register(agent_id="be_01", role="backend", skills=["api"],
                        lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
    rec = k.registry.get("be_01")
    rec.task_id = "t_be"
    k.queues.setdefault("be_01", [])

    def ctx(step: int, msg: Message | None = None, llm: bool = False,
            task_id: str = "t_be") -> ActorContext:
        rec.task_id = task_id
        return ActorContext(kernel=k, agent_id="be_01", msg=msg,
                            step_index=step, llm_available=llm)

    out: dict = {"decisions": [], "logs": [], "sleeping": [], "make_policy": {}}

    def run(name: str, policy, step: int, msg: Message | None = None,
            llm: bool = False, task_id: str = "t_be") -> None:
        c = ctx(step, msg, llm, task_id)
        try:
            a = policy.step(c, msg)
            out["decisions"].append({"case": name, "ok": True, **adict(a)})
        except Exception as e:  # the PollingForbidden path, exactly as the kernel sees it
            out["decisions"].append(
                {"case": name, "ok": False, "error": f"{type(e).__name__}: {e}"})
        out["logs"].append({"case": name, "logs": list(c.drain_log())})

    # ---- SimulatedWork: the reference progress curve, including -33% first
    p = SimulatedWork()
    for s in range(0, 5):
        run(f"sim/step{s}", p, s)
    # no task bound
    c = ctx(4, task_id=None)
    k.registry.get("be_01").task_id = None
    a = p.step(c, None)
    out["decisions"].append({"case": "sim/no_task", "ok": True, **adict(a)})
    k.registry.get("be_01").task_id = "t_be"

    # ---- WaitForArtifacts: park when the consume is missing
    p = WaitForArtifacts()
    run("wait/missing", p, 0)
    ready = Message(msg_type=MessageType.DEPENDENCY_READY, from_actor="dependency_manager",
                    to_actor="be_01", body="in/schema.sql is ready (published)")
    k.artifacts["in/schema.sql"] = {"artifact": "in/schema.sql", "version": 1}
    run("wait/resumed", p, 1, msg=ready)
    run("wait/integrate2", p, 2)
    run("wait/complete", p, 3)

    # ---- PollUntilReady: measurable anti-pattern
    p = PollUntilReady(limit=2)
    for s in range(1, 4):
        run(f"poll/step{s}", p, s)
    k.artifacts.clear()
    run("poll/clear", p, 4)

    # ---- EscalateOnComplexity
    p = EscalateOnComplexity()
    run("esc/ask", p, 0)          # est 2.0 >= threshold 2.0 -> specialist request
    run("esc/escalate", p, 1)     # already asked -> escalate
    p2 = EscalateOnComplexity()
    run("esc/low", p2, 0, task_id="t_small")   # below threshold -> proceed
    hand = Message(msg_type=MessageType.DEPENDENCY_READY, from_actor="dependency_manager",
                   to_actor="be_01", body="unblocked")
    p3 = EscalateOnComplexity(request_spawns=2)
    run("esc/handoff", p3, 0, msg=hand, task_id="t_small")
    app = Message(msg_type=MessageType.SPAWN_APPROVED, from_actor="parent",
                  to_actor="be_01", body="ok")
    run("esc/approved_log", p3, 1, msg=app, task_id="t_small")

    # ---- NeedsSpecialist
    p = NeedsSpecialist()
    run("ns/early1", p, 1)        # step_index 1 <= after_steps 2 -> working
    run("ns/early2", p, 2)
    run("ns/ask", p, 3)           # mid-run request
    run("ns/after", p, 4)
    run("ns/complete", p, 9)      # > after_steps + 6
    nsd = NeedsSpecialist(detect_from="data")
    run("ns/detect_ask", nsd, 3)  # t_be consumes in/schema.sql produced by role data
    nsd2 = NeedsSpecialist(detect_from="backend")
    run("ns/detect_self", nsd2, 3)  # backend on roster -> no ask
    rej = Message(msg_type=MessageType.SPAWN_REJECTED, from_actor="parent",
                  to_actor="be_01", body="no")
    rej.payload["rule"] = "CAP"
    run("ns/rejected_log", NeedsSpecialist(), 1, msg=rej)

    # ---- HybridPolicy with a broken adapter (tier B seam, no real LLM)
    class Boom:  # pragma: no cover - mirrors a model that is down
        name = "boom"

        def decide(self, prompt):
            raise RuntimeError("model is down")

    ph = HybridPolicy(heuristic=EscalateOnComplexity(), llm=Boom())
    c = ctx(1, llm=True, task_id="t_small")
    ph.llm_available = True
    a = ph.step(c, None)
    out["decisions"].append({"case": "hybrid/llm_down", "ok": True, **adict(a)})
    out["logs"].append({"case": "hybrid/llm_down", "logs": list(c.drain_log())})
    # same policy, llm_available False -> heuristic only, no log
    ph2 = HybridPolicy(heuristic=EscalateOnComplexity(), llm=Boom())
    ph2.llm_available = False
    c = ctx(1, llm=False, task_id="t_small")
    a = ph2.step(c, None)
    out["decisions"].append({"case": "hybrid/no_llm", "ok": True, **adict(a)})
    out["logs"].append({"case": "hybrid/no_llm", "logs": list(c.drain_log())})

    # ---- self_msg causality through the real context
    base = Message(msg_type=MessageType.TASK_ASSIGNED, from_actor="parent",
                   to_actor="be_01", body="go", task_id="t_be")
    c = ctx(1, msg=base)
    m = c.self_msg(MessageType.TASK_PROGRESS, "50%", task_id=None, percent=50)
    out["self_msg_inherit"] = mdict(m)

    # ---- is_sleeping_action surface
    out["sleeping"] = {v: is_sleeping_action(v) for v in
                       ("WAIT", "wait", "Complete", "ESCALATE", "ERROR", "error",
                        "PROCEED", "NOOP", "", "PUBLISH")}

    # ---- make_policy
    out["make_policy"]["names"] = {n: make_policy(n).name for n in
                                   ("simulated", "wait", "poll", "escalate", "specialist")}
    for bad in ("hybrid", "nope"):
        try:
            make_policy(bad)
            out["make_policy"][bad] = "<no error>"
        except KeyError as e:
            out["make_policy"][bad] = str(e)
    p = make_policy("simulated", steps=5, junk="ignored")
    out["make_policy"]["sim_steps5_complete_at"] = p.step(ctx(6), None).act == SimulatedWork().step(ctx(6), None).act  # noqa: E501 - both COMPLETE at 6
    sp = make_policy("specialist", after_steps=0, role="ml engineer")
    c = ctx(1)
    a = sp.step(c, None)
    out["make_policy"]["specialist_override"] = adict(a)

    with FIX.open("w") as f:
        json.dump(out, f, indent=1, sort_keys=True, default=str)
    print(f"wrote {FIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
