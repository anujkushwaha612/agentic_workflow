"""Cognition golden capture: drives validate_intent, Intent fingerprints,
Observation rendering, outcomes_from_results and the PolicyCognition adapter
(over a REAL ActorContext) across a scripted scenario. The Rust port
(`tests/cognition_parity.rs`) replicates the same facts and must produce
identical output.

Volatile fields are handled by construction: messages used for fingerprint
cases carry FIXED mid/correlation_id/ts/seq; adapter-produced messages are
compared as semantic shapes only. obj_id (a memory address) is excluded.

Regenerate: PYTHONPATH=. python3 tests/parity/cognition_capture.py
"""
from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from arena.actor import ActorContext
from arena.cognition import (Control, Intent, Observation, Outcome,
                             PolicyCognition, ToolCall, outcomes_from_results,
                             validate_intent)
from arena.graph import TaskSpec
from arena.kernel import Kernel
from arena.lifecycle import AgentState, Lifecycle
from arena.message import Message, MessageType
from arena.registry import SpawnBudget

FIX = Path(__file__).parent / "fixtures" / "cognition_golden.json"


@dataclass
class CapToolResult:
    """The fields outcomes_from_results reads (a stand-in for tools.ToolResult;
    the function only uses getattr, so shape is what matters)."""
    tool: str = ""
    ok: bool = False
    exit_code: int | None = None
    data: dict = field(default_factory=dict)
    refused: str | None = None
    rid: str | None = None
    _block: str = ""

    def as_block(self) -> str:  # noqa: ANN101 - matches ToolResult surface
        return self._block


def intent_dict(i: Intent) -> dict:
    d = {"calls": [c.to_dict() for c in i.calls], "control": i.control,
         "think": i.think, "reason": i.reason, "wait_for": i.wait_for,
         "max_calls": i.max_calls}
    if i.msg if hasattr(i, "msg") else False:
        d["msg"] = True
    return d


def fixed_message(msg_type: MessageType, frm: str = "a_01", to: str = "broadcast",
                  body: str = "", mid: str = "m-fixed000001", ts: float = 0.0,
                  depth: int = 0, task_id: str | None = None) -> Message:
    m = Message(msg_type, frm, to, body)
    m.mid = mid
    m.correlation_id = "c-" + mid[2:]
    m.ts = ts
    m.causal_depth = depth
    m.task_id = task_id
    return m


def main() -> int:
    out: dict = {}

    # ------------------------------------------------------ validate_intent
    vi: list[dict] = []

    def cap_vi(tag: str, intent, allowed: list[str], max_calls: int = 4) -> None:
        got, notes = validate_intent(intent, allowed_tools=allowed, max_calls=max_calls)
        vi.append({"case": tag, "intent": intent_dict(got), "notes": notes})

    # valid intent passes through untouched
    ok_intent = Intent.of({"tool": "bash", "args": {"argv": ["ls", "-la"]}},
                          {"tool": "read", "args": {}},
                          control=Control.PUBLISH, think="checking",
                          reason="publish after check")
    cap_vi("valid", ok_intent, ["bash", "read"])
    # missing intent
    cap_vi("missing", None, ["bash"])
    # unknown control (calls+think survive, verb/publish dropped)
    # NOTE: passing raw dicts as calls would crash the reference later at
    # `c.tool` (the rebuild copies calls verbatim); Rust's typed calls make
    # that unrepresentable, so the capture uses ToolCall objects.
    cap_vi("unknown_control", Intent(calls=(ToolCall("bash", {}),),
                                     control="run", think="original thought",
                                     reason="full speed", wait_for="x"),
           ["bash"])
    # unauthorized tools, no control -> paper-trail NOOP
    cap_vi("unauthorized_all", Intent.of({"tool": "rm", "args": {}},
                                         {"tool": "curl", "args": {}}), ["bash"])
    # unauthorized + valid control -> calls filtered, verb kept
    cap_vi("unauthorized_some", Intent.of({"tool": "rm", "args": {}},
                                          {"tool": "bash", "args": {}},
                                          control=Control.WAIT), ["bash"])
    # truncation (6 allowed calls, max 4)
    cap_vi("truncated", Intent.of(*[{"tool": f"t{n}", "args": {}} for n in range(6)],
                                  control=""), ["t0", "t1", "t2", "t3", "t4", "t5"])
    # mixed: one refused, six allowed -> 6>4 note
    cap_vi("mixed_refuse_truncate",
           Intent.of(*[{"tool": f"t{n}", "args": {}} for n in range(7)], control=""),
           ["t0", "t1", "t2", "t3", "t4", "t5"])
    # malformed: missing tool -> "" -> refused (a NON-dict call would crash
    # the reference inside Intent.of; Rust's from_dict degrades it to an empty
    # ToolCall instead — documented strictness, not exercised here)
    cap_vi("malformed_empty_tool", Intent.of({"args": {"x": 1}}, control=""), ["bash"])
    # unknown control AND all calls refused -> NOT the paper-trail branch
    # (rebuilt control NOOP is truthy) -> rebuilt intent returned
    cap_vi("unknown_control_and_refused",
           Intent(calls=(ToolCall("rm", {}),), control="run"), ["bash"])
    out["validate_intent"] = vi

    # --------------------------------------------------------- fingerprints
    fp: list[dict] = []

    def cap_fp(tag: str, i: Intent) -> None:
        fp.append({"case": tag, "fingerprint": i.fingerprint(),
                   "is_noop": i.is_noop})

    cap_fp("bare", Intent())
    cap_fp("wait", Intent(control=Control.WAIT, wait_for="db/schema.sql"))
    cap_fp("wait_reason", Intent(control=Control.WAIT, wait_for="db/schema.sql",
                                 reason="parked"))
    cap_fp("calls", Intent.of({"tool": "bash", "args": {"argv": ["ls"]}},
                              {"tool": "read", "args": {}}, control="NOOP"))
    cap_fp("spawn_dict", Intent(control=Control.SPAWN_REQUEST,
                                spawn={"requested_role": "database"}))
    cap_fp("artifacts", Intent(control=Control.COMPLETE,
                               publish={"artifacts": ["out/api.md"]}))
    pub = Intent(control=Control.PUBLISH, publish={"msg": fixed_message(
        MessageType.ARTIFACT_PUBLISHED, body="out/api.md ready",
        mid="m-pub00000001", ts=1.25, depth=2)})
    cap_fp("publish_msg", pub)
    spw = Intent(control=Control.SPAWN_REQUEST, spawn={"msg": fixed_message(
        MessageType.SPAWN_AGENT_REQUEST, to="parent", body="need a dba",
        mid="m-spw00000001", ts=2.0, depth=3, task_id="t_be")})
    cap_fp("spawn_msg", spw)
    out["fingerprints"] = fp

    # --------------------------------------------------------- observation
    obs = Observation(
        agent_id="a_01", role="worker", goal="build the thing",
        task={"task_id": "t_09", "status": "OPEN",
              "produces": ["db/schema.sql"],
              "verify": ["pytest", "ruff"]},
        step_index=3, unmet=["api/openapi.json"],
        workspace={"files": ["a.py", "b.py"], "dirty": True},
        recent=[
            Outcome(tool="bash", ok=True, exit_code=0, text="ignored in to_dict",
                    data={"n": 3}, refused=""),
            Outcome(tool="write", ok=False, exit_code=None, text="x",
                    data={}, refused="path outside workspace"),
        ],
        history=[Outcome(tool=f"h{n}", ok=True) for n in range(9)],
        notes=["note one"], state="WORKING",
        tool_schemas=[{"tool": "bash"}],
        graph_view=[{"task_id": "t_1"}],
        budget={"steps_left": 3},
        transcript_len=12,
    )
    out["render"] = obs.render()
    out["prompt_seed"] = json.loads(json.dumps(obs.prompt_seed()))

    # ------------------------------------------------- outcomes_from_results
    results = [
        CapToolResult(tool="bash", ok=True, exit_code=2, _block="== bash ==\nls\n",
                      data={"n": 3, "ratio": 0.5, "name": "x", "flag": False,
                            "nested": {}, "listy": []}),
        CapToolResult(_block="hidden", data={"keep": "v"}),
        CapToolResult(tool="write", refused="outside workspace", rid="r-1"),
    ]
    out["outcomes"] = [o.to_dict() for o in outcomes_from_results(results)]

    # ------------------------------------------- PolicyCognition adapter path
    k = Kernel(root=tempfile.mkdtemp(), budget=SpawnBudget(idle_ttl=1e9),
               detect_deadlocks=False, auto_assign=False, reap=False)
    k.graph.add(TaskSpec("t_be", "backend API", "backend", est_work=2.0,
                         produces=["out/api.md"], consumes=["in/schema.sql"]))
    k.graph.add(TaskSpec("t_schema", "schema", "data", est_work=1.0,
                         produces=["in/schema.sql"], consumes=[]))
    k.registry.register(agent_id="be_01", role="backend", skills=["api"],
                        lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
    rec = k.registry.get("be_01")
    rec.task_id = "t_be"
    k.queues.setdefault("be_01", [])
    k.artifacts.clear()  # a dict: artifact -> {"version": n, ...}

    def sem_msg(m: Message | None) -> dict | None:
        if m is None:
            return None
        return {"type": str(m.msg_type), "from": m.from_actor, "to": m.to_actor,
                "body": m.body, "task_id": m.task_id, "depth": m.causal_depth,
                "payload_keys": sorted(m.payload.keys())}

    ad: list[dict] = []

    def cap_adapter(tag: str, cog: PolicyCognition, ctx: ActorContext,
                    msg: Message | None) -> None:
        obs = Observation(agent_id=ctx.agent_id, role="backend", goal="",
                          task={}, message=msg, step_index=ctx.step_index,
                          state="WORKING")
        obs.context = ctx  # type: ignore[attr-defined]  # the projection the adapter uses
        intent = cog.decide(obs)
        vi_got, notes = validate_intent(intent, allowed_tools=[], max_calls=4)
        ad.append({"case": tag, "intent": intent_dict(intent),
                   "semantic_msg": sem_msg(intent.publish.get("msg") if intent.publish else None)
                   or sem_msg(intent.spawn.get("msg") if intent.spawn else None),
                   "wait_for": intent.wait_for,
                   "validated_control": vi_got.control, "notes": notes})

    from arena.policy import (NeedsSpecialist, SimulatedWork, WaitForArtifacts)

    cog = PolicyCognition(SimulatedWork(), "be_01")
    for step in (0, 3, 4):
        cap_adapter(f"sim/step{step}", cog,
                    ActorContext(k, "be_01", None, step, False), None)

    cog2 = PolicyCognition(WaitForArtifacts(), "be_01")
    cap_adapter("wait/unmet", cog2, ActorContext(k, "be_01", None, 0, False), None)
    k.artifacts["in/schema.sql"] = {"version": 1, "producer": "t_schema"}
    cap_adapter("wait/met", cog2, ActorContext(k, "be_01", None, 1, False), None)

    cog3 = PolicyCognition(NeedsSpecialist(detect_from="data"), "be_01")
    cap_adapter("specialist/ask", cog3, ActorContext(k, "be_01", None, 2, False), None)

    out["adapter"] = ad
    out["adapter_cognition"] = {
        "name": cog3.name, "model": cog3.model_id,
        "fingerprint_sans_obj_id": {k2: v for k2, v in cog3.fingerprint().items()
                                    if k2 != "obj_id"},
    }

    FIX.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
    print(f"wrote {FIX} ({len(vi)} validate_intent cases, {len(fp)} fingerprints, "
          f"{len(ad)} adapter cases)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
