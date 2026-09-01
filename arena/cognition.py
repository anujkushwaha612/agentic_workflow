"""The cognition seam (Phase 2.5 M0/M2): proposal structures + the swap point, with no execution.

    Cognition decides.  The runtime validates and executes.  Neither can impersonate the other.

What lives here: `Observation` (what an agent is allowed to know), `Intent` (what it may propose),
`Control` (the verbs the runtime recognises), the `CognitionSource` protocol, and
`PolicyCognition` - the adapter that makes every existing Phase 1/2 policy a cognition source
without changing a line of policy code.

What deliberately does NOT live here: subprocesses, filesystems, the bus, the graph, the registry.
There is no import of `arena.tools`, `arena.kernel` or `arena.graph` in this module, and that is the
enforcement: a cognition layer that cannot name `open()` cannot write a file. `LLMCognition` and
`ArenaCognition` (see `arena/code/cognition.py`) obey the same rule; the only thing that changes
between them and `PolicyCognition` is what `decide()` does with the Observation.

Why the boundary is drawn *here* and not inside the actor: `ActorContext` is the one object that
already knows both the graph and the agent's mailbox. `PolicyCognition` hands a policy a *read-only
projection* of that context (`View`), so a policy keeps working exactly as it did - it returns an
`Action`, which was always a proposal - and the runtime converts the proposal into an `Intent`. The
conversion is one-directional: Action -> Intent is mechanical, and nothing in this module can turn an
Intent back into a kernel mutation.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence, runtime_checkable


# --------------------------------------------------------------------------- verbs
class Control:
    """The only ways an Intent can end a turn. Strings, not an enum, so a provider that returns
    {"control": "WAIT"} needs no import path into our type system to be validated."""

    NONE = ""
    WAIT = "WAIT"
    PUBLISH = "PUBLISH"
    SPAWN_REQUEST = "SPAWN_REQUEST"
    ESCALATE = "ESCALATE"
    COMPLETE = "COMPLETE"
    VERIFY = "VERIFY"
    NOOP = "NOOP"

    ALL = frozenset({WAIT, PUBLISH, SPAWN_REQUEST, ESCALATE, COMPLETE, VERIFY, NOOP})


#: verbs that mean "this agent is done acting this turn" - kept in sync with the kernel's
#: SLEEPING_ACTIONS so a cognition-driven turn and a policy-driven turn cannot disagree about when
#: the inbox drain stops. (Phase-2 defect D2 was exactly this pair drifting.)
YIELDING = frozenset({Control.WAIT, Control.ESCALATE, Control.COMPLETE})


# --------------------------------------------------------------------------- observation
@dataclass
class Outcome:
    """A tool result as cognition is allowed to see it: text, digests, exit codes. Never a file
    handle, never a path outside the workspace."""

    tool: str
    ok: bool = False
    exit_code: int | None = None
    text: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    refused: str = ""
    rid: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"tool": self.tool, "ok": self.ok, "exit_code": self.exit_code,
                "refused": self.refused, "data": self.data}


@dataclass
class Observation:
    """Everything an agent may know at the start of a turn.

    `context` is the read-only projection described in the module docstring: property access only,
    plus the two message *builders*. It is present so the policy tier keeps working; a provider
    cognition source should read the explicit fields instead, because those are what a future
    serialisable prompt is built from.
    """

    agent_id: str
    role: str = ""
    goal: str = ""
    task: dict[str, Any] = field(default_factory=dict)
    message: Any = None
    step_index: int = 0
    unmet: tuple[str, ...] = ()
    consumed_ready: tuple[str, ...] = ()
    graph_view: tuple[dict[str, Any], ...] = ()
    unread: tuple[Any, ...] = ()
    recent: tuple[Outcome, ...] = ()
    workspace: dict[str, Any] = field(default_factory=dict)
    budget: dict[str, Any] = field(default_factory=dict)
    notes: tuple[str, ...] = ()
    state: str = ""
    tool_schemas: tuple[dict[str, Any], ...] = ()
    context: Any = None
    #: every tool result this agent has produced, newest last - the "reason again" half of the loop.
    #: `recent` is only the previous turn, which is enough to react but not enough to remember, so a
    #: correction cycle needs this. Owned per agent: there is deliberately no class-level store, and
    #: two agents cannot share a brain by accident.
    history: tuple[Outcome, ...] = ()
    #: appended transcript of this agent's own tool results, oldest first.
    transcript_len: int = 0

    def prompt_seed(self) -> dict[str, Any]:
        """The provider-facing view: explicit fields only, no live object references."""
        return {"agent_id": self.agent_id, "role": self.role, "goal": self.goal,
                "task": self.task, "state": self.state, "unmet": list(self.unmet),
                "graph_view": list(self.graph_view), "budget": self.budget,
                "workspace": self.workspace, "recent": [o.to_dict() for o in self.recent],
                "history_tail": [o.to_dict() for o in self.history][-6:],
                "notes": list(self.notes), "tools": list(self.tool_schemas)}

    def render(self) -> str:
        """A compact text view, so a deterministic policy and a log reader see the same facts."""
        lines = [f"agent={self.agent_id} role={self.role} state={self.state} step={self.step_index}",
                 f"task={self.task.get('task_id')} status={self.task.get('status')} "
                 f"produces={self.task.get('produces')} verify={len(self.task.get('verify') or [])}"]
        if self.unmet:
            lines.append(f"unmet={list(self.unmet)}")
        if self.workspace:
            lines.append(f"workspace files={self.workspace.get('files')} "
                         f"dirty={self.workspace.get('dirty')}")
        for o in self.recent[-3:]:
            lines.append(f"last {o.tool}: ok={o.ok} exit={o.exit_code} "
                         + (f"refused={o.refused}" if o.refused else ""))
        return "\n".join(lines)


# --------------------------------------------------------------------------- intent
@dataclass
class ToolCall:
    tool: str
    args: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"tool": self.tool, "args": dict(self.args)}


@dataclass
class Intent:
    """A proposal. `calls` run before `control` is applied, and the runtime may refuse either."""

    calls: tuple[ToolCall, ...] = ()
    control: str = Control.NONE
    think: str = ""
    reason: str = ""
    wait_for: str = ""
    publish: dict[str, Any] = field(default_factory=dict)
    spawn: dict[str, Any] = field(default_factory=dict)
    max_calls: int = 4

    @property
    def is_noop(self) -> bool:
        return not self.calls and self.control in (Control.NONE, Control.NOOP)

    def to_dict(self) -> dict[str, Any]:
        return {"calls": [c.to_dict() for c in self.calls], "control": self.control,
                "think": self.think, "reason": self.reason, "wait_for": self.wait_for,
                "publish": self.publish, "spawn": self.spawn}

    def fingerprint(self) -> str:
        blob = repr([self.to_dict() for _ in (0,)])
        return hashlib.sha256(blob.encode("utf-8", "replace")).hexdigest()[:12]

    # ---- constructors used by the runtime when it translates an Action or a control verb
    @classmethod
    def of(cls, *calls: ToolCall | dict[str, Any], control: str = "", think: str = "",
           **kw: Any) -> "Intent":
        out = []
        for c in calls:
            if isinstance(c, ToolCall):
                out.append(c)
            else:
                out.append(ToolCall(str(c.get("tool", "")), dict(c.get("args") or {})))
        return Intent(tuple(out), control=control, think=think, **kw)


# --------------------------------------------------------------------------- protocol
@runtime_checkable
class CognitionSource(Protocol):
    """The swap point. Nothing else in the kernel needs to know which implementation is bound."""

    name: str

    def decide(self, obs: Observation) -> Intent: ...

    def fingerprint(self) -> dict[str, Any]:
        """Introspection that makes an *accidentally shared* brain visible rather than plausible."""
        ...


@dataclass
class PolicyCognition:
    """Adapter: an existing `Policy` object becomes a `CognitionSource`.

    `view` is a read-only projection of `ActorContext` (`arena/actor.py:25`) - the policies call
    `ctx.task`, `ctx.self_msg(...)`, `ctx.request_specialist(...)`, and nothing else that mutates,
    so the projection is sufficient and the policy never sees the kernel. `Action` was already a
    proposal the kernel applies, so this adapter is a translation, not a privilege.
    """

    policy: Any
    agent_id: str = ""
    name: str = ""
    model_id: str = "deterministic"
    _obs: Any = None

    def __post_init__(self) -> None:
        if not self.name:
            self.name = f"policy:{getattr(self.policy, 'name', type(self.policy).__name__)}"

    def fingerprint(self) -> dict[str, Any]:
        return {"source": self.name, "kind": "policy", "model": self.model_id,
                "policy": type(self.policy).__name__,
                "obj_id": f"{id(self):x}",
                "prompt_sha256_16": hashlib.sha256(
                    f"{type(self.policy).__name__}|{self.agent_id}".encode()).hexdigest()[:16]}

    def decide(self, obs: Observation) -> Intent:
        from .policy import Act, Action        # local import: policy -> cognition must stay one-way
        self._obs = obs
        try:
            action: Action = self.policy.step(obs.context, obs.message)
        except AttributeError as e:             # a policy that reached for a real context member
            raise CognitionCapabilityError(
                f"policy {type(self.policy).__name__} used a context member that the read-only "
                f"projection does not expose: {e}") from e
        return self._translate(action, Act)

    def _translate(self, action: Any, Act: Any) -> Intent:   # noqa: N803 - enum passed in
        a = action
        act = a.act
        if act is Act.PUBLISH and a.msg is not None:
            from .message import MessageType
            if a.msg.msg_type is MessageType.SPAWN_AGENT_REQUEST:
                return Intent(control=Control.SPAWN_REQUEST, reason=a.reason or "spawn request",
                              spawn={"msg": a.msg})
            return Intent(control=Control.PUBLISH, reason=a.reason or "publish",
                          publish={"msg": a.msg})
        if act is Act.WAIT:
            return Intent(control=Control.WAIT, reason=a.reason or "", wait_for=a.condition or "")
        if act is Act.COMPLETE:
            return Intent(control=Control.COMPLETE, reason=a.reason or "complete",
                          publish={"artifacts": list((a.extra or {}).get("artifacts") or [])})
        if act is Act.ESCALATE:
            return Intent(control=Control.ESCALATE, reason=a.reason or "escalate",
                          publish={"extra": dict(a.extra or {})})
        if act is Act.NOOP:
            return Intent(control=Control.NOOP, reason=a.reason or "")
        return Intent(control=Control.NONE, reason=a.reason or "proceed")


class CognitionCapabilityError(RuntimeError):
    """A cognition source asked for something the runtime does not give it. The turn is refused
    and journalled; the kernel is never mutated by the attempt."""


# --------------------------------------------------------------------------- helpers
def outcomes_from_results(results: Sequence[Any]) -> tuple[Outcome, ...]:
    """ToolResult -> Outcome. Kept here (not in tools.py) so the direction of knowledge is one-way:
    the runtime converts, the cognition layer never imports the executor."""
    out = []
    for r in results:
        out.append(Outcome(tool=getattr(r, "tool", "?"), ok=bool(getattr(r, "ok", False)),
                           exit_code=getattr(r, "exit_code", None),
                           text=getattr(r, "as_block", lambda: "")(),
                           data={k: v for k, v in (getattr(r, "data", {}) or {}).items()
                                 if isinstance(v, (int, float, str, bool))},
                           refused=getattr(r, "refused", "") or "",
                           rid=getattr(r, "rid", "") or ""))
    return tuple(out)


def validate_intent(intent: Any, *, allowed_tools: Sequence[str], max_calls: int = 4) -> tuple[Intent, list[str]]:
    """Runtime-side validation. Returns the (possibly truncated) intent + refusal reasons.

    Never raises: an out-of-range proposal is data about the cognition layer, and the journal is
    where that data belongs.
    """
    notes: list[str] = []
    if intent is None:
        return Intent(control=Control.NOOP, reason="cognition returned nothing"), ["INTENT_MISSING"]
    if not isinstance(intent, Intent):
        return (Intent(control=Control.NOOP, reason="cognition returned a non-Intent"),
                [f"INTENT_NOT_AN_INTENT:{type(intent).__name__}"])
    if intent.control and intent.control not in Control.ALL:
        notes.append(f"CONTROL_UNKNOWN:{intent.control}")
        intent = Intent(calls=intent.calls, control=Control.NOOP, think=intent.think,
                        reason=f"unknown control {intent.control!r}; treated as NOOP")
    calls, dropped = [], 0
    for c in intent.calls:
        if c.tool not in allowed_tools:
            notes.append(f"TOOL_NOT_ALLOWED:{c.tool}")
            dropped += 1
            continue
        calls.append(c)
    if len(calls) > max_calls:
        notes.append(f"CALLS_TRUNCATED:{len(calls)}>{max_calls}")
        calls = calls[:max_calls]
    if dropped and not calls and not intent.control:
        # every proposal was refused and there is no control verb: that is a NOOP with a paper
        # trail, not a silent stall.
        return Intent(control=Control.NOOP, think=intent.think,
                      reason=f"all {dropped} call(s) refused: {';'.join(notes[:3])}"), notes
    return (intent if len(calls) == len(intent.calls) else
            Intent(tuple(calls), control=intent.control, think=intent.think, reason=intent.reason,
                   wait_for=intent.wait_for, publish=intent.publish, spawn=intent.spawn,
                   max_calls=max_calls)), notes


__all__ = ["Control", "YIELDING", "Outcome", "Observation", "ToolCall", "Intent",
           "CognitionSource", "PolicyCognition", "CognitionCapabilityError",
           "outcomes_from_results", "validate_intent"]
