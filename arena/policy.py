"""Agent policies + the pluggable cognition source (FEASIBILITY.md §4.3, tier D).

A policy is a pure function (context, message) -> Action. That indirection is the whole point:
Phase 1 ships deterministic heuristics, and a real model can be dropped in behind LLMAdapter
without touching the kernel, the bus, or the tests. Every escalation path ends in an inbox
file, which is how "me, the Arena agent, as the Parent's cortex" plugs in without a daemon
being able to call a model on its own.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Protocol

from .message import Message, MessageType


class Act(enum.StrEnum):
    PROCEED = "PROCEED"
    PUBLISH = "PUBLISH"
    WAIT = "WAIT"
    COMPLETE = "COMPLETE"
    ESCALATE = "ESCALATE"
    NOOP = "NOOP"


#: The actions that end an actor's turn: after one of these the actor has nothing more to do this
#: tick, so the kernel stops draining its inbox.
#:
#: Phase-2 defect D2: `AgentActor` stores `last_action = str(Act.X)`, i.e. UPPER CASE, while the
#: run loop used to compare against a lower-case literal tuple - the break could therefore never
#: fire, and every woken agent ran one extra step per tick. Whether a mid-run request got its answer
#: in tick N or N+1 then depended on how much mail happened to be queued. One shared set, compared
#: case-insensitively, so the two sites cannot drift again.
SLEEPING_ACTIONS = frozenset({Act.WAIT.value, Act.COMPLETE.value, Act.ESCALATE.value, "ERROR"})


def is_sleeping_action(last_action: str) -> bool:
    """True when this actor's previous turn ended in a state that should not be followed by more
    work in the same tick. Tolerates both `str(Act.X)` and a bare lower-case verb."""
    return (last_action or "").upper() in {a.upper() for a in SLEEPING_ACTIONS}


@dataclass
class Action:
    act: Act
    msg: Message | None = None
    condition: str = ""
    reason: str = ""
    timeout: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def proceed(cls, reason: str = "") -> "Action":
        return cls(Act.PROCEED, reason=reason)

    @classmethod
    def publish(cls, msg: Message, reason: str = "") -> "Action":
        return cls(Act.PUBLISH, msg=msg, reason=reason)

    @classmethod
    def wait(cls, condition: str, reason: str = "", timeout: float | None = None) -> "Action":
        return cls(Act.WAIT, condition=condition, reason=reason, timeout=timeout)

    @classmethod
    def complete(cls, reason: str = "", **extra: Any) -> "Action":
        return cls(Act.COMPLETE, reason=reason, extra=extra)

    @classmethod
    def escalate(cls, reason: str, **extra: Any) -> "Action":
        return cls(Act.ESCALATE, reason=reason, extra=extra)


# --------------------------------------------------------------------- interfaces
class Policy(Protocol):
    name: str

    def step(self, ctx: "Any", msg: Message | None) -> Action: ...


class LLMAdapter(Protocol):
    """Optional tier B. Implemented by whoever has a model key; absent here (measured: 0 keys)."""

    def decide(self, prompt: dict[str, Any]) -> Action: ...


class NullLLM:
    """Default: never called, present so the seam is real code and not a comment."""

    name = "null-llm"

    def decide(self, prompt: dict[str, Any]) -> Action:  # pragma: no cover - guard
        raise RuntimeError(
            "No LLM adapter configured. This sandbox has no model API key (verified: "
            "env has 0 api/token/secret vars). Either pass llm=<adapter> to AgentActor or "
            "leave policies deterministic and use the escalation inbox for Arena-side judgment.")


# --------------------------------------------------------------------- built-ins
@dataclass
class SimulatedWork:
    """Honest placeholder for 'the agent does its job': fixed number of progress steps."""

    steps: int = 3
    name: str = "simulated-work"
    #: role_overrides may carry args for other policies; ignoring them beats crashing on them
    _ignored: dict[str, Any] = field(default_factory=dict)

    def step(self, ctx: "Any", msg: Message | None) -> Action:
        # step_index comes from the actor (which the journal restores), never from private state,
        # so a recovered agent continues where it stopped instead of restarting its task.
        if ctx.step_index <= self.steps:
            pct = int(100 * min(1.0, (ctx.step_index - 1) / max(1, self.steps)))
            return Action.publish(ctx.self_msg(
                MessageType.TASK_PROGRESS, f"{pct}% of {ctx.task.title if ctx.task else ''}",
                task_id=ctx.task_id, percent=pct))
        return Action.complete("work finished", artifacts=list(ctx.task.produces)
                               if ctx.task else [])


@dataclass
class WaitForArtifacts:
    """The §6 pattern, correctly implemented: consume a dependency or park durably.

    It does NOT ask "is schema ready?" - it asks the graph once for what it still needs, and if
    something is missing it registers a durable wait and releases its slot. Zero polls.
    """

    timeout: float | None = 4.0
    steps: int = 2
    name: str = "wait-for-artifacts"
    _ignored: dict[str, Any] = field(default_factory=dict)

    def step(self, ctx: "Any", msg: Message | None) -> Action:
        if msg is not None and msg.msg_type is MessageType.DEPENDENCY_READY:
            ctx.log(f"resumed by {msg.body}")
        unmet = ctx.all_unmet()
        if unmet:
            cond = unmet[0]
            return Action.wait(f"artifact:{cond}", reason=f"needs {cond}", timeout=self.timeout)
        # NOTE: never gate on our own outputs - they are published at COMPLETE by definition.
        # An earlier draft did exactly that and the agent spun in PROCEED forever.
        if ctx.step_index <= self.steps:
            return Action.publish(ctx.self_msg(MessageType.TASK_PROGRESS,
                                               f"integrating {ctx.task_id}",
                                               task_id=ctx.task_id, consumed=ctx.consumed_ready()))
        return Action.complete("integrated against upstream artifacts",
                               artifacts=list(ctx.task.produces) if ctx.task else [])


@dataclass
class PollUntilReady:
    """DELIBERATE anti-pattern, used only by the chaos suite to prove polling is detectable."""

    limit: int = 5
    name: str = "poll-anti-pattern"
    _n: int = 0
    _ignored: dict[str, Any] = field(default_factory=dict)

    def step(self, ctx: "Any", msg: Message | None) -> Action:
        self._n += 1
        ctx.kernel.polls.hit(ctx.agent_id)  # counted; the suite asserts on this
        if ctx.unmet_artifacts():
            if self._n > self.limit:
                return Action.escalate(f"gave up after {self._n} polls")
            return Action.proceed(f"checking again ({self._n})")
        return Action.complete("dependency showed up on poll %d" % self._n)


@dataclass
class EscalateOnComplexity:
    """§9 + §10: worker discovers scope creep, requests either a feature or a specialist."""

    threshold: float = 2.0
    request_spawns: int = 1
    role: str = "security engineer"
    reason: str = "auth/authorization design needs specialized expertise"
    skills: tuple[str, ...] = ("auth", "crypto")
    est_work: float = 3.0
    produces: tuple[str, ...] = ()
    new_task_id: str = ""
    name: str = "escalate-on-complexity"
    _asked: int = 0
    _steps: int = 0
    max_steps: int = 6
    _ignored: dict[str, Any] = field(default_factory=dict)

    def _ask(self, ctx: "Any") -> Message:
        """Via ctx.request_specialist, i.e. the same shaped request any other producer emits."""
        return ctx.request_specialist(self.role, self.reason, skills=self.skills,
                                      outputs=self.produces, est_work=self.est_work)

    def step(self, ctx: "Any", msg: Message | None) -> Action:
        if msg is not None and msg.msg_type is MessageType.SPAWN_APPROVED:
            ctx.log(f"parent approved: {msg.body}")
        if msg is not None and msg.msg_type is MessageType.SPAWN_REJECTED:
            ctx.log(f"parent rejected spawn: {msg.payload.get('rule')}")
        if msg is not None and msg.msg_type is MessageType.API_CONTRACT_READY:
            ctx.log("feature request satisfied upstream")
        if self._steps == 0 and (ctx.task is None or ctx.task.est_work >= self.threshold):
            if self._asked < self.request_spawns:
                self._asked += 1
                m = self._ask(ctx)
                return Action.publish(m, "request specialist")
            return Action.escalate("repeated specialist need cannot be resolved locally",
                                   requested_role=self.role)
        if msg is not None and msg.to_actor == ctx.agent_id and \
                msg.msg_type is MessageType.DEPENDENCY_READY and self._asked < self.request_spawns:
            self._asked += 1
            m = self._ask(ctx)
            return Action.publish(m, "request specialist after upstream handoff")
        self._steps += 1
        if self._steps >= self.max_steps:
            return Action.complete("handled with available capability",
                                   artifacts=list(ctx.task.produces) if ctx.task else [])
        return Action.proceed("assessing complexity")


@dataclass
class HybridPolicy:
    """Tier D: heuristics first, model adapter second, inbox last. Default for this build is
    (heuristic, NullLLM) so it runs unattended; wiring a real adapter changes nothing else."""

    heuristic: Any = None
    llm: Any = field(default_factory=NullLLM)
    name: str = "hybrid"
    _ignored: dict[str, Any] = field(default_factory=dict)

    def step(self, ctx: "Any", msg: Message | None) -> Action:
        a = self.heuristic.step(ctx, msg) if self.heuristic is not None else Action.proceed()
        stalled = a.act in (Act.PROCEED, Act.NOOP) and ctx.step_index > 0
        if stalled and ctx.llm_available:
            try:  # tier B - only reached when an adapter with real judgment was injected
                return self.llm.decide({"agent": ctx.agent_id, "task": ctx.task_id,
                                        "unmet": ctx.unmet_artifacts(), "state": ctx.state})
            except RuntimeError as e:
                ctx.log(f"llm unavailable: {e}")
        return a


@dataclass
class NeedsSpecialist:
    """Phase 2 acceptance shape: work for a while, then discover the missing capability mid-task.

    The distinction that matters is *when* it asks. `after_steps > 0` means the request is emitted
    while the org is already running, on a tick chosen by the work, not by the planner - which is
    the difference between "the initial plan happened to include a payments agent" and "the backend
    agent found it needed one". `detect_from` is the honest version of the latter: ask only when the
    current task genuinely consumes an artifact whose declared producer role is not represented on
    the roster. No keyword matching against the original task text.
    """

    role: str = "payment specialist"
    reason: str = "provider-specific webhook handling is outside this agent's declared skills"
    skills: tuple[str, ...] = ("payments", "webhooks")
    est_work: float = 3.0
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ("artifacts/payments-webhooks.md",)
    capability_class: str = "payments"
    #: 0 = ask on the first step (planning-adjacent); >0 = ask after N real work steps
    after_steps: int = 2
    #: ask because the task demands a role nobody on the roster has
    detect_from: str = ""
    max_requests: int = 1
    name: str = "needs-specialist"
    _ignored: dict[str, Any] = field(default_factory=dict)
    _asked: int = 0
    _steps: int = 0

    def _should_ask(self, ctx: "Any") -> bool:
        if self._asked >= self.max_requests or ctx.step_index <= self.after_steps:
            return False
        if not self.detect_from:
            return True
        task = ctx.task
        if task is None:
            return False
        roles = {r.role for r in ctx.registry.agents.values()}
        if self.detect_from in roles:
            return False          # the capability already exists: never ask for a duplicate
        graph = ctx.kernel.graph
        for art in task.consumes:
            for producer in graph.producers.get(art, ()):
                spec = graph.tasks.get(producer)
                if spec is not None and spec.role == self.detect_from:
                    return True   # something I must consume is owned by a role nobody fills
        return False

    def step(self, ctx: "Any", msg: Message | None) -> Action:
        # Counted first, on every turn. `policy_cursor()` reads `_steps`, and the journal's
        # TASK_PROGRESS row carries it; if a step that asks or inspects mail did not advance the
        # counter, the LAST journaled cursor would lag the real progress and a replay would restore
        # a lower one (found by the Phase 2 parity check, which compares the two exactly).
        self._steps += 1
        if msg is not None:
            if msg.msg_type is MessageType.SPAWN_APPROVED:
                ctx.log(f"parent approved spawn: {msg.payload.get('agent_id')}")
            elif msg.msg_type is MessageType.SPAWN_REJECTED:
                ctx.log(f"parent refused spawn ({msg.payload.get('rule')}); continuing alone")
            elif msg.msg_type is MessageType.REQUEST_REROUTED:
                ctx.log(f"work rerouted to {msg.payload.get('owner')} instead of a new agent")
        if self._should_ask(ctx):
            self._asked += 1
            m = ctx.request_specialist(self.role, self.reason, skills=self.skills,
                                       inputs=self.inputs or tuple(ctx.task.consumes)
                                       if ctx.task else self.inputs,
                                       outputs=self.outputs, est_work=self.est_work,
                                       capability_class=self.capability_class)
            return Action.publish(m, "request specialist mid-run")
        if ctx.task is None:
            return Action.proceed("no task; waiting for work")
        if ctx.step_index > self.after_steps + 6:
            return Action.complete("finished with the capability available",
                                   artifacts=list(ctx.task.produces))
        return Action.proceed("working")


BUILTIN_POLICIES: dict[str, type] = {
    "simulated": SimulatedWork, "wait": WaitForArtifacts, "poll": PollUntilReady,
    "escalate": EscalateOnComplexity, "specialist": NeedsSpecialist,
}


def make_policy(name: str | Policy | None, **kw: Any) -> Any:
    # duck-typed on purpose: `Policy` is a static Protocol, so isinstance() on it raises
    if name is not None and not isinstance(name, str) and hasattr(name, "step"):
        return name
    if name is None:
        return SimulatedWork()
    cls = BUILTIN_POLICIES.get(name) if isinstance(name, str) else None
    if cls is None:
        raise KeyError(f"unknown policy {name!r}; choose from {sorted(BUILTIN_POLICIES)}")
    accepted = set(getattr(cls, "__dataclass_fields__", {}))
    if "_ignored" in accepted and accepted:
        known = {k: v for k, v in kw.items() if k in accepted}
        rest = {k: v for k, v in kw.items() if k not in accepted}
        return cls(**known, _ignored=rest)
    return cls(**kw)
