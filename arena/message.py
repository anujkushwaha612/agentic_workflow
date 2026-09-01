"""Message protocol (Phase 1: types, planes, causality, budget accounting).

Three planes, distinct delivery rules (design change #4 in FEASIBILITY.md):

  control    task.* parent.*  - Parent <-> Agent. Only the Parent may mutate task ownership.
  dependency dep.*   agent.*  - Agent <-> Agent + subscriptions. Routed by the Dependency Manager.
  resource   res.*   art.*    - Resource/Artifact managers <-> subscribers. NEVER delivered as a
                                generic agent broadcast in Phase 1; agents react to it only via
                                explicit subscription (prevents "shared_types changed" storms).

Causality: every message carries correlation_id (the logical request it belongs to) and
caused_by / causal_depth. The bus enforces MAX_CAUSAL_DEPTH, which is what kills infinite
A->B->A->B loops structurally instead of by hoping policies behave.
"""
from __future__ import annotations

import enum
import fnmatch
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4


class Plane(enum.StrEnum):
    CONTROL = "control"
    DEPENDENCY = "dependency"
    RESOURCE = "resource"


class MessageType(enum.StrEnum):
    # --- control plane -------------------------------------------------------
    TASK_ASSIGNED = "TASK_ASSIGNED"
    TASK_STARTED = "TASK_STARTED"
    TASK_PROGRESS = "TASK_PROGRESS"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    STATUS_UPDATE = "STATUS_UPDATE"
    AGENT_REGISTERED = "AGENT_REGISTERED"
    AGENT_TERMINATED = "AGENT_TERMINATED"
    AGENT_PAUSED = "AGENT_PAUSED"
    AGENT_RESUMED = "AGENT_RESUMED"
    STATE_TRANSITION = "STATE_TRANSITION"
    PLAN_CREATED = "PLAN_CREATED"
    PLAN_AMENDED = "PLAN_AMENDED"
    REPLAN = "REPLAN"
    # --- dependency plane ----------------------------------------------------
    DEPENDENCY_REQUEST = "DEPENDENCY_REQUEST"
    DEPENDENCY_READY = "DEPENDENCY_READY"
    DEPENDENCY_BLOCKED = "DEPENDENCY_BLOCKED"
    WAIT_REGISTERED = "WAIT_REGISTERED"
    WAIT_RESOLVED = "WAIT_RESOLVED"
    WAIT_TIMEOUT = "WAIT_TIMEOUT"
    API_CONTRACT_READY = "API_CONTRACT_READY"
    FEATURE_REQUEST = "FEATURE_REQUEST"
    REQUEST_ACK = "REQUEST_ACK"
    REQUEST_DECLINED = "REQUEST_DECLINED"
    SPAWN_AGENT_REQUEST = "SPAWN_AGENT_REQUEST"
    SPAWN_APPROVED = "SPAWN_APPROVED"
    SPAWN_REJECTED = "SPAWN_REJECTED"
    # --- Phase 2: the request itself is an event, not just its outcome -------------
    SPAWN_REQUEST_RECEIVED = "SPAWN_REQUEST_RECEIVED"
    SPAWN_REQUEST_RESOLVED = "SPAWN_REQUEST_RESOLVED"
    REQUEST_REROUTED = "REQUEST_REROUTED"
    SPAWN_ESCALATED = "SPAWN_ESCALATED"
    DEFERRED_FOR_CAPACITY = "DEFERRED_FOR_CAPACITY"
    GRAPH_AMENDED = "GRAPH_AMENDED"
    GRAPH_AMEND_REJECTED = "GRAPH_AMEND_REJECTED"
    # --- Phase 2.5 M1/M2: real execution. Two rules from Phase 2 apply verbatim:
    # (a) every decision event carries its keys in the PAYLOAD, not only in a column, because
    #     readers that key on `rid`/`agent_id` look at the payload;
    # (b) a refusal is an event, never an absence.
    WORKSPACE_BOUND = "WORKSPACE_BOUND"
    PROJECT_CREATED = "PROJECT_CREATED"
    TOOL_CALL = "TOOL_CALL"
    TOOL_RESULT = "TOOL_RESULT"
    TOOL_REFUSED = "TOOL_REFUSED"
    COGNITION_VIOLATION = "COGNITION_VIOLATION"
    COMPLETION_REFUSED = "COMPLETION_REFUSED"
    TASK_VERIFIED = "TASK_VERIFIED"
    COGNITION_BOUND = "COGNITION_BOUND"
    COGNITION_ERROR = "COGNITION_ERROR"
    HELP_REQUEST = "HELP_REQUEST"
    BLOCKED = "BLOCKED"
    ERROR_REPORT = "ERROR_REPORT"
    DUPLICATE_CLAIM = "DUPLICATE_CLAIM"
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"
    DEADLOCK_DETECTED = "DEADLOCK_DETECTED"
    CYCLE_REJECTED = "CYCLE_REJECTED"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    # --- resource plane ------------------------------------------------------
    RESOURCE_UPDATED = "RESOURCE_UPDATED"
    FILE_LOCKED = "FILE_LOCKED"
    FILE_RELEASED = "FILE_RELEASED"
    ARTIFACT_PUBLISHED = "ARTIFACT_PUBLISHED"
    # --- journal / runtime bookkeeping ---------------------------------------
    RUN_TICK = "RUN_TICK"
    STATS = "STATS"
    SNAPSHOT = "SNAPSHOT"
    CRASH_SIMULATED = "CRASH_SIMULATED"
    REPLAY_COMPLETE = "REPLAY_COMPLETE"


#: message types that must never trigger further messages (leaves of the causal tree)
TERMINAL_TYPES = frozenset({
    MessageType.STATUS_UPDATE,
    MessageType.TASK_PROGRESS,
    MessageType.AGENT_REGISTERED,
    MessageType.AGENT_TERMINATED,
    MessageType.STATE_TRANSITION,
    MessageType.RUN_TICK,
    MessageType.CRASH_SIMULATED,
    MessageType.REPLAY_COMPLETE,
    MessageType.DUPLICATE_CLAIM,
    MessageType.ILLEGAL_TRANSITION,
    MessageType.BUDGET_EXCEEDED,
    MessageType.TOOL_CALL, MessageType.TOOL_RESULT, MessageType.TOOL_REFUSED,
    MessageType.COGNITION_VIOLATION, MessageType.COMPLETION_REFUSED,
    MessageType.TASK_VERIFIED, MessageType.WORKSPACE_BOUND,
})

#: where each type lives; unknown types default to DEPENDENCY (agents talking to agents)
TYPE_PLANE: dict[MessageType, Plane] = {
    t: Plane.CONTROL for t in (  # noqa: C401
        MessageType.TASK_ASSIGNED, MessageType.TASK_STARTED, MessageType.TASK_COMPLETED,
        MessageType.TASK_FAILED, MessageType.TASK_PROGRESS, MessageType.STATUS_UPDATE,
        MessageType.AGENT_REGISTERED,
        MessageType.AGENT_TERMINATED, MessageType.AGENT_PAUSED, MessageType.AGENT_RESUMED,
        MessageType.STATE_TRANSITION, MessageType.PLAN_CREATED, MessageType.PLAN_AMENDED,
        MessageType.REPLAN, MessageType.SPAWN_APPROVED, MessageType.SPAWN_REJECTED,
        MessageType.DUPLICATE_CLAIM, MessageType.ILLEGAL_TRANSITION, MessageType.CYCLE_REJECTED,
        MessageType.DEADLOCK_DETECTED, MessageType.BUDGET_EXCEEDED, MessageType.RUN_TICK,
        MessageType.CRASH_SIMULATED, MessageType.REPLAY_COMPLETE, MessageType.STATS,
        MessageType.SNAPSHOT,
        MessageType.BLOCKED,
        MessageType.ERROR_REPORT, MessageType.HELP_REQUEST,
        MessageType.SPAWN_REQUEST_RECEIVED, MessageType.SPAWN_REQUEST_RESOLVED,
        MessageType.REQUEST_REROUTED, MessageType.SPAWN_ESCALATED,
        MessageType.DEFERRED_FOR_CAPACITY, MessageType.GRAPH_AMENDED,
        MessageType.GRAPH_AMEND_REJECTED,
        MessageType.TOOL_CALL, MessageType.TOOL_RESULT, MessageType.TOOL_REFUSED,
        MessageType.COGNITION_VIOLATION, MessageType.COMPLETION_REFUSED,
        MessageType.TASK_VERIFIED, MessageType.WORKSPACE_BOUND, MessageType.PROJECT_CREATED,
        MessageType.COGNITION_BOUND, MessageType.COGNITION_ERROR,
    )
}
for _t in (MessageType.RESOURCE_UPDATED, MessageType.FILE_LOCKED, MessageType.FILE_RELEASED,
           MessageType.ARTIFACT_PUBLISHED):
    TYPE_PLANE[_t] = Plane.RESOURCE
for _t in (MessageType.DEPENDENCY_REQUEST, MessageType.DEPENDENCY_READY,
           MessageType.DEPENDENCY_BLOCKED, MessageType.WAIT_REGISTERED,
           MessageType.WAIT_RESOLVED, MessageType.WAIT_TIMEOUT, MessageType.API_CONTRACT_READY,
           MessageType.FEATURE_REQUEST, MessageType.REQUEST_ACK, MessageType.REQUEST_DECLINED,
           MessageType.SPAWN_AGENT_REQUEST):
    TYPE_PLANE[_t] = Plane.DEPENDENCY


#: second topic segment: a category, so subscriptions can say 'control.task.*' (all task
#: lifecycle) without saying 'give me everything the parent sends'.
CATEGORY: dict[MessageType, str] = {}
for _cat, _types in {
    "task": (MessageType.TASK_ASSIGNED, MessageType.TASK_STARTED, MessageType.TASK_PROGRESS,
             MessageType.TASK_COMPLETED, MessageType.TASK_FAILED),
    "agent": (MessageType.AGENT_REGISTERED, MessageType.AGENT_TERMINATED, MessageType.AGENT_PAUSED,
              MessageType.AGENT_RESUMED, MessageType.STATE_TRANSITION, MessageType.STATUS_UPDATE,
              MessageType.ILLEGAL_TRANSITION, MessageType.ERROR_REPORT),
    "plan": (MessageType.PLAN_CREATED, MessageType.PLAN_AMENDED, MessageType.REPLAN,
             MessageType.CYCLE_REJECTED, MessageType.GRAPH_AMENDED,
             MessageType.GRAPH_AMEND_REJECTED),
    "spawn": (MessageType.SPAWN_AGENT_REQUEST, MessageType.SPAWN_APPROVED,
              MessageType.SPAWN_REJECTED, MessageType.SPAWN_REQUEST_RECEIVED,
              MessageType.SPAWN_REQUEST_RESOLVED, MessageType.SPAWN_ESCALATED,
              MessageType.DEFERRED_FOR_CAPACITY),
    "dependency": (MessageType.DEPENDENCY_REQUEST, MessageType.DEPENDENCY_READY,
                   MessageType.DEPENDENCY_BLOCKED, MessageType.WAIT_REGISTERED,
                   MessageType.WAIT_RESOLVED, MessageType.WAIT_TIMEOUT, MessageType.BLOCKED,
                   MessageType.HELP_REQUEST),
    "wait": (MessageType.WAIT_REGISTERED, MessageType.WAIT_RESOLVED, MessageType.WAIT_TIMEOUT),
    "request": (MessageType.FEATURE_REQUEST, MessageType.REQUEST_ACK, MessageType.REQUEST_DECLINED,
                MessageType.REQUEST_REROUTED),
    "conflict": (MessageType.DUPLICATE_CLAIM, MessageType.DEADLOCK_DETECTED,
                 MessageType.BUDGET_EXCEEDED),
    "contract": (MessageType.API_CONTRACT_READY,),
    "resource": (MessageType.RESOURCE_UPDATED, MessageType.FILE_LOCKED, MessageType.FILE_RELEASED),
    "artifact": (MessageType.ARTIFACT_PUBLISHED,),
    "exec": (MessageType.TOOL_CALL, MessageType.TOOL_RESULT, MessageType.TOOL_REFUSED,
             MessageType.COGNITION_VIOLATION, MessageType.WORKSPACE_BOUND,
             MessageType.PROJECT_CREATED, MessageType.COGNITION_BOUND,
             MessageType.COGNITION_ERROR),
    "verify": (MessageType.COMPLETION_REFUSED, MessageType.TASK_VERIFIED),
    "runtime": (MessageType.RUN_TICK, MessageType.CRASH_SIMULATED, MessageType.REPLAY_COMPLETE,
                MessageType.STATS, MessageType.SNAPSHOT),
}.items():
    for _t in _types:
        CATEGORY[_t] = _cat


def plane_of(msg_type: MessageType | str) -> Plane:
    try:
        return TYPE_PLANE.get(MessageType(msg_type), Plane.DEPENDENCY)
    except ValueError:
        return Plane.DEPENDENCY


def topic_for(msg_type: MessageType | str) -> str:
    """Topic = plane.category.type, e.g. 'control.task.assigned', 'dependency.ready'.

    The category segment is what makes subscription globs honest: 'control.task.*' means
    "every task-lifecycle event, and nothing else"; 'resource.*' means the whole resource plane.
    A bare 'task.*' would NOT match a type named 'task_assigned' under fnmatch - that was a real
    bug, caught by the first end-to-end run (7 parent assignments silently undelivered).

    The type segment has the category prefix stripped once (DEPENDENCY_READY -> 'ready'), so
    topics never repeat a word for no reason.
    """
    t = MessageType(msg_type)
    plane = plane_of(t)
    cat = CATEGORY.get(t, "misc")
    name = t.value.lower()
    if name.startswith(cat + "_") and len(name) > len(cat) + 1:
        name = name[len(cat) + 1:]
    return f"{plane.value}.{cat}.{name}"


@dataclass(slots=True)
class Message:
    msg_type: MessageType
    from_actor: str
    to_actor: str = "broadcast"
    body: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    topic: str = ""
    resource: str | None = None
    task_id: str | None = None
    plane: Plane = Plane.DEPENDENCY
    correlation_id: str = ""
    caused_by: str | None = None
    causal_depth: int = 0
    seq: int = -1
    ts: float = 0.0
    mid: str = ""

    def __post_init__(self) -> None:
        self.msg_type = MessageType(self.msg_type)
        self.plane = plane_of(self.msg_type)
        if not self.topic:
            self.topic = topic_for(self.msg_type)
        if not self.mid:
            self.mid = f"m-{uuid4().hex[:12]}"
        if not self.correlation_id:
            self.correlation_id = f"c-{self.mid[2:]}"

    # subscriptions are expressed as glob topics
    def matches(self, pattern: str) -> bool:
        return fnmatch.fnmatchcase(self.topic, pattern) or self.topic == pattern

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq, "ts": round(self.ts, 4), "type": str(self.msg_type),
            "plane": str(self.plane), "topic": self.topic, "from": self.from_actor,
            "to": self.to_actor, "correlation_id": self.correlation_id,
            "caused_by": self.caused_by, "causal_depth": self.causal_depth,
            "task_id": self.task_id, "resource": self.resource, "body": self.body,
            "payload": self.payload,
        }

    def child(self, msg_type: MessageType, from_actor: str, to_actor: str, **kw: Any) -> "Message":
        """Derive a causally-linked message. This is the ONLY sanctioned way for an agent to
        reply, so loop guards can never be bypassed by hand-built Messages."""
        return Message(
            msg_type=msg_type, from_actor=from_actor, to_actor=to_actor,
            correlation_id=kw.pop("correlation_id", self.correlation_id),
            caused_by=kw.pop("caused_by", self.mid),
            causal_depth=kw.pop("causal_depth", self.causal_depth + 1),
            task_id=kw.pop("task_id", self.task_id),
            **kw,
        )


MAX_CAUSAL_DEPTH = 3
#: per-agent outbound messages per run tick; flooding is a bug, so it is capped and logged
MSG_BUDGET_PER_TICK = 8
