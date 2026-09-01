"""Phase 2 — the spawn *request* as a first-class runtime object.

Three things live here, and none of them is "AI":

* :class:`SpawnRequest`  — the typed, journalled unit of mid-run replanning. An agent does not
  hand-build a dict; it calls ``ctx.request_specialist(...)`` which produces one of these.
* :class:`RequestState`  — a small lifecycle for the request itself, deliberately SEPARATE from the
  agent FSM in ``lifecycle.py``. Dynamic spawning must not need a new agent state: a spawned agent
  is born the same way a planned one is.
* :class:`CapabilityCatalog` — what this arena is even able to staff. ``REJECT_UNSUPPORTED`` comes
  from here, not from a hard-coded list inside a veto function.

The decision rules themselves stay in :class:`arena.parent.ParentArena` (Phase 1's veto engine,
renamed to the spec's vocabulary) so that the reviewed policy code keeps one owner.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Iterable

from .graph import TaskSpec

# --------------------------------------------------------------------------- vocabulary


class RequestState(StrEnum):
    RECEIVED = "RECEIVED"
    EVALUATING = "EVALUATING"
    APPROVED_COMMITTED = "APPROVED_COMMITTED"
    REROUTED = "REROUTED"
    REJECTED = "REJECTED"
    DEDUPLICATED = "DEDUPLICATED"
    ESCALATED = "ESCALATED"
    DEFERRED = "DEFERRED"


#: terminal states: a request in one of these can never move again, and a duplicate arriving later
#: is matched against them (that is what makes "we already decided this" cheap to answer).
TERMINAL_REQUEST_STATES = frozenset({
    RequestState.APPROVED_COMMITTED, RequestState.REROUTED, RequestState.REJECTED,
    RequestState.DEDUPLICATED, RequestState.ESCALATED,
})

REQUEST_TRANSITIONS: dict[RequestState, frozenset[RequestState]] = {
    RequestState.RECEIVED: frozenset({RequestState.EVALUATING}),
    RequestState.EVALUATING: frozenset({
        RequestState.APPROVED_COMMITTED, RequestState.REROUTED, RequestState.REJECTED,
        RequestState.DEDUPLICATED, RequestState.ESCALATED, RequestState.DEFERRED,
    }),
    # DEFERRED is the only state with a way back in: capacity frees up, the parent retries.
    RequestState.DEFERRED: frozenset({
        RequestState.EVALUATING, RequestState.APPROVED_COMMITTED, RequestState.REROUTED,
        RequestState.REJECTED,
    }),
}

#: the seven decisions the spec names, plus the two we added because a runtime needs them
RULES = (
    "APPROVE",
    "REJECT_NOT_WORTH_IT",
    "REJECT_CAP",
    "REJECT_DUPLICATE_CAPABILITY",
    "REJECT_SPAWN_DEPTH",
    "REJECT_UNSUPPORTED",
    "REJECT_MALFORMED",
    "REJECT_CYCLE",
    "ESCALATE",
    "DEDUPLICATE",
    "DEFER_FOR_CAPACITY",
)
RULES = frozenset(RULES)

#: required fields of a well-formed request. Missing any of these is REJECT_MALFORMED, which is a
#: journalled outcome - the point is not "the agent cannot forget", the point is "forgetting is loud".
REQUIRED_FIELDS = ("requester_agent_id", "requested_role", "reason", "estimated_work",
                   "expected_outputs", "correlation_id")


class MalformedRequest(ValueError):
    """Raised by SpawnRequest.from_message() when a hand-built message is missing required fields."""


# --------------------------------------------------------------------------- catalog


@dataclass(slots=True)
class CapabilityCatalog:
    """What kinds of work this arena can staff at all.

    Not a test stub: it is the extension point. A project that adds a GPU worker pool adds a class
    here and ``REJECT_UNSUPPORTED`` starts allowing it - no change to the veto engine.
    """

    serviceable: dict[str, tuple[str, ...]] = field(default_factory=lambda: {
        "general": ("general", "documentation", "review"),
        "api": ("api", "backend", "service", "integration"),
        "frontend": ("frontend", "ui", "dashboard"),
        "data": ("data", "analytics", "pipeline", "etl"),
        "database": ("database", "schema", "migration"),
        "security": ("security", "auth", "crypto"),
        "payments": ("payments", "billing", "stripe", "webhooks"),
        "cloud": ("cloud", "infra", "devops"),
        "ml": ("ml", "model", "training-script"),
    })
    #: classes the arena will refuse rather than fake (a real constraint of the host)
    unserviceable: dict[str, str] = field(default_factory=lambda: {
        "gpu-training": "no accelerator on this host; produce a training plan instead",
        "k8s-ops": "no cluster credentials in the sandbox; produce manifests instead",
        "db-admin": "no live database; produce migrations instead",
        "prod-deploy": "no production access; produce a runbook instead",
    })

    def known(self, capability_class: str) -> bool:
        c = (capability_class or "general").strip().lower()
        return c in self.serviceable or c in self.unserviceable

    def is_serviceable(self, capability_class: str) -> bool:
        return (capability_class or "general").strip().lower() in self.serviceable

    def reason_for_unserviceable(self, capability_class: str) -> str:
        return self.unserviceable.get((capability_class or "").strip().lower(), "")

    def class_for(self, role: str, skills: Iterable[str] = ()) -> str:
        """Map free-text role/skills onto a capability class (longest token wins)."""
        tokens = {t for t in (role or "").lower().replace("/", " ").replace("-", " ").split()}
        tokens |= {s.strip().lower() for s in skills if s and s.strip()}
        best, best_len = "general", 0
        for cls, kws in self.serviceable.items():
            for kw in kws:
                if kw in tokens and len(kw) > best_len:
                    best, best_len = cls, len(kw)
        return best

    def as_dict(self) -> dict[str, Any]:
        return {"serviceable": dict(self.serviceable), "unserviceable": dict(self.unserviceable)}


# --------------------------------------------------------------------------- request


@dataclass(slots=True)
class SpawnRequest:
    """A mid-run demand for capability the requesting agent does not have."""

    requester_agent_id: str = ""
    requested_role: str = ""
    reason: str = ""
    required_skills: tuple[str, ...] = ()
    estimated_work: float = 0.0
    required_inputs: tuple[str, ...] = ()
    expected_outputs: tuple[str, ...] = ()
    parent_task_id: str | None = None
    correlation_id: str = ""
    capability_class: str = "general"
    requires_judgment: bool = False
    #: set when the parent decides to reuse an existing agent instead of spawning
    preferred_owner: str | None = None
    mid: str = ""
    rid: str = ""
    state: RequestState = RequestState.RECEIVED
    rule: str = ""
    detail: str = ""
    at_tick: int = -1
    created_at: float = 0.0

    # -------------------------------------------------------------- construction
    @classmethod
    def from_message(cls, msg: Any, *, requester: str = "") -> "SpawnRequest":
        """Build one from a bus message. Raises MalformedRequest (never silently half-works)."""
        p = dict(getattr(msg, "payload", None) or {})
        from_actor = requester or getattr(msg, "from_actor", "") or p.get("from", "")
        role = p.get("requested_role") or p.get("role") or ""
        skills = tuple(s for s in (p.get("required_skills") or p.get("skills") or ()) if s)
        outputs = tuple(p.get("expected_outputs") or p.get("produces") or ())
        try:
            est = float(p.get("estimated_work") or p.get("work_estimate") or 0.0)
        except (TypeError, ValueError) as e:
            raise MalformedRequest(f"estimated_work is not a number: {p.get('estimated_work')!r}") from e
        req = cls(
            requester_agent_id=from_actor,
            requested_role=str(role),
            reason=str(p.get("reason") or getattr(msg, "body", "") or ""),
            required_skills=skills,
            estimated_work=est,
            required_inputs=tuple(p.get("required_inputs") or ()),
            expected_outputs=outputs,
            parent_task_id=p.get("parent_task_id") or p.get("task_id"),
            correlation_id=getattr(msg, "correlation_id", "") or p.get("correlation_id", ""),
            capability_class=str(p.get("capability_class") or "general"),
            requires_judgment=bool(p.get("requires_judgment", False)),
            mid=getattr(msg, "mid", "") or "",
        )
        # Normalise first: reason/outputs are inferable, and an inferable field must not be the
        # reason a legitimate request dies. validate() then only guards the fields where a wrong
        # value would silently mis-charge or mis-route the org.
        req.normalise()
        req.validate()
        return req

    def validate(self) -> None:
        missing = []
        if not self.requester_agent_id:
            missing.append("requester_agent_id")
        if not (self.requested_role or "").strip():
            missing.append("requested_role")
        if not (self.reason or "").strip():
            missing.append("reason")
        if self.estimated_work is None or self.estimated_work <= 0:
            missing.append("estimated_work")
        if not self.expected_outputs:
            missing.append("expected_outputs")
        if not (self.correlation_id or self.mid):
            missing.append("correlation_id")
        bad = [f for f in missing if f in REQUIRED_FIELDS]
        if bad:
            raise MalformedRequest(f"missing required field(s): {', '.join(sorted(bad))}")

    def normalise(self) -> list[str]:
        """Fill the fields a *runtime* can safely infer, and report which ones it had to.

        The split is deliberate. requester / estimate / correlation id are the ones where a wrong
        value silently mis-routes or mis-charges the org, so those stay hard-rejected (see
        validate()). `reason` and `expected_outputs` are descriptive: a legacy or hand-built request
        that omits them gets a default plus a journalled `inferred` list rather than a refusal, and
        the test can assert on exactly which fields were guessed. This keeps the Phase-1 payloads
        (which carry neither) working without loosening the parts that actually matter.
        """
        inferred: list[str] = []
        slug = re.sub(r"[^a-z0-9]+", "_", (self.requested_role or "specialist").strip().lower())
        if not (self.reason or "").strip():
            self.reason = f"specialist work: {self.requested_role or 'unspecified'}"
            inferred.append("reason")
        if not self.expected_outputs:
            self.expected_outputs = (f"artifacts/{slug}.md",)
            inferred.append("expected_outputs")
        if not self.required_skills:
            self.required_skills = (slug,) if self.requested_role else ("general",)
            inferred.append("required_skills")
        return inferred

    # -------------------------------------------------------------- identity
    def fingerprint(self) -> str:
        """Dedup key. Deliberately NOT keyed on the requester: two agents asking for the same
        missing capability is still one capability, and letting both through is how a 2-agent
        arena becomes a 6-agent arena inside four ticks."""
        core = {
            "class": (self.capability_class or "general").lower(),
            "role": (self.requested_role or "").strip().lower(),
            "skills": sorted({s.lower() for s in self.required_skills}),
            "outputs": sorted(self.expected_outputs),
        }
        return hashlib.sha256(json.dumps(core, sort_keys=True).encode()).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rid": self.rid, "requester_agent_id": self.requester_agent_id,
            "requested_role": self.requested_role, "reason": self.reason,
            "required_skills": list(self.required_skills),
            "estimated_work": self.estimated_work,
            "required_inputs": list(self.required_inputs),
            "expected_outputs": list(self.expected_outputs),
            "parent_task_id": self.parent_task_id, "correlation_id": self.correlation_id,
            "capability_class": self.capability_class,
            "requires_judgment": self.requires_judgment,
            "fingerprint": self.fingerprint(), "state": str(self.state), "rule": self.rule,
            "detail": self.detail, "at_tick": self.at_tick, "mid": self.mid,
        }

    # -------------------------------------------------------------- the mutation it implies
    def task_id(self) -> str:
        """Stable across a restart. Keyed on the fingerprint, NOT on rid: rid embeds a run-local
        counter, so a replayed kernel would invent `t_pay_0007` for work the live run called
        `t_pay_0003`, and the parity check would fail for a reason that has nothing to do with the
        journal."""
        base = re.sub(r"[^a-z0-9]+", "_", (self.requested_role or "specialist").strip().lower())
        return f"t_{base}_{self.fingerprint()[:6]}"

    def to_task_spec(self) -> TaskSpec:
        """The only sanctioned path from a request to a task.

        ``required_inputs`` becomes ``consumes``, which the graph then derives into real edges -
        that is what makes the amendment *plumbed* rather than merely appended.
        """
        return TaskSpec(
            task_id=self.task_id(),
            title=self.reason[:80] or f"{self.requested_role} work",
            role=self.requested_role,
            skills=list(self.required_skills),
            est_work=self.estimated_work,
            produces=list(self.expected_outputs),
            consumes=list(self.required_inputs),
            claims=[f"spawn:{self.fingerprint()[:8]}"],
        )


# --------------------------------------------------------------------------- ledger


@dataclass(slots=True)
class LedgerEntry:
    rid: str
    request: SpawnRequest
    state: RequestState = RequestState.RECEIVED
    rule: str = ""
    owner: str | None = None
    spawned_agent_id: str | None = None
    task_id: str | None = None
    attempts: int = 0
    history: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"rid": self.rid, "state": str(self.state), "rule": self.rule, "owner": self.owner,
                "spawned_agent_id": self.spawned_agent_id, "task_id": self.task_id,
                "attempts": self.attempts, "history": list(self.history),
                "requester": self.request.requester_agent_id,
                "role": self.request.requested_role,
                "capability_class": self.request.capability_class,
                "estimated_work": self.request.estimated_work,
                "expected_outputs": list(self.request.expected_outputs),
                "fingerprint": self.request.fingerprint(),
                "at_tick": self.request.at_tick,
                "correlation_id": self.request.correlation_id}


class IllegalRequestTransition(Exception):
    def __init__(self, frm: RequestState, to: RequestState) -> None:
        super().__init__(f"illegal request transition: {frm} -> {to}")
        self.frm, self.to = frm, to


@dataclass(slots=True)
class SpawnLedger:
    """In-memory index over the journal for questions the log answers badly.

    Two O(1) lookups are the whole reason this exists: "is a request for this capability in flight
    right now?" (dedup, before an agent is created) and "who asked for what, this run?"
    (monitoring). It is rebuilt from the journal on replay, so it can never outrun the log.
    """

    entries: dict[str, LedgerEntry] = field(default_factory=dict)
    by_fingerprint: dict[str, list[str]] = field(default_factory=dict)
    _seq: int = 0

    # ------------------------------------------------------------ write
    def open(self, req: SpawnRequest) -> LedgerEntry:
        self._seq += 1
        rid = req.rid or f"rq-{self._seq:04d}"
        req.rid = rid
        entry = LedgerEntry(rid=rid, request=req)
        self.entries[rid] = entry
        self.by_fingerprint.setdefault(req.fingerprint(), []).append(rid)
        return entry

    def move(self, rid: str, to: RequestState, note: str = "", *, strict: bool = True,
             rule: str = "") -> bool:
        entry = self.entries.get(rid)
        if entry is None:
            return False
        if rule:
            entry.rule = rule
            entry.request.rule = rule
        if entry.state is to:
            entry.history.append(f"{to}: {note}".strip())
            return True
        if strict and to not in REQUEST_TRANSITIONS.get(entry.state, frozenset()):
            raise IllegalRequestTransition(entry.state, to)
        entry.state = to
        entry.request.state = to
        entry.history.append(f"{to}: {note}".strip())
        return True

    def close_as(self, rid: str, state: RequestState, rule: str, detail: str = "") -> bool:
        entry = self.entries.get(rid)
        if entry is None:
            return False
        entry.rule = rule
        entry.request.rule = rule
        entry.request.detail = detail
        entry.state = state
        entry.request.state = state
        entry.history.append(f"{state}: {rule}")
        return True

    # ------------------------------------------------------------ read
    def inflight_for(self, fingerprint: str) -> LedgerEntry | None:
        for rid in self.by_fingerprint.get(fingerprint, ()):
            e = self.entries.get(rid)
            if e is not None and e.state in (RequestState.RECEIVED, RequestState.EVALUATING,
                                             RequestState.DEFERRED):
                return e
        return None

    def resolved_for(self, fingerprint: str) -> LedgerEntry | None:
        for rid in self.by_fingerprint.get(fingerprint, ()):
            e = self.entries.get(rid)
            if e is not None and e.state in TERMINAL_REQUEST_STATES:
                return e
        return None

    def newest_first(self) -> list[LedgerEntry]:
        return [self.entries[r] for r in sorted(self.entries, reverse=True)]

    def counts(self) -> dict[str, int]:
        out = {str(s): 0 for s in RequestState}
        for e in self.entries.values():
            out[str(e.state)] = out.get(str(e.state), 0) + 1
        out["total"] = len(self.entries)
        return out

    def by_rule(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for e in self.entries.values():
            if e.rule:
                out[e.rule] = out.get(e.rule, 0) + 1
        return out

    def rehydrate(self, req: SpawnRequest, state: RequestState, rule: str = "",
                  owner: str | None = None, spawned: str | None = None,
                  task_id: str | None = None) -> LedgerEntry:
        """Rebuild one entry from journalled events (replay path). No transition validation: the
        journal is authoritative for what already happened."""
        entry = self.open(req)
        entry.state = state
        entry.request.state = state
        entry.rule = rule
        entry.owner = owner
        entry.spawned_agent_id = spawned
        entry.task_id = task_id
        entry.history.append(f"replayed -> {state}")
        return entry
