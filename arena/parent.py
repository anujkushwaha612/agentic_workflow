"""Parent Arena (your §1, §12, §14) - planner, scheduler, decision engine.

Phase 1 uses a RuleBasedPlanner so the system is autonomous with no model present. Its output is
*not* a hardcoded agent list: it emits task specs with artifact produce/consume declarations, and
the DAG (graph.py) then derives who depends on whom. Change the task text, and the roles, the
agent count and the parallelism all change - that is the property the chaos suite checks.

The decision engine implements the four questions from §10 as an ordered veto list, and returns a
reason code on every path (including rejections) so 'why didn't it spawn' is answerable later.
Task ownership is claim-based (journal.claim, first writer wins) so a second agent claiming the
same unit of work is refused with a `DUPLICATE_CLAIM` event rather than overwriting the owner.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from .graph import CycleError, DependencyGraph, TaskSpec
from .lifecycle import AgentState
from .message import Message, MessageType
from .registry import AgentRecord, AgentRegistry, emit_registered, emit_terminated
from .spawn import (
    CapabilityCatalog, MalformedRequest, RequestState, SpawnLedger, SpawnRequest,
)


# ------------------------------------------------------------------------ planner
@dataclass
class Rule:
    #: tokens in the task text that trigger this role
    when: tuple[str, ...]
    task: tuple[str, str, list[str], list[str], list[str], float]
    #: (task_id, title, role, skills, produces, est_work)
    extra_tasks: tuple[tuple[str, str, list[str], list[str], list[str], float], ...] = ()
    requires: tuple[str, ...] = ()


class RuleBasedPlanner:
    """Cheap, explainable and honest about being deterministic. `arena plan` prints the reasons."""

    name = "rule-based"

    RULES: list[Rule] = [
        Rule(("database", "postgres", "sql", "schema", "db"),
             ("t_db_schema", "design schema + migrations", "database", ["sql", "schema"],
              ["database/schema.sql", "database/migrations"], 3.0)),
        Rule(("api", "backend", "rest", "endpoint", "server", "graphql"),
             ("t_api_contract", "author API contract (OpenAPI)", "backend", ["api", "contracts"],
              ["contracts/api.json"], 2.0),
             (("t_api_impl", "implement API endpoints", "backend", ["api", "server"],
               ["backend/src"], 4.0),)),
        Rule(("auth", "login", "session", "jwt", "oauth", "password"),
             ("t_auth", "authentication + session design", "auth", ["auth", "sessions", "jwt"],
              ["contracts/auth_api.json"], 3.0)),
        Rule(("frontend", "ui", "dashboard", "react", "web app", "screen"),
             ("t_fe_design", "UI architecture + design system", "frontend", ["ui", "react"],
              ["frontend/design"], 2.0),
             (("t_fe_integration", "wire UI to API", "frontend", ["ui", "integration"],
               ["frontend/src"], 3.0),)),
        Rule(("cloud", "deploy", "deployment", "aws", "infra", "terraform", "k8s"),
             ("t_cloud_infra", "infrastructure + deployment spec", "cloud", ["iac", "deploy"],
              ["deploy/main.tf", "deploy/compose.yaml"], 2.0)),
        Rule(("payment", "stripe", "billing", "checkout", "invoice"),
             ("t_payments", "payment provider integration", "payments", ["stripe", "billing"],
              ["contracts/payments_api.json", "backend/payments"], 4.0)),
        Rule(("analytics", "event", "metrics", "tracking", "telemetry"),
             ("t_analytics", "event taxonomy + pipeline", "data", ["analytics", "etl"],
              ["analytics/schema.json"], 3.0)),
        Rule(("ml", "model", "train", "inference", "dataset"),
             ("t_ml_pipeline", "data pipeline + training loop", "ml-engineer", ["ml", "pipeline"],
              ["ml/pipeline"], 4.0),
             (("t_ml_eval", "evaluation harness", "evaluation", ["metrics", "eval"],
               ["ml/eval"], 2.0),)),
        Rule(("test", "qa", "coverage", "e2e"),
             ("t_tests", "test strategy + suites", "testing", ["pytest", "e2e"],
              ["tests"], 3.0)),
        Rule(("security", "audit", "penetration", "encryption"),
             ("t_security", "threat model + controls", "security", ["threat-model", "crypto"],
              ["docs/security.md"], 2.5)),
    ]
    #: everything depends on shared types once the contract layer exists
    SHARED = "shared/types.ts"

    def plan(self, text: str) -> tuple[list[TaskSpec], dict[str, Any]]:
        low = text.lower()
        tasks: list[TaskSpec] = []
        hits: dict[str, list[str]] = {}
        for rule in self.RULES:
            matched = [w for w in rule.when if w in low]
            if not matched:
                continue
            hits[rule.task[2]] = matched
            tid, title, role, skills, produces, est = rule.task
            tasks.append(TaskSpec(task_id=tid, title=title, role=role, skills=skills,
                                  produces=produces, est_work=est))
            for et_id, et_title, et_role, et_skills, et_prod, et_est in rule.extra_tasks:
                hits.setdefault(et_role, []).append(f"{tid}:sequel")
                tasks.append(TaskSpec(task_id=et_id, title=et_title, role=et_role,
                                      skills=et_skills, produces=et_prod, est_work=et_est))
        by_id = {t.task_id: t for t in tasks}
        # derived dependencies: any task producing shared types unblocks consumers
        for t in tasks:
            if t.task_id == "t_fe_integration":
                t.consumes = [a for a in ("contracts/api.json", "contracts/auth_api.json",
                                         "contracts/payments_api.json") if any(
                    a in o.produces for o in tasks)]
            if t.task_id == "t_api_impl":
                t.consumes = [a for a in ("database/schema.sql", "contracts/auth_api.json",
                                          "database/migrations") if any(a in o.produces for o in tasks)]
            if t.task_id == "t_tests":
                t.consumes = [a for a in ("backend/src", "contracts/api.json", "frontend/src")
                              if any(a in o.produces for o in tasks)]
            if t.task_id == "t_api_contract":
                t.consumes = [a for a in ("database/schema.sql",) if any(a in o.produces for o in tasks)]
            if t.task_id == "t_ml_eval":
                t.consumes = [a for a in ("ml/pipeline",) if any(a in o.produces for o in tasks)]
            if t.task_id == "t_security":
                t.consumes = [a for a in ("contracts/auth_api.json",) if any(a in o.produces for o in tasks)]
            if t.task_id == "t_analytics" and "t_api_contract" in by_id:
                t.consumes = ["contracts/api.json"]
        del by_id
        roles = sorted({t.role for t in tasks})
        rationale = {"matched": hits, "roles": roles,
                     "planner": self.name,
                     "artifact_edges": {t.task_id: t.consumes for t in tasks if t.consumes}}
        return tasks, rationale


# ------------------------------------------------------------------- decision engine
@dataclass
class Decision:
    ok: bool
    rule: str
    detail: str
    owner: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "rule": self.rule, "detail": self.detail, "owner": self.owner}


@dataclass
class ParentArena:
    kernel: Any
    planner: Any = field(default_factory=RuleBasedPlanner)
    inbox: list[dict[str, Any]] = field(default_factory=list)
    escalations: list[dict[str, Any]] = field(default_factory=list)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    #: Phase 2: the queue carries SpawnRequest objects; legacy dict entries are still accepted
    #: (Phase-1 chaos scenarios push dicts directly) and normalised in intake().
    spawn_requests: list[Any] = field(default_factory=list)
    #: durable index over the request log: dedup + "who asked for what" in O(1)
    ledger: SpawnLedger = field(default_factory=SpawnLedger)
    catalog: CapabilityCatalog = field(default_factory=CapabilityCatalog)
    #: rid of every request currently parked for a judgement call. `apply_arena_decisions` reads
    #: these back, which is what makes ESCALATE a pause rather than a dead end.
    escalated: set[str] = field(default_factory=set)
    #: Phase-2 monitoring counters (see metrics() on the kernel)
    spawn_stats: dict[str, int] = field(default_factory=lambda: {
        "received": 0, "approved": 0, "rejected": 0, "deduplicated": 0, "escalated": 0,
        "deferred": 0, "reused": 0, "spawned_by_agents": 0, "cycles_rejected": 0})
    feature_requests: list[dict[str, Any]] = field(default_factory=list)
    accepted_decisions: list[dict[str, Any]] = field(default_factory=list)
    #: Arena-side cortex: decisions I write back get applied on the next run()
    decision_path: str = "var/decisions.jsonl"
    inbox_path: str = "var/inbox.jsonl"

    # ---------------------------------------------------------------- planning
    def submit(self, text: str) -> dict[str, Any]:
        tasks, rationale = self.planner.plan(text)
        if not tasks:
            self.kernel.journal.emit(MessageType.PLAN_CREATED, "parent", "parent",
                                     body="no task matched; nothing to spawn",
                                     rationale=rationale, tasks=[])
            return {"tasks": 0, "agents": [], "rationale": rationale}
        for t in tasks:
            self.kernel.graph.add(t)
        try:
            self.kernel.graph.validate()
        except CycleError as e:
            # The plan itself is unsatisfiable (artifact cycle). Refuse it here rather than let
            # the scheduler spin on a graph with no valid ordering.
            self.kernel.journal.emit(MessageType.CYCLE_REJECTED, "parent", "parent",
                                     body=f"plan rejected: {e}", cycle=[str(x) for x in e.cycle],
                                     rule="PLAN_CYCLE")
            raise
        self.kernel.journal.emit(MessageType.PLAN_CREATED, "parent", "parent",
                                 body=f"planned {len(tasks)} tasks from task text",
                                 tasks=[{"task_id": t.task_id, "title": t.title, "role": t.role,
                                        "skills": t.skills, "produces": t.produces,
                                        "consumes": t.consumes, "est_work": t.est_work,
                                        "deps": sorted(t.deps)} for t in tasks],
                                 rationale=rationale)
        agents = self.spawn_for_plan(tasks)
        return {"tasks": len(tasks), "agents": agents, "rationale": rationale,
                "order": self.kernel.graph.order(),
                "parallelism": [len(g) for g in self.kernel.graph.order()]}

    def spawn_for_plan(self, tasks: list[TaskSpec]) -> list[str]:
        made: dict[str, str] = {}
        cap = self.kernel.registry.budget.max_active_agents
        deferred: list[str] = []
        for t in tasks:
            if len(self.kernel.registry.active()) >= cap:
                # Over capacity: leave the task unowned and say so. The scheduler re-picks it up
                # when a slot frees. The earlier draft ignored the cap here entirely.
                deferred.append(t.task_id)
                continue
            existing = self.kernel.registry.cover(t.role, t.skills)
            reuse = [a for a in existing if a.epoch == 0]
            if reuse:
                made[t.task_id] = reuse[0].agent_id
                continue
            aid = self._spawn(role=t.role, skills=t.skills, reason=f"plan task {t.task_id}",
                              epoch=0, spawned_by="parent")
            if aid:
                made[t.task_id] = aid
        for tid, aid in made.items():
            self.assign(tid, aid, reason="planned")
        if deferred:
            self.kernel.journal.emit(MessageType.STATUS_UPDATE, "parent", "parent",
                                      body=f"plan under-staffed: {len(deferred)} task(s) deferred "
                                           f"by MAX_ACTIVE_AGENTS={cap}", deferred=deferred,
                                      rule="DEFERRED_FOR_CAPACITY")
        return sorted(set(made.values()))

    def _spawn(self, *, role: str, skills: list[str] | tuple[str, ...], reason: str,
               epoch: int, spawned_by: str, agent_id: str | None = None) -> str | None:
        reg = self.kernel.registry
        if agent_id is None and len(reg.active()) >= reg.budget.max_active_agents:
            return None
        aid = agent_id or reg.next_id(role)
        rec = reg.register(agent_id=aid, role=role, skills=skills, epoch=epoch,
                           spawned_by=spawned_by, spawn_reason=reason)
        # route through the bus so the kernel map and the record agree (an earlier draft passed
        # subscriptions to register(), which quietly dropped them -> nobody received resources)
        self.kernel.bus.subscribe(aid, self.kernel.agent_subscriptions)
        # AGENT_REGISTERED has to be journalled BEFORE the actor is bound: `make_actor` walks
        # CREATED -> INITIALIZING -> IDLE, fold() creates its agent record at the first event that
        # mentions the id, and a STATE_TRANSITION row that precedes the registration row is filed
        # nowhere. The live kernel showed the new agent IDLE while every replay of the same journal
        # left it CREATED - i.e. a resumed org silently refused to run the work it had just spawned
        # an agent for (found by the spawn-crash-after-approval chaos scenario).
        emit_registered(self.kernel.journal, rec)
        self.kernel.make_actor(aid)
        self.kernel.journal.emit(MessageType.STATUS_UPDATE, "parent", aid,
                                 body=f"agent {aid} ({role}) joined the arena", epoch=epoch)
        return aid

    # ------------------------------------------------------------- assignment
    def assign(self, task_id: str, agent_id: str, *, reason: str = "",
               correlation_id: str = "") -> bool:
        reg, graph = self.kernel.registry, self.kernel.graph
        task = graph.tasks[task_id]
        ok, incumbent = self.kernel.journal.claim(task.key, agent_id, task_id, self.kernel.now)
        if not ok:
            self.kernel.journal.emit(MessageType.DUPLICATE_CLAIM, agent_id, "parent",
                                     body=f"{task_id} already claimed by {incumbent}",
                                     task_id=task_id, owner=incumbent, rule="CLAIM_TAKEN")
            return False
        task.owner = agent_id
        if task.status == "pending":
            task.status = "assigned"
        rec = reg.get(agent_id)
        if rec is not None:
            if rec.task_id and rec.task_id != task_id:
                if task_id not in rec.task_queue:
                    rec.task_queue.append(task_id)
            elif rec.task_id is None:
                rec.task_id = task_id
            # An agent finishing its backlog reaches COMPLETED; new work re-opens it through the
            # one legal edge (COMPLETED -> INITIALIZING) instead of forcing WORKING behind the
            # guard. Assignment itself never edits state - that is the actor's job.
            if rec.lifecycle.state is AgentState.COMPLETED:
                self.kernel.transition(agent_id, AgentState.INITIALIZING, f"re-queued {task_id}")
        self.kernel.publish(Message(msg_type=MessageType.TASK_ASSIGNED, from_actor="parent",
                                    to_actor=agent_id, body=reason or task.title,
                                    task_id=task_id, correlation_id=correlation_id,
                                    payload={"owner": agent_id, "role": task.role,
                                             "produces": list(task.produces),
                                             "consumes": list(task.consumes)}))
        self.kernel.registry.version += 1
        return True

    # ----------------------------------------------------------- spawn vetoes
    #
    # Phase 2 splits the old single function into three explicitly separate phases, because the
    # ordering IS the safety property: nothing may mutate the graph or create an agent until the
    # request has been recorded, deduplicated and evaluated, and the graph must be validated
    # BEFORE an agent exists to own the new task.
    #
    #   intake(req)   -> SpawnRequest + ledger entry + a journalled RECEIVED row (no mutation)
    #   evaluate(req) -> Decision (pure read of registry/graph/budget)
    #   commit(req,d) -> graph.amend() first, then reuse-or-spawn, all journalled
    #
    # `decide_spawn()` keeps its name and signature and is simply the three phases in order, so
    # every Phase-1 call site (and the chaos suite) exercises the new path unchanged.

    #: Phase-2 canonical rule names -> the Phase-1 names they replaced. Kept so `arena why`
    #: output from a Phase-1 journal still reads correctly after a resume.
    LEGACY_RULE_NAMES = {
        "APPROVED": "APPROVE",
        "MAX_ACTIVE_AGENTS": "REJECT_CAP",
        "SPAWN_CHURN": "REJECT_CAP",
        "COVERED_BY_EXISTING": "REJECT_DUPLICATE_CAPABILITY",
        "NOT_WORTH_IT": "REJECT_NOT_WORTH_IT",
        "MAX_SPAWN_EPOCH": "REJECT_SPAWN_DEPTH",
    }

    def _canonical(self, rule: str) -> str:
        return self.LEGACY_RULE_NAMES.get(rule, rule)

    def _as_request(self, req: Any) -> SpawnRequest:
        """Accept a SpawnRequest, a bus Message, or a legacy payload dict.

        Normalisation lives here rather than only in `from_message`, because the Phase-1 enqueue
        path (actor.py -> `parent.spawn_requests.append({...})`) builds a bare dict and never
        passes through `from_message`. One funnel, one set of defaults: inferable fields are filled,
        then `validate()` hard-rejects only the fields a runtime cannot guess.
        """
        if isinstance(req, SpawnRequest):
            req.normalise()
            req.validate()
            return req
        if isinstance(req, Message):
            return SpawnRequest.from_message(req)
        payload = dict(req)
        # legacy dicts carry the message inside, or its fields flat
        msg = payload.pop("msg", None)
        if isinstance(msg, Message):
            r = SpawnRequest.from_message(msg, requester=payload.get("from", "") or "")
            for k, v in payload.items():
                if v not in (None, "", []) and getattr(r, k, None) in (None, "", ()):
                    setattr(r, k, v)
            return r
        payload.setdefault("correlation_id", payload.get("mid") or f"legacy-{id(req):x}")
        r = SpawnRequest.from_message(
            Message(msg_type=MessageType.SPAWN_AGENT_REQUEST,
                    from_actor=payload.pop("from", "") or "",
                    body=payload.get("reason") or payload.pop("body", "") or "",
                    task_id=payload.get("task_id"),
                    correlation_id=payload.get("correlation_id", ""),
                    mid=payload.get("mid", ""),
                    payload=payload))
        r.normalise()
        return r

    # ------------------------------------------------------------------ intake
    def intake_spawn_request(self, req: Any, *, tick: int | None = None) -> SpawnRequest | None:
        """Record and dedup. NO mutation happens here - a rejected request leaves the graph,
        the registry and the lifecycle table exactly as they were.

        Returns None when the request was refused at intake (malformed or duplicate), having
        journalled why. Returns the SpawnRequest when it is safe to evaluate.
        """
        st = self.spawn_stats
        st["received"] += 1
        try:
            r = self._as_request(req)
        except MalformedRequest as e:
            # Loud, not silent: a half-built request is a refused request with a reason.
            self.kernel.journal.emit(MessageType.SPAWN_REJECTED, "parent",
                                     getattr(req, "from_actor", "") or str(req)[:32],
                                     body=f"malformed spawn request: {e}", rule="REJECT_MALFORMED",
                                     correlation_id=getattr(req, "correlation_id", "") or "")
            st["rejected"] += 1
            self.decisions.append({"at": round(self.kernel.now, 3), "tick": self.kernel.tick,
                                   "rid": "", "from": getattr(req, "from_actor", ""),
                                   "role": "", "ok": False, "rule": "REJECT_MALFORMED",
                                   "detail": str(e), "owner": None})
            return None
        inferred = r.normalise()
        r.capability_class = (r.capability_class or "general")
        if r.capability_class == "general":
            r.capability_class = self.catalog.class_for(r.requested_role, r.required_skills)
        r.at_tick = self.kernel.tick if tick is None else tick
        r.created_at = self.kernel.now
        if not r.correlation_id:
            r.correlation_id = r.mid or f"spawn-{r.fingerprint()[:8]}"

        entry = self.ledger.open(r)
        self.kernel.journal.emit(
            MessageType.SPAWN_REQUEST_RECEIVED, "parent", "parent",
            body=f"{r.requester_agent_id or '?'} asks for a {r.requested_role}",
            rid=r.rid, correlation_id=r.correlation_id, task_id=r.parent_task_id,
            requester=r.requester_agent_id, requested_role=r.requested_role,
            capability_class=r.capability_class, estimated_work=r.estimated_work,
            fingerprint=r.fingerprint(), required_skills=list(r.required_skills),
            required_inputs=list(r.required_inputs),
            expected_outputs=list(r.expected_outputs), at_tick=r.at_tick,
            requires_judgment=r.requires_judgment, inferred_fields=inferred)

        # dedup: identical capability already decided, or already being decided
        inflight = self.ledger.inflight_for(r.fingerprint())
        if inflight is not None and inflight.rid != r.rid:
            self.ledger.close_as(r.rid, RequestState.DEDUPLICATED, "DEDUPLICATE",
                                 f"in flight as {inflight.rid}")
            self.kernel.journal.emit(MessageType.SPAWN_REJECTED, "parent", r.requester_agent_id,
                                     body=f"identical request {inflight.rid} is already being "
                                          f"evaluated; not queueing a second one",
                                     rule="DEDUPLICATE", correlation_id=r.correlation_id,
                                     rid=r.rid, duplicate_of=inflight.rid,
                                     requested_role=r.requested_role)
            st["deduplicated"] += 1
            return None
        prior = self.ledger.resolved_for(r.fingerprint())
        if prior is not None and prior.rid != r.rid:
            why = (f"already resolved as {prior.rid} ({prior.state})"
                    + (f", owner {prior.owner}" if prior.owner else ""))
            self.ledger.close_as(r.rid, RequestState.DEDUPLICATED, "DEDUPLICATE", why)
            self.kernel.journal.emit(MessageType.SPAWN_REJECTED, "parent", r.requester_agent_id,
                                     body=why, rule="DEDUPLICATE", correlation_id=r.correlation_id,
                                     rid=r.rid, duplicate_of=prior.rid,
                                     requested_role=r.requested_role)
            st["deduplicated"] += 1
            return None
        self.ledger.move(r.rid, RequestState.EVALUATING, "queued for evaluation")
        return r

    # ---------------------------------------------------------------- evaluate
    def evaluate_spawn(self, req: Any) -> Decision:
        """Ordered vetoes. Pure read: registry, graph and budget only.

        Cheapest rejection first, and *capability support* is checked before duplicate coverage -
        asking for something the host cannot staff is not the same event as asking for something a
        colleague could do, even when both end in "no".
        """
        r = req if isinstance(req, SpawnRequest) else self._as_request(req)
        reg, graph = self.kernel.registry, self.kernel.graph
        b = reg.budget
        requester = reg.get(r.requester_agent_id) if r.requester_agent_id else None

        if requester is not None and requester.epoch >= b.max_spawn_epoch:
            return Decision(False, "REJECT_SPAWN_DEPTH",
                            f"{r.requester_agent_id} is at spawn epoch {requester.epoch} >= "
                            f"{b.max_spawn_epoch}; only the parent may add agents below this depth")
        if len(reg.active()) >= b.max_active_agents:
            return Decision(False, "REJECT_CAP",
                            f"registry is at {len(reg.active())}/{b.max_active_agents}")
        total_spawns = sum(1 for _ in self.kernel.journal.events(
            etype=MessageType.AGENT_REGISTERED))
        if total_spawns > b.max_active_agents * 2:
            return Decision(False, "REJECT_CAP",
                            f"{total_spawns} registrations for a {b.max_active_agents}-agent arena "
                            f"looks like churn, not scaling")
        if r.capability_class and r.capability_class not in self.catalog.serviceable:
            why = self.catalog.reason_for_unserviceable(r.capability_class)
            return Decision(False, "REJECT_UNSUPPORTED",
                            f"capability class '{r.capability_class}' cannot be staffed in this "
                            f"arena" + (f": {why}" if why else ""))
        covered = reg.cover(r.requested_role, r.required_skills)
        if covered:
            return Decision(False, "REJECT_DUPLICATE_CAPABILITY",
                            f"{covered[0].agent_id} ({covered[0].role}) already covers "
                            f"'{r.requested_role}'; assign to it instead of spawning",
                            owner=covered[0].agent_id)
        remaining = graph.remaining_work() or r.estimated_work or 1.0
        if r.estimated_work and r.estimated_work < b.min_share_of_remaining * remaining:
            return Decision(False, "REJECT_NOT_WORTH_IT",
                            f"est work {r.estimated_work} is < {b.min_share_of_remaining:.0%} of "
                            f"remaining {remaining:.1f}; existing capacity absorbs this")
        if r.requester_agent_id and reg.overloaded(r.requester_agent_id):
            return Decision(False, "REJECT_REQUESTER_OVERLOADED",
                            f"{r.requester_agent_id} has more open work than it has completed; it "
                            f"must descope or delegate to a peer, not add headcount")
        if r.requires_judgment:
            return Decision(False, "ESCALATE",
                            f"marginal call for '{r.requested_role}': the heuristics do not "
                            f"clearly favour spawning, and the requester flagged it as needing "
                            f"judgement, so the cortex decides")
        return Decision(True, "APPROVE",
                        f"no coverage for '{r.requested_role}', work estimate "
                        f"{r.estimated_work} clears the bar, registry has room")

    # ------------------------------------------------------------------ commit
    def commit_spawn(self, r: SpawnRequest, d: Decision) -> Decision:
        """Apply a decision. Graph first, agent second, every outcome journalled."""
        st, cid, graph = self.spawn_stats, r.correlation_id, self.kernel.graph
        rid = r.rid

        if d.rule == "ESCALATE":
            self.ledger.move(rid, RequestState.ESCALATED, d.detail, rule="ESCALATE")
            self.escalated.add(rid)
            note = {"agent": r.requester_agent_id, "task_id": r.parent_task_id, "kind": "spawn",
                    "reason": d.detail, "role": r.requested_role, "rid": rid,
                    "at": round(self.kernel.now, 3), "capability_class": r.capability_class}
            self.inbox.append(note)
            self.write_inbox(note)
            self.kernel.journal.emit(MessageType.SPAWN_ESCALATED, "parent", r.requester_agent_id,
                                     body=d.detail, rule="ESCALATE", correlation_id=cid, rid=rid,
                                     requested_role=r.requested_role)
            st["escalated"] += 1
            # The org does not stop for this: the run continues, and the answer arrives on a later
            # tick via apply_arena_decisions -> honour_pending_escalations().
            return d

        if not d.ok:
            # Reuse is a rejection of the SPAWN, not of the WORK: the task still gets created and
            # handed to the agent who can already do it. That distinction is what the spec asks for
            # ("existing capacity must be checked first", "record WHY it was reused").
            if d.rule == "REJECT_DUPLICATE_CAPABILITY" and d.owner:
                return self._reroute(r, d)
            self.ledger.close_as(rid, RequestState.REJECTED, d.rule, d.detail)
            self._record(r, d)          # a refusal belongs in the decision log just as loudly
            self.kernel.journal.emit(MessageType.SPAWN_REJECTED, "parent", r.requester_agent_id,
                                     body=d.detail, rule=d.rule, correlation_id=cid, rid=rid,
                                     requested_role=r.requested_role, owner=d.owner)
            st["rejected"] += 1
            if r.requester_agent_id:
                self.kernel.publish(Message(msg_type=MessageType.REQUEST_DECLINED,
                                           from_actor="parent", to_actor=r.requester_agent_id,
                                           body=f"no new agent: {d.detail}", correlation_id=cid,
                                           payload={"rule": d.rule, "rid": rid,
                                                    "owner": d.owner, "do_it_yourself": bool(d.owner)}))
            return d

        spec = r.to_task_spec()
        added_deps: dict[str, list[str]] = {}
        if spec.task_id in graph.tasks:
            # same fingerprint, different request: reuse the task, do not re-add it
            self.kernel.journal.emit(MessageType.SPAWN_REQUEST_RESOLVED, "parent", "parent",
                                     body=f"{spec.task_id} already exists; linking request {rid}",
                                     rid=rid, task_id=spec.task_id, rule="REUSE_TASK",
                                     correlation_id=cid)
        else:
            # ---- THE GATE (Phase-1 defect D1): validate the mutation before anyone exists ----
            # amend() = snapshot -> add -> validate -> wire edges -> validate -> restore on any
            # CycleError. required_inputs have already been mapped onto consumes by to_task_spec(),
            # so derive_edges() supplies the real dependencies; new_deps stays empty on purpose.
            res = graph.amend([spec], {})
            if not res.get("ok", True):
                self.ledger.close_as(rid, RequestState.REJECTED, "REJECT_CYCLE",
                                     f"amendment rejected: {res.get('cycle')}")
                self.kernel.journal.emit(MessageType.GRAPH_AMEND_REJECTED, "parent", "parent",
                                         body=f"spawn-driven amendment for {spec.task_id} would "
                                              f"create a cycle: {res.get('cycle')}",
                                         rule="REJECT_CYCLE", correlation_id=cid, rid=rid,
                                         added_tasks=[spec.snapshot()], restored=res.get("restored"))
                st["cycles_rejected"] += 1
                self.kernel.journal.emit(MessageType.SPAWN_REJECTED, "parent", r.requester_agent_id,
                                         body="your request would create a dependency cycle; the "
                                              "graph was rolled back and no agent was created",
                                         rule="REJECT_CYCLE", correlation_id=cid, rid=rid,
                                         requested_role=r.requested_role)
                st["rejected"] += 1
                d = Decision(False, "REJECT_CYCLE", f"cycle: {res.get('cycle')}")
                self._record(r, d)
                return d
            deps_after = sorted(graph.tasks[spec.task_id].deps)
            if deps_after:
                added_deps = {spec.task_id: deps_after}
            self.kernel.journal.emit(MessageType.GRAPH_AMENDED, "parent", "parent",
                                     body=f"graph amended by {r.requester_agent_id}: "
                                          f"+{spec.task_id} (re-topologically sorted, "
                                          f"deps={deps_after})",
                                     added_tasks=[dict(spec.snapshot(),
                                                       claims=list(spec.claims),
                                                       skills=list(spec.skills),
                                                       consumes=list(spec.consumes))],
                                     added_deps=added_deps,
                                     order_after=[list(g) for g in graph.order()],
                                     rid=rid, correlation_id=cid, requested_role=r.requested_role)

        # ---- agent second, and never on a silent failure ----
        epoch = (self.kernel.registry.get(r.requester_agent_id).epoch + 1
                 if r.requester_agent_id in self.kernel.registry.agents else 1)
        aid = self._spawn(role=r.requested_role, skills=list(r.required_skills),
                          reason=r.reason, epoch=epoch,
                          spawned_by=r.requester_agent_id or "parent")
        if aid is None:
            # The task is legitimately in the graph now, but unowned. That is recoverable: the
            # scheduler picks it up when capacity frees. What must NOT happen is assigning work to
            # a name that does not exist (the old `aid or role` produced owner "None").
            self.ledger.move(rid, RequestState.DEFERRED, "capacity freed later",
                             rule="DEFER_FOR_CAPACITY")
            # the task survives the refusal, unowned, so a resumed kernel still sees the work
            self.ledger.entries[rid].task_id = spec.task_id
            self.kernel.journal.emit(MessageType.DEFERRED_FOR_CAPACITY, "parent",
                                     r.requester_agent_id,
                                     body=f"{spec.task_id} created but no agent slot; left pending "
                                          f"for the next scheduling pass",
                                     rule="DEFER_FOR_CAPACITY", correlation_id=cid, rid=rid,
                                     task_id=spec.task_id, deferred_task_id=spec.task_id,
                                     requested_role=r.requested_role)
            st["deferred"] += 1
            d = Decision(False, "DEFER_FOR_CAPACITY",
                         f"approved but {len(self.kernel.registry.active())}/"
                         f"{self.kernel.registry.budget.max_active_agents} slots busy")
            self._record(r, d)
            return d

        self.assign(spec.task_id, aid, reason=r.reason, correlation_id=cid)
        entry = self.ledger.entries.get(rid)
        if entry is not None:
            entry.spawned_agent_id, entry.task_id = aid, spec.task_id
        self.ledger.move(rid, RequestState.APPROVED_COMMITTED, f"{aid} <- {spec.task_id}",
                         rule="APPROVE")
        self.kernel.journal.emit(MessageType.SPAWN_APPROVED, "parent", aid,
                                 body=f"spawned {aid} for {r.requester_agent_id or 'parent'} "
                                      f"(epoch {epoch})", agent_id=aid, task_id=spec.task_id,
                                 approved_task_id=spec.task_id,
                                 rid=rid, correlation_id=cid, epoch=epoch,
                                 spawned_by=r.requester_agent_id or "parent",
                                 rule="APPROVE", requested_role=r.requested_role)
        st["approved"] += 1
        if r.requester_agent_id:
            st["spawned_by_agents"] += 1
            self.kernel.publish(Message(msg_type=MessageType.API_CONTRACT_READY,
                                       from_actor="parent", to_actor=r.requester_agent_id,
                                       body=f"{aid} owns {spec.task_id} now",
                                       correlation_id=cid,
                                       payload={"agent_id": aid, "task_id": spec.task_id,
                                                "rid": rid,
                                                "expected_outputs": list(r.expected_outputs)}))
        self._record(r, Decision(True, "APPROVE", f"{aid} <- {spec.task_id}"))
        self.kernel.wake_ready(f"spawn of {aid}")
        return d

    def _reroute(self, r: SpawnRequest, d: Decision) -> Decision:
        """Reuse instead of spawn: the capability exists, so only the *task* is new."""
        st, graph = self.spawn_stats, self.kernel.graph
        spec = r.to_task_spec()
        if spec.task_id not in graph.tasks:
            res = graph.amend([spec], {})
            if not res.get("ok", True):
                self.ledger.close_as(r.rid, RequestState.REJECTED, "REJECT_CYCLE",
                                     f"reuse amendment rejected: {res.get('cycle')}")
                self.kernel.journal.emit(MessageType.GRAPH_AMEND_REJECTED, "parent", "parent",
                                         body=f"reuse amendment would cycle: {res.get('cycle')}",
                                         rule="REJECT_CYCLE", correlation_id=r.correlation_id,
                                         rid=r.rid)
                st["cycles_rejected"] += 1
                return Decision(False, "REJECT_CYCLE", f"cycle: {res.get('cycle')}")
        ok = self.assign(spec.task_id, d.owner, reason=f"reuse: {d.detail}",
                         correlation_id=r.correlation_id)
        entry = self.ledger.entries.get(r.rid)
        if entry is not None:
            entry.owner, entry.task_id = d.owner, spec.task_id
        self.ledger.move(r.rid, RequestState.REROUTED, f"{d.owner} takes {spec.task_id}",
                         rule="REUSE_EXISTING")
        self.kernel.journal.emit(
            MessageType.REQUEST_REROUTED, "parent", d.owner,
            body=f"no agent spawned: {d.owner} already covers '{r.requested_role}'; "
                 f"{spec.task_id} assigned to it (why: {d.detail})",
            rid=r.rid, owner=d.owner, task_id=spec.task_id, rerouted_task_id=spec.task_id,
            rule="REJECT_DUPLICATE_CAPABILITY",
            correlation_id=r.correlation_id, reused=True, agent_spawned=False,
            requested_role=r.requested_role)
        st["reused"] += 1
        d = Decision(True, "REUSE_EXISTING", f"{d.owner} <- {spec.task_id}", owner=d.owner)
        d.ok = ok or d.ok
        self._record(r, d)
        self.kernel.wake_ready(f"reroute to {d.owner}")
        return d

    def _record(self, r: SpawnRequest, d: Decision) -> None:
        self.decisions.append({"at": round(self.kernel.now, 3), "tick": r.at_tick,
                               "rid": r.rid, "from": r.requester_agent_id,
                               "role": r.requested_role, "task_id": r.parent_task_id,
                               "capability_class": r.capability_class, **d.to_dict()})
        self.kernel.journal.emit(MessageType.SPAWN_REQUEST_RESOLVED, "parent",
                                 r.requester_agent_id, body=d.detail, rule=d.rule,
                                 ok=d.ok, rid=r.rid, owner=d.owner,
                                 correlation_id=r.correlation_id, task_id=r.parent_task_id)

    # ------------------------------------------------------------- entry point
    def decide_spawn(self, req: Any) -> Decision:
        """Receipt -> vetoes -> commit. The three phases exist so that a rejection cannot leave a
        half-mutated graph or an orphan agent behind."""
        r = self.intake_spawn_request(req)
        if r is None:
            return Decision(False, "REJECT_INTAKE",
                            "refused at intake (duplicate or malformed); see SPAWN_REJECTED")
        d = self.evaluate_spawn(r)
        out = self.commit_spawn(r, d)
        self.kernel.registry.version += 1
        return out

    def honour_pending_escalations(self) -> list[Decision]:
        """Answer requests parked by ESCALATE, once a decision exists.

        Called from apply_arena_decisions (i.e. at the top of every tick), so a human or an
        upstream cortex writing one line to var/decisions.jsonl is enough to resume a decision the
        heuristics refused to make. The request keeps its rid, so the receipt, the answer and the
        resulting mutation are one traceable chain rather than a fresh request.
        """
        out: list[Decision] = []
        for rid in sorted(self.escalated):
            entry = self.ledger.entries.get(rid)
            if entry is None or entry.state is not RequestState.ESCALATED:
                self.escalated.discard(rid)
                continue
            decided = [d for d in self.decisions
                       if d.get("rid") == rid and d.get("rule") in ("APPROVE", "REJECT_CAP")]
            approved = next((d for d in decided if d.get("rule") == "APPROVE"), None)
            if approved is None:
                continue
            self.escalated.discard(rid)
            # DEFERRED is the only terminal-ish state with a way back, so route through it: an
            # escalation that was answered is a re-evaluation, not a new decision from scratch.
            self.ledger.move(rid, RequestState.DEFERRED, "answered by the arena cortex",
                             strict=False)
            self.ledger.move(rid, RequestState.EVALUATING, "re-committed after escalation")
            d = self.commit_spawn(entry.request,
                                  Decision(True, "APPROVE",
                                           "escalation answered in the affirmative"))
            out.append(d)
        return out

    def recount_spawn_stats(self) -> dict[str, int]:
        """Rebuild the monitoring counters from the ledger (which is itself a projection of the
        journal). Deliberately derived rather than journalled as a separate number: two sources for
        'how many spawns were approved' is how Phase 1 got its replay-divergence bug."""
        out = {"received": 0, "approved": 0, "rejected": 0, "deduplicated": 0, "escalated": 0,
               "deferred": 0, "reused": 0, "spawned_by_agents": 0, "cycles_rejected": 0}
        for e in self.ledger.entries.values():
            out["received"] += 1
            st = str(e.state)
            if st == "APPROVED_COMMITTED":
                out["approved"] += 1
                if e.request.requester_agent_id:
                    out["spawned_by_agents"] += 1
            elif st == "REROUTED":
                out["reused"] += 1
            elif st == "REJECTED":
                out["rejected"] += 1
            elif st == "DEDUPLICATED":
                out["deduplicated"] += 1
            elif st == "ESCALATED":
                out["escalated"] += 1
            elif st == "DEFERRED":
                out["deferred"] += 1
            if e.rule == "REJECT_CYCLE":
                out["cycles_rejected"] += 1
        self.spawn_stats = out
        return out

    def drain_spawn_requests(self) -> list[Decision]:
        out = []
        while self.spawn_requests:
            out.append(self.decide_spawn(self.spawn_requests.pop(0)))
        return out

    # ------------------------------------------------ §9 autonomous feature requests
    def request_feature(self, *, requester: str, target: str, feature: str,
                        required_artifacts: list[str] | None = None,
                        skills: list[str] | None = None, est_work: float = 1.0,
                        role: str | None = None) -> Message:
        """An agent asking a peer for something it does not have. No parent involvement unless the
        peer escalates - which is what makes this 'autonomous agent-to-agent' rather than a chat."""
        msg = Message(msg_type=MessageType.FEATURE_REQUEST, from_actor=requester, to_actor=target,
                      body=f"need '{feature}' to finish {requester}",
                      payload={"feature": feature, "required_artifacts": required_artifacts or [],
                               "skills": skills or [], "est_work": est_work,
                               "role": role or str(feature).replace(" ", "_"),
                               "consumes": []})
        self.kernel.publish(msg)
        return msg

    def resolve_amendments(self) -> list[dict[str, Any]]:
        """Options A / B / C from §9, decided by the *receiving* agent's own capability.

        A: target can do it    -> implement directly (new task, claim, assign, graph revalidated)
        B: target cannot do it -> decline with a reason (requests are never silently dropped)
        C: target is asleep    -> escalate to the Parent, which may spawn a specialist
        """
        reg = self.kernel.registry
        out: list[dict[str, Any]] = []
        while self.feature_requests:
            fr = self.feature_requests.pop(0)
            target, payload = fr["to"], (fr["payload"] or {})
            feature = payload.get("feature", "unnamed")
            need = set(payload.get("skills") or [])
            art = (payload.get("required_artifacts") or [None])[0]
            res: dict[str, Any] = {"feature": feature, "from": fr["from"], "to": target,
                                   "correlation_id": fr.get("correlation_id", "")}
            trec = reg.get(target)
            if trec is None:  # Option C: nobody can service it, the Parent must decide
                res.update(option="C", why=f"unknown agent {target}; parent must decide")
                self.inbox.append({"agent": fr["from"], "reason": f"feature '{feature}' addressed "
                                   f"to unknown agent {target}", "kind": "unknown_target",
                                   "at": self.kernel.now})
                self.parent_note(res)
                self.kernel.journal.emit(MessageType.HELP_REQUEST, "parent", "parent",
                                         body=res["why"], correlation_id=res["correlation_id"])
                out.append(res)
                continue
            if art and self.kernel.artifact_exists(art):  # already delivered - cheap, no new work
                res.update(option="A", why=f"{art} already published "
                           f"v{self.kernel.artifacts[art]['version']}")
                self.kernel.publish(Message(msg_type=MessageType.API_CONTRACT_READY,
                                            from_actor=target, to_actor=fr["from"],
                                            body=f"{art} is available", resource=art,
                                            correlation_id=fr["correlation_id"]))
                out.append(res)
                continue
            if trec.lifecycle.state in (AgentState.WAITING_FOR_DEPENDENCY, AgentState.BLOCKED,
                                        AgentState.ESCALATED, AgentState.PAUSED):
                res.update(option="C", why=f"{target} is {trec.state}; escalated to parent")
                self.inbox.append({"agent": target, "task_id": trec.task_id,
                                   "reason": f"FEATURE_REQUEST '{feature}' arrived while "
                                             f"{trec.state}", "kind": "feature_while_sleeping",
                                   "at": self.kernel.now})
                self.parent_note(res)
                self.kernel.journal.emit(MessageType.HELP_REQUEST, target, "parent",
                                         body=f"cannot service {fr['from']}'s request while "
                                              f"{trec.state}", correlation_id=fr["correlation_id"])
                self.escalations.append(res)
                out.append(res)
                continue
            have = AgentRegistry._tokens(trec.role, trec.skills)
            if need and not (need & have):
                res.update(option="B", why=f"{target} skills {sorted(have)} do not cover "
                                           f"{sorted(need)}: out of scope")
                self.kernel.publish(Message(msg_type=MessageType.REQUEST_DECLINED,
                                            from_actor=target, to_actor=fr["from"], body=res["why"],
                                            correlation_id=fr["correlation_id"],
                                            payload={"feature": feature}))
                out.append(res)
                continue
            tid = f"t_feat_{str(feature).replace(' ', '_')}_{fr.get('mid', 'x')[-4:]}"
            new = {"task_id": tid, "title": f"{feature} (requested by {fr['from']})",
                   "role": trec.role, "skills": sorted(have | need),
                   "est_work": float(payload.get("est_work") or 1.0),
                   "produces": list(payload.get("required_artifacts") or [f"docs/{tid}.md"]),
                   "consumes": list(payload.get("consumes") or []), "claims": [f"feature:{feature}"]}
            amend = self.kernel.graph.amend(
                [TaskSpec(**{k: v for k, v in new.items() if k != "task_id"}, task_id=tid)],
                payload.get("deps"))
            if not amend["ok"]:
                res.update(option="B", why=f"would create a cycle {amend['cycle']}; rolled back",
                           cycle=[str(x) for x in amend["cycle"]])
                self.kernel.publish(Message(msg_type=MessageType.REQUEST_DECLINED,
                                            from_actor=target, to_actor=fr["from"], body=res["why"],
                                            correlation_id=fr["correlation_id"]))
                out.append(res)
                continue
            self.assign(tid, target, reason=f"feature request from {fr['from']}",
                        correlation_id=fr["correlation_id"])
            self.kernel.publish(Message(msg_type=MessageType.REQUEST_ACK, from_actor=target,
                                        to_actor=fr["from"],
                                        body=f"{feature}: accepted as {tid}",
                                        correlation_id=fr["correlation_id"],
                                        payload={"task_id": tid}))
            res.update(option="A", why=f"accepted as {tid} on {target}", task_id=tid)
            out.append(res)
        return out

    def parent_note(self, res: dict[str, Any]) -> None:
        self.kernel.journal.emit(MessageType.STATUS_UPDATE, "parent", "parent",
                                 body=f"feature request '{res.get('feature')}' -> "
                                      f"option {res.get('option')}: {res.get('why')}",
                                 correlation_id=res.get("correlation_id", ""),
                                 option=res.get("option"))

    # ---------------------------------------- §2 lifecycle control: pause / resume / terminate
    def pause(self, agent_id: str, reason: str = "paused by parent") -> bool:
        """Park an agent. Its mailbox is preserved; a paused agent is never scheduled onto a slot."""
        rec = self.kernel.registry.get(agent_id)
        if rec is None:
            return False
        ok = self.kernel.transition(agent_id, AgentState.PAUSED, reason)
        if ok:
            self.kernel.journal.emit(MessageType.AGENT_PAUSED, "parent", agent_id,
                                     body=reason, agent_id=agent_id, task_id=rec.task_id)
        return ok

    def resume(self, agent_id: str, reason: str = "resumed by parent") -> bool:
        """Restore a paused agent to IDLE so the scheduler can re-admit it."""
        rec = self.kernel.registry.get(agent_id)
        if rec is None:
            return False
        ok = self.kernel.transition(agent_id, AgentState.IDLE, reason)
        if ok:
            self.kernel.journal.emit(MessageType.AGENT_RESUMED, "parent", agent_id,
                                     body=reason, agent_id=agent_id, task_id=rec.task_id)
        return ok

    def terminate_agent(self, agent_id: str, reason: str = "terminated by parent") -> bool:
        """Reclaim an agent. Its unfinished work is requeued, not lost."""
        reg = self.kernel.registry
        rec = reg.get(agent_id)
        if rec is None:
            return False
        requeued: list[str] = []
        for tid in ([rec.task_id] if rec.task_id else []) + list(rec.task_queue):
            t = self.kernel.graph.tasks.get(tid)
            if t is not None and t.is_open:
                t.owner, t.status = None, "pending"
                requeued.append(tid)
        self.kernel.transition(agent_id, AgentState.TERMINATED, reason)
        emit_terminated(self.kernel.journal, rec, reason)
        reg.terminate(agent_id, reason)
        self.kernel.actors.pop(agent_id, None)
        self.kernel.queues.pop(agent_id, None)
        if requeued:
            self.kernel.journal.emit(MessageType.STATUS_UPDATE, "parent", "parent",
                                     body=f"{agent_id} removed; {len(requeued)} task(s) requeued",
                                     requeued=requeued, agent_id=agent_id)
        return True

    # ------------------------------------------------------------- supervision
    def schedule(self) -> list[tuple[str, str]]:
        """Assign work only to tasks that are (a) still pending and (b) unowned.

        Owning an open task already is enough - readiness is decided by the *agent* via a durable
        wait, not by the scheduler refusing to hand it the task. That is what lets a task with an
        unmet dependency be legitimately owned while parked: the agent registers the wait, the
        publisher wakes it, and nobody polls in between.
        """
        reg, graph = self.kernel.registry, self.kernel.graph
        assigned: list[tuple[str, str]] = []
        self.drain_spawn_requests()
        self.resolve_amendments()
        unowned = [tid for tid, t in graph.tasks.items()
                   if t.owner is None and t.status == "pending"]
        for tid in unowned:
            task = graph.tasks[tid]
            # least-loaded specialist first: load is backlog length, then steps done
            cand = sorted([a for a in reg.cover(task.role, task.skills)
                           if a.lifecycle.state in (AgentState.IDLE, AgentState.WORKING,
                                                     AgentState.WAITING_FOR_DEPENDENCY)
                           and tid not in a.pending_work],
                          key=lambda a: (a.load, a.work_done, a.agent_id))
            if not cand:
                cand = sorted([a for a in reg.with_state(AgentState.IDLE)
                               if tid not in a.pending_work], key=lambda a: (a.load, a.agent_id))
            if cand:
                if self.assign(tid, cand[0].agent_id, reason="scheduled"):
                    assigned.append((tid, cand[0].agent_id))
            elif not cand and len(reg.active()) < reg.budget.max_active_agents:
                aid = self._spawn(role=task.role, skills=task.skills,
                                  reason=f"no free agent for {tid}", epoch=0,
                                  spawned_by="parent")
                if aid and self.assign(tid, aid, reason="scheduled-after-spawn"):
                    assigned.append((tid, aid))
        return assigned

    def wake_sleeper(self, agent_id: str, reason: str = "") -> bool:
        reg = self.kernel.registry
        rec = reg.get(agent_id)
        if rec is None or rec.task_id is None:
            return False
        if self.kernel.graph.is_ready(rec.task_id):
            return self.kernel.transition(agent_id, AgentState.WORKING, f"resumed: {reason}")
        return False

    def reap_idle(self) -> list[str]:
        reg = self.kernel.registry
        reaped = []
        for rec in reg.idle_overdue(self.kernel.now):
            emit_terminated(self.kernel.journal, rec, f"idle > {reg.budget.idle_ttl}s")
            reg.terminate(rec.agent_id, "idle-ttl")
            reaped.append(rec.agent_id)
        return reaped

    def detect_deadlock(self) -> list[list[str]]:
        cycles = DependencyGraph.find_cycles(self.kernel.registry.wait_for_edges())
        for cyc in cycles:
            self.kernel.journal.emit(MessageType.DEADLOCK_DETECTED, "dependency_manager", "parent",
                                     body="wait-for cycle: " + " -> ".join(cyc),
                                     cycle=cyc, rule="WAIT_FOR_CYCLE")
        return cycles

    def resolve_deadlock(self, victim_rule: str = "lowest_priority") -> list[str]:
        """Break every detected cycle by suspending one agent (never by killing work already done).

        Victim = the last node in the cycle (deterministic: cycles are enumerated from sorted
        nodes), which we force to IDLE and requeue its task so the cycle provably disappears.
        """
        victims: list[str] = []
        for cyc in self.detect_deadlock():
            victim = cyc[-2] if len(cyc) > 1 else cyc[0]
            rec = self.kernel.registry.get(victim)
            if rec is None:
                continue
            self.kernel.transition(victim, AgentState.IDLE, f"deadlock victim ({victim_rule})")
            if rec.task_id and rec.task_id in self.kernel.graph.tasks:
                t = self.kernel.graph.tasks[rec.task_id]
                if t.is_open:
                    t.owner = None
                    t.status = "pending"
                    rec.task_id = None
                    self.kernel.registry.claims.pop(f"task:{rec.task_id}", None) \
                        if hasattr(self.kernel.registry, "claims") else None
            victims.append(victim)
        return victims

    # ----------------------------------------------------------- Arena cortex
    def write_inbox(self, decision: dict[str, Any]) -> None:
        """Escalations land here so the Arena agent (me) can answer between turns, and the
        kernel applies my answer from var/decisions.jsonl on its next run.

        Anchored through the kernel, never the CWD: `Path(root) if root else Path(".")` wrote an
        empty `var/` into whatever directory a test or demo happened to run from.
        """
        import pathlib
        base = getattr(self.kernel, "_side_anchor", None)
        if base is None:
            base = pathlib.Path(self.kernel.root) if str(self.kernel.root or "").strip() not in ("", ".") \
                else None
        if base is None:
            return
        p = base / self.inbox_path
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a") as f:
            f.write(json.dumps(decision, default=str) + "\n")

    def apply_arena_decisions(self) -> list[dict[str, Any]]:
        import pathlib
        base = getattr(self.kernel, "_side_anchor", None)
        if base is None:
            base = pathlib.Path(self.kernel.root) if str(self.kernel.root or "").strip() not in ("", ".") \
                else None
        if base is None:
            return []
        p = base / self.decision_path
        applied = []
        if not p.exists():
            return applied
        consumed: list[str] = []
        for line in p.read_text().splitlines():
            if not line.strip():
                continue
            d = json.loads(line)
            consumed.append(line)
            if d.get("kind") == "approve_spawn":
                # the answer to an ESCALATE: re-decide, honouring whatever the cortex said
                rid = d.get("rid", "")
                entry = self.ledger.entries.get(rid)
                if entry is not None and entry.state is RequestState.ESCALATED:
                    self.decisions.append({"at": round(self.kernel.now, 3), "rid": rid,
                                           "from": d.get("agent", "arena"),
                                           "role": entry.request.requested_role, "ok": True,
                                           "rule": "APPROVE",
                                           "detail": d.get("reason", "approved by the cortex"),
                                           "owner": None})
                else:
                    self.parent_reject_or_spawn(d)

            if d.get("kind") == "amend":
                self.amend(d)
            elif d.get("kind") == "force_state":
                rec = self.kernel.registry.get(d["agent_id"])
                if rec is not None:
                    self.kernel.transition(d["agent_id"], AgentState(d["state"]),
                                           "arena override")
            elif d.get("kind") == "force_terminate":
                rec = self.kernel.registry.get(d["agent_id"])
                if rec is not None:
                    emit_terminated(self.kernel.journal, rec, d.get("reason", "arena"))
                    self.kernel.registry.terminate(d["agent_id"], d.get("reason", "arena"))
            applied.append(d)
        if consumed:
            p.write_text("\n".join(l for l in p.read_text().splitlines() if l not in consumed)
                         + "\n")
        self.honour_pending_escalations()
        return applied

    def parent_reject_or_spawn(self, d: dict[str, Any]) -> None:
        """A cortex decision that is *not* answering a parked request: run it as a normal one."""
        self.decide_spawn({"from": d.get("agent", "parent"),
                           "requested_role": d.get("role", "specialist"),
                           "reason": d.get("reason", "approved by the arena cortex"),
                           "skills": list(d.get("skills") or []),
                           "work_estimate": float(d.get("work_estimate") or 1.0),
                           "produces": list(d.get("produces") or []),
                           "correlation_id": d.get("correlation_id", "")})

    def amend(self, d: dict[str, Any]) -> dict[str, Any]:
        """Apply a graph mutation that came from an agent FEATURE_REQUEST or from me."""
        new_tasks = [TaskSpec(**t) for t in d.get("tasks", [])]
        res = self.kernel.graph.amend(new_tasks, d.get("deps"))
        if res["ok"]:
            self.kernel.journal.emit(MessageType.PLAN_AMENDED, "parent", "parent",
                                     body=f"amend: +{res['added']}",
                                     added_tasks=[t.snapshot() for t in new_tasks],
                                     correlation_id=d.get("correlation_id", ""))
            for t in new_tasks:
                self.spawn_for_plan([t])
        else:
            self.kernel.journal.emit(MessageType.CYCLE_REJECTED, "parent", "parent",
                                     body=f"amend rejected, rolled back {res['rolled_back']}",
                                     cycle=res["cycle"], correlation_id=d.get("correlation_id", ""))
        self.accepted_decisions.append(d)
        return res

    # ------------------------------------------------------------------ report
    def render(self) -> str:
        reg, graph = self.kernel.registry, self.kernel.graph
        w = 60
        lines = ["┌" + "─" * w + "┐",
                 "│" + "PARENT ARENA".center(w) + "│",
                 "├" + "─" * w + "┤"]
        for a in sorted(reg.agents.values(), key=lambda x: x.agent_id):
            st = a.state
            lines.append(f"│ {a.agent_id:<20} {a.role:<16} ● {st:<22}│")
        if not reg.agents:
            lines.append(f"│ {'(no agents spawned yet)':<58}│")
        lines.append("├" + "─" * w + "┤")
        for tid, t in sorted(graph.tasks.items()):
            marks = "  ".join(filter(None, [
                f"owner={t.owner}" if t.owner else "owner=-",
                f"blocked_by={','.join(graph.unmet(tid))}" if graph.unmet(tid) else ""]))
            lines.append(f"│ {tid:<16} {t.status:<9} {marks:<34}│")
        lines.append("└" + "─" * w + "┘")
        return "\n".join(lines)

    def status(self) -> dict[str, Any]:
        reg, graph = self.kernel.registry, self.kernel.graph
        return {"clock": self.kernel.now, "events": self.kernel.journal.count(),
                "agents": {a.agent_id: {"state": a.state, "role": a.role, "task": a.task_id,
                                        "epoch": a.epoch, "waits": [w.get("condition")
                                                                    for w in a.pending_waits]}
                           for a in sorted(reg.agents.values(), key=lambda x: x.agent_id)},
                "tasks": {tid: {"status": t.status, "owner": t.owner, "deps": sorted(t.deps)}
                          for tid, t in sorted(graph.tasks.items())},
                "counts": {"working": len(reg.with_state(AgentState.WORKING)),
                           "waiting": len(reg.with_state(AgentState.WAITING_FOR_DEPENDENCY)),
                           "blocked": len(reg.with_state(AgentState.BLOCKED)),
                           "escalated": len(reg.with_state(AgentState.ESCALATED)),
                           "completed": len(reg.with_state(AgentState.COMPLETED))},
                "bus": self.kernel.bus.snapshot(),
                "escalations": len(self.inbox),
                "critical_path": graph.critical_path_length(),
                "polls": self.kernel.polls.polls,
                # Phase 2: the agent-count monitor rides along on `arena status`, because a
                # separate command is how a reviewer ends up with two answers to one question.
                "spawning": self.kernel.monitoring()}


def _selftest() -> None:  # pragma: no cover
    p = RuleBasedPlanner()
    for text in ("Build a SaaS app with authentication, dashboard, API backend, PostgreSQL "
                 "database, and cloud deployment",
                 "Train an ML model with a data pipeline, model optimization and evaluation"):
        tasks, rat = p.plan(text)
        g = DependencyGraph()
        for t in tasks:
            g.add(t)
        print(f"\n--- {text[:48]}...")
        print("roles:", sorted({t.role for t in tasks}))
        print("order:", g.order())
        print("matched:", rat["matched"])
        print("edges:", rat["artifact_edges"])


if __name__ == "__main__":
    _selftest()
