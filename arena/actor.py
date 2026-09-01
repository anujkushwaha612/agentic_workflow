"""The agent abstraction: a stateful actor with a mailbox, a lifecycle, durable waits and a policy.

Note what is *not* here: no knowledge of other agents' internals. An actor reads its own task,
the graph's public state and its own inbox, and the only way it affects anyone else is by
publishing a Message or completing a task (which the Parent turns into wakes). That is what
makes the "agents never mutate each other's state" property enforceable by construction.
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any, Sequence

from .graph import TaskSpec
from .lifecycle import AgentState

from .message import Message, MessageType
from .policy import Act, Action
from .cognition import (Control, CognitionCapabilityError, Intent, Observation, Outcome,
                        PolicyCognition, ToolCall, outcomes_from_results, validate_intent)
from .tools import ToolResult as ToolResult_


def ctx_task_title(task: Any) -> str:
    return getattr(task, "title", "") or "unspecified work"


@dataclass
class ActorContext:
    """The *entire* surface a policy gets. Deliberately read-only except for publish/wait/escalate.

    llm_available / polls are observable seams: the chaos suite flips them to prove that the
    default path never polls and that a model adapter would actually be consulted if present.
    """

    kernel: Any
    agent_id: str
    msg: Message | None = None
    step_index: int = 0
    llm_available: bool = False
    _log: list[str] = field(default_factory=list)

    # ------------------------------------------------------------- read side
    @property
    def registry(self):
        return self.kernel.registry

    @property
    def rec(self):
        return self.kernel.registry.require(self.agent_id)

    @property
    def task(self) -> TaskSpec | None:
        """The task the actor is gated on: current, else first queued one that is still open."""
        rec = self.rec
        for tid in rec.pending_work:
            t = self.kernel.graph.tasks.get(tid)
            if t is not None and t.is_open:
                return t
        return None

    @property
    def task_id(self) -> str | None:
        t = self.task
        return t.task_id if t else None

    @property
    def state(self) -> str:
        return self.rec.state

    def unmet_artifacts(self) -> list[str]:
        """Artifacts this task consumes that are not yet produced. One lookup - not a re-check."""
        t = self.task
        if t is None:
            return []
        return [a for a in t.consumes if not self.kernel.artifact_exists(a)]

    def unmet_upstream(self) -> list[str]:
        """Artifacts promised by upstream tasks this one depends on. The gate that matters."""
        t = self.task
        if t is None:
            return []
        return sorted({a for a in
                       (x for tid in self.kernel.graph.tasks[t.task_id].deps
                        for x in self.kernel.graph.tasks[tid].produces)
                       if not self.kernel.artifact_exists(a)})

    def all_unmet(self) -> list[str]:
        out, seen = [], set()
        for a in self.unmet_artifacts() + self.unmet_upstream():
            if a not in seen:
                seen.add(a)
                out.append(a)
        return out

    def consumed_ready(self) -> list[str]:
        t = self.task
        if t is None:
            return []
        return [a for a in t.consumes if self.kernel.artifact_exists(a)]

    def deps_open(self) -> list[str]:
        return self.kernel.graph.unmet(self.task_id) if self.task_id else []

    # ------------------------------------------------------------ write side
    def self_msg(self, msg_type: MessageType, body: str = "", to: str = "parent",
                 **payload: Any) -> Message:
        base = self.msg
        if base is not None and payload.pop("inherit", True):
            m = base.child(msg_type, self.agent_id, to, body=body,
                           task_id=payload.pop("task_id", self.task_id))
            m.payload.update(payload)
            return m
        return Message(msg_type=msg_type, from_actor=self.agent_id, to_actor=to, body=body,
                       task_id=payload.pop("task_id", self.task_id), payload=payload)

    def request_specialist(self, role: str, reason: str = "", *,
                           skills: Sequence[str] = (), inputs: Sequence[str] = (),
                           outputs: Sequence[str] = (), est_work: float = 0.0,
                           capability_class: str = "", requires_judgment: bool = False,
                           to: str = "parent") -> Message:
        """The ONLY sanctioned way for an agent to ask for a new colleague (Phase 2).

        It exists so that a request cannot be *shaped* wrong: every field the Parent needs to say
        'yes', 'no', or 'use the agent you already have' is filled here, in the one place, and the
        task/correlation ids are inherited from the message the agent is currently handling. A
        policy that hand-builds a SPAWN_AGENT_REQUEST instead is still allowed - the runtime is not
        a walled garden - but it will get a journalled REJECT_MALFORMED rather than silence, and
        that asymmetry is the whole enforcement story. Nothing here talks to the Parent directly:
        it returns a Message, which becomes an Action.publish, which travels the control plane like
        any other event.
        """
        p: dict[str, Any] = {
            "requested_role": role,
            "reason": reason or f"{role} capability needed for {ctx_task_title(self.task)}",
            "required_skills": list(skills or []),
            "required_inputs": list(inputs or []),
            "expected_outputs": list(outputs or []),
            "estimated_work": float(est_work or 0.0),
            "parent_task_id": self.task_id,
            "capability_class": capability_class or "",
            "requires_judgment": bool(requires_judgment),
        }
        return self.self_msg(MessageType.SPAWN_AGENT_REQUEST, p["reason"], to=to, **p)

    def log(self, text: str) -> None:
        self._log.append(text)
        self.rec.notes.append(f"[{self.kernel.now:.2f}] {text}")

    def drain_log(self) -> list[str]:
        out, self._log = list(self._log), []
        return out


@dataclass
class AgentActor:
    agent_id: str
    policy: Any
    kernel: Any = None
    steps_run: int = 0
    last_action: str = ""
    errors: list[str] = field(default_factory=list)
    # --- Phase 2.5 -----------------------------------------------------------
    #: The cognition source, or None. None is not "no brain": the runtime then uses
    #: `PolicyCognition(self.policy)` - i.e. the deterministic tier, through the *same*
    #: decide(Observation) -> Intent interface a real model will use later. That default is what
    #: lets M0 land with 177 unchanged tests and M2 land without an LLM.
    cognition: Any = None
    #: per-agent and per-instance on purpose. A class-level transcript would be the shared brain
    #: this design is explicitly built to prevent, and `arena status` would report it.
    transcript: list[dict[str, Any]] = field(default_factory=list, init=False)

    def cognition_source(self) -> Any:
        return self.cognition if self.cognition is not None else \
            PolicyCognition(policy=self.policy, agent_id=self.agent_id)

    def _use_cognition(self) -> bool:
        """Only an explicitly bound source takes the new path; a policy-only agent keeps running the
        legacy branch unchanged (so no Phase 1/2 behaviour is re-implemented here)."""
        return self.cognition is not None

    # --------------------------------------------------------------- helpers
    def _transition(self, to: AgentState, reason: str) -> bool:
        ok = self.kernel.transition(self.agent_id, to, reason)
        if not ok:
            self.errors.append(f"illegal: {self.kernel.registry.require(self.agent_id).state} "
                               f"-> {to}")
        return ok

    def policy_cursor(self) -> int:
        """How far the *policy* got inside the current task. Without this a resumed agent would
        repeat work steps that already happened (harmless here, fatal if the step were 'apply
        migration #3')."""
        return int(getattr(self.policy, "_steps", getattr(self.policy, "_worked", 0)) or 0)

    def rec_progress(self) -> None:
        """Persist per-actor progress. A kernel restarted after a sandbox recycle resumes mid-task
        because of this single line; without it every agent would restart from step 0."""
        self.kernel.journal.emit(MessageType.TASK_PROGRESS, self.agent_id, "parent",
                                 body=f"step {self.steps_run}",
                                 task_id=self.kernel.registry.require(self.agent_id).task_id,
                                 steps_run=self.steps_run, policy_cursor=self.policy_cursor(),
                                 last_action=self.last_action)

    def restore_cursor(self, steps_run: int, policy_cursor: int) -> None:
        self.steps_run = int(steps_run or 0)
        for field_name in ("_steps", "_worked", "_n", "_asked"):
            if hasattr(self.policy, field_name):
                setattr(self.policy, field_name, int(policy_cursor or 0))

    # ------------------------------------------------------------------ step
    def run_step(self) -> dict[str, Any]:
        """One actor turn: drain inbox -> policy -> apply action. Returns an audit record."""
        rec = self.kernel.registry.require(self.agent_id)
        inbox = self.kernel.queues.get(self.agent_id, [])
        msg = inbox.pop(0) if inbox else None
        if self._use_cognition():
            return self._run_cognition_step(rec, msg)
        ctx = ActorContext(kernel=self.kernel, agent_id=self.agent_id, msg=msg,
                           step_index=self.steps_run,
                           llm_available=getattr(self.policy, "llm_available", False))
        outcome: dict[str, Any] = {"agent": self.agent_id, "action": "noop", "detail": "",
                                   "msg_in": msg.mid if msg else None}

        try:
            action: Action = self.policy.step(ctx, msg)
        except Exception as e:  # a policy crash must not take the kernel down
            self.errors.append(f"{type(e).__name__}: {e}")
            self.kernel.journal.emit(MessageType.ERROR_REPORT, self.agent_id, "parent",
                                     body=f"policy crashed: {type(e).__name__}: {e}",
                                     task_id=rec.task_id, exception=type(e).__name__)
            if rec.lifecycle.state is not AgentState.TERMINATED:
                self._transition(AgentState.BLOCKED, "policy exception")
            outcome.update(action="error", detail=f"{type(e).__name__}: {e}",
                           log=ctx.drain_log())
            return outcome

        self.steps_run += 1
        self.rec_progress()
        self.last_action = str(action.act)
        outcome.update(action=str(action.act), detail=action.reason or "", log=ctx.drain_log())
        self._apply_action(action, rec, ctx, msg, outcome)
        if rec.lifecycle.state in (AgentState.INITIALIZING, AgentState.CREATED) \
                and rec.pending_work:
            self._transition(AgentState.WORKING, "began task")
        return outcome

    def _apply_action(self, action: Action, rec: Any, ctx: "ActorContext",
                      msg: Message | None, outcome: dict[str, Any]) -> None:
        """One applier for both turns. The legacy and cognition paths differ in *where the action
        came from*; what an action does is a single implementation, or the two would drift (which is
        precisely how Phase 2's D2 bug - last_action casing - changed the tick a request was
        answered on)."""
        if action.act is Act.WAIT:
            self.kernel.bus.wait_for(self.agent_id, action.condition, task_id=rec.task_id,
                                     correlation_id=msg.correlation_id if msg else "",
                                     timeout=action.timeout)
            if rec.lifecycle.state in (AgentState.IDLE, AgentState.WORKING, AgentState.ESCALATED,
                                       AgentState.BLOCKED, AgentState.PAUSED):
                self._transition(AgentState.WAITING_FOR_DEPENDENCY, action.reason or "waiting")
            outcome["detail"] = f"parked on {action.condition}"
        elif action.act is Act.ESCALATE:
            self._transition(AgentState.ESCALATED, action.reason or "escalation")
            note = {"agent": self.agent_id, "task_id": rec.task_id, "kind": "escalation",
                    "reason": action.reason, "extra": action.extra, "at": self.kernel.now}
            self.kernel.parent.inbox.append(note)
            self.kernel.parent.write_inbox(note)
            outcome["detail"] = "escalated to parent"
        elif action.act is Act.COMPLETE:
            self._complete(action, rec, ctx=ctx)
        elif action.act is Act.PUBLISH and action.msg is not None:
            self.kernel.publish(action.msg)
            rec.msgs_sent += 1
            if action.msg.msg_type is MessageType.SPAWN_AGENT_REQUEST:
                self.kernel.parent.spawn_requests.append(
                    {"from": self.agent_id, **action.msg.payload, "task_id": action.msg.task_id,
                     "correlation_id": action.msg.correlation_id, "mid": action.msg.mid})
            outcome["detail"] = f"{action.msg.msg_type} -> {action.msg.to_actor}"
        else:
            rec.work_done += self.kernel.work_unit
            outcome["detail"] = action.reason or "proceeded"


    # ------------------------------------------------------- cognition-driven turn
    def observe(self, ctx: ActorContext, msg: Message | None) -> Observation:
        """What the agent is allowed to know. Explicit fields, plus the read-only context that the
        policy tier still needs; a provider source reads `prompt_seed()` and never touches `ctx`."""
        task = ctx.task
        graph_view = tuple({"task_id": t.task_id, "title": t.title, "role": t.role,
                            "status": t.status, "owner": t.owner,
                            "produces": list(t.produces)}
                           for t in self.kernel.graph.tasks.values())
        recent = tuple(o for o in outcomes_from_results(
            getattr(self, "_last_results", []) or []))
        # full history from the in-memory record (the authoritative one); `transcript` is the
        # on-disk mirror, and re-parsing it would let a write failure change agent behaviour
        history: tuple[Outcome, ...] = tuple(
            outcomes_from_results(getattr(self, "_all_results", []) or []))
        return Observation(
            agent_id=self.agent_id, role=self.kernel.registry.require(self.agent_id).role,
            goal=self.kernel.task_text, task=(task.snapshot() if task is not None else {}),
            message=msg, step_index=self.steps_run,
            unmet=tuple(ctx.all_unmet()), consumed_ready=tuple(ctx.consumed_ready()),
            graph_view=graph_view,
            unread=tuple(self.kernel.queues.get(self.agent_id, [])),
            recent=recent, history=tuple(history),
            workspace=self.kernel.workspace_view(self.agent_id),
            budget={"steps_left": max(0, 400 - self.steps_run),
                    "workers": self.kernel.budget.max_concurrent_workers,
                    "tool_stats": dict(self.kernel.tools.stats) if self.kernel.tools else {}},
            notes=tuple(self.kernel.registry.require(self.agent_id).notes[-3:]),
            state=ctx.state,
            tool_schemas=tuple(self.kernel.tools.registry.schemas()) if self.kernel.tools else (),
            context=ctx, transcript_len=len(self.transcript))

    def _run_cognition_step(self, rec: Any, msg: Message | None) -> dict[str, Any]:
        ctx = ActorContext(kernel=self.kernel, agent_id=self.agent_id, msg=msg,
                           step_index=self.steps_run,
                           llm_available=getattr(self.policy, "llm_available", False))
        source = self.cognition
        outcome: dict[str, Any] = {"agent": self.agent_id, "action": "noop", "detail": "",
                                   "msg_in": msg.mid if msg else None, "calls": []}
        obs = self.observe(ctx, msg)
        # ---- decide ------------------------------------------------------------
        try:
            intent = source.decide(obs)
        except CognitionCapabilityError as e:
            self.errors.append(str(e))
            self.kernel.journal.emit(MessageType.COGNITION_ERROR, self.agent_id, "parent",
                                     body=f"cognition reached outside its seam: {e}",
                                     task_id=rec.task_id, kind="capability")
            self._transition(AgentState.BLOCKED, "cognition capability error")
            outcome.update(action="error", detail=str(e))
            return outcome
        except Exception as e:  # a model that is down, or a policy that crashed
            self.errors.append(f"{type(e).__name__}: {e}")
            self.kernel.journal.emit(MessageType.COGNITION_ERROR, self.agent_id, "parent",
                                     body=f"cognition failed: {type(e).__name__}: {e}",
                                     task_id=rec.task_id, kind="error",
                                     exception=type(e).__name__)
            self._transition(AgentState.BLOCKED, "cognition error")
            outcome.update(action="error", detail=f"{type(e).__name__}: {e}")
            return outcome
        # ---- validate: cognition proposes, the runtime disposes ---------------
        allowed = self.kernel.tools.registry.names() if self.kernel.tools else []
        intent, notes = validate_intent(intent, allowed_tools=allowed)
        if notes:
            for n in notes:
                self.kernel.journal.emit(MessageType.COGNITION_VIOLATION, self.agent_id, "parent",
                                         body=f"intent refused: {n}", code=n,
                                         task_id=rec.task_id, agent_id=self.agent_id,
                                         intent_fingerprint=intent.fingerprint())
        self._record_turn({"intent": intent.to_dict(), "notes": notes,
                           "source": getattr(source, "name", "?")})
        # ---- execute the tool calls -------------------------------------------
        results = []
        if intent.calls and self.kernel.tools is not None:
            rids = self.kernel.tools.plan(self.agent_id,
                                          [c.to_dict() for c in intent.calls],
                                          task_id=rec.task_id,
                                          correlation_id=msg.correlation_id if msg else "")
            for rid, call in zip(rids, intent.calls):
                res = self.kernel.tools.execute(self.agent_id, call.tool, call.args, rid=rid,
                                                task_id=rec.task_id,
                                                correlation_id=msg.correlation_id if msg else "")
                results.append(res)
                self._record_turn({"tool_result": res.as_block(), "rid": rid})
        elif intent.calls:
            for call in intent.calls:
                res = ToolResult_(tool=call.tool, ok=False,
                                  stderr="this kernel has no tool executor bound; "
                                         "Kernel.bind_tools() is required before tools run")
                results.append(res)
                self.kernel.journal.emit(MessageType.TOOL_REFUSED, self.agent_id, "kernel",
                                         body=f"{call.tool}: REFUSE_NO_EXECUTOR",
                                         tool=call.tool, code="REFUSE_NO_EXECUTOR",
                                         agent_id=self.agent_id, task_id=rec.task_id)
        self._all_results = list(getattr(self, "_all_results", [])) + list(results)
        self._last_results = results
        if results:
            outcome["calls"] = [{"tool": r.tool, "ok": r.ok, "exit": r.exit_code,
                                 "refused": r.refused} for r in results]
        # ---- apply the control verb, through the existing action machinery ----
        action = self._intent_to_action(intent, ctx, msg)
        self.steps_run += 1
        self.rec_progress()
        self.last_action = str(action.act)
        outcome.update(action=str(action.act), detail=action.reason or outcome.get("detail", ""))
        self._apply_action(action, rec, ctx, msg, outcome)
        if rec.lifecycle.state in (AgentState.INITIALIZING, AgentState.CREATED) and rec.pending_work:
            self._transition(AgentState.WORKING, "began task")
        return outcome

    def _intent_to_action(self, intent: Intent, ctx: ActorContext, msg: Message | None) -> Action:
        c = intent.control
        if c == Control.WAIT:
            cond = intent.wait_for or (ctx.all_unmet() or [""])[0]
            if not cond:
                return Action(Act.NOOP, reason=intent.reason or "nothing to wait on")
            return Action.wait(f"artifact:{cond}" if ":" not in cond else cond,
                               intent.reason or "waiting")
        if c == Control.ESCALATE:
            return Action.escalate(intent.reason or "escalation")
        if c == Control.COMPLETE:
            arts = list((intent.publish or {}).get("artifacts") or [])
            if arts:
                return Action.complete(intent.reason or "complete", artifacts=arts)
            return Action.complete(intent.reason or "complete")
        if c == Control.VERIFY:
            # VERIFY is not a self-declared state: it re-runs the task's commands through the tool
            # path and *then* completes, so the agent cannot verify by assertion.
            return Action(Act.COMPLETE, reason=intent.reason or "verify then complete",
                          extra={"verify_now": True, "artifacts": [],
                                 "intent_think": intent.think})
        if c == Control.PUBLISH:
            m = (intent.publish or {}).get("msg")
            if m is None:
                m = ctx.self_msg(MessageType.STATUS_UPDATE, intent.reason or "update")
            return Action.publish(m, intent.reason or "publish")
        if c == Control.SPAWN_REQUEST:
            m = (intent.spawn or {}).get("msg")
            if m is None:
                raise CognitionCapabilityError(
                    "SPAWN_REQUEST without a shaped message: use ctx.request_specialist() "
                    "(or arena.cognition's ToolCall('request_specialist', ...)) so every field "
                    "the Parent needs is present")
            return Action.publish(m, intent.reason or "spawn request")
        if c == Control.NOOP:
            return Action(Act.NOOP, reason=intent.reason or "noop")
        if intent.reason:
            return Action.proceed(intent.reason)
        return Action.proceed("worked")

    def _record_turn(self, row: dict[str, Any]) -> None:
        """Per-agent transcript: this agent's own reasoning trail, on disk when configured."""
        row = dict(row, at=round(self.kernel.now, 4), tick=self.kernel.tick,
                   step=self.steps_run, agent_id=self.agent_id)
        self.transcript.append(row)
        d = getattr(self.kernel, "transcript_dir", None)
        if not d:
            return
        try:
            import json
            p = pathlib.Path(d)
            p.mkdir(parents=True, exist_ok=True)
            with (p / f"{self.agent_id}.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, default=str) + "\n")
        except OSError as e:                      # a transcript is evidence, never a dependency
            self.errors.append(f"transcript write failed: {e}")

    # ---------------------------------------------------------------- finish
    def _complete(self, action: Action, rec: Any, ctx: Any = None) -> None:
        task = self.kernel.graph.tasks.get(rec.task_id or "")
        # ---- THE VERIFY GATE (M2). A worker may not talk its way into "done".
        gate = self.kernel.completion_gate(self.agent_id, rec)
        if not gate.get("allow", True):
            ran = (action.extra or {}).get("verify_now")
            verdict = self.kernel.run_verify(self.agent_id, task_id=gate.get("task_id")) \
                if self.kernel.tools is not None else {"ok": False}
            if verdict.get("ok"):
                gate = {"allow": True, "verified": True}
            else:
                self.kernel.journal.emit(
                    MessageType.COMPLETION_REFUSED, self.agent_id, "parent",
                    body=f"{gate.get('rule')}: {gate.get('detail')}"
                         + (" (verify re-run by the runtime and it still failed)" if ran else ""),
                    task_id=gate.get("task_id"), rule=gate.get("rule"), verified=bool(verdict.get("ok")),
                    agent_id=self.agent_id,
                    verify_results=verdict.get("results") or [])
                if ctx is not None:
                    ctx.log("completion refused: verification failed")
                # the refusal is *fed back*, not just recorded: the agent gets a message with the
                # failing output, so "observe -> reason again" is possible inside the same run
                self.kernel.publish(Message(
                    msg_type=MessageType.TASK_PROGRESS, from_actor="parent", to_actor=self.agent_id,
                    body="runtime refused completion: verification did not pass",
                    task_id=gate.get("task_id"),
                    payload={"rule": gate.get("rule"), "percent": 0,
                             "verify": (verdict.get("results") or [])[:2]}))
                if task is not None:
                    task.status = "waiting"
                # WORKING -> IDLE is the legal edge back to "I still have work"; BLOCKED would
                # strand the agent outside the scheduler and turn a fixable failure into a stall.
                self._transition(AgentState.IDLE, "verify failed; work remains")
                return
        if task is not None and getattr(task, "verify", None) and not task.verified:
            # the gate above can only pass once verify succeeded; keep the invariant explicit
            task.verified = True
        if task is None and self.last_action == str(Act.COMPLETE):
            # Completing twice is not an event, it is a bug. Found by Phase 2: once the inbox-drain
            # break started firing (defect D2), an agent could run a second step in the same tick
            # right after finishing, and each finish emitted a *second* `TASK_COMPLETED` row with
            # body "agent finished with no bound task" - journal noise that also corrupted replay
            # counts. A genuinely idle agent with no task still completes normally (its
            # last_action is not COMPLETE), so this refuses only the immediate repeat.
            self.errors.append("ignoring repeated COMPLETE with no bound task")
            return
        if task is None:
            self._transition(AgentState.COMPLETED, "no task bound")
            self.kernel.journal.emit(MessageType.TASK_COMPLETED, self.agent_id, "parent",
                                     body="agent finished with no bound task")
            return
        for a in (action.extra or {}).get("artifacts", []) or list(task.produces):
            self.kernel.publish_artifact(a, producer=self.agent_id, task_id=task.task_id)
        task.status = "done"
        # advance the backlog: the next queued task becomes the current one
        rec.task_queue = [t for t in rec.task_queue if t != task.task_id]
        rec.task_id = rec.task_queue.pop(0) if rec.task_queue else None
        task.finished_at = self.kernel.now
        rec.work_done += task.est_work
        self.kernel.registry.version += 1
        already_done = rec.lifecycle.state is AgentState.COMPLETED
        self.kernel.journal.emit(MessageType.TASK_COMPLETED, self.agent_id, "parent",
                                 body=f"{task.task_id} done", task_id=task.task_id,
                                 artifacts=list(task.produces), owner=self.agent_id)
        if not already_done:      # COMPLETED -> COMPLETED is not an edge; don't journal a no-op
            self._transition(AgentState.COMPLETED, "task done")
        self.kernel.journal.emit(MessageType.STATS, self.agent_id, "parent",
                                 body="task done", agent_id=self.agent_id,
                                 msgs_sent=rec.msgs_sent, work_done=round(rec.work_done, 6),
                                 steps_run=self.steps_run)
        # event-driven wake, not a re-scan: whoever needed these artifacts is resumed now
        for a in task.produces:
            self.kernel.bus.resolve(f"artifact:{a}", reason=f"{task.task_id} published {a}")

    # ------------------------------------------------------------------ loop
    def has_mail(self) -> bool:
        return bool(self.kernel.queues.get(self.agent_id))

    def runnable(self) -> bool:
        """Runnable = in a runnable state with work to do, OR parked but holding mail.

        A COMPLETED agent with a non-empty backlog is runnable again - that is the difference
        between 'finished a task' and 'finished being useful'.
        """
        rec = self.kernel.registry.get(self.agent_id)
        if rec is None:
            return False
        from .kernel import RUNNABLE
        if rec.pending_work and rec.lifecycle.state in (AgentState.COMPLETED, AgentState.IDLE,
                                                         AgentState.INITIALIZING):
            return True
        if rec.lifecycle.state in RUNNABLE and rec.pending_work:
            return True
        return bool(rec.pending_waits) and self.has_mail()

    def run(self, max_steps: int = 12) -> list[dict[str, Any]]:
        out = []
        for _ in range(max_steps):
            rec = self.kernel.registry.get(self.agent_id)
            if rec is None or rec.lifecycle.state in (AgentState.TERMINATED, AgentState.COMPLETED,
                                                      AgentState.WAITING_FOR_DEPENDENCY,
                                                      AgentState.ESCALATED, AgentState.PAUSED,
                                                      AgentState.BLOCKED):
                break
            r = self.run_step()
            out.append(r)
            if r["action"] in ("complete", "wait", "escalate", "error"):
                break
        return out

    def snapshot(self) -> dict[str, Any]:
        rec = self.kernel.registry.require(self.agent_id)
        return {"agent_id": self.agent_id, "state": rec.state, "steps_run": self.steps_run,
                "cognition": dict(rec.cognition or {}),
                "transcript_len": len(self.transcript),
                "workspace": bool(self.kernel.workspaces.get(self.agent_id)),
                "last_action": self.last_action, "errors": list(self.errors),
                "task_id": rec.task_id, "waits": [w.get("condition") for w in rec.pending_waits],
                "policy": type(self.policy).__name__}
