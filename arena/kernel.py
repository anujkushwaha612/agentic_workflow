"""The kernel: composition root. Owns journal, graph, registry, bus, parent, actors and the clock.

Two run modes over the same semantics (this is the answer to 'can a daemon really run between Arena
turns?'):
  run(ticks)         - deterministic burst; virtual clock; used by the chaos suite and CI
  run_resident(...)  - wall-clock loop, yields to sleep, keeps serving; used with the live preview

Both write the identical journal, which is what makes `from_journal()` recovery real rather than
aspirational. `from_journal` is what I would run after the sandbox VM recycles mid-project.
"""
from __future__ import annotations

import contextlib
import pathlib
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .actor import AgentActor
from .bus import Bus, PollCounter
from .clock import make_clock
from .graph import DependencyGraph, TaskSpec
from .journal import Journal
from .lifecycle import AgentState
from .message import Message, MessageType
from typing import Any, Sequence
from .parent import ParentArena, RuleBasedPlanner
from .cognition import Control, Intent, Observation, PolicyCognition, ToolCall, \
    outcomes_from_results, validate_intent
from .policy import SimulatedWork, WaitForArtifacts, is_sleeping_action, make_policy
from .registry import AgentRegistry, SpawnBudget

#: states in which an actor may consume a worker slot this tick (it must also hold open work)
RUNNABLE = (AgentState.IDLE, AgentState.WORKING, AgentState.INITIALIZING, AgentState.COMPLETED)


@dataclass
class Kernel:
    journal_path: str | Path = ":memory:"
    root: str | Path = "."
    planner: Any = None
    budget: SpawnBudget = field(default_factory=SpawnBudget)
    clock_mode: str = "virtual"
    clock_step: float = 0.01
    work_unit: float = 0.25
    default_policy: str = "simulated"
    role_policies: dict[str, str] = field(default_factory=lambda: {
        "frontend": "wait", "backend": "wait", "testing": "wait", "data": "wait",
        "evaluation": "wait", "cloud": "simulated", "security": "wait"})
    role_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)
    policy_args: dict[str, Any] = field(default_factory=dict)
    detect_deadlocks: bool = True
    deadlock_action: str = "resolve"          # "resolve" | "report" | "off"
    auto_assign: bool = True
    reap: bool = True
    agent_subscriptions: tuple[str, ...] = ("resource.*",)
    trace_path: str = "var/events.jsonl"
    #: Phase 2: a resident kernel reads requests from here between ticks. Single-writer append
    #: (one JSON object per line), so any other process can inject work without a lock.
    inject_path: str = "var/inject.jsonl"
    # --- Phase 2.5 (M1/M2): real execution, attached to the runtime but never entangled with it.
    #: `tool_config` is passed to `arena.tools.Executor`; `cognition_sources` maps a role to a
    #: *factory* (a callable), because a shared instance would mean every agent of that role
    #: literally shares one brain - the exact failure this phase is here to make visible.
    tool_config: dict[str, Any] = field(default_factory=dict)
    cognition_sources: dict[str, Any] = field(default_factory=dict)
    transcript_dir: str | None = None
    logs_dir: str | None = None
    #: no progress for this many consecutive ticks -> stop and report (progress may need a human)
    stall_limit: int = 6
    stalled: bool = False
    _sig_seen: dict[Any, int] = field(default_factory=dict, init=False)

    graph: DependencyGraph = field(init=False)
    registry: AgentRegistry = field(init=False)
    journal: Journal = field(init=False)
    bus: Bus = field(init=False)
    parent: ParentArena = field(init=False)
    polls: PollCounter = field(init=False)
    clock: Any = field(init=False)
    actors: dict[str, AgentActor] = field(default_factory=dict, init=False)
    queues: dict[str, list[Message]] = field(default_factory=dict, init=False)
    subscriptions: dict[str, list[str]] = field(default_factory=dict, init=False)
    artifacts: dict[str, dict[str, Any]] = field(default_factory=dict, init=False)
    sent_this_tick: dict[str, int] = field(default_factory=dict, init=False)
    tick: int = 0
    task_text: str = ""
    log_lines: list[str] = field(default_factory=list, init=False)

    _deadlocks: list[list[str]] = field(default_factory=list, init=False)
    #: agent_id -> Jail / git root, set by `bind_tools`. Lives on the kernel because the workspace
    #: *is* runtime state; the Executor that uses it is still a separate object.
    workspaces: dict[str, Any] = field(default_factory=dict, init=False)
    git_roots: dict[str, Any] = field(default_factory=dict, init=False)
    tools: Any = field(default=None, init=False)
    # declared, not conjured: `run_resident` used to set an attribute that existed only after a
    # resident run, so any monitoring read before the first loop hit AttributeError
    resident_loops: int = field(default=0, init=False)
    injected_rows: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.clock = make_clock(self.clock_mode, self.clock_step)
        self.polls = PollCounter()
        self.graph = DependencyGraph()
        self.registry = AgentRegistry(budget=self.budget, graph=self.graph)
        self.journal = Journal(path=self.journal_path, now_fn=self.clock.now)
        self.bus = Bus(kernel=self)
        self.subscriptions["parent"] = list(self.bus.parent_patterns)
        # 'parent' and 'kernel' are listed as recipients, so they need real mailboxes - without
        # these two lines every control-plane message aimed at the Parent was silently dropped.
        self.queues.setdefault("parent", [])
        self.queues.setdefault("kernel", [])
        self.parent = ParentArena(kernel=self, planner=self.planner or RuleBasedPlanner())
        self._side_anchor = self._compute_side_anchor()
        self._trace = self._anchor(self.trace_path)
        if self._trace is not None:
            with contextlib.suppress(Exception):
                self._trace.parent.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------- side-file anchoring
    def _compute_side_anchor(self) -> Path | None:
        """Where relative side-files (trace, inject, config) belong.

        `root` has always defaulted to `"."`, which meant every kernel ever constructed without an
        explicit root wrote `var/events.jsonl`, `var/inject.jsonl` and `.arena.json` into *whatever
        directory the process happened to be started from* - so a plain `pytest` run littered the
        repository with an empty `var/`. The journal is the one location that is always explicit and
        always meant for this kernel, so relative paths are anchored to its directory; only a real,
        explicitly-set root overrides that.
        """
        root = str(self.root or "").strip()
        jp = str(self.journal_path or "")
        journal_dir = Path(jp).parent if jp and jp != ":memory:" and not Path(jp).name.startswith(":") else None
        if root and root not in (".", "./"):
            return Path(root)
        if journal_dir is not None and str(journal_dir) not in ("", "."):
            return journal_dir
        if root in (".", "./"):
            # still no anchor: do NOT guess with CWD. Absolute paths keep working, relative ones
            # are simply not materialised, and that is stated rather than silently written.
            return None
        return None

    def _anchor(self, rel: str | Path) -> Path | None:
        q = Path(str(rel))
        if q.is_absolute():
            return q
        return None if self._side_anchor is None else self._side_anchor / q

    # ------------------------------------------------------------- properties
    @property
    def now(self) -> float:
        return self.clock.now()

    @property
    def claims(self) -> dict[str, str]:
        return {k: v["owner"] for k, v in self.journal.claims().items()}

    def recipients(self) -> list[str]:
        return ["parent", "kernel", *self.registry.agents.keys()]

    def parent_inbox(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        """What the Parent actually received, oldest first."""
        q = self.queues.get("parent", [])
        return [m.to_dict() for m in (q if limit is None else q[-limit:])]

    def clear_inboxes(self) -> None:
        for q in self.queues.values():
            q.clear()

    def bind_actor(self, agent_id: str) -> AgentActor:
        """Bind a policy to an agent WITHOUT touching the lifecycle or the journal.

        `from_journal` used to call `make_actor()` here, which walked every replayed agent through
        CREATED -> INITIALIZING -> IDLE and journalled all of it. Two consequences, both bad: the
        replay *rewrote the finished agent's last transition* (a second fold then read IDLE where
        the run had ended COMPLETED), and every replay appended ~2 rows per agent to the journal it
        was only supposed to read. Reconstruction must be a pure projection.
        """
        rec = self.registry.require(agent_id)
        pol_name = self.role_policies.get(rec.role, self.default_policy)
        extra = dict(self.policy_args)
        extra.update(self.role_overrides.get(rec.role, {}))
        policy = make_policy(pol_name, **extra) if extra else make_policy(pol_name)
        actor = AgentActor(agent_id=agent_id, policy=policy, kernel=self)
        self.actors[agent_id] = actor
        self.queues.setdefault(agent_id, [])
        return actor

    # --------------------------------------------------- phase 2.5: workspaces & cognition
    def bind_tools(self, agent_id: str, *, root: str | Path, writes: Sequence[str] = ("",),
                   reads: Sequence[str] = ("",), allowed: Sequence[str] | None = None,
                   git_root: str | Path | None = "", **kw: Any) -> Any:
        """Give an agent a workspace it is jailed to, and create the executor if needed.

        Order matters and is a Phase-2 lesson (`fold()` files anything that precedes an entity's
        first row nowhere): `WORKSPACE_BOUND` is journalled *after* `AGENT_REGISTERED`, so the
        replayed agent record exists before the workspace is attached to it.
        """
        from .tools import Executor, Jail
        if self.tools is None:
            self.tools = Executor(kernel=self, **{**self.tool_config, **kw})
        jail = Jail(root=pathlib.Path(root), writes=tuple(writes), reads=tuple(reads),
                     agent_id=agent_id)
        self.workspaces[agent_id] = jail
        # `git_root=""` means "same as the jail root"; None means deliberately "no repo", which is
        # what makes `commit` refuse rather than fail in a shell.
        if git_root == "":
            git_root = jail.root
        if git_root is not None:
            self.git_roots[agent_id] = pathlib.Path(git_root)
        self.tools.bind(agent_id, jail=jail, allowed=allowed)
        return jail

    def git_root_of(self, agent_id: str) -> Any:
        return self.git_roots.get(agent_id)

    def ensure_git(self, agent_id: str) -> dict[str, Any]:
        """Give the agent's tree a repository, through the tool path.

        Nothing may appear in a workspace without being journalled - including the `git init` that
        makes `commit` possible at all. It is recorded as a TOOL_CALL/TOOL_RESULT pair with
        `tool="ensure_git"`, so an audit of the run shows the repo's birth instead of finding it
        already there.
        """
        import shutil
        import subprocess
        root = self.git_roots.get(agent_id)
        if root is None:
            return {"ok": False, "detail": "no git root bound"}
        if (pathlib.Path(root) / ".git").exists():
            return {"ok": True, "existed": True}
        if shutil.which("git") is None:
            return {"ok": False, "detail": "git is not installed in this environment"}
        rids = self.tools.plan(agent_id, [{"tool": "ensure_git", "args": {"argv": ["git", "init"]}}],
                               task_id=None)
        try:
            proc = subprocess.run(["git", "init"], cwd=str(root), stdin=subprocess.DEVNULL,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                  timeout=20.0)
            ok, out, err = proc.returncode == 0, proc.stdout, proc.stderr
        except Exception as e:
            ok, out, err = False, "", f"{type(e).__name__}: {e}"
        self.tools.stats["executions"] += 1
        self.journal.emit(MessageType.TOOL_RESULT, agent_id, "kernel",
                          body=f"ensure_git -> {'ok' if ok else 'failed'}", rid=rids[0],
                          tool="ensure_git", agent_id=agent_id, ok=ok, exit_code=0 if ok else 1,
                          stdout=(out or "")[:400], stderr=(err or "")[:400], changed=[],
                          data={"cwd": str(root)})
        return {"ok": ok, "existed": False}

    def bind_cognition(self, agent_id: str, source: Any = None) -> dict[str, Any]:
        """Attach a cognition source to one agent. Returns its fingerprint (also journalled).

        With no `source`, the role's factory in `cognition_sources` is *called*, so two agents of
        the same role get two objects with two transcripts. If a factory is absent the agent keeps
        its policy, reached through `PolicyCognition` at actor construction - i.e. binding cognition
        is always an upgrade, never a prerequisite.
        """
        from .cognition import PolicyCognition
        if source is None:
            maker = self.cognition_sources.get(self.registry.require(agent_id).role)
            if maker is None:
                raise KeyError(f"no cognition source for role "
                               f"{self.registry.require(agent_id).role!r}")
            source = maker() if callable(maker) else maker
        if not hasattr(source, "decide"):
            raise TypeError(f"cognition source {source!r} has no decide(obs) -> Intent")
        actor = self.actors.get(agent_id) or self.make_actor(agent_id)
        actor.cognition = source
        if isinstance(source, PolicyCognition) or not hasattr(source, "fingerprint"):
            fp = PolicyCognition(policy=source, agent_id=agent_id).fingerprint() \
                if not hasattr(source, "fingerprint") else source.fingerprint()
        else:
            fp = source.fingerprint()
        rec = self.registry.require(agent_id)
        rec.cognition = dict(fp)
        self.journal.emit(MessageType.COGNITION_BOUND, "kernel", agent_id,
                          body=f"cognition bound: {fp.get('source')}", agent_id=agent_id,
                          source=str(fp.get("source")), kind=str(fp.get("kind")),
                          model_id=str(fp.get("model", "")),
                          prompt_sha256_16=str(fp.get("prompt_sha256_16", "")),
                          obj_id=str(fp.get("obj_id", "")))
        return fp

    def cognition_report(self) -> list[dict[str, Any]]:
        """Per-agent cognition, including the numbers that expose a shared brain.

        `shared_obj_id` / `shared_prompt` are computed, not asserted by a caller: if two agents
        report the same object id or the same prompt hash, that is a *finding*, and `arena status`
        says it out loud instead of letting "5 agents" read as an organisation.
        """
        rows = []
        for aid, rec in self.registry.agents.items():
            actor = self.actors.get(aid)
            src = getattr(actor, "cognition", None) if actor else None
            fp = dict(rec.cognition or {})
            rows.append({"agent_id": aid, "role": rec.role,
                         "source": fp.get("source") or type(getattr(actor, "policy", None)).__name__
                         if actor else "?",
                         "kind": fp.get("kind", "policy"),
                         "model_id": fp.get("model", ""),
                         "prompt_sha256_16": fp.get("prompt_sha256_16", ""),
                         "obj_id": fp.get("obj_id", ""),
                         "transcript_len": len(getattr(actor, "transcript", []) or []),
                         "steps_run": getattr(actor, "steps_run", 0) if actor else 0})
        by_obj: dict[str, list[str]] = {}
        by_prompt: dict[str, list[str]] = {}
        for r in rows:
            by_obj.setdefault(r["obj_id"], []).append(r["agent_id"])
            if r["prompt_sha256_16"]:
                by_prompt.setdefault(r["prompt_sha256_16"], []).append(r["agent_id"])
        shared_obj = [v for v in by_obj.values() if len(v) > 1 and v[0]]
        shared_prompt = [v for v in by_prompt.values() if len(v) > 1]
        for r in rows:
            r["shared_obj_id"] = any(len(v) > 1 and r["agent_id"] in v for v in by_obj.values()
                                     if r["obj_id"])
            r["shared_prompt"] = any(len(v) > 1 and r["agent_id"] in v for v in by_prompt.values()
                                     if r["prompt_sha256_16"])
        return [{"agents": len(rows), "distinct_obj_ids": len([k for k in by_obj if k]),
                 "distinct_prompts": len([k for k in by_prompt if k]),
                 "shared_obj_groups": shared_obj, "shared_prompt_groups": shared_prompt,
                 "rows": rows}][0]

    def workspace_view(self, agent_id: str) -> dict[str, Any]:
        if self.tools is None:
            return {"bound": False}
        return self.tools.describe(agent_id)

    def completion_gate(self, agent_id: str, rec: Any) -> dict[str, Any]:
        """May this agent complete right now? {"allow": True} or a reason the journal will show.

        A task that declares `verify` commands must have them pass first; a task that declares none
        is Phase 1/2 behaviour and is left alone, which is why every existing test still completes.
        """
        tid = rec.task_id
        t = self.graph.tasks.get(tid or "")
        if t is None or not getattr(t, "verify", None):
            return {"allow": True}
        if getattr(t, "verified", False):
            return {"allow": True, "verified": True}
        return {"allow": False, "rule": "REJECT_UNVERIFIED",
                "detail": f"{t.task_id} declares {len(t.verify)} verify command(s); none has "
                          f"exited 0 for this task yet",
                "task_id": t.task_id}

    def run_verify(self, agent_id: str, *, task_id: str | None = None) -> dict[str, Any]:
        """Execute the task's verify commands through the *tool* path and record the outcome.

        Not a special "verification runner": they are `run_command` calls, so they get the same
        jail, timeout, redaction and journal rows as anything else the agent does. That is what
        makes the evidence the same kind of evidence a human would accept.
        """
        rec = self.registry.require(agent_id)
        tid = task_id or rec.task_id
        t = self.graph.tasks.get(tid or "")
        if t is None:
            return {"ok": False, "rule": "REJECT_NO_TASK"}
        if self.tools is None:
            return {"ok": False, "rule": "REJECT_NO_EXECUTOR",
                    "detail": "verify declared but this kernel has no tool executor bound"}
        results = []
        for argv in (getattr(t, "verify", None) or []):
            res = self.tools.execute(agent_id, "run_command",
                                     {"argv": list(argv), "cwd": "", "timeout": 180.0},
                                     rid="", task_id=t.task_id)
            results.append({"tool": res.tool, "argv": list(argv), "exit": res.exit_code,
                            "ok": bool(res.ok), "stderr_tail": (res.stderr or "")[-400:],
                            "stdout_tail": (res.stdout or "")[-400:]})
        ok = bool(results) and all(r["ok"] for r in results)
        t.verified = ok
        t.verified_at = self.now
        self.journal.emit(MessageType.TASK_VERIFIED, "kernel", "parent",
                          body=f"{t.task_id} verify {'PASSED' if ok else 'FAILED'} "
                               f"({len(results)} command(s))",
                          task_id=t.task_id, agent_id=agent_id, verified=ok,
                          verify_results=results, rule="VERIFY_PASSED" if ok else "VERIFY_FAILED")
        self.registry.version += 1
        return {"ok": ok, "results": results, "task_id": t.task_id}

    def transition(self, agent_id: str, to: AgentState | str, reason: str = "") -> bool:
        """The ONLY sanctioned way to change an agent's lifecycle state.

        Every accepted *and* rejected edge is journalled through here, so replay can never
        diverge from live state because some code path mutated the FSM behind the journal's back.
        """
        rec = self.registry.get(agent_id)
        if rec is None:
            return False
        res = rec.lifecycle.request(to, reason, now=self.now)
        if res.ok:
            self.registry.version += 1
            self.journal.emit(MessageType.STATE_TRANSITION, agent_id, "parent",
                              body=f"{res.frm} -> {res.to} ({reason})", agent_id=agent_id,
                              frm=str(res.frm), to=str(res.to), reason=reason)
            return True
        self.journal.emit(MessageType.ILLEGAL_TRANSITION, agent_id, "parent",
                          body=res.reason, agent_id=agent_id, frm=str(res.frm), to=str(res.to))
        return False

    def log(self, text: str) -> None:
        self.log_lines.append(f"[{self.now:7.2f}] {text}")

    # --------------------------------------------------------------- plumbing
    def make_actor(self, agent_id: str) -> AgentActor:
        rec = self.registry.require(agent_id)
        pol_name = self.role_policies.get(rec.role, self.default_policy)
        extra = dict(self.policy_args)
        extra.update(self.role_overrides.get(rec.role, {}))
        policy = make_policy(pol_name, **extra) if extra else make_policy(pol_name)
        actor = AgentActor(agent_id=agent_id, policy=policy, kernel=self)
        self.actors[agent_id] = actor
        self.queues.setdefault(agent_id, [])
        # CREATED -> INITIALIZING -> IDLE, journaled through the same guard the policies use, so a
        # replayed agent lands in exactly the state the live one is in.
        self.transition(agent_id, AgentState.INITIALIZING, "bound to policy")
        self.transition(agent_id, AgentState.IDLE, "ready")
        return actor

    def deliver(self, actor_id: str, msg: Message) -> bool:
        """Used by the dependency manager for targeted wakes - bypasses broadcast, never queues
        for an agent that is not subscribed."""
        if actor_id not in self.queues:
            return False
        self.queues[actor_id].append(msg)
        return True

    def publish(self, msg: Message) -> Any:
        self.sent_this_tick[msg.from_actor] = self.sent_this_tick.get(msg.from_actor, 0) + 1
        out = self.bus.publish(msg)
        if msg.msg_type is MessageType.FEATURE_REQUEST:
            self.parent.feature_requests.append(
                {"from": msg.from_actor, "to": msg.to_actor, "body": msg.body,
                 "payload": msg.payload, "correlation_id": msg.correlation_id, "mid": msg.mid})
        if self._trace is not None:
            with contextlib.suppress(Exception):
                with self._trace.open("a") as f:
                    f.write(json.dumps(msg.to_dict(), default=str) + "\n")
        return out

    # -------------------------------------------------------------- artifacts
    def artifact_exists(self, artifact: str) -> bool:
        return artifact in self.artifacts

    def missing_artifacts(self, artifacts) -> list[str]:
        return [a for a in artifacts if a not in self.artifacts]

    def wake_ready(self, reason: str = "") -> list[str]:
        """Re-arm parked agents whose wait is satisfied. Called from state transitions (one event),
        never from a timer - this is what 'resume automatically' means here."""
        woke: list[str] = []
        for aid, rec in list(self.registry.agents.items()):
            if rec.lifecycle.state not in (AgentState.WAITING_FOR_DEPENDENCY, AgentState.BLOCKED,
                                           AgentState.ESCALATED):
                continue
            satisfied = [w for w in rec.pending_waits
                         if self._condition_met(str(w.get("condition")))]
            for w in satisfied:
                rec.pending_waits.remove(w)
            if rec.pending_waits:
                continue
            actor = self.actors.get(aid)
            if actor is not None and not actor.has_mail():
                self.journal.emit(MessageType.DEPENDENCY_READY, "dependency_manager", aid,
                                  body=f"unblocked: {reason}", task_id=rec.task_id)
                self.deliver(aid, Message(msg_type=MessageType.DEPENDENCY_READY,
                                          from_actor="dependency_manager", to_actor=aid,
                                          body=f"unblocked: {reason}", task_id=rec.task_id))
                self.bus.stats["wakes"] += 1
            self.transition(aid, AgentState.WORKING, f"unblocked: {reason}")
            woke.append(aid)
        return woke

    def _condition_met(self, condition: str) -> bool:
        if condition.startswith("artifact:"):
            return condition.split(":", 1)[1] in self.artifacts
        if condition.startswith("task:"):
            tid = condition.split(":", 1)[1]
            t = self.graph.tasks.get(tid)
            return bool(t) and not t.is_open
        return False

    def publish_artifact(self, artifact: str, *, producer: str, task_id: str | None = None,
                         digest: str = "", files: Sequence[dict[str, Any]] | None = None) -> None:
        version = (self.artifacts.get(artifact, {}).get("version") or 0) + 1
        self.artifacts[artifact] = {"artifact": artifact, "version": version,
                                    "producer": producer, "task_id": task_id,
                                    "at": self.now, "digest": digest,
                                    "files": [dict(f) for f in (files or [])]}
        self.graph.known_artifacts.add(artifact)
        # task-level bookkeeping follows artifact truth: once every promised output of a task
        # exists, the task is done, whoever or however many agents contributed.
        if task_id and task_id in self.graph.tasks:
            t = self.graph.tasks[task_id]
            if t.produces and not self.missing_artifacts(t.produces) and t.is_open:
                t.status = "done"
                t.finished_at = self.now
                self.journal.emit(MessageType.TASK_COMPLETED, producer, "parent",
                                  body=f"{task_id} auto-closed: all artifacts published",
                                  task_id=task_id, artifacts=list(t.produces), owner=t.owner,
                                  auto=True)
        self.wake_ready(f"artifact {artifact} v{version}")
        self.registry.version += 1
        self.journal.emit(MessageType.RESOURCE_UPDATED, producer, "broadcast",
                          body=f"{artifact} v{version} ready", resource=artifact,
                          task_id=task_id, version=version,
                          payload={"artifact": artifact, "version": version,
                                   # M1: an artifact now carries the bytes' digest, so "it exists"
                                   # can be checked against the workspace instead of a name match
                                   "digest": digest, "files": [dict(f) for f in (files or [])]})
        self.bus.resolve(f"artifact:{artifact}", reason=f"{producer} published {artifact} v{version}")

    def requeue(self, task_id: str, reason: str = "") -> bool:
        """Drop a claim so the unit of work can be legitimately picked up again (used to break a
        wait-for cycle). The claim row is kept for audit; only the assignment is cleared."""
        t = self.graph.tasks.get(task_id)
        if t is None or not t.is_open:
            return False
        owner = t.owner
        t.owner, t.status = None, "pending"
        if owner and owner in self.registry.agents:
            self.registry.agents[owner].task_id = None
        self.registry.version += 1
        self.log(f"requeued {task_id}: {reason}")
        return True

    # ------------------------------------------------------------------- runs
    def _begin_if_assigned(self, agent_id: str) -> None:
        """Kernel owns 'task started': journal and live state advance together, so fold() can
        never disagree with what the scheduler believes."""
        rec = self.registry.get(agent_id)
        if rec is None or rec.task_id is None:
            return
        t = self.graph.tasks.get(rec.task_id)
        # `not t.started` rather than a status match: assignment and scheduling both leave a task
        # in "pending", and an earlier guard on status=="assigned" meant most tasks were never
        # marked started at all (no TASK_STARTED event, no IDLE -> WORKING edge, live and replay
        # then disagreed about every agent's state).
        if t is None or t.started or not t.is_open:
            return
        t.started = True
        t.status = "running"
        t.started_at = self.now
        self.journal.emit(MessageType.TASK_STARTED, agent_id, "parent",
                          body=f"{t.task_id} started", task_id=t.task_id, owner=agent_id)
        # Starting work IS a state change, so it must go through the journal like everything else.
        # (Previously the kernel set task.status and left the agent IDLE, which is exactly how a
        # live kernel and its replay ended up disagreeing about agent state.)
        if rec.lifecycle.state is AgentState.IDLE:
            self.transition(agent_id, AgentState.WORKING, f"started {t.task_id}")

    def submit(self, task_text: str) -> dict[str, Any]:
        self.task_text = task_text
        out = self.parent.submit(task_text)
        for aid in self.registry.agents:
            self.make_actor(aid)
        self.log(f"planned {out.get('tasks')} tasks, {len(out.get('agents', []))} agents")
        return out

    def _eligible(self) -> list[str]:
        """Runnable agents, least-worked first, capped by the *hardware* limit.

        This is why max_concurrent_workers is a separate knob from max_active_agents: 8 logical
        agents can be registered, but at most 2 hold a slot, and the rest wait for a slot rather
        than being spawned as extra processes.
        """
        run = []
        for aid, rec in self.registry.agents.items():
            actor = self.actors.get(aid) or self.make_actor(aid)
            if not actor.runnable():
                continue
            woke = 0 if self.queues.get(aid) else 1
            run.append((actor.steps_run, woke, aid))
        run.sort()
        return [aid for _, _, aid in run[: max(1, self.budget.max_concurrent_workers)]]

    def _signature(self) -> tuple:
        return (tuple(sorted((t, s.status, s.owner or "") for t, s in self.graph.tasks.items())),
                tuple(sorted((a, r.state) for a, r in self.registry.agents.items())))

    def run(self, ticks: int = 60) -> dict[str, Any]:
        start = self.tick
        self._sig_seen = {}
        for _ in range(ticks):
            self.tick += 1
            self.clock.tick()
            self.sent_this_tick.clear()
            self.parent.apply_arena_decisions()
            for row in self.bus.expire_timeouts():
                rec = self.registry.get(row["agent_id"])
                if rec is not None:
                    self.parent.inbox.append({"agent": rec.agent_id, "task_id": rec.task_id,
                                              "reason": f"WAIT_TIMEOUT on {row['condition']}",
                                              "at": self.now, "kind": "timeout"})
                    self.parent.write_inbox(self.parent.inbox[-1])
            if self.detect_deadlocks and self.deadlock_action != "off":
                if self.deadlock_action == "resolve":
                    self._deadlocks.extend(self.parent.resolve_deadlock())
                else:
                    self._deadlocks.extend(self.parent.detect_deadlock())
            if self.auto_assign:
                self.parent.schedule()
            for aid in self._eligible():
                actor = self.actors.get(aid) or self.make_actor(aid)
                self._begin_if_assigned(aid)
                # a non-empty inbox means "you were woken": drain it, then continue working.
                while self.queues.get(aid):
                    actor.run_step()
                    if is_sleeping_action(actor.last_action):
                        break
                if self.registry.agents[aid].lifecycle.state in RUNNABLE:
                    actor.run_step()
            if self.reap:
                self.parent.reap_idle()
            if self._all_done():
                break
        self.checkpoint(reason="run boundary")
        self._trace_flush()
        return self.summary()

    # NOTE: `run_resident` is defined exactly once, further down, and it is the Phase-2 version
    # (injection drain + on_tick + checkpoint per loop). An earlier copy of the old, simpler body
    # survived here as dead code after Phase 2 added the real one; the design doc §14 recorded it
    # and M0 deletes it, because a duplicate definition of the method that will host tool execution
    # is precisely the kind of thing that becomes a live bug later.

    def _final_stats(self) -> None:
        """Per-agent counters are projected by fold(), so a replay can be compared to the live
        kernel field by field instead of 'close enough'."""
        for aid, actor in self.actors.items():
            rec = self.registry.get(aid)
            if rec is None:
                continue
            self.journal.emit(MessageType.STATS, aid, "parent", body="run stats",
                              agent_id=aid, msgs_sent=rec.msgs_sent,
                              work_done=round(rec.work_done, 6), steps_run=actor.steps_run)

    CONFIG_FILE = "kernel_config.json"

    def config(self) -> dict[str, Any]:
        """The kernel's own construction settings, in reloadable form.

        Why this is persisted separately from the journal: *which policy each role runs* is not an
        event, it is configuration, and without it a kernel rebuilt by another process would
        re-create every actor with the default policy. Phase 2 found exactly that - the demo's
        `backend_01` came back as WaitForArtifacts instead of NeedsSpecialist, so it kept the right
        counters and the wrong brain. Config lives next to the journal, in the run directory.
        """
        return {"default_policy": self.default_policy, "role_policies": dict(self.role_policies),
                "cognition_roles": sorted(self.cognition_sources),
                "tool_config": {kk: vv for kk, vv in self.tool_config.items()
                                if kk not in ("jail",)},
                "role_overrides": {k: dict(v) for k, v in self.role_overrides.items()},
                "policy_args": dict(self.policy_args), "budget": asdict(self.budget),
                "agent_subscriptions": list(self.agent_subscriptions),
                "detect_deadlocks": self.detect_deadlocks, "deadlock_action": self.deadlock_action,
                "auto_assign": self.auto_assign, "reap": self.reap,
                "stall_limit": self.stall_limit, "work_unit": self.work_unit,
                "clock_mode": self.clock_mode, "clock_step": self.clock_step,
                "task_text": self.task_text}

    def _config_path(self) -> Path | None:
        # anchored like every other side-file: a kernel with an in-memory/no-root configuration
        # must not write its policy file into the current working directory
        path = self._anchor(self.CONFIG_FILE)
        if path is None:
            return None
        # `config()`/`read_config()` are also used to *recover* a run, and the pre-existing
        # semantics were "root/CONFIG_FILE"; keep that when root is explicit and real.
        return path

    def write_config(self) -> None:
        path = self._config_path()
        if path is None:
            return
        with contextlib.suppress(OSError):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(self.config(), indent=2, default=str))

    @classmethod
    def read_config(cls, root: str | Path) -> dict[str, Any]:
        try:
            return json.loads((Path(root) / cls.CONFIG_FILE).read_text())
        except (OSError, ValueError):
            return {}

    def apply_config(self, cfg: dict[str, Any]) -> "Kernel":
        """Adopt persisted config, keeping anything the caller passed explicitly."""
        for key in ("default_policy", "detect_deadlocks", "deadlock_action", "auto_assign",
                    "reap", "stall_limit", "work_unit", "task_text"):
            if key in cfg and getattr(self, key, None) in (None, "", (), {}, []):
                setattr(self, key, cfg[key])
        if cfg.get("role_policies") and self.role_policies == Kernel().role_policies:
            self.role_policies = dict(cfg["role_policies"])
        if cfg.get("role_overrides") and not self.role_overrides:
            self.role_overrides = {k: dict(v) for k, v in cfg["role_overrides"].items()}
        if cfg.get("policy_args") and not self.policy_args:
            self.policy_args = dict(cfg["policy_args"])
        if cfg.get("agent_subscriptions") and self.agent_subscriptions == ("resource.*",):
            self.agent_subscriptions = tuple(cfg["agent_subscriptions"])
        return self

    def checkpoint(self, *, reason: str = "run boundary") -> dict[str, Any]:
        """Journal a full kernel snapshot, so `tick` and per-actor cursors survive a restart.

        Phase 1 shipped `latest_snapshot()` reading `etype='SNAPSHOT'` but **no writer at all**, so
        the resume path silently always started at tick 0 and the call was dead code. Discovered
        while proving Phase 2's replay parity. A snapshot is a *projection of projections*: it never
        becomes a second source of truth, because every field in it is also derivable from the event
        rows - it is a cache the replay prefers when one exists.
        """
        snap = self.snapshot()
        self.write_config()
        self.journal.emit(MessageType.SNAPSHOT, "parent", "parent",
                          body=f"checkpoint @tick {self.tick} ({reason})",
                          snapshot=snap, tick=self.tick, reason=reason)
        return snap

    def _all_done(self) -> bool:
        return not any(t.is_open for t in self.graph.tasks.values()) if self.graph.tasks else False

    def _trace_flush(self) -> None:
        if self._trace is None:
            return
        with contextlib.suppress(Exception):
            with self._trace.open("a") as f:
                f.write(json.dumps({"kernel_snapshot": self.status()["counts"],
                                    "tick": self.tick, "done": self._all_done()}) + "\n")

    # ------------------------------------------------------ Phase 2: monitoring
    def state_counts(self) -> dict[str, int]:
        """Agent counts by lifecycle state. Derived from the registry, never cached, so it can
        disagree with the journal only if something mutated state behind `transition()`."""
        out = {str(s): 0 for s in AgentState}
        for rec in self.registry.agents.values():
            out[str(rec.lifecycle.state)] = out.get(str(rec.lifecycle.state), 0) + 1
        return out

    def metrics(self) -> dict[str, Any]:
        """The agent-count monitor (spec §5): roster size, per-state counts, spawn-request flow,
        generation/depth, and remaining capacity - all recomputed, nothing stored.

        `blocked` is a *graph* fact (an open task whose deps are unmet), not a lifecycle state, so
        it counts agents whose current task is blocked; `waiting` counts durable waits armed in
        the journal. Both are intentionally derived here rather than maintained as counters: a
        counter that drifts is worse than a cheap recompute.
        """
        reg, graph, st = self.registry, self.graph, self.spawn_stats_view()
        active = reg.active()
        by_state = self.state_counts()
        blocked = 0
        for rec in active:
            tid = rec.task_id
            if tid and graph.tasks.get(tid) is not None and graph.blocked_by(tid):
                blocked += 1
        depths: dict[str, int] = {}
        for rec in reg.agents.values():
            depths[str(rec.epoch)] = depths.get(str(rec.epoch), 0) + 1
        epochs = [r.epoch for r in reg.agents.values()]
        waiting = sum(len(r.pending_waits) for r in reg.agents.values())
        cap = reg.budget.max_active_agents
        resolved = st["approved"] + st["rejected"] + st["deduplicated"] + st["escalated"]
        return {
            "agents_total": len(reg.agents),
            "active": len(active),
            "idle": by_state[str(AgentState.IDLE)],
            "working": by_state[str(AgentState.WORKING)],
            "waiting": waiting,
            "blocked": blocked,
            "escalated": by_state[str(AgentState.ESCALATED)],
            "completed": by_state[str(AgentState.COMPLETED)],
            "requests_received": st["received"],
            "requests_approved": st["approved"],
            "requests_rejected": st["rejected"],
            "requests_deduplicated": st["deduplicated"],
            "requests_escalated": st["escalated"],
            "requests_deferred": st["deferred"],
            "reuses": st["reused"],
            "spawned_by_agents": st["spawned_by_agents"],
            "cycles_rejected": st["cycles_rejected"],
            "by_rule": dict(self.parent.ledger.by_rule()),
            "generation_epoch_max": max(epochs) if epochs else 0,
            "generation_epoch_mean": round(sum(epochs) / len(epochs), 3) if epochs else 0.0,
            "spawn_depth_hist": depths,
            "spawn_depth_max": reg.budget.max_spawn_epoch,
            "remaining_capacity": max(0, cap - len(active)),
            "agent_budget": cap,
            "worker_slots_in_use": min(len(self._eligible()), reg.budget.max_concurrent_workers),
            "workers_max": reg.budget.max_concurrent_workers,
            "open_graph_tasks": sum(1 for t in graph.tasks.values() if t.is_open),
            "rejection_rate": round(st["rejected"] / resolved, 3) if resolved else 0.0,
            "ledger": self.parent.ledger.counts(),
            "stalled": self.stalled,
            "tick": self.tick,
        }

    def spawn_stats_view(self) -> dict[str, int]:
        """Parent counters, with any key the parent has not seen yet defaulted to 0."""
        base = {"received": 0, "approved": 0, "rejected": 0, "deduplicated": 0, "escalated": 0,
                "deferred": 0, "reused": 0, "spawned_by_agents": 0, "cycles_rejected": 0}
        base.update(getattr(self.parent, "spawn_stats", {}) or {})
        return base

    def monitoring(self) -> dict[str, Any]:
        """Compact form for `arena status`: what a reviewer actually wants on one screen."""
        m = self.metrics()
        return {k: m[k] for k in (
            "active", "idle", "working", "waiting", "blocked", "escalated", "completed",
            "requests_received", "requests_approved", "requests_rejected", "requests_deduplicated",
            "requests_escalated", "reuses", "generation_epoch_max", "spawn_depth_hist",
            "remaining_capacity", "agent_budget", "worker_slots_in_use", "workers_max",
            "open_graph_tasks")}

    # ------------------------------------------------- Phase 2: runtime injection
    def idle_predicate(self) -> bool:
        """True when this kernel has nothing it can do on its own.

        Three separate reasons to keep spinning - a runnable agent, an armed durable wait (which
        the bus may resolve), or a queued spawn request (which the *parent* must answer). Only when
        all three are empty is the kernel genuinely parked, and that is what distinguishes 'idle'
        from 'stalled' in resident mode.
        """
        if self.parent.spawn_requests or self.parent.feature_requests:
            return False
        if any(self.registry.get(a).pending_waits for a in list(self.registry.agents)):
            return False
        if any(self.queues.get(a) for a in list(self.actors)):
            return False
        try:
            if self._eligible():
                return False
            # an unowned task that is *ready* is the parent's job, not a reason to park: if the
            # scheduler left it alone we are genuinely stuck, not idle.
            if any(t.is_open and self.graph.is_ready(t.task_id) and not t.owner
                   for t in self.graph.tasks.values()):
                return False
        except Exception:
            return False
        return True

    def inject_spawn_request(self, req: Any, *, source: str = "external",
                            journal: bool = True) -> Any:
        """Hand the running org a request *without* restarting it.

        This is the seam the whole resident story needs: a policy, another process, or a test can
        put a request in front of the parent, and it is answered on the next tick by the same
        kernel object. The append is journalled here so that even an externally-injected request
        has a durable record of its arrival, not just of its outcome.
        """
        if isinstance(req, dict):
            reserved = ("from", "requester_agent_id", "correlation_id", "task_id")
            payload = {k: v for k, v in req.items() if k not in reserved}
            msg = Message(msg_type=MessageType.SPAWN_AGENT_REQUEST,
                          from_actor=req.get("from") or req.get("requester_agent_id") or source,
                          to_actor="parent", body=req.get("reason", ""),
                          task_id=req.get("task_id"),
                          correlation_id=req.get("correlation_id", ""), payload=payload)
            if journal:
                # only JSON-safe scalars/sequences are journalled as first-class payload keys;
                # anything exotic stays inside `payload`, which the journal already serialises.
                safe = {k: v for k, v in payload.items()
                        if isinstance(v, (str, int, float, bool, list, tuple, type(None)))}
                self.journal.emit(MessageType.SPAWN_AGENT_REQUEST, msg.from_actor, "parent",
                                  body=msg.body, task_id=msg.task_id,
                                  correlation_id=msg.correlation_id, source=source,
                                  injected=True, **safe)
            self.parent.spawn_requests.append(msg)
        else:
            if journal:
                self.journal.emit(MessageType.SPAWN_AGENT_REQUEST,
                                  getattr(req, "requester_agent_id", source), "parent",
                                  body=getattr(req, "reason", ""),
                                  correlation_id=getattr(req, "correlation_id", ""),
                                  source=source, injected=True)
            self.parent.spawn_requests.append(req)
        return self.parent.spawn_requests[-1]

    def drain_injection_file(self, *, consume: bool = True) -> int:
        """Read (and optionally truncate) the append-only inject file. Called from the resident
        loop, so injection needs no lock, no port, no signal handler."""
        path = self._anchor(self.inject_path)
        if path is None:
            return 0            # nowhere to read from: no rows consumed, and no CWD guess made
        if not path.exists():
            return 0
        rows, text = [], path.read_text()
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                # a torn tail from a killed writer: ignore it, keep the rest usable
                continue
        for row in rows:
            self.inject_spawn_request(row.get("spawn_request", row))
        self.injected_rows += len(rows)
        if consume and rows:
            with contextlib.suppress(OSError):
                path.write_text("")
        return len(rows)

    def run_resident(self, seconds: float, idle: float = 0.05,
                     ticks_per_loop: int = 3, on_tick: Any = None,
                     drain_inbox: bool = True) -> dict[str, Any]:
        """Continuous mode: the kernel stays up, so a request can arrive *between* ticks.

        Not threaded, on purpose. A second thread mutating the graph would need locking, and the
        whole design brief says avoid locking - so instead each iteration (a) polls the injection
        file, cheap and single-writer, and (b) hands control to `on_tick`, which is how a caller
        observes a real mid-run transition instead of reconstructing one from three test cases.
        `idle` is the outer-loop pause, not a per-agent poll; agents never poll the parent.
        """
        deadline = time.monotonic() + seconds
        loops = 0
        last = self.tick
        while time.monotonic() < deadline:
            loops += 1
            if drain_inbox:
                self.drain_injection_file()
            out = self.run(ticks_per_loop)
            self.checkpoint(reason=f"resident loop {loops}")
            if on_tick is not None:
                on_tick(self, out)
            if out["done"] and not self.parent.spawn_requests:
                break
            if self.tick == last and self.idle_predicate():
                # nothing to do and nothing was injected: park, but keep listening for injections
                time.sleep(idle)
            last = self.tick
            if out.get("stalled") and not self.parent.spawn_requests:
                break
        self.resident_loops = loops
        return self.summary()

    # ----------------------------------------------------------------- report
    def status(self) -> dict[str, Any]:
        return self.parent.status()

    def summary(self) -> dict[str, Any]:
        open_tasks = [t for t, s in self.graph.tasks.items() if s.is_open]
        return {"tick": self.tick, "done": not open_tasks, "stalled": self.stalled,
                "open_tasks": open_tasks, "agents": len(self.registry.agents),
                "events": self.journal.count(), "artifacts": len(self.artifacts),
                "polls": self.polls.polls, "escalations": len(self.parent.inbox),
                "deadlocks": len(self._deadlocks),
                # how the run was fed: an injected line is what makes "no restart needed" true
                "resident_loops": self.resident_loops, "injected": self.injected_rows,
                "chain_ok": self.journal.verify_chain()[0]}

    def report(self) -> str:
        s, st = self.summary(), self.status()
        lines = [self.parent.render(), "",
                 f"tick={s['tick']} done={s['done']} events={s['events']} "
                 f"polls={s['polls']} deadlocks={s['deadlocks']} chain_ok={s['chain_ok']}",
                 f"counts: {st['counts']}",
                 f"bus: { {k: v for k, v in st['bus'].items() if k != 'subscriptions'} }",
                 f"artifacts: { {a: v['version'] for a, v in sorted(self.artifacts.items())} }",
                 f"open tasks: {s['open_tasks'] or 'none'}"]
        for tid, t in sorted(self.graph.tasks.items()):
            lines.append(f"  {tid:<20} {t.status:<8} owner={str(t.owner):<14} "
                         f"deps={sorted(t.deps)}")
        return "\n".join(lines)

    def why(self, agent_id: str) -> dict[str, Any]:
        rec = self.registry.get(agent_id)
        if rec is None:
            return {"error": f"unknown agent {agent_id}"}
        t = self.graph.tasks.get(rec.task_id or "")
        return {"agent_id": agent_id, "state": rec.state, "task": rec.task_id,
                "task_status": t.status if t else None,
                "unmet_deps": self.graph.unmet(t.task_id) if t else [],
                "missing_artifacts": [a for a in (t.consumes if t else [])
                                      if not self.artifact_exists(a)],
                "durable_waits": [w.get("condition") for w in rec.pending_waits],
                "steps": self.actors[agent_id].steps_run if agent_id in self.actors else 0,
                "recent": [self.journal._rowdict(r) for r in
                           self.journal.events(actor=agent_id, limit=6)]}

    def trace(self, correlation_id: str) -> list[dict[str, Any]]:
        return self.journal.trace(correlation_id)

    def snapshot(self) -> dict[str, Any]:
        """Everything a replay must reproduce byte-for-byte."""
        return {"agents": {aid: {"role": a.role, "state": a.state, "task_id": a.task_id,
                                 "epoch": a.epoch, "skills": sorted(a.skills),
                                 "msgs_sent": a.msgs_sent, "task_queue": list(a.task_queue),
                                 "work_done": round(a.work_done, 6),
                                 "steps_run": self.actors[aid].steps_run if aid in self.actors else 0,
                                 "policy_cursor": (self.actors[aid].policy_cursor()
                                                   if aid in self.actors else 0),
                                 "waits": sorted(str(w.get("condition")) for w in a.pending_waits)}
                          for aid, a in sorted(self.registry.agents.items())},
                "tasks": {tid: {"status": t.status, "owner": t.owner, "deps": sorted(t.deps),
                                "consumes": list(t.consumes), "produces": list(t.produces),
                                "est_work": t.est_work, "claims": list(t.claims)}
                          for tid, t in sorted(self.graph.tasks.items())},
                "artifacts": {a: meta["version"] for a, meta in sorted(self.artifacts.items())},
                "claims": {k: v["owner"] for k, v in sorted(self.journal.claims().items())},
                "tick": self.tick, "events": self.journal.count()}

    def replay(self) -> dict[str, Any]:
        """Fold the journal alone and return it (the crash-recovery check)."""
        return self.journal.fold()

    @classmethod
    def from_journal(cls, path: str | Path, *, quiet: bool = False,
                     **kw: Any) -> "Kernel":
        """Rebuild a kernel from a persisted journal - what runs after a sandbox recycle.

        `quiet=True` performs no writes at all (no REPLAY_COMPLETE row). That is what `verify`
        uses: a tool that checks the integrity of a log must not change the log. A genuine resume
        keeps the marker, since "this process rebuilt from the journal" is itself a fact.
        """
        k = cls(journal_path=path, **kw)
        # Rebuild with the SAME policy configuration the run wrote down (see config()). Skipped for
        # the caller if they passed role config explicitly, so an override still wins.
        # read what *this* run's root recorded. `_side_anchor` deliberately avoids the CWD, so it is
        # the fallback only for a kernel that has no root at all; using it unconditionally would
        # relocate config recovery from <root>/ to <journal-dir>/ for every `from_journal` caller.
        cfg = Kernel.read_config(k._side_anchor or k.root) if (k.root or k._side_anchor) else {}
        if cfg:
            k.apply_config(cfg)
        fold = k.journal.fold()
        # resume the clock where the crashed kernel left it, otherwise re-armed waits could
        # expire in negative time
        k.clock.advance(Kernel._restore_clock_from(fold))
        from .registry import AgentRecord
        for aid, a in fold["agents"].items():
            if aid not in k.registry.agents:
                rec = k.registry.register(agent_id=aid, role=a.get("role", "?"),
                                          skills=a.get("skills", []), epoch=a.get("epoch", 0),
                                          spawned_by=a.get("spawned_by", "parent"))
                rec.lifecycle.state = AgentState(a.get("state", "IDLE"))
                rec.lifecycle.since = a.get("state_since", 0.0)
                rec.task_id = a.get("task_id")
                rec.msgs_sent = a.get("msgs_sent", 0)
                rec.work_done = a.get("work_done", 0.0)
                k.bind_actor(aid)
                # CRITICAL: restore the backlog. A resumed kernel that forgot which tasks it was
                # holding would leave them owned-but-unworked forever (found by the cross-process
                # CLI test, invisible to an in-process test that only replays finished kernels).
                rec.task_id = a.get("task_id")
                rec.task_queue = list(a.get("task_queue") or [])
                # bind_actor does not walk CREATED -> IDLE (see its docstring); the assignment
                # below is what pins the agent to its journaled final state.
                replayed = k.registry.agents[aid]
                replayed.lifecycle.state = AgentState(a.get("state", "IDLE"))
                replayed.lifecycle.since = a.get("state_since", 0.0)
                # (cursor restore happens once, after the whole fold has been projected - see
                # "authoritative restore" below. Doing it here read `policy_cursor` from a fold
                # entry that had not seen any TASK_PROGRESS row yet, so every replayed actor got a
                # cursor of 0 and would re-run its policy from the beginning.)
        for tid, spec in fold["tasks"].items():
            if tid not in k.graph.tasks:
                k.graph.add(TaskSpec(task_id=tid, title=spec.get("title", ""),
                                     role=spec.get("role", ""), skills=spec.get("skills", []),
                                     est_work=spec.get("est_work", 1.0),
                                     produces=list(spec.get("produces", [])),
                                     consumes=list(spec.get("consumes", [])),
                                     claims=list(spec.get("claims", []))), derive=False)
            t = k.graph.tasks[tid]
            for art in t.produces:
                k.graph.producers.setdefault(art, set()).add(tid)
            t.status = spec.get("status", "pending")
            t.owner = spec.get("owner")
            t.deps |= set(spec.get("deps", []))
        # Phase 2: tasks loaded with derive=False above (and PLAN_CREATED rows that recorded no
        # deps) would otherwise replay as an un-wired graph - a recovered agent would then run a
        # task whose upstream artifacts do not exist yet. Re-derive from the artifact
        # produces/consumes declarations, which are the source of truth for edges.
        with contextlib.suppress(Exception):
            self_derived = k.graph.derive_edges()
            if self_derived:
                k.log(f"replay: re-derived edges for {len(self_derived)} task(s)")
        # artifact state is a projection of RESOURCE_UPDATED events; without this a recovered
        # kernel would forget every published contract and re-block its consumers forever.
        for art, version in (fold.get("artifacts") or {}).items():
            k.artifacts[art] = {"artifact": art, "version": version, "producer": "replay",
                                "task_id": None, "at": 0.0}
            k.graph.known_artifacts.add(art)
        # Phase 2: the request ledger must replay too - otherwise a resumed kernel would happily
        # re-approve the same mid-run request that the crashed one already spent an agent on.
        from .spawn import RequestState, SpawnRequest
        for rid, e in (fold.get("requests") or {}).items():
            if rid in k.parent.ledger.entries:
                continue
            req = SpawnRequest(
                requester_agent_id=e.get("requester", ""), requested_role=e.get("requested_role", ""),
                reason=e.get("reason", ""), required_skills=tuple(e.get("required_skills", ())),
                estimated_work=float(e.get("estimated_work", 0.0) or 0.0),
                required_inputs=tuple(e.get("required_inputs", ())),
                expected_outputs=tuple(e.get("expected_outputs", ())),
                parent_task_id=e.get("parent_task_id"), correlation_id=e.get("correlation_id", ""),
                capability_class=e.get("capability_class", "general"), at_tick=int(e.get("at_tick", 0)))
            try:
                state = RequestState(e.get("state", "RECEIVED"))
            except ValueError:
                state = RequestState.RECEIVED
            k.parent.ledger.rehydrate(req, state, e.get("rule", ""), owner=e.get("owner"),
                                      spawned=e.get("spawned_agent_id"), task_id=e.get("task_id"))
        # The monitoring counters are recomputed from the replayed ledger instead of starting at
        # zero, so `arena agents` on a resumed run reports the whole run's spawn history, not just
        # what happened since the restart. (A fresh zero here would make a resumed kernel look like
        # it had never been asked for anything.)
        k.parent.spawn_stats = k.parent.recount_spawn_stats()
        for w in fold["waits"].values():
            rec = k.registry.get(w["agent_id"])
            if rec is not None:
                rec.pending_waits.append(dict(w))
        for aid, a in fold["agents"].items():
            if a.get("state") == "WAITING_FOR_DEPENDENCY" and aid in k.registry.agents:
                k.registry.agents[aid].lifecycle.state = AgentState.WAITING_FOR_DEPENDENCY
        snap = k.journal.latest_snapshot()
        if snap:
            # the kernel's own snapshot: authoritative for what event projections approximate
            # (tick, message counts). Only consulted on a *completed/stalled* journal, since a
            # snapshot is written at run end - a mid-run journal legitimately has none.
            k.tick = int(snap.get("tick", k.tick))
        # Authoritative restore, last step on purpose: by now fold() has projected every
        # TASK_PROGRESS row, and a checkpoint (when one exists) is preferred over the projection
        # because it records the actor's counters at the moment the run actually stopped.
        src = (snap or {}).get("agents") or fold["agents"]
        for aid, a in src.items():
            if aid in k.actors:
                k.actors[aid].restore_cursor(int(a.get("steps_run", 0) or 0),
                                             int(a.get("policy_cursor", 0) or 0))
        if not quiet:
            k.journal.emit(MessageType.REPLAY_COMPLETE, "parent", "parent",
                           body=f"rebuilt {len(k.registry.agents)} agents / "
                                f"{len(k.graph.tasks)} tasks from {k.journal.count()} journal rows",
                           agents=len(k.registry.agents), tasks=len(k.graph.tasks))
        return k

    @staticmethod
    def _restore_clock_from(fold: dict) -> float:
        return float((fold.get("meta") or {}).get("tick_ts", 0.0))
