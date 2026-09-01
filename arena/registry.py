"""Agent Registry / Agent Pool (your §2) with spawn-lineage accounting.

Two separate limits, because they mean different things (§3.1 of FEASIBILITY.md):
  max_active_agents      - organisational ambition: how many engineers the Parent will manage
  max_concurrent_workers - hardware reality: how many may hold an execution slot (2 cores here)

Spawn lineage (epoch + parent) is what makes "infinite spawning" structurally impossible rather
than merely discouraged: agents at max epoch cannot request agents, and total spawns are also
bounded by a work-estimate budget, so churning short-lived agents to dodge a cap does not work.
Guard names asserted by the chaos suite: `max_spawn_epoch` (lineage depth), `idle` TTL reaping
(idle_ttl -> AGENT_TERMINATED), `max_active_agents` (registry size).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .graph import DependencyGraph
from .lifecycle import AgentState, Lifecycle
from .message import MessageType


@dataclass
class AgentRecord:
    agent_id: str
    role: str
    skills: set[str] = field(default_factory=set)
    epoch: int = 0
    spawned_by: str = "parent"
    spawn_reason: str = ""
    task_id: str | None = None
    lifecycle: Lifecycle = field(default_factory=Lifecycle)
    pending_waits: list[dict[str, Any]] = field(default_factory=list)
    #: backlog: a specialist given two tasks must not be forced to drop one (real teams queue work)
    task_queue: list[str] = field(default_factory=list)
    subscriptions: list[str] = field(default_factory=list)
    msgs_sent: int = 0
    work_done: float = 0.0
    progress_steps: int = 0
    notes: list[str] = field(default_factory=list)
    #: Phase 2.5: how this agent decides, as data. Journaled at bind time and projected by fold(),
    #: so `arena status` on a *replayed* kernel still reports whether the org had real cognition.
    cognition: dict[str, Any] = field(default_factory=dict)

    @property
    def state(self) -> str:
        return str(self.lifecycle.state)

    @property
    def pending_work(self) -> list[str]:
        return ([self.task_id] if self.task_id else []) + [t for t in self.task_queue
                                                            if t != self.task_id]

    @property
    def load(self) -> int:
        return len(self.pending_work)

    def snapshot(self) -> dict[str, Any]:
        return {"agent_id": self.agent_id, "role": self.role,
                "skills": sorted(self.skills), "state": str(self.lifecycle.state),
                "state_since": round(self.lifecycle.since, 4), "epoch": self.epoch,
                "spawned_by": self.spawned_by, "task_id": self.task_id,
                "msgs_sent": self.msgs_sent, "work_done": round(self.work_done, 6),
                "queue": list(self.task_queue), "load": self.load,
                "cognition": dict(self.cognition),
                "waiting_on": [w.get("condition") for w in self.pending_waits]}


@dataclass
class SpawnBudget:
    max_active_agents: int = 8
    max_concurrent_workers: int = 2
    max_spawn_epoch: int = 3
    #: a new agent must be worth at least this fraction of the graph's remaining work
    min_share_of_remaining: float = 0.10
    #: an agent may not ask for help while it is itself this far over its own estimate
    requester_overload_factor: float = 2.0
    #: idle/completed agents are reaped after this much unproductive time
    idle_ttl: float = 5.0


@dataclass
class AgentRegistry:
    agents: dict[str, AgentRecord] = field(default_factory=dict)
    budget: SpawnBudget = field(default_factory=SpawnBudget)
    graph: DependencyGraph | None = None
    _seq: dict[str, int] = field(default_factory=dict)
    #: bumped on every mutation; replay tests assert live.version == replay.version
    version: int = 0

    # ------------------------------------------------------------- creation
    def next_id(self, role: str) -> str:
        slug = role.strip().lower().replace(" ", "_").replace("-", "_")
        n = self._seq.get(slug, 0) + 1
        self._seq[slug] = n
        return f"{slug}_{n:02d}"

    def register(self, *, agent_id: str, role: str, skills: Iterable[str] = (), epoch: int = 0,
                 spawned_by: str = "parent", spawn_reason: str = "",
                 lifecycle: Lifecycle | None = None,
                 subscriptions: Iterable[str] = ()) -> AgentRecord:
        if agent_id in self.agents:
            raise ValueError(f"agent {agent_id} already registered")
        rec = AgentRecord(agent_id=agent_id, role=role, skills=set(skills), epoch=epoch,
                          spawned_by=spawned_by, spawn_reason=spawn_reason,
                          lifecycle=lifecycle or Lifecycle(),
                          subscriptions=list(subscriptions))
        rec.notes.append(f"registered by {spawned_by}: {spawn_reason or 'planned'}")
        self.agents[agent_id] = rec
        self.version += 1
        return rec

    def get(self, agent_id: str) -> AgentRecord | None:
        return self.agents.get(agent_id)

    def require(self, agent_id: str) -> AgentRecord:
        rec = self.agents.get(agent_id)
        if rec is None:
            raise KeyError(f"unknown agent {agent_id!r}")
        return rec

    def terminate(self, agent_id: str, reason: str = "") -> AgentRecord | None:
        rec = self.agents.pop(agent_id, None)
        if rec is not None:
            rec.notes.append(f"terminated: {reason or 'unspecified'}")
            self.version += 1
        return rec

    # -------------------------------------------------------------- queries
    def active(self) -> list[AgentRecord]:
        return [a for a in self.agents.values() if a.lifecycle.state != AgentState.TERMINATED]

    def with_state(self, *states: AgentState | str) -> list[AgentRecord]:
        want = {str(s) for s in states}
        return [a for a in self.agents.values() if str(a.lifecycle.state) in want]

    @staticmethod
    def _tokens(role: str, skills: Iterable[str]) -> set[str]:
        toks = {t for t in role.replace("-", "_").replace("/", "_").split("_") if len(t) > 2}
        toks |= {s.lower() for s in skills}
        return toks

    def cover(self, role: str, skills: Iterable[str] = ()) -> list[AgentRecord]:
        """Agents whose skill set covers the ask, best-matching first. Backs the 'can an existing
        agent do this?' question in your §10 evaluation."""
        need = self._tokens(role, skills)
        scored: list[tuple[int, int, AgentRecord]] = []
        need_toks = {t for t in str(role).lower().replace("-", "_").replace("/", "_").split("_")
                     if len(t) > 2}
        for a in self.active():
            have = self._tokens(a.role, a.skills)
            overlap = len(need & have)
            a_toks = {t for t in a.role.lower().replace("-", "_").replace("/", "_").split("_")
                      if len(t) > 2}
            # Raw substring matching is wrong: '' in 'backend' is True, so an unskilled role
            # silently 'covered' everything. Compare whole role tokens.
            name_hit = int(bool(need_toks & a_toks))
            if overlap or name_hit:
                scored.append((-(overlap + 2 * name_hit), a.load, a))
        return [a for _, _, a in sorted(scored, key=lambda x: (x[0], x[2].agent_id))]

    def overloaded(self, agent_id: str) -> bool:
        a = self.agents.get(agent_id)
        if a is None or self.graph is None:
            return False
        mine = sum(t.est_work for t in self.graph.tasks.values()
                   if t.owner == agent_id and t.is_open)
        return mine > self.budget.requester_overload_factor * max(a.work_done, 0.25)

    def idle_overdue(self, now: float) -> list[AgentRecord]:
        out = []
        for a in self.active():
            if a.lifecycle.state in (AgentState.IDLE, AgentState.COMPLETED):
                if now - a.lifecycle.since >= self.budget.idle_ttl:
                    out.append(a)
        return out

    def working_count(self) -> int:
        return len(self.with_state(AgentState.WORKING))

    # ------------------------------------------------------------- rendering
    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {a.agent_id: a.snapshot()
                for a in sorted(self.agents.values(), key=lambda x: x.agent_id)}

    def status_lines(self) -> list[str]:
        glyphs = {AgentState.WORKING: "*", AgentState.IDLE: "o",
                  AgentState.WAITING_FOR_DEPENDENCY: "~", AgentState.BLOCKED: "!",
                  AgentState.ESCALATED: "^", AgentState.PAUSED: "=",
                  AgentState.COMPLETED: "+", AgentState.CREATED: ".",
                  AgentState.INITIALIZING: ".", AgentState.TERMINATED: "x"}
        lines = []
        for a in sorted(self.agents.values(), key=lambda x: (x.epoch, x.agent_id)):
            glyph = glyphs.get(a.lifecycle.state, "?")
            suffix = f"  [{a.task_id}]" if a.task_id else ""
            lines.append(f"{a.agent_id:<22} {a.role:<18} {glyph} {a.state}{suffix}")
        return lines

    def wait_for_edges(self) -> dict[str, set[str]]:
        """agent -> agents it is directly blocked on, for deadlock detection (§15)."""
        if self.graph is None:
            return {}
        plain = {aid: {"state": a.state, "waits": a.pending_waits}
                 for aid, a in self.agents.items()}
        return DependencyGraph.wait_for_edges(plain, self.graph)

    # ---------------------------------------------------------- journal sync
    def sync_from(self, other: "AgentRegistry") -> None:
        """Adopt a replayed registry (used by the crash/replay test)."""
        self.agents = dict(other.agents)
        self._seq = dict(other._seq)
        self.version = other.version


def emit_registered(journal: Any, rec: AgentRecord) -> None:
    journal.emit(MessageType.AGENT_REGISTERED, "parent", rec.agent_id,
                 agent_id=rec.agent_id, role=rec.role, skills=sorted(rec.skills),
                 epoch=rec.epoch, spawned_by=rec.spawned_by, reason=rec.spawn_reason)


def emit_terminated(journal: Any, rec: AgentRecord, reason: str) -> None:
    journal.emit(MessageType.AGENT_TERMINATED, "parent", rec.agent_id,
                 agent_id=rec.agent_id, reason=reason)


def _selftest() -> None:  # pragma: no cover
    from .graph import TaskSpec
    g = DependencyGraph()
    g.add(TaskSpec("t1", "api", "backend", est_work=4.0))
    reg = AgentRegistry(graph=g, budget=SpawnBudget())
    a = reg.register(agent_id="backend_01", role="backend", skills=["api"])
    reg.register(agent_id="security_01", role="security engineer", skills=["auth", "crypto"])
    print("cover('security','auth'):", [x.agent_id for x in reg.cover("security", ["auth"])])
    print("next_id('ml engineer'):", reg.next_id("ml engineer"))
    print("status:\n  " + "\n  ".join(reg.status_lines()))
    a.task_id, g.tasks["t1"].owner = "t1", "backend_01"
    print("overloaded (no progress yet):", reg.overloaded("backend_01"))
    a.work_done = 3.0
    print("overloaded (after progress):", reg.overloaded("backend_01"))


if __name__ == "__main__":
    _selftest()
