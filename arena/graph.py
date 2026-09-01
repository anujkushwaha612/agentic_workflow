"""Dependency DAG (your §5) + the wait-for graph used for deadlock detection (§15).

The important design decision: **task edges are DERIVED, not hand-written.** A task declares the
artifacts it produces and consumes; an edge B->A appears iff A consumes an artifact B produces.
That is the only way "dynamic" is real rather than cosmetic: the Parent never hardcodes
"backend depends on database", it discovers it from what each task needs to exist.
"""
from __future__ import annotations

import graphlib
import itertools
from dataclasses import dataclass, field
from typing import Any, Iterable

OPEN = ("pending", "assigned", "running", "waiting")
DONE = ("done",)


@dataclass
class TaskSpec:
    task_id: str
    title: str
    role: str
    skills: list[str] = field(default_factory=list)
    est_work: float = 1.0
    produces: list[str] = field(default_factory=list)
    consumes: list[str] = field(default_factory=list)
    claims: list[str] = field(default_factory=list)
    deps: set[str] = field(default_factory=set)
    owner: str | None = None
    status: str = "pending"
    # --- Phase 2.5 M2: verification is part of the task, not a favour the worker grants it.
    #: commands (argv lists) that must exit 0 before this task may be completed. Empty means
    #: "nothing to verify", which is what every Phase 1/2 task declares - so the gate is opt-in and
    #: cannot retroactively change how an existing run behaves.
    verify: list[list[str]] = field(default_factory=list)
    verified: bool = False
    verified_at: float | None = None
    #: started exactly once, so TASK_STARTED cannot be double-journaled on a resumed kernel
    started: bool = False
    started_at: float | None = None
    finished_at: float | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def key(self) -> str:
        """Dedup/ownership key for the claim table: what work, on what resources."""
        return "|".join([self.role] + sorted(set(self.produces)) + sorted(self.claims))

    @property
    def is_open(self) -> bool:
        return self.status in OPEN

    def snapshot(self) -> dict[str, Any]:
        return {"task_id": self.task_id, "title": self.title, "role": self.role,
                "status": self.status, "owner": self.owner, "deps": sorted(self.deps),
                "produces": list(self.produces), "consumes": list(self.consumes),
                "est_work": self.est_work, "started": self.started,
                "verify": [list(v) for v in self.verify], "verified": self.verified}


class CycleError(Exception):
    def __init__(self, cycle: Iterable[str]) -> None:
        self.cycle = list(cycle)
        super().__init__("circular dependency: " + " -> ".join(self.cycle))


@dataclass
class DependencyGraph:
    tasks: dict[str, TaskSpec] = field(default_factory=dict)
    #: artifact -> task_ids that produce it
    producers: dict[str, set[str]] = field(default_factory=dict)
    #: the kernel keeps this in sync; gating is done on artifacts, not on task status alone
    known_artifacts: set[str] = field(default_factory=set)

    # ------------------------------------------------------------- mutation
    def add(self, spec: TaskSpec, *, derive: bool = True) -> None:
        if spec.task_id in self.tasks:
            raise ValueError(f"duplicate task_id {spec.task_id}")
        self.tasks[spec.task_id] = spec
        for a in spec.produces:
            self.producers.setdefault(a, set()).add(spec.task_id)
        if derive:
            self.derive_edges()

    def remove(self, task_id: str) -> TaskSpec:
        spec = self.tasks.pop(task_id)
        for a in spec.produces:
            self.producers.get(a, set()).discard(task_id)
        for t in self.tasks.values():
            t.deps.discard(task_id)
        return spec

    def derive_edges(self) -> dict[str, list[str]]:
        """B depends on A if B consumes an artifact A produces. Returns the edges added."""
        added: dict[str, list[str]] = {}
        for tid, spec in self.tasks.items():
            for artifact in spec.consumes:
                for producer in self.producers.get(artifact, ()):
                    if producer != tid and producer not in spec.deps:
                        spec.deps.add(producer)
                        added.setdefault(tid, []).append(producer)
        return added

    def add_edge(self, dependent: str, dependency: str) -> None:
        """Validated edge insert used by PLAN_AMENDED. Rolls back if it creates a cycle."""
        spec = self.tasks[dependent]
        if dependency in spec.deps:
            return
        spec.deps.add(dependency)
        try:
            self.validate()
        except CycleError:
            spec.deps.discard(dependency)
            raise

    def validate(self) -> None:
        try:
            graphlib.TopologicalSorter(self._adj()).prepare()
        except graphlib.CycleError as e:  # args[1] is the offending cycle
            raise CycleError(e.args[1]) from e

    def _adj(self) -> dict[str, set[str]]:
        return {t: {d for d in s.deps if d in self.tasks} for t, s in self.tasks.items()}

    # --------------------------------------------------------------- reading
    def order(self) -> list[list[str]]:
        """Topological generations: each inner list can run in parallel."""
        ts = graphlib.TopologicalSorter(self._adj())
        ts.prepare()
        gens: list[list[str]] = []
        while ts.is_active():
            batch = sorted(ts.get_ready())
            gens.append(batch)
            for b in batch:
                ts.done(b)
        return gens

    def predecessors(self, task_id: str) -> set[str]:
        return {p for p, s in self.tasks.items() if task_id in s.deps}

    def successors(self, task_id: str) -> set[str]:
        return set(self.tasks[task_id].deps)

    def blocked_by(self, task_id: str) -> list[str]:
        return sorted(d for d in self.successors(task_id) if self.tasks[d].is_open)

    def ready(self, *, require_owner: bool = True) -> list[str]:
        out = []
        for tid, s in self.tasks.items():
            if s.status != "pending":
                continue
            if require_owner and s.owner is None:
                continue
            if all(not self.tasks[d].is_open for d in s.deps if d in self.tasks):
                out.append(tid)
        return sorted(out)

    def is_ready(self, task_id: str) -> bool:
        s = self.tasks[task_id]
        return all(not self.tasks[d].is_open for d in s.deps if d in self.tasks)

    def unmet_artifact_producers(self, task_id: str) -> list[str]:
        """Upstream *tasks* whose outputs are not all present yet.

        The task DAG orders work, but only artifacts can gate it - otherwise a task that has
        published everything it promised stays 'pending' forever while its consumer waits on a
        task-level notion of done that nothing ever sets. Keeping one gate (artifacts) removes
        the whole class of task-vs-artifact divergence bugs.
        """
        out: list[str] = []
        for d in sorted(self.tasks[task_id].deps):
            dep = self.tasks.get(d)
            if dep is None:
                continue
            if any(a for a in dep.produces if a not in self._known_artifacts):
                out.append(d)
        return out

    @property
    def _known_artifacts(self) -> set[str]:
        return getattr(self, "known_artifacts", set())

    def unmet(self, task_id: str) -> list[str]:
        """What this task is waiting on, by name - backs `arena why`."""
        s = self.tasks[task_id]
        return [f"{d}:{self.tasks[d].status}" for d in sorted(s.deps)
                if d in self.tasks and self.tasks[d].is_open]

    def remaining_work(self) -> float:
        return sum(t.est_work for t in self.tasks.values() if t.is_open)

    def remaining_work_for(self, role: str) -> float:
        return sum(t.est_work for t in self.tasks.values() if t.is_open and t.role == role)

    def all_artifacts(self) -> dict[str, list[str]]:
        return {a: sorted(t) for a, t in sorted(self.producers.items())}

    def snapshot(self) -> dict[str, Any]:
        return {"order": self.order() if not self.has_cycle() else None,
                "tasks": {t: s.snapshot() for t, s in sorted(self.tasks.items())},
                "artifacts": self.all_artifacts()}

    def has_cycle(self) -> bool:
        try:
            self.validate()
            return False
        except CycleError:
            return True

    # ------------------------------------------------------------ amendment
    def _snapshot(self) -> tuple[dict[str, TaskSpec], dict[str, set[str]]]:
        return ({tid: spec for tid, spec in self.tasks.items()},
                {tid: set(spec.deps) for tid, spec in self.tasks.items()})

    def _restore(self, snap: tuple[dict[str, TaskSpec], dict[str, set[str]]]) -> None:
        self.tasks, deps = snap
        for tid, d in deps.items():
            if tid in self.tasks:
                self.tasks[tid].deps = set(d)
        self.producers = {}
        for tid, spec in self.tasks.items():
            for a in spec.produces:
                self.producers.setdefault(a, set()).add(tid)

    def amend(self, new_tasks: list[TaskSpec],
              new_deps: dict[str, list[str]] | None = None) -> dict[str, Any]:
        """Feature-request path (your §9): mutate the graph, revalidate, and roll back *everything*
        if the result is not a DAG. A partial rollback is worse than none - it leaves edges
        pointing at deleted tasks, which is how a scheduler starts handing out impossible work."""
        before = self._snapshot()
        added: list[str] = []
        try:
            for spec in new_tasks:
                if spec.task_id not in self.tasks:
                    self.add(spec)
                    added.append(spec.task_id)
            # validate before touching edges: an added task can close a cycle all by itself,
            # purely through the artifact it consumes (no explicit new_deps involved).
            self.validate()
            touched: dict[str, set[str]] = {}
            for dependent, deps in (new_deps or {}).items():
                for d in deps:
                    if dependent in self.tasks and d in self.tasks:
                        pre = set(self.tasks[dependent].deps)
                        self.add_edge(dependent, d)
                        touched.setdefault(dependent, set()).update(
                            pre ^ self.tasks[dependent].deps)
            self.validate()
        except CycleError as e:
            self._restore(before)
            return {"ok": False, "rolled_back": added, "cycle": e.cycle, "restored": True}
        return {"ok": True, "added": added,
                "new_edges": {k: sorted(v) for k, v in touched.items()}}

    # -------------------------------------------------- agent wait-for graph
    @staticmethod
    def wait_for_edges(agents: dict[str, dict[str, Any]], graph: "DependencyGraph") -> dict[str, set[str]]:
        """agent -> set(agents) it is directly blocked on.

        A waiting agent blocks on a task; that task's owner blocks it. If the owner is itself
        waiting on something the first agent owns, the two form a wait-for cycle = deadlock.
        """
        edges: dict[str, set[str]] = {}
        for aid, a in agents.items():
            if a["state"] not in ("WAITING_FOR_DEPENDENCY", "BLOCKED"):
                continue
            for wait in a.get("waits", ()):
                tid = wait.get("task_id")
                if not tid or tid not in graph.tasks:
                    continue
                owner = graph.tasks[tid].owner
                if owner and owner != aid and owner in agents:
                    edges.setdefault(aid, set()).add(owner)
        return edges

    @staticmethod
    def find_cycles(edges: dict[str, set[str]]) -> list[list[str]]:
        """Simple DFS cycle enumeration; enough for the (small) agent/lock graphs.

        Self-edges are ignored: an agent awaiting its own artifact is a policy bug to log,
        not a deadlock.
        """
        cycles: list[list[str]] = []
        seen: set[tuple[str, ...]] = set()
        state: dict[str, int] = {}
        stack: list[str] = []

        def dfs(u: str) -> None:
            state[u] = 1
            stack.append(u)
            for v in sorted(edges.get(u, ())):
                if v == u:
                    continue
                if state.get(v, 0) == 1:
                    i = stack.index(v)
                    cyc = tuple(stack[i:])
                    key = frozenset(cyc)
                    if key not in seen:
                        seen.add(key)
                        cycles.append(list(cyc) + [v])
                elif state.get(v, 0) == 0:
                    dfs(v)
            stack.pop()
            state[u] = 2

        for node in sorted(edges):
            if state.get(node, 0) == 0:
                dfs(node)
        return cycles

    def critical_path_length(self) -> int:
        """Longest chain of open tasks - the Parent's bottleneck signal (§14)."""
        memo: dict[str, int] = {}

        def depth(tid: str) -> int:
            if tid in memo:
                return memo[tid]
            spec = self.tasks[tid]
            if not spec.is_open:
                memo[tid] = 0
                return 0
            subs = [depth(d) for d in spec.deps if d in self.tasks]
            memo[tid] = 1 + (max(subs) if subs else 0)
            return memo[tid]

        return max((depth(t) for t in self.tasks), default=0)


def artifact_key(artifact: str) -> str:
    return f"artifact:{artifact}"


def _selftest() -> None:  # pragma: no cover
    g = DependencyGraph()
    g.add(TaskSpec("t_db", "schema", "database", produces=["database/schema.sql"]))
    g.add(TaskSpec("t_api", "api", "backend", consumes=["database/schema.sql"],
                   produces=["contracts/api.json"]))
    g.add(TaskSpec("t_fe", "ui", "frontend", consumes=["contracts/api.json"]))
    g.add(TaskSpec("t_infra", "iac", "cloud", produces=["deploy/main.tf"]))
    print("derived order:", g.order())
    print("t_api deps (auto):", g.tasks["t_api"].deps)
    try:
        g.add_edge("t_db", "t_fe")
    except CycleError as e:
        print("cycle correctly rejected:", e)
    other = {"a1": {"state": "WAITING_FOR_DEPENDENCY", "waits": [{"task_id": "t_x"}]},
             "a2": {"state": "WAITING_FOR_DEPENDENCY", "waits": [{"task_id": "t_y"}]}}
    g2 = DependencyGraph()
    g2.add(TaskSpec("t_x", "x", "r1", owner="a2"))
    g2.add(TaskSpec("t_y", "y", "r2", owner="a1"))
    print("wait-for edges:", DependencyGraph.wait_for_edges(other, g2))
    print("deadlock cycles:", DependencyGraph.find_cycles(DependencyGraph.wait_for_edges(other, g2)))


if __name__ == "__main__":
    _selftest()
