"""`arena-code` orchestration: plan data in, a running organisation out.

This is the only module that joins the launcher's world (a prompt, a project directory) to the
engine's world (a kernel, a task graph, agents). It contains no scheduling, no lifecycle and no
execution logic of its own - it *configures* those, then gets out of the way. That is what keeps the
kernel provider-agnostic: nothing here is LLM-specific, and nothing in `arena/` knows this file
exists.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..graph import TaskSpec
from ..kernel import Kernel
from ..message import MessageType
from ..registry import SpawnBudget, emit_registered
from ..tools import make_jail
from .agent import CodingAgent
from .project import Project

DEFAULT_BUDGET = {"max_active_agents": 8, "max_concurrent_workers": 3, "max_spawn_epoch": 3,
                  "idle_ttl": 1.0e9}


def load_plan(path: str | Path | None = None) -> dict[str, Any]:
    """`--plan file.json`, or the packaged acceptance plan. A plan is *data*: a task graph plus the
    files each agent owes. Nothing here interprets the prompt text to decide the plan, so this tier
    is not a keyword planner wearing a coat."""
    if path:
        raw = json.loads(Path(path).read_text())
    else:
        from .plans import calc
        raw = calc.plan()
    for a in raw.get("agents", []):
        if not a.get("task", {}).get("task_id"):
            raise ValueError(f"plan agent {a.get('agent_id')!r} has no task.task_id")
        if not a.get("seed"):
            raise ValueError(f"plan agent {a.get('agent_id')!r} has no seed - an agent that owes no "
                             f"files has nothing to be verified against")
    return raw


@dataclass
class RunResult:
    project: Project
    kernel: Any = None
    summary: dict[str, Any] = field(default_factory=dict)
    verification: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    ok: bool = False
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "project": str(self.project.root),
                "project_id": self.project.manifest.get("project_id"),
                "state": self.project.manifest.get("state"),
                "summary": self.summary, "verification": self.verification,
                "notes": self.notes}


def organise(workspace: str | Path, goal: str, *, plan_path: str | Path | None = None,
             ticks: int = 60, project_id_hint: str = "", resume: bool = False,
             allow_egress: bool = False, clock_mode: str = "wall",
             provider: str = "policy", model: str = "", out: str | Path | None = None,
             verbose: bool = True) -> RunResult:
    """Create-or-resume the project, start the Parent, bind tools + cognition, run, verify."""
    plan = load_plan(plan_path)
    goal = goal or plan.get("goal", "")
    if resume and project_id_hint:
        project = Project.load(workspace, project_id_hint)
        project.set_state("resuming", at=time.time())
    else:
        project = Project.create(workspace, goal, project_id_hint=project_id_hint,
                                 meta={"plan": str(plan_path or "packaged:calc"),
                                       "provider": provider, "model": model})
        # keep the plan itself (not just a path to it) in the manifest: `arena-code verify` re-runs the
    # recorded verify commands in a fresh process, and reading them back off disk would let the
    # packager decide what "verified" means instead of the run that produced the work.
    project.update(plan=plan, plan_source=str(plan_path or "packaged:calc"))
    journal = project.journal_path
    if journal.exists():
        before = _row_count(journal)
    else:
        before = 0

    budget = SpawnBudget(**{**DEFAULT_BUDGET,
                            "max_active_agents": max(8, 2 * len(plan["agents"]) + 4)})
    k = Kernel(root=project.arena_dir, journal_path=journal, budget=budget,
               clock_mode=clock_mode, stall_limit=max(8, ticks // 2),
               detect_deadlocks=False, auto_assign=True, reap=False)
    k.transcript_dir = str(project.context_dir)
    k.logs_dir = str(project.logs_dir)
    k.tool_config = {"allow_egress": allow_egress, "max_out": 6000}
    project.update(kernel_root=str(project.arena_dir), journal=str(journal))

    # ---- the task graph (derived edges, as always) ------------------------------------------
    specs: dict[str, TaskSpec] = {}
    for a in plan["agents"]:
        t = a["task"]
        specs[t["task_id"]] = TaskSpec(task_id=t["task_id"], title=t["title"], role=t["role"],
                                       skills=list(t.get("skills", [])),
                                       est_work=float(t.get("est_work", 2.0)),
                                       produces=list(t.get("produces", [])),
                                       consumes=list(t.get("consumes", [])),
                                       verify=[list(v) for v in (t.get("verify") or [])])
    for tid, spec in specs.items():
        k.graph.add(spec, derive=False)
    k.graph.derive_edges()
    k.graph.validate()
    k.journal.emit(MessageType.PLAN_CREATED, "parent", "parent",
                   body=f"arena-code plan: {len(specs)} task(s) from "
                        f"{project.manifest.get('plan')}",
                   tasks=[dict(s.snapshot(), claims=list(s.claims), skills=list(s.skills),
                               consumes=list(s.consumes)) for s in specs.values()])

    # ---- the agents: registry, actor, jail, cognition --------------------------------------
    for a in plan["agents"]:
        aid, role = a["agent_id"], a["role"]
        rec = k.registry.register(agent_id=aid, role=role, skills=a.get("skills", [role]))
        emit_registered(k.journal, rec)
        k.make_actor(aid)
        src = project.source_dir
        k.bind_tools(aid, root=src, writes=tuple(a.get("writes", [""])),
                     reads=tuple(a.get("reads", [""])),
                     allowed=a.get("allowed_tools") or None, git_root=src)
        k.ensure_git(aid)
        pol = a.get("policy", {})
        source = CodingAgent(seed=dict(a.get("seed") or {}),
                             test_command=tuple(pol.get("test_command") or ("true",)),
                             wait_for=pol.get("wait_for", ""),
                             # the task's declared inputs, handed to the policy so a WAIT is only
                             # ever proposed on something the runtime agrees is missing
                             consumes=tuple(a["task"].get("consumes") or ()),
                             fix_files=tuple(a.get("fix_files") or ()),
                             role=role)
        k.bind_cognition(aid, source)
        k.parent.assign(a["task"]["task_id"], aid, reason="arena-code plan",
                        correlation_id=f"arena-code:{a['task']['task_id']}")
    k.parent.recount_spawn_stats()

    if verbose:
        print(f"arena-code: project {project.manifest['project_id']} at {project.root}")
        print(f"arena-code: {len(specs)} task(s), {len(plan['agents'])} agent(s), "
              f"cognition={provider}")
    project.set_state("running")
    t0 = time.monotonic()
    out_run = k.run(ticks=ticks)
    wall = time.monotonic() - t0

    # ---- verification, by the runtime, on the merged tree -----------------------------------
    verification = _verify_all(k, plan, project)
    committed = _commit_all(k, plan, project) if verification["ok"] and plan.get(
        "commit_after_verify") else {"skipped": "not verified or plan opted out"}
    state = "verified" if verification["ok"] else "awaiting-cortex"
    if verification["ok"] and all(not t.is_open for t in k.graph.tasks.values()):
        state = "completed"
    project.set_state(state, verified=verification["ok"], wall_s=round(wall, 3),
                      ticks=out_run.get("tick"), commit=committed)
    project.update(execution={"subprocess_executions": k.tools.stats["executions"],
                             "commands": k.tools.stats["commands"],
                             "test_runs": k.tools.stats["tests"],
                             "file_mutations": k.tools.stats["file_mutations"],
                             "refusals": k.tools.stats["refusals"],
                             "violations": k.tools.stats["violations"],
                             "timeouts": k.tools.stats["timeouts"]},
                  events=_row_count(journal) - before, wall_s=round(wall, 3))
    res = RunResult(project=project, kernel=k, summary=out_run, verification=verification,
                    events=_chain(k), ok=bool(verification["ok"] and state == "completed"),
                    notes=[f"wall {wall:.2f}s", f"commit: {committed}"])
    if out:
        Path(out).write_text(render_report(res))
    return res


# --------------------------------------------------------------------------- helpers
def _row_count(path: Path) -> int:
    import sqlite3
    if not Path(path).exists():
        return 0
    con = sqlite3.connect(str(path))
    try:
        return con.execute("select count(*) from events").fetchone()[0]
    finally:
        con.close()


def _verify_all(k: Kernel, plan: dict[str, Any], project: Project) -> dict[str, Any]:
    """Re-run each task's declared commands from the *runtime*, then check the files exist."""
    results, ok = [], True
    for a in plan["agents"]:
        tid = a["task"]["task_id"]
        t = k.graph.tasks.get(tid)
        cmds = list(getattr(t, "verify", None) or [])
        one = {"task_id": tid, "commands": [], "files": {}, "open": bool(t and t.is_open)}
        for argv in cmds:
            r = k.tools.execute(a["agent_id"], "run_tests", {"argv": list(argv)},
                                task_id=tid)
            one["commands"].append({"argv": list(argv), "exit": r.exit_code, "ok": bool(r.ok),
                                    "stderr_tail": (r.stderr or "")[-240:]})
            ok = ok and bool(r.ok)
        for rel in (t.produces if t else []):
            p = project.source_dir / rel
            one["files"][rel] = {"exists": p.is_file(), "bytes": p.stat().st_size if p.is_file() else 0,
                                 "sha256_16": __import__("arena.tools", fromlist=["file_digest"])
                                 .file_digest(p) if p.is_file() else ""}
            ok = ok and p.is_file()
        results.append(one)
    return {"ok": ok, "tasks": results, "at": time.time()}


def _commit_all(k: Kernel, plan: dict[str, Any], project: Project) -> dict[str, Any]:
    """Commit the verified tree from the project's own repo, through the tool layer."""
    aid = plan["agents"][0]["agent_id"]
    res = k.tools.execute(aid, "commit", {"message": "arena-code: verified project (runtime commit)",
                                          "paths": ["."], "author_name": "arena-code"},
                          rid="runtime")
    return {"ok": bool(res.ok), "exit": res.exit_code, "detail": (res.stdout or res.stderr)[-160:]}


def _chain(k: Kernel) -> list[dict[str, Any]]:
    rows = []
    for r in k.journal.iterate():
        et = r["etype"] if "etype" in r else r.get("msg_type")
        if et in ("TOOL_CALL", "TOOL_RESULT", "TOOL_REFUSED", "COMPLETION_REFUSED",
                  "TASK_VERIFIED", "COGNITION_VIOLATION", "PROJECT_CREATED", "WORKSPACE_BOUND",
                  "COGNITION_BOUND", "TASK_COMPLETED", "SPAWN_REQUEST_RECEIVED", "SPAWN_APPROVED",
                  "AGENT_REGISTERED", "ERROR_REPORT", "COGNITION_ERROR"):
            rows.append({"seq": r["seq"], "etype": et, "actor": r["actor"],
                         "body": (r["payload"] or {}).get("body", "")})
    return rows


def render_report(res: RunResult) -> str:
    m = res.project.manifest
    v = res.verification
    lines = [
        f"# arena-code acceptance report — `{m.get('project_id')}`",
        "",
        f"- **prompt**: {m.get('prompt')!r}",
        f"- **runtime**: {m.get('runtime_version')} · created {m.get('created_at_iso')}",
        f"- **cognition**: provider=`{m.get('provider')}` model=`{m.get('model') or '—'}` "
        f"(deterministic tier; no LLM was consulted)",
        f"- **state**: `{m.get('state')}` · result: **{'VERIFIED' if res.ok else 'NOT VERIFIED'}**",
        f"- **project root**: `{res.project.root}`",
        "",
        "## Real execution",
        "",
        f"- subprocess executions: **{m['execution']['subprocess_executions']}**",
        f"- commands run: {m['execution']['commands']} · test runs: {m['execution']['test_runs']}",
        f"- filesystem mutations observed: **{m['execution']['file_mutations']}**",
        f"- tool refusals: {m['execution']['refusals']} · cognition violations: "
        f"{m['execution']['violations']} · timeouts: {m['execution']['timeouts']}",
        f"- journal rows appended: {m['events']} · wall clock {res.notes[0] if res.notes else ''}",
        "",
        "## Verification (executed by the runtime, outside any agent's policy)",
        "",
    ]
    for t in v.get("tasks", []):
        lines.append(f"- `{t['task_id']}` open={t['open']}")
        for c in t["commands"]:
            lines.append(f"  - `{' '.join(c['argv'])}` → exit **{c['exit']}** "
                         f"{'✅' if c['ok'] else '❌ ' + c['stderr_tail'][:120]}")
        for rel, f in t["files"].items():
            lines.append(f"  - `{rel}` exists={f['exists']} bytes={f['bytes']} "
                         f"sha256[:16]={f['sha256_16']}")
    lines += ["", "## Event chain (execution-relevant rows only)", "",
              "| seq | event | actor | body |", "|---|---|---|---|"]
    for e in res.events:
        lines.append(f"| {e['seq']} | {e['etype']} | {e['actor']} | {str(e['body'])[:96]} |")
    return "\n".join(lines) + "\n"
