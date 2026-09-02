"""Registry golden capture: exercises every observable AgentRegistry behavior and
dumps it as JSON so the Rust port (`tests/registry_parity.rs`) can be compared
against the Python oracle structurally.

Regenerate: python3 tests/parity/registry_capture.py
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from arena.graph import DependencyGraph, TaskSpec
from arena.journal import Journal
from arena.lifecycle import AgentState, Lifecycle
from arena.registry import (AgentRegistry, SpawnBudget, emit_registered,
                            emit_terminated)

FIX = Path(__file__).parent / "fixtures" / "registry_golden.json"


def main() -> int:
    g = DependencyGraph()
    g.add(TaskSpec("big", "lots of work", "backend", est_work=10.0))
    g.add(TaskSpec("t2", "other", "b", est_work=1.0))
    g.add(TaskSpec("closed", "done thing", "backend", est_work=8.0))
    g.tasks["closed"].status = "done"

    r = AgentRegistry(graph=g, budget=SpawnBudget(max_active_agents=3, max_spawn_epoch=2,
                                                min_share_of_remaining=0.25, idle_ttl=1.0))

    out: dict = {}

    # ---- id generation (incl. the '/' quirk: next_id does not fold slashes)
    out["next_ids"] = [r.next_id("Frontend Engineer"), r.next_id("Frontend Engineer"),
                       r.next_id("ml engineer"), r.next_id("dev ops/infra"),
                       r.next_id("ml engineer")]

    def add(aid, role, skills=(), epoch=0, state=AgentState.IDLE, since=0.0,
            spawned_by="parent", reason=""):
        return r.register(agent_id=aid, role=role, skills=list(skills), epoch=epoch,
                          spawned_by=spawned_by, spawn_reason=reason,
                          lifecycle=Lifecycle(state=state, since=since))

    add("be", "backend", ["api", "http"])
    add("generalist", "backend", ["api"])
    add("specialist", "auth engineer", ["auth", "oauth", "jwt"])
    add("idle", "x", state=AgentState.IDLE, since=0.0)
    add("busy", "y", state=AgentState.WORKING, since=0.0)
    add("done_guy", "z", state=AgentState.COMPLETED, since=0.25)
    add("ghost", "gone", state=AgentState.TERMINATED, since=0.0)  # not popped
    add("paused", "p", state=AgentState.PAUSED, since=0.0)
    add("newbie", "n", state=AgentState.CREATED, since=0.0)
    add("boot", "init", state=AgentState.INITIALIZING, since=0.0)
    add("stuck", "s", state=AgentState.BLOCKED, since=0.0)
    add("up", "e", state=AgentState.ESCALATED, since=0.0)
    add("waiter", "w", state=AgentState.WAITING_FOR_DEPENDENCY, since=0.0)
    add("db_02", "database engineer", ["sql", "schema"], epoch=1,
        spawned_by="backend_01", reason="plan task t_db")

    # duplicate rejection message
    try:
        add("be", "backend")
        out["duplicate_error"] = "<no error>"
    except ValueError as e:
        out["duplicate_error"] = str(e)

    # ---- cover
    out["cover"] = {
        "backend": [a.agent_id for a in r.cover("backend", [])],
        "api design": [a.agent_id for a in r.cover("api design", ["api"])],
        "oauth auth flow": [a.agent_id for a in r.cover("oauth auth flow", ["auth", "oauth"])],
        "database": [a.agent_id for a in r.cover("database", [])],
        "crew_0": [a.agent_id for a in r.cover("crew_0", [])],
        "empty": [a.agent_id for a in r.cover("", [])],
        "payments stripe": [a.agent_id for a in r.cover("payments", ["stripe"])],
        "sql tuning": [a.agent_id for a in r.cover("sql tuning", ["sql"])],
    }

    # ---- membership queries (insertion order preserved)
    out["active"] = [a.agent_id for a in r.active()]
    out["with_state_IDLE"] = [a.agent_id for a in r.with_state(AgentState.IDLE)]
    out["with_state_WORKING"] = [a.agent_id for a in r.with_state(AgentState.WORKING)]
    out["working_count"] = r.working_count()

    # ---- queues / load
    r.agents["be"].task_id = "big"
    r.agents["be"].task_queue = ["t2", "big", "extra"]
    rec = r.agents["be"]
    out["pending_work"] = rec.pending_work
    out["load"] = rec.load

    # ---- overload (graph borrowed per call)
    g.tasks["big"].owner = "be"
    g.tasks["closed"].owner = "be"
    g.tasks["t2"].owner = "idle"
    out["overloaded"] = {
        "be@0.0": r.overloaded("be"),
        "idle": r.overloaded("idle"),
        "unknown": r.overloaded("nobody"),
    }
    r.agents["be"].work_done = 9.0
    out["overloaded"]["be@9.0"] = r.overloaded("be")
    r.agents["be"].work_done = 0.0

    # ---- idle TTL
    out["idle_overdue"] = {
        "0.5": [a.agent_id for a in r.idle_overdue(0.5)],
        "1.5": [a.agent_id for a in r.idle_overdue(1.5)],
        "0.3": [a.agent_id for a in r.idle_overdue(0.3)],
    }

    # ---- waits + wait-for edges (BLOCKED and WAITING both participate)
    r.agents["waiter"].pending_waits = [{"task_id": "t2", "condition": "artifact:t2.done"},
                                        {"condition": "tick:9"}]
    r.agents["stuck"].pending_waits = [{"task_id": "big", "condition": "artifact:big.out"}]
    r.agents["idle"].pending_waits = [{"task_id": "big"}]  # IDLE: ignored by the edge rule
    g.tasks["big"].owner = "waiter"
    out["wait_for_edges"] = {k: sorted(v) for k, v in r.wait_for_edges().items()}

    # ---- snapshot (floats exercise round4/round6; missing condition → null)
    r.agents["db_02"].lifecycle.since = 0.123456789
    r.agents["db_02"].work_done = 1.23456789
    r.agents["db_02"].msgs_sent = 7
    r.agents["db_02"].progress_steps = 3
    r.agents["db_02"].cognition = {"kind": "policy", "name": "simulated"}
    out["snapshot"] = r.snapshot()

    # ---- status rendering (all glyphs, epoch sort, task suffix)
    out["status_lines"] = r.status_lines()

    # ---- journal emit payload parity (canonical strings from the row)
    j = Journal()
    emit_registered(j, r.agents["db_02"])
    emit_terminated(j, r.agents["db_02"], "idle reap")
    rows = list(j.iterate())
    out["emit_registered_payload"] = rows[0]["payload"]
    out["emit_terminated_payload"] = rows[1]["payload"]
    out["emit_types"] = [rows[0]["etype"], rows[1]["etype"]]
    out["emit_targets"] = [rows[0]["target"], rows[1]["target"]]

    # ---- terminate + version
    v0 = r.version
    rec = r.terminate("idle", "ttl")
    out["terminate"] = {
        "returned": rec.agent_id if rec is not None else None,
        "version_bumped": r.version > v0,
        "gone": r.get("idle") is None,
    }

    with FIX.open("w") as f:
        json.dump(out, f, indent=1, sort_keys=True, default=str)
    print(f"wrote {FIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
