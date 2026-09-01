#!/usr/bin/env python3
"""Capture golden parity fixtures from the Python reference implementation.

Run:  python3 tests/parity/capture.py
Writes into tests/parity/fixtures/<name>/ :
    journal.db   the SQLite event log of the scenario (virtual clock => deterministic
                 except uuid-derived mids/correlation ids, which the parity
                 comparison deliberately excludes)
    fold.json    journal.fold() output
    snapshot.json  kernel.snapshot() output
    events.csv   seq,etype,actor,target,task_id,resource per row (mid-free)
    meta.json    scenario parameters

The Rust implementation must reproduce fold/snapshot/events from journal.db alone.
"""
from __future__ import annotations

import csv
import json
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from arena.kernel import Kernel  # noqa: E402
from arena.registry import SpawnBudget  # noqa: E402

FIX = Path(__file__).resolve().parent / "fixtures"
SAAS = ("Build a SaaS application with authentication, dashboard, API backend, PostgreSQL "
        "database, and cloud deployment")
ML = ("Train an ML model for churn prediction: data pipeline, feature engineering, "
      "model optimization, evaluation harness")


def _mk(root: Path, **kw) -> Kernel:
    root.mkdir(parents=True, exist_ok=True)
    return Kernel(root=str(root), journal_path=str(root / "journal.db"),
                  budget=kw.pop("budget", None) or SpawnBudget(
                      max_active_agents=6, max_concurrent_workers=2, max_spawn_epoch=3,
                      idle_ttl=1.0e9),
                  clock_mode="virtual", clock_step=0.01, **kw)


def _dump(k: Kernel, out: Path, ticks_run: int, extra: dict | None = None) -> None:
    fold = k.journal.fold()
    snap = k.snapshot()
    with (out / "fold.json").open("w") as f:
        json.dump(fold, f, indent=1, sort_keys=True, default=str)
    with (out / "snapshot.json").open("w") as f:
        json.dump(snap, f, indent=1, sort_keys=True, default=str)
    with (out / "events.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seq", "etype", "actor", "target", "task_id", "resource"])
        for r in k.journal.iterate():
            w.writerow([r["seq"], r["etype"], r["actor"], r["target"] or "",
                        r["task_id"] or "", r["resource"] or ""])
    meta = {"ticks": ticks_run, "chain_ok": k.journal.verify_chain()[0],
            "summary": {kk: vv for kk, vv in k.summary().items() if kk != "chain_ok"},
            "events": k.journal.count()}
    meta.update(extra or {})
    with (out / "meta.json").open("w") as f:
        json.dump(meta, f, indent=1, sort_keys=True, default=str)


def scenario_saas() -> None:
    out = FIX / "saas"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    k = _mk(out)
    k.submit(SAAS)
    k.run(400)
    _dump(k, out, 400)


def scenario_ml() -> None:
    out = FIX / "ml"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    k = _mk(out)
    k.submit(ML)
    k.run(400)
    _dump(k, out, 400)


def scenario_midrun() -> None:
    """A mid-run spawn request injected from outside, answered inside one kernel."""
    out = FIX / "midrun"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    k = _mk(out, role_overrides={"backend": {"policy": "needs-specialist",
                                              "role": "payment specialist",
                                              "capability_class": "payments",
                                              "outputs": ("artifacts/payments-webhooks.md",),
                                              "after_steps": 2}})
    k.submit(SAAS)
    k.run(6)
    k.inject_spawn_request({"from": "parent", "requested_role": "database tuner",
                            "reason": "query planning is outside this roster",
                            "skills": ["database", "tuning"], "work_estimate": 3.0,
                            "produces": ["database/tuning.md"]})
    k.run(120)
    _dump(k, out, 126)


def scenario_crash_resume() -> None:
    out = FIX / "crash_resume"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    k = _mk(out)
    k.submit(SAAS)
    k.run(25)
    _dump(k, out, 25, extra={"phase": "pre-crash"})
    # a second process rebuilds from the same journal and finishes the run
    k2 = Kernel.from_journal(out / "journal.db", root=str(out), quiet=False,
                             budget=SpawnBudget(max_active_agents=6,
                                                max_concurrent_workers=2,
                                                max_spawn_epoch=3, idle_ttl=1e9))
    out2 = out / "resumed"
    out2.mkdir(exist_ok=True)
    shutil.copy(out / "journal.db", out2 / "journal.db")
    k2.journal.path = out2 / "journal.db"  # keep writing to the copy
    k2.run(400)
    _dump(k2, out2, 400, extra={"phase": "resumed"})


def scenario_cli_plan() -> None:
    from arena.parent import RuleBasedPlanner
    from arena.graph import DependencyGraph
    out = FIX / "cli_plan"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    tasks, rationale = RuleBasedPlanner().plan(SAAS)
    g = DependencyGraph()
    for t in tasks:
        g.add(t)
    with (out / "plan.json").open("w") as f:
        json.dump({"roles": sorted({t.role for t in tasks}),
                   "generations": g.order(),
                   "tasks": {t.task_id: t.snapshot() for t in tasks},
                   "why": rationale["matched"], "edges": rationale["artifact_edges"]},
                  f, indent=1, sort_keys=True, default=str)


def scenario_e2e_calc() -> None:
    """The real arena-code acceptance run (files, subprocesses, verification)."""
    out = FIX / "e2e_calc"
    shutil.rmtree(out, ignore_errors=True)
    ws = out / "ws"
    ws.mkdir(parents=True)
    from arena.code.orchestrate import organise
    pid = "fixture-calc"
    res = organise(str(ws), "Build a small, tested Python CLI calculator "
                    "(src/calc.py, src/calc_cli.py, tests/test_calc.py) and document it.",
                   project_id_hint=pid, ticks=90, clock_mode="virtual", verbose=False)
    proj = out / "project"
    shutil.copytree(res.project.root, proj)
    with (out / "result.json").open("w") as f:
        json.dump(res.to_dict(), f, indent=1, sort_keys=True, default=str)
    with (out / "verification.json").open("w") as f:
        json.dump(res.verification, f, indent=1, sort_keys=True, default=str)


def main() -> int:
    t0 = time.time()
    FIX.mkdir(parents=True, exist_ok=True)
    scenario_saas()
    print("saas done", flush=True)
    scenario_ml()
    print("ml done", flush=True)
    scenario_midrun()
    print("midrun done", flush=True)
    scenario_crash_resume()
    print("crash_resume done", flush=True)
    scenario_cli_plan()
    print("cli_plan done", flush=True)
    scenario_e2e_calc()
    print("e2e_calc done", flush=True)
    print(f"all fixtures captured in {time.time() - t0:.1f}s -> {FIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
