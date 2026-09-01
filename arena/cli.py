"""`arena` CLI - inspect and drive a kernel from the shell, across turns.

The kernel is event-sourced, so these subcommands compose: `submit` in one process, `run` in the
next, `status`/`why`/`trace` read-only at any time. That is what makes "the system persists between
Arena turns" true rather than aspirational.

  python3 -m arena.cli plan "<task text>"
  python3 -m arena.cli submit "<task text>" [--budget N]
  python3 -m arena.cli run [--ticks 60] [--resident 120]
  python3 -m arena.cli status | board | why <agent> | trace <correlation_id>
  python3 -m arena.cli pause <agent> | resume <agent> | terminate <agent> [reason]
  python3 -m arena.cli verify            # journal chain + replay parity
  python3 -m arena.cli chaos-report      # the adversarial suite, writes var/chaos_report.md
  python3 -m arena.cli chaos-run <id>
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .chaos.chaos_kernel import BY_ID, render_report, run_scenarios
from .graph import CycleError
from .kernel import Kernel
from .parent import RuleBasedPlanner
from .registry import SpawnBudget

#: default state dir, resolved from the project root so `python3 -m arena.cli` works from /home/user
#: the repo's `var/` is gitignored scratch; anchoring defaults there (rather than the CWD) is what
#: stops a CLI call from creating a stray `var/` inside the source tree where it would be mistaken
#: for deliverable code
VAR = Path(__file__).resolve().parents[1] / "var"
DEFAULT_ROOT = str(VAR / "run")


def _anchored(path: str | Path | None, *, root: str | Path, name: str) -> Path:
    """A relative `--out` belongs to the run's root, not to whatever directory the shell happened
    to be in. Defaulting to CWD is how `var/` reappeared in the middle of a clean tree."""
    q = Path(path) if path else Path(str(root)) / name
    return q if q.is_absolute() else Path(str(root)) / q.name


def _kernel(args) -> Kernel:
    root = Path(args.root)
    if not root.is_absolute():
        root = Path.cwd() / root
    root.mkdir(parents=True, exist_ok=True)
    jp = root / "journal.db"
    budget = SpawnBudget(max_active_agents=args.budget, max_concurrent_workers=args.workers,
                         idle_ttl=args.idle_ttl)
    if jp.exists():
        return Kernel.from_journal(jp, root=str(root), budget=budget,
                                   clock_mode="wall" if args.wall else "virtual")
    return Kernel(journal_path=jp, root=str(root), budget=budget,
                   clock_mode="wall" if args.wall else "virtual")


def cmd_plan(args) -> int:
    tasks, rationale = RuleBasedPlanner().plan(args.text)
    from .graph import DependencyGraph
    g = DependencyGraph()
    for t in tasks:
        g.add(t)
    try:
        order = g.order()
        cyc = None
    except CycleError as e:
        order, cyc = None, list(map(str, e.cycle))
    print(json.dumps({"roles_needed": sorted({t.role for t in tasks}),
                      "tasks": [t.snapshot() for t in tasks],
                      "generations": order, "cycle": cyc,
                      "why": rationale["matched"], "edges": rationale["artifact_edges"]},
                     indent=2, default=str))
    return 0


def cmd_submit(args) -> int:
    k = _kernel(args)
    try:
        out = k.submit(args.text)
    except CycleError as e:
        print(f"PLAN REJECTED (unschedulable): {e}", file=sys.stderr)
        return 2
    print(f"planned {out['tasks']} task(s); {len(out['agents'])} agent(s); "
          f"generations={out.get('parallelism')}\n{k.parent.render()}")
    return 0


def cmd_run(args) -> int:
    k = _kernel(args)
    if not k.task_text and not k.graph.tasks:
        print("nothing submitted yet (empty journal); run `submit` first", file=sys.stderr)
        return 1
    if args.resident:
        k.run_resident(args.resident, ticks_per_loop=args.ticks)
    else:
        k.run(ticks=args.ticks)
    print(k.report())
    return 0 if k.summary()["done"] else 1


def cmd_status(args) -> int:
    k = _kernel(args)
    print(json.dumps(k.status(), indent=2, default=str))
    return 0


def cmd_agents(args) -> int:
    """The spawn monitor (spec §5): who is here, where they came from, and what it cost."""
    k = _kernel(args)
    reg = k.registry
    rows = []
    for a in sorted(reg.agents.values(), key=lambda x: (x.epoch, x.agent_id)):
        t = k.graph.tasks.get(a.task_id or "")
        rows.append({
            "agent": a.agent_id, "role": a.role, "state": a.state, "task": a.task_id,
            "epoch": a.epoch, "spawned_by": a.spawned_by, "queued": len(a.task_queue),
            "waits": len(a.pending_waits),
            "blocked_on": k.graph.blocked_by(a.task_id) if a.task_id else [],
            "spawn_reason": (a.spawn_reason or "")[:48]})
    if args.json:
        print(json.dumps({"roster": rows, "monitoring": k.monitoring()}, indent=2, default=str))
        return 0
    print(f"{'AGENT':<22}{'ROLE':<20}{'STATE':<26}{'TASK':<26}{'DEP':<4}{'SPAWNED BY':<16}NOTE")
    for r in rows:
        print(f"{r['agent']:<22}{r['role']:<20}{r['state']:<26}{str(r['task']):<26}"
              f"{r['epoch']:<4}{r['spawned_by']:<16}{r['spawn_reason']}")
    m = k.monitoring()
    print(f"\nactive {m['active']}/{m['agent_budget']}  (idle {m['idle']}, working {m['working']}, "
          f"waiting {m['waiting']}, blocked {m['blocked']}, escalated {m['escalated']}, "
          f"completed {m['completed']})")
    print(f"spawn requests {m['requests_received']}: {m['requests_approved']} approved, "
          f"{m['requests_rejected']} rejected, {m['requests_deduplicated']} duplicate, "
          f"{m['requests_escalated']} escalated  |  reused existing agent {m['reuses']}x")
    print(f"generation epoch max {m['generation_epoch_max']} "
          f"(depth cap {k.registry.budget.max_spawn_epoch}), depth hist {m['spawn_depth_hist']}, "
          f"remaining capacity {m['remaining_capacity']}, "
          f"worker slots {m['worker_slots_in_use']}/{m['workers_max']}")
    return 0


def cmd_spawns(args) -> int:
    """Every spawn request this run saw, newest first, with the rule that decided it."""
    k = _kernel(args)
    entries = k.parent.ledger.newest_first()
    if args.json:
        print(json.dumps([e.to_dict() for e in entries], indent=2, default=str))
        return 0
    if not entries:
        print("no spawn requests recorded")
        return 0
    print(f"{'RID':<10}{'STATE':<20}{'RULE':<30}{'AGENT':<22}{'TASK':<28}REQUESTER")
    for e in entries:
        print(f"{e.rid:<10}{str(e.state):<20}{e.rule:<30}{str(e.spawned_agent_id):<22}"
              f"{str(e.task_id):<28}{e.request.requester_agent_id}")
        if args.verbose:
            print(f"          asked: {e.request.requested_role} "
                  f"{list(e.request.required_skills)} est={e.request.estimated_work}")
            print(f"          why  : {e.request.reason[:110]}")
            for h in e.history:
                print(f"          ·    {h[:110]}")
    return 0


def cmd_request(args) -> int:
    """Inject a spawn request into a running org (or into the journal, for the next `run`)."""
    k = _kernel(args)
    req = {"from": args.agent, "requested_role": args.role, "reason": args.reason,
           "required_skills": list(args.skill or []),
           "required_inputs": list(args.needs or []),
           "expected_outputs": list(args.produces or []),
           "estimated_work": args.work, "capability_class": args.capability_class,
           "requires_judgment": args.judge, "correlation_id": args.correlation_id or ""}
    if args.task:
        req["parent_task_id"] = args.task
    k.inject_spawn_request(req)
    if not args.no_run:
        if args.resident:
            k.run_resident(args.resident, ticks_per_loop=args.ticks)
        else:
            k.run(ticks=args.ticks)
    print(json.dumps({"injected": req, "decisions": k.parent.decisions[-1:],
                      "ledger": [e.to_dict() for e in k.parent.ledger.newest_first()[:1]],
                      "monitoring": k.monitoring()}, indent=2, default=str))
    return 0


def cmd_watch(args) -> int:
    """Poll *the journal*, not the agents: a monitor that asks agents 'are you done?' is exactly
    the polling this design forbids. Prints one line per sample."""
    import time as _t
    root = Path(args.root)
    jp = root / "journal.db"
    last_events = -1
    for _ in range(args.count):
        k = _kernel(args)
        m = k.metrics()
        n = k.journal.count()
        new = n - last_events if last_events >= 0 else 0
        last_events = n
        print(f"{_t.strftime('%H:%M:%S')} tick={m['tick']:<5} agents={m['active']} "
              f"(work {m['working']}, idle {m['idle']}, wait {m['waiting']}, block {m['blocked']}) "
              f"req={m['requests_received']} ok={m['requests_approved']} "
              f"no={m['requests_rejected']} reuse={m['reuses']} "
              f"cap={m['remaining_capacity']} events=+{new} "
              f"{'IDLE' if k.idle_predicate() else 'BUSY'}")
        if args.once:
            break
        _t.sleep(args.every)
    return 0


def cmd_accept(args) -> int:
    """Phase 2 acceptance demonstration: a live run in which a new agent is born mid-flight."""
    from .chaos.phase2_demo import run_demo
    out = run_demo(Path(args.root), out=_anchored(args.out, root=args.root,
                                                  name="phase2_acceptance.md"), ticks=args.ticks)
    print(out["markdown"])
    if not out["ok"]:
        print("\nACCEPTANCE: FAILED", file=sys.stderr)
        return 1
    print("\nACCEPTANCE: PASSED (every step observed inside one continuous run)")
    return 0


def cmd_board(args) -> int:
    print(_kernel(args).report())
    return 0


def cmd_why(args) -> int:
    k = _kernel(args)
    print(json.dumps(k.why(args.agent), indent=2, default=str))
    return 0 if "error" not in k.why(args.agent) else 1


def cmd_trace(args) -> int:
    k = _kernel(args)
    rows = k.trace(args.correlation_id)
    if not rows:
        print(f"no events for {args.correlation_id}", file=sys.stderr)
        return 1
    for r in rows:
        print(f"{r['seq']:>5} {r['ts']:>8.3f} {r['etype']:<22} {r['actor']:>14} -> "
              f"{str(r['target']):<14} d{r['depth']} {r['body'][:70]}")
    return 0


def cmd_control(args) -> int:
    k = _kernel(args)
    fn = {"pause": k.parent.pause, "resume": k.parent.resume}.get(args.op)
    ok = fn(args.agent) if fn else k.parent.terminate_agent(args.agent, args.reason or "cli")
    print(f"{args.op} {args.agent}: {'ok' if ok else 'refused'}")
    if not ok:
        return 1
    if args.op == "run":
        k.run(ticks=args.ticks)
    return 0


def cmd_verify(args) -> int:
    """Two independent checks, reported separately:
       1. hash chain   - is the journal intact?
       2. replay parity - does a kernel rebuilt from the journal reproduce the one that wrote it?
    Parity compares kernel.snapshot() to kernel.from_journal().snapshot(); comparing the raw
    fold() instead was wrong because fold() projects fewer fields than a snapshot carries.
    """
    k = _kernel(args)
    ok, why, where = k.journal.verify_chain()
    print(f"journal rows: {k.journal.count()}  chain: {'VALID' if ok else f'BROKEN ({why})'}")
    if not ok:
        return 1
    live = k.snapshot()
    root = Path(k.root)
    r = Kernel.from_journal(k.journal.path, root=str(root), quiet=True,
                            budget=SpawnBudget(max_active_agents=args.budget,
                                               max_concurrent_workers=args.workers,
                                               idle_ttl=1e9))
    rp = r.snapshot()
    diffs = sorted(key for key in ("agents", "tasks", "claims", "artifacts")
                   if live.get(key) != rp.get(key))
    fold = k.journal.fold()
    print(f"replay parity : {'OK' if not diffs else f'DIVERGED {diffs}'}")
    print(f"event fold    : {len(fold['agents'])} agents / {len(fold['tasks'])} tasks / "
          f"{len(fold['claims'])} claims / {len(fold['waits'])} live waits")
    return 0 if not diffs else 1


def cmd_inbox(args) -> int:
    k = _kernel(args)
    p = Path(k.root) / k.parent.inbox_path
    if not p.exists():
        print("(empty - no escalations)")
        return 0
    for line in p.read_text().splitlines():
        if line.strip():
            print(line)
    return 0


def cmd_chaos_report(args) -> int:
    results = run_scenarios()
    text = render_report(results)
    out = _anchored(args.out, root=args.root, name="chaos_report.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    out.with_suffix(".json").write_text(json.dumps([r.to_dict() for r in results], indent=2,
                                                   default=str))
    ok = sum(1 for r in results if r.ok)
    print(f"{ok}/{len(results)} scenarios passed -> {out}")
    for r in results:
        if not r.ok:
            print(f"  FAIL {r.scenario.id}: {r.failure}")
    return 0 if ok == len(results) else 1


def cmd_chaos_run(args) -> int:
    sc = BY_ID.get(args.id)
    if sc is None:
        print(f"unknown scenario {args.id}; available: {', '.join(sorted(BY_ID))}",
              file=sys.stderr)
        return 1
    try:
        print(json.dumps(sc.fn(), indent=2, default=str))
    except Exception as e:  # noqa: BLE001
        print(f"SCENARIO FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="arena", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--budget", type=int, default=6, help="MAX_ACTIVE_AGENTS")
    ap.add_argument("--workers", type=int, default=2, help="MAX_CONCURRENT_WORKERS")
    ap.add_argument("--idle-ttl", type=float, default=1e9, help="reap agents idle this long")
    ap.add_argument("--wall", action="store_true", help="use the real clock instead of a virtual one")
    sub = ap.add_subparsers(dest="op", required=True)

    p = sub.add_parser("plan", help="dry-run: what would the Parent build?")
    p.add_argument("text")
    p.set_defaults(fn=cmd_plan)

    p = sub.add_parser("submit", help="plan, spawn, assign")
    p.add_argument("text")
    p.set_defaults(fn=cmd_submit)

    p = sub.add_parser("run", help="advance the scheduler")
    p.add_argument("--ticks", type=int, default=60)
    p.add_argument("--resident", type=float, default=0.0, help="seconds to keep serving")
    p.set_defaults(fn=cmd_run)

    for name, fn in (("status", cmd_status), ("board", cmd_board), ("inbox", cmd_inbox),
                     ("verify", cmd_verify)):
        p = sub.add_parser(name)
        p.set_defaults(fn=fn)

    p = sub.add_parser("agents", help="roster + spawn monitoring (states, requests, capacity)")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_agents)

    p = sub.add_parser("spawns", help="every SPAWN_AGENT_REQUEST and how it was decided")
    p.add_argument("--json", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(fn=cmd_spawns)

    p = sub.add_parser("request", help="inject a spawn request into the running org")
    p.add_argument("--agent", required=True, help="requesting agent id")
    p.add_argument("--role", required=True)
    p.add_argument("--reason", default="operator-requested capability")
    p.add_argument("--skill", action="append")
    p.add_argument("--needs", action="append", help="artifact this work consumes")
    p.add_argument("--produces", action="append", help="artifact this work must publish")
    p.add_argument("--task", default="", help="parent task id this request belongs to")
    p.add_argument("--work", type=float, default=3.0)
    p.add_argument("--capability-class", dest="capability_class", default="")
    p.add_argument("--judge", action="store_true", help="flag it as needing human judgement")
    p.add_argument("--correlation-id", dest="correlation_id", default="")
    p.add_argument("--ticks", type=int, default=30)
    p.add_argument("--resident", type=float, default=0.0)
    p.add_argument("--no-run", action="store_true", help="only queue it; answer on the next run")
    p.set_defaults(fn=cmd_request)

    p = sub.add_parser("watch", help="sample the journal (never the agents) on an interval")
    p.add_argument("--every", type=float, default=1.0)
    p.add_argument("--count", type=int, default=30)
    p.add_argument("--once", action="store_true")
    p.set_defaults(fn=cmd_watch)

    p = sub.add_parser("accept", help="Phase 2 acceptance demo (mid-run spawn, one continuous run)")
    p.add_argument("--out", default=str(VAR / "phase2_acceptance.md"))
    p.add_argument("--ticks", type=int, default=40)
    p.set_defaults(fn=cmd_accept)

    p = sub.add_parser("why", help="what is this agent blocked on, and since when")
    p.add_argument("agent")
    p.set_defaults(fn=cmd_why)

    p = sub.add_parser("trace", help="full causal chain for a correlation id")
    p.add_argument("correlation_id")
    p.set_defaults(fn=cmd_trace)

    for name in ("pause", "resume", "terminate"):
        p = sub.add_parser(name)
        p.add_argument("agent")
        p.add_argument("reason", nargs="?", default="")
        p.add_argument("--ticks", type=int, default=20)
        p.set_defaults(fn=cmd_control)

    p = sub.add_parser("chaos-report")
    p.add_argument("--out", default=str(VAR / "chaos_report.md"))
    p.set_defaults(fn=cmd_chaos_report)

    p = sub.add_parser("chaos-run")
    p.add_argument("id")
    p.set_defaults(fn=cmd_chaos_run)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
