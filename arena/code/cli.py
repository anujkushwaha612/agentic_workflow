"""`arena-code` — the CLI. Prompt in, organisation out, evidence on disk.

    arena-code "Build a task tracker ..."      one-shot
    arena-code                                 interactive prompt
    echo "<prompt>" | arena-code -             stdin (what another agent should use)
    arena-code doctor [--live]
    arena-code status | verify | inspect | recover | config

Exit codes are part of the interface, because a caller (the Arena chat agent, CI, a shell script) must
be able to tell "the project is verified" from "the run finished": 0 verified · 10 completed but not
verified · 20 escalations open · 30 tool/environment failure · 64 usage. Never 0 for an unverified
result — that is the entire point of the VERIFY gate.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from . import RUNTIME_VERSION
from .doctor import build_report, main as doctor_main
from .project import Project

EXIT_VERIFIED, EXIT_NOT_VERIFIED, EXIT_ESCALATED, EXIT_ENV, EXIT_USAGE = 0, 10, 20, 30, 64


def _read_prompt(args: Any) -> tuple[str, str]:
    """Returns (prompt, source). Interactive mode only when stdin is a TTY - so a piped prompt is
    never mistaken for a missing one."""
    if getattr(args, "stdin", False) or (args.prompt == "-" and not sys.stdin.isatty()):
        text = sys.stdin.read().strip()
        if not text:
            raise SystemExit("arena-code: nothing on stdin")
        return text, "stdin"
    if args.prompt:
        return args.prompt, "argv"
    if sys.stdin.isatty():
        print("What do you want to build?")
        try:
            text = input("> ").strip()
        except EOFError:
            text = ""
        if not text:
            raise SystemExit("arena-code: no goal given (pass a prompt, pipe one, or use --plan)")
        return text, "interactive"
    raise SystemExit("arena-code: no prompt (give one, or pipe one: `echo ... | arena-code -`)")


def cmd_run(args: Any) -> int:
    from .orchestrate import organise
    prompt, source = _read_prompt(args)
    out = args.out or None
    res = organise(args.workspace, prompt, plan_path=args.plan, ticks=args.ticks,
                   project_id_hint=args.project, resume=bool(args.resume),
                   allow_egress=args.allow_egress, clock_mode=args.clock,
                   provider=args.provider, model=args.model or "", out=out,
                   verbose=not args.quiet)
    if args.events:
        print("\n## event chain")
        for e in res.events:
            print(f"  {e['seq']:>4}  {e['etype']:<22} {e['actor']:<14} {str(e['body'])[:78]}")
    print()
    print(json.dumps({"project": str(res.project.root), "state": res.project.manifest.get("state"),
                      "ok": res.ok, "verification": res.verification,
                      "execution": res.project.manifest.get("execution"),
                      "report": str(out) if out else ""}, indent=2, default=str))
    if res.ok:
        return EXIT_VERIFIED
    if res.project.manifest.get("state") == "awaiting-cortex":
        return EXIT_ESCALATED
    return EXIT_NOT_VERIFIED


def cmd_status(args: Any) -> int:
    rows = Project.discover(args.workspace)
    if not rows:
        print(f"no projects under {Path(args.workspace) / 'projects'}")
        return EXIT_VERIFIED
    print(f"{'project id':<34} {'state':<16} {'src files':>9} {'bytes':>8}  prompt")
    for row in rows:
        pr = Project(root=Path(row["_path"]), manifest=row)
        st = pr.status_row()
        print(f"{st['project_id']:<34} {str(st['state']):<16} {st['source_files']:>9} "
              f"{st['source_bytes']:>8}  {st['prompt'][:44]}")
    return EXIT_VERIFIED


def cmd_verify(args: Any) -> int:
    """Re-verify a project from OUTSIDE the run: fresh process, fresh kernel, same journal."""
    from ..kernel import Kernel
    from ..registry import SpawnBudget
    project = Project.load(args.workspace, args.project)
    if not project.journal_path.exists():
        print(f"no journal at {project.journal_path}")
        return EXIT_ENV
    k = Kernel.from_journal(project.journal_path, root=project.arena_dir, quiet=True,
                            budget=SpawnBudget())
    rows, ok = [], True
    for aid, rec in k.registry.agents.items():
        rows.append({"agent": aid, "role": rec.role, "state": rec.state,
                     "cognition": (rec.cognition or {}).get("source", "")})
    for tid, t in k.graph.tasks.items():
        files = {p: (project.source_dir / p).is_file() for p in t.produces}
        ok = ok and all(files.values()) and not t.is_open
        rows.append({"task": tid, "status": t.status, "verified": bool(t.verified), "files": files})
    # ...but a replayed `verified: true` is only evidence that the *previous* process verified. A
    # check that must be honest about a possibly-lost workspace has to run the commands again, here,
    # from this process, and report what they do. The plan is data on the manifest, so this stays
    # outside any agent's policy logic.
    commands: list[dict[str, Any]] = []
    if not getattr(args, "no_exec", False):
        # `Project.update(plan=<data>)` puts the plan itself in the manifest, which is what makes
        # this possible: the commands come from recorded data, not from an import of the coder's
        # policy module, so verifying a project never requires trusting the thing under test.
        plan = project.manifest.get("plan") or {}
        if not plan.get("agents"):
            print("arena-code verify: the manifest records no plan data, so there are no commands "
                  "to re-run here; refusing to call this verified", file=sys.stderr)
            ok = False
            plan = {}
        for a in (plan.get("agents") or []):
            for argv in ((a["task"].get("verify") or []) if a.get("task") else []):
                r = subprocess.run([sys.executable if c == "python3" else c for c in argv],
                                   cwd=str(project.source_dir), capture_output=True, text=True,
                                   timeout=300)
                commands.append({"argv": list(argv), "exit": r.returncode, "ok": r.returncode == 0,
                                 "stderr_tail": (r.stderr or "")[-200:]})
                ok = ok and r.returncode == 0
        if plan and not commands:
            ok = False            # a plan whose tasks declare no verify commands verifies nothing
    chain_ok = k.journal.verify_chain()[0]
    # An empty verification is not a passed one. `all([])` is True, so a replay of a journal with no
    # agents and no tasks used to report `ok: true` - the exact "exit 0 for an unverified result"
    # failure this CLI exists to prevent, reached by having nothing to check rather than by checking
    # and failing.
    n_tasks = sum(1 for r in rows if "task" in r)
    n_agents = sum(1 for r in rows if "agent" in r)
    # A replayed `verified: true` proves the *running* process checked; a fresh `verify` has to have
    # checked too, and an empty verification is never a passed one.
    verdict = bool(ok and chain_ok and n_tasks)
    verdict = bool(verdict and all(c["ok"] for c in commands) and (commands or n_tasks == 0))
    print(json.dumps({"project_id": project.manifest.get("project_id"),
                      "replayed_from": str(project.journal_path), "chain_ok": chain_ok,
                      "agents": n_agents, "tasks": n_tasks, "rows": rows,
                      "commands_re_run_here": commands, "ok": verdict,
                      "chain": len(k.journal.events())}, indent=2, default=str))
    return EXIT_VERIFIED if verdict else EXIT_NOT_VERIFIED


def cmd_inspect(args: Any) -> int:
    from ..journal import Journal
    project = Project.load(args.workspace, args.project)
    j = Journal(path=project.journal_path)
    try:
        rows = list(j.iterate())
        if args.only:
            want = set(args.only.split(","))
            rows = [r for r in rows if r["etype"] in want]
        for r in rows[-args.tail:]:
            p = r.get("payload") or {}
            print(f"{r['seq']:>4}  {r['etype']:<22} {str(r.get('actor', '')):<14} "
                  f"{str(p.get('body', ''))[:88]}")
        if args.tools:
            for r in rows:
                if r["etype"] in ("TOOL_CALL", "TOOL_RESULT", "TOOL_REFUSED"):
                    # NOTE(migration audit): the original nested this dict-comprehension inside an
                    # f-string across lines, which is a SyntaxError on Python <= 3.11 (PEP 701 is
                    # 3.12+). Hoisted to a local so the file parses on the documented runtime; the
                    # printed bytes are identical.
                    blob = json.dumps(
                        {k: v for k, v in (r["payload"] or {}).items()
                         if k in ('tool', 'ok', 'exit_code', 'args_preview', 'code', 'truncated',
                                  'changed')}, default=str)
                    print(f"{r['seq']:>4}  {r['etype']:<12} {blob[:220]}")
    finally:
        j.close()
    return EXIT_VERIFIED


def cmd_recover(args: Any) -> int:
    """What a recycled sandbox leaves behind, stated exactly instead of papered over."""
    project = Project.load(args.workspace, args.project)
    facts = {"journal": project.journal_path.exists(),
             "source_dir": project.source_dir.is_dir(),
             "source_files": sum(1 for _ in project.source_dir.rglob("*")) if
             project.source_dir.is_dir() else 0,
             "git_repo": (project.source_dir / ".git").exists(),
             "state": project.manifest.get("state")}
    verdict = ("healthy" if all(facts[k] for k in ("journal", "source_dir", "git_repo"))
               else "workspace-gone: the org replays, the work does not — do NOT re-run agents over "
                    "a tree that no longer exists")
    print(json.dumps({"project_id": project.manifest.get("project_id"), "facts": facts,
                      "verdict": verdict}, indent=2))
    return EXIT_VERIFIED if verdict == "healthy" else EXIT_ENV


def cmd_config(args: Any) -> int:
    from .doctor import config_path, resolve_config
    if args.action == "show":
        conf = resolve_config()
        conf["api_key"] = "***" if conf.get("api_key") else ""
        print(json.dumps({"config_file": str(config_path()), "effective": conf}, indent=2))
        return EXIT_VERIFIED
    if args.action == "set":
        path = config_path()
        assert path is not None, "no writable config path (HOME unset)"
        data = {}
        if path.is_file():
            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError:
                data = {}
        data[args.key] = args.value
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2) + "\n")
        os.chmod(path, 0o600)
        print(f"wrote {args.key} to {path}")
        return EXIT_VERIFIED
    if args.action == "set-key":
        path = config_path()
        assert path is not None, "no writable config path (HOME unset)"
        var = args.from_env
        value = os.environ.get(var, "")
        if not value:
            print(f"{var} is not set in this environment; refusing to store an empty secret")
            return EXIT_ENV
        data = {}
        if path.is_file():
            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError:
                data = {}
        data["api_key"] = value
        data.setdefault("provider", "openai-compat" if "OPENAI" in var else "anthropic")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2) + "\n")
        os.chmod(path, 0o600)
        print(f"stored a {len(value)}-character secret in {path} (0600). The value is not echoed.")
        return EXIT_VERIFIED
    return EXIT_USAGE


def cmd_plan(args: Any) -> int:
    from .orchestrate import load_plan
    plan = load_plan(args.plan)
    print(json.dumps({"goal": plan.get("goal"),
                      "agents": [{"id": a["agent_id"], "role": a["role"],
                                  "task": a["task"]["task_id"], "writes": a.get("writes"),
                                  "owed": list(a.get("seed", {})),
                                  "verify": a["task"].get("verify")} for a in plan["agents"]],
                      "note": "this is the plan as DATA; the engine never reads the prompt text to "
                              "choose it"}, indent=2))
    return EXIT_VERIFIED


def build_parser() -> Any:
    import argparse
    ap = argparse.ArgumentParser(prog="arena-code",
                                 description="Start the Arena organisation on a real project.")
    # the runtime version *and* the interpreter it is running on, in one line: the acceptance
    # contract is that `--version` tells you which build you are talking about, and half the bugs
    # in this project have been Python-version-specific (pytest 9's output shape, for one)
    ap.add_argument("--version", action="version",
                    version=f"arena-code {RUNTIME_VERSION} (python {sys.version.split()[0]})")
    sub = ap.add_subparsers(dest="cmd")

    def common(p: Any, *, project: bool = False) -> None:
        p.add_argument("--workspace", default=os.environ.get("ARENA_CODE_WORKSPACE", "."),
                       help="where projects/ lives (default: cwd)")
        if project:
            p.add_argument("--project", required=True, help="project id under projects/")

    run = sub.add_parser("run", help="create a project and start the org")
    common(run)
    run.add_argument("prompt", nargs="?", default="")
    run.add_argument("--stdin", action="store_true")
    run.add_argument("--plan", default="", help="plan JSON; defaults to the packaged acceptance plan")
    run.add_argument("--project", default="", help="project id to create/resume")
    run.add_argument("--resume", action="store_true")
    run.add_argument("--ticks", type=int, default=90)
    run.add_argument("--out", default="", help="write the acceptance report here")
    run.add_argument("--provider", default="policy", help="policy | openai-compat | anthropic | arena-cortex")
    run.add_argument("--model", default="")
    run.add_argument("--allow-egress", action="store_true")
    run.add_argument("--clock", default="wall", choices=["wall", "virtual"],
                   help="wall = real seconds, so a timeout means what it says; virtual = deterministic, for tests")
    run.add_argument("--events", action="store_true", help="print the journalled event chain")
    run.add_argument("--quiet", action="store_true")
    run.set_defaults(fn=cmd_run)

    st = sub.add_parser("status", help="list projects")
    common(st)
    st.set_defaults(fn=cmd_status)

    vf = sub.add_parser("verify", help="re-verify a project from a fresh process")
    common(vf, project=True)
    vf.add_argument("--no-exec", action="store_true",
                    help="only replay the journal; do not re-run the plan's verify commands")
    vf.set_defaults(fn=cmd_verify)

    ins = sub.add_parser("inspect", help="read the journal")
    common(ins, project=True)
    ins.add_argument("--tail", type=int, default=40)
    ins.add_argument("--only", default="", help="comma separated event types")
    ins.add_argument("--tools", action="store_true", help="show tool call/result rows in full")
    ins.set_defaults(fn=cmd_inspect)

    rec = sub.add_parser("recover", help="check whether a project's workspace still exists")
    common(rec, project=True)
    rec.set_defaults(fn=cmd_recover)

    pl = sub.add_parser("plan", help="print the plan a run would use")
    pl.add_argument("--plan", default="")
    pl.set_defaults(fn=cmd_plan)

    cfg = sub.add_parser("config", help="show/set configuration")
    cfg.add_argument("action", choices=["show", "set", "set-key"])
    cfg.add_argument("key", nargs="?", default="")
    cfg.add_argument("value", nargs="?", default="")
    cfg.add_argument("--from-env", dest="from_env", default="ARENA_CODE_API_KEY")
    cfg.set_defaults(fn=cmd_config)

    doc = sub.add_parser("doctor", help="environment capability report")
    common(doc)
    doc.add_argument("--json", action="store_true")
    doc.add_argument("--live", action="store_true")
    doc.add_argument("--project", default="")
    def _doctor(a: Any) -> int:
        argv = ["--workspace", a.workspace]
        if a.json:
            argv.append("--json")
        if a.live:
            argv.append("--live")
        if a.project:
            argv += ["--project", a.project]
        return doctor_main(argv)

    doc.set_defaults(fn=_doctor)
    return ap


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    known = {"run", "status", "verify", "inspect", "recover", "plan", "config", "doctor", "-h",
             "--help", "--version"}
    # `arena-code "prompt"` and `arena-code -` are the headline forms, so bare argv is `run`
    if not argv or (argv[0] not in known and argv[0] not in ("-",) and not argv[0].startswith("-")):
        argv = ["run", *argv]
    elif argv and argv[0] == "-":
        argv = ["run", "--stdin", "-"]
    ap = build_parser()
    args = ap.parse_args(argv)
    if not getattr(args, "fn", None):
        ap.print_help()
        return EXIT_USAGE
    try:
        return int(args.fn(args))
    except FileExistsError as e:
        print(f"arena-code: {e}", file=sys.stderr)
        return EXIT_USAGE
    except FileNotFoundError as e:
        print(f"arena-code: {e}", file=sys.stderr)
        return EXIT_ENV
    except SystemExit as e:
        # a usage problem is 64, a refusal is not a crash. `raise SystemExit("...")` from inside a
        # command would otherwise reach the interpreter and exit 1, which is not in the documented
        # exit-code set at all - so a caller scripting on the codes would mis-read it as "verified
        # with a warning" or "any other failure".
        if e.code in (0, None):
            return EXIT_USAGE
        if isinstance(e.code, int):
            return e.code if e.code in (EXIT_VERIFIED, EXIT_NOT_VERIFIED, EXIT_ESCALATED, EXIT_ENV,
                                        EXIT_USAGE) else EXIT_USAGE
        print(f"arena-code: {e.code}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    raise SystemExit(main())
