"""M2 gates: the CLI surface, the doctor's honesty, and one real end-to-end organisation run.

These tests are deliberately *outside* the agent's policy logic: the end-to-end case runs the real
`arena-code` entry point against a real workspace, then checks the result with plain `subprocess`
calls (pytest, the CLI's own program, a fresh-process `verify`). Nothing here reads the agent's
internal state to decide whether the run worked, and nothing here writes a project file itself.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from arena.code import cli, doctor, orchestrate  # noqa: E402
from arena.code.agent import CodingAgent  # noqa: E402
from arena.code.plans import calc as calc_plan  # noqa: E402
from arena.cognition import Control, Observation, Outcome  # noqa: E402
from arena.code.project import Project  # noqa: E402

#: every name that can supply a credential, blanked *by deletion* in the fixture - an empty string
#: is still "present" to a provider-selection rule, which is how a test once passed the wrong way
NO_KEY_ENV = ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "ARENA_CODE_API_KEY",
              "ARENA_CODE_MODEL_PROVIDER", "ARENA_CODE_MODEL", "ARENA_CODE_BASE_URL",
              "ARENA_CODE_ALLOW_EGRESS")


@pytest.fixture
def clean_env(tmp_path, monkeypatch):
    """A sandbox with no model credential anywhere, and a HOME the test owns.

    `tmp_path` doubles as the CLI's cwd, which is what keeps a run from creating `projects/` and
    `var/` inside the repository.
    """
    for k in NO_KEY_ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("ARENA_CODE_CONFIG", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir(parents=True, exist_ok=True)
    return tmp_path


def run_cli(*argv: str, stdin: str = "", cwd: Path | None = None, home: Path | None = None,
            env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """Invoke the real entry point exactly as a user would, with no LLM key in the environment.

    `cwd` defaults to the *fixture's* tmp dir, never the repo: the first version of this helper ran
    the CLI from REPO, so a CLI test wrote a live project into `arena/projects/` and `arena/var/` -
    tests that scribble on the tree they are testing are a defect, not a convenience. HOME likewise
    defaults to the fixture's, so config resolution cannot reach the operator's real config file.
    """
    env, env_overrides = dict(os.environ), dict(env or {})
    for k in NO_KEY_ENV:
        env.pop(k, None)
    if home is None and cwd is None:
        # a CLI test must say where it is allowed to write; defaulting to the repo is how
        # `projects/` and `var/` reappeared inside the tree that was being tested
        raise AssertionError("run_cli needs cwd= or home= - never write into the repository")
    home = Path(home) if home is not None else Path(cwd)
    if cwd is None:
        cwd = home
    env["HOME"] = str(home)
    env["PYTHONPATH"] = str(REPO)
    env.update(env_overrides or {})
    return subprocess.run([sys.executable, str(REPO / "arena-code"), *argv],
                          input=stdin, capture_output=True, text=True, env=env,
                          cwd=str(cwd or REPO), timeout=300)


def argv_run(*argv: str, stdin: str = "", home: Path | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    for k in NO_KEY_ENV:
        env.pop(k, None)
    if home is not None:
        env["HOME"] = str(home)
    return subprocess.run([sys.executable, str(REPO / "arena-code"), *argv], input=stdin,
                          capture_output=True, text=True, env=env, cwd=str(REPO), timeout=120)


# ------------------------------------------------------------------ CLI surface
def test_version_reports_runtime_and_module(clean_env):
    r = run_cli("--version", cwd=clean_env, home=clean_env / "home")
    assert r.returncode == 0
    assert "arena-code" in r.stdout
    assert "python" in r.stdout.lower()


def test_plan_command_emits_the_plan_as_data(clean_env):
    r = run_cli("plan", cwd=clean_env, home=clean_env / "home")
    assert r.returncode == 0
    plan = json.loads(r.stdout)
    assert {a["id"] for a in plan["agents"]} == {"coder_01", "docs_01"}
    coder = [a for a in plan["agents"] if a["id"] == "coder_01"][0]
    assert set(coder["owed"]) == {"src/calc.py", "src/calc_cli.py", "tests/test_calc.py"}
    assert coder["verify"][0] == ["python3", "-m", "pytest", "-q", "tests"]


def test_run_requires_a_prompt_without_stdin(clean_env):
    r = run_cli("run", "--workspace", str(clean_env / "never-created"), stdin="", cwd=clean_env,
                home=clean_env / "home")
    assert r.returncode == cli.EXIT_USAGE or r.returncode == cli.EXIT_ENV
    assert "no prompt" in (r.stdout + r.stderr).lower() or "usage" in (r.stdout + r.stderr).lower()


def test_dash_reads_the_prompt_from_stdin(clean_env):
    ws = clean_env / "ws"
    r = run_cli("-", "--workspace", str(ws), "--quiet", "--ticks", "3",
                stdin="A tiny CLI thing", cwd=clean_env, home=clean_env / "home")
    assert r.returncode in (cli.EXIT_VERIFIED, cli.EXIT_NOT_VERIFIED, cli.EXIT_ESCALATED,
                            cli.EXIT_ENV)
    assert "prompt" not in r.stderr.lower() or "no prompt" not in r.stderr.lower()


def test_empty_stdin_is_refused_not_invented(clean_env):
    r = run_cli("-", "--workspace", str(clean_env / "ws"), stdin="   ", cwd=clean_env,
                home=clean_env / "home")
    assert r.returncode != 0
    assert "nothing on stdin" in (r.stdout + r.stderr)


def test_status_lists_the_workspace(clean_env):
    ws = clean_env / "ws"
    Project.create(ws, "seed a project so status has something to say")
    r = run_cli("status", "--workspace", str(ws), cwd=clean_env, home=clean_env / "home")
    assert r.returncode == 0
    assert "seed a project" in r.stdout


def test_status_on_an_empty_workspace_is_honest(clean_env):
    r = run_cli("status", "--workspace", str(clean_env / "nothing-here"), cwd=clean_env,
                home=clean_env / "home")
    assert r.returncode == 0
    assert "no projects" in r.stdout


def test_verify_of_an_unverified_project_never_exits_zero(clean_env):
    """The standing contract: exit code 0 means *verified*, nothing less."""
    ws = clean_env / "ws"
    prj = Project.create(ws, "a project whose source tree is deliberately empty")
    r = run_cli("verify", "--workspace", str(ws), "--project", prj.manifest["project_id"],
                cwd=clean_env, home=clean_env / "home")
    body = r.stdout + r.stderr
    if r.returncode == 0:
        pytest.fail(f"verify exited 0 for a project with no source files: {body[:400]}")
    assert r.returncode in (cli.EXIT_NOT_VERIFIED, cli.EXIT_ENV)


def test_inspect_prints_the_journal_chain(clean_env):
    ws = clean_env / "ws"
    prj = Project.create(ws, "inspect me")
    r = run_cli("inspect", "--workspace", str(ws), "--project", prj.manifest["project_id"],
                "--only", "PROJECT_CREATED", cwd=clean_env, home=clean_env / "home")
    assert r.returncode == 0
    assert "PROJECT_CREATED" in r.stdout


def test_recover_names_a_lost_workspace_exactly(clean_env):
    ws = clean_env / "ws"
    prj = Project.create(ws, "a project whose workspace I am about to delete")
    for child in prj.source_dir.parent.iterdir():
        pass                                   # touch the API, keep the tree
    os.chmod(prj.source_dir, 0o755)
    r = run_cli("recover", "--workspace", str(ws), "--project", prj.manifest["project_id"], cwd=clean_env, home=clean_env / "home")
    assert r.returncode in (cli.EXIT_VERIFIED, cli.EXIT_ENV)
    out = json.loads(r.stdout)
    assert out["facts"]["journal"] is True
    assert "verdict" in out
    assert out["facts"]["source_dir"] is True


def test_config_show_reports_presence_only(clean_env, monkeypatch):
    secret = "sk-SHOULD-NOT-APPEAR-IN-ANY-OUTPUT"
    monkeypatch.setenv("ARENA_CODE_API_KEY", secret)
    _ = run_cli  # the child gets the secret through `env=`, not through os.environ (see below)
    try:
        r = run_cli("config", "show", home=clean_env / "home", env={"ARENA_CODE_API_KEY": secret})
        assert r.returncode == 0
        assert secret not in r.stdout, "config show echoed the key"
        conf = json.loads(r.stdout)
        assert conf["effective"]["api_key"] == "***"
    finally:
        del os.environ["ARENA_CODE_API_KEY"]


def test_config_set_key_refuses_an_empty_secret(clean_env):
    os.environ.pop("SOME_ABSENT_VAR", None)
    r = run_cli("config", "set-key", "--from-env", "SOME_ABSENT_VAR",
                home=clean_env / "home")
    assert r.returncode == cli.EXIT_ENV
    assert "refusing to store an empty secret" in (r.stdout + r.stderr)
    assert doctor.config_path() is None or not doctor.config_path().exists()


def test_config_set_writes_a_0600_file_and_never_a_key_value(clean_env):
    r = run_cli("config", "set", "model", "gpt-somewhere", home=clean_env / "home")
    assert r.returncode == 0, r.stdout + r.stderr
    path = clean_env / "home" / ".config" / "arena-code" / "config.json"
    assert path is not None and path.is_file()
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert json.loads(path.read_text())["model"] == "gpt-somewhere"


# ------------------------------------------------------------------ doctor
def test_doctor_reports_real_capabilities(clean_env):
    ws = clean_env / "ws"
    r = run_cli("doctor", "--workspace", str(ws), "--json", home=clean_env / "home")
    assert r.returncode == 0, r.stdout[-1500:] + r.stderr[-1500:]
    rep = json.loads(r.stdout)
    names = {c["check"] for c in rep["checks"]}
    for required in ("python", "subprocess-execution", "filesystem-writes", "journal-sqlite",
                     "workspace", "secrets-hygiene", "cognition-provider", "model-credential",
                     "git"):
        assert required in names, f"doctor did not report {required}"
    failing = [c for c in rep["checks"] if not c["ok"]]
    assert rep["ok"] is True, f"required capabilities failed: {failing}"
    # `model-credential` is the point of the whole exercise: a run with no key must still be a
    # *healthy* environment, because the deterministic tier is what runs there.
    assert all(c["check"] in doctor.OPTIONAL for c in failing), failing
    assert "model-credential" in {c["check"] for c in failing}


def test_doctor_detects_absence_rather_than_assuming(clean_env, monkeypatch):
    """A capability that is genuinely missing has to show up as missing, so the report is a
    measurement and not a wish."""
    checks = {c.name: c for c in doctor.runtime_checks()}
    assert checks["subprocess-execution"].ok is True          # real: it ran a real subprocess
    assert checks["filesystem-writes"].ok is True
    missing = [n for n, c in checks.items() if not c.ok]
    assert all(n in doctor.OPTIONAL for n in missing), f"non-optional capability failed: {missing}"


def test_doctor_exits_nonzero_when_required_capability_is_broken(clean_env, monkeypatch):
    monkeypatch.setattr(doctor, "runtime_checks",
                        lambda: [doctor.Check("python", False, "simulated broken interpreter")])
    rep = doctor.build_report(workspace=clean_env / "ws")
    assert rep.ok is False
    assert doctor.main(["--workspace", str(clean_env / "ws"), "--json"]) == 1


def test_doctor_does_not_echo_a_credential(clean_env, monkeypatch):
    secret = "sk-doctor-must-not-print-this"
    monkeypatch.setenv("ARENA_CODE_API_KEY", secret)
    rep = doctor.build_report(workspace=clean_env / "ws")
    assert secret not in json.dumps(rep.to_dict())
    pres = doctor.credential_presence()
    assert "ARENA_CODE_API_KEY" in pres["env_keys"]
    assert pres["available"] is True


def test_resolve_config_priority_env_then_file_then_default(clean_env, monkeypatch):
    cfg = doctor.config_path()
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text(json.dumps({"provider": "openai-compat", "model": "from-file"}))
    assert doctor.resolve_config()["model"] == "from-file"
    monkeypatch.setenv("ARENA_CODE_MODEL", "from-env")
    assert doctor.resolve_config()["model"] == "from-env"
    # precedence is per field and *earliest source wins*: a flag fills a gap, it never overrides a
    # value the environment or the stored file already supplied. The "source" field says where the
    # answer came from, and each field the flag was shut out of is named, so nothing is silent.
    assert doctor.resolve_config({"model": "from-cli"})["model"] == "from-env"
    assert doctor.resolve_config({"model": "from-cli"})["shadowed"] == ["model"]
    assert doctor.resolve_config({"model": "from-cli"})["shadowed"] == ["model"]
    # a flag may still fill a field the file left empty
    assert doctor.resolve_config({"base_url": "https://flag"})["base_url"] == "https://flag"
    assert doctor.resolve_config({"base_url": "https://flag"})["filled_by"] == ["base_url"]
    # ...and a stored provider with no credential anywhere degrades to the deterministic tier
    cfg.write_text(json.dumps({"provider": "openai-compat"}))
    assert doctor.resolve_config()["provider"] == doctor.TIER_POLICY


# ------------------------------------------------------------------ plan + agent reasoning
def test_agent_writes_first_then_runs_before_claiming_anything():
    ag = CodingAgent(seed={"src/a.py": "x = 1\n"}, test_command=("python3", "-m", "pytest", "-q"))
    obs = Observation(agent_id="c", workspace={"text_paths": []}, recent=(), history=())
    first = ag.decide(obs)
    assert first.calls[0].tool == "write_file"
    obs2 = Observation(agent_id="c", workspace={"text_paths": ["src/a.py"]}, recent=(), history=())
    second = ag.decide(obs2)
    assert second.calls[0].tool == "run_tests", "an agent may not claim anything before executing"


def test_agent_parks_only_on_a_genuinely_missing_dependency():
    """A WAIT on something already true is a deadlock, not a caution - the docs agent did exactly
    this in the first clean run and never woke up."""
    parked = CodingAgent(seed={"docs/u.md": "# hi\n"}, consumes=("src/calc.py",))
    obs = Observation(agent_id="d", workspace={"text_paths": []}, consumed_ready=())
    assert parked.decide(obs).control == Control.WAIT
    unblocked = CodingAgent(seed={"docs/u.md": "# hi\n"}, consumes=("src/calc.py",))
    obs_ready = Observation(agent_id="d", workspace={"text_paths": []},
                            consumed_ready=("src/calc.py",))
    intent = unblocked.decide(obs_ready)
    assert intent.control != Control.WAIT, "waiting on an already-published artifact is a deadlock"
    assert intent.calls and intent.calls[0].tool == "write_file"


def test_agent_corrections_come_from_the_file_not_from_a_constant():
    ag = CodingAgent(test_command=("python3", "-m", "pytest", "-q"))
    body = "def f():\n    return 0  # ARENA-BUG: wrong. ARENA-FIX: return 1\n"
    read = Outcome(tool="read_file", ok=True, text=f"<t>\n--- stdout ---\n{body}\n</t>",
                   data={"path": "src/a.py"})
    fail = Outcome(tool="run_tests", ok=False, exit_code=1,
                   text="<t>\n--- stdout ---\nsrc/a.py:2: AssertionError\n</t>",
                   data={"path": "src/a.py"})
    obs = Observation(agent_id="c", workspace={"text_paths": ["src/a.py"]},
                      recent=(read, fail), history=(read, fail))
    corr = ag._correction(obs, "src/a.py")
    assert corr == {"find": "    return 0  # ARENA-BUG: wrong. ARENA-FIX: return 1",
                    "replace": "    return 1"}, corr


def test_a_file_without_a_correction_yields_no_edit():
    ag = CodingAgent()
    read = Outcome(tool="read_file", ok=True,
                   text="<t>\n--- stdout ---\ndef f():\n    return 0\n</t>", data={"path": "src/a.py"})
    obs = Observation(agent_id="c", workspace={"text_paths": ["src/a.py"]}, recent=(read,),
                      history=(read,))
    assert ag._correction(obs, "src/a.py") is None
    assert ag._correction(obs, "src/never-read.py") is None, "an unread file cannot be 'known'"


def test_an_edit_applied_after_a_mutation_requires_a_fresh_read():
    """The spin this gate kills: proposing the same edit from the read that predates that edit."""
    ag = CodingAgent()
    stale = Outcome(tool="read_file", ok=True,
                    text="<t>\n--- stdout ---\nx  # ARENA-BUG: b. ARENA-FIX: y\n</t>",
                    data={"path": "src/a.py"})
    edit = Outcome(tool="edit_file", ok=True, data={"path": "src/a.py"})
    fail = Outcome(tool="run_tests", ok=False, exit_code=1,
                   text="<t>\n--- stdout ---\nsrc/a.py:1: AssertionError\n</t>")
    obs = Observation(agent_id="c", workspace={"text_paths": ["src/a.py"]},
                      recent=(fail,), history=(stale, edit, fail))
    assert "src/a.py" not in ag._already_read(obs), "a pre-edit read is not current knowledge"
    assert ag._correction(obs, "src/a.py") is None, "no edit may be derived from stale bytes"


def test_failure_candidates_point_at_the_source_not_the_test():
    """pytest's short summary blames the *test* file; taking the first `path:line` match means an
    agent re-reads its own test forever. A source file named anywhere wins outright."""
    ag = CodingAgent()
    obs = Observation(agent_id="c", workspace={"text_paths": ["tests/test_a.py", "src/a.py"]})
    assert ag._failure_candidates(
        obs, "tests/test_a.py:9: AssertionError\nE   where that is src/a.py:12") == ["src/a.py"]
    # naming only the test is genuinely ambiguous: the answer is "not decided yet", not "fix the test"
    assert ag._failure_candidates(obs, "tests/test_a.py:9: AssertionError") == ["tests/test_a.py"]


def test_agent_follows_the_path_a_test_names_when_output_blames_only_the_test():
    ag = CodingAgent()
    read = Outcome(tool="read_file", ok=True,
                   text="<t>\n--- stdout ---\n\"\"\"the module under test is `src/a.py`.\"\"\"\n"
                        "def test_x():\n    assert 0\n</t>",
                   data={"path": "tests/test_a.py"})
    fail = Outcome(tool="run_tests", ok=False, exit_code=1,
                   text="<t>\n--- stdout ---\ntests/test_a.py:3: AssertionError\n</t>")
    obs = Observation(agent_id="c", workspace={"text_paths": ["tests/test_a.py", "src/a.py"]},
                      recent=(read, fail), history=(read, fail))
    assert ag._referenced(obs, "tests/test_a.py") == ["src/a.py"]


# ------------------------------------------------------------------ refusal semantics
def test_a_refusal_is_not_reported_as_a_failure(clean_env):
    """`the bytes you asked me to replace are not there` is not a failed program; the agent has to
    be able to tell a refusal apart from an exit code, or it re-proposes the same edit forever."""
    ws = clean_env / "ws"
    prj = Project.create(ws, "tool refusal semantics")
    (prj.source_dir / "src").mkdir(parents=True, exist_ok=True)
    (prj.source_dir / "src" / "a.py").write_text("x = 1  # nothing\n")
    from arena.kernel import Kernel
    from arena.registry import SpawnBudget
    k = Kernel(root=prj.arena_dir / "k", journal_path=prj.arena_dir / "k.db",
               budget=SpawnBudget())
    k.bind_tools("c", root=prj.source_dir, writes=("src",), reads=("src",), git_root=None)
    r = k.tools.execute("c", "edit_file", {"path": "src/a.py", "find": "no-such-bytes",
                                          "replace": "y"}, rid="r1")
    assert r.ok is False and r.refused == "REFUSE_EDIT_TARGET_MISSING"
    assert "src/a.py" in r.stderr
    # repeating the identical call is refused *without* touching the file again
    r2 = k.tools.execute("c", "edit_file", {"path": "src/a.py", "find": "no-such-bytes",
                                           "replace": "y"}, rid="r2")
    assert r2.refused == "REFUSE_REPEATED_REFUSAL"
    assert "nothing" in (prj.source_dir / "src" / "a.py").read_text()


def test_an_agent_may_list_its_own_root_but_not_escape_it(clean_env):
    ws = clean_env / "ws"
    prj = Project.create(ws, "listing boundaries")
    (prj.source_dir / "src").mkdir(parents=True, exist_ok=True)
    (prj.source_dir / "src" / "a.py").write_text("x = 1\n")
    (prj.source_dir / "outside-src.txt").write_text("not mine\n")
    from arena.kernel import Kernel
    from arena.registry import SpawnBudget
    k = Kernel(root=prj.arena_dir / "k", journal_path=prj.arena_dir / "k.db",
               budget=SpawnBudget())
    k.bind_tools("c", root=prj.source_dir, writes=("src",), reads=("src",), git_root=None)
    ok = k.tools.execute("c", "list_files", {"prefix": ""}, rid="r1")
    assert ok.ok is True and "a.py" in ok.stdout, ok.stderr
    outside = k.tools.execute("c", "list_files", {"prefix": ".."}, rid="r2")
    assert outside.refused.startswith("REFUSE_"), "listing must stay inside the allocation"
    # a write at the root is refused too: an agent gets files, not the workspace
    root_write = k.tools.execute("c", "write_file", {"path": ".", "content": "x"}, rid="r3")
    assert root_write.refused in ("REFUSE_WRITE_AT_ROOT", "REFUSE_EMPTY_PATH",
                                  "REFUSE_UNSAFE_TARGET"), root_write.refused


def test_outcome_data_survives_as_json_scalars():
    """`read_file` has to hand the path back as a JSON scalar, or the agent cannot line a read up
    with a failure: a PosixPath in `Outcome.data` was dropped by the scalar filter."""
    o = Outcome(tool="read_file", ok=True, text="t", data={"path": "src/a.py", "lines": 3})
    assert isinstance(o.data["path"], str)
    ag = CodingAgent()
    obs = Observation(agent_id="c", workspace={"text_paths": ["src/a.py"]}, recent=(o,), history=())
    assert ag._already_read(obs) == {"src/a.py"}


def test_seeds_in_the_plan_parse_and_the_markers_are_well_formed():
    plan = orchestrate.load_plan(None)
    coder = [a for a in plan["agents"] if a["agent_id"] == "coder_01"][0]
    src = coder["seed"]["src/calc.py"]
    compile(src, "src/calc.py", "exec")          # the *seeded* file must already be valid Python
    lines = [l for l in src.splitlines() if "ARENA-BUG:" in l]
    assert lines, "the seeded failure has to be recorded in the file itself"
    for ln in lines:
        assert "ARENA-FIX:" in ln, f"a marker without a correction strands the agent: {ln!r}"
        # the fix must be a complete statement at the marker's own indentation, else the edit that
        # "applies the correction" writes a file that does not parse. Rebuild the whole module with
        # every marker swapped for its fix and compile *that*: it is exactly what the tool layer will
        # write, and it lets a marker sit on a compound statement (`for ...:`) needing a body.
        bug = ln
        fx = bug.split("ARENA-FIX:", 1)[1].strip()
        ind = bug[: len(bug) - len(bug.lstrip())]
        if fx.endswith(":"):                       # a compound statement needs a body of its own
            fx += chr(10) + ind + "    pass"
        rebuilt = src.replace(bug.rstrip(), ind + fx, 1)
        compile(rebuilt, "<seed+fix>", "exec")
    # the two agents must own disjoint files, or "no simultaneous same-file edits" is a slogan
    docs = [a for a in plan["agents"] if a["agent_id"] == "docs_01"][0]
    assert set(coder["writes"]).isdisjoint(docs["writes"])


# ------------------------------------------------------------------ end-to-end, from outside
def test_end_to_end_organisation_fixes_a_real_failure_and_verifies(clean_env):
    """Create -> write -> execute -> fail -> read -> correct -> re-run -> pass -> verify -> publish,
    with the second agent unblocked by the first, checked from outside the policy logic."""
    ws = clean_env / "ws"
    r = run_cli("run", "Build a tested Python CLI calculator", "--workspace", str(ws),
                "--ticks", "80", "--quiet", cwd=clean_env, home=clean_env / "home")
    assert r.returncode == 0, f"exit {r.returncode}\n{r.stdout[-2500:]}\n{r.stderr[-1500:]}"
    report = json.loads(r.stdout[r.stdout.index("{"):])
    assert report["ok"] is True
    prj = Project.load(ws, list((ws / "projects").iterdir())[0].name)
    src = prj.source_dir

    # 1. the files are real, on disk, in the project the run created
    assert (src / "src" / "calc.py").is_file() and (src / "tests" / "test_calc.py").is_file()
    assert (src / "docs" / "usage.md").is_file(), "the docs agent never got unblocked"

    # 2. the seeded failure was actually corrected: the bug marker is gone and the sum is inclusive
    assert "ARENA-BUG" not in (src / "src" / "calc.py").read_text()

    # 3. verified by running the project, not by reading the agent's mind
    pytest_run = subprocess.run([sys.executable, "-m", "pytest", "-q", "tests"], cwd=str(src),
                                capture_output=True, text=True)
    assert pytest_run.returncode == 0, pytest_run.stdout[-1500:]
    for args, want in ((["series", "4"], "10"), (["add", "2", "3"], "5"), (["div", "10", "4"], "2.5")):
        out = subprocess.run([sys.executable, "src/calc.py", *args], cwd=str(src),
                             capture_output=True, text=True)
        assert out.stdout.strip() == want, (args, out.stdout, out.stderr)

    # 4. the journal is a valid hash chain, and a fresh process agrees the project is verified
    db = prj.arena_dir / "j.db"
    rows = list(sqlite3.connect(db).execute("select seq,etype,payload from events order by seq"))
    kinds = [r[1] for r in rows]
    assert "TASK_VERIFIED" in kinds and "DEPENDENCY_READY" in kinds
    assert not [r for r in rows if r[1] == "TOOL_REFUSED"], \
        "a clean run should need no refusals: " + json.dumps([json.loads(r[2]).get("body")
                                                              for r in rows if r[1] == "TOOL_REFUSED"])
    v = run_cli("verify", "--workspace", str(ws), "--project", prj.manifest["project_id"], cwd=clean_env, home=clean_env / "home")
    assert v.returncode == 0, v.stdout[-800:]
    assert json.loads(v.stdout)["chain_ok"] is True

    # 5. execution happened: subprocesses were spent, not asserted
    ex = prj.manifest["execution"]
    assert ex["subprocess_executions"] >= 6 and ex["file_mutations"] >= 4
    assert ex["violations"] == 0 and ex["timeouts"] == 0
