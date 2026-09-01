"""The deterministic tier's acceptance plan: a real, tiny, *tested* Python project.

Why this project and not the React/FastAPI/PostgreSQL one from the brief: this sandbox has no
postgres binaries and 1 GiB of RAM (measured, `REAL_AGENT_RUNTIME_DESIGN.md` §11.3), so a plan that
needed them could never be *verified* here - it would end exactly the way this design warns against,
with a green status painted over something that never ran. A CLI calculator with a real test suite
exercises the whole chain instead: files on disk, an import, a subprocess CLI run, genuine failures,
corrections, reruns, verification.

The two bugs below are in the source on purpose, each with its fix recorded next to it as a comment.
Not because the runtime needs a plant, but because the acceptance requirement is "the agent must
observe a real failure and correct it" - so it is handed genuine defects, and the tests are what catch
them. The replacement text the agent applies is read out of the file *from disk*; nothing in this
module edits the project, and nothing in the engine knows it exists.

Two agents, two non-overlapping write sets (`src`+`tests` vs `docs`). The docs task's own verify
command *runs the coding agent's program*, so the dependency between them is proven by execution
rather than asserted in a test file.
"""
from __future__ import annotations

from typing import Any

SRC = '''"""arena-code: a tiny calculator. Written to disk by an agent, through the tool layer."""
from __future__ import annotations


def add(a: float, b: float) -> float:
    return a + b


def sub(a: float, b: float) -> float:
    return a - b


def mul(a: float, b: float) -> float:
    return a * b


def div(a: float, b: float) -> float:
    if b == 0:
        raise ValueError("division by zero")
    return a / b


def _stop(n: int) -> int:
    """The exclusive bound for the series loop: `range(1, _stop(n))` runs 1..n."""
    return n


def sum_series(n: int) -> int:
    """Sum 1..n inclusive."""
    total = 0
    for i in range(1, _stop(n)):  # ARENA-BUG: this bound is exclusive. ARENA-FIX: for i in range(1, _stop(n) + 1):
        total += i
    return total


def main(argv=None) -> int:
    import argparse
    from calc_cli import compute
    ap = argparse.ArgumentParser(prog="calc", description="tiny calculator")
    ap.add_argument("op")
    ap.add_argument("a", type=float)
    ap.add_argument("b", type=float, nargs="?", default=0.0)
    ns = ap.parse_args(argv)
    print(compute(ns.op, ns.a, ns.b))  # ARENA-BUG: floats print as `5.0`. ARENA-FIX: print(_fmt(compute(ns.op, ns.a, ns.b)))
    return 0


def _fmt(v):
    """Whole numbers print without a trailing `.0`."""
    return int(v) if isinstance(v, float) and v == int(v) else v


if __name__ == "__main__":
    raise SystemExit(main())
'''

CLI = '''"""arena-code: the arithmetic front end. Authored here; deliberately out of this suite's scope."""
from __future__ import annotations


def sum_series(n: int) -> int:
    """A copy of the series sum, kept local so the CLI does not depend on `calc` being importable."""
    total = 0
    for i in range(1, n):  # ARENA-BUG: exclusive bound. ARENA-FIX: for i in range(1, n + 1):
        total += i
    return total


def compute(op: str, a: float, b: float = 0.0):
    from calc import add, div, mul, sub
    ops = {"add": add, "sub": sub, "mul": mul, "div": div}
    if op == "series":
        return sum_series(int(a))
    if op not in ops:
        raise ValueError("unknown op %r" % op)
    return ops[op](a, b)
'''

TESTS = '''"""arena-code: the test suite. Also written by an agent, also real.

The module under test is `src/calc.py`; the CLI front end is `src/calc_cli.py`. A failing
assertion here means the correction belongs in one of those two files, never in this one. The series bound
is computed by `_stop` in `src/calc.py`, and `range(1, _stop(n))` is deliberately exclusive there.
Three assertions can fail, with three different causes in two different files: a wrong bound in
`src/calc.py`, a wrong print format in `src/calc.py`, and the CLI's own copy of the series in
`src/calc_cli.py` (`python3 src/calc.py series 4` runs that file, so a bad number there is *not*
evidence about `src/calc.py`). Reading one file is not enough to know where a failure lives. Nothing in this
suite asserts anything about `src/calc_cli.py`'s own copy of `sum_series`, so a failure here is not
evidence about it.
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "src" / (name + ".py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_basic_operations():
    c = load("calc")
    assert c.add(2, 3) == 5
    assert c.sub(9, 4) == 5
    assert c.mul(3, 4) == 12
    assert c.div(10, 4) == 2.5


def test_division_by_zero_raises():
    c = load("calc")
    try:
        c.div(1, 0)
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_series_is_inclusive():
    c = load("calc")
    assert c.sum_series(4) == 10, "1+2+3+4 must be 10, got %s" % c.sum_series(4)


def test_cli_series_output():
    out = subprocess.run([sys.executable, "src/calc.py", "series", "4"],
                         capture_output=True, text=True, cwd=str(ROOT))
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "10", "CLI said %r, expected '10'" % out.stdout.strip()


def test_cli_arithmetic():
    out = subprocess.run([sys.executable, "src/calc.py", "add", "2", "3"],
                         capture_output=True, text=True, cwd=str(ROOT))
    assert out.stdout.strip() == "5", out.stdout
'''

DOC = """# calc

A tiny calculator built inside an Arena workspace.

```
python3 src/calc.py add 2 3      # -> 5
python3 src/calc.py series 4     # -> 10
```

Operations: add, sub, mul, div, series. Division by zero raises ValueError.
"""


def plan() -> dict[str, Any]:
    """The default plan, in exactly the shape `--plan <file.json>` accepts."""
    return {
        "goal": "Build a small, tested Python CLI calculator (src/calc.py, src/calc_cli.py, "
                "tests/test_calc.py) and document it.",
        "src_dir": "source",
        "agents": [
            {
                "agent_id": "coder_01",
                "role": "coder",
                "task": {
                    "task_id": "t_calc_code",
                    "title": "author calculator + tests, run them, correct what they catch",
                    "role": "coder",
                    "skills": ["python", "cli", "testing"],
                    "est_work": 4.0,
                    "produces": ["src/calc.py", "src/calc_cli.py", "tests/test_calc.py"],
                    "consumes": [],
                    "verify": [
                        ["python3", "-m", "pytest", "-q", "tests"],
                        ["python3", "src/calc.py", "series", "4"],
                        ["python3", "src/calc.py", "add", "2", "3"],
                    ],
                },
                "writes": ["src", "tests"],
                "reads": ["src", "tests"],
                "fix_files": [],
                "seed": {"src/calc.py": SRC, "src/calc_cli.py": CLI, "tests/test_calc.py": TESTS},
                "policy": {
                    "test_command": ["python3", "-m", "pytest", "-q", "tests"],
                    "wait_for": "",
                },
            },
            {
                "agent_id": "docs_01",
                "role": "docs",
                "task": {
                    "task_id": "t_calc_docs",
                    "title": "document the CLI against the code that actually exists",
                    "role": "docs",
                    "skills": ["docs"],
                    "est_work": 1.0,
                    "produces": ["docs/usage.md"],
                    "consumes": ["src/calc.py"],
                    # verify runs in source/, so paths are project-relative from there. The first
                    # command is the coding agent's program: if its bug is not fixed, this task's
                    # verification fails and the docs agent cannot complete either.
                    "verify": [
                        ["python3", "src/calc.py", "series", "4"],
                        ["python3", "-m", "pytest", "-q", "tests"],
                    ],
                },
                "writes": ["docs"],
                "reads": ["src", "tests", "docs"],
                "fix_files": ["docs/usage.md"],
                "seed": {"docs/usage.md": DOC},
                "policy": {"test_command": ["python3", "-c", "pass"],
                           "wait_for": "artifact:src/calc.py"},
            },
        ],
        "commit_after_verify": True,
    }
