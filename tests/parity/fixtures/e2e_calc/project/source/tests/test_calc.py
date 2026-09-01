"""arena-code: the test suite. Also written by an agent, also real.

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
