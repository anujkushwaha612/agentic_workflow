"""arena-code: a tiny calculator. Written to disk by an agent, through the tool layer."""
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
    for i in range(1, _stop(n) + 1):
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
    print(_fmt(compute(ns.op, ns.a, ns.b)))
    return 0


def _fmt(v):
    """Whole numbers print without a trailing `.0`."""
    return int(v) if isinstance(v, float) and v == int(v) else v


if __name__ == "__main__":
    raise SystemExit(main())
