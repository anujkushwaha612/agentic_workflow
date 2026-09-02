"""arena-code: the arithmetic front end. Authored here; deliberately out of this suite's scope."""
from __future__ import annotations


def sum_series(n: int) -> int:
    """A copy of the series sum, kept local so the CLI does not depend on `calc` being importable."""
    total = 0
    for i in range(1, n + 1):
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
