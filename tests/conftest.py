"""Phase 1 test suite. Run:  python3 -m pytest tests -q   (or  python3 -m arena.cli selftest)

These are not happy-path tests: every file attacks the kernel. The chaos scenarios are re-run
here as pytest cases so CI gets the same gate the report shows.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
