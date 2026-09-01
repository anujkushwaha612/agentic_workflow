"""`arena-code`: the launcher layer of the Arena runtime (Phase 2.5).

Separation of concerns, stated once so the tree enforces it:

  arena/          the *engine* — kernel, orchestration, lifecycle, journal, tools, cognition seam.
                  It knows nothing about project directories, prompts from humans, or CLIs.
  arena/code/     the *product* — project workspaces, the `arena-code` CLI, environment doctor,
                  concrete cognition sources (policy tier now, provider tier behind the same seam).

Nothing in `arena/` imports from here. The dependency is one-way.
"""
from __future__ import annotations

__all__ = ["RUNTIME_VERSION"]

RUNTIME_VERSION = "0.3.0-m0m2"
