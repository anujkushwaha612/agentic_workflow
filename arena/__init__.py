"""Phase 1 of the Dynamic Multi-Agent Software Engineering Arena.

Kernel only: clock, message protocol, agent lifecycle FSM, append-only event-sourced journal,
agent registry, dependency DAG, bus delivery policy, agent actor + policies, Parent Arena
(orchestrator + decision engine).

Deliberately NOT in Phase 1 (they are later phases, stubs only where an interface is needed):
  - durable cross-process ZeroMQ transport (Phase 3)
  - shared-resource lock manager / git artifacts (Phase 5)
  - live HTTP dashboard (Phase 7)

Everything here is pure stdlib and deterministic under a virtual clock.
"""

__version__ = "0.1.0"
__all__ = ["clock", "message", "lifecycle", "journal", "registry", "graph",
           "bus", "policy", "actor", "parent", "kernel"]
