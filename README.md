# Kernel of the Dynamic Multi-Agent Software Engineering Arena — Phase 1 (kernel) + Phase 2 (spawning)

Not a fixed roster of agents. A **Parent Arena** that reads a task text, derives a task graph, spawns
only the roles that graph needs, and coordinates everything through an event-sourced journal, a
plane-separated message bus, and artifact-gated scheduling.

Run the whole thing:

```bash
python3 -m pytest tests -q              # 176 tests
python3 -m arena.cli chaos-report       # 31 adversarial scenarios -> var/chaos_report.md
python3 -m arena.cli --root var/acceptance_run accept   # Phase-2 acceptance demo
```

Drive it as the Parent:

```bash
python3 -m arena.cli plan "<task text>"              # dry run: roles, tasks, generations, why
python3 -m arena.cli submit "<task text>"            # plan -> derive DAG -> spawn -> assign
python3 -m arena.cli run --ticks 200                 # advance the scheduler
python3 -m arena.cli board | status | why <agent> | trace <correlation_id>
python3 -m arena.cli pause|resume|terminate <agent>  # §2 lifecycle control (journaled)
python3 -m arena.cli verify                          # hash chain + replay parity

# Phase 2 — mid-run spawning and the org it produces
python3 -m arena.cli request --from backend_01 --role payments --skills stripe --work 4 \
        --reason "provider webhooks"                  # queue a request into var/inject.jsonl
python3 -m arena.cli run --resident --seconds 5        # a resident kernel drains that file per loop
python3 -m arena.cli status | agents | spawns -v       # counts, roster, every decision + why
python3 -m arena.cli watch --once                      # one-shot monitor snapshot
```

---

## What Phase 1 contains

| Module | Responsibility | Your section |
|---|---|---|
| `arena/message.py` | 40 message types, 3 planes, topic scheme, causality (`correlation_id`, `caused_by`, `causal_depth`) | §4 |
| `arena/lifecycle.py` | Closed FSM: 10 states, `TRANSITIONS` table, rejected edges are events not exceptions | §11 |
| `arena/journal.py` | Append-only sqlite(WAL) log, SHA-256 hash chain, `fold()` projections, atomic `claim()`, durable `waits` | §13, §15 |
| `arena/registry.py` | Agent pool, skill-coverage search, spawn lineage/epoch, idle-TTL reaping | §2 |
| `arena/graph.py` | **Derived** DAG from artifact produces/consumes, `graphlib` validation, rollback-on-cycle, wait-for graph + cycle detection | §5, §15 |
| `arena/bus.py` | Plane routing, subscription gating, causal depth cap, per-tick sender budget, durable wait/resolve/expire | §3, §6 |
| `arena/policy.py` | Pluggable cognition: simulated work, artifact-waiting, spawn-requesting, **polling anti-pattern**, `HybridPolicy` + `LLMAdapter` seam | §4.3 of FEASIBILITY |
| `arena/actor.py` | Agent actor: mailbox, lifecycle, durable waits, backlog, journaled progress cursor | §11 |
| `arena/parent.py` | Rule-based planner, scheduler, **spawn veto engine**, §9 feature-request A/B/C, pause/resume/terminate, dashboard render | §1, §9, §10, §12, §14 |
| `arena/kernel.py` | Composition root: burst mode + resident mode, worker-slot cap, **stall detection**, `from_journal()` recovery | §13 |
| `arena/chaos/` | 22 adversarial scenarios + report generator | §15 |

## The three design decisions that matter

1. **Dependencies are derived, not declared.** A task lists `produces`/`consumes`; `t_api_impl`
   depends on `t_db_schema` *because it consumes `database/schema.sql`*. The Parent never hardcodes
   "backend waits for database" — so the same code yields a different org per task. Chaos scenario
   `dynamic-org` asserts a SaaS text and an ML text produce disjoint role sets and different graphs.
2. **Artifacts gate execution, tasks only order it.** An agent blocked on a missing artifact writes a
   `waits` row and releases its slot. The publisher wakes it. Measured: `polls == 0` on the default
   path, and `> 0` attributed to the actor when the polling anti-pattern is used.
3. **The journal is the only truth.** Every state change (including the ones that *fail*) is one row.
   `Kernel.from_journal()` rebuilds agents, tasks, claims, waits, artifact versions and per-policy
   progress cursors — so the runtime survives the sandbox being recycled, and survives being driven
   from separate processes.

## Verified behaviours (chaos report is the evidence)

Spawn cap, spawn-depth cap, spawn-churn guard, not-worth-a-new-agent veto, duplicate-work refusal
(atomic first-writer-wins), explicit + derived cycle rejection with full rollback, deadlock detection
and victim resolution, message-loop cap with journalled drops, polling detection with attribution,
durable wait surviving a process death, wait-timeout escalation to the Parent, replay parity,
in-place-tampering detection via hash chain + excision-recovery, idle reaping, resource-plane
subscription gating, §9 Options A/B/C against real agents, flood budget, and a plan that correctly
refuses to spawn an org for a one-line edit.

## Phase 2 — the org can change while it is running

An agent that discovers work nobody can do emits `SPAWN_AGENT_REQUEST` (9 fields: requester, role,
skills, reason, estimated work, required inputs, expected outputs, correlation id, parent task). The
Parent receipts it, runs an ordered read-only veto pass, and either amends the graph and spawns, or
hands the task to an agent that already covers the capability, or refuses **with a reason in the
journal** — a silent reject is treated as a bug, not an optimization.

Ten rules, every one journalled: `APPROVE`, `REUSE_EXISTING`, `DEDUPLICATE`, `REJECT_CAP`,
`REJECT_SPAWN_DEPTH`, `REJECT_DUPLICATE_CAPABILITY`, `REJECT_NOT_WORTH_IT`, `REJECT_UNSUPPORTED`,
`REJECT_REQUESTER_OVERLOADED`, `ESCALATE` (+ `REJECT_CYCLE`, `REJECT_MALFORMED`,
`DEFER_FOR_CAPACITY` at commit time).

The invariants, each with a test behind it:

- **Amend before spawn.** `GRAPH_AMENDED` precedes `AGENT_REGISTERED`; a `CycleError` rolls the
  amendment back and creates no agent at all.
- **Reuse before spawn**, with the *why* recorded on both sides of the exchange.
- **One kernel object for the whole transition** — `run_resident()` never restarts, `id(k)` is
  constant across the request → evaluation → spawn → new-agent-works sequence, and `polls == 0`.
- **The journal is still the only truth.** A kernel killed at the instant of approval replays the
  same roster, the same task graph, the same request-ledger state, and the same per-agent states.
- **Agent count is a monitored surface**: `arena status` reports active/idle/working/waiting/blocked/
  completed, requests received/approved/rejected/reused/deferred, spawn depth and generation epoch,
  and remaining capacity; `arena spawns -v` shows each decision with its reason.

Phase 2 is *runtime*, not intelligence: the request in the demo is issued by a policy layer that
notices a missing capability class, and nothing in it is smarter for having been handled correctly.
`PHASE2_DESIGN.md` §8 lists the five defects this phase found in Phase 1's engine and how each is now
pinned by a test.

## Honest limitations (Phase 1 scope, per `FEASIBILITY.md`)

- **No real LLM reasoning.** `env` has 0 API keys (verified), so worker policies are deterministic
  code; `NullLLM.decide()` raises rather than pretending. Judgment calls are written to
  `var/run/inbox.jsonl` and consumed from `var/run/decisions.jsonl` — that is the Arena-as-cortex tier.
- **Work is simulated, not performed.** Policies emit progress/completion events and artifact *names*.
  Actually running `npm`/`tsc`/pytest is Phase 4+.
- **`arena/cli.py` writes no real files** and there is no git branch per agent yet (Phase 5), no
  `flock` resource manager (Phase 5), no ZeroMQ cross-process transport (Phase 3), no HTTP dashboard
  (Phase 7). The kernel is single-process by design for Phase 1: the bus *semantics* are what Phase 3
  moves to `pyzmq` (measured ~91k pub/s here), not the policies.
- Policy objects restore their *cursor* on recovery, but only the built-in ones — a custom policy
  storing non-integer state needs its own snapshot hook (Phase 5, with the artifact registry).
