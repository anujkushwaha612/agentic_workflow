# RUST_ARCHITECTURE_DECISIONS.md

Intentional deviations from the Python prototype, classified per the
migration mandate. Python is evidence; this file is the design authority
for the Rust canonical implementation.

Legend: **PRESERVE** / **RESTRUCTURE** / **REMOVE** / **CHANGE** / **DEFER**.

---

## ADR-001 — Actor does not hold a Kernel pointer

**Python:** `AgentActor.kernel` and `ActorContext.kernel` are live
aliases; policies can (and `PollUntilReady` does) reach
`ctx.kernel.polls`.

**Architecture:** Cognition proposes; the runtime validates and executes.
Cognition must not mutate Kernel state.

**Rust:** `AgentActor` owns only per-agent state. All reads/writes go
through [`ActorRuntime`]. `PolicyContext` is a snapshot taken at the
start of the turn plus `log` / `poll_hit` / message builders. There is
no `kernel` field.

**Classification:** RESTRUCTURE.

---

## ADR-002 — Single cognition execution loop

**Python:** `_use_cognition()` dual-paths `policy.step` vs
`cognition.decide`. Policy-only agents emit `ERROR_REPORT` on crash;
bound cognition emits `COGNITION_ERROR`.

**Architecture:** Every agent has a cognition source. `PolicyCognition`
is an adapter, not a second runtime.

**Rust:** `AgentActor::new(policy)` wraps the policy in
`PolicyCognition`. `run_step` is always observe → decide → validate →
tools → apply. Policy crashes surface as `CognitionError::Other` and
are journalled `COGNITION_ERROR` (kind=`error`). Capability breaches
journal `kind=capability`.

**Classification:** CHANGE (journal event for an unbound-policy crash).
The apply-action semantics (WAIT/ESCALATE/COMPLETE/PUBLISH) are
PRESERVE.

---

## ADR-003 — Observation is a per-turn snapshot

**Python:** `observe()` reads the kernel live; `Observation.context` is
the same `ActorContext` the policy mutates through.

**Architecture:** Stale observations must not justify actions.
Cognition must not hold a live mutable world.

**Rust:** `ActorCtx::open` copies graph/task/unmet/roster/workspace
into owned fields. `observe()` builds `Observation` from that copy.
The next `run_step` opens a fresh snapshot. `Observation` has no
`context` pointer (already true in the Rust cognition module).

**Classification:** RESTRUCTURE.

---

## ADR-004 — Cognition cursor lives on the Cognition trait

**Python:** `policy_cursor` / `restore_cursor` poke `_steps` / `_worked`
/ `_n` / `_asked` on the policy object via `getattr`.

**Architecture:** Future LLM sources must restore without pretending to
be a policy.

**Rust:** `Cognition::policy_cursor` / `restore_cursor` default to 0 /
no-op. `PolicyCognition` forwards to `Policy`. The actor never
inspects policy fields.

**Classification:** RESTRUCTURE.

---

## ADR-004b — Repeated-COMPLETE guard uses previous last_action

**Python:** `last_action` is assigned *before* `_apply_action`, so
`task is None and last_action == COMPLETE` is true on the first
no-task COMPLETE. The comment says a genuinely idle agent should still
complete once.

**Rust:** `apply_action` runs first; `last_action` is updated after.
The first no-task COMPLETE journals `TASK_COMPLETED`; a second is
ignored.

**Classification:** CHANGE (bugfix of documented intent).

---

## ADR-005 — Shared-brain audit without `obj_id`

**Python:** `PolicyCognition.fingerprint` includes `obj_id = id(self)`.

**Rust:** No stable object identity. Fingerprint uses
`prompt_sha256_16` (class|agent_id) plus per-actor transcripts.
Documented in the cognition module; kernel `cognition_report` (M8)
will use prompt hashes + transcript lengths.

**Classification:** CHANGE (already decided at M6).

---

## ADR-006 — ActorRuntime is the Kernel seam (M8)

Kernel will implement `ActorRuntime`. The actor tests use a harness
that owns the same modules (journal, bus, registry, graph, clock)
without being a Kernel. This is the ownership model from the migration
plan §8.3: Kernel owns mutable state; `step_actor` will `remove` the
actor, `run_step(&mut self)`, reinsert.

**Classification:** RESTRUCTURE (vs Python's self-referential web).
**DEFER:** Kernel composition, tick loop, `from_journal` — M8.

---

## ADR-007 — Tick ordering (preview for M8)

Python `Kernel.run`: tick → cortex → timeouts → deadlock → schedule →
actors → reap → checkpoint.

Evaluated:

| Step | Kind | Decision |
|---|---|---|
| clock/tick + budget reset | invariant (determinism, sender budget) | PRESERVE |
| cortex decisions before schedule | invariant (human override precedes spawn) | PRESERVE |
| expire timeouts before schedule | invariant (a timed-out wait is an escalation, not work) | PRESERVE |
| deadlock detect before schedule | invariant (don't assign into a cycle) | PRESERVE |
| schedule before actors | invariant (new assignments must be visible this tick) | PRESERVE |
| inbox drain then one run_step | invariant (wake-then-work; D2 sleeping-action break) | PRESERVE |
| reap after actors | invariant (don't reap an agent that just finished useful work) | PRESERVE |
| checkpoint at run boundary | invariant (cursors survive restart) | PRESERVE |

No Python-only quirks identified that should be removed from this
sequence. M8 will implement it and re-evaluate with evidence.

---

## ADR-008 — Side-file anchoring (M8)

PRESERVE: explicit root > journal directory > write nothing (never CWD).

---

## ADR-009 — Kernel is composition root, not a god object

**Python:** `Kernel` owns clock/journal/graph/registry/bus/actors *and*
Parent, Spawn, tools, planner, workspaces, cortex file IO.

**Rust:** Kernel owns mutable runtime state and implements `ActorRuntime`.
Parent, Spawn, Tools, planner, LLM providers are **seams**:
`spawn_requests` queue, `ToolExecutor` trait, `CognitionFactory`,
`submit(text)` returns empty. Cortex `force_state` / `force_terminate`
stay on the tick loop so a later Parent does not have to own scheduling.

**Classification:** RESTRUCTURE.

---

## ADR-010 — Actor/Kernel ownership (remove → run_step → reinsert)

**Python:** `AgentActor.kernel` is a live pointer; tick holds both.

**Rust:** `&mut Kernel` is the tick. `step_actor` `remove`s the actor,
calls `run_step(&mut self)`, reinserts. No `Arc<Mutex<Everything>>`,
no kernel field on `AgentActor`. Two kernels never share maps.

**Classification:** RESTRUCTURE (safety).

---

## ADR-011 — Quiet `from_journal` is a pure projection

**Python:** recovery always emits `REPLAY_COMPLETE`.

**Rust:** `from_journal(path, quiet, opts)`. Quiet: no journal append,
no lifecycle side effects, no tools, no publish, no workspace mutation.
Loud appends exactly one `REPLAY_COMPLETE`. Live snapshot == recovered
snapshot (agents/tasks/artifacts/tick). Side-file anchor is
explicit root > journal dir > write nothing (never CWD) — ADR-008.

Counters that are not evented (`work_done`, `msgs_sent`) overlay from
the latest `SNAPSHOT`. `STATS` fold no longer zeros missing fields
(Python STATS payloads often omit them).

**Classification:** CHANGE (correctness of recovery).

---

## ADR-012 — Cognition factory is per-agent

**Python:** role → one policy class; instances can be accidentally shared.

**Rust:** `CognitionFactory` is `Fn(&str) -> Box<dyn Cognition>`. The
agent id is an argument so `PolicyCognition` fingerprints
(`class|agent_id`) cannot alias. Kernel never stores a shared brain.

**Classification:** RESTRUCTURE.

---

## ADR-013 — Tick order (confirmed)

ADR-007's sequence is implemented verbatim:

tick++ / clock / `bus.reset_tick` → cortex `force_state`/`force_terminate`
→ expire waits → deadlock → schedule → eligible (least `steps_run`,
then woke, cap = `max_concurrent_workers` not agent count) → inbox drain
then one `run_step` → reap → stop-if-done → checkpoint at run boundary.

Worker cap ≠ agent count. COMPLETED + backlog is eligible again.
PAUSED is not.

**Classification:** PRESERVE.

---

## ADR-014 — Alignment: Intent is the apply IR; Parent is above Kernel

**Was:** Actor mapped `Intent` → `Action` → `apply_action`. Kernel parsed
`var/decisions.jsonl` / `var/inject.jsonl` inside the tick and named
`parent_escalate` / `parent_spawn_request` on `ActorRuntime`. Kernel
fields were `pub`. `AgentActor::new` required a `Policy`.

**Architecture:** Cognition proposes an `Intent`. The runtime validates
and applies it. `Action` exists only inside `PolicyCognition`. Parent
is a Kernel collaborator, not an actor method. Cortex/inject/inbox
files are a `ControlPlane` adapter, not Kernel internals.

**Rust:**
1. `AgentActor::apply_intent` executes a validated `Intent`. Turn
   action strings (`WAIT`/`COMPLETE`/`PUBLISH`/`ESCALATE`/`NOOP`/`PROCEED`)
   stay the sleeping-action contract.
2. `WorldView` (reads) + `RuntimeEffects` (mutations).
   `ActorRuntime` is the blanket combination. Effects are
   `record_escalation` / `enqueue_spawn_request`.
3. `ControlPlane` + `FileControlPlane`. Disk kernels bind the file
   adapter at construction (`bind_control_plane` replaces it). Tick
   calls `poll_decisions` then `apply_decision`. Kernel fields are
   private; `journal()` is the observer accessor.
4. `AgentActor::from_cognition` is the non-policy constructor. Kernel
   `bind_actor` uses it when a role factory is registered.
5. `ToolExecutor` lives in `tools`. Unbound execute is
   `REFUSE_NO_EXECUTOR`.

**Classification:** RESTRUCTURE (pipeline IR and seams). Apply
semantics PRESERVE.

---

## ADR-015 — M9 Parent / spawn funnel

**Python:** `ParentArena` holds a live `kernel` pointer. `decide_spawn` is
intake → evaluate → commit. `Kernel.submit` calls the planner.
`Kernel.schedule` is `parent.schedule`. Recovery rehydrates the ledger
from `fold()["requests"]`. `ControlPlane` decisions `approve_spawn` /
`amend` were swallowed by Kernel's force_* parser in the Rust seam.

**Architecture:** Parent is a Kernel collaborator, not an Actor method
and not a Kernel field pointing back at Kernel. The funnel order is a
safety property. Graph.amend before any agent. Quiet recovery must not
re-approve spent rids.

**Rust:**
1. `src/spawn.rs` — request FSM, catalog, fingerprint, ledger.
2. `src/parent.rs` — `Parent` (ledger/catalog/planner/stats) with **no
   Kernel pointer**. Funnel methods are `impl Kernel`.
3. `Kernel.submit` runs `RuleBasedPlanner` (engine, not product). Empty
   match invents nothing.
4. `Kernel.schedule` drains spawn requests, then assigns unowned
   pending (cover, then idle, then spawn-under-cap).
5. Assignment is claim-first (`DUPLICATE_CLAIM` on loss).
6. `AGENT_REGISTERED` is journalled before `make_actor`.
7. `from_journal` rehydrates the ledger **using the fold rid** (CHANGE
   vs Python, which re-issued `rq-NNNN` and could collide).
8. Journal fold projects `SPAWN_REJECTED` so DEDUPLICATED/REJECTED
   survive replay (Python fold omitted this).
9. `ControlDecision::{ApproveSpawn, Amend}` are parsed, not dropped.
10. Plan cycle rolls the graph back (CHANGE vs Python, which left the
    cyclic graph in place).

**KEEP:** M1–M8 modules, Intent apply IR, WorldView/RuntimeEffects,
per-agent cognition, quiet recovery purity, worker cap ≠ agent count.
**REFACTOR:** `submit`/`schedule`/`assign`, ControlPlane spawn/amend,
journal fold of spawn rejects, ledger rid on rehydrate.
**DEFER:** real Tools, workspace/Git, LLM providers, ArenaCognition,
ZeroMQ, Kanban, arena-code (M10+).

**Classification:** RESTRUCTURE (ownership) + PRESERVE (funnel
semantics) + CHANGE (recovery rid, plan-cycle rollback, SPAWN_REJECTED
fold).

---

## Not in this milestone (still)

Tools implementation, product/`arena-code`, Kanban, external LLM
providers, ArenaCognition, ZeroMQ. Kernel exposes the seams so those
layers plug in without rewriting Actor or Parent.
