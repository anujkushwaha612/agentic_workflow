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

## Not in this milestone

Parent, Spawn, Tools, product/`arena-code`, Kanban, external LLM
providers, ZeroMQ. Actor exposes the seams (`ActorRuntime::execute_tool`,
`bind_cognition`, spawn-request as a Message) so those layers plug in
without rewriting Actor.
