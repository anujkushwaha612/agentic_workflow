# Phase 2 Design — Dynamic Spawning, Mid-Run Replanning, Agent-Count Monitoring

Status: **design only** — no Phase 2 code written. Every claim below was checked against the source
and, where it mattered, against a live probe run in this session (probes under `var/p2probe*`).

---

## 0. What Phase 1 already gives me (read from source, not from memory)

The spawn path is **already event-driven and already on the per-tick path**. Phase 2 strengthens it.

| Existing piece | Where | Verified behaviour |
|---|---|---|
| Policy emits a request | `policy.py` `EscalateOnComplexity.step` | `ctx.self_msg(SPAWN_AGENT_REQUEST, …, to="parent")` → an `Act.PUBLISH`. A real runtime event, not a test call. |
| Bus hook | `kernel.py` `Kernel.publish()` | `FEATURE_REQUEST` diverted to `parent.feature_requests`; `SPAWN_AGENT_REQUEST` enqueued in `actor.py`'s `Act.PUBLISH` branch into `parent.spawn_requests`. |
| Drained every tick | `parent.py` `schedule()` line 1 | `self.drain_spawn_requests()`; `schedule()` runs from `Kernel.run()` when `auto_assign=True`. **Mid-run processing already exists.** |
| Veto engine | `parent.py` `evaluate_spawn()` | 7 ordered rules → `Decision(ok, rule, detail, owner)`. |
| Spawn | `parent.py` `_spawn()` | cap-checked `if agent_id is None and len(reg.active()) >= cap: return None`; registers, `bus.subscribe`, `make_actor`, journals `AGENT_REGISTERED`. |
| Reuse check | `registry.py` `cover()` | token-based skill/role overlap (Phase-1 `"" in role` substring bug already fixed). |
| Validated mutation | `graph.py` `amend()` | snapshot → mutate → `validate()` → `_restore()` on `CycleError`. **Atomic — and currently bypassed by the spawn path.** |
| Lifecycle | `kernel.py` `transition()` | sole FSM entry; journals accepted *and* rejected edges. |
| Resident loop | `kernel.py` `run_resident()` | exists (`monotonic` deadline + `time.sleep`), **but nothing can feed it**: no injection API, no idle detection. |
| Replay | `kernel.py` `from_journal()` | restores backlog, policy cursors, tick. |

### 0.1 Verified facts I will not re-litigate

* **`fold()` DOES project `PLAN_AMENDED`.** `journal.py:371` `elif t in (PLAN_AMENDED, REPLAN)` reads
  `added_tasks`/`added_deps` with the same field names `TaskSpec.snapshot()` writes. My first draft
  claimed otherwise; that was wrong and I am recording the correction here rather than designing
  around it. Probe `ATTACK 4`: a dynamically-spawned `payments_01` (epoch 1) and its task
  `t_payments` both came back byte-equal from `from_journal`
  (`state COMPLETED, task_id None, msgs_sent 4, work_done 3.0, steps_run 5, policy_cursor 0`).
* **The cap cannot be over-run at commit time.** Probe: `max_active_agents=2`, 2 live agents,
  `decide_spawn` → `MAX_ACTIVE_AGENTS`, 0 `PLAN_AMENDED` rows, no `payments_*` agent created.
* **Replay parity of a *normal* run is intact** (Phase 1: 5 processes, 195 rows, chain VALID).
  The two snapshot diffs my probe printed (`agents`, `tasks.deps`) are **probe artifacts** — I
  registered agents directly without journaling `PLAN_CREATED`, and `from_journal` adds tasks with
  `derive=False` (`kernel.py:492`) without calling `derive_edges()`. Item 2.6 addresses the latter.

### 0.2 Two real Phase-1 defects, reproduced this session

**D1 — a mid-run spawn mutation is NOT cycle-validated, and poisons the graph.** `decide_spawn`
calls `graph.add(new_task)` directly, never `graph.amend()`, and never calls `validate()`.

```
probe: t_pay3 consumes contracts/api.json (produced by t_be),
       t_be    consumes docs/x.md        (produced by t_pay3)
  decide_spawn -> {"rule": "APPROVED"}          # no cycle check ran
  graph.validate() -> CycleError: t_be -> t_pay3 -> t_be
  graph.order()    -> CRASH CycleError ('nodes are in a cycle', ['t_be','t_pay3','t_be'])
  agents on the poisoned graph: ['backend_01','database_01','payments_01']   # still "healthy"
```
The org keeps running on a graph that cannot be topologically sorted. This directly violates the
Phase-2 requirement *"amend the plan → validate the amended graph for cycles → if a cycle is found,
reject the amendment and journal the rejection"*. It is also invisible: `status()` and `report()`
both returned "ok".

**D2 — `actor.last_action` case mismatch makes a `break` unreachable.** `actor.py` sets
`last_action = str(Act.X)` → `"PUBLISH"`; `kernel.py:341` breaks on
`actor.last_action in ("wait","escalate","complete","error")`. Probe: `last_action: 'PUBLISH'`,
`break unreachable: True`. So after a wake-up the inbox is drained *and* an extra step runs in the
same tick. Harmless in Phase 1's run-to-completion; in Phase 2 it makes request→response timing
depend on how much happened to be in the mailbox — exactly the timing I have to prove.

**G1 — the outcome is journalled but the request is not.** No `SPAWN_REQUEST_RECEIVED` row exists;
`MessageType` has no such member (probe: `AttributeError`). Rejections *are* journalled, which is
good, but there is no "we received it and are evaluating it" record, so a request that dies inside
`evaluate` leaves no trace.

**Rule-string coupling (not a defect, a cost):** renaming decision rules touches 6 assertion sites —
`chaos_kernel.py:95,96,135,643,645,647` and `tests/test_kernel.py:153,165`.

---

## 1. Phase 2 architecture

**Untouched:** `lifecycle.py` FSM table (and its 8 tests), journal hash-chain schema, `graph.py`
derivation/`amend()`, `bus.py` planes + depth cap + budget, `actor.py` step protocol, every Phase-1
invariant.

```
 policy                       parent (ParentArena)                 kernel
 ──────                       ───────────────────                 ──────
 request_specialist() ──┐                                            ┌─ inject_request()  (new)
                         ▼      ┌────────────────────────────┐     │
              SPAWN_AGENT_REQUEST│ intake()   dedup + record  │◄────┤
              (bus message)      │ evaluate() 7 rules         │     ├─ idle_predicate()  (new)
                                 │ commit()   graph-FIRST      │     │   (replaces blind sleep)
                                 │  └ graph.amend()  ← D1 fix  │     └─ wake_ready()
                                 └──────────┬─────────────────┘
                                            ▼
        SPAWN_REQUEST_RECEIVED / GRAPH_AMENDED / REQUEST_REROUTED /
        SPAWN_REJECTED / SPAWN_ESCALATED  ─────────────────────► journal
```

New module **`arena/spawn.py`**: `SpawnRequest`, `RequestState`, `CapabilityCatalog`, `SpawnLedger`.
`parent.py` keeps the veto *policy* and delegates intake/commit. `parent.py` is 720 lines with 21
public methods; the ledger is a separate state machine and deserves its own auditable diff.

---

## 2. Exact design

### 2.1 The request is a typed event whose shape is enforced structurally

`message.py` adds `SPAWN_REQUEST_RECEIVED`, `GRAPH_AMENDED`, `GRAPH_AMEND_REJECTED`,
`REQUEST_REROUTED`, `SPAWN_ESCALATED` (fixes G1).

`spawn.py`:
```python
@dataclass(slots=True)
class SpawnRequest:
    requester_agent_id: str; requested_role: str
    required_skills: tuple[str, ...]; reason: str; estimated_work: float
    required_inputs: tuple[str, ...]; expected_outputs: tuple[str, ...]
    parent_task_id: str | None; correlation_id: str
    capability_class: str = "general"; requires_judgment: bool = False
    mid: str = ""; rid: str = ""
    def fingerprint(self) -> str        # dedup key: (requester, class, sorted(skills), sorted(outputs))
    def to_task_spec(self) -> TaskSpec  # ONLY path from request -> task; wires required_inputs as consumes
    @classmethod
    def from_message(cls, m) -> "SpawnRequest"   # raises MalformedRequest
```

**The honest version of "the agent cannot construct a malformed request".** `ActorContext.request_specialist(role, reason, required_inputs, expected_outputs, est_work)` becomes the single
sanctioned helper and it *builds the whole `SpawnRequest` and serialises every field* — so the
correct path is the only path a policy has any reason to take. I am **not** claiming a hand-built
`Message` is impossible; I make it *unignorable*: `from_message()` raises `MalformedRequest`, the
parent journals `SPAWN_REJECTED rule=REJECT_MALFORMED`, and a test asserts that row exists.
"Structural" = the failure mode is a journalled rejection, not silence. That I can test.

### 2.2 Request lifecycle (the agent FSM is **not** extended)

```
RECEIVED → EVALUATING → APPROVED_COMMITTED
                      → REROUTED           (existing agent took the work)
                      → REJECTED           (rule code required, always journalled)
                      → ESCALATED          (written to inbox for the cortex)
                      → DEDUPLICATED       (merged onto an in-flight rid)
```
Held in a `SpawnLedger` (in-memory, `rid`-keyed) **and mirrored by events**, so `fold()` rebuilds it.
Ledger answers the one question an event log answers badly: "is a request with this fingerprint
in-flight *right now*?"

### 2.3 Decision algorithm (ordered; every branch journalled; **graph before agent**)

```
intake(req):    # no mutation at all in this phase
  journal SPAWN_REQUEST_RECEIVED (rid, requester, role, fingerprint)      [fixes G1]
  fingerprint in-flight or already resolved → REJECT_DUPLICATE, mark DEDUPLICATED
  missing/invalid fields                    → REJECT_MALFORMED

evaluate(req):  # Phase-1 order and thresholds preserved, canonical names
  1 requester.epoch >= max_spawn_epoch        → REJECT_SPAWN_DEPTH
  2 len(active) >= max_active_agents          → REJECT_CAP
  3 registrations > 2 * cap (churn)           → REJECT_CAP
  4 capability_class not in SERVICEABLE       → REJECT_UNSUPPORTED
  5 cover(role, skills) non-empty             → REUSE(owner)      → REROUTED, never spawns
  6 est < min_share_of_remaining*remaining    → REJECT_NOT_WORTH_IT
  7 reg.overloaded(requester)                 → REJECT_REQUESTER_OVERLOADED
  8 requires_judgment and a marginal 5/6/7    → ESCALATE
  else                                        → APPROVE

commit(req, decision):
  a) spec   = req.to_task_spec()                       # required_inputs -> consumes -> derived deps
  b) res    = graph.amend([spec], {spec.task_id: producers_of(req.required_inputs)})
     └ not ok → journal GRAPH_AMEND_REJECTED, return REJECT_CYCLE
       **no agent, no owner, graph restored byte-for-byte**                [fixes D1]
  c) REROUTED → assign(spec.task_id, owner); journal REQUEST_REROUTED{rid, owner, why}
  d) APPROVE  → aid = _spawn(...)
        if aid is None: journal DEFERRED_FOR_CAPACITY, task stays pending/unowned, return
        (never `aid or role` — no owner named "None")                      [fixes the orphan risk]
  e) assign(spec.task_id, aid)
  f) journal SPAWN_APPROVED + GRAPH_AMENDED(+order_after) in ONE append with the state changes
  g) kernel.wake_ready("graph amended")  → newly-ready work admitted this tick
```

Why (b) before (d): the cycle gate must run *before* an agent exists. Today it never runs at all.
`amend()` already does snapshot+restore atomically, so this is a **re-ordering onto existing
machinery**, not new machinery.

### 2.4 Monitoring

`Kernel.metrics()`, read-only single pass:
```python
{"active","idle","working","waiting","blocked","escalated","completed",
 "requests_received","requests_approved","requests_rejected","requests_deduplicated",
 "requests_escalated","by_rule":{...},
 "max_epoch","mean_epoch","spawn_depth_hist":{...},
 "remaining_capacity","agent_budget","worker_slots_in_use","workers_max",
 "reuses","spawned_by_agents","open_graph_tasks","stalled"}
```
CLI: `arena agents` (roster with `origin`, `spawned_by`, `epoch`, queue), `arena spawns` (ledger,
newest first), `arena status` (+ `monitoring` block), `arena watch --every N`,
`arena request --from … --role … --needs … --produces …`, `arena run --resident S`.
Self-check identity: `received == approved + rejected + dedup + escalated + in-flight`.

### 2.5 CapabilityCatalog

`SERVICEABLE` classes (`general`, `api`, `data`, `frontend`, `payments`, …); anything else
(`gpu-training`, `k8s-ops`) → `REJECT_UNSUPPORTED`. Lives on the Kernel, listed by the CLI. A policy
hook the project can extend, not a test stub.

### 2.6 Resident mode, fixed for real injection

* `Kernel.idle_predicate()` — true when no agent is runnable **and** no durable wait is pending
  **and** the spawn queue is empty. `run_resident` waits on an `Event` with this predicate instead of
  `time.sleep(0.05)`; a kernel parked for injection is not "stalled", so `stall_limit` stays honest.
* `Kernel.inject_request(req)` — appends `SPAWN_REQUEST_RECEIVED`, enqueues, `event.set()`. One
  writer, one wake-up, zero polling.
* `run_resident(seconds, on_tick=cb)` — the callback is how the demo observes a transition *while
  the loop is alive*, which is what "MID-RUN" must mean.
* **D2 fix:** one `SLEEPING_ACTIONS = {"wait","escalate","complete","error"}` set in `policy.py`,
  imported by both sites, compared as `actor.last_action.lower()` — so they cannot drift again.
* **0.1 last bullet:** `from_journal` calls `graph.derive_edges()` after loading tasks, so a replayed
  kernel does not lose artifact-derived edges (`t_be.deps` came back `[]` in my probe).

### 2.7 Journal

`fold()` gains `GRAPH_AMENDED` (same field names as `PLAN_AMENDED`) and `spawn_ledger` projections.
`events` table schema unchanged → Phase-1 journals still replay.

---

## 3. State-machine changes

| | change |
|---|---|
| Agent FSM `lifecycle.py` | **none.** 10 states, `TRANSITIONS`, 10 tests byte-identical. A spawned agent enters at `CREATED` like a planned one — proof that dynamic spawning adds no lifecycle special-case. |
| Request FSM `spawn.py` | new, 6 states (§2.2), `RequestState.can()`; illegal request transition journals `ILLEGAL_TRANSITION` with `scope="request"` and mutates nothing. |
| Journal | new message types; `fold()` +2 projections; no `events` schema change. |

## 4. Failure / recovery cases → tests

| Case | Journalled evidence asserted |
|---|---|
| Cycle from an amendment | `GRAPH_AMEND_REJECTED`; **no agent created**; `order()` identical before/after |
| Cap hit at commit (drift) | `DEFERRED_FOR_CAPACITY`; task pending/unowned; no owner `"None"` |
| Crash right after approval | replay has the agent (`epoch>0`) **and** the task **with its artifacts and derived deps** |
| Crash after registration, before assignment | agent exists, `task_id is None`, no phantom task, snapshot parity holds |
| Duplicate request in-flight | `DEDUPLICATED`; count agents with that role == 1 |
| Same tick, two requests for the same role | exactly one spawn; second `DEDUPLICATED` or `REJECT_DUPLICATE_CAPABILITY` |
| Spawn while requester at epoch cap | `REJECT_SPAWN_DEPTH`; requester gets `REQUEST_DECLINED` |
| Unsupported capability | `REJECT_UNSUPPORTED`; nothing spawned |
| Needs judgment | `SPAWN_ESCALATED` + a row in `var/run/inbox.jsonl`; next run applies `decisions.jsonl` |
| Injection into a resident kernel | new agent at tick N>0 in the **same object**; `id(kernel)` and monotone `tick` asserted |

## 5. Tests to add

`tests/test_spawn.py` (unit): schema completeness; fingerprint stability;
`to_task_spec` wires `required_inputs → consumes → deps`; malformed → `REJECT_MALFORMED`;
catalog → `REJECT_UNSUPPORTED`; `RequestState` legality; ledger dedup; `metrics()` key set +
arithmetic (`active ≤ budget`, the §2.4 identity, `reuses + approvals == resolved`).

`tests/test_phase2_e2e.py`: the 12 spec cases, each asserting **journal rows**, not return values,
plus `test_spawn_happens_midrun_same_kernel`, `test_no_restart_between_spawn_and_completion`,
`test_replay_rebuilds_dynamic_org`, `test_wake_ready_admits_new_work_same_tick`,
`test_d1_spawn_cycle_is_rejected_and_rolled_back`, `test_d2_sleeping_actions_break_fires`,
`test_no_none_owner_after_deferred_spawn`, `test_from_journal_rederives_edges`.

`arena/chaos/chaos_kernel.py`: 8 new scenarios (`midrun-spawn`, `spawn-cap-drift`,
`spawn-cycle-rejected`, `spawn-reuse-not-spawn`, `spawn-dedup`, `spawn-unsupported`,
`spawn-crash-after-approval`, `resident-injection`) so `chaos-report` covers Phase 2; and update the
6 rule-string sites in §0.2 to the canonical names.

`arena/chaos/phase2_demo.py` + `arena accept` → `var/phase2_acceptance.md`.

## 6. Acceptance demo (deterministic, not keyword-planning)

Initial org fixed by the demo, **not** derived from the prompt: `database_01`, `backend_01`,
`frontend_01`. `backend_01` runs a `PaymentComplexityPolicy` that after 2 working steps calls
`ctx.request_specialist(role="payment-specialist", required_inputs=["contracts/api.json"],
expected_outputs=["backend/payments/webhooks.py"], est_work=3.0)`.

Driven by `k.run(1)` in a loop on **one kernel object**, capturing per tick, with the required
transition table (`tick / requests / approvals / rejections / active agents / states`) printed at
every tick, showing e.g.

```
tick 4  agents=3  backend=WORKING      request=RECEIVED             ← run() still alive
tick 5  agents=4  backend=WORKING      request=APPROVED_COMMITTED   payment_01=CREATED
tick 6  agents=4  payment_01=WORKING   backend=WORKING              ← others kept working
tick 7  agents=4  payment_01=COMPLETED backend=WORKING              ← artifact published
tick 8  agents=4  backend=WORKING → t_api_impl unblocked           ← dependent work admitted
```

Assertions that make it *runtime* and not a story: `id(k)` constant across the whole table; `k.tick`
strictly monotone and never reset; `len(active)` never drops below 3 after tick 5; the artifact
`backend/payments/webhooks.py` has producer `payment_01` in the journal; the event order
`SPAWN_REQUEST_RECEIVED → SPAWN_AGENT_REQUEST → GRAPH_AMENDED → AGENT_REGISTERED → TASK_ASSIGNED →
TASK_PROGRESS → RESOURCE_UPDATED → DEPENDENCY_READY → TASK_COMPLETED` holds by `seq`; and
`from_journal` reproduces all of it.

## 7. Non-goals for Phase 2

No real LLM. No ZeroMQ (Ph.3). No git branches/file locks (Ph.5). No dashboard (Ph.7). Agents still
publish artifact *names*, not built software. I will not call a policy-issued request intelligent —
what Phase 2 delivers is that the **runtime** admits, evaluates, validates, mutates, spawns, wakes
and replays correctly, with the cognition source swappable.

## 8. What actually shipped, and where the design was wrong

Status: implemented, 176 tests green, 31/31 chaos scenarios green, acceptance demo PASSED on a
fresh root and reproducible (two independent runs differ only in the scratch path printed in the
report; per-agent state tables are identical).

### 8.1 Defects the implementation found (all in the engine, not in the tests)

| # | Defect | How it was found | Fix |
|---|---|---|---|
| P2-1 | `TASK_ASSIGNED`/`GRAPH_AMENDED` were journalled with `task_id=` as an event **column**, so every reader that keys off the payload (`arena spawns -v`, the ledger replay projection) saw `None`. The reuse path recorded "we handed task X to agent Y" with no X. | `test_06`, `test_11` | `_reroute`/`commit_spawn` now also write `rerouted_task_id` / `approved_task_id` into the payload; `fold()` prefers the payload copy and falls back to the column. Same class of bug as the earlier `deferred_task_id`. |
| P2-2 | `AGENT_REGISTERED` was emitted **after** `make_actor()`, whose `CREATED → INITIALIZING → IDLE` transitions therefore preceded the registration row. `fold()` builds an agent's record at its first row and files the earlier transitions nowhere → the live org showed the new agent `IDLE`, every replay showed it `CREATED`, and a resumed kernel **never ran the work it had just spawned an agent for** (task stuck `assigned`, agent `CREATED`, `open_graph_tasks` forever). | new chaos scenario `spawn-crash-after-approval`, which asserts live-vs-replayed per-agent state | `emit_registered` moved before `make_actor` in `_spawn`; the scenario now compares every agent's state across the crash boundary, so the class of bug is pinned, not just this instance. |
| P2-3 | `ESCALATE` was a dead end: the ledger said `ESCALATED`, nothing ever re-read it, so "escalate to a higher-level decision" meant "drop the request politely". | `test_12` (I wrote the rule into the design; the test made me check it was wired) | `parent.escalated` holds the rid; `honour_pending_escalations()` runs at the top of every tick via `apply_arena_decisions()`, which now accepts `{"kind":"approve_spawn","rid":...,"reason":...}`. The rid survives, so receipt → escalation → answer → mutation is one traceable chain rather than a new request. |
| P2-4 | `SpawnRequest.normalise()` raised `UnboundLocalError` for a request with skills but no expected outputs (`slug` was defined inside another branch). | `test_spawn` | hoisted, and the shared-local pattern is now commented against. |
| P2-5 | The test fixture replaced `kernel.graph` with a fresh `DependencyGraph`, orphaning `registry.graph`/`bus` references to the old object — four tests failed for a reason that had nothing to do with the engine. | `test_spawn` 4 failures | `_reset(g)` clears the graph **in place**. Recorded here because it is the kind of bug that would have been "fixed" by weakening an assertion. |

### 8.2 Deviations from §2 (design → implementation)

- **Reuse beats spawn by rule order, not by scoring.** §2 sketched a weighted score; the shipped
  `evaluate_spawn` is a strictly ordered read-only veto chain
  (`DEPTH → CAP → churn → UNSUPPORTED → DUPLICATE → NOT_WORTH_IT → REQUESTER_OVERLOADED → ESCALATE`)
  with the *first* firing veto deciding. Ordering is auditable; a score is a story. `reason` in the
  journal is the veto's own detail string, so "why did you not spawn" has one answer per request.
- **Approve commits the graph before the agent exists.** `commit_spawn` amends + validates first; a
  `CycleError` rolls the amendment back and creates **no** agent (spec §3 "cycle → reject +
  rollback"). `_spawn` returning `None` (capacity closed in between) leaves the task legitimately
  unowned and journals `DEFERRED_FOR_CAPACITY` — never `owner="None"`.
- **Replay reads `var/inject.jsonl`, torn tail tolerated**, so "another process fed this kernel work"
  needs no port, no lock, no signal handler (spec §6/§7).
- **`kernel_config.json`** is written next to the journal at every run boundary. A replay that
  reconstructs state but not *configuration* binds a different policy class and silently diverges —
  this cost six debug rounds before I stopped comparing snapshots and started diffing the actors.

### 8.3 Where the ≥12 named adversarial tests live

`tests/test_phase2_e2e.py` numbers its tests `test_01 … test_21` to map 1:1 onto the twelve
requirements of the brief (mid-run request, dedup, same-tick race, depth ceiling, cycle gate — twice,
at the parent and at the graph — reuse, deleted upstream, deadlock broken by the gate, crash after
approval, unsupported, artifact conflict, escalation, the acceptance demo + its determinism), plus
monitoring, cross-process injection, torn tail, `polls == 0` under a spawn storm, causal-depth
bound, chain integrity, and one test that re-asserts the Phase-1 invariants *because* Phase 2
mutates the graph. Each asserts on **journal rows**, not on in-memory state.

`tests/test_spawn.py` (31) covers the units: schema, normalise-vs-reject split, fingerprint
stability, stable task ids, catalog, ledger dedup and terminal states, "every decision has a rule",
"the agent FSM was not touched by Phase 2", depth, and the monitoring arithmetic.

The nine new chaos scenarios (`midrun-spawn`, `spawn-cap-race`, `spawn-cycle-rejected`,
`spawn-reuse-not-spawn`, `spawn-dedup`, `spawn-unsupported`, `spawn-crash-after-approval`,
`resident-injection`, `spawn-depth-ceiling`) are the same adversarial surface with `guards` metadata
in `var/chaos_report.md`, because the report is what a reviewer reads.

### 8.4 Honest limit

`NeedsSpecialist` is a policy that emits a well-formed request after N steps of work on a task whose
capability class is missing from the registry. Phase 2 makes the **runtime** correct: admit,
evaluate, validate, mutate, spawn, wake, replay. Nothing here makes a request *smart* — the cortex
that would generate them is §7 of `FEASIBILITY.md` and is still not built.
