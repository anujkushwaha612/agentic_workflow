# RUST_MIGRATION_PLAN.md

**Scope:** full rewrite of the Agentic OS / `arena-code` runtime (Python) into idiomatic Rust,
preserving externally observable behavior. The Python implementation is the behavioral
specification until the Rust parity suite proves equivalence.

**Status of this document:** Step 0 (audit) + Step 1 (architecture proposal) only.
No Rust implementation has been started, per instruction.

---

## 0. Audit baseline (measured in this sandbox, 2026-09-01)

| Check | Result |
|---|---|
| `python3 -m pytest tests -q` | **207 passed** (16 bus, 34 chaos-as-tests, 30 code_m2, 14 graph, 12 journal, 24 kernel, 10 lifecycle, 23 phase2_e2e, 13 registry, 31 spawn) |
| `python3 -m arena.cli chaos-report` | **31/31 scenarios passed** |
| `./arena-code "Build a small, tested Python CLI calculator…" --workspace /tmp/golden-ws` | **exit 0 (VERIFIED)**; 15 subprocess executions, 13 commands, 10 test runs, 7 file mutations, 0 refusals/violations/timeouts |
| `./arena-code verify --workspace … --project …` (fresh process) | **exit 0**; chain VALID, replay parity OK, 5 verify commands re-run, all pass |
| Code size | 13,336 lines Python across 39 files (26 runtime modules, 11 test modules, 2 launchers) |

### Defects / environment gaps found during the audit (fixes already applied)

1. **`arena/code/cli.py:168` — pre-existing SyntaxError.** A dict-comprehension nested inside a
   multi-line f-string expression. That is a Python 3.12+ (PEP 701) construct; the documented
   runtime is Python ≥ 3.11 (sandbox has 3.11.2), so `tests/test_code_m2.py` could not even be
   **collected**. Fixed with a behavior-preserving hoist (identical printed bytes). The Rust port
   must not "port" this bug — the equivalent `inspect --tools` output must be correct.
2. **`httpx` missing** in this sandbox → 3 doctor tests failed. `httpx` is imported *unguarded* by
   `arena/code/doctor.py:248` even though the adapter "uses stdlib http.client too". Installed to
   establish the baseline. Rust note: the provider-sdk check must degrade gracefully — absence of
   an HTTP client must be a report line, never a crash.

### Environment constraint that blocks implementation (must be resolved first)

| Host | Reachable? | Consequence |
|---|---|---|
| `github.com`, `api.github.com` | ✅ | `git clone` works ⇒ **cargo git dependencies are viable** |
| `static.rust-lang.org` | ❌ | **rustup / Rust toolchain cannot be downloaded** |
| `crates.io`, `index.crates.io`, `static.crates.io` | ❌ | **cargo registry dependencies cannot be downloaded** (git deps still work) |
| Debian apt mirrors | ❌ | no `apt install rustc` |

There is **no Rust toolchain in the sandbox** and no apt/rustup route. `gcc` 12.2 is present, so
`rusqlite --features bundled` can compile once a toolchain exists. **Options, in order of
preference:**

- **A. Platform preinstalls Rust** (rustc + cargo) in the sandbox image. Best.
- **B. Egress allowlist** for `static.rust-lang.org` + `index.crates.io`/`static.crates.io`.
- **C. Vendored delivery**: a toolchain tarball placed in the workspace (e.g. by the user), plus
   `cargo vendor`-ed crate sources. Git dependencies from github.com are a workable fallback for
   crates, but *not* for the toolchain itself.

The milestone plan below assumes one of A–C is resolved at "Milestone −1".

---

## 1. Complete module inventory and Rust mapping

```
Python module                     LOC  Rust module                        Kind
--------------------------------------------------------------------------------------
arena/__init__.py                  17  src/lib.rs                          crate root, RUNTIME_VERSION
arena/clock.py                     71  src/clock.rs                        Clock, Virtual, Wall
arena/message.py                  281  src/msg.rs                          Plane, EventType(61), Message, topics, causality caps
arena/lifecycle.py                126  src/lifecycle.rs                    AgentState FSM (closed table), Lifecycle
arena/journal.py                  476  src/journal/                        Journal (SQLite WAL), hash chain, fold, claims, waits
                                        src/journal/schema.rs               table DDL, canonical JSON, py-float formatter
                                        src/journal/fold.rs                 event → projection fold
arena/graph.py                    357  src/graph.rs                        TaskSpec, DependencyGraph, cycles, amend/rollback, wait-for graph
arena/registry.py                 243  src/registry.rs                     AgentRecord, AgentRegistry, SpawnBudget, cover()
arena/bus.py                      239  src/bus.rs                          routing, subscriptions, durable waits, PollCounter
arena/policy.py                   338  src/policy/mod.rs                   Act/Action, SLEEPING_ACTIONS
                                        src/policy/builtins.rs              6 built-in policies
arena/cognition.py                316  src/cognition/mod.rs                Control, Observation, Intent, validate_intent
                                        src/cognition/policy_adapter.rs     PolicyCognition
arena/actor.py                    570  src/actor.rs                        AgentActor, ActorContext, run_step, verify gate
arena/spawn.py                    434  src/spawn.rs                        SpawnRequest, RequestState FSM, catalog, ledger
arena/parent.py                  1150  src/parent/mod.rs                   ParentArena: intake→evaluate→commit, schedule, cortex
                                        src/parent/planner.rs               RuleBasedPlanner
arena/kernel.py                  1140  src/kernel/mod.rs                   Kernel composition root, run(), run_resident()
                                        src/kernel/recovery.rs              from_journal() (pure projection)
                                        src/kernel/metrics.rs               metrics/monitoring/state_counts
arena/tools.py                    873  src/tools/mod.rs                    ToolSpec registry (13 tools), Executor
                                        src/tools/jail.rs                   Jail + path resolution + REFUSE_* codes
                                        src/tools/redact.rs                 secret redaction, digests
                                        src/tools/proc.rs                   argv exec, timeout, process-group kill, diff, spill logs
arena/cli.py                      428  src/bin/arena.rs + src/cli/arena.rs `arena` CLI (19 subcommands)
arena/chaos/chaos_kernel.py      1069  tests/adversarial/*.rs (suite) + src/cli parity (`chaos-report`, `chaos-run`)
arena/chaos/phase2_demo.py        438  src/acceptance.rs (+ `arena accept`) Phase-2 acceptance demo
arena/code/__init__.py             16  src/product/mod.rs                  RUNTIME_VERSION (engine↔product split preserved)
arena/code/project.py             199  src/product/project.rs              projects/<id>/ layout, manifest, PROJECT_CREATED
arena/code/orchestrate.py         274  src/product/orchestrate.rs          organise(): plan→bind→run→verify→commit
arena/code/agent.py               382  src/product/coding_agent.rs         CodingAgent (evidence-driven decision loop)
arena/code/cognition.py           240  src/product/cognition.rs            ScriptedCoding, ReflectiveCoding
arena/code/plans/calc.py          241  src/product/plans/calc.rs           packaged acceptance plan (as Rust data)
arena/code/cli.py                 377  src/bin/arena-code.rs + src/cli/code.rs  arena-code CLI + exit-code contract
arena/code/doctor.py              295  src/product/doctor.rs + src/config.rs  doctor, config precedence, secrets hygiene
arena-code (shim)                  20  (not needed — cargo installs the binary)
tests/*.py                      2,742  tests/*.rs (see §9)
```

**Engine/product boundary is preserved structurally:** `src/kernel|msg|journal|graph|registry|bus|
actor|spawn|parent|policy|cognition|tools|clock` (the engine, no knowledge of projects/prompts) and
`src/product/*` (launcher layer). Nothing in the engine modules may import from `src/product`.
This mirrors the Python rule "nothing in `arena/` imports from `arena/code/`" and is enforced with a
workspace lint (see §7.10).

### 1.1 Every Python class/function of note (condensed; full enumeration lives in the source)

- **clock.py** — `Clock` (ABC: `now/tick/advance`), `WallClock`, `VirtualClock(step, now()==ticks*step)`,
  `make_clock(mode, step)`.
- **message.py** — `Plane{CONTROL,DEPENDENCY,RESOURCE}`; `MessageType` (**61 variants**, §5);
  `TERMINAL_TYPES` (18); `TYPE_PLANE`; `CATEGORY` (14 categories); `topic_for()` →
  `plane.category.type` with category prefix stripped once; `Message` (15 fields, `mid=m-<hex12>`,
  `correlation_id=c-<hex12>` auto-derived, `matches(glob)`, `to_dict()`, `child()` = the only
  sanctioned causal reply); `MAX_CAUSAL_DEPTH=3`; `MSG_BUDGET_PER_TICK=8`.
- **lifecycle.py** — `AgentState` (10 states); `SLEEPING`; `ADMITTABLE`; `TRANSITIONS` (closed
  table); `can_transition`; `assert_transition` → `IllegalTransition`; `TransitionRecord{frm,to,
  ok,reason}`; `Lifecycle{state,since,history}` with `request(to, reason, *, now)`.
- **journal.py** — schema below; `_hash_row(prev, row)`; `append(msg, extra)` (canonical JSON
  payload, body hoisted into payload); `emit(...)`; `events(actor/task_id/etype/correlation_id/
  after_seq/limit)`; `count`; `trace`; `_rowdict` (body hoisted out of payload); `latest_snapshot`;
  `_load_last_hash`; `verify_chain() -> (ok, why, seq)`; `truncate_after(seq)`; durable waits
  (`arm_wait/resolve_wait/active_waits`); claims (`claim()` = INSERT-or-IntegrityError
  first-writer-wins with `losers` list, `claims()`); `fold()` (the projection, §6.3); `iterate()`.
- **graph.py** — `TaskSpec` (17 fields incl. `verify: list[list[str]]`, `key` property, `is_open`);
  `OPEN=("pending","assigned","running","waiting")`; `CycleError(cycle)`; `DependencyGraph`:
  `add/remove/derive_edges/add_edge/validate/order (generations)/predecessors/successors/
  blocked_by/ready(require_owner)/is_ready/unmet_artifact_producers/unmet/remaining_work(_for_role)/
  all_artifacts/snapshot/has_cycle/amend (full rollback)/wait_for_edges/find_cycles (DFS, self-edges
  ignored)/critical_path_length`; `artifact_key`.
- **registry.py** — `AgentRecord` (15 fields incl. `task_queue`, `cognition`); `SpawnBudget`
  (max_active_agents=8, max_concurrent_workers=2, max_spawn_epoch=3,
  min_share_of_remaining=0.10, requester_overload_factor=2.0, idle_ttl=5.0); `AgentRegistry`:
  `next_id(role)` → `<slug_NN>`, `register/terminate/active/with_state/_tokens/cover/overloaded/
  idle_overdue/working_count/snapshot/status_lines/wait_for_edges/sync_from`; `emit_registered`,
  `emit_terminated`.
- **bus.py** — `PollCounter` (+`PollingForbidden`); `Delivery{recipients,dropped_depth,
  dropped_budget,not_subscribed}`; `Bus`: default agent patterns `("control.task.*",
  "control.agent.*", "dependency.*")`, parent patterns `("control.*","dependency.*","resource.*")`,
  kernel `"*"`; `subscribe/unsubscribe/patterns_for`; `publish` (depth cap → BUDGET_EXCEEDED,
  self-broadcast skip, resource-plane subscription gating, sender-side budget); `wait_for` /
  `resolve(condition)` / `expire_timeouts()`; `snapshot`.
- **policy.py** — `Act{PROCEED,PUBLISH,WAIT,COMPLETE,ESCALATE,NOOP}`; `SLEEPING_ACTIONS={WAIT,
  COMPLETE,ESCALATE,ERROR}` (+case-insensitive `is_sleeping_action`); `Action` + constructors;
  `Policy` protocol; `NullLLM`; built-ins: `SimulatedWork`, `WaitForArtifacts`, `PollUntilReady`
  (deliberate anti-pattern), `EscalateOnComplexity`, `HybridPolicy`, `NeedsSpecialist`
  (incl. `detect_from`); `BUILTIN_POLICIES`; `make_policy` (per-role `_ignored` kwargs absorb).
- **cognition.py** — `Control` verbs `{WAIT,PUBLISH,SPAWN_REQUEST,ESCALATE,COMPLETE,VERIFY,NOOP}`;
  `YIELDING`; `Outcome`; `Observation` (19 fields incl. `history`, `transcript_len`, `prompt_seed()`,
  `render()`); `ToolCall`; `Intent{calls≤4,…,fingerprint}`; `CognitionSource` protocol;
  `PolicyCognition` (adapter, `fingerprint()` with `obj_id` + `prompt_sha256_16`);
  `CognitionCapabilityError`; `outcomes_from_results`; `validate_intent` (never raises; refusal
  codes `INTENT_MISSING`, `INTENT_NOT_AN_INTENT:*`, `CONTROL_UNKNOWN:*`, `TOOL_NOT_ALLOWED:*`,
  `CALLS_TRUNCATED:n>m`).
- **actor.py** — `ActorContext` (read surface + `self_msg`/`request_specialist`/`log`);
  `AgentActor`: `cognition_source()`, `policy_cursor()`, `rec_progress()`, `restore_cursor()`,
  `run_step()` (inbox drain → policy **or** cognition → `_apply_action`), `observe()`,
  `_run_cognition_step()`, `_intent_to_action()`, `_record_turn()` (transcript jsonl),
  `_complete()` (verify gate + backlog advance + repeated-COMPLETE guard), `has_mail`,
  `runnable()`, `run(max_steps)`, `snapshot`.
- **spawn.py** — `RequestState` (8 states) + `REQUEST_TRANSITIONS` (DEFERRED is the only re-entry);
  `TERMINAL_REQUEST_STATES`; `RULES` (11); `REQUIRED_FIELDS` (6); `MalformedRequest`;
  `CapabilityCatalog` (9 serviceable classes, 4 unserviceable with reasons, `class_for`);
  `SpawnRequest` (`from_message`, `validate`, `normalise` → inferred fields list, `fingerprint`
  (sha256 of class/role/skills/outputs, **not keyed on requester**), `task_id()` keyed on
  fingerprint (restart-stable), `to_task_spec` (required_inputs → consumes, claim
  `spawn:<fp8>`); `LedgerEntry`; `IllegalRequestTransition`; `SpawnLedger` (`open`, `move`
  (strict), `close_as`, `inflight_for`, `resolved_for`, `newest_first`, `counts`, `by_rule`,
  `rehydrate`).
- **parent.py** — `Rule` table + `RuleBasedPlanner.plan()` (keyword rules → TaskSpecs + curated
  consumes + rationale); `Decision{ok,rule,detail,owner}`; `ParentArena`: `submit`,
  `spawn_for_plan` (reuse epoch-0 else spawn, cap defers), `_spawn` (register → subscribe →
  AGENT_REGISTERED **before** make_actor), `assign` (claim first, backlog queue, COMPLETED →
  INITIALIZING re-queue edge), `LEGACY_RULE_NAMES`; `_as_request` (SpawnRequest | Message | legacy
  dict funnel); `intake_spawn_request` (record → normalise → class inference → ledger RECEIVED →
  journalled → in-flight/terminal dedup); `evaluate_spawn` (ordered vetoes, §6.7);
  `commit_spawn` (ESCALATE parks; rejections journaled + REQUEST_DECLINED; REUSE → `_reroute`;
  graph.amend gate → spawn → assign → SPAWN_APPROVED → API_CONTRACT_READY → wake_ready; DEFERRED
  on vanished slot; REJECT_CYCLE rollback); `decide_spawn`; `honour_pending_escalations`;
  `recount_spawn_stats`; `drain_spawn_requests`; `request_feature`; `resolve_amendments`
  (options A/B/C); `pause/resume/terminate_agent`; `schedule()`; `wake_sleeper`; `reap_idle`;
  `detect_deadlock/resolve_deadlock` (victim = cyc[-2]); cortex files (`write_inbox`,
  `apply_arena_decisions` — approve_spawn/amend/force_state/force_terminate, `parent_reject_or_spawn`,
  `amend`); `render`; `status`.
- **kernel.py** — `RUNNABLE`; `Kernel` (30+ fields); side-file anchoring (`_compute_side_anchor`:
  explicit root > journal dir, never CWD); `bind_actor` (replay-safe: no lifecycle writes);
  `bind_tools`; `ensure_git` (journalled TOOL_CALL/RESULT pair); `bind_cognition` (factory per
  role, fingerprint journaled); `cognition_report` (shared-brain detection); `completion_gate`
  (REJECT_UNVERIFIED); `run_verify`; `transition` (the ONLY sanctioned state change, journalled
  accepted **and** rejected); `make_actor`; `deliver`; `publish`; artifact helpers
  (`publish_artifact` with auto-close + wake + RESOURCE_UPDATED + bus.resolve); `requeue`;
  `_begin_if_assigned`; `submit`; `_eligible` (least-worked, worker cap); `_signature`; `run()`
  (tick loop order §6.9); `run_resident()` (single-threaded, injection drain, on_tick, park);
  `checkpoint` (SNAPSHOT + write_config); `metrics/monitoring/spawn_stats_view/state_counts`;
  `idle_predicate`; `inject_spawn_request`; `drain_injection_file`; `summary/report/why/trace/
  snapshot/replay`; `from_journal()` (§6.10 recovery contract); `_final_stats`.
- **tools.py** — `JailViolation`; `REDACTIONS` (4 regex classes) + `redact` (+extra secrets ≥6
  chars); `digest` (sha256[:16] of text); `file_digest`; `ToolResult` (`to_dict`,
  `as_block`, `refusal`); `Jail` (root, reads, writes, `deny_names=(".arena",".git")`, `resolve`
  — abs/`~` refuse, write-at-root refuse, normpath **and** realpath containment, protected parts,
  read/write allocation prefixes; `snapshot` (size,mtime) capped 4000); `ToolSpec` (risk ∈
  {read,write,exec,exec-network,vcs-commit}, timeout, idempotent, required, argv_only);
  `TOOLS` (13: list_files, read_file, write_file, edit_file, run_command, run_tests, inspect_git,
  commit, publish_artifact, send_message, wait_for_event, request_specialist); `EGRESS_TOOLS`;
  `NEVER_RUN`; `ToolRegistry`; `Executor`: `bind`, `jail_of`, `can` (grants + REFUSE_NO_GIT_REPO),
  `plan` (TOOL_CALL before effect, `args_digest`, `args_preview` redacted), `note_violation`,
  `execute` (→ TOOL_RESULT/TOOL_REFUSED, always a result), `_execute` (repeated-refusal memo,
  unknown tool, grant, missing args, handler dispatch, JailViolation → refusal), handlers
  (list_files prefix/root allocation semantics; read_file binary sniff + cap; write_file tmp+
  `os.replace` atomic + identical-bytes note; edit_file 0-match = **refusal** REFUSE_EDIT_TARGET_MISSING,
  >1-match ambiguity refusal, `all=true`; run_command argv-only, string-argv shlex recovery unless
  metacharacters, NEVER_RUN, egress gate, PATH lookup → exit 127, cwd jail, timeout clamp
  [0.05, 600], env `ARENA_AGENT` etc., changed-diff, spill log; run_tests = run_command with
  `tool="run_tests"`; inspect_git status/diff/log/files; commit porcelain-clean = ok:false noop,
  deterministic author/date env, `--no-verify`, head rev-parse; publish_artifact files-must-exist),
  `describe` (workspace view for Observation, git head/dirty), `note_refusal`.
- **cli.py (`arena`)** — 19 subcommands (§7.3), `DEFAULT_ROOT=var/run`, `_kernel()` resume-or-new.
- **chaos_kernel.py** — `Scenario{id,title,guards,fn}`; `_mk` (scratch dir, virtual clock,
  budget override); `_ev` (event filter); 31 `s_*` functions (§10); `run_scenarios`,
  `render_report`, `BY_ID`.
- **phase2_demo.py** — `run_demo` (continuous run, stage table, monotonicity checks, markdown).
- **code/project.py** — `project_id()` (`YYYYMMDD-HHMMSS-slug`); `Project` (paths: `.arena`,
  `source`, `artifacts`, `logs`, `j.db`, `inject.jsonl`, `decisions.jsonl`, `context/`);
  `create/load/discover`; manifest (project_id, prompt, created_at(_iso), runtime_version, state,
  history, workspace, layout, provenance, plan, plan_source, execution, kernel_root, journal);
  `write_manifest` (**two copies**: `.arena/project.json` + root); `set_state/update`;
  `_emit_created` (PROJECT_CREATED journaled); `status_row`.
- **code/orchestrate.py** — `DEFAULT_BUDGET` (idle_ttl=1e9, workers=3, agents ≥ 8/2·N+4);
  `load_plan` (validates: every agent has task.task_id + non-empty seed); `organise()` (create-or-
  resume → Kernel(root=arena_dir, journal in project) → graph add(derive=False)+derive+validate →
  per agent: register+emit, make_actor, bind_tools(source root, writes/reads, git_root=source),
  ensure_git, bind_cognition(CodingAgent), assign → run(ticks) → `_verify_all` (runtime re-runs
  verify via `run_tests` + checks produces exist w/ digests) → `_commit_all` (commit tool,
  `commit_after_verify`) → state verified/awaiting-cortex/completed → manifest execution stats →
  `RunResult` → optional `render_report` markdown).
- **code/agent.py** — `CodingAgent.decide()`: 0) WAIT only on genuinely-missing consumes;
  1) write missing seed files; 2) run_tests when nothing executed since last change; 3) on pass:
  publish ONE artifact with all owed files then COMPLETE; 4) on fail: specialist trigger →
  SPAWN_REQUEST; max_fixes → ESCALATE; last refused → re-read/escalate; just-read → apply paired
  `ARENA-BUG:`/`ARENA-FIX:` correction (indent-preserving, single-line replacement) or read the
  next implicated file (non-test candidates first, `tests/` last resort, `_referenced` follow-up);
  `_already_read`/`_read_of`/`_last_mutation_at` (evidence index — may only conclude from reads
  made after the last mutation of that path); `fix_files` grant enforcement (ESCALATE if outside).
- **code/cognition.py** — `ScriptedCoding` (plan replay); `ReflectiveCoding` (older reflective
  tier; kept as a cognition source).
- **code/cli.py** — §7.4 (exit codes 0/10/20/30/64; bare-argv → run; `-` → stdin; SystemExit(str)
  → 64).
- **code/doctor.py** — `Check/Report`; `OPTIONAL` set (7 checks that cannot fail the verdict);
  `runtime_checks` (runtime-version, python, engine-imports, tools-registered ≥10,
  subprocess-execution demo, git/pytest/node/npm/postgres probes, filesystem-writes probe,
  journal-sqlite); `credential_presence` (presence only); `config_path` (`ARENA_CODE_CONFIG` →
  `~/.config/arena-code/config.json`); `resolve_config` (env → file → flags-fill-gaps per field,
  provider downgrade to policy without key, `filled_by`/`shadowed` audit lists);
  `build_report` (+live egress probe, workspace/projects, secrets-hygiene line); `main`.
- **plans/calc.py** — SRC/CLI/TESTS/DOC source strings (2 seeded `ARENA-BUG:` defects with inline
  `ARENA-FIX:`), `plan()` (2 agents: coder_01 writes src+tests, docs_01 waits on
  `artifact:src/calc.py`, writes docs; verify commands; commit_after_verify).

---

## 2. Python dependencies

**Runtime: Python stdlib only** (`sqlite3`, `subprocess`, `pathlib`, `hashlib`, `re`, `shlex`,
`enum`, `dataclasses`, `graphlib`, `fnmatch`, `tempfile`, `shutil`, `contextlib`, `argparse`,
`json`, `time`, `uuid`, `signal`, `errno`, `os`). No third-party imports in `arena/` at runtime —
`httpx` appears only in doctor's informational check (and currently crashes if absent: §0).

**Test-time:** `pytest`, `httpx`.

The Rust equivalent therefore aims for a *minimal* dependency tree (§8), which matters given the
crates.io egress block.

---

## 3. Filesystem layouts (all must be preserved)

```
engine run dir (--root, default <repo>/var/run)      product workspace (--workspace, default .)
├── journal.db            (SQLite WAL)               └── projects/<project-id>/
├── kernel_config.json    (Kernel.config())              ├── project.json           (manifest copy)
├── events.jsonl          (trace)                        ├── .arena/
├── inject.jsonl          (external spawn injection)     │   ├── project.json       (manifest)
├── decisions.jsonl       (cortex → kernel decisions)    │   ├── j.db               (SQLite WAL journal)
├── inbox.jsonl           (escalations out)              │   ├── kernel_config.json
└── chaos_report.md/.json (chaos-report)                 │   ├── events.jsonl / inject.jsonl / decisions.jsonl
                                                           │   ├── context/<agent>.jsonl   (transcripts)
                                                           │   └── tool-log/
                                                           ├── source/               (agent worktree, git init)
                                                           │   └── … project files …
                                                           ├── artifacts/
                                                           └── logs/<agent>-<seq>-<prog>.log  (command spill)
```

**Anchoring rules (tested behavior, must not regress):** relative side-files are anchored to the
explicit `root` if set, else to the journal's directory; a kernel with an in-memory journal and
default root writes **nothing** to the CWD. `.arena` and `.git` are protected jail path names —
agents can never write into their own evidence.

---

## 4. Configuration & environment surface

| Variable / file | Used by | Meaning |
|---|---|---|
| `ARENA_CODE_WORKSPACE` | arena-code | default `--workspace` |
| `ARENA_CODE_CONFIG` | config | config file path override |
| `ARENA_CODE_MODEL_PROVIDER`, `ARENA_CODE_MODEL`, `ARENA_CODE_BASE_URL` | config | provider tier |
| `ARENA_CODE_ALLOW_EGRESS` | config | bool for executor egress |
| `ARENA_CODE_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` | config | secrets — **presence-only reporting, never logged, redacted in tool output** |
| `HOME` | config | `~/.config/arena-code/config.json` (0600 on write) |
| `ARENA_AGENT`, `PS1`, `GIT_PAGER`, `PAGER`, `npm_config_fund`, `npm_config_audit` | executor | injected env for child processes |
| `GIT_AUTHOR_*`, `GIT_COMMITTER_*` (date `@0 +0000`) | commit tool | deterministic commits |
| `kernel_config.json` | Kernel | role policies, budget, flags, task_text (reload on `from_journal`) |
| `project.json` manifest | product | §1 project.py |

**Precedence contract (tested):** env → file → CLI flags, *per field*: flags only fill gaps and are
recorded as `filled_by`; values already set are recorded as `shadowed`. A provider without a key is
downgraded to `policy`. Rust keeps the same contract with a typed `Config` struct and a
`Secret(String)` wrapper whose `Display`/`Debug`/serde impls are redacted.

---

## 5. Journal event types (61) and planes

`MessageType`: control — TASK_ASSIGNED, TASK_STARTED, TASK_PROGRESS, TASK_COMPLETED, TASK_FAILED,
STATUS_UPDATE, AGENT_REGISTERED, AGENT_TERMINATED, AGENT_PAUSED, AGENT_RESUMED, STATE_TRANSITION,
PLAN_CREATED, PLAN_AMENDED, REPLAN, DUPLICATE_CLAIM, ILLEGAL_TRANSITION, CYCLE_REJECTED,
DEADLOCK_DETECTED, BUDGET_EXCEEDED, RUN_TICK, CRASH_SIMULATED, REPLAY_COMPLETE, STATS, SNAPSHOT,
BLOCKED, ERROR_REPORT, HELP_REQUEST, SPAWN_REQUEST_RECEIVED, SPAWN_REQUEST_RESOLVED,
REQUEST_REROUTED, SPAWN_ESCALATED, DEFERRED_FOR_CAPACITY, GRAPH_AMENDED, GRAPH_AMEND_REJECTED,
TOOL_CALL, TOOL_RESULT, TOOL_REFUSED, COGNITION_VIOLATION, COMPLETION_REFUSED, TASK_VERIFIED,
WORKSPACE_BOUND, PROJECT_CREATED, COGNITION_BOUND, COGNITION_ERROR;
dependency — DEPENDENCY_REQUEST, DEPENDENCY_READY, DEPENDENCY_BLOCKED, WAIT_REGISTERED,
WAIT_RESOLVED, WAIT_TIMEOUT, API_CONTRACT_READY, FEATURE_REQUEST, REQUEST_ACK, REQUEST_DECLINED,
SPAWN_AGENT_REQUEST; resource — RESOURCE_UPDATED, FILE_LOCKED, FILE_RELEASED, ARTIFACT_PUBLISHED.

Rust: `enum EventType` with **61 variants**, `FromStr`/`Display` (SCREAMING_SNAKE), plane+category
lookup tables as `const` arrays, unknown-string handling for reading legacy rows (`EventType` keeps
an exhaustive match; the DB never stores unknown values because writes come from the enum).

**SQLite schema (must stay byte-compatible so Rust can verify/replay Python journals):**

```sql
events(seq INTEGER PK AUTOINCREMENT, ts REAL, etype TEXT, plane TEXT, topic TEXT, actor TEXT,
       target TEXT, task_id TEXT, resource TEXT, correlation_id TEXT, caused_by TEXT,
       depth INTEGER, payload TEXT, hash TEXT, prev_hash TEXT)
  + indexes ev_task, ev_corr, ev_type, ev_act
claims(claim_key TEXT PK, first_owner TEXT, task_id TEXT, ts REAL, losers TEXT '[]')
waits(wait_id TEXT PK, agent_id TEXT, condition TEXT, task_id TEXT, correlation_id TEXT,
      armed_at REAL, timeout_at REAL, state TEXT 'WAITING', wake_reason TEXT)
  + indexes wa_cond, wa_agent
PRAGMAs: journal_mode=WAL, synchronous=FULL, busy_timeout=5000
```

**Hash chain:** `sha256(prev_hash_bytes ‖ canonical_json(row))` where `row` is the 12-tuple
`(ts, etype, plane, topic, actor, target, task_id, resource, correlation_id, caused_by, depth,
payload)` and canonical JSON is Python's `json.dumps(row, sort_keys=True,
separators=(",",":"))`. GENESIS = `"0" * 64`. `verify_chain()` recomputes the chain and localizes
the first bad seq. **Parity hazard:** Python float repr (`1e-07`) differs from Rust/ryu (`1e-7`)
in exponent form, and Python renders integral floats as `0.0`. The Rust journal implements a
`py_repr_f64` formatter used by the canonical serializer, and a parity test re-verifies a journal
produced by the Python reference. Payload bodies are sorted-key JSON objects; `body` lives inside
the payload (hoisted on read).

---

## 6. State machines, invariants, and hidden assumptions

### 6.1 Agent lifecycle FSM (closed)
CREATED → {INITIALIZING, TERMINATED}; INITIALIZING → {IDLE, WORKING, TERMINATED}; IDLE → {WORKING,
WAITING_FOR_DEPENDENCY, BLOCKED, ESCALATED, PAUSED, TERMINATED}; WORKING → {IDLE,
WAITING_FOR_DEPENDENCY, BLOCKED, ESCALATED, PAUSED, COMPLETED, TERMINATED};
WAITING_FOR_DEPENDENCY → {WORKING, IDLE, BLOCKED, ESCALATED, TERMINATED}; BLOCKED → {WORKING, IDLE,
ESCALATED, TERMINATED}; ESCALATED → {WORKING, IDLE, WAITING_FOR_DEPENDENCY, BLOCKED, TERMINATED};
PAUSED → {IDLE, WORKING, WAITING_FOR_DEPENDENCY, TERMINATED}; COMPLETED → {TERMINATED,
INITIALIZING} (drain state — re-open only through the explicit re-init edge); TERMINATED → ∅.
Rejected edges **never mutate** and are journalled (`ILLEGAL_TRANSITION`). `Kernel::transition` is
the only sanctioned mutation path; nothing else may write `lifecycle.state` (replay sets it
directly, as a projection, which is the documented exception).

### 6.2 Spawn-request FSM
RECEIVED → EVALUATING → {APPROVED_COMMITTED, REROUTED, REJECTED, DEDUPLICATED, ESCALATED,
DEFERRED}; DEFERRED → {EVALUATING, APPROVED_COMMITTED, REROUTED, REJECTED} (only re-entry).
Terminal: APPROVED_COMMITTED, REROUTED, REJECTED, DEDUPLICATED, ESCALATED. Rules (11): APPROVE,
REJECT_NOT_WORTH_IT, REJECT_CAP, REJECT_DUPLICATE_CAPABILITY, REJECT_SPAWN_DEPTH,
REJECT_UNSUPPORTED, REJECT_MALFORMED, REJECT_CYCLE, ESCALATE, DEDUPLICATE, DEFER_FOR_CAPACITY
(+ REJECT_REQUESTER_OVERLOADED and REJECT_INTAKE appear in decisions/journal but not RULES).
Legacy alias map (APPROVED→APPROVE, MAX_ACTIVE_AGENTS/SPAWN_CHURN→REJECT_CAP,
COVERED_BY_EXISTING→REJECT_DUPLICATE_CAPABILITY, NOT_WORTH_IT→REJECT_NOT_WORTH_IT,
MAX_SPAWN_EPOCH→REJECT_SPAWN_DEPTH) must be honored when reading old journals.

### 6.3 Task status lifecycle
pending → assigned → running → done|failed|waiting (waiting = verify-refused, returns to running).
OPEN = {pending, assigned, running, waiting}. Tasks auto-close when every declared produce is
published (`TASK_COMPLETED` with `auto=true`).

### 6.4 Ordering invariants (the "hidden assumptions" the chaos suite pins)
1. `AGENT_REGISTERED` is journalled **before** the actor exists (fold files any row that precedes
   the registration nowhere).
2. `WORKSPACE_BOUND` after `AGENT_REGISTERED`.
3. Assignment is not a lifecycle change; WORKING/INITIALIZING edges arrive as their own
   STATE_TRANSITION rows.
4. `TASK_STARTED` is emitted exactly once per task (`started` flag), by the kernel.
5. Tool intent (`TOOL_CALL`) is journalled **before** execution; outcome after
   (TOOL_RESULT | TOOL_REFUSED).
6. A refusal is an event, never an absence.
7. Replay must be a **pure projection**: `from_journal(quiet)` writes nothing; `bind_actor` does
   not walk CREATED→IDLE.
8. `from_journal` restores, in order: config → agents (state/since/task/backlog) → tasks
   (derive=False then re-derive edges) → artifacts → request ledger (+`recount_spawn_stats`) →
   pending waits → WAITING_FOR_DEPENDENCY pin → snapshot tick → **then** per-actor cursor restore
   (snapshot preferred over fold).
9. `checkpoint` writes SNAPSHOT + `kernel_config.json` at run boundaries; snapshots are caches of
   projections, never a second source of truth.
10. Completing twice with no bound task is refused in the actor (`last_action == COMPLETE` guard)
    — journal noise is a correctness bug.
11. The inbox-drain loop breaks on SLEEPING_ACTIONS (case-insensitive compare — Python defect D2).
12. A COMPLETED agent with a backlog is runnable (finished a task ≠ finished being useful).
13. `list_files` with no prefix walks the agent's **read allocation** (never the raw root);
    write-at-root is refused.
14. An agent may only reason from a read made **after** the last mutation of that path.
15. `edit_file` with 0 matches is a **refusal** (`REFUSE_EDIT_TARGET_MISSING`), not `ok=false`;
    an identical re-proposal is `REFUSE_REPEATED_REFUSAL` without re-execution.
16. WAIT only on conditions the runtime agrees are missing (compare `consumes` vs `consumed_ready`).
17. One `publish_artifact` carries **all** owed files (per-file publishing would auto-close the
    task mid-publication).
18. Empty verification ≠ passed verification (`verify` requires tasks **and** re-run commands).
19. Secrets: presence-only in doctor/config; redaction in every journal/transcript/log path.
20. Side-file anchoring: never the CWD (§3).

### 6.5 Bus delivery invariants
Topic = `plane.category.type` (category prefix stripped once: `DEPENDENCY_READY` →
`dependency.wait.resolved`… see `topic_for`). fnmatch-case globs. Parent is the only full-visibility
subscriber (`control.*`, `dependency.*`, `resource.*`); kernel `*`; agents default
`control.task.*`, `control.agent.*`, `dependency.*` + explicit subscriptions. Resource-plane
messages are **never** delivered to unsubscribed agents. Self-broadcast delivery is skipped for
agents but not for parent/kernel/dependency_manager. `MAX_CAUSAL_DEPTH=3` enforced at publish →
`BUDGET_EXCEEDED` journaled drop. Per-tick send budget 8 is the **sender's** allowance.

### 6.6 Determinism
Virtual clock (`now = ticks × step`) + pure policies ⇒ the chaos suite and replay-parity checks are
byte-stable. Registry id generation `<slug>_<NN>`; sorted iterations everywhere order matters
(fold sorts deps/skills; `order()` sorts each generation; ledger newest-first by rid).

### 6.7 Spawn decision funnel (ordering IS the safety property)
intake (record/dedup; **no mutation**) → evaluate (pure read; veto order: SPAWN_DEPTH → CAP →
churn (registrations > 2×cap) → UNSUPPORTED → DUPLICATE_CAPABILITY (cover()) → NOT_WORTH_IT
(est < 10% of remaining) → REQUESTER_OVERLOADED → requires_judgment ESCALATE → APPROVE) →
commit (ESCALATE parks; reject+notify; REUSE reroutes task to existing agent; **graph.amend gate
first** — cycle ⇒ full rollback + REJECT_CYCLE, no agent; then spawn (epoch = requester+1); assign;
ledger APPROVED_COMMITTED; `API_CONTRACT_READY` to requester; `wake_ready`). If the slot vanishes
between evaluate and commit → task stays in graph unowned, request DEFERRED (recoverable). Spawned
task id is fingerprint-keyed (restart-stable), claim `spawn:<fp8>`.

### 6.8 Workspace/tool security invariants
Jail: absolute/`~` paths refused; write-at-root refused; containment checked on **both** the
lexical normpath and the realpath (symlink escape); `.arena`/`.git` protected for writes;
read/write prefix allocations. Execution: argv-only (string argv recovered via shlex **unless** it
contains shell metacharacters → REFUSE_SHELL_METACHARACTERS); NEVER_RUN programs; egress gate;
PATH lookup miss → exit 127 result (not an exception); timeout clamped [0.05s, 600s], whole process
group killed on expiry (exit code 124); output truncated at `max_out`; redaction applied once at
the executor boundary; changed-files diff (size,mtime before/after); full output spilled to
`logs/` (journal carries digest + tails). Atomic writes: tmp + rename. Commit: refuses on clean
worktree (`noop=true`), deterministic identity/dates, `--no-verify`, push never available.

### 6.9 Tick loop order (`Kernel.run`)
1. tick++, clock.tick, clear `sent_this_tick`
2. `apply_arena_decisions()` (cortex file: approve_spawn/amend/force_state/force_terminate; then
   honour pending escalations)
3. `bus.expire_timeouts()` → parent inbox escalation notes
4. deadlock detect/resolve (unless off)
5. `parent.schedule()` (drain spawn requests → resolve feature amendments → assign unowned pending;
   spawn-if-under-cap)
6. for each eligible agent (least-worked first, ≤ max_concurrent_workers): `_begin_if_assigned`;
   drain inbox (break on sleeping action); one `run_step` if still RUNNABLE
7. `reap_idle` (idle/completed > idle_ttl)
8. stop when all tasks closed
Then `checkpoint()` + trace flush. `run_resident` wraps this with injection-file drain, checkpoint
per loop, `on_tick`, park-when-idle, stop-on-stall — single-threaded by design.

### 6.10 Recovery contract (`Kernel.from_journal`)
Rebuilds agents/tasks/edges/artifacts/ledger/waits/counters/tick/cursors from the journal (+config
file + optional SNAPSHOT). `quiet=true` writes nothing (verify uses this). Clock is advanced to the
last event ts so re-armed waits do not expire in negative time. Re-derives graph edges from
produces/consumes declarations. The request ledger is rehydrated so a resumed kernel will not
re-approve a spent request; `spawn_stats` are recounted from it.

### 6.11 Verification gate (M2)
A task declaring `verify` commands may not complete until they exit 0 **through the tool path**
(same jail/timeout/redaction/journal rows). Refusal → `COMPLETION_REFUSED` (rule
`REJECT_UNVERIFIED`) + a `TASK_PROGRESS` feedback message to the agent (observe→reason-again in the
same run) + task → `waiting` + agent WORKING→IDLE (legal back edge — BLOCKED would strand it).
`run_verify` emits `TASK_VERIFIED` (VERIFY_PASSED/FAILED) and is the only thing that sets
`verified`.

---

## 7. Public interfaces to preserve

### 7.1 Engine API (library surface)
`Kernel::new(cfg)`, `submit(text)`, `run(ticks)`, `run_resident(...)`, `checkpoint`, `transition`,
`publish_artifact`, `inject_spawn_request`, `drain_injection_file`, `bind_tools`, `bind_cognition`,
`ensure_git`, `run_verify`, `completion_gate`, `metrics/monitoring/state_counts`, `summary`,
`report`, `why`, `trace`, `snapshot`, `replay`, `Kernel::from_journal(path, quiet, …)`,
`idle_predicate`, `wake_ready`, `requeue`. In Rust these become inherent methods on `Kernel`
taking `&mut self` (no `Arc<>`, no `Mutex<>` — §7.10).

### 7.2 Cognition/tool seams (traits)
```rust
trait Cognition { fn decide(&mut self, obs: &Observation) -> Result<Intent, CognitionError>;
                  fn fingerprint(&self) -> CognitionFingerprint; }
trait Policy     { fn step(&mut self, ctx: &mut ActorCtx, msg: Option<&Message>) -> Action; }
```
`PolicyCognition` adapts `Policy` → `Cognition` exactly as Python does (read-only view; capability
errors on missing members are impossible by construction in Rust — the type system replaces the
runtime `AttributeError` catch). `Tool` handlers stay private to the executor; the registry is
data. `LLMCognition`/`ArenaCognition`/`LocalModelCognition` remain **seams only** — no providers
are implemented (Step 25).

### 7.3 `arena` CLI (19 subcommands)
plan, submit, run (--ticks/--resident), status, board, inbox, verify, agents (--json), spawns
(--json/-v), request (--agent/--role/--skill/--needs/--produces/--task/--work/
--capability-class/--judge/--ticks/--resident/--no-run), watch (--every/--count/--once), accept
(--out/--ticks), why <agent>, trace <cid>, pause/resume/terminate <agent>, chaos-report (--out),
chaos-run <id>. Globals: --root, --budget=6, --workers=2, --idle-ttl=1e9, --wall. Exit 0/1/2 per
command semantics (verify: 0 chain+parity OK, 1 otherwise; submit: 2 on unschedulable plan).

### 7.4 `arena-code` CLI
`arena-code "<prompt>"` | `-`/`--stdin` | interactive TTY prompt; `--plan`, `--project`,
`--resume`, `--ticks 90`, `--out`, `--provider policy|…`, `--model`, `--allow-egress`, `--clock
wall|virtual`, `--events`, `--quiet`, `--workspace`; subcommands: status, verify (--no-exec),
inspect (--tail/--only/--tools), recover, plan, config show|set|set-key (--from-env), doctor
(--json/--live/--project). **Exit codes are the interface:** 0 verified · 10 completed-not-verified
· 20 escalations open · 30 environment failure · 64 usage. Bare argv → `run`; `-` → stdin;
string `SystemExit` → 64. `--version` prints runtime version + toolchain.

---

## 8. Proposed Rust architecture (Step 1)

### 8.1 Crate layout
One crate, two binaries (splits can come after parity; see §8.11):

```
Cargo.toml            [lib] name = "arena"   [[bin]] arena   [[bin]] arena-code
src/
  lib.rs              engine re-exports; RUNTIME_VERSION; engine/product boundary doc
  clock.rs            trait Clock; VirtualClock; WallClock
  msg.rs              Plane, EventType (61), Topic, Message, newtypes for ids, causality consts
  ids.rs              AgentId, TaskId, ProjectId, CorrelationId, SpawnRequestId(Rid),
                      ArtifactName, ClaimKey, WaitId, ToolName (newtypes over Arc<str>/String)
  lifecycle.rs        AgentState, TRANSITIONS (const fn table), Lifecycle, TransitionRecord
  journal/mod.rs      Journal (rusqlite), append/emit/verify_chain/truncate_after/claims/waits
  journal/canon.rs    canonical JSON + py_repr_f64 (hash-chain parity)
  journal/fold.rs     Fold projection (agents/tasks/artifacts/requests/waits/claims/meta)
  graph.rs            TaskSpec, TaskStatus, DependencyGraph (petgraph-free; Kahn generations),
                      CycleError, amend/rollback, wait-for graph, critical path
  registry.rs         AgentRecord, AgentRegistry, SpawnBudget, cover()
  bus.rs              Bus, Delivery, PollCounter, durable waits, expire_timeouts
  policy/mod.rs       Act, Action, SLEEPING_ACTIONS, Policy trait, make_policy
  policy/builtins.rs  SimulatedWork, WaitForArtifacts, PollUntilReady, EscalateOnComplexity,
                      Hybrid, NeedsSpecialist
  cognition/mod.rs    Control, Observation, Outcome, ToolCall, Intent, validate_intent,
                      Cognition trait, CognitionError
  cognition/policy_adapter.rs  PolicyCognition
  actor.rs            AgentActor, ActorContext, run_step, observe, intent→action, _complete gate
  spawn.rs            RequestState FSM, SpawnRequest, CapabilityCatalog, SpawnLedger, Rule enum
  parent/mod.rs       ParentArena: intake/evaluate/commit, assign, schedule, supervision,
                      feature requests, cortex files, deadlock, render/status
  parent/planner.rs   RuleBasedPlanner (rule table as const data)
  kernel/mod.rs       Kernel (owns everything), run(), run_resident(), checkpoint, injection,
                      verify gate, artifacts, config side-file
  kernel/recovery.rs  from_journal
  kernel/metrics.rs   metrics/monitoring
  tools/mod.rs        ToolSpec table (13), ToolResult, Executor (execute/plan/describe/can/bind)
  tools/jail.rs       Jail, resolve(), RefuseCode enum, snapshot()
  tools/redact.rs     REDACTIONS, redact(), digest(), file_digest()
  tools/proc.rs       argv exec (std::process + process_group(0)), timeout thread + killpg,
                      env injection, PATH probe, spill logs, changed-diff
  acceptance.rs       phase-2 acceptance demo (arena accept)
  cli/arena.rs        `arena` clap definitions + commands
  cli/code.rs         `arena-code` clap definitions + commands + exit-code mapping
  config.rs           typed Config, precedence (env→file→flags fill-gaps), Secret wrapper
  product/mod.rs      RUNTIME_VERSION; engine/product boundary (engine never imports product)
  product/project.rs  Project create/load/discover, manifest, PROJECT_CREATED
  product/orchestrate.rs organise(), _verify_all, _commit_all, RunResult, report
  product/coding_agent.rs  CodingAgent cognition source
  product/cognition.rs      ScriptedCoding, ReflectiveCoding
  product/plans/calc.rs     acceptance plan as const data
  product/doctor.rs   doctor checks, OPTIONAL set, credential presence
  bin/arena.rs        thin main → arena::cli::arena::main()
  bin/arena-code.rs   thin main → arena::cli::code::main()
tests/
  unit_*              inline #[cfg(test)] in modules (lifecycle table, jail paths, redaction…)
  test_journal.rs … test_spawn.rs        mirrors of tests/test_*.py (§9)
  adversarial/mod.rs  31 chaos scenarios + Scenario harness (also drives `arena chaos-report`)
  parity/mod.rs       golden-fixture comparisons against Python-produced journals/folds
  e2e_code.rs         arena-code acceptance path in a temp workspace
benches/bench.rs      std::time measurement binary (no criterion; see §12)
```

### 8.2 Domain types (Step 2)
Strong enums for `AgentState` (10), `RequestState` (8), `TaskStatus` (Pending|Assigned|Running|
Waiting|Done|Failed), `Plane`, `EventType` (61), `Act`, `Control`, `Risk`, `RefuseCode`
(one variant per REFUSE_*, incl. REFUSE_UNKNOWN_TOOL, REFUSE_TOOL_NOT_GRANTED, REFUSE_NO_GIT_REPO,
REFUSE_EMPTY_PATH, REFUSE_WRITE_AT_ROOT, REFUSE_ABSOLUTE_PATH, REFUSE_STAT_FAILED,
REFUSE_PATH_JAILBREAK, REFUSE_PROTECTED_PATH, REFUSE_WRITE_OUTSIDE_ALLOCATION,
REFUSE_READ_OUTSIDE_ALLOCATION, REFUSE_NO_WORKSPACE, REFUSE_MISSING_ARGS, REFUSE_NOT_IMPLEMENTED,
REFUSE_BINARY_FILE, REFUSE_EDIT_TARGET_MISSING, REFUSE_SHELL_METACHARACTERS, REFUSE_BAD_ARGV,
REFUSE_FORBIDDEN_PROGRAM, REFUSE_EGRESS_DISABLED, REFUSE_EMPTY_MESSAGE, REFUSE_BAD_WHAT,
REFUSE_REPEATED_REFUSAL, REFUSE_NO_EXECUTOR). Newtypes `AgentId`, `TaskId`, `ProjectId`,
`CorrelationId`, `Rid`, `ArtifactName`, `ClaimKey`, `WaitId`, `AgentRole`, `CapabilityClass`,
`WorkspacePath` (validated relative path). Journal payload fields are a typed `EventFields`
(serde `serde_json::Map` under the hood — payloads are open by design, like Python's `**fields`)
but every field the fold projects on is a named constant checked by test.

### 8.3 Ownership model (Step 3)
The Python kernel is explicitly single-owner ("Not thread-safe by design: a single kernel owns
it"). The Rust port keeps that as a **type-level guarantee** rather than adding locks:

- `Kernel` owns `Journal`, `DependencyGraph`, `AgentRegistry`, `Bus`, `ParentArena`, actor map,
  artifact map, queues, tool executor, clock. `&mut Kernel` flows through the tick loop.
- The Python self-referential tangle (actor ↔ kernel ↔ parent ↔ bus all hold `kernel=`) is
  **restructured, not translated**: in Rust only `Kernel` exists as the owner; `bus`, `parent`,
  `tools` become *modules with methods taking `&mut Kernel`* (e.g. `bus::publish(k: &mut Kernel,
  msg)`), or small owned structs for pure data (subscriptions, stats, ledger). This is the single
  biggest structural change and is justified by Rust's aliasing rules: the Python code's shared
  mutable web is exactly what `Arc<Mutex<Everything>>` would reproduce — and is what Step 3 forbids.
- Actor step borrows: `Kernel::step_actor(aid)` does `let mut actor = self.actors.remove(&aid)`
  (or `std::mem::take`) → `actor.run_step(self)` → reinsert. Documented pattern; avoids
  double-borrow with zero runtime cost and zero locks.
- Mailboxes are `VecDeque<Message>` owned by the kernel (as in Python: `queues: dict[str, list]`).
- Message passing, not shared mutation, remains the inter-agent contract (already true in Python).

### 8.4 Concurrency model (Step 4) — *evaluation of tokio*
The runtime is a deterministic, single-threaded, tick-driven state machine; **logical agents are
in-memory actors scheduled over bounded worker slots** (max_concurrent_workers ≠ agent count) — no
OS thread/process per agent in either implementation, and replay parity (fold == live) is an
acceptance gate that arbitrary parallelism would jeopardize.

Decision: **synchronous std-only core.** Tokio is *evaluated and rejected for the core*:
(a) the journal is single-writer by contract; (b) virtual-clock determinism is a tested property;
(c) Python achieves all current behavior without threads. Where async would genuinely help later
(parallel real tool execution across worker slots) it is a **post-parity** change, explicitly out
of scope (Step 25). The one place Python uses concurrency primitives is subprocess **timeout**:
`subprocess.run(timeout=…, start_new_session=True)` + group kill. Rust equivalent without tokio:
`CommandExt::process_group(0)` (stable), a watcher thread doing `try_wait()` polling with the
kernel-provided virtual-clock-aware timeout, and `killpg` via the `nix` crate (or a tiny
`libc`-free fallback: `Child::kill` on the leader + `/proc` sweep — final choice at
implementation, documented then). No `unsafe`.

### 8.5 Persistence (Step 5)
`rusqlite` with `bundled` feature (gcc present). Identical schema/PRAGMAs/indexes (§5), identical
hash chain (with `py_repr_f64` canonical serializer), `fsync_every` checkpointing retained.
Rust reads Python-written journals (parity fixtures prove it) and writes journals Python could
read. Event schema is versioned via `runtime_version` in config/manifest; payloads stay
shape-compatible with Python (no new "v" field inside hashed payloads — that would break chain
verification of existing logs).

### 8.6 Serialization (Step 6)
`serde`/`serde_json` for journal payloads, manifests, config, plans, CLI JSON output, inject/
decisions/inbox JSONL, transcripts. Structs: `Message` (serde), `TaskSpec` snapshot, `SpawnRequest`
(`to_dict` parity), `LedgerEntry`, `Manifest`, `PlanFile`, `KernelConfig`, `DecisionRecord`,
`Outcome`, `Intent` (serde with `control` as string — validated at the boundary, matching Python's
provider-friendly contract), `Observation::prompt_seed`. No `Debug`-format persistence anywhere.

### 8.7 DAG engine (Step 7)
Implemented directly (no petgraph): Kahn's algorithm reproduces `graphlib.TopologicalSorter`
generations exactly (Python sorts each ready batch; Rust does the same for byte-parity of
`order()`); `add_edge` = insert → validate → rollback on cycle; `amend` = snapshot → add →
validate → wire → validate → full restore on any cycle (identical semantics incl. the
"added task can close a cycle by itself" case); DFS cycle enumeration on the wait-for graph with
self-edges ignored; `critical_path_length` memoized DFS. All iteration orders that reach output
are sorted (BTreeMap/BTreeSet or explicit sorts) to preserve determinism.

### 8.8 Error model (Step 21)
`thiserror` `Error` enum per subsystem, one top-level `ArenaError` with explicit variants for
validation / tool refusal (refusals are **values**, not errors — `ToolResult { refused: Some(
RefuseCode) }`, exactly like Python) / execution / timeout / dependency / journal / recovery /
config / cognition. `anyhow` only at the two `main()`s. No `unwrap`/`expect` outside tests and
documented invariants (e.g. rusqlite connection after successful open; UTF-8 project roots).

### 8.9 Logging/observability (Step 22)
`tracing` + `tracing-subscriber` (compact fmt, env filter `ARENA_LOG`). Spans carry
`agent_id/task_id/event_id/correlation_id/project_id`. The JSON side-channel `var/events.jsonl`
trace is preserved (it is observable behavior). Secrets never logged (tested).

### 8.10 CLI (Step 13)
`clap` v4 derive, two binaries. Exit-code contracts preserved verbatim (§7.3/§7.4). Stdin/TTY
detection via `std::io::IsTerminal`. JSON outputs structurally identical (`serde_json::to_string_
pretty` matches `json.dumps(indent=2)` for the shared value shapes).

### 8.11 Why one crate (for now)
Two+ crates (`arena-core`, `arena-product`, `arena-cli`) would enforce the engine/product boundary
mechanically, but multi-crate builds amplify the dependency-vendoring problem under the egress
block and slow the parity loop. The boundary is enforced instead by a `tests/boundary.rs` test
that greps the engine modules for `product::` references (plus review discipline). Splitting into
a workspace is a mechanical, post-parity refactor.

### 8.12 Dependencies (minimal; git-dep fallback documented per crate)

| Crate | Why | git-dep fallback (crates.io blocked) |
|---|---|---|
| `rusqlite` (bundled) | journal | github.com/rusqlite/rusqlite |
| `serde`, `serde_json` | serialization | github.com/serde-rs/{serde,json} (+serde_derive) |
| `sha2` | hash chain, digests | github.com/RustCrypto/hashes |
| `clap` (derive) | CLIs | github.com/clap-rs/clap |
| `thiserror` | domain errors | github.com/dtolnay/thiserror |
| `regex` | redaction patterns, failure parsing | github.com/rust-lang/regex |
| `tracing`, `tracing-subscriber` | logging | github.com/tokio-rs/tracing |
| `nix` (or none) | killpg for timeout group-kill | github.com/nix-rust/nix |
| `libc` | only if nix is avoided | — |

All are build-time-only from source; no dynamic deps; `bundled` SQLite needs gcc (present).

---

## 9. Test migration plan (Step 15)

Python 207 pytest cases ↔ Rust targets (target: **≥207 Rust test cases**, no assertion weakened):

| Python file (cases) | Rust target | Notes |
|---|---|---|
| test_lifecycle (10) | `tests/test_lifecycle.rs` | FSM table completeness, rejections journaled, since-clock |
| test_journal (12) | `tests/test_journal.rs` | chain, torn tail, truncate, fold parity, claims race, waits |
| test_graph (14) | `tests/test_graph.rs` | derived edges, generations, cycles, amend rollback, critical path |
| test_registry (13) | `tests/test_registry.rs` | cover() scoring, overload, idle TTL, ids, lineage |
| test_bus (16) | `tests/test_bus.rs` | planes, globs, depth cap, budget, resource gating, wakes |
| test_kernel (24) | `tests/test_kernel.rs` | submit→run end states, artifacts auto-close, snapshot/replay, anchoring |
| test_spawn (31) | `tests/test_spawn.rs` | funnel, dedup, reuse, defer, cycle rollback, cap race, ledger FSM |
| test_phase2_e2e (23) | `tests/test_phase2_e2e.rs` | mid-run spawn, injection, crash-resume, no-rework, defect regressions |
| test_chaos (34) | `tests/adversarial/` | 31 scenarios + suite harness (§10) |
| test_code_m2 (30) | `tests/e2e_code.rs` + `tests/test_product.rs` | jail, refusals, verify gate, doctor, CLI exits, manifest |

Organization: `#[cfg(test)]` unit tests (fast, per-module), `tests/` integration (kernel-level),
`tests/adversarial/` (chaos), `tests/parity/` (golden fixtures), `tests/e2e_code.rs` (real
subprocess/filesystem acceptance). Fixture strategy: run the Python reference under the virtual
clock to capture journals + `fold()` + `snapshot()` + CLI JSON into `tests/parity/fixtures/`
(committed; small), then assert the Rust runtime reproduces them (semantic equality where floats/
ids are nondeterministic, byte equality for chain/hash/format). The chaos suite additionally ships
as `arena chaos-report` for CLI parity.

---

## 10. Chaos suite port (Step 16) — all 31 scenarios

illegal-transition, spawn-hard-limit, spawn-depth, not-worth-it, cycle-explicit, cycle-derived,
deadlock, duplicate-claim, message-loop, polling-measured, durable-wait, wait-timeout,
replay-parity, torn-tail, spawn-explosion, idle-reap, resource-gating, feature-options,
flood-budget, dynamic-org, crash-resume, resume-no-rework, midrun-spawn, spawn-cap-race,
spawn-cycle-rejected, spawn-reuse-not-spawn, spawn-dedup, spawn-unsupported,
crash-after-approval, resident-injection, spawn-depth-ceiling.

Ported 1:1 with their guards (`Scenario{id,title,guards,fn}` struct, same ids so reports diff
cleanly). Where Python uses `tempfile.mkdtemp`, Rust uses `tempfile`-style scratch dirs under
`std::env::temp_dir()` (small helper; no extra crate). No assertion weakened; any intentional
divergence is listed in `PARITY_NOTES.md` with justification (none anticipated).

---

## 11. Golden behavior (Step 17)

Captured before any Python removal (procedure, re-runnable via `tests/parity/capture.py` kept
temporarily):
1. Deterministic kernel runs (virtual clock) for: SaaS plan, ML plan, mid-run spawn, dedup,
   defer/cap-race, crash+resume → journals + fold + snapshot JSON.
2. One real end-to-end `arena-code` project (already produced in audit: exit 0, chain VALID,
   verify re-runs 5 commands) — manifest + journal + verify JSON.
3. CLI outputs: `arena plan/submit/status/agents/spawns/why/trace/verify`, `arena-code doctor
   --json`, `arena-code status/verify/recover`.
4. `var/chaos_report.{md,json}` from the Python suite.
Comparison is semantic where nondeterministic (timestamps, obj ids): compare event-type
sequences, per-agent states/epochs, ledger rules, task statuses/owners/deps, artifact versions,
claim owners, refusal codes, exit codes.

---

## 12. Performance measurement plan (Step 18)

A `benches/bench.rs` binary measuring, on identical workloads, Rust vs Python (Python harness
`benches/bench.py`, same scenarios): event append throughput (msgs/s), journal insert+fsync rate,
fold/replay time for a 10k-event journal, scheduling tick cost (100-agent graph), message delivery
fan-out, agent transitions/s, subprocess execution overhead (spawn+capture of `/bin/true`, N=200),
CLI startup (`arena-code --version`, cold), RSS during a 10k-event run. Numbers land in
`RUST_MIGRATION_REPORT.md` — no claims without measurements.

---

## 13. Milestones (Step 19/23) — each ends green on `cargo test` + a commit

| # | Commit | Content | Gate |
|---|---|---|---|
| −1 | `rust: toolchain + scaffold` | resolve toolchain (§0), crate skeleton, CI if applicable | `cargo build` |
| 0 | `rust: audit fixtures` | commit parity fixtures from Python runs | fixtures present |
| 1 | `rust: domain model` | ids/msg/lifecycle/graph types, unit tests | unit tests |
| 2 | `rust: journal` | rusqlite journal + chain + fold + waits/claims; parity fixture tests verify Python journals | journal tests + fixture chain verify |
| 3 | `rust: scheduler` | clock/registry/bus/kernel tick loop/policies/actor | kernel tests |
| 4 | `rust: spawning` | spawn/parent funnel + ledger + planner; phase-2 tests | spawn + phase2_e2e tests |
| 5 | `rust: tool executor` | tools/* (jail, exec, redaction, git) | jail + tool tests |
| 6 | `rust: workspace` | product/project, orchestrate seams, config | product tests |
| 7 | `rust: cognition` | cognition seam, PolicyCognition, CodingAgent, verify gate | cognition + M2 gate tests |
| 8 | `rust: cli` | both CLIs, doctor, exit codes | CLI + doctor tests |
| 9 | `rust: parity suite` | adversarial (31) + parity fixtures + e2e acceptance run | 31/31 + fixtures + e2e exit 0 |
| 10 | `rust: benches + report` | measurements, final migration report | numbers recorded |
| 11 | `rust: remove python runtime` | delete `arena/`, `tests/*.py`, `arena-code` shim (keep `tests/parity/capture.py`+fixtures until sign-off) | full suite green without Python |

Python stays runnable (untouched except the §0 fix) until milestone 11.

---

## 14. Risks / open questions

1. **Toolchain availability** (§0) — hard blocker for implementation; needs environment decision.
2. **Float formatting in the hash chain** — handled by `py_repr_f64` + fixture tests; residual
   risk: exotic exponents in `ts` (mitigated by fixture coverage).
3. **`fold()` behavioral edge cases** (double-nested legacy payloads, missing fields defaulting)
   are subtle; ported branch-by-branch from `journal.py` with fixture replay.
4. **Subprocess group-kill without tokio** — design chosen (§8.4); if `nix` proves problematic
   under git-deps, fallback documented.
5. **SQLite bundled build time** under vendoring; acceptable (one-time).
6. **Determinism of dict ordering** — Python relies on insertion order in a few places (e.g.
   `rationale`, `cover()` tie-breaks on `sorted`); Rust uses explicit ordering; fixtures catch drift.
7. **`inspect --tools` output** — Python's was un-runnable (§0 defect); Rust produces the intended
   output; noted as the one place where "behavior" improves rather than replicates.

---

## 15. Acceptance criteria (Step 24, restated as the definition of done)

Rust builds; ≥207 tests + 31 chaos scenarios pass; real filesystem + subprocess execution works
through the jail; verification gate works; journal replay + crash recovery + torn-tail recovery
work (including on Python-written journals); dynamic mid-run spawning + injection work; both CLIs
work incl. `arena-code "<prompt>"`, stdin mode, doctor, project isolation; behavior matches the
verified Python implementation on the golden fixtures; no critical Python runtime dependency
remains; `README`/docs explain build+run; the real end-to-end acceptance project runs VERIFIED
with journal + verification + spawn evidence, reported in `RUST_MIGRATION_REPORT.md`.
