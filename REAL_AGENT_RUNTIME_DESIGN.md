# Real Agent Runtime — design proposal (Phase 2.5)

Status: **design only.** No file under `arena/` was modified for this document. Everything marked
*measured* was executed in this sandbox during the audit and can be re-executed by the commands
printed in §11.3; everything marked *design* is not built.

What this document answers: A (what is a real agent today), B (what is still fake), C (cognition
vs runtime), D (the real coding-agent loop), E (how to get real LLM cognition *here*), F (multi-agent
cognition isolation), G (workspace creation), H (Arena integration), I (fresh-chat reproducibility),
plus the failure/security/rollout sections and one recommendation.

---

## 1. What exists today, in one honest sentence

Phases 1–2 built the **scheduling and recovery substrate** of an organization: an append-only event
log that is the only truth, an artifact-gated dependency graph that can be amended mid-run without
creating a cycle, a plane-separated bus with durable waits, per-agent lifecycle FSMs, a Parent that
admits/evaluates/refuses agent-creation requests with recorded reasons, and replay that reconstructs
the same org from the log. What does not exist is any part of an agent that *does software
engineering*: no file is read, no file is written, no command is executed, no test is run, nothing
is verified against reality, and no model is consulted.

The loop is real. The body of the loop is a stub.

## 2. A. What is currently a real agent — the actual execution path

Traced through source (line numbers are from the current tree).

```
Kernel.run(ticks)     ← the tick loop IS run(); there is no separate tick()   arena/kernel.py:339-377
  └─ per tick:
       ├─ clock.tick()                                          (virtual clock: +clock_step/tick)
       ├─ sent_this_tick.clear()                                (per-tick send budget)
       ├─ ParentArena.apply_arena_decisions()             arena/parent.py:1001 (drains var/decisions.jsonl)
       │      └─ honour_pending_escalations()                   (the human/cortex seam, Phase 2)
       ├─ Bus.expire_timeouts()                            arena/bus.py:214 (durable waits that ran out)
       ├─ ParentArena.schedule()                                 (slot assignment, cap-bounded)
       ├─ for aid in Kernel._eligible()                    arena/kernel.py:318-333 (least-worked-first,
       │                                                        capped by max_concurrent_workers)
       │    ├─ actor = self.actors.get(aid) or self.make_actor(aid)   arena/kernel.py:168
       │    ├─ self._begin_if_assigned(aid)                  arena/kernel.py:286 (IDLE→WORKING + TASK_STARTED)
       │    ├─ while self.queues.get(aid): actor.run_step()  ← mailbox drain, arena/kernel.py:361-364
       │    └─ if state in RUNNABLE: actor.run_step()
       └─ checkpoint(reason="run boundary")                      (SNAPSHOT + kernel_config.json)

AgentActor.run_step()                               arena/actor.py:190-252
  ├─ rec = registry.require(agent_id)
  ├─ msg = kernel.queues[agent_id].pop(0)           ← the mailbox, one message per step
  ├─ ctx = ActorContext(kernel, agent_id, msg, step_index=steps_run, llm_available=…)   :195-198
  ├─ action = policy.step(ctx, msg)                 ← THIS IS THE ENTIRE DECISION              :202
  ├─ steps_run += 1; rec_progress()                  (TASK_PROGRESS + policy_cursor → journal)
  └─ apply exactly one Act:  WAIT | ESCALATE | COMPLETE | PUBLISH | else work_done += work_unit
```

| Question | Answer | Evidence |
|---|---|---|
| Only a logical actor? | Yes — a record + a mailbox + a policy object, all inside one process. There is no per-agent thread and no per-agent OS process, by design. | `Kernel.actors: dict[str, AgentActor]`, `queues: dict[str, list[Message]]` (`arena/kernel.py:71-72`) |
| Own execution state? | Yes, and it is durable: `steps_run`, `last_action`, `errors`, plus `registry` record fields (`task_id`, `task_queue`, `msgs_sent`, `work_done`, lifecycle state). | `arena/actor.py:152-158` (dataclass fields), `arena/actor.py:203-208` |
| Own mailbox? | Yes. `kernel.queues[agent_id]`, filled only by the bus (subscription-filtered) or `Kernel.deliver()`. One message popped per step, and inbox draining stops at a sleeping action. | `arena/kernel.py:183-189` (`deliver`), `arena/bus.py:120` (`publish`), `arena/kernel.py:361-364` |
| Own policy state? | Yes — per-agent private counters (`_steps`, `_asked`, `_n`), and they are **journaled and restored**: `policy_cursor()` reads them, `TASK_PROGRESS` carries them, `restore_cursor()` puts them back after replay. | `arena/actor.py:168-187` (`policy_cursor` / `rec_progress` / `restore_cursor`) |
| Independent reasoning context? | **No.** There is no reasoning. There is a read-only observation surface and a deterministic branch. Nothing accumulates, nothing is inferred, nothing is remembered beyond a counter. | `ActorContext` read side (`arena/actor.py:25-99`) |
| What executes the next action? | `policy.step(ctx, msg)` — one Python call — and the kernel's `if/elif` over the `Act` enum (`arena/policy.py:18-24`) applies it. The policy never touches the filesystem, a process, or a network. | `arena/actor.py:202`, `arena/actor.py:217-247` |

Real, and worth saying plainly because it is what makes the rest possible: identity, state, mailbox,
durable waits, lifecycle legality, budget enforcement, causally-capped messaging, the journal, and
recovery of all of the above.

## 3. B. What is still fake — no softening

| # | Fabrication | Where | Why it matters |
|---|---|---|---|
| F1 | **Progress is arithmetic.** `SimulatedWork` emits `TASK_PROGRESS` rows with `pct = 100*(step-1)/steps`. Nothing happened; a percentage was printed. | `arena/policy.py:101-118` (`SimulatedWork.step`) | Every downstream metric (`work_done`, "3 agents worked") is derived from a counter, not from evidence. |
| F2 | **Effort is a constant.** `rec.work_done += kernel.work_unit` (0.25) for every non-sleeping action; `work_unit: float = 0.25` is a Kernel field. | `arena/actor.py:245`, `arena/kernel.py:43` | The Parent's `REJECT_NOT_WORTH_IT` veto therefore weighs a *claimed* `estimated_work` against *simulated* effort. It is structurally correct and empirically meaningless. |
| F3 | **Artifacts are names, not bytes.** `publish_artifact()` adds a version counter to a dict and journals it. `grep -rn "write_text\|open(" arena/` returns the journal, the trace file, `kernel_config.json`, `var/decisions.jsonl` and the acceptance report — **no writer in the repo creates a project file**. | `arena/kernel.py:246-250` | `contracts/api.json`, `database/schema.sql`, `backend/pay.py` have never existed as files. |
| F4 | **No execution, at all.** Zero `subprocess`/`Popen`/`os.system` in `arena/` (measured, §11.3). No test run, no build, no install, no server. | whole tree | Nothing in the system can fail against reality — which also means nothing in it can *succeed* against reality. |
| F5 | **Completion is self-declared.** `Act.COMPLETE` marks `task.status="done"`. It is gated on the task's *promised* outputs existing as names, never on them being valid. | `arena/actor.py:255-300` (`_complete`) | "Done" currently means "the policy reached its step count". |
| F6 | **Dependencies resolve on name, not on interface.** `WaitForArtifacts` parks on `artifact:<name>` and resumes when that name appears. An empty or malformed `contracts/api.json` satisfies it. | `arena/policy.py:122-148` (`WaitForArtifacts`) | The strongest-looking part of the "artifact-gated" claim is a string comparison. |
| F7 | **The planner is keyword matching.** `RuleBasedPlanner.RULES` maps tuples like `("database","postgres","sql","schema","db")` onto task specs. | `arena/parent.py:40-128` (`RuleBasedPlanner.RULES`) | Documented as such; still, "dynamic planning" today means "which keywords were present". |
| F8 | **The LLM tier is a guard rail that raises.** `NullLLM.decide()` throws; `HybridPolicy` only calls it when `ctx.llm_available`, which is `getattr(policy, "llm_available", False)`. | `arena/policy.py:87-96` (`NullLLM.decide` raises at :93), `arena/actor.py:195-198` | Correct and honest — but the *only* judgment path in the runtime today is the human writing to `var/decisions.jsonl`. |
| F9 | **Time is a fiction.** Virtual clock, `clock_step=0.01` per tick; `wait timeout=4.0` is 400 ticks, not four seconds. | `arena/kernel.py:42` (`clock_step: float = 0.01`) | Every timeout in the design must be re-expressed when real processes (which take real seconds) exist. See §5.4. |
| F10 | **Escalation ends in a file, not a brain.** `write_inbox` appends JSON to `var/inbox.jsonl`; nothing reads it unless a human (or an Arena chat agent, outside the process) answers. | `arena/parent.py:991` (`write_inbox`) | The cortex tier is real as an *interface* and empty as an *actor*. |
| F11 | **The acceptance demo's "payment specialist" is a scripted policy.** `NeedsSpecialist` asks after N steps when `detect_from` names a role absent from the roster. That is a genuine *runtime* trigger (not keyword planning) and it is not cognition. | `arena/policy.py:242-315` (`NeedsSpecialist`) | Already stated in Phase 2; restated here because the temptation to over-read that demo is exactly what this document exists to resist. |

Consequence, stated once so it is not repeated later: **the runtime's correctness claims are all
supported, and none of its capability claims are.** Do not describe the current system as "agents
that build software"; it is an event-sourced scheduler for simulated work.

## 4. C. The split: runtime owns consequences, cognition owns decisions

```
                        ┌──────────────── Agent Runtime (exists, Phases 1–2) ───────────────┐
                        │ state (registry rec + lifecycle)                                    │
                        │ mailbox (kernel.queues[a], bus subscription-filtered)               │
                        │ durable waits (bus.wait_for / resolve / expire_timeouts)             │
                        │ workspace (NEW: jail root + worktree binding, §7)                    │
                        │ tools   (NEW: registry + executor, §6.2)                            │
                        │ budget  (SendBudget + step/token/cost ceilings)                     │
                        └───────────────┬────────────────────────────────────────────────────┘
                                        │  Observation  ▲  Result
                                        ▼               │
                              ┌───────────────────────────────┐
                              │      Cognition Adapter        │  decide(Observation) -> Intent
                              │  (one per agent, bound at     │  never sees the filesystem,
                              │   registration, journaled)    │  never mutates the graph directly
                              └───┬───────────┬───────────┬───┘
                                  ▼           ▼           ▼
                              LLMAdapter   ArenaCortex  PolicyCognition
                            (httpx→API)  (inbox/decisions)  (wraps today's policies)
```

Three rules make the split real rather than decorative:

1. **The runtime never asks what kind of brain this is.** `AgentActor` keeps exactly one new seam:
   `self.policy` → `self.cognition`, with `PolicyCognition` adapting every existing policy class.
   All 177 tests must stay green with no semantic change — that is the definition of M0 (§10).
2. **The brain cannot touch consequences.** A cognition source returns an `Intent` (a tool name plus
   arguments, or a control verb). The runtime validates it against the tool registry and the jail,
   executes it, and journals it. An Intent that tries to mutate the graph, the roster or another
   agent's state is refused with a journalled `COGNITION_VIOLATION` — never silently dropped, which
   is the same rule Phase 2 uses for spawn requests.
3. **Cognition is swappable and the swap is observable.** `arena status` reports, per agent: source
   name (`policy` / `openai-compat` / `anthropic` / `arena-cortex`), model id, transcript length,
   tool calls issued, tokens in/out, est. cost. A run where "5 agents" report the same prompt hash
   and the same model id is then *visible as that*, instead of reading as an org (§6.4).

## 5. D. The real coding-agent loop

### 5.1 Observation — what is fed in

```python
@dataclass(frozen=True)
class Observation:
    agent_id: str; role: str; goal: str; task: dict            # task_id, title, deps, promises
    unread: tuple[Message, ...]          # ≤ N; the rest counted, not inlined
    workspace: dict                      # root-relative paths, sizes, mtimes, dirty flag
    git: dict                            # branch, head, ahead/behind, porcelain digest
    tests: dict | None                   # last real run: command, exit, digest, tail of output
    graph_view: tuple                    # public: task status + artifact name + producer id, nothing else
    recent_tools: tuple                  # last K (tool, args-digest, exit, output-tail)
    budget: dict                         # steps_left, tokens_left, cost_left, wall_left_s, tool_calls_left
    notes: tuple                           # this agent's own journaled log lines
```

`ActorContext` is already the right shape for the read side (`arena/actor.py:25-99`) and keeps being
the source of `graph_view`/`unread`/`budget`; the new fields come from the workspace tool layer.

### 5.2 Intent — what comes out

```python
@dataclass(frozen=True)
class Intent:
    tool: str = ""            # "" => control verb below
    args: dict = {}
    think: str = ""           # rationale → journal, and to the human-facing trace; never trusted
    control: str = ""         # WAIT | PUBLISH | ESCALATE | COMPLETE | SPAWN_REQUEST | NOOP
    wait_for: str = ""        # condition, e.g. "artifact:contracts/api.json"
    calls: tuple["Intent", ...] = ()      # ≤ 4, executed serially by the runtime
```

Why `calls` exists: a real turn is "read the file, then edit it, then run the test". Without a batch
field you either burn a tick per tool (slow, and it breaks the `SLEEPING_ACTIONS` turn discipline) or
you let a model drive a loop the runtime cannot bound. Serial + ≤4 + per-call validation keeps the
budget honest.

### 5.3 One step, end to end

```
1  runtime builds Observation                      (reads: graph, registry, bus, workspace, git)
2  cognition.decide(obs) -> Intent                 (only place any model or heuristic is consulted)
3  runtime validates: tool in registry? args schema-valid? paths inside jail? risk tier allowed?
     └─ no  -> journal COGNITION_VIOLATION, feed refusal back as a tool_result, re-decide (≤2x), else ESCALATE
4  journal TOOL_CALL  {rid, tool, args_digest, risk, step}          ← the durable intent
5  execute (serially; one subprocess at a time; timeout; output cap)
6  journal TOOL_RESULT {rid, exit, out_digest, out_len, changed:[(path, sha256, len)]}
7  append tool_result to this agent's transcript; obs = obs + result; back to 2 while budget & calls remain
8  exit path: WAIT (bus.wait_for) | COMPLETE (VERIFY-gated, §5.5) | ESCALATE (inbox) | PAUSE_FOR_CORTEX
```

### 5.4 Time and latency

Real `npm ci` takes 20 seconds; a tick takes microseconds. Two changes, both small:

- `Kernel.clock_mode="real"` (wall clock) for tool-using runs — waits, timeouts and `idle_ttl` then
  mean seconds, which is the only defensible unit once processes exist.
- Resident driving: `run_resident(seconds, ticks_per_loop, on_tick)` already exists (`arena/kernel.py:657-688`)
  and is the right host loop — but a blocking `decide()` inside `run_step` parks every *other*
  eligible agent for the duration. Accepted for M2 (single-process, ≤2 workers), with
  `budget.max_concurrent_workers` reduced to 1 during model calls; the honest alternative (a worker
  pool) is exactly the concurrency work Phase 3 was scoped for, so it stays there.

### 5.5 Verification, and why it changes the graph

`TaskSpec gains` (design): `verify: tuple[str, ...]` (commands), `verify_kind: "test"|"build"|"none"`.

- `Act.COMPLETE` is refused unless `verify_kind == "none"` or the last recorded `run_tests` for this
  task exited 0. Refusal is journalled as `COMPLETION_REFUSED` — a worker cannot lie about being done,
  which is the one capability claim F4/F5 currently makes by absence.
- The Parent gains a veto consistent with Phase 2's rule list: `REJECT_UNVERIFIED` (a task with no
  verify command and a non-trivial `est_work` is not allowed to complete; it must either run
  something or escalate). Reuse/dedup/cycle behaviour is untouched.
- `publish_artifact` gets an optional digest, so `known_artifacts`-style gating can check *content*,
  not just the name (fixes F6 without a new protocol).

## 6. Tool execution model

### 6.1 The toolset, against what this sandbox can actually do (measured §9)

| tool | maps to | sandbox reality |
|---|---|---|
| `list_files`, `stat_files` | `os.walk` + git status | works |
| `read_file`, `write_file`, `edit_file` | `open()`; `edit_file` = exact-string replace with uniqueness check | works |
| `run_command` | `subprocess.run(shell=False, cwd=worktree, timeout, capture_output)` | works (`/bin/sh`, `/usr/bin/timeout`) |
| `run_tests` | policy over `run_command`: `pytest -q`, `npm test`, `python -m build` | `python3 -m pytest` 9.0.3 ✓, `node` 20.20.2 + `npm` 10.8.2 ✓, `make`/`gcc` ✓ |
| `inspect_git` | `git status/diff/log/ls-files` | git 2.47.3 ✓ |
| `commit` | `git add -A && git commit -m …` (never push) | ✓ (clone over https measured working) |
| `publish_artifact` | existing `Kernel.publish_artifact` + digest | exists |
| `send_message`, `request_dependency`, `request_specialist`, `wait_for_event` | existing bus + Phase-2 request path | exists, unchanged |
| `finish` | `Act.COMPLETE` through the §5.5 gate | new gate |

Deliberately **absent, with a reason**: `apply_patch` (unified diffs get mis-copied by models;
`edit_file`'s exact-match-with-uniqueness is a better primitive for a first cut), a `browse`/web tool
(no need, and it is the biggest egress risk), and any `git push`/remote write.

### 6.2 The executor contract

Every tool call goes through one function, so there is one place where safety lives:

```python
execute(tool, args, *, agent_id, worktree, timeout_s, max_out_bytes) -> ToolResult
```

- **Jail:** `realpath(args["path"])` must be under `project/source/<agent>` (its own worktree) —
  else `REFUSED_PATH_JAILBREAK`, journalled. Writes to `.arena/` and to another agent's worktree are
  refused by the same check. Reads of `project/artifacts/` are allowed (that is the shared surface).
- **Args:** never a shell string. `run_command` takes `argv: list[str]`, so `; rm -rf` is an argument,
  not a command. `shell=False`.
- **Process hygiene:** `start_new_session=True`, kill the whole group on timeout; output truncated to
  `max_out_bytes` with a `truncated:true` marker; stdout/stderr digested (sha256) so the journal stays
  small and replay is checkable.
- **Secret redaction** on the way into the journal, the transcript and `arena trace` (§9).
- **Risk tiers:** `read` (free) / `write` (workspace-only) / `exec` (timeout+budget) /
  `exec-network` (needs `--allow-egress`) / `vcs-commit` (local only). The tier is data on the registry
  entry, so a policy-cognition agent and an LLM agent are subject to the identical ceiling.
- **Budgets:** per-step tool-call ceiling, per-task wall-clock ceiling, per-agent token/cost ceiling.
  Exhausted ⇒ forced `WAIT`/`ESCALATE`, journalled — never a silent stall (same principle as a silent
  spawn reject).

### 6.3 How results re-enter context

`TOOL_RESULT` is appended to *that agent's* transcript as an untrusted block:

```
<tool_result tool="run_tests" exit="1" truncated="false">
  …last 4 KiB…
</tool_result>
```

Two invariants: (a) the runtime never interprets tool output — only the cognition layer reads it;
(b) no agent's transcript is ever fed into another's. Cross-agent information flows only through
`graph_view` (names/statuses/producer ids) and bus messages, i.e. the §2 boundary at
`arena/actor.py:1-6` ("an actor reads its own task, the graph's public state and its own inbox").
A model-injection path from agent B into agent A's reasoning is therefore structurally unavailable,
not filtered.

### 6.4 Context isolation, and how to prove it isn't theatre

- Per-agent transcript file `projects/<id>/.arena/context/<agent>.jsonl`, append-only, replayable.
- System prompt seeded from the **task**, never from a persona file: role, goal, task spec, worktree
  path, tool schemas, budgets, "other agents are separate programs; you see only their published
  artifacts". The persona string is *not* the isolation — the transcript and the jail are.
- **Binding:** the runtime refuses an Intent whose write targets fall outside this agent's worktree,
  so a shared/leaky prompt cannot silently make one brain operate everyone's files.
- **Metrics that make fakeness visible:** `arena status` shows, per agent, `prompt_sha256[:8]`,
  model id, transcript size. `arena runs report` prints `distinct_prompts / agents`. Two agents with
  an identical prompt hash and identical transcript prefix is reported as `SHARED_BRAIN` — that is a
  CI-visible state, not an opinion.
- A chaos scenario `cognition-not-shared` asserts: first assistant turn differs across agents, and
  every tool call's path resolves inside the caller's own worktree.

## 7. Workspace isolation

```
<arena-workspace>/
  arena-code/                 # the framework (this repo checkout) — never a build target
  projects/
    20260901-2130-task-tracker/
      .arena/                 # kernel root: j.db, kernel_config.json, inbox/decisions/inject.jsonl,
      │                      # context/*.jsonl, tool-log/, merges/, acceptance.md
      source/                 # the git repo the org builds in
        <agent_a>/  <agent_b>/  …   # one worktree per agent, one branch each
      artifacts/              # published, digested outputs (shared read surface)
      logs/                   # raw stdout/stderr per tool call (digests go to the journal)
      project.json            # manifest: id, goal, created_at, kernel root, verify commands, provider tier
```

Design decisions, each because a Phase-1/2 lesson said so:

- **`.arena/` lives inside the project.** The journal must be *next to the work it describes*; a
  global journal for N projects would make `verify`/replay ambiguous (and `Kernel.root` +
  `journal_path` already support any layout — `arena/kernel.py:37-38`).
- **Per-agent worktrees, not file locks.** This is the constraint from your brief ("prefer isolated
  workspaces/branches/versioned artifacts over locking") and it keeps the kernel single-writer:
  `git worktree add source/<agent> -b arena/<agent>`; merges are explicit Parent events
  (`MERGE_REQUESTED` → `MERGE_APPLIED` / `MERGE_CONFLICT` → escalate after 3 retries).
- **Final validation runs on the merged tree**, not in a worktree. A per-agent green test is evidence
  about an agent, not about the product.
- **Never clobber.** `arena-code` refuses to create a project id whose directory exists without a
  `project.json`, and refuses to run if `project/source` is already a non-empty git repo without
  `--resume`. `--resume <project-id>` is the only path that touches an existing tree.
- **Snapshot + checkpoint:** `SNAPSHOT` rows already carry `tick`/`config`; the project manifest records
  the git head so a resumed run can detect that the tree moved underneath it (`STALE_WORKSPACE`).

## 8. Message flow: what crosses the bus, and what must not

Principle: **coordination crosses the bus; effects do not.** Tool output stays in the caller's
transcript; only its consequences (an artifact, a commit, a failure worth waking someone for) become
events. Otherwise the bus becomes a stdout multiplexer and every payload size, plane and budget
decision made in Phase 1 has to be re-litigated.

New `MessageType`s (design; the enum is in `arena/message.py`): `TOOL_CALL`, `TOOL_RESULT`,
`COGNITION_VIOLATION`, `COMPLETION_REFUSED`, `VERIFY_FAILED`, `MERGE_REQUESTED`, `MERGE_APPLIED`,
`MERGE_CONFLICT`, `WORKSPACE_REBUILT`, `CORTEX_DECISION` (an `arena-code`-as-cortex answer, so a
human-answered escalation is distinguishable from a policy one).

`TOOL_CALL`/`TOOL_RESULT` are paired by a `rid` exactly like Phase 2's spawn requests, which buys two
things for free: `arena trace <correlation_id>` shows *why* an agent claimed what it claimed, and the
crash-recovery rule in §9 is a single unfinished-`rid` lookup.

## 9. Failure, recovery and replay (the part the existing kernel is best at, and most at risk from)

The journal's meaning changes: today it *is* the state; with tools it is the **intent + effect
index**, and `project/source/` is where the bytes are. The two are bound by digests.

| Failure | Behaviour |
|---|---|
| Crash between `TOOL_CALL` and `TOOL_RESULT` | **Never blindly re-run.** Resume asks: are the promised `changed` paths present with matching sha256? Match → synthesise the missing `TOOL_RESULT` from the filesystem and continue. No match → `MARK_UNSAFE_TO_RESUME`, park that one agent, run every other agent normally, escalate. |
| Tool effect exists but journal lost/truncated | `verify_chain()` already localises corruption; the workspace is authoritative for bytes, and a rebuild journals `WORKSPACE_REBUILT` rather than pretending to replay. |
| Workspace gone (sandbox recycled, project dir deleted) | The org's *state* replays (proven today); the *work* is gone. `arena-code recover` detects `j.db` without `project.json`/`source/` and reports the exact mismatch instead of "resuming". |
| Replay determinism | Replay must never execute a tool. Rows store digests + lengths, not raw stdout, so `verify` stays read-only and cheap — the property Phase 2 had to earn twice (`bind_actor`, `quiet=True`). |
| Idempotency | `write_file` is idempotent by content; `edit_file` refuses if its target string is absent (already-applied or wrong place); `run_command` is flagged `idempotent=False` unless the tool declares it, which is what makes "re-run this?" a decision instead of a surprise. |
| Model/provider failure | Timeout, retry with jittered backoff, then `PAUSE_FOR_CORTEX` (the existing `var/decisions.jsonl` seam) — an unavailable model is a *pause*, never a fallback-to-luck. The `HybridPolicy` order (heuristic → LLM → inbox, `arena/policy.py:203-222`) is kept, so a run degrades to the deterministic tier *only* when configured, and says so in the journal. |

## 10. Implementation phases (each independently verifiable, none requires a key)

| M | Scope | Gate |
|---|---|---|
| **M0 seams** | `CognitionSource` protocol + `Intent`/`Observation`; `PolicyCognition` wrapping every existing policy; tool registry module with no executor. | **177 tests green, zero semantic diff** — chaos 31/31 unchanged, acceptance demo byte-comparable. |
| **M1 executor** | jail, `subprocess`, truncation, digests, redaction, `TOOL_CALL`/`TOOL_RESULT`; chaos: jailbreak, torn write, runaway-process timeout, secret leak. | agents can build a real file tree *with policy cognition*; journal explains every byte. |
| **M2 loop** | batched `Intent.calls`, transcript store, `run_tests`, `VERIFY` gate, `COMPLETION_REFUSED`, `REJECT_UNVERIFIED`. | a deterministic `policy-cognition` agent produces + verifies a runnable project. **This is the test that makes the loop independent of any model.** |
| **M3 adapter** | `httpx`-based OpenAI-compatible + Anthropic adapters, SSE handling, budget accounting, `arena-code doctor` live probe; `--live` opt-in only. | one live run producing a real project; without a key the suite still passes unchanged. |
| **M4 multi-agent cognition** | per-agent prompts/transcripts, prompt-hash reporting, `SHARED_BRAIN` status, cross-worktree guard, `cognition-not-shared`. | 4 agents ⇒ 4 distinct prompt hashes, 0 cross-worktree writes, real merge with ≥1 seeded conflict handled. |
| **M5 launcher + repo** | `arena-code`, project lifecycle, `recover`, `verify`, README bootstrap, packaging. | a **fresh chat** clones, runs `doctor`, `arena-code "<prompt>"`, gets a validated project, and never reads this conversation. |

Effort ≈ 4–5 focused days (M0 0.5, M1 1, M2 1, M3 0.5, M4 0.5, M5 0.5–1). M0–M2 is the part that
converts simulation into engineering; M3–M4 are additive on top of a seam that already works.

### 10.1 M0–M2: delivered status (measured, not asserted)

M0, M1 and M2 are done; M3–M5 are **not** started, and nothing below claims otherwise.

| Gate from the table | What was actually measured |
|---|---|
| M0: 177 green, zero semantic diff | `python3 -m pytest tests -q` → 177 passed at the M0 boundary; `chaos-report` 31/31; `arena.cli --root var/acc_final verify` → chain VALID, replay parity OK (89 rows: the +1 row is `REPLAY_COMPLETE`, and the Phase-2 acceptance journal is unchanged otherwise). |
| M1: agents build a real tree under jail + redaction | `arena/tools.py` executes argv (never `shell=True`), jails every path to per-agent `writes`/`reads` prefixes, digests each mutation, truncates and redacts before the journal. A refusal is `TOOL_REFUSED` with a code, *not* a failed exit code — `REFUSE_EDIT_TARGET_MISSING`, `REFUSE_REPEATED_REFUSAL`, `REFUSE_PATH_JAILBREAK`, `REFUSE_WRITE_AT_ROOT`, `REFUSE_ABSOLUTE_PATH`, `REFUSE_READ_OUTSIDE_ALLOCATION`. |
| M2: a deterministic agent produces **and verifies** a runnable project | `./arena-code run "Build a tested Python CLI calculator" --workspace var/acc-m2` → exit **0**, 15 real subprocess executions, 7 filesystem mutations, 0 refusals, 0 violations, 0 timeouts. The three seeded failures were real pytest failures; the agent read the file the output implicated, applied the correction recorded in that file, re-ran, passed, published `src/calc.py` (v1, v2), and `docs_01` — parked on `artifact:src/calc.py` — was unblocked by the dependency manager and wrote `docs/usage.md`. Verification was executed by the runtime (`TASK_VERIFIED … PASSED (3 commands)`), not by the agent's own say-so. |
| never exit 0 when unverified | A plan whose test cannot pass (`/tmp/badplan.json`) → exit **20** (`awaiting-cortex`), `ok: false`, no artifact published, no commit. `arena-code verify` on a project with no tasks/plan → **10**, never 0: an empty verification is not a passed one. |
| no LLM key anywhere | Every run above had `OPENAI_API_KEY`/`ANTHROPIC_API_KEY`/`ARENA_CODE_API_KEY` deleted from the environment; `--version` reports `arena-code 0.3.0-m0m2 (python 3.13.14)`, `doctor` → `verdict: READY (16/18 checks pass)` with the two failures being the *optional* `model-credential` and `provider-sdk`. |
| tests only got stronger | 30 new tests in `tests/test_code_m2.py` (CLI surface, doctor honesty/secret policy, refusal semantics, the agent's read-before-fix rules, plan-well-formedness, and the end-to-end check run *from outside* the policy logic). Total `pytest tests -q` → **207 passed**. No Phase-1/2 assertion was weakened or deleted; nothing was skipped or xfailed. |

Six behaviours below §10 were only discoverable by running the loop, and are now gated rather than
known-in-head: (1) `edit_file` that matches nothing must be a *refusal*, else the agent re-proposes it
forever; (2) a refusal must reach `Outcome.refused` and a refused call must not re-execute; (3) a read
that predates the agent's own edit is not evidence — `_already_read` is index-compared against the last
mutation; (4) `ARENA-FIX:` text must carry the bug line's indentation *and* replace the whole
statement, or a successful write produces an unparseable file that the runtime reports as `ok`;
(5) a WAIT on an already-satisfied condition is a deadlock, not caution, so `consumes` is checked
against `consumed_ready` before parking; (6) `fold()` had no `TASK_VERIFIED` branch, so a replayed
journal contradicted the run that produced it — `verified` is now projected, and `arena-code verify`
re-*executes* the plan's commands in the checking process instead of trusting the log.

What M0–M2 does **not** claim: no LLM was consulted by any of it; the cognition tier here is a
decision procedure over real evidence, correctly shaped for `decide(Observation) -> Intent`. M3
(provider adapters) is the next milestone and is not started.

## 11. Launcher, fresh-chat workflow, and the measured environment

### 11.1 `arena-code`

```bash
arena-code "Build a task tracker with React, FastAPI and PostgreSQL"   # one-shot
arena-code                                                        # interactive: "What do you want to build?"
echo "<prompt>" | arena-code -                                    # stdin (cleanest from another agent)
arena-code --prompt-file brief.md --ticks 400 --live --out report.md
arena-code --resume 20260901-2130-task-tracker
arena-code status | agents | spawns | trace <cid> | inspect | recover      # thin wrappers: --root projects/<id>/.arena
arena-code doctor [--live]
arena-code config set provider openai  |  config set model <id>  |  config set-key --from-env
```

Exit codes: `0` verified (every task's `verify` passed) · `10` completed-with-warnings · `20`
escalations open · `30` tool failure · `40` cognition unavailable · `64` usage. Stdin mode exists
because that is what another agent will actually use; interactive mode is for people.

### 11.2 Configuration and secrets (the real mechanism, not an invented one)

Resolution order: env → `$ARENA_CODE_CONFIG` → `~/.config/arena-code/config.json` (chmod 600) →
project `.arena/config.json` → flags. Keys are **read from env or file, never stored by the tool,
never written to the journal, never echoed** — including in errors.

```
ARENA_CODE_MODEL_PROVIDER = openai-compat | anthropic | arena-cortex | policy
ARENA_CODE_MODEL          = model id (no default: an absent model is a configuration error, not a guess)
ARENA_CODE_API_KEY        = the secret (only read at adapter construction)
ARENA_CODE_BASE_URL       = for openai-compat; required if not api.openai.com
ARENA_CODE_ALLOW_EGRESS   = 0 → agent-run commands get no network (design default: 0, --allow-egress to open)
ARENA_CODE_MAX_TOKENS / ARENA_CODE_MAX_COST_USD   = per-agent ceilings
```

This sandbox **has no key** and no broker (measured below), so the config mechanism is the whole
answer for E: the adapter is real code, the credential is the user's, and `policy` tier keeps every
test runnable without it. A user who does not want to paste a key into a chat exports it themselves;
the tool never asks for it on stdin and never persists it.

### 11.3 Environment audit — measured today, reproducible

```
env | grep -icE 'key|token|secret|api'            → 0        # no credential of any kind
env                                               → only PATH/HOME/USER/E2B_SANDBOX*/E2B_TEMPLATE_ID
curl https://api.openai.com/v1/models             → HTTP 401  "Missing bearer authentication"
curl -XPOST https://api.anthropic.com/v1/messages → HTTP 401
ss -ltn                                           → jupyter on 127.0.0.1:8888 (Tornado), link-local
                                                    169.254.0.21 (E2B envd), no 11434
E2B_EVENTS_ADDRESS=http://192.0.2.1               → 404 (not an inference endpoint)
torch/transformers/llama_cpp/vllm/openai/anthropic→ all absent
nproc=2  Mem=1Gi  disk 20Gi free  → local model inference is impossible (not "slow": impossible)
pip install fastapi uvicorn sqlalchemy            → works   (fastapi 0.141.1, sqlalchemy 2.0.52)
npm install react                                 → works   (react 19.2.8)
git clone https://github.com/octocat/Hello-World  → works   (git 2.47.3)
toolchain present: node 20.20.2 npm 10.8.2 make gcc jq diff patch tar curl timeout pytest 9.0.3
toolchain absent: psql sqlite3 rg tmux pipx uv;  no postgres server/initdb binaries
grep -rn "subprocess|Popen|os.system" arena/       → 0 hits # no execution path exists yet
grep -rn "write_text|open(" arena/ (non-chaos)     → journal, trace, kernel_config.json,
                                                    decisions.jsonl, acceptance report only
```

Answers to E, numbered as asked:

1. **Arena's own reasoning** — reachable only as *another process deciding between turns*: the kernel
   already reads `var/decisions.jsonl` and honours `approve_spawn`/`amend`/`force_state`/
   `force_terminate` (`arena/parent.py`). It is **not** a callable API inside the sandbox, and
   `arena-code` must not assume it exists. So: tier `arena-cortex` = a human or the outer Arena agent
   answering escalations, out-of-band, at the Parent level only. Real, but not per-step cognition.
2. **External model API** — the only route to genuine per-step reasoning here. Egress is genuine and
   not TLS-intercepted (the 401 bodies are the providers'), the key is absent, SDKs are installable
   from PyPI, and stdlib + `httpx` are already enough. Verdict: **real, contingent on a user key.**
3. **Local models** — no server, no runtime, no RAM, no weights. **Not available, at any size.** Do
   not design for it.
4. **Deterministic policy** — what runs today; sufficient to validate the *loop*, which is why M2's
   gate does not need M3.

## 12. Arena integration boundary

The boundary is a directory convention plus one command:

```
Arena chat agent (outside the kernel, never in the tick loop)
   │  arena-code -   (prompt on stdin, cwd inside the shared workspace)
   ▼
arena-code  ──creates──►  projects/<id>/{.arena,source,artifacts,logs}
   │                       │
   │  in-process Kernel.run_resident()      ── agents: worktrees, tools, commits, tests
   │  writes accept --out projects/<id>/.arena/acceptance.md
   ▼
stdout: project dir + final `status` JSON + report path        # machine-readable summary, no scrollback needed
```

Rules: no `localhost`/port contract (the platform proxy already exposes ports for *viewing*, which is
a fine follow-on but not an interface we depend on); the chat agent may answer an escalation by
appending to `projects/<id>/.arena/decisions.jsonl`, which is the same file the kernel already polls
between ticks. The framework directory is never a build target, and `arena-code` writes nothing above
`projects/`.

## 13. Security

- **Jail, not permission prompt.** Every file tool is path-checked against the agent's worktree;
  `..`, symlink escape and absolute paths are refused *and journalled*.
- **argv, not shell.** No `shell=True` anywhere in the executor. Interpreters are allowed only as
  `python3 -c` / `node -e` equivalents with the same jail, and flagged `idempotent=False`.
- **Egress is a mode, not an accident.** `ARENA_CODE_ALLOW_EGRESS=0` by default: `pip`/`npm` installs
  are then refused with an explicit reason rather than hanging. Raising it is a user decision recorded
  in `project.json`.
- **Secrets:** regex redaction (`sk-…`, `ghp_…`, `Bearer …`, `x-api-key`) at three chokepoints — tool
  output, transcript, journal — plus a `doctor --live` assertion that a canary value never appears in
  `j.db` after a run. Nothing is ever stored client-side by this tool.
- **Prompt injection:** tool results and other agents' published text are marked untrusted in the
  transcript; the runtime (not the prompt) owns interpreting them. `git push`, remote writes and
  `curl | sh`-shaped commands are `exec-network`/`vcs-remote` tiers, denied by default.
- **Resource ceilings:** per-call timeout, output byte cap, per-agent wall/token/cost ceilings,
  process-group kill. With 1 GiB RAM the ceiling is not a nicety: an unbounded `npm` install can kill
  the sandbox that is holding the journal.
- **Single writer, no locks:** one kernel process owns the journal (unchanged from Phase 1); agents
  isolate by worktree rather than sharing a tree.

## 14. What Phase 2 leaves that Phase 2.5 will have to respect

Four things, so they are not rediscovered as "surprising":

1. `task_id` is a journal **column**, not payload — every new decision event needs an explicit
   payload copy for rid-keyed readers (this bit us three times in Phase 2). `TOOL_CALL`/`TOOL_RESULT`
   must therefore carry `rid` + `args_digest` in the payload from day one.
2. `fold()` builds an entity's record at its **first** journalled row; anything that precedes the
   entity's registration row is filed nowhere (that is how a replayed agent got stuck in `CREATED`).
   So `AGENT_REGISTERED` must precede the first `TOOL_CALL` for that agent, and
   `WORKSPACE_BOUND` must precede both.
3. Replay must stay read-only. Adding `WORKSPACE_BOUND`/`MERGE_*` is safe; adding any executor call
   inside `from_journal` would break the "verify writes nothing" invariant that took six debug rounds
   to earn.
4. **Found while writing this, not fixed here:** `Kernel.run_resident` is defined **twice** —
   `arena/kernel.py:379-388` (the older `run`-loop version, no injection drain, no `on_tick`) and
   `arena/kernel.py:657-688` (the Phase-2 version). Python keeps the last definition, so behaviour is
   correct today and the first block is unreachable dead code. It is exactly the shape of bug that
   becomes a hazard the moment `on_tick` grows a tool-execution role, so M0's seam work should delete
   the dead copy (one-line deletion, no semantic change, all 177 tests are its regression check).

---

## 15. Recommendation — Option B: real tool execution + the cognition seam first

**Choose B, and take it in M0→M2 order (seams → executor → loop), leaving Phase 3 and the launcher
polish after it.**

Why not **A (Phase 3 first)**: Phase 3 adds *multiplicity*, not *capability*. The limitation you
named is "cognition is still a deterministic policy" — shipping ZeroMQ transport changes that
sentence by zero words. Worse, it is the sequence that costs the most rework: today the bus carries
small control-plane payloads with a per-sender budget, a causal-depth cap and plane separation, and
every one of those rules has to be re-decided the moment a message must carry real stdout, a diff or
an artifact's bytes. If transport lands before tools, you write the transport twice. If tools land
first, Phase 3 ships as a pure transport swap with `TOOL_CALL`/`MERGE_*` payloads already shaped.

Why not **C (launcher/workspace first)**: C is packaging around a stub — a nicer way to produce the
same fake project, faster. Its value is real but it is ~half a day at the end (M5), and its design
depends on things B decides (per-agent worktrees under `source/`, `.arena/` as the kernel root, which
commands count as `verify`). Building the launcher now means hard-coding a layout that M1's jail will
want to change.

Why B is the right cut, in one line each:

- it attacks **the** stated limitation instead of an adjacent one;
- M0–M2 stay fully testable **offline**, so "the loop closes" gets proven by a deterministic agent
  building and verifying a real project — a model becomes an *upgrade*, never a *dependency*;
- the seam is cheap to build *now* and expensive to retrofit: putting `Intent`/`Observation` + tool
  registry in front of the existing `policy.step(ctx, msg)` (the single call at `arena/actor.py:202`)
  is a refactor; inventing it after three layers of transport and a launcher is a rewrite;
- it forces the two honesty fixes that everything else depends on — the `VERIFY` completion gate
  (§5.5) and content-digested artifacts (§5.5). Without those, an LLM plugged into today's runtime
  would produce *confident, unverified, journalled fiction*, which is strictly worse than the current
  transparent simulation, because it looks like evidence.

Explicit non-goals for this step, so B stays B: no multi-process transport, no dashboard, no real
Postgres server (none exists here — §11.3), no autonomous git push, no claim that any agent is
intelligent until a live provider run exists and is labelled as such.
