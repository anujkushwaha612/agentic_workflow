# Feasibility & Architecture Analysis — Dynamic Multi-Agent Software Engineering Arena
### Can this be built *inside* the Arena Agent sandbox? (measured, not assumed)

Date: 2026-09-01 · Status: analysis only, no system code written yet.
Every number below came from a probe I ran in this sandbox during this session.

---

## 1. Verdict (one paragraph)

**Yes — with one specific substitution.** Every piece of *machinery* you specified is buildable here as a
real, running, concurrent system: multiple OS processes, a genuine message/event bus, a DAG scheduler,
blocking-without-polling, file locks, versioned artifacts with git merges, spawn budgets, cycle detection,
deadlock prevention, a live dashboard. None of that needs to be simulated or faked.

The one thing this sandbox cannot natively provide is **N independent LLM brains**. There is no model API
key in the environment (`env | grep -c api_key|token|secret` → **0**). So a "Frontend Agent" cannot
independently *think* by calling a model. It can still be a real autonomous agent — it just needs a
decision policy that is either (a) deterministic code, (b) a real LLM call using a key you supply, or
(c) me, the Arena agent, acting as the Parent Arena's reasoning layer across turns. Everything else about
the spec survives intact. This is a swap of the *cognition source*, not a reduction of the architecture.

---

## 2. Measured sandbox facts (the constraints that shape the design)

| Measured | Result | Design consequence |
|---|---|---|
| Python | 3.13.14 | `asyncio`, `graphlib` (topo sort!), `sqlite3`, `dataclasses`, `enum`, `contextvars` all stdlib |
| Preinstalled libs | `aiohttp`, `pyzmq`, `msgpack`, `networkx`, `pytest`, `numpy`, `bokeh`, `redis-py` **absent but pip works** | Bus can be **real ZeroMQ**; dashboard can be **aiohttp**; no uvicorn needed |
| CPU / RAM | **2 cores**, 1984 MB total, ~1501 MB available | Hard budget: ~12–20 heavy worker processes, or unlimited-count *logical* agents on 2 real workers |
| pids / threads | 7917 procs, 15835 threads, `ulimit -x` unlimited file locks | Process-per-agent is allowed by the kernel; RAM is the binding limit, not PID count |
| IPC: `mp.fork` + `Queue` | **123,503 msg/s** across 4 worker procs; **181 µs** round-trip latency | In-daemon queues are ~free; no IPC bottleneck |
| IPC: asyncio in-proc fanout | **14,375,939 deliveries/s** (20k msgs × 8 subs, 11 ms) | Publish/subscribe dispatch is *never* the bottleneck |
| IPC: `pyzmq` XSUB/XPUB | **91,360 pub/s**; cross-process child→parent **2000/2000 delivered** | A real broker-less pub/sub bus with topic filters works here today |
| Durability: sqlite WAL | 5000 inserts in **13 ms**; 2000 events from 4 connections, **no clashes**; 2nd connection sees all rows | Event journal + artifact registry + lock table can all be one WAL database. No Redis needed |
| Locking: `fcntl.flock` | 50 competing competitors → **50 serialized acquisitions, 0 clashes** | Real cross-process resource locking is available |
| Git | 2.47.3, 3 branches + 3 commits in **37 ms**, `ort` merge works, conflicts detectable | Branch-per-agent + merge-based coordination is *cheap enough to use for every task step* |
| `/home/user` is a git repo? | **No** (`fatal: not a git repository`) | I must `git init` it as part of setup — good, that makes the workspace VCS-native |
| Outbound network | TCP 443 open to pypi/openai/1.1.1.1; pypi HTTP **200**; api.openai.com **401 Unauthorized** | Agents can npm/pip install real deps and can call an LLM **if you give a key**; `fetch_page`/`web_search` remain available to me regardless |
| Process survival across turns | PID 1515 daemon survived 5+ separate tool calls, reparented to PID 1, ticked 56→59 over 6 s of **real wall-clock** between calls | A supervisor daemon genuinely runs continuously between my turns. Live dashboard is real, not a screenshot |
| Sandbox lifetime | `/proc/uptime` was only 68 s early in this session | **The VM can be recycled mid-conversation.** Event-sourcing/replay is not optional elegance — it is required |
| bash call model | fresh session per call, no cwd/env/state persistence, timeout **max 1800 s** | Never put system state in shell variables; long runs must be checkpointed bounded bursts |
| Node | `/usr/bin/node` + `npm` present | Worker agents can actually build frontend code, not just describe it |
| docker / podman / kubectl / terraform / psql / initdb / redis-server / mongod | **all MISSING** | "Cloud deploy" and "PostgreSQL" agents can produce *real artifacts* (TF, compose, schema, CI config) and validate them statically — they cannot execute a cluster. SQLite can substitute for live migration testing |
| Disk | 25 GB, 20 GB free | Plenty for per-agent branches, node_modules, artifacts |

---

## 3. Critical review of *your* proposed architecture

Your spec is unusually good, but read as an engineer I'd push back on six things before building it:

1. **Do not spawn an OS process per agent.** With 2 cores / 1.5 GB, "50 spawned agents" dies at the OOM
   killer, and the *number* of agents has almost no effect on throughput. The correct model is the one an
   OS already uses: **agents are lightweight logical actors** (asyncio tasks with their own mailbox,
   state machine and budget) scheduled onto **a small fixed pool of real worker processes**. This makes
   `MAX_ACTIVE_AGENTS` a resource *utilization* limit and `MAX_CONCURRENT_WORKERS` a *hardware* limit —
   two different knobs that your spec conflates. It also makes 200-agent scenarios testable here.

2. **"Event-driven, not polling" needs one extra mechanism you didn't mention.** An actor blocked on
   "wait for `DATABASE_SCHEMA_COMPLETED`" is *itself* just parking on a queue — fine in-process, but it
   burns a slot and dies with the process. Add a **wait-graph / subscription table in the journal**: a
   blocked agent persists `(agent, awaited_event, timeout_at, continuation)` and frees its slot; the
   event publisher wakes it. That is how you get *durable* non-polling waiting, and it is exactly what
   makes resume-after-sandbox-recycle possible.

3. **Locks are the wrong default and you half-knew it.** You wrote "prefer isolated workspaces/branches,
   lock only when necessary" — I'd go further: make **git branch + merge the only write-coordination
   primitive**, and demote `FILE_LOCKED` to a *negotiated advisory* used solely for genuinely
   unmergeable files (generated locks, binary, lockfile). Then "multiple agents editing the same file"
   becomes a merge-conflict event the Resource Manager resolves, not a corruption. Cost measured: 37 ms
   for a branch+commit round — negligible.

4. **Your message-type list mixes three different planes.** `TASK_*` = control plane (Parent↔Agent).
   `DEPENDENCY_*` = data plane (Agent↔Agent, resolved via the dependency manager). `FILE_LOCKED` /
   `RESOURCE_UPDATED` = resource plane (Resource Manager↔Agent). If these all flow as undifferentiated
   JSON on one bus you *will* get the feedback loops you fear (a `STATUS_UPDATE` that triggers a
   re-plan that triggers `STATUS_UPDATE`…). I will implement them as **three topics with distinct
   delivery rules**, plus a hard per-agent message budget per tick.

5. **"Infinite spawning" prevention needs a cost model, not just a cap.** A cap of `MAX_ACTIVE_AGENTS`
   can still be filled by churning short-lived agents. The robust guard is an **agent-epoch DAG with
   depth ≤ 3 and parent-lineage accounting**, plus a spawn veto that rejects requests whose declared
   work estimate is below a threshold relative to the requester's remaining work (i.e. "is this worth a
   new engineer?"). I'll implement both.

6. **Add a `REPLAN`/`AMEND` path you omitted.** Real projects change: your §9 has a Frontend agent
   requesting a password-reset endpoint, but no defined consequence when that request *mutates the DAG*.
   Without it, the Parent silently diverges from reality. I'll make `FEATURE_REQUEST` accepted →
   **graph mutation validated for cycles → re-topological-sort → reassign**, with the mutation journaled.

---

## 4. Proposed architecture (what I'd actually build)

```
                    ┌──────────────────────────────────────────────┐
                    │  YOU (Arena chat)  ←→  me = Parent's cortex  │  optional tier
                    └───────────────▲──────────────┬───────────────┘
                                    │ escalate     │ decisions.jsonl
┌───────────────────────────────────┴──────────────▼──────────────────────────┐
│                     SUPERVISOR DAEMON  (1–3 real OS procs)                   │
│  ┌───────────────┐ ┌──────────────┐ ┌───────────────┐ ┌──────────────────┐  │
│  │ Parent Arena  │ │ Dependency   │ │ Resource Mgr  │ │ Artifact Registry│  │
│  │ Planner       │ │ Manager(DAG) │ │ (flock+merge) │ │ (git+versions)   │  │
│  │ Scheduler     │ │ wait-graph   │ │ budgets       │ │ subscribers      │  │
│  │ Decision Eng  │ └──────▲───────┘ └───────────────┘ └────────────────────┘ │
│  └──────┬────────┘        │                                                   │
│         │ dispatch        │ wake(awaited_event)                               │
│  ┌──────▼─────────────────┴──────────────────────────────────────────────┐   │
│  │   EVENT / MESSAGE BUS  — 3 planes, ZeroMQ XSUB/XPUB + in-proc fastpath │   │
│  │   plane: task.*   dep.*   res.*   art.*   agent.*   parent.*           │   │
│  └──┬───────────┬───────────┬───────────┬───────────┬─────────┬─────────┘   │
│     ▼           ▼           ▼           ▼           ▼         ▼             │
│  ┌────────┐ ┌────────┐ ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐       │
│  │ agent  │ │ agent  │ │ agent  │  │ agent  │  │ agent  │  │ agent  │  …    │
│  │ actor  │ │ actor  │ │ actor  │  │ actor  │  │ actor  │  │ actor  │       │
│  │ mailbox│ │ mailbox│ │ mailbox│  │ mailbox│  │ mailbox│  │ mailbox│       │
│  │ FSM    │ │ FSM    │ │ FSM    │  │ FSM    │  │ FSM    │  │ FSM    │       │
│  │ policy │ │ policy │ │ policy │  │ policy │  │ policy │  │ policy │       │
│  └───┬────┘ └────────┘ └────────┘  └────────┘  └────────┘  └────────┘       │
│      │ executes on 2-slot real worker pool (bounded by cores, not ambition) │
│  ┌───▼───────────────────────────────────────────────────────────────────┐   │
│  │ JOURNAL (append-only, sqlite WAL) — replayable → survives VM recycle   │   │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────┬────────────────────────────────────────────────┬──┘
                           │                                                │
                 ┌─────────▼──────────┐                        ┌─────────────▼────────────┐
                 │ WORKTREE / GIT REPO│  branch per agent      │ LIVE PREVIEW :8080       │
                 │ artifacts/*.v3.json│                        │ aiohttp dashboard + SSE  │
                 └────────────────────┘                        │ (registry, DAG, locks,   │
                                                               │  message log, your inbox)│
                                                               └──────────────────────────┘
```

**Why this shape:** one event loop owns the bus and the graph (no distributed-consensus problem to solve,
25 million msgs/s of headroom), while isolation and real parallelism come from bounded worker processes
that *execute* code. ZeroMQ is already installed and gives you topic-filtered pub/sub across processes —
so "agents talk through a bus, not by touching each other" is enforced by the transport, not by
convention. The journal exists because I measured that the VM can be recycled mid-conversation.

### 4.1 Execution flow (one task, end to end)

1. `arena submit "<task>"` → Parent *plans*: extracts required expertise → **dedups against the live
   registry by skill-coverage** → emits draft task graph with declared inputs/outputs as *artifact refs*
   (dependencies are derived from artifact production/consumption, not hand-written — that's what makes
   it honest and non-hardcoded).
2. `graphlib.TopologicalSorter` validates acyclicity, then returns **parallel frontier sets**.
3. Scheduler admits tasks while `active ≤ MAX_ACTIVE_AGENTS` **and** `running ≤ MAX_CONCURRENT_WORKERS`,
   choosing owner by expertise match + current load; spawns an agent only when no existing agent covers it
   (`SpawnVeto` records the *reason* even on rejection — observable, not silent).
4. Agent actor runs its policy loop → emits `TASK_PROGRESS`; publishes artifacts via
   `git commit` on `agent/<id>` branch → Registry bumps version → bus publishes `art.updated`.
5. Waiting is **durable**: agent needing a schema writes a wait-graph row, state →
   `WAITING_FOR_DEPENDENCY`, slot released (no busy-wait, no polling). Publisher event →
   Dependency Manager resolves → targeted `dep.ready` wake → Parent re-admits that agent.
6. `WAIT_TIMEOUT` → `escalate` → Parent decides (reassign / spawn / descope / park). If it's a judgment
   call beyond policy → written to `inbox/` for me, surfaced to you; my `decisions.jsonl` feeds back.
7. Every transition is one journal row. `arena resume` replays and continues; `arena trace <id>` shows
   the full causal chain (correlation_id threaded through every derived message); `arena why <agent>`
   shows exactly what it's blocked on and since when.

### 4.2 Chaos-safeguard matrix (your §15, made concrete)

| Risk | Mechanism | How I'll prove it works |
|---|---|---|
| Simultaneous edits | branch-per-agent + `ort` merge; `flock` only for unmergeable paths | test: 2 agents same file → conflict event, no corruption |
| Duplicate work | claim-based task assignment (single `claimed_by`), artifact-level dedup | test: 2 agents same request → 1 claim, 1 `NOOP` |
| Cycles | `graphlib.TopologicalSorter` on every mutation, incl. agent-requested | test: cycle → `REJECT_CYCLE` + which edge |
| Message loops | causal depth cap, `correlation_id`, per-agent msg budget/tick, `derived_from` chain | test: A→B→A ping-pong stops at depth 3 |
| Spawn explosion | max agents, max spawn-depth 3, min-value veto, idle-termination TTL | test: recursive spawn demand caps + reaps |
| Deadlock | global lock ordering + wait-for-graph cycle detection → kill lowest-priority victim | test: 2-cycle lock grab → detected & resolved |
| Stalled forever | `WAIT_TIMEOUT`, task deadline, heartbeats, stuck-state watchdog | test: dead agent → parent reassigns |

### 4.3 The "intelligence" source — pick per agent, hot-swappable

| Tier | What decides | Autonomous? | Real reasoning? |
|---|---|---|---|
| **A. Policy code** | deterministic heuristics in Python | ✅ fully | ❌ |
| **B. LLM adapter** | real model call (key you supply; network verified open) | ✅ fully | ✅ |
| **C. Arena-driven** | **me**, as the Parent's cortex, per turn | ⚠️ turn-paced | ✅✅ |
| **D. Hybrid (recommended)** | A for execution, B if key, C only for high-stakes forks | ✅ | ✅ where it matters |

---

## 5. What is genuinely NOT possible here (I'm not going to pretend)

1. **N truly independent LLM agents** without you providing a model API key. In-me `generate_*` tools
   can't be invoked by a sandbox process.
2. **Real cloud deployment** — no docker/podman/kubectl/terraform. Terraform/K8s/compose files will be
   *real, validated artifacts*, not applied infrastructure.
3. **A live PostgreSQL server** — no `initdb`/`psql`. (pip is open, so a portable binary is *attemptable*;
   unverified.) SQLite stands in for executing migrations; Postgres remains the declared target.
4. **Multi-day continuous runtime.** 1800 s max per bash call; VM can be recycled. Bounded, resumable
   bursts + replay is the answer — which I'd argue makes the result *better* than a blobby daemon.
5. **Real ML training at scale** (2 cores, no GPU) — the ML-agents variant will orchestrate honestly but
   train tiny/toy models.
6. **35 agents at full IPC cadence** (measured ~60–70 µs/msg through sqlite at that scale — fine) but not
   35 *heavy* processes (RAM). Logical-agent scale is where the number gets big.

---

## 6. Build plan mapped to your 7 phases (each ends in a runnable check, not a vibe)

| Phase | Ships | Verification I'll run |
|---|---|---|
| 1 | kernel: Agent/Task/Registry/lifecycle FSM + **journal** | `pytest`: FSM legality matrix, journal replay == live state |
| 2 | spawning, assignment, pause/resume/terminate, vetoes | cap respected, veto reasons logged, orphan reap |
| 3 | **bus**: 3 planes, DM + broadcast + subscribe, correlation | no direct-state mutation; loop cap; delivery order |
| 4 | DAG, parallel frontier, durable wait/wake, timeouts | zero polling (assert poll-counter == 0), auto-resume, cycle reject |
| 5 | artifact registry (git+versions+subscribers), resource mgr | stale-version read blocked; conflict→merge event; lock only when needed |
| 6 | agent-initiated spawn/feature requests, Parent decision engine | approve/descope/escalate paths; epoch-depth limit |
| 7 | deadlock+conflict detection, **observability**: `arena status/trace/why/dot` + live dashboard | injected 2-cycle deadlock detected; dashboard SSE on :8080; replay after simulated recycle |

---

## 7. Recommendation

Build **D (hybrid)** on **asyncio actors + 2-worker pool + ZeroMQ bus + sqlite WAL journal + git
artifacts + aiohttp dashboard**, all inside `/home/user/arena/`. You get a system that is 100% of your
runtime spec, that demonstrably does dynamic spawning, event-driven waiting, conflict-safe parallelism and
replayable recovery — and where genuine judgment is needed it comes from me or from your key, through an
adapter I can leave stubbed or wired.
