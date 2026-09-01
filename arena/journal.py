"""Event-sourced journal: the only mutable-truth source in the system.

Two reasons this is Phase 1 and not Phase 7:
  1. I measured that the sandbox VM can be recycled mid-conversation (/proc/uptime read 68s
     during probing). A daemon whose state lives in RAM is not an autonomous system, it is a
     demo. Here, every state change is one journaled row, so `fold(journal)` reconstructs
     agents, tasks, claims and pending durable waits exactly.
  2. Causal debugging. correlation_id + caused_by on every event turns "why is the backend
     agent stuck?" into a single query instead of a post-mortem.

Durability knobs are per-append (fsync every N, default 1) - the measured cost of sqlite WAL
here is 13ms/5000 inserts, so we can afford to be honest about it.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .message import Message, MessageType

GENESIS = "0" * 64

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,
  ts          REAL    NOT NULL,
  etype       TEXT    NOT NULL,
  plane       TEXT    NOT NULL,
  topic       TEXT    NOT NULL,
  actor       TEXT    NOT NULL,
  target      TEXT,
  task_id     TEXT,
  resource    TEXT,
  correlation_id TEXT,
  caused_by   TEXT,
  depth       INTEGER NOT NULL DEFAULT 0,
  payload     TEXT    NOT NULL,
  hash        TEXT    NOT NULL,
  prev_hash   TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS ev_task  ON events(task_id);
CREATE INDEX IF NOT EXISTS ev_corr ON events(correlation_id);
CREATE INDEX IF NOT EXISTS ev_type ON events(etype);
CREATE INDEX IF NOT EXISTS ev_act  ON events(actor);

CREATE TABLE IF NOT EXISTS claims(
  claim_key   TEXT PRIMARY KEY,
  first_owner TEXT NOT NULL,
  task_id     TEXT,
  ts          REAL NOT NULL,
  losers      TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS waits(
  wait_id     TEXT PRIMARY KEY,
  agent_id    TEXT NOT NULL,
  condition   TEXT NOT NULL,
  task_id     TEXT,
  correlation_id TEXT,
  armed_at    REAL NOT NULL,
  timeout_at  REAL,
  state       TEXT NOT NULL DEFAULT 'WAITING',
  wake_reason TEXT
);
CREATE INDEX IF NOT EXISTS wa_cond ON waits(condition, state);
CREATE INDEX IF NOT EXISTS wa_agent ON waits(agent_id, state);
"""


def _hash_row(prev_hash: str, row: tuple) -> str:
    h = hashlib.sha256()
    h.update(prev_hash.encode())
    h.update(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode())
    return h.hexdigest()


@dataclass
class Journal:
    """Append-only log + fold projections. Not thread-safe by design: a single kernel owns it."""

    path: str | Path = ":memory:"
    fsync_every: int = 1
    now_fn: Callable[[], float] = field(default=lambda: 0.0)
    _conn: sqlite3.Connection = field(init=False)
    _appends: int = field(default=0, init=False)
    _last_hash: str = field(default=GENESIS, init=False)

    def __post_init__(self) -> None:
        if str(self.path) != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._last_hash = self._load_last_hash()

    # ------------------------------------------------------------------ append
    def append(self, msg: Message, extra: dict[str, Any] | None = None) -> Message:
        payload = dict(msg.payload)
        if extra:
            payload.update(extra)
        ts = msg.ts if msg.ts else self.now_fn()
        # Message.body is not a column, so it travels inside the payload. Canonical JSON is
        # required: the hashed bytes and the stored bytes must be identical or verify_chain()
        # flags rows that nobody touched.
        payload.setdefault("body", msg.body)
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        row = (ts, str(msg.msg_type), str(msg.plane), msg.topic, msg.from_actor, msg.to_actor,
               msg.task_id, msg.resource, msg.correlation_id, msg.caused_by, msg.causal_depth,
               blob)
        prev = self._last_hash
        h = _hash_row(prev, row)
        cur = self._conn.execute(
            "INSERT INTO events(ts,etype,plane,topic,actor,target,task_id,resource,correlation_id,"
            "caused_by,depth,payload,hash,prev_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (*row, h, prev),
        )
        self._last_hash = h
        msg.seq = int(cur.lastrowid)
        msg.ts = ts
        self._appends += 1
        if self.fsync_every and self._appends % self.fsync_every == 0 and str(self.path) != ":memory:":
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return msg

    def emit(self, msg_type: MessageType | str, actor: str, target: str = "parent",
             body: str = "", task_id: str | None = None, resource: str | None = None,
             correlation_id: str = "", caused_by: str | None = None, causal_depth: int = 0,
             payload: dict[str, Any] | None = None, **fields: Any) -> Message:
        # An explicit `payload=` used to be wrapped *inside* **fields, producing
        # {"payload": {...}, "body": ...} and hiding 'artifact' from every projection that
        # reads the payload. Merge instead of nesting.
        merged = dict(payload or {})
        merged.update(fields)
        m = Message(msg_type=msg_type, from_actor=actor, to_actor=target, body=body,
                    task_id=task_id, resource=resource, correlation_id=correlation_id,
                    caused_by=caused_by, causal_depth=causal_depth, payload=merged)
        return self.append(m)

    # ------------------------------------------------------------------- read
    def events(self, *, actor: str | None = None, task_id: str | None = None,
               etype: str | None = None, correlation_id: str | None = None,
               after_seq: int = 0, limit: int | None = None) -> list[sqlite3.Row]:
        sql, args = "SELECT * FROM events WHERE seq > ?", [after_seq]
        for col, val in (("actor", actor), ("task_id", task_id), ("etype", etype),
                         ("correlation_id", correlation_id)):
            if val is not None:
                sql += f" AND {col}=?"
                args.append(val)
        sql += " ORDER BY seq"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return list(self._conn.execute(sql, args))

    def count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    def trace(self, correlation_id: str) -> list[dict[str, Any]]:
        return [self._rowdict(r) for r in self.events(correlation_id=correlation_id)]

    def _rowdict(self, r: sqlite3.Row) -> dict[str, Any]:
        """Row -> dict. Note: `body` is not a column, it lives inside the payload blob; we hoist
        it so callers (arena why / trace / the chaos suite) can read it without knowing that."""
        d = {k: r[k] for k in r.keys() if k != "payload"}
        d["payload"] = json.loads(r["payload"])
        d["body"] = d["payload"].get("body", "")
        return d

    def latest_snapshot(self) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT payload FROM events WHERE etype='SNAPSHOT' "
                                 "ORDER BY seq DESC LIMIT 1").fetchone()
        return json.loads(row["payload"])["snapshot"] if row else None

    # ------------------------------------------------------- chain integrity
    def _load_last_hash(self) -> str:
        row = self._conn.execute("SELECT hash FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        return row["hash"] if row else GENESIS

    def verify_chain(self) -> tuple[bool, str, int]:
        """Recompute the hash chain. (ok, problem, seq_of_problem)."""
        prev = GENESIS
        for r in self._conn.execute("SELECT * FROM events ORDER BY seq"):
            row = (r["ts"], r["etype"], r["plane"], r["topic"], r["actor"], r["target"],
                   r["task_id"], r["resource"], r["correlation_id"], r["caused_by"], r["depth"],
                   r["payload"])   # stored payload text is the canonical JSON that was hashed
            exp = _hash_row(prev, row)
            if exp != r["hash"]:
                return False, f"hash mismatch (torn/tampered row) at seq {r['seq']}", int(r["seq"])
            if r["prev_hash"] != prev:
                return False, f"chain link broken at seq {r['seq']}", int(r["seq"])
            prev = r["hash"]
        return True, "", -1

    def truncate_after(self, seq: int) -> int:
        """Used by chaos tests to simulate a truncated tail after a crash."""
        self._conn.execute("DELETE FROM events WHERE seq > ?", (seq,))
        self._last_hash = self._load_last_hash()
        return self.count()

    # ------------------------------------------------------------- durable waits
    def arm_wait(self, wait_id: str, agent_id: str, condition: str, task_id: str | None,
                 correlation_id: str, armed_at: float, timeout_at: float | None) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO waits(wait_id,agent_id,condition,task_id,correlation_id,"
            "armed_at,timeout_at,state,wake_reason) VALUES(?,?,?,?,?,?,?,'WAITING',NULL)",
            (wait_id, agent_id, condition, task_id, correlation_id, armed_at, timeout_at))

    def resolve_wait(self, wait_id: str, state: str, wake_reason: str) -> None:
        self._conn.execute("UPDATE waits SET state=?, wake_reason=? WHERE wait_id=?",
                           (state, wake_reason, wait_id))

    def active_waits(self, condition: str | None = None) -> list[dict[str, Any]]:
        if condition:
            rows = self._conn.execute(
                "SELECT * FROM waits WHERE state='WAITING' AND condition=?", (condition,))
        else:
            rows = self._conn.execute("SELECT * FROM waits WHERE state='WAITING'")
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ claims
    def claim(self, key: str, owner: str, task_id: str | None, ts: float) -> tuple[bool, str]:
        """Atomic first-writer-wins. Returns (acquired, incumbent). This is the duplicate-work
        guard: it is a PRIMARY KEY, not a read-then-write check, so it cannot race."""
        try:
            self._conn.execute(
                "INSERT INTO claims(claim_key, first_owner, task_id, ts) VALUES(?,?,?,?)",
                (key, owner, task_id, ts))
            return True, ""
        except sqlite3.IntegrityError:
            row = self._conn.execute("SELECT first_owner FROM claims WHERE claim_key=?",
                                     (key,)).fetchone()
            incumbent = row["first_owner"] if row else "?"
            losers = json.loads(self._conn.execute(
                "SELECT losers FROM claims WHERE claim_key=?", (key,)).fetchone()["losers"])
            losers.append(owner)
            self._conn.execute("UPDATE claims SET losers=? WHERE claim_key=?",
                               (json.dumps(losers), key))
            return False, incumbent

    def claims(self) -> dict[str, dict[str, Any]]:
        out = {}
        for r in self._conn.execute("SELECT * FROM claims"):
            out[r["claim_key"]] = {"owner": r["first_owner"], "task_id": r["task_id"],
                                   "losers": json.loads(r["losers"])}
        return out

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------- fold
    def fold(self) -> dict[str, Any]:
        """Rebuild live state from the log alone. The replay test asserts fold == live state."""
        from .lifecycle import AgentState
        agents: dict[str, dict[str, Any]] = {}
        tasks: dict[str, dict[str, Any]] = {}
        artifacts: dict[str, int] = {}
        # Phase 2 request ledger: a *refused* request is exactly what a reviewer looks for, so it
        # is projected in its own map rather than conflated with tasks that were never created.
        requests: dict[str, dict[str, Any]] = {}
        for r in self._conn.execute("SELECT * FROM events ORDER BY seq"):
            p = json.loads(r["payload"])
            t = r["etype"]
            actor, target = r["actor"], r["target"]
            if t == MessageType.AGENT_REGISTERED:
                aid = p.get("agent_id") or target
                agents[aid] = {"agent_id": aid, "role": p.get("role", "?"),
                                "skills": list(p.get("skills", [])),
                                "state": str(AgentState.CREATED), "state_since": r["ts"],
                                "transitions": 0, "epoch": p.get("epoch", 0),
                                "spawned_by": p.get("spawned_by", "parent"),
                                "terminated_reason": None, "task_id": None, "task_queue": [],
                                "msgs_sent": 0, "work_done": 0.0, "steps_run": 0}
            elif t == MessageType.STATE_TRANSITION:
                aid = actor if actor in agents else p.get("agent_id")
                if aid in agents:
                    agents[aid]["state"] = p.get("to") or agents[aid]["state"]
                    agents[aid]["state_since"] = r["ts"]
                    agents[aid]["transitions"] += 1
            elif t == MessageType.AGENT_PAUSED:
                aid = p.get("agent_id") or r["target"]
                if aid in agents:
                    agents[aid]["state"] = "PAUSED"
                    agents[aid]["state_since"] = r["ts"]
            elif t == MessageType.AGENT_RESUMED:
                aid = p.get("agent_id") or r["target"]
                if aid in agents:
                    agents[aid]["state"] = "IDLE"
                    agents[aid]["state_since"] = r["ts"]
            elif t == MessageType.AGENT_TERMINATED:
                aid = p.get("agent_id") or target
                if aid in agents:
                    agents[aid]["state"] = str(AgentState.TERMINATED)
                    agents[aid]["terminated_reason"] = p.get("reason")
            elif t == MessageType.TASK_PROGRESS and r["target"] == "parent" \
                    and p.get("steps_run") is not None and actor in agents:
                agents[actor]["steps_run"] = p["steps_run"]
                agents[actor]["policy_cursor"] = p.get("policy_cursor", 0)
            elif t == MessageType.STATS and actor in agents:
                agents[actor]["msgs_sent"] = p.get("msgs_sent", 0)
                agents[actor]["work_done"] = p.get("work_done", 0.0)
                agents[actor]["steps_run"] = max(p.get("steps_run", 0),
                                                 agents[actor].get("steps_run", 0))
            elif t == MessageType.RESOURCE_UPDATED and (p.get("artifact")
                                                        or (p.get("payload") or {}).get("artifact")):
                # Unwrap a legacy double-nested payload too, so logs written before the fix still
                # fold. Artifact versions are themselves a projection of the log.
                inner = p.get("payload") or {}
                art = p.get("artifact") or inner.get("artifact")
                artifacts[art] = p.get("version", inner.get("version",
                                                             artifacts.get(art, 0) + 1))
            elif t == MessageType.PLAN_CREATED:
                for spec in p.get("tasks", []):
                    tasks[spec["task_id"]] = {"task_id": spec["task_id"],
                                              "title": spec.get("title", ""),
                                              "role": spec.get("role", ""),
                                              "skills": list(spec.get("skills", [])),
                                              "owner": None, "status": "pending",
                                              "deps": set(spec.get("deps", [])),
                                              "produces": list(spec.get("produces", [])),
                                              "consumes": list(spec.get("consumes", [])),
                                              "est_work": float(spec.get("est_work", 1.0)),
                                              "claims": list(spec.get("claims", []))}
            elif t == MessageType.TASK_ASSIGNED:
                tid = r["task_id"] or p.get("task_id")
                who = p.get("owner") or r["target"]
                if tid in tasks:
                    tasks[tid]["owner"] = who
                    tasks[tid]["status"] = "assigned"
                    tasks[tid]["started"] = False
                if who in agents:
                    # A recovered kernel must know which tasks an agent is *holding*, not just who
                    # owns them - without this, a resume replayed the org correctly and then did
                    # nothing, because every agent looked idle with an empty backlog.
                    if agents[who].get("task_id") is None:
                        agents[who]["task_id"] = tid
                    elif tid not in agents[who].setdefault("task_queue", []):
                        agents[who].setdefault("task_queue", []).append(tid)
                # No state projection here: assignment is not a lifecycle change. The
                # WORKING / INITIALIZING edges arrive as their own journaled STATE_TRANSITION
                # rows, so a replay cannot invent a state the live kernel never reached.
            elif t == MessageType.TASK_STARTED:
                tid = r["task_id"] or p.get("task_id")
                if tid in tasks:
                    tasks[tid]["status"] = "running"
            elif t == MessageType.TASK_COMPLETED:
                tid = r["task_id"] or p.get("task_id")
                who = p.get("owner") or r["actor"]
                if tid in tasks:
                    tasks[tid]["status"] = "done"
                if who in agents:
                    # release the hat exactly as the live actor does, then advance its backlog;
                    # a queue-empty agent is COMPLETED, one with work left is still WORKING
                    agents[who]["task_id"] = None
                    q = [x for x in (agents[who].get("task_queue") or []) if x != tid]
                    if q:
                        agents[who]["task_id"] = q[0]
                        agents[who]["task_queue"] = q[1:]
                    else:
                        agents[who]["task_queue"] = []
                        # NB: state is NOT projected here. Lifecycle edges come from
                        # STATE_TRANSITION rows only; inventing one here is what made a replay
                        # claim COMPLETED for an agent the live kernel left in IDLE.
            elif t == MessageType.TASK_FAILED:
                tid = r["task_id"] or p.get("task_id")
                if tid in tasks:
                    tasks[tid]["status"] = "failed"
            elif t == MessageType.TASK_VERIFIED:
                # `Kernel.verify_task` sets `t.verified` in memory, and this projection is the only
                # thing a replayed process has. Without the branch here a finished, verified project
                # replayed with `verified: false` on every task - which made `arena-code verify`
                # (a fresh process, reading only the journal) contradict the run that had just
                # passed its own checks. The event carries the verdict, so the fold can be faithful.
                tid = r["task_id"] or p.get("task_id")
                if tid in tasks:
                    tasks[tid]["verified"] = bool(p.get("verified"))
                    tasks[tid]["verified_at"] = r["ts"]
            elif t in (MessageType.PLAN_AMENDED, MessageType.REPLAN, MessageType.GRAPH_AMENDED):
                # Phase 2: GRAPH_AMENDED is what the spawn path writes. Projecting only
                # PLAN_AMENDED here meant a mid-run-created task vanished on replay while the
                # agent that owned it came back - a kernel resuming with an agent holding no work.
                # Same payload shape (added_tasks/added_deps), so one branch serves both.
                for spec in p.get("added_tasks", []):
                    tasks.setdefault(spec["task_id"], {
                        "task_id": spec["task_id"], "title": spec.get("title", ""),
                        "role": spec.get("role", ""), "skills": list(spec.get("skills", [])),
                        "owner": None, "status": "pending", "deps": set(spec.get("deps", [])),
                        "produces": list(spec.get("produces", [])),
                        "consumes": list(spec.get("consumes", [])),
                        "est_work": float(spec.get("est_work", 1.0)),
                        "claims": list(spec.get("claims", []))})
                for tid, new_deps in (p.get("added_deps") or {}).items():
                    if tid in tasks:
                        tasks[tid]["deps"] |= set(new_deps)
            elif t == MessageType.RESOURCE_UPDATED:
                pass  # handled above, next to the artifact projection

            # ---- Phase 2: the spawn-request lifecycle, one row per state change ----
            elif t == MessageType.SPAWN_REQUEST_RECEIVED:
                rid = p.get("rid", "")
                requests[rid] = {
                    "rid": rid, "requester": p.get("requester", actor),
                    "requested_role": p.get("requested_role", ""),
                    "capability_class": p.get("capability_class", "general"),
                    "reason": p.get("body", ""),
                    "estimated_work": float(p.get("estimated_work", 0.0) or 0.0),
                    "required_skills": list(p.get("required_skills", [])),
                    "required_inputs": list(p.get("required_inputs", [])),
                    "expected_outputs": list(p.get("expected_outputs", [])),
                    "parent_task_id": p.get("task_id"),
                    "correlation_id": p.get("correlation_id", ""),
                    "fingerprint": p.get("fingerprint", ""),
                    "at_tick": int(p.get("at_tick", 0) or 0),
                    "state": "RECEIVED", "rule": "", "owner": None,
                    "spawned_agent_id": None, "task_id": None, "seq": r["seq"]}
            elif t == MessageType.SPAWN_REQUEST_RESOLVED:
                ent = requests.setdefault(p.get("rid", ""), {
                    "rid": p.get("rid", ""), "requester": actor, "requested_role": "",
                    "capability_class": "general", "reason": p.get("body", ""),
                    "estimated_work": 0.0, "required_skills": [], "required_inputs": [],
                    "expected_outputs": [], "parent_task_id": p.get("task_id"),
                    "correlation_id": p.get("correlation_id", ""), "fingerprint": "",
                    "at_tick": 0, "state": "RECEIVED", "rule": "", "owner": None,
                    "spawned_agent_id": None, "task_id": None, "seq": r["seq"]})
                rule = p.get("rule", "")
                ent["rule"] = rule
                ent["owner"] = p.get("owner")
                if p.get("task_id"):
                    ent["task_id"] = p["task_id"]
                ent["state"] = ("APPROVED_COMMITTED" if rule == "APPROVE" and p.get("ok")
                                else "REROUTED" if rule == "REUSE_EXISTING" else "REJECTED")
                ent["resolved_seq"] = r["seq"]
            elif t == MessageType.SPAWN_APPROVED:
                ent = requests.get(p.get("rid", ""))
                if ent is not None:
                    # task_id lives in its own column, not inside the payload
                    ent.update({"state": "APPROVED_COMMITTED", "rule": "APPROVE",
                                "spawned_agent_id": p.get("agent_id"),
                                "task_id": (p.get("approved_task_id") or r["task_id"]
                                            or ent.get("task_id"))})
            elif t == MessageType.REQUEST_REROUTED:
                ent = requests.get(p.get("rid", ""))
                if ent is not None:
                    ent.update({"state": "REROUTED", "rule": "REUSE_EXISTING",
                                "owner": p.get("owner"),
                                "task_id": p.get("rerouted_task_id") or ent.get("task_id")})
            elif t == MessageType.DEFERRED_FOR_CAPACITY:
                ent = requests.get(p.get("rid", ""))
                if ent is not None:
                    ent.update({"state": "DEFERRED", "rule": "DEFER_FOR_CAPACITY",
                                "task_id": p.get("task_id")})
            elif t == MessageType.SPAWN_ESCALATED:
                ent = requests.get(p.get("rid", ""))
                if ent is not None:
                    ent.update({"state": "ESCALATED", "rule": "ESCALATE"})

        waits = {w["wait_id"]: w for w in self._conn.execute(
            "SELECT * FROM waits WHERE state='WAITING'")}
        last = self._conn.execute("SELECT ts FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        for spec in tasks.values():
            spec["deps"] = sorted(spec["deps"])
            spec["skills"] = sorted(spec.get("skills", []))
        return {"agents": agents, "tasks": tasks, "waits": waits, "artifacts": artifacts,
                "requests": requests,
                "claims": self.claims(), "events": self.count(),
                "meta": {"tick_ts": last["ts"] if last else 0.0}}

    def iterate(self) -> Iterator[dict[str, Any]]:
        for r in self._conn.execute("SELECT * FROM events ORDER BY seq"):
            yield self._rowdict(r)
