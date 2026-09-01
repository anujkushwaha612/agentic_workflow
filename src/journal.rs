//! Event-sourced journal: the only mutable-truth source (`arena/journal.py`).
//!
//! Byte-compatible with the Python implementation: same SQLite schema, same
//! PRAGMAs, same canonical JSON hash chain (see [`crate::sys::json`]), so Rust
//! reads/replays/extends Python journals and Python verifies Rust ones.
//! `fold()` is the pure event→state projection used by crash recovery.

use crate::ids::{AgentId, ClaimKey, WaitId};
use crate::msg::{EventType, Message};
use crate::sys::json::{parse, JMap, JValue};
use crate::sys::sha256::Sha256;
use crate::sys::sqlite::{Db, Param};
use std::path::{Path, PathBuf};

pub const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

pub const SCHEMA: &str = r#"
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
"#;

/// One journal row as read back from SQLite.
#[derive(Debug, Clone)]
pub struct EventRow {
    pub seq: i64,
    pub ts: f64,
    pub etype: String,
    pub plane: String,
    pub topic: String,
    pub actor: String,
    pub target: Option<String>,
    pub task_id: Option<String>,
    pub resource: Option<String>,
    pub correlation_id: Option<String>,
    pub caused_by: Option<String>,
    pub depth: i64,
    pub payload_text: String,
    pub hash: String,
    pub prev_hash: String,
}

impl EventRow {
    pub fn payload(&self) -> JValue {
        parse(&self.payload_text).unwrap_or(JValue::Null)
    }
    /// Python `_rowdict`: all columns + parsed payload + hoisted `body`.
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("seq".into(), JValue::Int(self.seq));
        m.insert("ts".into(), JValue::Float(self.ts));
        m.insert("etype".into(), JValue::Str(self.etype.clone()));
        m.insert("plane".into(), JValue::Str(self.plane.clone()));
        m.insert("topic".into(), JValue::Str(self.topic.clone()));
        m.insert("actor".into(), JValue::Str(self.actor.clone()));
        m.insert(
            "target".into(),
            self.target
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "task_id".into(),
            self.task_id
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "resource".into(),
            self.resource
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "correlation_id".into(),
            self.correlation_id
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "caused_by".into(),
            self.caused_by
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m.insert("depth".into(), JValue::Int(self.depth));
        m.insert("hash".into(), JValue::Str(self.hash.clone()));
        m.insert("prev_hash".into(), JValue::Str(self.prev_hash.clone()));
        let payload = self.payload();
        let body = payload
            .get("body")
            .and_then(JValue::as_str)
            .unwrap_or("")
            .to_string();
        m.insert("payload".into(), payload);
        m.insert("body".into(), JValue::Str(body));
        m
    }
    pub fn body(&self) -> String {
        self.payload()
            .get("body")
            .and_then(JValue::as_str)
            .unwrap_or("")
            .to_string()
    }
}

#[derive(Debug, Clone)]
pub struct WaitRow {
    pub wait_id: WaitId,
    pub agent_id: AgentId,
    pub condition: String,
    pub task_id: Option<String>,
    pub correlation_id: Option<String>,
    pub armed_at: f64,
    pub timeout_at: Option<f64>,
    pub state: String,
    pub wake_reason: Option<String>,
}

impl WaitRow {
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("wait_id".into(), JValue::Str(self.wait_id.as_str().into()));
        m.insert(
            "agent_id".into(),
            JValue::Str(self.agent_id.as_str().into()),
        );
        m.insert("condition".into(), JValue::Str(self.condition.clone()));
        m.insert(
            "task_id".into(),
            self.task_id
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "correlation_id".into(),
            self.correlation_id
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m.insert("armed_at".into(), JValue::Float(self.armed_at));
        m.insert(
            "timeout_at".into(),
            self.timeout_at.map(JValue::Float).unwrap_or(JValue::Null),
        );
        m.insert("state".into(), JValue::Str(self.state.clone()));
        m.insert(
            "wake_reason".into(),
            self.wake_reason
                .as_deref()
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        m
    }
}

/// The Python `_hash_row` byte format: sha256(prev_hash ‖ canonical_json(row)).
fn hash_row(prev_hash: &str, row: &RowTuple<'_>) -> String {
    let mut h = Sha256::new();
    h.update(prev_hash.as_bytes());
    // json.dumps(row, sort_keys=True, separators=(",", ":")) — a 12-item list
    let mut out = String::new();
    out.push('[');
    out.push_str(&crate::sys::json::py_float_repr(row.ts));
    out.push(',');
    out.push_str(&JValue::Str(row.etype.into()).to_canon_string());
    out.push(',');
    out.push_str(&JValue::Str(row.plane.into()).to_canon_string());
    out.push(',');
    out.push_str(&JValue::Str(row.topic.into()).to_canon_string());
    out.push(',');
    out.push_str(&JValue::Str(row.actor.into()).to_canon_string());
    out.push(',');
    out.push_str(&opt_str(row.target));
    out.push(',');
    out.push_str(&opt_str(row.task_id));
    out.push(',');
    out.push_str(&opt_str(row.resource));
    out.push(',');
    out.push_str(&opt_str(row.correlation_id));
    out.push(',');
    out.push_str(&opt_str(row.caused_by));
    out.push(',');
    out.push_str(&row.depth.to_string());
    out.push(',');
    out.push_str(&JValue::Str(row.payload.into()).to_canon_string());
    out.push(']');
    h.update(out.as_bytes());
    crate::sys::sha256::to_hex(&h.finalize())
}

fn opt_str(v: Option<&str>) -> String {
    match v {
        Some(s) => JValue::Str(s.into()).to_canon_string(),
        None => "null".into(),
    }
}

struct RowTuple<'a> {
    ts: f64,
    etype: &'a str,
    plane: &'a str,
    topic: &'a str,
    actor: &'a str,
    target: Option<&'a str>,
    task_id: Option<&'a str>,
    resource: Option<&'a str>,
    correlation_id: Option<&'a str>,
    caused_by: Option<&'a str>,
    depth: i64,
    payload: &'a str,
}

#[derive(Debug)]
pub enum JournalError {
    Sql(crate::sys::sqlite::SqlError),
    Io(String),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Sql(e) => write!(f, "journal sql error: {e}"),
            JournalError::Io(e) => write!(f, "journal io error: {e}"),
        }
    }
}
impl std::error::Error for JournalError {}
impl From<crate::sys::sqlite::SqlError> for JournalError {
    fn from(e: crate::sys::sqlite::SqlError) -> Self {
        JournalError::Sql(e)
    }
}

/// Append-only log + fold projections. Single owner by design (as in Python).
pub struct Journal {
    pub path: JournalPath,
    pub fsync_every: u64,
    now_override: Option<f64>,
    conn: Option<Db>,
    appends: u64,
    last_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JournalPath {
    Memory,
    File(PathBuf),
}

impl JournalPath {
    pub fn is_memory(&self) -> bool {
        matches!(self, JournalPath::Memory)
    }
}

impl Journal {
    pub fn open_memory() -> Result<Journal, JournalError> {
        Journal::open(JournalPath::Memory, None, 1)
    }

    pub fn open_file(path: impl AsRef<Path>, now_fn: Option<f64>) -> Result<Journal, JournalError> {
        let p = path.as_ref().to_path_buf();
        if let Some(parent) = p.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| JournalError::Io(format!("mkdir {}: {e}", parent.display())))?;
            }
        }
        Journal::open(JournalPath::File(p), now_fn, 1)
    }

    fn open(
        path: JournalPath,
        now_override: Option<f64>,
        fsync_every: u64,
    ) -> Result<Journal, JournalError> {
        let db = match &path {
            JournalPath::Memory => Db::open(Path::new(":memory:"))?,
            JournalPath::File(p) => Db::open(p)?,
        };
        let mut j = Journal {
            path,
            fsync_every,
            now_override,
            conn: Some(db),
            appends: 0,
            last_hash: GENESIS.to_string(),
        };
        let conn = j.conn.as_ref().expect("conn");
        conn.exec("PRAGMA journal_mode=WAL")?;
        conn.exec("PRAGMA synchronous=FULL")?;
        conn.exec("PRAGMA busy_timeout=5000")?;
        conn.exec(SCHEMA)?;
        j.last_hash = j.load_last_hash()?;
        Ok(j)
    }

    fn conn(&self) -> &Db {
        self.conn.as_ref().expect("journal connection")
    }

    pub fn now(&self) -> f64 {
        self.now_override.unwrap_or(0.0)
    }
    pub fn set_now_provider(&mut self, now: f64) {
        self.now_override = Some(now);
    }

    fn load_last_hash(&self) -> Result<String, JournalError> {
        let rows = self
            .conn()
            .run("SELECT hash FROM events ORDER BY seq DESC LIMIT 1", &[])?;
        Ok(rows
            .first()
            .map(|r| r.text("hash"))
            .unwrap_or_else(|| GENESIS.to_string()))
    }

    // ------------------------------------------------------------------ append
    pub fn append(&mut self, msg: &mut Message) -> Message {
        let mut payload = msg.payload.clone();
        if !payload.contains_key("body") {
            payload.insert("body".into(), JValue::Str(msg.body.clone()));
        }
        let ts = if msg.ts != 0.0 { msg.ts } else { self.now() };
        let payload_text = JValue::Obj(payload.clone()).to_canon_string();
        let row = RowTuple {
            ts,
            etype: msg.msg_type.as_str(),
            plane: msg.plane.as_str(),
            topic: &msg.topic,
            actor: msg.from_actor.as_str(),
            target: Some(msg.to_actor.as_str()),
            task_id: msg.task_id.as_deref(),
            resource: msg.resource.as_deref(),
            correlation_id: Some(msg.correlation_id.as_str()),
            caused_by: msg.caused_by.as_deref(),
            depth: msg.causal_depth,
            payload: &payload_text,
        };
        let prev = self.last_hash.clone();
        let h = hash_row(&prev, &row);
        let conn = self.conn.as_ref().expect("conn");
        let _ = conn.execute(
            "INSERT INTO events(ts,etype,plane,topic,actor,target,task_id,resource,correlation_id,\
             caused_by,depth,payload,hash,prev_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            &[
                Param::Real(ts),
                Param::Text(msg.msg_type.as_str().into()),
                Param::Text(msg.plane.as_str().into()),
                Param::Text(msg.topic.clone()),
                Param::Text(msg.from_actor.as_str().to_string()),
                Param::Text(msg.to_actor.as_str().to_string()),
                msg.task_id.as_ref().map(|t| t.as_str().to_string()).into(),
                msg.resource.clone().into(),
                Param::Text(msg.correlation_id.as_str().to_string()),
                msg.caused_by.clone().into(),
                Param::Int(msg.causal_depth),
                Param::Text(payload_text),
                Param::Text(h.clone()),
                Param::Text(prev),
            ],
        );
        msg.seq = conn.last_insert_rowid();
        msg.ts = ts;
        msg.payload = payload;
        self.last_hash = h;
        self.appends += 1;
        if self.fsync_every > 0
            && self.appends.is_multiple_of(self.fsync_every)
            && !self.path.is_memory()
        {
            let _ = self.conn().exec("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        msg.clone()
    }

    /// `Journal.emit` equivalent: build + append in one call.
    #[allow(clippy::too_many_arguments)] // arity mirrors Journal.emit in Python
    pub fn emit(
        &mut self,
        msg_type: EventType,
        actor: &str,
        target: &str,
        body: &str,
        fields: JMap,
        task_id: Option<&str>,
        resource: Option<&str>,
        correlation_id: Option<&str>,
        caused_by: Option<&str>,
        causal_depth: i64,
    ) -> Message {
        let mut m = Message::new(msg_type, actor, target);
        m.body = body.to_string();
        m.task_id = task_id.map(crate::ids::TaskId::new);
        m.resource = resource.map(|s| s.to_string());
        if let Some(c) = correlation_id {
            m.correlation_id = crate::ids::CorrelationId::new(c);
        }
        m.caused_by = caused_by.map(|s| s.to_string());
        m.causal_depth = causal_depth;
        m.payload = fields;
        self.append(&mut m)
    }

    // ------------------------------------------------------------------- read
    pub fn events(&self, filter: &EventFilter) -> Result<Vec<EventRow>, JournalError> {
        let mut sql = String::from("SELECT * FROM events WHERE seq > ?");
        let mut args: Vec<Param> = vec![Param::Int(filter.after_seq)];
        if let Some(a) = &filter.actor {
            sql.push_str(" AND actor=?");
            args.push(Param::Text(a.clone()));
        }
        if let Some(t) = &filter.task_id {
            sql.push_str(" AND task_id=?");
            args.push(Param::Text(t.clone()));
        }
        if let Some(e) = &filter.etype {
            sql.push_str(" AND etype=?");
            args.push(Param::Text(e.clone()));
        }
        if let Some(c) = &filter.correlation_id {
            sql.push_str(" AND correlation_id=?");
            args.push(Param::Text(c.clone()));
        }
        sql.push_str(" ORDER BY seq");
        if let Some(n) = filter.limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        Ok(self.conn().run(&sql, &args)?.iter().map(row_from).collect())
    }

    pub fn count(&self) -> Result<i64, JournalError> {
        let rows = self.conn().run("SELECT COUNT(*) as n FROM events", &[])?;
        Ok(rows.first().map(|r| r.int("n")).unwrap_or(0))
    }

    pub fn trace(&self, correlation_id: &str) -> Result<Vec<JMap>, JournalError> {
        Ok(self
            .events(&EventFilter::new().correlation(correlation_id))?
            .iter()
            .map(|r| r.to_dict())
            .collect())
    }

    pub fn iterate(&self) -> Result<Vec<EventRow>, JournalError> {
        self.events(&EventFilter::new())
    }

    pub fn latest_snapshot(&self) -> Result<Option<JValue>, JournalError> {
        let rows = self.conn().run(
            "SELECT payload FROM events WHERE etype='SNAPSHOT' ORDER BY seq DESC LIMIT 1",
            &[],
        )?;
        match rows.first() {
            None => Ok(None),
            Some(r) => {
                let p = parse(&r.text("payload")).unwrap_or(JValue::Null);
                Ok(p.get("snapshot").cloned())
            }
        }
    }

    // ------------------------------------------------------- chain integrity
    pub fn verify_chain(&self) -> Result<(bool, String, i64), JournalError> {
        let rows = self.conn().run("SELECT * FROM events ORDER BY seq", &[])?;
        let mut prev = GENESIS.to_string();
        for r in &rows {
            let row = row_from(r);
            let tuple = RowTuple {
                ts: row.ts,
                etype: &row.etype,
                plane: &row.plane,
                topic: &row.topic,
                actor: &row.actor,
                target: row.target.as_deref(),
                task_id: row.task_id.as_deref(),
                resource: row.resource.as_deref(),
                correlation_id: row.correlation_id.as_deref(),
                caused_by: row.caused_by.as_deref(),
                depth: row.depth,
                payload: &row.payload_text,
            };
            let exp = hash_row(&prev, &tuple);
            if exp != row.hash {
                return Ok((
                    false,
                    format!("hash mismatch (torn/tampered row) at seq {}", row.seq),
                    row.seq,
                ));
            }
            if row.prev_hash != prev {
                return Ok((
                    false,
                    format!("chain link broken at seq {}", row.seq),
                    row.seq,
                ));
            }
            prev = row.hash.clone();
        }
        Ok((true, String::new(), -1))
    }

    /// Simulate a truncated tail after a crash (chaos suite).
    pub fn truncate_after(&mut self, seq: i64) -> Result<i64, JournalError> {
        self.conn()
            .execute("DELETE FROM events WHERE seq > ?", &[Param::Int(seq)])?;
        self.last_hash = self.load_last_hash()?;
        self.count()
    }

    // ------------------------------------------------------------- durable waits
    #[allow(clippy::too_many_arguments)]
    pub fn arm_wait(
        &self,
        wait_id: &str,
        agent_id: &str,
        condition: &str,
        task_id: Option<&str>,
        correlation_id: &str,
        armed_at: f64,
        timeout_at: Option<f64>,
    ) -> Result<(), JournalError> {
        self.conn().execute(
            "INSERT OR REPLACE INTO waits(wait_id,agent_id,condition,task_id,correlation_id,\
             armed_at,timeout_at,state,wake_reason) VALUES(?,?,?,?,?,?,?,'WAITING',NULL)",
            &[
                Param::Text(wait_id.into()),
                Param::Text(agent_id.into()),
                Param::Text(condition.into()),
                task_id.map(|s| s.to_string()).into(),
                Param::Text(correlation_id.into()),
                Param::Real(armed_at),
                timeout_at.into(),
            ],
        )?;
        Ok(())
    }

    pub fn resolve_wait(
        &self,
        wait_id: &str,
        state: &str,
        wake_reason: &str,
    ) -> Result<(), JournalError> {
        self.conn().execute(
            "UPDATE waits SET state=?, wake_reason=? WHERE wait_id=?",
            &[
                Param::Text(state.into()),
                Param::Text(wake_reason.into()),
                Param::Text(wait_id.into()),
            ],
        )?;
        Ok(())
    }

    pub fn active_waits(&self, condition: Option<&str>) -> Result<Vec<WaitRow>, JournalError> {
        let rows = match condition {
            Some(c) => self.conn().run(
                "SELECT * FROM waits WHERE state='WAITING' AND condition=?",
                &[Param::Text(c.into())],
            )?,
            None => self
                .conn()
                .run("SELECT * FROM waits WHERE state='WAITING'", &[])?,
        };
        Ok(rows.iter().map(wait_row_from).collect())
    }

    // ------------------------------------------------------------------ claims
    /// Atomic first-writer-wins. Returns (acquired, incumbent).
    pub fn claim(
        &self,
        key: &str,
        owner: &str,
        task_id: Option<&str>,
        ts: f64,
    ) -> Result<(bool, String), JournalError> {
        let res = self.conn().execute(
            "INSERT INTO claims(claim_key, first_owner, task_id, ts) VALUES(?,?,?,?)",
            &[
                Param::Text(key.into()),
                Param::Text(owner.into()),
                task_id.map(|s| s.to_string()).into(),
                Param::Real(ts),
            ],
        );
        match res {
            Ok(_) => Ok((true, String::new())),
            Err(e) if e.is_constraint => {
                let rows = self.conn().run(
                    "SELECT first_owner, losers FROM claims WHERE claim_key=?",
                    &[Param::Text(key.into())],
                )?;
                let incumbent = rows
                    .first()
                    .map(|r| r.text("first_owner"))
                    .unwrap_or_else(|| "?".into());
                let losers_text = rows
                    .first()
                    .map(|r| r.text("losers"))
                    .unwrap_or_else(|| "[]".into());
                let mut losers = parse(&losers_text).unwrap_or(JValue::Arr(vec![]));
                if let JValue::Arr(v) = &mut losers {
                    v.push(JValue::Str(owner.into()));
                }
                let _ = self.conn().execute(
                    "UPDATE claims SET losers=? WHERE claim_key=?",
                    &[
                        Param::Text(losers.to_canon_string()),
                        Param::Text(key.into()),
                    ],
                );
                Ok((false, incumbent))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn claims(&self) -> Result<BTreeMap_<ClaimKey, JValue>, JournalError> {
        let rows = self.conn().run("SELECT * FROM claims", &[])?;
        let mut out = BTreeMap_::new();
        for r in rows.iter() {
            let d = r.text("claim_key");
            let losers = parse(&r.text("losers")).unwrap_or(JValue::Arr(vec![]));
            let mut m = JMap::new();
            m.insert("owner".into(), JValue::Str(r.text("first_owner")));
            m.insert(
                "task_id".into(),
                r.opt_text("task_id")
                    .map(JValue::Str)
                    .unwrap_or(JValue::Null),
            );
            m.insert("losers".into(), losers);
            out.insert(ClaimKey::new(d), JValue::Obj(m));
        }
        Ok(out)
    }

    // ------------------------------------------------------------------- fold
    /// Rebuild live state from the log alone.
    pub fn fold(&self) -> Result<JMap, JournalError> {
        let mut f = FoldState::default();
        for r in self.conn().run("SELECT * FROM events ORDER BY seq", &[])? {
            fold_event(&mut f, &row_from(&r));
        }
        // waits
        let mut waits = JMap::new();
        for w in self.active_waits(None)? {
            waits.insert(w.wait_id.as_str().to_string(), JValue::Obj(w.to_dict()));
        }
        let waits = JValue::Obj(waits);
        let last_ts = self
            .conn()
            .run("SELECT ts FROM events ORDER BY seq DESC LIMIT 1", &[])?
            .first()
            .map(|r| r.real("ts"))
            .unwrap_or(0.0);

        let mut agents = JMap::new();
        for (k, v) in f.agents {
            agents.insert(k.as_str().to_string(), JValue::Obj(v));
        }
        let mut tasks = JMap::new();
        for (k, mut v) in f.tasks {
            if let Some(deps) = v.get("deps").cloned() {
                v.insert("deps".into(), sort_jv_list(&deps));
            }
            if let Some(sk) = v.get("skills").cloned() {
                v.insert("skills".into(), sort_jv_list(&sk));
            }
            tasks.insert(k.as_str().to_string(), JValue::Obj(v));
        }
        let mut requests = JMap::new();
        for (k, v) in f.requests {
            requests.insert(k, v);
        }
        let mut out = JMap::new();
        out.insert("agents".into(), JValue::Obj(agents));
        out.insert("tasks".into(), JValue::Obj(tasks));
        out.insert("waits".into(), waits);
        out.insert("artifacts".into(), JValue::Obj(f.artifacts));
        out.insert("requests".into(), JValue::Obj(requests));
        out.insert(
            "claims".into(),
            JValue::Obj(
                self.claims()?
                    .into_iter()
                    .map(|(k, v)| (k.as_str().to_string(), v))
                    .collect(),
            ),
        );
        out.insert("events".into(), JValue::Int(self.count()?));
        let mut meta = JMap::new();
        meta.insert("tick_ts".into(), JValue::Float(last_ts));
        out.insert("meta".into(), JValue::Obj(meta));
        Ok(out)
    }

    pub fn close(&mut self) {
        self.conn = None;
    }
}

pub type BTreeMap_<K, V> = std::collections::BTreeMap<K, V>;

#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    pub actor: Option<String>,
    pub task_id: Option<String>,
    pub etype: Option<String>,
    pub correlation_id: Option<String>,
    pub after_seq: i64,
    pub limit: Option<i64>,
}

impl EventFilter {
    pub fn new() -> EventFilter {
        EventFilter::default()
    }
    pub fn actor(mut self, a: impl Into<String>) -> Self {
        self.actor = Some(a.into());
        self
    }
    pub fn task(mut self, t: impl Into<String>) -> Self {
        self.task_id = Some(t.into());
        self
    }
    pub fn etype(mut self, e: impl Into<String>) -> Self {
        self.etype = Some(e.into());
        self
    }
    pub fn correlation(mut self, c: impl Into<String>) -> Self {
        self.correlation_id = Some(c.into());
        self
    }
    pub fn after(mut self, seq: i64) -> Self {
        self.after_seq = seq;
        self
    }
    pub fn limit(mut self, n: i64) -> Self {
        self.limit = Some(n);
        self
    }
}

fn row_from(r: &crate::sys::sqlite::Row) -> EventRow {
    EventRow {
        seq: r.int("seq"),
        ts: r.real("ts"),
        etype: r.text("etype"),
        plane: r.text("plane"),
        topic: r.text("topic"),
        actor: r.text("actor"),
        target: r.opt_text("target"),
        task_id: r.opt_text("task_id"),
        resource: r.opt_text("resource"),
        correlation_id: r.opt_text("correlation_id"),
        caused_by: r.opt_text("caused_by"),
        depth: r.int("depth"),
        payload_text: r.text("payload"),
        hash: r.text("hash"),
        prev_hash: r.text("prev_hash"),
    }
}

fn wait_row_from(r: &crate::sys::sqlite::Row) -> WaitRow {
    WaitRow {
        wait_id: WaitId::new(r.text("wait_id")),
        agent_id: AgentId::new(r.text("agent_id")),
        condition: r.text("condition"),
        task_id: r.opt_text("task_id"),
        correlation_id: r.opt_text("correlation_id"),
        armed_at: r.real("armed_at"),
        timeout_at: if r.is_null("timeout_at") {
            None
        } else {
            Some(r.real("timeout_at"))
        },
        state: r.text("state"),
        wake_reason: r.opt_text("wake_reason"),
    }
}

fn sort_jv_list(v: &JValue) -> JValue {
    match v {
        JValue::Arr(items) => {
            let mut strs: Vec<String> = items
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
            strs.sort();
            JValue::Arr(strs.into_iter().map(JValue::Str).collect())
        }
        other => other.clone(),
    }
}

// --------------------------------------------------------------------- fold
// A faithful port of Journal.fold()'s per-event projection (arena/journal.py).

#[derive(Default)]
struct FoldState {
    agents: std::collections::BTreeMap<AgentId, JMap>,
    tasks: std::collections::BTreeMap<crate::ids::TaskId, JMap>,
    artifacts: JMap,
    requests: JMap,
}

fn jstr(v: Option<&str>) -> JValue {
    v.map(|s| JValue::Str(s.into())).unwrap_or(JValue::Null)
}

fn fold_task_entry(spec: &JValue) -> JMap {
    let mut m = JMap::new();
    m.insert(
        "task_id".into(),
        spec.get("task_id").cloned().unwrap_or(JValue::Null),
    );
    m.insert("title".into(), spec.str_or("title", "").into());
    m.insert("role".into(), spec.str_or("role", "").into());
    m.insert(
        "skills".into(),
        spec.get("skills").cloned().unwrap_or(JValue::Arr(vec![])),
    );
    m.insert("owner".into(), JValue::Null);
    m.insert("status".into(), JValue::Str("pending".into()));
    let deps = match spec.get("deps") {
        Some(JValue::Arr(v)) => v.clone(),
        _ => vec![],
    };
    m.insert("deps".into(), JValue::Arr(deps));
    m.insert(
        "produces".into(),
        spec.get("produces").cloned().unwrap_or(JValue::Arr(vec![])),
    );
    m.insert(
        "consumes".into(),
        spec.get("consumes").cloned().unwrap_or(JValue::Arr(vec![])),
    );
    m.insert(
        "est_work".into(),
        JValue::Float(spec.get("est_work").and_then(JValue::as_f64).unwrap_or(1.0)),
    );
    m.insert(
        "claims".into(),
        spec.get("claims").cloned().unwrap_or(JValue::Arr(vec![])),
    );
    m
}

fn fold_event(state: &mut FoldState, row: &EventRow) {
    use crate::msg::EventType as E;
    let Some(t) = EventType::parse(&row.etype) else {
        return;
    };
    let p = row.payload();
    let actor = row.actor.as_str();
    let target = row.target.as_deref();
    match t {
        E::AgentRegistered => {
            let aid = p
                .get("agent_id")
                .and_then(JValue::as_str)
                .map(|s| s.to_string())
                .or_else(|| target.map(|s| s.to_string()))
                .unwrap_or_default();
            let mut m = JMap::new();
            m.insert("agent_id".into(), JValue::Str(aid.clone()));
            m.insert("role".into(), JValue::Str(p.str_or("role", "?")));
            m.insert(
                "skills".into(),
                p.get("skills").cloned().unwrap_or(JValue::Arr(vec![])),
            );
            m.insert("state".into(), JValue::Str("CREATED".into()));
            m.insert("state_since".into(), JValue::Float(row.ts));
            m.insert("transitions".into(), JValue::Int(0));
            m.insert(
                "epoch".into(),
                JValue::Int(p.get("epoch").and_then(JValue::as_int).unwrap_or(0)),
            );
            m.insert(
                "spawned_by".into(),
                JValue::Str(p.str_or("spawned_by", "parent")),
            );
            m.insert("terminated_reason".into(), JValue::Null);
            m.insert("task_id".into(), JValue::Null);
            m.insert("task_queue".into(), JValue::Arr(vec![]));
            m.insert("msgs_sent".into(), JValue::Int(0));
            m.insert("work_done".into(), JValue::Float(0.0));
            m.insert("steps_run".into(), JValue::Int(0));
            state.agents.insert(AgentId::new(aid), m);
        }
        E::StateTransition => {
            let aid = if state.agents.contains_key(&AgentId::new(actor)) {
                actor.to_string()
            } else {
                p.str_or("agent_id", "")
            };
            if let Some(a) = state.agents.get_mut(&AgentId::new(aid)) {
                if let Some(to) = p.get("to").and_then(JValue::as_str) {
                    a.insert("state".into(), JValue::Str(to.to_string()));
                }
                a.insert("state_since".into(), JValue::Float(row.ts));
                let t = a.get("transitions").and_then(JValue::as_int).unwrap_or(0) + 1;
                a.insert("transitions".into(), JValue::Int(t));
            }
        }
        E::AgentPaused => {
            let aid = p.str_or("agent_id", target.unwrap_or(""));
            if let Some(a) = state.agents.get_mut(&AgentId::new(aid)) {
                a.insert("state".into(), JValue::Str("PAUSED".into()));
                a.insert("state_since".into(), JValue::Float(row.ts));
            }
        }
        E::AgentResumed => {
            let aid = p.str_or("agent_id", target.unwrap_or(""));
            if let Some(a) = state.agents.get_mut(&AgentId::new(aid)) {
                a.insert("state".into(), JValue::Str("IDLE".into()));
                a.insert("state_since".into(), JValue::Float(row.ts));
            }
        }
        E::AgentTerminated => {
            let aid = p.str_or("agent_id", target.unwrap_or(""));
            if let Some(a) = state.agents.get_mut(&AgentId::new(aid)) {
                a.insert("state".into(), JValue::Str("TERMINATED".into()));
                a.insert(
                    "terminated_reason".into(),
                    p.get("reason").cloned().unwrap_or(JValue::Null),
                );
            }
        }
        E::TaskProgress => {
            if target == Some("parent")
                && p.get("steps_run").is_some()
                && state.agents.contains_key(&AgentId::new(actor))
            {
                if let Some(a) = state.agents.get_mut(&AgentId::new(actor)) {
                    a.insert(
                        "steps_run".into(),
                        p.get("steps_run").cloned().unwrap_or(JValue::Int(0)),
                    );
                    a.insert(
                        "policy_cursor".into(),
                        p.get("policy_cursor").cloned().unwrap_or(JValue::Int(0)),
                    );
                }
            }
        }
        E::Stats => {
            if let Some(a) = state.agents.get_mut(&AgentId::new(actor)) {
                a.insert(
                    "msgs_sent".into(),
                    p.get("msgs_sent").cloned().unwrap_or(JValue::Int(0)),
                );
                a.insert(
                    "work_done".into(),
                    JValue::Float(p.get("work_done").and_then(JValue::as_f64).unwrap_or(0.0)),
                );
                let candidate = p.get("steps_run").and_then(JValue::as_int).unwrap_or(0);
                let existing = a.get("steps_run").and_then(JValue::as_int).unwrap_or(0);
                a.insert("steps_run".into(), JValue::Int(candidate.max(existing)));
            }
        }
        E::ResourceUpdated => {
            let art = p
                .get("artifact")
                .and_then(JValue::as_str)
                .map(|s| s.to_string())
                .or_else(|| {
                    p.get("payload")
                        .and_then(|inner| inner.get("artifact"))
                        .and_then(JValue::as_str)
                        .map(|s| s.to_string())
                });
            if let Some(art) = art {
                let version = p
                    .get("version")
                    .and_then(JValue::as_int)
                    .map(JValue::Int)
                    .or_else(|| {
                        p.get("payload")
                            .and_then(|inner| inner.get("version"))
                            .cloned()
                    })
                    .unwrap_or_else(|| {
                        JValue::Int(
                            state
                                .artifacts
                                .get(&art)
                                .and_then(JValue::as_int)
                                .unwrap_or(0)
                                + 1,
                        )
                    });
                state.artifacts.insert(art, version);
            }
        }
        E::PlanCreated => {
            if let Some(JValue::Arr(specs)) = p.get("tasks") {
                for spec in specs.clone() {
                    let tid = spec.str_or("task_id", "");
                    state
                        .tasks
                        .insert(crate::ids::TaskId::new(tid), fold_task_entry(&spec));
                }
            }
        }
        E::TaskAssigned => {
            let tid = row
                .task_id
                .clone()
                .or_else(|| {
                    p.get("task_id")
                        .and_then(JValue::as_str)
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            let who = p
                .get("owner")
                .and_then(JValue::as_str)
                .map(|s| s.to_string())
                .or_else(|| target.map(|s| s.to_string()))
                .unwrap_or_default();
            if let Some(task) = state.tasks.get_mut(&crate::ids::TaskId::new(tid.clone())) {
                task.insert("owner".into(), JValue::Str(who.clone()));
                task.insert("status".into(), JValue::Str("assigned".into()));
                task.insert("started".into(), JValue::Bool(false));
            }
            if let Some(a) = state.agents.get_mut(&AgentId::new(who.clone())) {
                let cur = a.get("task_id").cloned();
                match cur {
                    Some(JValue::Null) | None => {
                        a.insert("task_id".into(), JValue::Str(tid.clone()));
                    }
                    Some(JValue::Str(current)) if current != tid => {
                        let q = a.get("task_queue").cloned().unwrap_or(JValue::Arr(vec![]));
                        if let JValue::Arr(mut v) = q {
                            if !v.iter().any(|x| x.as_str() == Some(tid.as_str())) {
                                v.push(JValue::Str(tid.clone()));
                            }
                            a.insert("task_queue".into(), JValue::Arr(v));
                        }
                    }
                    _ => {}
                }
            }
        }
        E::TaskStarted => {
            let tid = row
                .task_id
                .clone()
                .or_else(|| {
                    p.get("task_id")
                        .and_then(JValue::as_str)
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            if let Some(task) = state.tasks.get_mut(&crate::ids::TaskId::new(tid)) {
                task.insert("status".into(), JValue::Str("running".into()));
            }
        }
        E::TaskCompleted => {
            let tid = row
                .task_id
                .clone()
                .or_else(|| {
                    p.get("task_id")
                        .and_then(JValue::as_str)
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            let who = p
                .get("owner")
                .and_then(JValue::as_str)
                .map(|s| s.to_string())
                .unwrap_or_else(|| actor.to_string());
            if let Some(task) = state.tasks.get_mut(&crate::ids::TaskId::new(tid.clone())) {
                task.insert("status".into(), JValue::Str("done".into()));
            }
            if let Some(a) = state.agents.get_mut(&AgentId::new(who)) {
                let q = a.get("task_queue").cloned().unwrap_or(JValue::Arr(vec![]));
                let mut qv = match q {
                    JValue::Arr(v) => v,
                    _ => vec![],
                };
                qv.retain(|x| x.as_str() != Some(tid.as_str()));
                if !qv.is_empty() {
                    a.insert("task_id".into(), qv[0].clone());
                    a.insert("task_queue".into(), JValue::Arr(qv[1..].to_vec()));
                } else {
                    a.insert("task_id".into(), JValue::Null);
                    a.insert("task_queue".into(), JValue::Arr(vec![]));
                }
            }
        }
        E::TaskFailed => {
            let tid = row
                .task_id
                .clone()
                .or_else(|| {
                    p.get("task_id")
                        .and_then(JValue::as_str)
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            if let Some(task) = state.tasks.get_mut(&crate::ids::TaskId::new(tid)) {
                task.insert("status".into(), JValue::Str("failed".into()));
            }
        }
        E::TaskVerified => {
            let tid = row
                .task_id
                .clone()
                .or_else(|| {
                    p.get("task_id")
                        .and_then(JValue::as_str)
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            if let Some(task) = state.tasks.get_mut(&crate::ids::TaskId::new(tid)) {
                task.insert(
                    "verified".into(),
                    JValue::Bool(p.get("verified").and_then(JValue::as_bool).unwrap_or(false)),
                );
                task.insert("verified_at".into(), JValue::Float(row.ts));
            }
        }
        E::PlanAmended | E::Replan | E::GraphAmended => {
            if let Some(JValue::Arr(specs)) = p.get("added_tasks") {
                for spec in specs.clone() {
                    let tid = spec.str_or("task_id", "");
                    state
                        .tasks
                        .entry(crate::ids::TaskId::new(tid))
                        .or_insert_with(|| fold_task_entry(&spec));
                }
            }
            if let Some(JValue::Obj(deps)) = p.get("added_deps") {
                for (tid, new_deps) in deps.clone() {
                    if let Some(task) = state.tasks.get_mut(&crate::ids::TaskId::new(tid.clone())) {
                        let mut merged: Vec<JValue> = match task.get("deps").cloned() {
                            Some(JValue::Arr(v)) => v,
                            _ => vec![],
                        };
                        if let JValue::Arr(add) = new_deps {
                            for d in add {
                                if !merged.contains(&d) {
                                    merged.push(d);
                                }
                            }
                        }
                        task.insert("deps".into(), JValue::Arr(merged));
                    }
                }
            }
        }
        E::SpawnRequestReceived => {
            let rid = p.str_or("rid", "");
            let mut m = JMap::new();
            m.insert("rid".into(), JValue::Str(rid.clone()));
            m.insert(
                "requester".into(),
                jstr(p.get("requester").and_then(JValue::as_str).or(Some(actor))),
            );
            m.insert(
                "requested_role".into(),
                JValue::Str(p.str_or("requested_role", "")),
            );
            m.insert(
                "capability_class".into(),
                JValue::Str(p.str_or("capability_class", "general")),
            );
            m.insert("reason".into(), JValue::Str(p.str_or("body", "")));
            m.insert(
                "estimated_work".into(),
                JValue::Float(
                    p.get("estimated_work")
                        .and_then(JValue::as_f64)
                        .unwrap_or(0.0),
                ),
            );
            m.insert(
                "required_skills".into(),
                p.get("required_skills")
                    .cloned()
                    .unwrap_or(JValue::Arr(vec![])),
            );
            m.insert(
                "required_inputs".into(),
                p.get("required_inputs")
                    .cloned()
                    .unwrap_or(JValue::Arr(vec![])),
            );
            m.insert(
                "expected_outputs".into(),
                p.get("expected_outputs")
                    .cloned()
                    .unwrap_or(JValue::Arr(vec![])),
            );
            m.insert("parent_task_id".into(), jstr(row.task_id.as_deref()));
            m.insert(
                "correlation_id".into(),
                JValue::Str(p.str_or("correlation_id", "")),
            );
            m.insert(
                "fingerprint".into(),
                JValue::Str(p.str_or("fingerprint", "")),
            );
            m.insert(
                "at_tick".into(),
                JValue::Int(p.get("at_tick").and_then(JValue::as_int).unwrap_or(0)),
            );
            m.insert("state".into(), JValue::Str("RECEIVED".into()));
            m.insert("rule".into(), JValue::Str("".into()));
            m.insert("owner".into(), JValue::Null);
            m.insert("spawned_agent_id".into(), JValue::Null);
            m.insert("task_id".into(), JValue::Null);
            m.insert("seq".into(), JValue::Int(row.seq));
            state.requests.insert(rid, JValue::Obj(m));
        }
        E::SpawnRequestResolved => {
            let rid = p.str_or("rid", "");
            let entry = state
                .requests
                .entry(rid)
                .or_insert_with(|| JValue::Obj(default_request_entry(&p, actor, row)));
            let obj = match entry {
                JValue::Obj(m) => m,
                _ => return,
            };
            let rule = p.str_or("rule", "");
            obj.insert("rule".into(), JValue::Str(rule.clone()));
            obj.insert(
                "owner".into(),
                p.get("owner").cloned().unwrap_or(JValue::Null),
            );
            if let Some(tid) = row.task_id.clone() {
                obj.insert("task_id".into(), JValue::Str(tid));
            }
            let ok = p.get("ok").and_then(JValue::as_bool).unwrap_or(false);
            let st = if rule == "APPROVE" && ok {
                "APPROVED_COMMITTED"
            } else if rule == "REUSE_EXISTING" {
                "REROUTED"
            } else {
                "REJECTED"
            };
            obj.insert("state".into(), JValue::Str(st.into()));
            obj.insert("resolved_seq".into(), JValue::Int(row.seq));
        }
        E::SpawnApproved => {
            let rid = p.str_or("rid", "");
            if let Some(JValue::Obj(obj)) = state.requests.get_mut(&rid) {
                obj.insert("state".into(), JValue::Str("APPROVED_COMMITTED".into()));
                obj.insert("rule".into(), JValue::Str("APPROVE".into()));
                obj.insert(
                    "spawned_agent_id".into(),
                    p.get("agent_id").cloned().unwrap_or(JValue::Null),
                );
                let tid = p
                    .get("approved_task_id")
                    .and_then(JValue::as_str)
                    .map(|s| s.to_string())
                    .or_else(|| row.task_id.clone())
                    .or_else(|| {
                        obj.get("task_id")
                            .and_then(JValue::as_str)
                            .map(|s| s.to_string())
                    });
                obj.insert("task_id".into(), jstr(tid.as_deref()));
            }
        }
        E::RequestRerouted => {
            let rid = p.str_or("rid", "");
            if let Some(JValue::Obj(obj)) = state.requests.get_mut(&rid) {
                obj.insert("state".into(), JValue::Str("REROUTED".into()));
                obj.insert("rule".into(), JValue::Str("REUSE_EXISTING".into()));
                obj.insert(
                    "owner".into(),
                    p.get("owner").cloned().unwrap_or(JValue::Null),
                );
                let tid = p
                    .get("rerouted_task_id")
                    .and_then(JValue::as_str)
                    .map(|s| s.to_string())
                    .or_else(|| {
                        obj.get("task_id")
                            .and_then(JValue::as_str)
                            .map(|s| s.to_string())
                    });
                obj.insert("task_id".into(), jstr(tid.as_deref()));
            }
        }
        E::DeferredForCapacity => {
            let rid = p.str_or("rid", "");
            if let Some(JValue::Obj(obj)) = state.requests.get_mut(&rid) {
                obj.insert("state".into(), JValue::Str("DEFERRED".into()));
                obj.insert("rule".into(), JValue::Str("DEFER_FOR_CAPACITY".into()));
                obj.insert("task_id".into(), jstr(row.task_id.as_deref()));
            }
        }
        E::SpawnEscalated => {
            let rid = p.str_or("rid", "");
            if let Some(JValue::Obj(obj)) = state.requests.get_mut(&rid) {
                obj.insert("state".into(), JValue::Str("ESCALATED".into()));
                obj.insert("rule".into(), JValue::Str("ESCALATE".into()));
            }
        }
        _ => {}
    }
}

fn default_request_entry(p: &JValue, actor: &str, row: &EventRow) -> JMap {
    let mut m = JMap::new();
    m.insert("rid".into(), JValue::Str(p.str_or("rid", "")));
    m.insert("requester".into(), JValue::Str(actor.into()));
    m.insert("requested_role".into(), JValue::Str("".into()));
    m.insert("capability_class".into(), JValue::Str("general".into()));
    m.insert("reason".into(), JValue::Str(p.str_or("body", "")));
    m.insert("estimated_work".into(), JValue::Float(0.0));
    m.insert("required_skills".into(), JValue::Arr(vec![]));
    m.insert("required_inputs".into(), JValue::Arr(vec![]));
    m.insert("expected_outputs".into(), JValue::Arr(vec![]));
    m.insert("parent_task_id".into(), jstr(row.task_id.as_deref()));
    m.insert(
        "correlation_id".into(),
        JValue::Str(p.str_or("correlation_id", "")),
    );
    m.insert("fingerprint".into(), JValue::Str("".into()));
    m.insert("at_tick".into(), JValue::Int(0));
    m.insert("state".into(), JValue::Str("RECEIVED".into()));
    m.insert("rule".into(), JValue::Str("".into()));
    m.insert("owner".into(), JValue::Null);
    m.insert("spawned_agent_id".into(), JValue::Null);
    m.insert("task_id".into(), JValue::Null);
    m.insert("seq".into(), JValue::Int(row.seq));
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jmap;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("arena-journal-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn append_verify_truncate() {
        let d = tmpdir("chain");
        let mut j = Journal::open_file(d.join("j.db"), Some(0.01)).unwrap();
        j.set_now_provider(0.01);
        for i in 0..5 {
            let mut m = Message::new(EventType::StatusUpdate, "parent", "broadcast");
            m.body = format!("row {i}");
            j.append(&mut m);
        }
        let (ok, why, at) = j.verify_chain().unwrap();
        assert!(ok, "{why} at {at}");
        assert_eq!(j.count().unwrap(), 5);
        // truncate the tail; chain must still verify (rows removed wholesale)
        j.truncate_after(3).unwrap();
        assert_eq!(j.count().unwrap(), 3);
        let (ok2, _, _) = j.verify_chain().unwrap();
        assert!(ok2);
        // tamper with a committed row -> chain must localize it
        let conn = j.conn();
        conn.execute("UPDATE events SET actor='hacker' WHERE seq=2", &[])
            .unwrap();
        let (ok3, _, at3) = j.verify_chain().unwrap();
        assert!(!ok3);
        assert_eq!(at3, 2);
        j.close();
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn claims_first_writer_wins() {
        let j = Journal::open_memory().unwrap();
        let (a, _) = j
            .claim("backend|api.md", "backend_01", Some("t1"), 0.1)
            .unwrap();
        assert!(a);
        let (b, incumbent) = j
            .claim("backend|api.md", "backend_02", Some("t1"), 0.2)
            .unwrap();
        assert!(!b);
        assert_eq!(incumbent, "backend_01");
        let claims = j.claims().unwrap();
        let c = &claims[&ClaimKey::new("backend|api.md")];
        assert_eq!(c.get("owner").unwrap().as_str().unwrap(), "backend_01");
        match c.get("losers").unwrap() {
            JValue::Arr(v) => assert_eq!(v.len(), 1),
            _ => panic!("losers list"),
        }
    }

    #[test]
    fn waits_lifecycle() {
        let j = Journal::open_memory().unwrap();
        j.arm_wait(
            "w-0001",
            "backend_01",
            "artifact:api.json",
            Some("t1"),
            "c-1",
            0.5,
            Some(4.5),
        )
        .unwrap();
        assert_eq!(j.active_waits(Some("artifact:api.json")).unwrap().len(), 1);
        assert_eq!(j.active_waits(None).unwrap().len(), 1);
        j.resolve_wait("w-0001", "RESOLVED", "producer published")
            .unwrap();
        assert_eq!(j.active_waits(None).unwrap().len(), 0);
    }

    #[test]
    fn fold_projects_core_lifecycle() {
        let mut j = Journal::open_memory().unwrap();
        j.set_now_provider(0.01);
        let mut fields = jmap! {
            "agent_id" => "backend_01",
            "role" => "backend",
            "skills" => JValue::Arr(vec![JValue::Str("api".into())]),
            "epoch" => 0i64,
            "spawned_by" => "parent",
        };
        fields.insert(
            "skills".into(),
            JValue::Arr(vec![JValue::Str("api".into())]),
        );
        j.emit(
            EventType::AgentRegistered,
            "parent",
            "backend_01",
            "",
            fields,
            None,
            None,
            None,
            None,
            0,
        );
        let mut st = jmap! {"frm" => "CREATED", "to" => "IDLE"};
        st.insert("agent_id".into(), JValue::Str("backend_01".into()));
        j.emit(
            EventType::StateTransition,
            "backend_01",
            "parent",
            "CREATED -> IDLE",
            st,
            None,
            None,
            None,
            None,
            0,
        );
        let tasks = jmap! {
            "tasks" => JValue::Arr(vec![JValue::Obj({
                let mut t = JMap::new();
                t.insert("task_id".into(), JValue::Str("t1".into()));
                t.insert("title".into(), JValue::Str("api".into()));
                t.insert("role".into(), JValue::Str("backend".into()));
                t.insert("skills".into(), JValue::Arr(vec![]));
                t.insert("produces".into(), JValue::Arr(vec![JValue::Str("api.md".into())]));
                t.insert("consumes".into(), JValue::Arr(vec![]));
                t.insert("est_work".into(), JValue::Float(2.0));
                t
            })]),
        };
        j.emit(
            EventType::PlanCreated,
            "parent",
            "parent",
            "planned",
            tasks,
            None,
            None,
            None,
            None,
            0,
        );
        j.emit(
            EventType::TaskAssigned,
            "parent",
            "backend_01",
            "assigned",
            jmap! {"owner" => "backend_01"},
            Some("t1"),
            None,
            None,
            None,
            0,
        );
        j.emit(
            EventType::TaskCompleted,
            "backend_01",
            "parent",
            "t1 done",
            jmap! {"owner" => "backend_01", "artifacts" => JValue::Arr(vec![])},
            Some("t1"),
            None,
            None,
            None,
            0,
        );
        j.emit(
            EventType::ResourceUpdated,
            "backend_01",
            "broadcast",
            "api.md v1 ready",
            jmap! {"resource" => "api.md", "version" => 1i64,
            "payload" => JValue::Obj({
                let mut p = JMap::new();
                p.insert("artifact".into(), JValue::Str("api.md".into()));
                p.insert("version".into(), JValue::Int(1));
                p
            })},
            None,
            Some("api.md"),
            None,
            None,
            0,
        );
        let fold = j.fold().unwrap();
        let agents = fold.get("agents").unwrap();
        assert_eq!(
            agents
                .get("backend_01")
                .unwrap()
                .get("state")
                .unwrap()
                .as_str()
                .unwrap(),
            "IDLE"
        );
        let tasks = fold.get("tasks").unwrap();
        assert_eq!(
            tasks
                .get("t1")
                .unwrap()
                .get("status")
                .unwrap()
                .as_str()
                .unwrap(),
            "done"
        );
        let artifacts = fold.get("artifacts").unwrap();
        assert_eq!(artifacts.get("api.md").unwrap().as_int(), Some(1));
    }

    #[test]
    fn python_chain_format_crosscheck() {
        // A row emitted by CPython's journal with GENESIS prev must hash
        // identically here. Reference computed with:
        //   python3 -c "from arena.journal import _hash_row; print(_hash_row('0'*64, (0.01,'STATUS_UPDATE','control','agent.status_update','parent','broadcast',None,None,'c-1',None,0,'{\"body\":\"x\"}')))"
        let row = RowTuple {
            ts: 0.01,
            etype: "STATUS_UPDATE",
            plane: "control",
            topic: "agent.status_update",
            actor: "parent",
            target: Some("broadcast"),
            task_id: None,
            resource: None,
            correlation_id: Some("c-1"),
            caused_by: None,
            depth: 0,
            payload: "{\"body\":\"x\"}",
        };
        assert_eq!(
            hash_row(GENESIS, &row),
            "f0d6e8730e9e8de115d1d8c002c3ce1b6dd552be2f0881ba1c27c4ed7eddccf8"
        );
        // second reference: full row with task/resource/causality + canonical payload
        let row2 = RowTuple {
            ts: 0.0,
            etype: "AGENT_REGISTERED",
            plane: "control",
            topic: "agent.agent.registered",
            actor: "parent",
            target: Some("backend_01"),
            task_id: Some("t_api"),
            resource: None,
            correlation_id: Some("c-abc123"),
            caused_by: Some("m-1"),
            depth: 1,
            payload: "{\"body\":\"agent joined\",\"epoch\":0}",
        };
        let h2 = hash_row(GENESIS, &row2);
        assert_eq!(
            h2,
            "9ff4c9d06322441ead765fac4a591c3a1696eced39c134483d442de6c7dce55a"
        );
        // third: the 1e-07 exponent form must match CPython repr exactly
        let row3 = RowTuple {
            ts: 1e-7,
            etype: "TOOL_RESULT",
            plane: "control",
            topic: "exec.tool_result",
            actor: "backend_01",
            target: Some("kernel"),
            task_id: None,
            resource: Some("out.txt"),
            correlation_id: Some("c-x"),
            caused_by: Some("m-2"),
            depth: 2,
            payload: "{\"exit_code\":0,\"ok\":true}",
        };
        assert_eq!(
            hash_row(&h2, &row3),
            "7cce3c5c73ea744486e180c338a98f4def49ec46899835408edb91406c065ea3"
        );
    }
}
