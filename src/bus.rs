//! Message bus delivery policy (`arena/bus.py`) — the part of the bus that
//! must exist in Phase 1: planes, subscriptions, loop guard, budgets, durable
//! wakes, on an in-kernel transport.
//!
//! Two rules that prevent the chaos:
//! - **Plane separation.** A RESOURCE/ARTIFACT broadcast is never delivered as
//!   a generic agent broadcast — an agent receives it only if it explicitly
//!   subscribed to that topic. This kills "shared_types.ts changed → 12 agents
//!   wake up and reply to each other".
//! - **Causal depth cap.** Replies must be built with `Message::child()`,
//!   which inherits `causal_depth`. A → B → A ping-pong is therefore capped
//!   structurally, and drops are journaled so you can see what would have
//!   been a runaway.
//!
//! ## Ownership translation
//! The Python bus holds `kernel: Any` and reaches into kernel internals. In
//! Rust the kernel stays the sole owner of mutable runtime state, so `Bus`
//! owns exactly what the transport layer owns — mailboxes, subscriptions,
//! per-tick send accounting, delivery stats, poll instrumentation — and every
//! method that needs the journal or the registry takes them as explicit
//! `&mut` parameters. No `Arc<Mutex<…>>` anywhere.
//!
//! ## Transport seam
//! Delivery is behind the [`Transport`] trait. `InProcessTransport` is the
//! Phase 1 implementation; a future cross-process transport implements the
//! same trait without touching bus policy rules or the agent API.
//!
//! ## Determinism
//! Broadcast targets iterate in registration order (`parent`, `kernel`, then
//! agents in the order the kernel bound them), so `Delivery::recipients` and
//! drop lists are byte-stable run to run.

use std::collections::BTreeMap;

use crate::ids::WaitId;
use crate::journal::{EventFilter, Journal, JournalError, WaitRow};
use crate::msg::{EventType, Message, Plane, MAX_CAUSAL_DEPTH, MSG_BUDGET_PER_TICK};
use crate::registry::AgentRegistry;
use crate::sys::json::{JMap, JValue};
use crate::sys::sha256::hex_digest;

// ------------------------------------------------------------------ errors

/// Typed bus failures. Normal delivery drops are NOT errors — they are
/// structured [`Delivery`] evidence (a drop is a decision, not a deletion).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    /// A policy polled past its eagerness limit instead of declaring a wait.
    PollingForbidden {
        actor: String,
        hits: i64,
        limit: i64,
    },
}

impl std::fmt::Display for BusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BusError::PollingForbidden { actor, hits, limit } => write!(
                f,
                "{actor} polled {hits} times (limit {limit}): \
                 use kernel.wait_for() instead of re-checking"
            ),
        }
    }
}
impl std::error::Error for BusError {}

// ------------------------------------------------------------- poll counter

/// Instrumentation for the "event-driven, not polling" requirement.
///
/// A policy that *asks* "is it ready?" instead of declaring a durable wait
/// increments this. The chaos suite asserts `polls == 0` for the polite path
/// and `> 0` for the anti-pattern path, so "no polling" is a measured
/// property of the system, not a claim in a README. The counter exists in
/// Rust for the same reason: it is an invariant/diagnostic, not a need of
/// the implementation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PollCounter {
    pub polls: i64,
    pub by_actor: BTreeMap<String, i64>,
    /// how many times an agent may look at a dependency before giving up
    /// (0 = unlimited, mirroring the reference's truthiness check)
    pub eagerness_limit: i64,
}

impl PollCounter {
    pub fn hit(&mut self, actor: &str) -> Result<i64, BusError> {
        self.polls += 1;
        let hits = self.by_actor.entry(actor.to_string()).or_insert(0);
        *hits += 1;
        if self.eagerness_limit > 0 && *hits > self.eagerness_limit {
            return Err(BusError::PollingForbidden {
                actor: actor.to_string(),
                hits: *hits,
                limit: self.eagerness_limit,
            });
        }
        Ok(self.polls)
    }

    pub fn reset(&mut self) {
        self.polls = 0;
        self.by_actor.clear();
    }

    /// Offending actors, sorted (deterministic attribution).
    pub fn offenders(&self) -> Vec<String> {
        self.by_actor.keys().cloned().collect()
    }
}

// ----------------------------------------------------------------- delivery

/// Evidence produced by one publish: who got it, and every relevant drop.
/// `dropped_depth` stays empty by design — the depth guard returns before
/// per-target routing — but the field exists to mirror the reference shape.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Delivery {
    pub recipients: Vec<String>,
    pub dropped_depth: Vec<String>,
    pub dropped_budget: Vec<String>,
    pub not_subscribed: Vec<String>,
}

impl Delivery {
    /// The reference's `__bool__`: a delivery is truthy iff someone got it.
    pub fn delivered_any(&self) -> bool {
        !self.recipients.is_empty()
    }
}

// ---------------------------------------------------------------- transport

/// The physical movement of messages. Phase 1 is in-process; Phase 3 swaps
/// this for a cross-process transport (e.g. ZeroMQ XSUB/XPUB) without
/// changing a single bus policy rule, because policies only ever call
/// `Bus::publish` / `Bus::wait_for`.
pub trait Transport {
    /// Give the actor a mailbox, appending it to the delivery roster.
    fn register_actor(&mut self, actor: &str);
    /// Recipient enumeration order: `parent`, `kernel`, then agents in the
    /// order the kernel bound them. This ordering IS the observable delivery
    /// order for broadcasts.
    fn actors(&self) -> Vec<String>;
    /// Whether the actor holds a mailbox (the reference's `actor in queues`).
    fn has(&self, actor: &str) -> bool;
    fn enqueue(&mut self, actor: &str, msg: Message);
    /// Take up to `limit` messages (oldest first); `None` = drain all.
    fn drain(&mut self, actor: &str, limit: Option<usize>) -> Vec<Message>;
    fn queue_len(&self, actor: &str) -> usize;
    /// Read-only view of a mailbox (tests/snapshots).
    fn queue(&self, actor: &str) -> Vec<Message>;
}

/// Phase 1 in-process mailboxes: an ordered actor roster plus per-actor FIFO.
#[derive(Debug, Clone, Default)]
pub struct InProcessTransport {
    order: Vec<String>,
    queues: BTreeMap<String, Vec<Message>>,
}

impl Transport for InProcessTransport {
    /// Register an actor's mailbox, appended to the delivery roster.
    /// Re-registering is a no-op (the kernel may `setdefault` idempotently).
    fn register_actor(&mut self, actor: &str) {
        if !self.queues.contains_key(actor) {
            self.queues.insert(actor.to_string(), Vec::new());
            self.order.push(actor.to_string());
        }
    }

    fn actors(&self) -> Vec<String> {
        self.order.clone()
    }

    fn has(&self, actor: &str) -> bool {
        self.queues.contains_key(actor)
    }

    fn enqueue(&mut self, actor: &str, msg: Message) {
        self.queues.entry(actor.to_string()).or_default().push(msg);
    }

    fn drain(&mut self, actor: &str, limit: Option<usize>) -> Vec<Message> {
        let Some(q) = self.queues.get_mut(actor) else {
            return Vec::new();
        };
        let n = limit.unwrap_or(q.len()).min(q.len());
        q.drain(..n).collect()
    }

    fn queue_len(&self, actor: &str) -> usize {
        self.queues.get(actor).map(|q| q.len()).unwrap_or(0)
    }

    fn queue(&self, actor: &str) -> Vec<Message> {
        self.queues.get(actor).cloned().unwrap_or_default()
    }
}

// ---------------------------------------------------------------------- bus

/// Default agent subscriptions: the control-plane work topics plus the whole
/// dependency plane. Segment 1 is the plane, so a category filter must be
/// qualified: `dependency.*` matches every dependency-plane topic; a bare
/// `conflict.*` would match nothing.
pub const DEFAULT_AGENT_PATTERNS: [&str; 3] = ["control.task.*", "control.agent.*", "dependency.*"];
/// The parent is the only full-visibility subscriber.
pub const PARENT_PATTERNS: [&str; 3] = ["control.*", "dependency.*", "resource.*"];

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BusStats {
    pub published: i64,
    pub delivered: i64,
    pub dropped_depth: i64,
    pub dropped_budget: i64,
    pub dropped_unsubscribed: i64,
    pub wakes: i64,
}

impl BusStats {
    fn to_map(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("published".into(), JValue::Int(self.published));
        m.insert("delivered".into(), JValue::Int(self.delivered));
        m.insert("dropped_depth".into(), JValue::Int(self.dropped_depth));
        m.insert("dropped_budget".into(), JValue::Int(self.dropped_budget));
        m.insert(
            "dropped_unsubscribed".into(),
            JValue::Int(self.dropped_unsubscribed),
        );
        m.insert("wakes".into(), JValue::Int(self.wakes));
        m
    }
}

/// One armed durable wait, as handed back to the caller (and mirrored onto
/// the agent's registry record).
#[derive(Debug, Clone, PartialEq)]
pub struct WaitEntry {
    pub wait_id: WaitId,
    pub condition: String,
    pub task_id: Option<String>,
    pub armed_at: f64,
    pub timeout_at: Option<f64>,
}

impl WaitEntry {
    pub fn to_map(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("wait_id".into(), JValue::Str(self.wait_id.as_str().into()));
        m.insert("condition".into(), JValue::Str(self.condition.clone()));
        m.insert(
            "task_id".into(),
            self.task_id
                .clone()
                .map(JValue::Str)
                .unwrap_or(JValue::Null),
        );
        m.insert("armed_at".into(), JValue::Float(self.armed_at));
        m.insert(
            "timeout_at".into(),
            self.timeout_at.map(JValue::Float).unwrap_or(JValue::Null),
        );
        m
    }
}

/// The in-process bus: subscriptions, routing, budgets, durable waits.
/// Owns mailbox state; the journal and registry stay kernel-owned and are
/// passed in per call.
pub struct Bus {
    transport: Box<dyn Transport>,
    /// topic patterns an actor has explicitly subscribed to
    /// (insertion-ordered, deduplicated)
    subscriptions: BTreeMap<String, Vec<String>>,
    /// the SENDER's outbound allowance counter, reset each tick
    sent_this_tick: BTreeMap<String, i64>,
    pub stats: BusStats,
    pub polls: PollCounter,
}

impl Default for Bus {
    fn default() -> Self {
        Bus::new()
    }
}

impl Bus {
    pub fn new() -> Bus {
        let mut transport = Box::new(InProcessTransport::default());
        // the kernel pre-creates the orchestration mailboxes
        transport.register_actor("parent");
        transport.register_actor("kernel");
        let mut subscriptions = BTreeMap::new();
        subscriptions.insert(
            "parent".to_string(),
            PARENT_PATTERNS.iter().map(|s| s.to_string()).collect(),
        );
        Bus {
            transport,
            subscriptions,
            sent_this_tick: BTreeMap::new(),
            stats: BusStats::default(),
            polls: PollCounter::default(),
        }
    }

    /// Register an actor's mailbox on the delivery roster (the kernel calls
    /// this when binding an actor; ordering is observable in broadcasts).
    pub fn register_actor(&mut self, actor: &str) {
        self.transport.register_actor(actor);
    }

    /// A transport-level peek at a mailbox (read-only; actors drain via
    /// [`Bus::drain`]).
    pub fn mailbox(&self, actor: &str) -> Vec<Message> {
        self.transport.queue(actor)
    }

    pub fn mailbox_len(&self, actor: &str) -> usize {
        self.transport.queue_len(actor)
    }

    /// Take messages from an actor's mailbox, oldest first. This is the
    /// actor-side consumption path; the bus owns delivery, the actor owns
    /// interpretation.
    pub fn drain(&mut self, actor: &str, limit: Option<usize>) -> Vec<Message> {
        self.transport.drain(actor, limit)
    }

    /// Start a new tick: the sender budget allowance resets. The kernel calls
    /// this once per tick before any stepping (budget is per tick, not
    /// lifetime).
    pub fn reset_tick(&mut self) {
        self.sent_this_tick.clear();
    }

    // ------------------------------------------------------------ subscriptions

    /// The ONE way to express interest. Mirrored onto the [`AgentRecord`] so
    /// a snapshot/replay and the bus can never disagree about who subscribed
    /// to what. Returns the actor's full pattern list. Note the reference's
    /// `setdefault`: even an empty pattern list creates the (observable)
    /// subscriptions entry.
    pub fn subscribe(
        &mut self,
        registry: &mut AgentRegistry,
        actor: &str,
        patterns: &[&str],
    ) -> Vec<String> {
        let cur = self.subscriptions.entry(actor.to_string()).or_default();
        for p in patterns {
            if !cur.iter().any(|x| x == p) {
                cur.push(p.to_string());
            }
        }
        if let Some(rec) = registry.get_mut(actor) {
            for p in patterns {
                if !rec.subscriptions.iter().any(|x| x == p) {
                    rec.subscriptions.push(p.to_string());
                }
            }
        }
        cur.clone()
    }

    /// Overload used by the kernel when the pattern list is already owned.
    pub fn unsubscribe(&mut self, registry: &mut AgentRegistry, actor: &str, pattern: &str) {
        if let Some(cur) = self.subscriptions.get_mut(actor) {
            if let Some(pos) = cur.iter().position(|x| x == pattern) {
                cur.remove(pos);
            }
        }
        if let Some(rec) = registry.get_mut(actor) {
            if let Some(pos) = rec.subscriptions.iter().position(|x| x == pattern) {
                rec.subscriptions.remove(pos);
            }
        }
    }

    /// Effective patterns for an actor: kernel sees everything, the parent
    /// has full visibility, agents get the defaults plus explicit extras.
    pub fn patterns_for(&self, actor: &str) -> Vec<String> {
        if actor == "parent" {
            return PARENT_PATTERNS.iter().map(|s| s.to_string()).collect();
        }
        if actor == "kernel" {
            return vec!["*".to_string()];
        }
        let mut out: Vec<String> = DEFAULT_AGENT_PATTERNS
            .iter()
            .map(|s| s.to_string())
            .collect();
        if let Some(extra) = self.subscriptions.get(actor) {
            out.extend(extra.iter().cloned());
        }
        out
    }

    // ---------------------------------------------------------------- routing

    /// The complete publish path. Mirrors the reference composition
    /// `Kernel.publish` + `Bus.publish`:
    ///
    /// 1. the sender's per-tick outbound counter increments (the budget is
    ///    the **sender's** allowance, never the recipient's inbound capacity)
    /// 2. the message is journaled (a later drop is a decision, not a
    ///    deletion — the journal still holds everything)
    /// 3. causal-depth guard → journaled `BUDGET_EXCEEDED` drop
    /// 4. per-target: self-broadcast rule → mailbox existence → subscription
    ///    gating (agents only; parent/kernel see everything) → sender budget
    ///    → enqueue
    pub fn publish(
        &mut self,
        msg: &mut Message,
        journal: &mut Journal,
    ) -> Result<Delivery, JournalError> {
        *self
            .sent_this_tick
            .entry(msg.from_actor.as_str().to_string())
            .or_insert(0) += 1;
        journal.append(msg);
        self.stats.published += 1;
        let mut d = Delivery::default();

        if msg.causal_depth > MAX_CAUSAL_DEPTH {
            self.stats.dropped_depth += 1;
            let body = format!(
                "dropped {}: causal depth {} > {}",
                msg.msg_type.as_str(),
                msg.causal_depth,
                MAX_CAUSAL_DEPTH
            );
            let mut fields = JMap::new();
            fields.insert("reason".into(), JValue::Str("MAX_CAUSAL_DEPTH".into()));
            fields.insert("dropped".into(), JValue::Str(msg.mid.clone()));
            journal.emit(
                EventType::BudgetExceeded,
                msg.from_actor.as_str(),
                "parent",
                &body,
                fields,
                None,
                None,
                Some(msg.correlation_id.as_str()),
                Some(msg.mid.as_str()),
                0,
            );
            return Ok(d);
        }

        let targets: Vec<String> = if !matches!(msg.to_actor.as_str(), "broadcast" | "*" | "") {
            vec![msg.to_actor.as_str().to_string()]
        } else {
            self.transport.actors()
        };

        let sender_exempt = matches!(
            msg.from_actor.as_str(),
            "parent" | "kernel" | "dependency_manager"
        );

        for actor in targets {
            // Skip an agent receiving its own broadcast (noise reduction). Do
            // NOT skip orchestration senders: parent → agent assignments must
            // be delivered.
            if actor == msg.from_actor.as_str()
                && !matches!(actor.as_str(), "parent" | "dependency_manager" | "kernel")
            {
                continue;
            }
            if !self.transport.has(&actor) {
                continue;
            }
            let is_infrastructure = matches!(actor.as_str(), "parent" | "kernel");
            if !is_infrastructure {
                // Agents are subscription-gated on EVERY plane. The reference
                // spells the resource-plane case out in its own branch
                // (identical effect); both exist to make the rule obvious:
                // a RESOURCE/ARTIFACT broadcast is never delivered as a
                // generic agent broadcast, and no other plane is a
                // free-for-all either.
                let pats = self.patterns_for(&actor);
                let _ = msg.plane == Plane::Resource; // (documented above)
                if !pats.iter().any(|p| msg.matches(p)) {
                    self.stats.dropped_unsubscribed += 1;
                    d.not_subscribed.push(actor.clone());
                    continue;
                }
            }
            // Budget is the SENDER's outbound allowance — enforcing it on the
            // recipient meant a flood was never actually capped (a real
            // defect the Python chaos suite caught).
            if !sender_exempt {
                let used = self
                    .sent_this_tick
                    .get(msg.from_actor.as_str())
                    .copied()
                    .unwrap_or(0);
                if used > MSG_BUDGET_PER_TICK {
                    self.stats.dropped_budget += 1;
                    d.dropped_budget.push(actor.clone());
                    continue;
                }
            }
            self.transport.enqueue(&actor, msg.clone());
            d.recipients.push(actor.clone());
            self.stats.delivered += 1;
        }
        Ok(d)
    }

    // -------------------------------------------------------- durable waiting

    /// Park the agent on a condition. No queue is held open, nothing is
    /// checked again. The wait is a *row in the journal*, so a blocked agent
    /// costs zero runtime and can be woken even by a process that restarts
    /// after the VM was recycled. No busy-wait anywhere.
    #[allow(clippy::too_many_arguments)]
    pub fn wait_for(
        &mut self,
        journal: &mut Journal,
        registry: &mut AgentRegistry,
        now: f64,
        actor: &str,
        condition: &str,
        task_id: Option<&str>,
        correlation_id: &str,
        timeout: Option<f64>,
    ) -> Result<WaitEntry, JournalError> {
        let n = journal
            .events(&EventFilter::new().etype(EventType::WaitRegistered.as_str()))?
            .len() as i64;
        let wait_id = WaitId::new(format!("w-{:04}", n + 1));
        let timeout_at = timeout.map(|t| now + t);
        journal.arm_wait(
            wait_id.as_str(),
            actor,
            condition,
            task_id,
            correlation_id,
            now,
            timeout_at,
        )?;
        let entry = WaitEntry {
            wait_id: wait_id.clone(),
            condition: condition.to_string(),
            task_id: task_id.map(|t| t.to_string()),
            armed_at: now,
            timeout_at,
        };
        if let Some(rec) = registry.get_mut(actor) {
            rec.pending_waits.push(entry.to_map());
        }
        let body = format!("waiting for {condition}");
        let mut fields = JMap::new();
        fields.insert("wait_id".into(), JValue::Str(wait_id.as_str().into()));
        fields.insert("condition".into(), JValue::Str(condition.into()));
        fields.insert(
            "timeout_at".into(),
            timeout_at.map(JValue::Float).unwrap_or(JValue::Null),
        );
        journal.emit(
            EventType::WaitRegistered,
            actor,
            "parent",
            &body,
            fields,
            task_id,
            None,
            None,
            None,
            0,
        );
        Ok(entry)
    }

    /// Publish-side wake. Returns the agents resumed. Exactly one lookup, no
    /// timers spinning; the woken agent gets a targeted `DEPENDENCY_READY`
    /// message (this is a targeted wake, never a broadcast).
    pub fn resolve(
        &mut self,
        journal: &mut Journal,
        registry: &mut AgentRegistry,
        condition: &str,
        reason: &str,
        payload: Option<&JMap>,
    ) -> Result<Vec<String>, JournalError> {
        let mut woken: Vec<String> = Vec::new();
        let rows = journal.active_waits(Some(condition))?;
        for row in rows {
            journal.resolve_wait(row.wait_id.as_str(), "RESOLVED", reason)?;
            if let Some(rec) = registry.get_mut(row.agent_id.as_str()) {
                rec.pending_waits.retain(|w| {
                    w.get("wait_id").and_then(|v| v.as_str()) != Some(row.wait_id.as_str())
                });
            }
            let body = format!("{condition} is ready ({reason})");
            let mut msg = Message::new(
                EventType::DependencyReady,
                "dependency_manager",
                row.agent_id.as_str(),
            );
            msg.body = body;
            msg.task_id = row.task_id.as_deref().map(crate::ids::TaskId::new);
            if let Some(c) = row.correlation_id.as_deref() {
                if !c.is_empty() {
                    msg.correlation_id = crate::ids::CorrelationId::new(c);
                }
            }
            let mut pl: JMap = payload.cloned().unwrap_or_default();
            pl.insert("wait_id".into(), JValue::Str(row.wait_id.as_str().into()));
            pl.insert("condition".into(), JValue::Str(condition.into()));
            msg.payload = pl;
            self.publish(&mut msg, journal)?;
            let body2 = format!("{condition} -> {reason}");
            let mut fields = JMap::new();
            fields.insert("wait_id".into(), JValue::Str(row.wait_id.as_str().into()));
            journal.emit(
                EventType::WaitResolved,
                "dependency_manager",
                row.agent_id.as_str(),
                &body2,
                fields,
                row.task_id.as_deref(),
                None,
                non_empty(row.correlation_id.as_deref()),
                None,
                0,
            );
            self.stats.wakes += 1;
            woken.push(row.agent_id.as_str().to_string());
        }
        Ok(woken)
    }

    /// `WAIT_TIMEOUT` → escalate. One clock-driven scan per tick, not a loop:
    /// each timed-out wait is resolved, the agent's registry wait entry is
    /// cleared, and a targeted `WAIT_TIMEOUT` message is delivered so the
    /// Parent/supervision layer can observe the escalation.
    pub fn expire_timeouts(
        &mut self,
        journal: &mut Journal,
        registry: &mut AgentRegistry,
        now: f64,
    ) -> Result<Vec<WaitRow>, JournalError> {
        let mut expired: Vec<WaitRow> = Vec::new();
        let rows = journal.active_waits(None)?;
        for row in rows {
            if let Some(t) = row.timeout_at {
                if now >= t {
                    journal.resolve_wait(row.wait_id.as_str(), "TIMED_OUT", "WAIT_TIMEOUT")?;
                    if let Some(rec) = registry.get_mut(row.agent_id.as_str()) {
                        rec.pending_waits.retain(|w| {
                            w.get("wait_id").and_then(|v| v.as_str()) != Some(row.wait_id.as_str())
                        });
                    }
                    let body = format!(
                        "gave up waiting for {} after {:.2}s",
                        row.condition,
                        now - row.armed_at
                    );
                    let mut msg = Message::new(
                        EventType::WaitTimeout,
                        "dependency_manager",
                        row.agent_id.as_str(),
                    );
                    msg.body = body;
                    msg.task_id = row.task_id.as_deref().map(crate::ids::TaskId::new);
                    if let Some(c) = row.correlation_id.as_deref() {
                        if !c.is_empty() {
                            msg.correlation_id = crate::ids::CorrelationId::new(c);
                        }
                    }
                    let mut pl = JMap::new();
                    pl.insert("wait_id".into(), JValue::Str(row.wait_id.as_str().into()));
                    pl.insert("condition".into(), JValue::Str(row.condition.clone()));
                    msg.payload = pl;
                    self.publish(&mut msg, journal)?;
                    expired.push(row);
                }
            }
        }
        Ok(expired)
    }

    // ------------------------------------------------------------- rendering

    /// Delivery stats plus poll instrumentation and the subscription table.
    pub fn snapshot(&self) -> JMap {
        let mut m = self.stats.to_map();
        m.insert("polls".into(), JValue::Int(self.polls.polls));
        m.insert(
            "poll_offenders".into(),
            JValue::Arr(
                self.polls
                    .offenders()
                    .into_iter()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        let mut subs = JMap::new();
        for (k, v) in &self.subscriptions {
            subs.insert(
                k.clone(),
                JValue::Arr(v.iter().cloned().map(JValue::Str).collect()),
            );
        }
        m.insert("subscriptions".into(), JValue::Obj(subs));
        m
    }
}

fn non_empty(s: Option<&str>) -> Option<&str> {
    match s {
        Some("") | None => None,
        Some(s) => Some(s),
    }
}

/// Deterministic digest helper used by parity tooling to fingerprint a
/// delivery outcome (recipients + drops) independent of message ids.
pub fn delivery_fingerprint(d: &Delivery) -> String {
    let mut m = JMap::new();
    m.insert(
        "recipients".into(),
        JValue::Arr(d.recipients.iter().cloned().map(JValue::Str).collect()),
    );
    m.insert(
        "dropped_budget".into(),
        JValue::Arr(d.dropped_budget.iter().cloned().map(JValue::Str).collect()),
    );
    m.insert(
        "not_subscribed".into(),
        JValue::Arr(d.not_subscribed.iter().cloned().map(JValue::Str).collect()),
    );
    hex_digest(JValue::Obj(m).to_canon_string().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{AgentState, Lifecycle};

    /// The reference test fixture: a bus + journal + registry with working
    /// agents registered and mailboxes bound, in insertion order.
    struct Fixture {
        bus: Bus,
        journal: Journal,
        registry: AgentRegistry,
    }

    fn fixture(ids: &[&str]) -> Fixture {
        let mut f = Fixture {
            bus: Bus::new(),
            journal: Journal::open_memory().unwrap(),
            registry: AgentRegistry::default(),
        };
        for i in ids {
            f.registry
                .register(
                    i,
                    "talker",
                    &["t"],
                    0,
                    "parent",
                    "",
                    Some(Lifecycle::with_state(AgentState::Working, 0.0)),
                    &[],
                )
                .unwrap();
            f.bus.register_actor(i);
        }
        f
    }

    fn msg(t: EventType, from: &str, to: &str, body: &str) -> Message {
        let mut m = Message::new(t, from, to);
        m.body = body.to_string();
        m
    }

    #[test]
    fn direct_message_reaches_only_its_target() {
        let mut f = fixture(&["a", "b", "c"]);
        let mut m = msg(EventType::DependencyRequest, "a", "b", "hi");
        let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
        assert_eq!(out.recipients, vec!["b"]);
        assert_eq!(f.bus.mailbox_len("b"), 1);
        assert_eq!(f.bus.mailbox_len("c"), 0);
        assert_eq!(f.bus.mailbox("b")[0].body, "hi");
    }

    #[test]
    fn broadcast_reaches_every_subscribed_agent_but_not_the_sender() {
        let mut f = fixture(&["a", "b", "c"]);
        let mut m = msg(
            EventType::DependencyReady,
            "a",
            "broadcast",
            "schema is ready",
        );
        let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
        assert!(out.recipients.contains(&"b".to_string()));
        assert!(out.recipients.contains(&"c".to_string()));
        assert!(
            !out.recipients.contains(&"a".to_string()),
            "an agent must not receive its own broadcast"
        );
        // deterministic order: parent, kernel, then agents in roster order
        assert_eq!(out.recipients, vec!["parent", "kernel", "b", "c"]);
    }

    #[test]
    fn parent_and_kernel_have_full_visibility() {
        let mut f = fixture(&["a"]);
        let mut m = msg(EventType::TaskProgress, "a", "parent", "30%");
        f.bus.publish(&mut m, &mut f.journal).unwrap();
        assert!(f
            .bus
            .mailbox("parent")
            .iter()
            .any(|x| x.msg_type == EventType::TaskProgress));
        // parent self-broadcast is NOT skipped (orchestration sender rule);
        // agents are still pattern-gated: control.plan.* is not in defaults
        // (verified against the reference)
        let mut m2 = msg(EventType::PlanCreated, "parent", "broadcast", "plan");
        assert_eq!(m2.topic, "control.plan.created");
        let out = f.bus.publish(&mut m2, &mut f.journal).unwrap();
        assert_eq!(out.recipients, vec!["parent", "kernel"]);
        assert_eq!(out.not_subscribed, vec!["a"]);
        // kernel patterns are "*"
        assert_eq!(f.bus.patterns_for("kernel"), vec!["*"]);
        let pats = f.bus.patterns_for("parent");
        assert_eq!(pats, vec!["control.*", "dependency.*", "resource.*"]);
    }

    #[test]
    fn resource_plane_is_subscription_gated() {
        let mut f = fixture(&["quiet", "loud"]);
        f.bus.subscribe(&mut f.registry, "loud", &["resource.*"]);
        let mut m = msg(EventType::ResourceUpdated, "a", "broadcast", "changed");
        m.resource = Some("shared_types.ts".to_string());
        let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
        assert!(out.recipients.contains(&"loud".to_string()));
        assert!(
            out.not_subscribed.contains(&"quiet".to_string()),
            "unsubscribed agent was notified of a resource change"
        );
        assert_eq!(f.bus.mailbox_len("quiet"), 0);
        assert!(f.bus.mailbox_len("loud") >= 1);
        // parent/kernel still see the resource plane
        assert!(out.recipients.contains(&"parent".to_string()));
        assert!(out.recipients.contains(&"kernel".to_string()));
    }

    #[test]
    fn non_resource_agent_topic_without_subscription_is_gated_too() {
        // Agents are pattern-gated on every plane (the resource branch is the
        // explicit case of the general rule), even for direct messages.
        let mut f = fixture(&["be"]);
        // STATUS_UPDATE lives under control.agent.* -> delivered
        let mut m = msg(EventType::StatusUpdate, "parent", "be", "hello");
        let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
        assert_eq!(out.recipients, vec!["be"]);
        assert_eq!(m.topic, "control.agent.status_update");
        // PLAN_AMENDED lives under control.plan.* -> not in agent defaults
        let mut m2 = msg(EventType::PlanAmended, "parent", "be", "amend");
        assert_eq!(m2.topic, "control.plan.amended");
        let out2 = f.bus.publish(&mut m2, &mut f.journal).unwrap();
        assert!(out2.not_subscribed.contains(&"be".to_string()));
        assert!(out2.recipients.is_empty());
        // resource plane likewise
        let mut m3 = msg(EventType::ArtifactPublished, "parent", "be", "pub");
        let out3 = f.bus.publish(&mut m3, &mut f.journal).unwrap();
        assert!(out3.not_subscribed.contains(&"be".to_string()));
        // until the agent subscribes explicitly
        f.bus
            .subscribe(&mut f.registry, "be", &["control.plan.*", "resource.*"]);
        let mut m4 = msg(EventType::PlanAmended, "parent", "be", "amend");
        let out4 = f.bus.publish(&mut m4, &mut f.journal).unwrap();
        assert_eq!(out4.recipients, vec!["be"]);
    }

    #[test]
    fn causal_depth_cap_stops_a_ping_pong() {
        let mut f = fixture(&["x", "y"]);
        let mut root = msg(EventType::DependencyRequest, "x", "y", "");
        f.bus.publish(&mut root, &mut f.journal).unwrap();
        let mut cur = root.clone();
        let mut hops = 0;
        loop {
            let nxt = cur.child(
                EventType::DependencyRequest,
                cur.to_actor.as_str(),
                cur.from_actor.as_str(),
            );
            let mut nxt = nxt;
            let out = f.bus.publish(&mut nxt, &mut f.journal).unwrap();
            if !out.delivered_any() {
                break;
            }
            cur = nxt;
            hops += 1;
            assert!(hops <= MAX_CAUSAL_DEPTH + 3, "loop guard failed to engage");
        }
        let drops = f
            .journal
            .events(&EventFilter::new().etype("BUDGET_EXCEEDED"))
            .unwrap();
        assert!(
            !drops.is_empty(),
            "a dropped cascade must be visible in the journal"
        );
        let drop_row = &drops[0];
        assert!(drop_row.payload_text.contains("MAX_CAUSAL_DEPTH"));
        assert!(drop_row
            .payload_text
            .contains("\"reason\":\"MAX_CAUSAL_DEPTH\""));
        assert_eq!(f.bus.stats.dropped_depth, 1);
        // the guard fires on the message at depth CAP+1; everything up to the cap is delivered
        assert!(f.bus.stats.delivered >= MAX_CAUSAL_DEPTH);
        assert_eq!(drop_row.etype, "BUDGET_EXCEEDED");
        assert_eq!(drop_row.target.as_deref(), Some("parent"));
    }

    #[test]
    fn sender_budget_caps_one_agents_output_per_tick() {
        let mut f = fixture(&["flooder", "victim"]);
        f.bus.reset_tick();
        let mut drops = 0usize;
        for i in 0..40 {
            let mut m = msg(
                EventType::DependencyRequest,
                "flooder",
                "victim",
                &format!("spam {i}"),
            );
            let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
            drops += out.dropped_budget.len();
        }
        // budget = 8: accepted 8, dropped 32
        assert_eq!(
            f.bus.mailbox_len("victim"),
            MSG_BUDGET_PER_TICK as usize,
            "sender budget did not apply"
        );
        assert_eq!(drops, 32);
        assert_eq!(f.bus.stats.dropped_budget, 32);
        // journal still holds everything: a drop is a decision, not a deletion
        assert!(f.journal.count().unwrap() >= 40);
    }

    #[test]
    fn budget_resets_between_ticks() {
        let mut f = fixture(&["flooder", "victim"]);
        for _ in 0..(MSG_BUDGET_PER_TICK + 5) {
            let mut m = msg(EventType::DependencyRequest, "flooder", "victim", "");
            f.bus.publish(&mut m, &mut f.journal).unwrap();
        }
        let first: usize = f.bus.mailbox_len("victim");
        assert_eq!(first, MSG_BUDGET_PER_TICK as usize);
        f.bus.reset_tick();
        let mut m = msg(EventType::DependencyRequest, "flooder", "victim", "");
        f.bus.publish(&mut m, &mut f.journal).unwrap();
        assert_eq!(
            f.bus.mailbox_len("victim"),
            first + 1,
            "budget must be per tick, not lifetime"
        );
    }

    #[test]
    fn orchestration_senders_are_budget_exempt() {
        let mut f = fixture(&["a"]);
        for _ in 0..30 {
            let mut m = msg(EventType::TaskAssigned, "parent", "a", "");
            let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
            assert!(out.dropped_budget.is_empty());
        }
        assert_eq!(f.bus.mailbox_len("a"), 30);
        // dependency_manager too (it drives wakes)
        for _ in 0..30 {
            let mut m = msg(EventType::DependencyReady, "dependency_manager", "a", "");
            let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
            assert!(out.dropped_budget.is_empty());
        }
        assert_eq!(f.bus.mailbox_len("a"), 60);
    }

    #[test]
    fn wait_arms_a_durable_row_and_frees_the_agent() {
        let mut f = fixture(&["be"]);
        let entry = f
            .bus
            .wait_for(
                &mut f.journal,
                &mut f.registry,
                0.0,
                "be",
                "artifact:schema.sql",
                Some("t1"),
                "",
                Some(5.0),
            )
            .unwrap();
        let rows = f.journal.active_waits(Some("artifact:schema.sql")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].agent_id.as_str(), "be");
        assert_eq!(rows[0].timeout_at, Some(5.0));
        assert_eq!(
            f.bus.mailbox_len("be"),
            0,
            "waiting must not fill a mailbox"
        );
        assert_eq!(entry.wait_id.as_str(), "w-0001");
        // the registry record mirrors the pending wait
        let rec = f.registry.get("be").unwrap();
        assert_eq!(rec.pending_waits.len(), 1);
        assert_eq!(
            rec.pending_waits[0].get("wait_id").and_then(|v| v.as_str()),
            Some("w-0001")
        );
        // WAIT_REGISTERED is journaled with the reference payload shape
        let rows = f
            .journal
            .events(&EventFilter::new().etype("WAIT_REGISTERED"))
            .unwrap();
        assert!(rows[0]
            .payload_text
            .contains("\"condition\":\"artifact:schema.sql\""));
        assert!(rows[0].payload_text.contains("\"wait_id\":\"w-0001\""));
    }

    #[test]
    fn wait_ids_number_from_the_journal_count() {
        let mut f = fixture(&["a", "b"]);
        let e1 = f
            .bus
            .wait_for(
                &mut f.journal,
                &mut f.registry,
                0.0,
                "a",
                "c1",
                None,
                "",
                None,
            )
            .unwrap();
        let e2 = f
            .bus
            .wait_for(
                &mut f.journal,
                &mut f.registry,
                0.0,
                "b",
                "c2",
                None,
                "",
                None,
            )
            .unwrap();
        assert_eq!(e1.wait_id.as_str(), "w-0001");
        assert_eq!(e2.wait_id.as_str(), "w-0002");
    }

    #[test]
    fn resolve_wakes_only_the_interested() {
        let mut f = fixture(&["be", "unrelated"]);
        f.bus
            .wait_for(
                &mut f.journal,
                &mut f.registry,
                0.0,
                "be",
                "artifact:schema.sql",
                Some("t1"),
                "",
                None,
            )
            .unwrap();
        let woken = f
            .bus
            .resolve(
                &mut f.journal,
                &mut f.registry,
                "artifact:schema.sql",
                "published",
                None,
            )
            .unwrap();
        assert_eq!(woken, vec!["be"]);
        assert!(f.journal.active_waits(None).unwrap().is_empty());
        assert!(f
            .bus
            .mailbox("be")
            .iter()
            .any(|m| m.msg_type == EventType::DependencyReady));
        assert_eq!(
            f.bus.mailbox_len("unrelated"),
            0,
            "resolve must be targeted, not a broadcast"
        );
        // the registry wait entry is cleared
        assert!(f.registry.get("be").unwrap().pending_waits.is_empty());
        assert_eq!(f.bus.stats.wakes, 1);
        // journal order: WAIT_REGISTERED, DEPENDENCY_READY, WAIT_RESOLVED
        let etypes: Vec<String> = f
            .journal
            .iterate()
            .unwrap()
            .iter()
            .map(|r| r.etype.clone())
            .collect();
        assert_eq!(
            etypes,
            vec!["WAIT_REGISTERED", "DEPENDENCY_READY", "WAIT_RESOLVED"]
        );
    }

    #[test]
    fn timeout_escalates_instead_of_spinning() {
        let mut f = fixture(&["be"]);
        f.registry.get_mut("be").unwrap().lifecycle.state = AgentState::WaitingForDependency;
        // arm at t=1.0 with a 0.5s deadline (the reference advances first)
        assert!(f
            .bus
            .expire_timeouts(&mut f.journal, &mut f.registry, 0.0)
            .unwrap()
            .is_empty());
        let _ = f
            .bus
            .wait_for(
                &mut f.journal,
                &mut f.registry,
                1.0,
                "be",
                "artifact:never",
                None,
                "",
                Some(0.5),
            )
            .unwrap();
        assert!(f
            .bus
            .expire_timeouts(&mut f.journal, &mut f.registry, 1.0)
            .unwrap()
            .is_empty()); // not yet
        let expired = f
            .bus
            .expire_timeouts(&mut f.journal, &mut f.registry, 2.0)
            .unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].condition, "artifact:never");
        assert!(f.journal.active_waits(None).unwrap().is_empty());
        assert!(f
            .bus
            .mailbox("be")
            .iter()
            .any(|m| m.msg_type == EventType::WaitTimeout));
        let body = &f.bus.mailbox("be")[0].body;
        assert_eq!(body, "gave up waiting for artifact:never after 1.00s");
        // the wait row records the timeout, observable to supervision
        let rows = f.journal.all_waits().unwrap();
        assert_eq!(rows[0].state, "TIMED_OUT");
        assert_eq!(rows[0].wake_reason.as_deref(), Some("WAIT_TIMEOUT"));
    }

    #[test]
    fn poll_counter_default_is_zero_and_attributes_offenders() {
        let mut p = PollCounter::default();
        assert_eq!(p.polls, 0, "the default path never polls");
        p.hit("a").unwrap();
        p.hit("a").unwrap();
        p.hit("b").unwrap();
        assert_eq!(p.polls, 3);
        assert_eq!(p.offenders(), vec!["a", "b"]);
        assert_eq!(p.by_actor["a"], 2);
        p.reset();
        assert_eq!(p.polls, 0);
        assert!(p.offenders().is_empty());
    }

    #[test]
    fn polling_agent_can_be_hard_stopped() {
        let mut p = PollCounter {
            eagerness_limit: 2,
            ..Default::default()
        };
        assert!(p.hit("nerd").is_ok());
        assert!(p.hit("nerd").is_ok());
        let err = p.hit("nerd").unwrap_err();
        assert_eq!(
            err,
            BusError::PollingForbidden {
                actor: "nerd".into(),
                hits: 3,
                limit: 2
            }
        );
        assert_eq!(
            err.to_string(),
            "nerd polled 3 times (limit 2): use kernel.wait_for() instead of re-checking"
        );
    }

    #[test]
    fn snapshot_reports_delivery_stats_and_subscriptions() {
        let mut f = fixture(&["a", "b"]);
        let mut m = msg(EventType::DependencyRequest, "a", "b", "");
        f.bus.publish(&mut m, &mut f.journal).unwrap();
        let snap = f.bus.snapshot();
        assert_eq!(snap.get("published").unwrap().as_int(), Some(1));
        assert_eq!(snap.get("delivered").unwrap().as_int(), Some(1));
        assert!(snap.contains_key("subscriptions"));
        // parent's default subscription entry is observable
        let subs = snap.get("subscriptions").unwrap();
        assert!(subs
            .to_canon_string()
            .contains("\"parent\":[\"control.*\",\"dependency.*\",\"resource.*\"]"));
    }

    #[test]
    fn registration_carries_subscriptions_into_the_bus() {
        let mut f = fixture(&[]);
        f.registry
            .register(
                "x",
                "r",
                &["s"],
                0,
                "parent",
                "",
                Some(Lifecycle::with_state(AgentState::Idle, 0.0)),
                &["resource.*"],
            )
            .unwrap();
        let rec_subs = f.registry.get("x").unwrap().subscriptions.clone();
        let refs: Vec<&str> = rec_subs.iter().map(|s| s.as_str()).collect();
        f.bus.subscribe(&mut f.registry, "x", &refs);
        let pats = f.bus.patterns_for("x");
        assert!(pats.contains(&"resource.*".to_string()));
        assert_eq!(pats.len(), 4, "defaults + explicit");
    }

    #[test]
    fn subscribe_dedupes_and_mirrors_onto_the_registry() {
        let mut f = fixture(&["a"]);
        let cur = f.bus.subscribe(
            &mut f.registry,
            "a",
            &["resource.*", "resource.*", "control.x.*"],
        );
        assert_eq!(cur, vec!["resource.*", "control.x.*"]);
        let rec = f.registry.get("a").unwrap();
        assert_eq!(rec.subscriptions, vec!["resource.*", "control.x.*"]);
        // empty subscribe still creates the observable entry
        let _ = f.bus.subscribe(&mut f.registry, "ghost", &[]);
        let snap = f.bus.snapshot();
        assert!(JValue::Obj(snap).to_canon_string().contains("\"ghost\":[]"));
        // unsubscribe removes from both views
        f.bus.unsubscribe(&mut f.registry, "a", "resource.*");
        assert!(!f.bus.patterns_for("a").contains(&"resource.*".to_string()));
        assert!(!f
            .registry
            .get("a")
            .unwrap()
            .subscriptions
            .contains(&"resource.*".to_string()));
    }

    #[test]
    fn mailbox_drain_is_fifo_and_bounded() {
        let mut f = fixture(&["a", "b"]);
        for i in 0..5 {
            let mut m = msg(EventType::DependencyRequest, "b", "a", &format!("m{i}"));
            f.bus.publish(&mut m, &mut f.journal).unwrap();
        }
        assert_eq!(f.bus.mailbox_len("a"), 5);
        let first2 = f.bus.drain("a", Some(2));
        assert_eq!(first2.len(), 2);
        assert_eq!(first2[0].body, "m0");
        assert_eq!(first2[1].body, "m1");
        assert_eq!(f.bus.mailbox_len("a"), 3);
        let rest = f.bus.drain("a", None);
        assert_eq!(rest.len(), 3);
        assert_eq!(rest[0].body, "m2");
        assert_eq!(f.bus.mailbox_len("a"), 0);
        // draining an unknown actor is empty, not an error
        assert!(f.bus.drain("nobody", None).is_empty());
    }

    #[test]
    fn unregistered_recipient_is_silently_skipped_not_delivered() {
        // direct message to an actor without a mailbox: no recipient, no crash
        let mut f = fixture(&["a"]);
        let mut m = msg(EventType::DependencyRequest, "a", "ghost", "hi");
        let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
        assert!(out.recipients.is_empty());
        assert!(!out.delivered_any());
        // but the message itself is still journaled (no silent loss)
        assert_eq!(f.journal.count().unwrap(), 1);
    }

    #[test]
    fn repeated_publish_is_stateless_routing() {
        let mut f = fixture(&["a", "b"]);
        for _ in 0..3 {
            let mut m = msg(EventType::DependencyRequest, "a", "b", "again");
            let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
            assert_eq!(out.recipients, vec!["b"]);
        }
        assert_eq!(f.bus.mailbox_len("b"), 3);
        assert_eq!(f.bus.stats.delivered, 3);
    }
}
