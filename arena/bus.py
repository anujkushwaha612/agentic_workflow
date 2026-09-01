"""Message bus delivery policy (your §3, §4, §6) - the part of the bus that must exist in Phase 1.

Phase 1 ships the *semantics* (planes, subscriptions, loop guard, budgets, durable wakes) on an
in-kernel transport. Phase 3 swaps the transport for cross-process ZeroMQ XSUB/XPUB (already
measured here at ~91k pub/s) without changing a single policy rule, because policies only ever
call ctx.publish(...) / kernel.wait_for(...).

Waits carry a deadline. The kernel owns expiry (`expire_timeouts`), so a `wait_timeout` is one
clock-driven scan per tick and never a busy loop: the woken agent gets a targeted message and the
Parent gets an escalation instead of an agent dying on a timeout.

Two rules that prevent the chaos you were worried about:

  * Plane separation. A RESOURCE/ARTIFACT broadcast is never delivered as a generic agent
    broadcast - an agent receives it only if it explicitly subscribed to that topic. This kills
    "shared_types.ts changed -> 12 agents wake up and reply to each other".
  * Causal depth cap. Replies must be built with Message.child(), which inherits causal_depth.
    A -> B -> A ping-pong is therefore capped structurally, and drops are journaled so you can
    see what would have been a runaway.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .message import MAX_CAUSAL_DEPTH, MSG_BUDGET_PER_TICK, Message, MessageType, Plane


@dataclass
class PollCounter:
    """Instrumentation for the 'event-driven, not polling' requirement.

    A policy that *asks* "is it ready?" instead of declaring a durable wait increments this.
    The chaos suite asserts counter == 0 for the polite path and > 0 for the anti-pattern path,
    so 'no polling' is a measured property of the system, not a claim in a README.
    """

    polls: int = 0
    by_actor: dict[str, int] = field(default_factory=dict)
    #: how many times an agent looked at a dependency before giving up
    eagerness_limit: int = 0

    def hit(self, actor: str) -> int:
        self.polls += 1
        self.by_actor[actor] = self.by_actor.get(actor, 0) + 1
        if self.eagerness_limit and self.by_actor[actor] > self.eagerness_limit:
            raise PollingForbidden(actor, self.by_actor[actor], self.eagerness_limit)
        return self.polls

    def reset(self) -> None:
        self.polls = 0
        self.by_actor.clear()

    @property
    def offenders(self) -> list[str]:
        return sorted(self.by_actor)


class PollingForbidden(Exception):
    def __init__(self, actor: str, hits: int, limit: int) -> None:
        super().__init__(f"{actor} polled {hits} times (limit {limit}): "
                         f"use kernel.wait_for() instead of re-checking")


@dataclass
class Delivery:
    recipients: list[str] = field(default_factory=list)
    dropped_depth: list[str] = field(default_factory=list)
    dropped_budget: list[str] = field(default_factory=list)
    not_subscribed: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.recipients)


@dataclass
class Bus:
    kernel: "Any"
    #: topic patterns an actor receives. The parent is the only full-visibility subscriber.
    # segment 1 is the plane, so a category filter must be qualified: 'dependency.*' matches
    # every dependency-plane topic; a bare 'conflict.*' would match nothing.
    default_agent_patterns: tuple[str, ...] = ("control.task.*", "control.agent.*",
                                               "dependency.*")
    parent_patterns: tuple[str, ...] = ("control.*", "dependency.*", "resource.*")
    stats: dict[str, int] = field(default_factory=lambda: {
        "published": 0, "delivered": 0, "dropped_depth": 0, "dropped_budget": 0,
        "dropped_unsubscribed": 0, "wakes": 0})

    # ------------------------------------------------------------ subscriptions
    def subscribe(self, actor: str, patterns: Iterable[str]) -> list[str]:
        """The ONE way to express interest. Mirrored onto the AgentRecord so a snapshot/replay
        and the bus can never disagree about who subscribed to what."""
        cur = self.kernel.subscriptions.setdefault(actor, [])
        for p in patterns:
            if p not in cur:
                cur.append(p)
        rec = self.kernel.registry.get(actor)
        if rec is not None:
            for p in patterns:
                if p not in rec.subscriptions:
                    rec.subscriptions.append(p)
        return list(cur)

    def unsubscribe(self, actor: str, pattern: str) -> None:
        cur = self.kernel.subscriptions.get(actor, [])
        if pattern in cur:
            cur.remove(pattern)
        rec = self.kernel.registry.get(actor)
        if rec is not None and pattern in rec.subscriptions:
            rec.subscriptions.remove(pattern)

    def patterns_for(self, actor: str) -> list[str]:
        if actor == "parent":
            return list(self.parent_patterns)
        if actor == "kernel":
            return ["*"]
        return list(self.default_agent_patterns) + list(self.kernel.subscriptions.get(actor, []))

    # ---------------------------------------------------------------- routing
    def publish(self, msg: Message) -> Delivery:
        self.kernel.journal.append(msg)
        self.stats["published"] += 1
        d = Delivery()
        if msg.causal_depth > MAX_CAUSAL_DEPTH:
            self.stats["dropped_depth"] += 1
            self.kernel.journal.emit(MessageType.BUDGET_EXCEEDED, msg.from_actor, "parent",
                                     body=f"dropped {msg.msg_type}: causal depth "
                                          f"{msg.causal_depth} > {MAX_CAUSAL_DEPTH}",
                                     correlation_id=msg.correlation_id, caused_by=msg.mid,
                                     reason="MAX_CAUSAL_DEPTH", dropped=msg.mid)
            return d

        targets = [msg.to_actor] if msg.to_actor not in ("broadcast", "*", "") else \
            list(self.kernel.recipients())
        for actor in targets:
            # Skip an agent receiving its own broadcast (noise reduction). Do NOT skip
            # orchestration senders: parent -> agent assignments must be delivered.
            if actor == msg.from_actor and actor not in ("parent", "dependency_manager", "kernel"):
                continue
            if actor not in self.kernel.queues:
                continue
            if msg.plane in (Plane.RESOURCE,) and actor not in ("parent", "kernel"):
                # resource plane is subscription-gated, never a free-for-all
                pats = self.patterns_for(actor)
                if not any(msg.matches(p) for p in pats):
                    self.stats["dropped_unsubscribed"] += 1
                    d.not_subscribed.append(actor)
                    continue
            elif actor not in ("parent", "kernel") and not any(
                    msg.matches(p) for p in self.patterns_for(actor)):
                d.not_subscribed.append(actor)
                self.stats["dropped_unsubscribed"] += 1
                continue
            # Budget is the SENDER's outbound allowance, not the recipient's inbound capacity -
            # enforcing it on the recipient meant a flood was never actually capped.
            if msg.from_actor not in ("parent", "kernel", "dependency_manager"):
                used = self.kernel.sent_this_tick.get(msg.from_actor, 0)
                if used > MSG_BUDGET_PER_TICK:
                    self.stats["dropped_budget"] += 1
                    d.dropped_budget.append(actor)
                    continue
            self.kernel.queues[actor].append(msg)
            d.recipients.append(actor)
            self.stats["delivered"] += 1
        return d

    # -------------------------------------------------------- durable waiting
    def wait_for(self, actor: str, condition: str, *, task_id: str | None = None,
                 correlation_id: str = "", timeout: float | None = None) -> dict[str, Any]:
        """Park the agent on a condition. No queue is held open, nothing is checked again.

        The wait is a *row in the journal*, so a blocked agent costs zero runtime and can be
        woken even by a process that restarts after the VM was recycled.
        """
        now = self.kernel.now
        wait_id = f"w-{len(self.kernel.journal.events(etype=MessageType.WAIT_REGISTERED)) + 1:04d}"
        timeout_at = now + timeout if timeout is not None else None
        self.kernel.journal.arm_wait(wait_id, actor, condition, task_id, correlation_id,
                                     now, timeout_at)
        rec = self.kernel.registry.get(actor)
        entry = {"wait_id": wait_id, "condition": condition, "task_id": task_id,
                 "armed_at": now, "timeout_at": timeout_at}
        if rec is not None:
            rec.pending_waits.append(entry)
        self.kernel.journal.emit(MessageType.WAIT_REGISTERED, actor, "parent",
                                 body=f"waiting for {condition}", task_id=task_id,
                                 wait_id=wait_id, condition=condition, timeout_at=timeout_at)
        return entry

    def resolve(self, condition: str, *, reason: str = "satisfied",
                 payload: dict[str, Any] | None = None) -> list[str]:
        """Publish-side wake. Returns the agents resumed. Exactly one lookup, no timers spinning."""
        woken: list[str] = []
        for row in self.kernel.journal.active_waits(condition):
            self.kernel.journal.resolve_wait(row["wait_id"], "RESOLVED", reason)
            rec = self.kernel.registry.get(row["agent_id"])
            if rec is not None:
                rec.pending_waits = [w for w in rec.pending_waits
                                     if w.get("wait_id") != row["wait_id"]]
            msg = Message(
                msg_type=MessageType.DEPENDENCY_READY, from_actor="dependency_manager",
                to_actor=row["agent_id"], body=f"{condition} is ready ({reason})",
                task_id=row["task_id"], correlation_id=row["correlation_id"],
                payload=dict(payload or {}, wait_id=row["wait_id"], condition=condition))
            self.publish(msg)
            self.kernel.journal.emit(MessageType.WAIT_RESOLVED, "dependency_manager",
                                     row["agent_id"], body=f"{condition} -> {reason}",
                                     wait_id=row["wait_id"], task_id=row["task_id"],
                                     correlation_id=row["correlation_id"])
            self.stats["wakes"] += 1
            woken.append(row["agent_id"])
        return woken

    def expire_timeouts(self) -> list[dict[str, Any]]:
        """WAIT_TIMEOUT -> escalate (§6/§15). One scan per tick, driven by the clock, not a loop."""
        expired = []
        now = self.kernel.now
        for row in self.kernel.journal.active_waits():
            if row["timeout_at"] is not None and now >= row["timeout_at"]:
                self.kernel.journal.resolve_wait(row["wait_id"], "TIMED_OUT", "WAIT_TIMEOUT")
                rec = self.kernel.registry.get(row["agent_id"])
                if rec is not None:
                    rec.pending_waits = [w for w in rec.pending_waits
                                         if w.get("wait_id") != row["wait_id"]]
                msg = Message(msg_type=MessageType.WAIT_TIMEOUT, from_actor="dependency_manager",
                              to_actor=row["agent_id"],
                              body=f"gave up waiting for {row['condition']} after "
                                   f"{now - row['armed_at']:.2f}s",
                              task_id=row["task_id"], correlation_id=row["correlation_id"],
                              payload={"wait_id": row["wait_id"], "condition": row["condition"]})
                self.publish(msg)
                expired.append(row)
        return expired

    def snapshot(self) -> dict[str, Any]:
        return dict(self.stats) | {"polls": self.kernel.polls.polls,
                                    "poll_offenders": self.kernel.polls.offenders,
                                    "subscriptions": {k: list(v) for k, v in
                                                     self.kernel.subscriptions.items()}}
