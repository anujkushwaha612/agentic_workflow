"""Bus golden capture: exercises the observable bus semantics (routing,
subscriptions, planes, self-broadcast, depth cap, sender budget, durable
waits, wakes, timeouts, mailboxes, stats) and dumps them as JSON so the Rust
port (`tests/bus_parity.rs`) can be compared structurally against the Python
oracle.

Runtime-specific identifiers (message ids, correlation ids, journal hashes)
are excluded; every compared value is semantic and deterministic.

Regenerate: PYTHONPATH=. python3 tests/parity/bus_capture.py
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from arena.kernel import Kernel
from arena.lifecycle import AgentState, Lifecycle
from arena.message import MAX_CAUSAL_DEPTH, Message, MessageType
from arena.registry import SpawnBudget

FIX = Path(__file__).parent / "fixtures" / "bus_golden.json"


def _msgdict(m: Message) -> dict:
    return {"type": str(m.msg_type), "plane": str(m.plane), "topic": m.topic,
            "from": m.from_actor, "to": m.to_actor, "body": m.body,
            "task_id": m.task_id}


def _delivery(case: str, d) -> dict:
    return {"case": case, "recipients": list(d.recipients),
            "dropped_depth": list(d.dropped_depth),
            "dropped_budget": list(d.dropped_budget),
            "not_subscribed": list(d.not_subscribed)}


def _strip(payload: dict) -> dict:
    return {k: v for k, v in payload.items()
            if k not in ("correlation_id", "caused_by", "dropped")}


def main() -> int:
    k = Kernel(root=tempfile.mkdtemp(), clock_mode="virtual",
               budget=SpawnBudget(idle_ttl=1e9))
    out: dict = {"deliveries": []}

    agents = ["a", "b", "c", "flooder", "victim", "quiet", "loud", "x", "y",
              "be", "unrelated"]
    for aid in agents:
        k.registry.register(agent_id=aid, role="talker", skills=["t"], epoch=0,
                            lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
        k.queues.setdefault(aid, [])

    k.bus.subscribe("loud", ["resource.*"])
    k.bus.subscribe("ghost", [])  # empty subscribe still creates the entry
    out["patterns"] = {a: k.bus.patterns_for(a) for a in ("parent", "kernel", "loud", "quiet")}

    # ------------------------------------------------------------- routing
    out["deliveries"].append(_delivery(
        "direct_dependency", k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST,
                                                from_actor="a", to_actor="b", body="hi"))))
    out["deliveries"].append(_delivery(
        "broadcast_dependency", k.publish(Message(msg_type=MessageType.DEPENDENCY_READY,
                                                   from_actor="a", to_actor="broadcast",
                                                   body="schema is ready"))))
    out["deliveries"].append(_delivery(
        "progress_to_parent", k.publish(Message(msg_type=MessageType.TASK_PROGRESS,
                                                 from_actor="a", to_actor="parent", body="30%"))))
    res = Message(msg_type=MessageType.RESOURCE_UPDATED, from_actor="a",
                  to_actor="broadcast", resource="shared_types.ts", body="changed")
    out["deliveries"].append(_delivery("resource_broadcast", k.publish(res)))
    out["deliveries"].append(_delivery(
        "status_direct", k.publish(Message(msg_type=MessageType.STATUS_UPDATE,
                                            from_actor="parent", to_actor="be", body="hello"))))
    out["deliveries"].append(_delivery(
        "plan_gated", k.publish(Message(msg_type=MessageType.PLAN_AMENDED,
                                         from_actor="parent", to_actor="be", body="amend"))))
    out["deliveries"].append(_delivery(
        "parent_broadcast", k.publish(Message(msg_type=MessageType.PLAN_CREATED,
                                               from_actor="parent", to_actor="broadcast",
                                               body="plan"))))
    out["direct_topic"] = Message(msg_type=MessageType.DEPENDENCY_REQUEST,
                                  from_actor="a", to_actor="b").topic
    out["resource_topic"] = res.topic

    # ------------------------------------------------------- causal depth cap
    root = Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="x", to_actor="y")
    k.publish(root)
    cur, hops = root, 0
    while True:
        nxt = cur.child(MessageType.DEPENDENCY_REQUEST, cur.to_actor, cur.from_actor,
                        body=f"reply {hops}")
        if not k.publish(nxt).recipients:
            break
        cur, hops = nxt, hops + 1
        assert hops <= MAX_CAUSAL_DEPTH + 3, "loop guard failed to engage"
    out["depth"] = {"hops_delivered": hops, "dropped_depth_stat": k.bus.stats["dropped_depth"],
                    "budget_exceeded_events":
                        len(k.journal.events(etype=MessageType.BUDGET_EXCEEDED))}

    # ------------------------------------------------------ sender budget
    k.sent_this_tick.clear()
    delivered = dropped = 0
    last = None
    for i in range(40):
        d = k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="flooder",
                              to_actor="victim", body=f"spam {i}"))
        delivered += len(d.recipients)
        dropped += len(d.dropped_budget)
        last = d
    out["budget"] = {"delivered": delivered, "dropped": dropped,
                     "victim_queue_len": len(k.queues["victim"]),
                     "last_delivery": _delivery("flood_last", last)}

    # ------------------------------------------------------ durable waits
    k.clock.advance(1.0)  # now == 1.0
    e1 = k.bus.wait_for("be", "artifact:schema.sql", task_id="t1", timeout=5.0)
    e2 = k.bus.wait_for("unrelated", "artifact:schema.sql")
    e3 = k.bus.wait_for("quiet", "artifact:never", timeout=0.5)
    out["wait_ids"] = [e1["wait_id"], e2["wait_id"], e3["wait_id"]]
    out["timeout_at"] = [e1["timeout_at"], e2["timeout_at"], e3["timeout_at"]]
    out["expire_at_1.0"] = [r["condition"] for r in k.bus.expire_timeouts()]
    woken = k.bus.resolve("artifact:schema.sql", reason="published")
    out["woken"] = woken
    k.clock.advance(1.0)  # now == 2.0
    expired = k.bus.expire_timeouts()
    out["expired"] = [{"wait_id": r["wait_id"], "condition": r["condition"],
                       "agent": r["agent_id"]} for r in expired]
    out["active_waits_after"] = [dict(r) for r in k.journal.active_waits()]
    out["all_waits_states"] = [{"wait_id": r["wait_id"], "state": r["state"],
                                "wake_reason": r["wake_reason"]}
                               for r in k.journal._conn.execute("SELECT * FROM waits")]
    out["timeout_body"] = next(m.body for m in k.queues["quiet"]
                                if m.msg_type is MessageType.WAIT_TIMEOUT)
    out["registry_pending_after"] = {a: [w.get("wait_id") for w in r.pending_waits]
                                     for a, r in k.registry.agents.items()}

    # ------------------------------------------------------ journal evidence
    rows = list(k.journal.iterate())
    out["journal_events"] = [[r["seq"], r["etype"], r["actor"], r["target"] or "",
                              r["task_id"] or ""] for r in rows]
    def _payload(r: dict) -> dict:
        p = r["payload"]
        return _strip(p if isinstance(p, dict) else json.loads(p))

    out["budget_exceeded_payload"] = _payload(
        k.journal.events(etype=MessageType.BUDGET_EXCEEDED)[0])
    wr = [r for r in rows if r["etype"] == "WAIT_REGISTERED"]
    out["wait_registered_payloads"] = [_payload(r) for r in wr]
    wres = [r for r in rows if r["etype"] == "WAIT_RESOLVED"]
    out["wait_resolved_payloads"] = [_payload(r) for r in wres]

    # ------------------------------------------------------ mailboxes/stats
    out["mailboxes"] = {a: [_msgdict(m) for m in q] for a, q in k.queues.items()
                        if a not in ("kernel",)}  # kernel mailbox only gets broadcasts
    out["kernel_broadcast_count"] = len(k.queues["kernel"])
    out["stats"] = dict(k.bus.stats)
    out["snapshot"] = k.bus.snapshot()
    out["polls_default_zero"] = k.polls.polls

    with FIX.open("w") as f:
        json.dump(out, f, indent=1, sort_keys=True, default=str)
    print(f"wrote {FIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
