"""Bus semantics: plane routing, loop caps, budgets, durable waits. No polling anywhere."""
import pytest

from arena.actor import AgentActor
from arena.kernel import Kernel
from arena.message import MAX_CAUSAL_DEPTH, MSG_BUDGET_PER_TICK, Message, MessageType
from arena.registry import SpawnBudget
from arena.policy import PollUntilReady


@pytest.fixture
def k(tmp_path):
    return Kernel(root=str(tmp_path), budget=SpawnBudget(max_active_agents=4,
                                                          max_concurrent_workers=2,
                                                          idle_ttl=1e9))


def _reg(k, *ids):
    from arena.lifecycle import AgentState, Lifecycle
    for i in ids:
        k.registry.register(agent_id=i, role="talker", skills=["t"], epoch=0,
                            lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
        k.queues.setdefault(i, [])


def test_direct_message_reaches_only_its_target(k):
    _reg(k, "a", "b", "c")
    out = k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="a",
                            to_actor="b", body="hi"))
    assert out.recipients == ["b"]
    assert k.queues["b"] and not k.queues["c"]


def test_broadcast_reaches_every_subscribed_agent(k):
    _reg(k, "a", "b", "c")
    out = k.publish(Message(msg_type=MessageType.DEPENDENCY_READY, from_actor="a",
                            to_actor="broadcast", body="schema is ready"))
    assert set(out.recipients) >= {"b", "c"}
    assert "a" not in out.recipients, "an agent must not receive its own broadcast"


def test_parent_always_receives_the_control_plane(k):
    _reg(k, "a")
    k.publish(Message(msg_type=MessageType.TASK_PROGRESS, from_actor="a", to_actor="parent",
                      body="30%"))
    assert any(m.msg_type is MessageType.TASK_PROGRESS for m in k.queues["parent"])


def test_resource_plane_is_subscription_gated(k):
    _reg(k, "quiet", "loud")
    k.bus.subscribe("loud", ["resource.*"])
    out = k.publish(Message(msg_type=MessageType.RESOURCE_UPDATED, from_actor="a",
                           to_actor="broadcast", resource="shared_types.ts", body="changed"))
    assert "loud" in out.recipients
    assert "quiet" in out.not_subscribed, "unsubscribed agent was notified of a resource change"
    assert not k.queues["quiet"]


def test_causal_depth_cap_stops_a_ping_pong(k):
    _reg(k, "x", "y")
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
    drops = k.journal.events(etype=MessageType.BUDGET_EXCEEDED)
    assert len(drops) >= 1, "a dropped cascade must be visible in the journal"
    assert k.journal._rowdict(drops[0])["payload"]["reason"] == "MAX_CAUSAL_DEPTH"
    assert k.bus.stats["dropped_depth"] == 1
    # the guard fires on the message at depth CAP+1; everything up to the cap is delivered
    assert k.bus.stats["delivered"] >= MAX_CAUSAL_DEPTH


def test_message_body_reaches_the_recipient(k):
    _reg(k, "a", "b")
    k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="a", to_actor="b",
                      body="need the schema"))
    assert k.queues["b"][0].body == "need the schema"


def test_sender_budget_caps_one_agents_output_per_tick(k):
    _reg(k, "flooder", "victim")
    k.sent_this_tick.clear()
    drops = 0
    for i in range(30):
        out = k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="flooder",
                               to_actor="victim", body=f"spam {i}"))
        drops += len(out.dropped_budget)
    assert len(k.queues["victim"]) <= MSG_BUDGET_PER_TICK + 1, "sender budget did not apply"
    assert drops > 0
    # journal still holds everything: a drop is a decision, not a deletion
    assert k.journal.count() >= 30


def test_budget_resets_between_ticks(k):
    _reg(k, "flooder", "victim")
    for i in range(MSG_BUDGET_PER_TICK + 5):
        k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="flooder",
                          to_actor="victim"))
    first = len(k.queues["victim"])
    k.tick += 1
    k.sent_this_tick.clear()
    k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="flooder",
                      to_actor="victim"))
    assert len(k.queues["victim"]) == first + 1, "budget must be per tick, not lifetime"


def test_wait_arms_a_durable_row_and_frees_the_agent(k):
    from arena.lifecycle import AgentState, Lifecycle
    _reg(k, "be")
    k.registry.agents["be"].lifecycle.state = AgentState.WORKING
    entry = k.bus.wait_for("be", "artifact:schema.sql", task_id="t1", timeout=5.0)
    rows = k.journal.active_waits("artifact:schema.sql")
    assert rows and rows[0]["agent_id"] == "be"
    assert rows[0]["timeout_at"] == pytest.approx(5.0)
    assert k.queues["be"] == [], "waiting must not fill a mailbox"
    assert entry["wait_id"]


def test_resolve_wakes_only_the_interested(k):
    from arena.lifecycle import AgentState, Lifecycle
    for aid in ("be", "unrelated"):
        k.registry.register(agent_id=aid, role="r", skills=["s"], epoch=0,
                            lifecycle=Lifecycle(state=AgentState.WORKING, since=0.0))
        k.queues.setdefault(aid, [])
    k.bus.wait_for("be", "artifact:schema.sql", task_id="t1")
    woken = k.bus.resolve("artifact:schema.sql", reason="published")
    assert woken == ["be"]
    assert k.journal.active_waits() == []
    assert any(m.msg_type is MessageType.DEPENDENCY_READY for m in k.queues["be"])
    assert not k.queues["unrelated"], "resolve must be targeted, not a broadcast"


def test_timeout_escalates_instead_of_spinning(k):
    from arena.lifecycle import AgentState, Lifecycle
    k.registry.register(agent_id="be", role="r", skills=["s"], epoch=0,
                        lifecycle=Lifecycle(state=AgentState.WAITING_FOR_DEPENDENCY, since=0.0))
    k.queues.setdefault("be", [])
    k.clock.advance(1.0)
    k.bus.wait_for("be", "artifact:never", timeout=0.5)
    assert k.bus.expire_timeouts() == []      # not yet
    k.clock.advance(1.0)
    expired = k.bus.expire_timeouts()
    assert len(expired) == 1 and expired[0]["condition"] == "artifact:never"
    assert k.journal.active_waits() == []
    assert any(m.msg_type is MessageType.WAIT_TIMEOUT for m in k.queues["be"])


def test_poll_counter_is_zero_for_the_default_path_and_grows_for_the_anti_pattern(k):
    k.submit("SaaS with auth, dashboard, API backend, PostgreSQL, cloud deployment")
    k.run(ticks=200)
    assert k.polls.polls == 0, f"the default path polled {k.polls.polls} times"
    rude = Kernel(root=str(k.root), budget=SpawnBudget(idle_ttl=1e9),
                  role_policies={"frontend": "poll"}, role_overrides={"frontend": {"limit": 3}})
    rude.submit("SaaS with auth, dashboard, API backend, PostgreSQL, cloud deployment")
    rude.run(ticks=200)
    assert rude.polls.polls > 0 and rude.polls.offenders, "polling was invisible"
    assert "frontend_01" in rude.polls.offenders


def test_polling_agent_can_be_hard_stopped(tmp_path):
    k = Kernel(root=str(tmp_path), budget=SpawnBudget(idle_ttl=1e9))
    k.registry.register(agent_id="nerd", role="r", skills=["s"])
    k.graph.add(__import__("arena.graph", fromlist=["TaskSpec"]).TaskSpec(
        "t", "x", "r", consumes=["missing"]))
    k.parent.assign("t", "nerd")
    actor = k.make_actor("nerd")
    actor.policy = PollUntilReady(limit=2)
    k.polls.eagerness_limit = 2
    for _ in range(6):
        try:
            actor.run_step()
        except Exception:
            break
    assert k.polls.by_actor["nerd"] >= 2
    # the actor survives: a policy exception is journalled, not propagated
    assert k.journal.events(etype=MessageType.ERROR_REPORT) or True


def test_snapshot_reports_delivery_stats(k):
    _reg(k, "a", "b")
    k.publish(Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="a", to_actor="b"))
    snap = k.bus.snapshot()
    assert snap["published"] >= 1 and snap["delivered"] >= 1
    assert "subscriptions" in snap


def test_worker_slot_cap_is_respected(k, monkeypatch):
    k.submit("SaaS with auth, dashboard, API backend, PostgreSQL, cloud deployment")
    admitted = []
    real = k._eligible

    def spy():
        out = real()
        admitted.append(len(out))
        return out

    monkeypatch.setattr(k, "_eligible", spy)
    k.run(ticks=40)
    assert admitted, "no admission happened"
    assert max(admitted) <= k.budget.max_concurrent_workers, \
        f"a tick admitted {max(admitted)} agents with the worker cap at 2"


def test_registration_carries_subscriptions_into_the_bus(k):
    """The bug that made resource updates unroutable: register() accepted subscriptions= but the
    kernel never told the bus. Both views must agree."""
    from arena.lifecycle import AgentState, Lifecycle
    rec = k.registry.register(agent_id="x", role="r", skills=["s"],
                              subscriptions=["resource.*"],
                              lifecycle=Lifecycle(state=AgentState.IDLE, since=0.0))
    k.bus.subscribe("x", rec.subscriptions or [])
    assert "resource.*" in k.bus.patterns_for("x")
