"""The journal is the only source of truth, so: fold completeness, atomic claims, chain integrity."""
import json
import sqlite3

import pytest

from arena.journal import GENESIS, Journal
from arena.message import Message, MessageType


@pytest.fixture
def j(tmp_path):
    jr = Journal(path=tmp_path / "j.db", now_fn=lambda: 1.25)
    yield jr
    jr.close()


def test_append_sets_seq_ts_and_hash(j):
    m = j.emit(MessageType.STATUS_UPDATE, "agent_a", "parent", body="hello")
    assert m.seq == 1 and m.ts == 1.25
    row = j.events()[0]
    assert row["hash"] != GENESIS and row["prev_hash"] == GENESIS
    assert json.loads(row["payload"])["body"] == "hello"


def test_body_is_hoisted_for_readers(j):
    j.emit(MessageType.TASK_PROGRESS, "a", "parent", body="50%")
    d = j._rowdict(j.events()[0])
    assert d["body"] == "50%"
    assert d["payload"]["body"] == "50%"


def test_explicit_payload_is_merged_not_nested(j):
    j.emit(MessageType.RESOURCE_UPDATED, "a", "parent", resource="x", artifact="x", version=2)
    p = j._rowdict(j.events()[0])["payload"]
    assert "payload" not in p, f"payload got double-nested again: {p}"
    assert p["artifact"] == "x" and p["version"] == 2


def test_chain_detects_in_place_edit(j, tmp_path):
    for i in range(12):
        j.emit(MessageType.TASK_PROGRESS, "a", "parent", body=f"p{i}", payload={"i": i})
    assert j.verify_chain() == (True, "", -1)
    with sqlite3.connect(str(tmp_path / "j.db")) as raw:
        pl = raw.execute("SELECT payload FROM events WHERE seq=7").fetchone()[0]
        raw.execute("UPDATE events SET payload=? WHERE seq=7",
                    (json.dumps({**json.loads(pl), "i": 999}),))
    ok, why, where = j.verify_chain()
    assert not ok and where == 7 and "hash mismatch" in why


def test_replay_fold_projects_every_plane(j):
    j.emit(MessageType.AGENT_REGISTERED, "parent", "fe_01", agent_id="fe_01", role="frontend",
           skills=["react"], epoch=0, spawned_by="parent")
    j.emit(MessageType.STATE_TRANSITION, "fe_01", "parent", agent_id="fe_01", to="WORKING")
    j.emit(MessageType.PLAN_CREATED, "parent", "parent",
           tasks=[{"task_id": "t1", "title": "x", "role": "frontend", "skills": ["react"],
                   "produces": ["a.js"], "consumes": ["s.sql"], "est_work": 2.5, "deps": []}],
           rationale={})
    j.emit(MessageType.TASK_ASSIGNED, "parent", "fe_01", task_id="t1", owner="fe_01")
    j.emit(MessageType.TASK_STARTED, "fe_01", "parent", task_id="t1")
    j.emit(MessageType.RESOURCE_UPDATED, "fe_01", "parent", resource="a.js",
           artifact="a.js", version=1)
    j.emit(MessageType.TASK_COMPLETED, "fe_01", "parent", task_id="t1", owner="fe_01")
    f = j.fold()
    # TASK_COMPLETED closes the task and releases the agent's hat, but it does NOT change agent
    # state - lifecycle only moves on STATE_TRANSITION rows. This test has no COMPLETED
    # transition event, so a correct fold must still say WORKING.
    assert f["agents"]["fe_01"]["state"] == "WORKING"
    assert f["agents"]["fe_01"]["task_id"] is None
    j.emit(MessageType.STATE_TRANSITION, "fe_01", "parent", agent_id="fe_01",
           frm="WORKING", to="COMPLETED")
    assert j.fold()["agents"]["fe_01"]["state"] == "COMPLETED"
    assert f["tasks"]["t1"] == {**f["tasks"]["t1"], "status": "done", "owner": "fe_01"}
    assert f["tasks"]["t1"]["est_work"] == 2.5
    assert f["artifacts"] == {"a.js": 1}, "artifact versions must be a projection of the log"


def test_state_transition_projection_ignores_missing_agents(j):
    j.emit(MessageType.STATE_TRANSITION, "ghost", "parent", agent_id="ghost", to="WORKING")
    assert j.fold()["agents"] == {}


def test_claim_is_first_writer_wins_and_records_losers(j):
    ok, incumbent = j.claim("key1", "a1", "t1", 0.0)
    assert ok and incumbent == ""
    ok2, inc2 = j.claim("key1", "a2", "t1", 1.0)
    assert not ok2 and inc2 == "a1"
    claims = j.claims()
    assert claims["key1"]["owner"] == "a1" and claims["key1"]["losers"] == ["a2"]


def test_claim_survives_reopen(tmp_path):
    p = tmp_path / "k.db"
    a = Journal(path=p)
    a.claim("dup", "winner", "t", 0.0)
    a.close()
    b = Journal(path=p)
    ok, inc = b.claim("dup", "loser", "t", 1.0)
    assert not ok and inc == "winner", "claims must be durable, not per-connection"


def test_durable_wait_roundtrip_and_timeout_fields(j):
    j.arm_wait("w1", "agent_a", "artifact:schema", "t1", "c-1", 1.0, 3.0)
    w = j.active_waits("artifact:schema")[0]
    assert w["agent_id"] == "agent_a" and w["timeout_at"] == 3.0
    j.resolve_wait("w1", "RESOLVED", "published")
    assert j.active_waits() == []
    assert j.fold()["waits"] == {}


def test_truncate_after_lets_a_damaged_log_recover(j):
    for i in range(6):
        j.emit(MessageType.TASK_PROGRESS, "a", "parent", body=str(i))
    assert j.verify_chain()[0]
    assert j.truncate_after(3) == 3
    assert j.verify_chain()[0], "the intact prefix must still verify"
    j.emit(MessageType.TASK_PROGRESS, "a", "parent", body="new tail")
    assert j.verify_chain()[0]


def test_trace_follows_correlation_ids(j):
    root = Message(msg_type=MessageType.DEPENDENCY_REQUEST, from_actor="fe", to_actor="be",
                   body="need contract")
    j.append(root)
    j.append(root.child(MessageType.DEPENDENCY_READY, "be", "fe", body="ready"))
    j.append(root.child(MessageType.TASK_PROGRESS, "fe", "parent", body="resumed"))
    j.emit(MessageType.STATUS_UPDATE, "unrelated", "parent", body="noise")
    tr = j.trace(root.correlation_id)
    assert len(tr) == 3
    assert {r["actor"] for r in tr} == {"fe", "be"}
    assert tr[1]["depth"] == 1 and tr[2]["depth"] == 1


def test_fold_events_count_matches(j):
    j.emit(MessageType.STATUS_UPDATE, "a", "parent")
    assert j.fold()["events"] == j.count() == 1
