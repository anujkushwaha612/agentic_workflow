"""The DAG: dependencies must be DERIVED from artifacts, and every mutation revalidated."""
import pytest

from arena.graph import CycleError, DependencyGraph, TaskSpec


def _g():
    g = DependencyGraph()
    g.add(TaskSpec("db", "schema", "database", produces=["schema.sql"]))
    g.add(TaskSpec("api", "endpoints", "backend", consumes=["schema.sql"], produces=["api.json"]))
    g.add(TaskSpec("fe", "ui", "frontend", consumes=["api.json"]))
    g.add(TaskSpec("infra", "iac", "cloud", produces=["main.tf"]))
    return g


def test_edges_are_derived_not_declared():
    g = _g()
    assert g.tasks["api"].deps == {"db"}, "backend must be blocked on the schema it consumes"
    assert g.tasks["fe"].deps == {"api"}
    assert g.tasks["infra"].deps == set(), "independent work must stay independent"


def test_generations_expose_parallelism():
    g = _g()
    assert g.order() == [["db", "infra"], ["api"], ["fe"]], g.order()
    # readiness is per-task, and infra never waits on the schema chain
    assert "infra" in g.order()[0] and "db" in g.order()[0]


def test_ready_respects_owner_and_completion():
    g = _g()
    assert g.ready(require_owner=True) == [], "nothing is owned yet"
    for t in g.tasks.values():
        t.owner = "someone"
    assert g.ready() == ["db", "infra"]
    g.tasks["db"].status = "done"
    # db finished -> api is schedulable. Ownership is not a readiness gate: an agent with an
    # unmet *artifact* parks itself, which is what makes durable waiting possible.
    assert "api" in g.ready()
    assert "fe" not in g.ready(), "frontend may not start before the contract exists"


def test_add_edge_rolls_back_on_cycle():
    g = _g()
    with pytest.raises(CycleError) as ei:
        g.add_edge("db", "fe")
    assert {"db", "fe"} <= set(map(str, ei.value.cycle))
    assert "fe" not in g.tasks["db"].deps, "a rejected edge must not linger"


def test_amend_rolls_back_the_whole_mutation_on_cycle():
    g = _g()
    # x consumes api.json and db depends on x -> db -> x -> api -> db. Genuinely cyclic.
    res = g.amend([TaskSpec("x", "new", "misc", produces=["p"], consumes=["api.json"])],
                  {"db": ["x"]})
    assert res["ok"] is False and res["restored"] is True, res
    assert g.tasks["db"].deps == set(), "an edge survived the rollback"
    assert "x" not in g.tasks, "rollback was partial (task survived)"
    assert g.producers.get("p", set()) in (set(), None), "the producer index survived the rollback"
    assert g.order() == [["db", "infra"], ["api"], ["fe"]], "the graph was not restored exactly"


def test_amend_of_a_acyclic_expansion_is_kept():
    g = _g()
    res = g.amend([TaskSpec("x", "new", "misc", produces=["p"])], {"db": ["x"]})
    assert res["ok"] is True, res
    assert g.tasks["db"].deps == {"x"}
    assert g.tasks["x"].deps == set()
    assert "x" in g.order()[0], "an unblocked new task should join the first wave"


def test_amend_accepts_a_valid_expansion():
    g = _g()
    res = g.amend([TaskSpec("pay", "payments", "payments", consumes=["api.json"],
                            produces=["pay.json"])])
    assert res["ok"] and res["added"] == ["pay"]
    assert g.tasks["pay"].deps == {"api"}, "produced/consumed match must create the edge"
    assert g.order()[-1] == ["fe"] or "pay" in g.order()[-1]


def test_artifact_producer_index():
    g = _g()
    assert g.producers["schema.sql"] == {"db"}
    g.remove("db")
    assert g.producers["schema.sql"] == set()
    assert "db" not in g.tasks["api"].deps


def test_unmet_reports_named_blockers():
    g = _g()
    assert g.unmet("fe") == ["api:pending"], g.unmet("fe")   # api still open -> it blocks
    g.tasks["api"].status = "done"
    assert g.unmet("fe") == [], "a satisfied chain must report no blockers"
    g.tasks["api"].status = "running"
    assert g.unmet("fe") == ["api:running"]


def test_critical_path_measures_the_bottleneck():
    g = _g()
    assert g.critical_path_length() == 3   # db -> api -> fe


def test_wait_for_edges_and_cycle_detection():
    g = _g()
    g.add(TaskSpec("tx", "x", "a", owner="a1", produces=["px"]))
    g.add(TaskSpec("ty", "y", "b", owner="b1", produces=["py"]))
    agents = {"a1": {"state": "WAITING_FOR_DEPENDENCY", "waits": [{"task_id": "ty"}]},
              "b1": {"state": "WAITING_FOR_DEPENDENCY", "waits": [{"task_id": "tx"}]}}
    edges = DependencyGraph.wait_for_edges(agents, g)
    assert edges == {"a1": {"b1"}, "b1": {"a1"}}
    cycles = DependencyGraph.find_cycles(edges)
    assert cycles and set(cycles[0]) == {"a1", "b1"}


def test_self_edge_is_not_reported_as_deadlock():
    edges = {"a1": {"a1"}}
    assert DependencyGraph.find_cycles(edges) == [], "awaiting yourself is a bug, not a deadlock"


def test_no_cycle_after_one_side_releases():
    g = _g()
    g.add(TaskSpec("tx", "x", "a", owner="a1"))
    g.add(TaskSpec("ty", "y", "b", owner="b1"))
    agents = {"a1": {"state": "WAITING_FOR_DEPENDENCY", "waits": [{"task_id": "ty"}]},
              "b1": {"state": "IDLE", "waits": []}}
    assert DependencyGraph.find_cycles(DependencyGraph.wait_for_edges(agents, g)) == []


def test_remaining_work_tracks_open_tasks():
    g = _g()
    before = g.remaining_work()
    g.tasks["db"].status = "done"
    assert g.remaining_work() == before - 1.0
