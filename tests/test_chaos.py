"""Every chaos scenario also runs as a pytest case, so CI and the report can never disagree."""
import pytest

from arena.chaos.chaos_kernel import SCENARIOS, run_scenarios

BY_ID = {s.id: s for s in SCENARIOS}


@pytest.mark.parametrize("scenario_id", [s.id for s in SCENARIOS], ids=[s.id for s in SCENARIOS])
def test_chaos_scenario(scenario_id):
    sc = BY_ID[scenario_id]
    try:
        evidence = sc.fn()
    except AssertionError as e:
        pytest.fail(f"[{sc.id}] {e}", pytrace=False)
    assert isinstance(evidence, dict) and evidence, f"[{sc.id}] returned no evidence"


def test_all_scenarios_pass_and_report_is_clean():
    results = run_scenarios()
    bad = [r for r in results if not r.ok]
    assert not bad, "failing scenarios: " + "; ".join(f"{r.scenario.id}: {r.failure}" for r in bad)
    assert len(results) >= 20, f"only {len(results)} scenarios - the suite must not shrink"


def test_no_scenario_swallows_its_evidence():
    """A 'passing' scenario that proves nothing is worse than a failing one."""
    for sc in SCENARIOS:
        ev = sc.fn()
        assert len(ev) >= 2, f"{sc.id} produced only {list(ev)}"


def test_every_guard_in_the_spec_is_covered():
    """Every safeguard from your §15 must be (a) named somewhere in the codebase docs and
    (b) exercised by a scenario. This fails if someone deletes a scenario or its documentation,
    which is the failure mode that actually happens: the guard stays, the proof quietly vanishes.
    """
    import arena
    doc_text = ""
    for mod in ("kernel", "bus", "graph", "journal", "message", "registry", "parent", "policy",
                "actor", "lifecycle", "spawn"):
        doc_text += (getattr(__import__(f"arena.{mod}", fromlist=[mod]), "__doc__", "") or "")
    scenario_text = " ".join(s.id + " " + s.title + " " + " ".join(s.guards)
                             for s in SCENARIOS).lower()
    required = {
        "max_active_agents": "spawn cap", "max_spawn_epoch": "spawn depth",
        "duplicate_claim": "duplicate work", "deadlock": "deadlock prevention",
        "causal_depth": "infinite message loops", "wait_timeout": "stuck agents",
        "cycle": "circular dependencies", "correlation_id": "correlation ids",
        "budget": "unnecessary communication", "idle": "idle termination",
    }
    missing_proof = [k for k in required if k.lower() not in scenario_text]
    missing_doc = [k for k in required if k.lower() not in doc_text.lower()
                   and k.lower().replace("_", " ") not in doc_text.lower()]
    assert not missing_proof, f"§15 safeguards with no chaos scenario: {missing_proof}"
    assert not missing_doc, f"§15 safeguards not explained in the code: {missing_doc}"
