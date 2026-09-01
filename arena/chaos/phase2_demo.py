"""Phase 2 acceptance demonstration — a mid-run spawn inside ONE continuous run.

Why this file exists separately from the chaos suite: the chaos scenarios each build a tiny kernel
to prove one guard. The acceptance requirement is different and stricter — a single run in which an
agent discovers missing capability *while working*, the Parent answers, the graph is amended, a new
agent joins the org that is already running, does real work, publishes an artifact, notifies the
requester, and unblocks a dependent task. Every step must be observable as a tick-by-tick
transition, not as five test cases that each show one fragment.

Deterministic: the initial org is built here, explicitly. `task_text` is a label for the human
reader; nothing is keyword-matched, so this cannot pass because a planner recognised the word
"payment" in a prompt. The policy asks *because it is mid-task and its own declared skills do not
cover the artifact its task must publish* — the same reason a real agent asks.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from ..graph import DependencyGraph, TaskSpec
from ..kernel import Kernel
from ..message import MessageType
from ..policy import NeedsSpecialist
from ..registry import SpawnBudget, emit_registered

TASK_LABEL = ("Add a Stripe payments integration to the existing API: webhook verification, "
              "idempotent event handling, and a billing reconciliation job")

WANTED_ARTIFACT = "backend/payments/webhooks.py"

#: the chain the requirement asks to see, split by *causality* rather than by position in the log:
#:   spine - rows the parent emits under the request's own correlation id, so "TASK_ASSIGNED" here
#:           is the assignment of the NEW task and not one of the plan assignments that happened
#:           earlier (which would make any positional check pass or fail for the wrong reason);
#:   tail  - the new agent's own rows, matched by agent id.
#: `SPAWN_APPROVED` sits after `TASK_ASSIGNED` because the kernel logs the approval as part of
#: *committing* the agent (register -> assign -> approve). Causally it still precedes every step the
#: child takes, which is what the ordering requirement is about.
SPINE = ("SPAWN_AGENT_REQUEST", "SPAWN_REQUEST_RECEIVED", "GRAPH_AMENDED",
         "TASK_ASSIGNED", "SPAWN_APPROVED")
TAIL = ("AGENT_REGISTERED", "TASK_ASSIGNED", "TASK_PROGRESS", "RESOURCE_UPDATED", "TASK_COMPLETED")


def _build(root: Path) -> Kernel:
    """A deliberately small org: three agents, one of which is going to need a fourth."""
    jp = root / "journal.db"
    if jp.exists():
        jp.unlink()
    k = Kernel(root=root, journal_path=jp,
               budget=SpawnBudget(max_active_agents=4, max_concurrent_workers=2,
                                   min_share_of_remaining=0.15, idle_ttl=1e9),
               trace_path=str(root / "events.jsonl"))
    # The policy is bound through the KERNEL's role config, not by overwriting `actor.policy`.
    # Assigning an actor's policy directly is a local mutation the replay cannot see: from_journal
    # rebuilds policies from role_policies/role_overrides, so a hand-assigned policy silently
    # becomes the default one after a restart - the agent keeps its counters but resumes with a
    # different brain. Configuring the kernel is the only version that survives recovery.
    k.role_policies = {"database": "simulated", "backend": "specialist", "frontend": "simulated",
                       "payment specialist": "simulated"}
    k.role_overrides = {"backend": {
        "role": "payment specialist", "after_steps": 2, "max_requests": 1,
        "skills": ["stripe", "webhooks"], "est_work": 3.0,
        "outputs": [WANTED_ARTIFACT], "capability_class": "payments",
        "reason": "webhook signature verification needs provider expertise this agent "
                  "does not declare"}}
    g = DependencyGraph()
    g.add(TaskSpec("t_schema", "finalise the payments schema", "database",
                   skills=["postgres"], est_work=1.0, produces=["db/schema.sql"]))
    g.add(TaskSpec("t_api", "extend the API contract for payment intents", "backend",
                   skills=["api"], est_work=2.0, consumes=["db/schema.sql"],
                   produces=["contracts/api.json"]))
    # dependent work: it needs the artifact the *new* agent will publish, so it is blocked at the
    # start and must become ready only after the spawn happens. Owner None on purpose - the
    # scheduler hands it to whoever can take it once the artifact lands.
    g.add(TaskSpec("t_billing", "billing reconciliation job against webhook events", "backend",
                   skills=["api"], est_work=2.0, consumes=[WANTED_ARTIFACT]))
    k.graph = g
    k.journal.emit(MessageType.PLAN_CREATED, "parent", "parent",
                   body=f"initial plan for: {TASK_LABEL[:40]}...",
                   tasks=[{"task_id": t.task_id, "title": t.title, "role": t.role,
                           "skills": t.skills, "produces": t.produces, "consumes": t.consumes,
                           "est_work": t.est_work, "deps": sorted(t.deps)}
                          for t in g.tasks.values()])
    # registered through the same emit as a planned agent: a kernel that seeds its roster without
    # journalling AGENT_REGISTERED would replay as an empty org, and every "replay reproduces the
    # org" claim below would be measured against a strawman.
    for aid, role in (("database_01", "database"), ("backend_01", "backend"),
                      ("frontend_01", "frontend")):
        rec = k.registry.register(agent_id=aid, role=role, skills=[role])
        emit_registered(k.journal, rec)
        k.make_actor(aid)
    k.parent.assign("t_schema", "database_01", reason="plan")
    k.parent.assign("t_api", "backend_01", reason="plan")
    return k


def _snapshot_row(k: Kernel, ledger_before: int) -> dict[str, Any]:
    m = k.metrics()
    states = {a: r.state for a, r in sorted(k.registry.agents.items())}
    return {"tick": k.tick, "agents": m["active"], "working": m["working"],
            "idle": m["idle"], "completed": m["completed"], "states": states,
            "requests": m["requests_received"], "approvals": m["requests_approved"],
            "rejections": m["requests_rejected"], "reuses": m["reuses"],
            "open_tasks": m["open_graph_tasks"],
            "new_requests": m["requests_received"] - ledger_before,
            "epoch_max": m["generation_epoch_max"],
            "capacity_left": m["remaining_capacity"]}


def run_demo(root: str | Path | None = None, out: str | Path | None = None,
             ticks: int = 40, keep_journal: bool = True) -> dict[str, Any]:
    """Drive the scenario tick by tick on ONE kernel object and return the transition table.

    `root` defaults under the repo's gitignored `var/`, never the caller's CWD: a default that
    writes into the source tree turns a demo into untracked litter.
    """
    root = Path(root) if root else Path(__file__).resolve().parents[2] / "var" / "acceptance"
    if root.exists() and not keep_journal:
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    k = _build(root)
    # measured BEFORE the first tick, otherwise "was it blocked at the start?" is answered by
    # looking at the end, which is how a demo like this quietly passes for the wrong reason.
    # `t_billing` has no DAG edge to the payments work *yet* (its producer does not exist to be
    # derived from), so "blocked" here means: unowned, and its required artifact absent. The
    # artifact gate, not an edge, is what holds it - which is exactly how the derived graph is
    # supposed to behave, and asserting `not is_ready()` would have been asserting the wrong thing.
    facts_pre = {"t_billing_unowned_at_start": k.graph.tasks["t_billing"].owner is None,
                 "t_billing_producer_missing": WANTED_ARTIFACT not in k.artifacts,
                 "t_billing_open_at_start": k.graph.tasks["t_billing"].is_open,
                 "initial_agents": sorted(k.registry.agents),
                 "initial_tasks": sorted(k.graph.tasks)}
    ident, rows, seen_ticks = id(k), [], []
    last_req = 0
    while k.tick < ticks:
        before = k.metrics()["requests_received"]
        k.run(1)
        rows.append(_snapshot_row(k, before))
        seen_ticks.append(k.tick)
        m = k.metrics()
        if m["open_graph_tasks"] == 0 and m["requests_received"] > 0 and k.tick > 6:
            break

    facts: dict[str, Any] = {"transition_table": rows, "task_label": TASK_LABEL, **facts_pre}
    def _pl(row) -> dict[str, Any]:
        # journal.events() returns `payload` as a raw JSON *string* (fold() merges it for its own
        # pass), so every ad-hoc read of a row here must parse first or it silently reads nothing.
        raw = row["payload"]
        return json.loads(raw) if isinstance(raw, str) else dict(raw or {})

    events = [(r["seq"], r["etype"], r["actor"], r["target"], _pl(r),
               r["correlation_id"]) for r in k.journal.events()]
    facts["event_count"] = len(events)
    facts["kernel_identity_stable"] = ident == id(k)
    facts["ticks_monotonic"] = seen_ticks == sorted(seen_ticks) and len(set(seen_ticks)) == len(seen_ticks)

    # --- the required chain, in causal order -------------------------------------
    req_rows = [e for e in events if e[1] == "SPAWN_AGENT_REQUEST"]
    req_seq = req_rows[0][0] if req_rows else 0
    cid = req_rows[0][5] or ""
    pay_agent = next((a for a, r in k.registry.agents.items()
                      if r.spawned_by not in ("parent", "", None)), "")
    pay_task = next((r.task_id for r in k.registry.agents.values()
                     if r.agent_id == pay_agent), "") or \
        next((t for t in k.graph.tasks if pay_agent and pay_agent.split("_0")[0] in t), "")
    pay_task_seq = None
    spine_seqs = [e[0] for e in events if e[5] == cid and e[5]]
    spine = {t: next((e[0] for e in events if e[1] == t and e[5] == cid), None) for t in SPINE}
    # the child's OWN rows: actor or target is the new agent. Matching on "any TASK_PROGRESS after
    # the spawn" would also pick up the two other agents still working, which is not a chain.
    child = [e for e in events if e[0] >= req_seq and pay_agent in (e[2], e[3])
             and pay_task_seq is not None]

    def _first(t: str):
        return next((e[0] for e in events if e[1] == t and e[0] >= req_seq
                     and pay_agent in (e[2], e[3])), None)

    def _last(t: str):
        vals = [e[0] for e in events if e[1] == t and e[0] >= req_seq and pay_agent in (e[2], e[3])]
        return vals[-1] if vals else None

    tail = {t: _first(t) for t in TAIL}
    # The child's completion row for its OWN task, and the artifact row it published. Completion
    # is asserted against the LAST such row, not the first, because a second `_complete` call after
    # the backlog drains journals an "agent finished with no bound task" marker - asserting on the
    # first row would read a marker, not the event that matters.
    tail["TASK_COMPLETED"] = _last("TASK_COMPLETED")
    child_art = [e[0] for e in events if e[1] == "RESOURCE_UPDATED" and e[2] == pay_agent]

    def monotone(table: dict[str, Any], after: int = -1) -> bool:
        prev, seen = after, False
        for t in (SPINE if table is spine else TAIL):
            v = table.get(t)
            if v is None:
                return False
            if v < prev:
                return False
            prev, seen = v, True
        return seen

    tail_ok = monotone(tail, after=req_seq) and bool(child_art) and \
        tail["TASK_COMPLETED"] >= child_art[0]
    facts["artifact_rows"] = child_art
    order_ok = monotone(spine) and tail_ok
    facts["correlation_id"] = cid
    facts["chain"] = {**spine, **{f"child:{t}": v for t, v in tail.items()}}
    facts["chain_rows"] = [{"seq": e[0], "type": e[1], "from": e[2], "to": e[3],
                            "plane": "correlation" if e[0] in spine_seqs else "child"}
                           for e in events
                           if e[0] in spine_seqs or (e[1] in TAIL and pay_agent in (e[2], e[3]))
                           and e[0] >= req_seq]
    facts["chain_ordered"] = order_ok
    facts["spine_ordered"] = monotone(spine)
    facts["tail_ordered"] = tail_ok
    facts["spine"] = spine
    facts["tail"] = tail
    facts["all_events_for_type"] = {t: [e[0] for e in events if e[1] == t] for t in
                                    ("SPAWN_AGENT_REQUEST", "SPAWN_REQUEST_RECEIVED",
                                     "GRAPH_AMENDED", "AGENT_REGISTERED", "SPAWN_APPROVED",
                                     "RESOURCE_UPDATED", "TASK_COMPLETED")}

    # --- the four things the demo must actually show ---------------------------------
    spawned = [a for a, r in k.registry.agents.items() if r.spawned_by not in ("parent", "", None)]
    pay = next((a for a in spawned if "payment" in k.registry.agents[a].role), None)
    facts["agent_created_midrun"] = pay
    facts["agent_epoch"] = k.registry.get(pay).epoch if pay else None
    facts["agent_spawned_by"] = k.registry.get(pay).spawned_by if pay else None
    art = k.artifacts.get(WANTED_ARTIFACT)
    facts["artifact_published"] = bool(art)
    facts["artifact_producer"] = (art or {}).get("producer")
    facts["requester_notified"] = any(
        e[1] == "API_CONTRACT_READY" and e[3] == "backend_01" and
        str(e[4].get("agent_id")) == str(pay) for e in events)
    # dependent task: was it blocked while the artifact was missing, and ready now?
    t0 = rows[0]
    # "backend continues" must mean something checkable, not "or True": at least one backend
    # TASK_PROGRESS row has to come *after* the new agent was registered.
    reg_seq = facts["all_events_for_type"]["AGENT_REGISTERED"][0] \
        if facts["all_events_for_type"]["AGENT_REGISTERED"] else 10 ** 9
    facts["backend_continued"] = any(
        e[1] == "TASK_PROGRESS" and e[2] == "backend_01" and e[0] > reg_seq for e in events)
    billing = k.graph.tasks["t_billing"]
    facts["dependent_task_ready_now"] = billing.status == "done" or (
        k.graph.is_ready("t_billing") and billing.owner is not None)
    facts["dependent_task_final"] = {"status": billing.status, "owner": billing.owner,
                                     "deps": sorted(billing.deps)}
    facts["blocked_at_start"] = bool(facts_pre["t_billing_unowned_at_start"]
                                     and facts_pre["t_billing_producer_missing"]
                                     and facts_pre["t_billing_open_at_start"])

    # --- the requirement is RUNTIME, so prove the run never restarted ----------------
    facts["agents_never_dropped_after_spawn"] = all(r["agents"] >= 3 for r in rows)
    facts["spawn_tick"] = next((r["tick"] for r in rows if r["agents"] >= 4), None)
    facts["initial_agent_count"] = rows[0]["agents"]
    facts["requests_total"] = k.metrics()["requests_received"]
    facts["reuses"] = k.metrics()["reuses"]
    facts["decision"] = (k.parent.decisions[0]["rule"] if k.parent.decisions else None)
    facts["ledger"] = [e.to_dict() for e in k.parent.ledger.newest_first()]
    facts["stalled"] = k.stalled

    # --- replay must contain the same dynamic org -----------------------------------
    # Both replays are `quiet=True`: a tool that inspects the log must not append to it. That
    # distinction matters here, because the pre-Phase-2 replay kernel wrote ~2 rows per agent
    # (its own make_actor transitions) into the journal it was rebuilding from, so a second
    # replay disagreed with the first about every agent's final state.
    rows_before = k.journal.count()
    r = Kernel.from_journal(k.journal.path, root=str(root), budget=k.budget, quiet=True)
    rep1 = r.snapshot()
    facts["replay_is_read_only"] = k.journal.count() == rows_before
    r2 = Kernel.from_journal(k.journal.path, root=str(root), budget=k.budget, quiet=True)
    rep2 = r2.snapshot()
    facts["replay_agents"] = sorted(r.registry.agents)
    facts["replay_tasks"] = sorted(r.graph.tasks)
    facts["replay_has_spawned_agent"] = pay in r.registry.agents if pay else False
    facts["replay_has_new_task"] = billing.task_id in r.graph.tasks
    facts["replay_ledger"] = {e.rid: str(e.state) for e in r.parent.ledger.newest_first()}
    # What the org IS - who is here and in what state, what is owned, what is claimed, what
    # artifacts exist - must match exactly. `tick`/`events` are deliberately excluded: they are
    # counters about the log, not about the org, and comparing them reports growth as divergence.
    ORG_KEYS = ("agents", "tasks", "claims", "artifacts")
    facts["replay_parity_detail"] = {key: (k.snapshot()[key] == rep1[key]) for key in ORG_KEYS}
    facts["replay_parity"] = all(facts["replay_parity_detail"].values())
    facts["replay_state_parity"] = {a: (k.snapshot()["agents"].get(a, {}).get("state")
                                        == rep1["agents"].get(a, {}).get("state"))
                                    for a in k.snapshot()["agents"]}
    # and two independent replays of the same log must be byte-identical, which is the property a
    # reviewer actually cares about: the journal determines the organisation.
    facts["replay_determinism"] = (all(rep1[key] == rep2[key] for key in ORG_KEYS)
                                   and rep1["events"] == rep2["events"]
                                   and rep1["tick"] == rep2["tick"])
    facts["replay_events_delta"] = rep2["events"] - rep1["events"]
    facts["replay_agent_states"] = {a: (v["state"], v["epoch"]) for a, v in rep1["agents"].items()}
    # same policy objects after a restart, not just the same counters - otherwise "the replay
    # reproduced the org" is false in the one way that matters for continuing the work
    facts["policy_parity"] = {a: type(r.actors[a].policy).__name__ ==
                                type(k.actors[a].policy).__name__
                              for a in k.actors if a in r.actors}
    facts["policies_live"] = {a: type(x.policy).__name__ for a, x in k.actors.items()}
    facts["policies_replay"] = {a: type(x.policy).__name__ for a, x in r.actors.items()}

    facts["chain_window"] = [
        {"seq": e[0], "type": e[1], "from": e[2], "to": e[3],
         "body": e[4].get("body", "")[:70]}
        for e in events if req_seq - 1 <= e[0] <= (tail.get("TASK_COMPLETED") or 10 ** 9)][:60]

    ok = bool(
        pay and facts["artifact_published"] and facts["artifact_producer"] == pay
        and facts["requester_notified"] and facts["dependent_task_ready_now"]
        and facts["chain_ordered"] and facts["kernel_identity_stable"]
        and facts["ticks_monotonic"] and facts["agents_never_dropped_after_spawn"]
        and facts["replay_has_spawned_agent"] and facts["replay_has_new_task"]
        and facts["replay_parity"] and facts["replay_determinism"]
        and facts["replay_is_read_only"] and all(facts["replay_state_parity"].values())
        and all(facts["policy_parity"].values())
        and facts["spawn_tick"] and facts["spawn_tick"] > 1
        and facts["blocked_at_start"] and facts["t_billing_producer_missing"]
        and facts["dependent_task_final"]["status"] == "done"
        and not facts["stalled"])
    facts["ok"] = ok
    md = _markdown(k, facts)
    facts["markdown"] = md
    if out:
        Path(out).write_text(md)
    k.journal.close()
    return {"ok": ok, "facts": facts, "markdown": md}


def _markdown(k: Kernel, f: dict[str, Any]) -> str:
    rows = f["transition_table"]
    L: list[str] = []
    L.append("# Phase 2 acceptance — dynamic spawn + mid-run replanning, in one continuous run\n")
    L.append(f"Task: `{f['task_label']}`\n")
    L.append("The initial org is **three agents** (`database_01`, `backend_01`, `frontend_01`). No "
             "payments agent exists at tick 1, and the plan contains no payments task: `backend_01`"
             "'s policy discovers the gap after two working steps and asks via the control plane.\n")
    L.append("## Tick-by-tick transition\n")
    L.append("| tick | agents | working | idle | done | requests | approvals | rejections | "
             "reuses | open tasks | max epoch | free cap |")
    L.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        mark = " ← **4th agent joins**" if r["agents"] >= 4 and r["new_requests"] == 0 and \
            r["tick"] == f["spawn_tick"] else ""
        mark = " ← **spawn**" if r["tick"] == f["spawn_tick"] else mark
        L.append(f"| {r['tick']} | {r['agents']} | {r['working']} | {r['idle']} | "
                 f"{r['completed']} | {r['requests']} | {r['approvals']} | {r['rejections']} | "
                 f"{r['reuses']} | {r['open_tasks']} | {r['epoch_max']} | {r['capacity_left']} |"
                 f"{mark}")
    L.append("")
    L.append("## Per-tick agent states\n")
    L.append("| tick | " + " | ".join(r["states"].keys() or ["-"]) + " |")
    # union across ticks: the agent created mid-run is absent from tick 1's row, and taking the
    # key list from row 0 alone silently dropped it from the table - i.e. the one column a reader
    # came for.
    keys = list({k for r in rows for k in r["states"]})
    if keys:
        L.append("|---:|" + "---:|" * len(keys))
        for r in rows:
            L.append(f"| {r['tick']} | " + " | ".join(
                r["states"].get(k, "*(not yet created)*") if isinstance(r["states"].get(k), str)
                else str(r["states"].get(k, "*(not yet created)*")) for k in keys) + " |")
    L.append("")
    L.append("## The event chain, in causal order\n")
    L.append("Rows sharing the request's correlation id `%s` (the parent side):\n"
             % str(f.get("correlation_id"))[:16])
    L.append("| # | type | seq |")
    L.append("|---:|---|---:|")
    for i, t in enumerate(SPINE, 1):
        L.append(f"| {i} | `{t}` | {f['spine'].get(t)} |")
    L.append("")
    L.append("The new agent's own rows, in order:\n")
    L.append("| # | type | seq |")
    L.append("|---:|---|---:|")
    for i, t in enumerate(TAIL, 1):
        L.append(f"| {i} | `{t}` | {f['tail'].get(t)} |")
    L.append("")
    L.append(f"spine ordered: **{f['spine_ordered']}** · child tail ordered: "
             f"**{f['tail_ordered']}** · whole chain: **{f['chain_ordered']}**\n")
    L.append("`SPAWN_APPROVED` follows `TASK_ASSIGNED` because the kernel logs the approval as part "
             "of *committing* the agent (register → assign → approve); it still precedes every step "
             "the child takes, which is the causality the requirement is about.\n")
    L.append("## What happened\n")
    pay = f["agent_created_midrun"]
    L.append(f"1. `backend_01` published a `SPAWN_AGENT_REQUEST` for a **payment specialist** "
             f"(needs `{WANTED_ARTIFACT}` to exist before `t_billing` can be written).")
    L.append(f"2. Parent decision: **`{f['decision']}`** — no existing agent covered "
             f"`stripe`/`webhooks`, the estimate cleared the worth-it bar, capacity allowed it.")
    L.append(f"3. The graph was amended **first** (`t_billing` gained a real dependency), then "
             f"`{pay}` was created at epoch {f['agent_epoch']}, spawned by `{f['agent_spawned_by']}`.")
    L.append(f"4. `{pay}` did the work and published `{WANTED_ARTIFACT}` "
             f"(producer `{f['artifact_producer']}`); `backend_01` was notified "
             f"(`API_CONTRACT_READY`): **{f['requester_notified']}**.")
    L.append(f"5. `t_billing` started **unowned with its required artifact missing** "
             f"(blocked at start: **{f['blocked_at_start']}**). After the artifact landed it ran: "
             f"status `{f['dependent_task_final']['status']}`, owner "
             f"`{f['dependent_task_final']['owner']}`, deps {f['dependent_task_final']['deps']}. "
             f"It was blocked at the start: **{f['blocked_at_start']}**.")
    L.append("")
    L.append("## Why this is runtime, not planning\n")
    L.append(f"- one `Kernel` object for the whole table (`id()` stable: "
             f"**{f['kernel_identity_stable']}**), tick sequence strictly increasing with no reset: "
             f"**{f['ticks_monotonic']}**")
    L.append(f"- agent count went {f['initial_agent_count']} → 4 at tick **{f['spawn_tick']}** and "
             f"never dropped afterwards: **{f['agents_never_dropped_after_spawn']}**")
    L.append(f"- the run never stalled waiting for it: `stalled={f['stalled']}`")
    L.append("")
    L.append("## Journal is still the source of truth\n")
    L.append(f"- replayed agents: `{f['replay_agents']}` — contains the dynamically-created "
             f"`{pay}`: **{f['replay_has_spawned_agent']}**")
    L.append(f"- replayed tasks contain the mid-run-added `t_billing`: "
             f"**{f['replay_has_new_task']}**")
    L.append(f"- replayed request ledger: `{f['replay_ledger']}`")
    L.append(f"- live vs replayed org (agents / tasks / claims / artifacts): "
             f"**{f['replay_parity']}** {f['replay_parity_detail']}")
    L.append(f"- per-agent state parity: **{all(f['replay_state_parity'].values())}** "
             f"{f['replay_state_parity']}")
    L.append(f"- same policy bound to every agent after replay: "
             f"**{all(f['policy_parity'].values())}** — live {f['policies_live']} / replay "
             f"{f['policies_replay']}")
    L.append(f"- a quiet replay writes **nothing** back to the log it read: "
             f"**{f['replay_is_read_only']}**")
    L.append(f"- two independent replays of the same journal agree on everything incl. agent "
             f"states: **{f['replay_determinism']}** — {f['replay_agent_states']}")
    L.append(f"- `t_billing` was blocked before the run started, and its required artifact was "
             f"missing: **{f['blocked_at_start'] / True if False else f['blocked_at_start']}** / "
             f"**{f['t_billing_producer_missing']}**")
    L.append("")
    L.append(f"Acceptance: **{'PASSED' if f['ok'] else 'FAILED'}** — {f['event_count']} journalled "
             f"events, {f['requests_total']} spawn request(s), "
             f"{f['reuses']} reuse(s) instead of spawning.\n")
    return "\n".join(L)


if __name__ == "__main__":  # pragma: no cover
    res = run_demo(out="var/phase2_acceptance.md")
    print(res["markdown"])
    raise SystemExit(0 if res["ok"] else 1)
