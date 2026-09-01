"""`arena-code`: the agent's decision procedure for the deterministic tier.

One class, one loop, five branches, and every branch is a *read of something the runtime just
showed it*:

    a dependency the runtime can satisfy            -> WAIT (durable park, zero polls)
    a file this task owes that is not on disk       -> write_file
    nothing executed since the last change           -> run_tests (real subprocess, real exit code)
    the run failed                                   -> read what the output blames, then apply the
                                                        correction recorded *in that file*, then run
                                                        again
    the run passed                                   -> publish the real bytes, then COMPLETE - and
                                                         the runtime, not this class, decides whether
                                                         "complete" is allowed

Nothing here knows the acceptance project. It never mentions `calc`, never fabricates content for a
result, and has no path to `open()`, `subprocess`, the graph or the registry: it implements
`decide(Observation) -> Intent`, exactly the interface a model adapter will implement in M3, and the
runtime validates and executes what it proposes.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from ..cognition import Control, Intent, Observation, ToolCall


@dataclass
class CodingAgent:
    """A coding agent's decision procedure over real evidence. Deterministic by construction."""

    #: what this task owes on disk, as {workspace-relative path: contents}. This is the plan handed
    #: to the agent as *data*; writing it still goes through the jail, the digest and the journal.
    seed: dict[str, str] = field(default_factory=dict)
    test_command: tuple[str, ...] = ("python3", "-m", "pytest", "-q", "tests")
    #: park until this condition is met - an artifact name or a full "task:<id>" condition, both of
    #: which the runtime already understands (`Kernel._condition_met`). Durable wait, not a poll.
    wait_for: str = ""
    #: artifacts this task consumes. Checked against the runtime's own `consumed_ready` before the
    #: agent parks, so a wait is only ever registered on something genuinely missing.
    consumes: tuple[str, ...] = ()
    bug_marker: str = "ARENA-BUG:"
    fix_marker: str = "ARENA-FIX:"
    #: which files this agent may correct. Empty = anything the evidence supports. Non-empty is a
    #: *grant*, not a convenience: it is what stops two agents editing the same file at once, and it
    #: is enforced by the runtime's jail as well as honoured here.
    fix_files: tuple[str, ...] = ()
    max_fixes: int = 4
    max_turns: int = 40
    #: if the failing output contains this, ask the Parent for a new colleague instead of fixing
    specialist_trigger: str = ""
    specialist: dict[str, Any] = field(default_factory=dict)
    role: str = "coder"
    name: str = "policy:arena-code"
    # per-instance state on purpose: two agents must never share a turn counter or a transcript
    _turn: int = 0
    _fixes: int = 0
    _asked: bool = False
    _wrote: set[str] = field(default_factory=set)
    _published: bool = False
    #: paths edited this run - the agent must read the new bytes before concluding anything else
    _needs_verify: set = field(default_factory=set)

    def fingerprint(self) -> dict[str, Any]:
        blob = json.dumps({"role": self.role, "seed": sorted(self.seed),
                           "test": list(self.test_command), "wait_for": self.wait_for,
                           "fix_files": list(self.fix_files)}, sort_keys=True)
        return {"kind": "policy-arena-code", "source": self.name, "model": "deterministic",
                "role": self.role, "obj_id": f"{id(self):x}",
                "prompt_sha256_16": hashlib.sha256(blob.encode()).hexdigest()[:16]}

    # ------------------------------------------------------------------ evidence
    @staticmethod
    def _paths(obs: Observation) -> list[str]:
        return list((obs.workspace or {}).get("text_paths") or [])

    def _all(self, obs: Observation) -> tuple:
        return tuple(obs.history or ()) + tuple(obs.recent or ())

    def _last_run(self, obs: Observation) -> Any:
        for o in reversed(self._all(obs)):
            if o.tool in ("run_command", "run_tests"):
                return o
        return None

    def _last(self, obs: Observation) -> Any:
        allres = self._all(obs)
        return allres[-1] if allres else None

    def _evidence(self, obs: Observation) -> list:
        """The run's outcomes with an index, so "did I look at this *after* I changed it?" is answerable.

        Without the index a file read at turn 3 stays "known" forever: the agent applies an edit, the
        test still fails, and it re-proposes the same edit from the same stale read. That is exactly
        the spin this run ended in three times over. An agent may only conclude from what it read
        *after* the last mutation of that path - and a mutation it caused itself counts as one.
        """
        return list(self._all(obs))

    def _last_mutation_at(self, ev: list, path: str) -> int:
        for i in range(len(ev) - 1, -1, -1):
            o = ev[i]
            if o.ok and o.tool in ("edit_file", "write_file") \
                    and str((o.data or {}).get("path") or "") == path:
                return i
        return -1

    def _read_of(self, obs: Observation, path: str) -> str:
        ev = self._evidence(obs)
        floor = self._last_mutation_at(ev, path)
        for i in range(len(ev) - 1, max(floor, 0) - 1, -1):
            o = ev[i]
            if o.tool == "read_file" and str((o.data or {}).get("path") or "") == path:
                return (o.text or "").split("--- stdout ---", 1)[-1]
        return ""

    def _already_read(self, obs: Observation) -> set[str]:
        """Paths whose *current* bytes this agent has seen; a read before a later edit does not count."""
        ev = self._evidence(obs)
        out: set[str] = set()
        for i, o in enumerate(ev):
            if o.tool != "read_file":
                continue
            path = str((o.data or {}).get("path") or "")
            if path and self._last_mutation_at(ev, path) < i:
                out.add(path)
        return out

    def _failure_candidates(self, obs: Observation, text: str) -> list[str]:
        """Every `path:line` the real output mentions, ordered by where a correction belongs.

        pytest names the *test* file first (`tests/test_calc.py:43: AssertionError`) and the file at
        fault only inside the traceback or the message. Taking the first match - the obvious
        implementation - makes an agent re-read its own test forever, which is exactly the loop the
        first end-to-end run got stuck in. Non-test files come first; the test file stays as a
        fallback, not as the answer.
        """
        import re
        found = re.findall(r"([A-Za-z0-9_./-]+\.py):(\d+)", text or "")
        known = [q for q in self._paths(obs) if q.endswith(".py")]
        out: list[str] = []
        for needle, _line in found:
            for cand in known:
                if (cand == needle or cand.endswith("/" + needle)) and cand not in out:
                    out.append(cand)
        # If a source file is named anywhere in the output it is the candidate, full stop. pytest's
        # short summary puts the *test* path first (`tests/test_calc.py:43: AssertionError`) with the
        # module at fault buried in the traceback, so appending the test file as an equal produced
        # agents that re-read their own test forever. The test file is a last resort only.
        named = [q for q in out if not q.startswith("tests/") and "/test_" not in q]
        return named or out

    def _referenced(self, obs: Observation, within: str) -> list[str]:
        """Paths named *inside a file the agent already read*, restricted to what exists.

        The second move when the failure output only names the test file: follow the test's own words
        ("the module under test is `src/calc.py`"). It is retrieval from real file content, not a rule
        about this project - a file that mentions nothing yields nothing, and the agent escalates
        instead of guessing.
        """
        import re
        out: list[str] = []
        for o in self._all(obs):
            if o.tool != "read_file" or str((o.data or {}).get("path") or "") != within:
                continue
            for m in re.finditer(r"[`'\"\s(]([A-Za-z0-9_./-]+\.py)(?=[`'\"\s).,;:])", o.text or ""):
                cand = m.group(1)
                for known in self._paths(obs):
                    if known == within:
                        continue
                    if (known == cand or known.endswith("/" + cand) or cand.endswith("/" + known)) \
                            and known not in out:
                        out.append(known)
        return out

    def _correction(self, obs: Observation, path: str) -> dict[str, str] | None:
        """Pair this file's `ARENA-BUG:` line with the `ARENA-FIX:` annotation under it.

        The replacement text comes from the file *as read from disk*, never from a constant in here -
        so the project is the authority on what the fix is, and a file with no marker produces no
        edit. An agent that "fixes" a file it never read is inventing a cause.
        """
        text = self._read_of(obs, path)
        if not text:
            return None
        lines = text.splitlines()
        for i, ln in enumerate(lines):
            if self.bug_marker not in ln:
                continue
            # the correction is written *on the failing statement*, as
            # `    x = 1  # ARENA-BUG: why. ARENA-FIX: x = 2`
            # - so the replacement keeps that statement's indentation and replaces exactly one line.
            # Putting the fix on the following comment line instead looked tidier and was unusable:
            # deleting the marker comment left the original statement dangling one level too deep,
            # i.e. an IndentationError, and the agent then "fixed" a file that would not parse.
            inline = self.fix_marker in ln
            fixed = ln.split(self.fix_marker, 1)[1].strip() if inline else ""
            if not fixed:
                for nxt in lines[i + 1:i + 4]:
                    if self.fix_marker in nxt:
                        fixed = nxt.split(self.fix_marker, 1)[1].lstrip(":").strip()
                        break
            fixed = fixed.split(self.bug_marker, 1)[0].strip().rstrip(".")

            if fixed:
                # keep the indentation of the line being replaced: the marker lives inside a loop or
                # a function body, and text that starts at column 0 there is a syntax error. (My first
                # version copied the comment's own indentation, which produced exactly that.)
                # An inline marker reads `    return n  # ARENA-BUG: why. ARENA-FIX: return n + 1`:
                # the fix text is the whole replacement statement, comment stripped. Two wrong turns
                # of mine are recorded here so they are not repeated - prefixing the fix with the text
                # before the `#` produced `return nreturn n + 1`, and taking the fix bare produced a
                # statement at column 0. Both wrote the file *successfully*, which is worse than a
                # refused edit: the runtime said yes to an unparseable file.
                # The replacement is the fix text carrying the bug line's own indentation. Dropping
                # the indent (my previous attempt) wrote `return n + 1` at column 0 inside a function
                # body: the edit succeeded, the file stopped parsing, and the runtime had nothing to
                # complain about - a successful write of a broken file.
                indent = ln[: len(ln) - len(ln.lstrip())]
                return {"find": ln.rstrip(), "replace": (indent + fixed).rstrip()}
        return None

    # ------------------------------------------------------------------ the loop
    def decide(self, obs: Observation) -> Intent:
        self._turn += 1
        if self._turn > self.max_turns:
            return Intent(control=Control.ESCALATE,
                          reason=f"{self.role}: no verified state after {self.max_turns} turns")
        present = set(self._paths(obs))
        last = self._last(obs)

        # ---- 0. park on what the runtime can satisfy, rather than spinning on it. A WAIT on a
        # condition that is already true is a bug, not a caution: the first end-to-end run had the
        # docs agent park itself on an artifact its colleague had published two turns earlier, and
        # because the unblock is an edge the task never woke up again.
        if last is None and (self.wait_for or self.consumes):
            missing = [q for q in self.consumes if q not in tuple(obs.consumed_ready or ())] \
                if self.consumes else ([q for q in (self.wait_for,)
                                        if q not in tuple(obs.consumed_ready or ())])
            if missing:
                want = missing[0]
                cond = want if ":" in want else f"artifact:{want}"
                return Intent(control=Control.WAIT, wait_for=cond, reason=f"needs {want}")

        # ---- 1. create what this task owes and the tree does not have
        missing = [q for q in self.seed if q not in present]
        if missing:
            path = missing[0]
            self._wrote.add(path)
            self._published = False          # a change to the tree invalidates a prior publish
            return Intent.of(ToolCall("write_file", {"path": path, "content": self.seed[path]}),
                             think=f"{path} is required by this task and absent from the workspace",
                             reason=f"create {path}")

        # ---- 2. the only admissible evidence is an actual run
        last_run = self._last_run(obs)
        needs_run = last_run is None or (last is not None and last.tool in ("write_file",
                                                                           "edit_file"))
        if needs_run:
            return Intent.of(ToolCall("run_tests", {"argv": list(self.test_command)}),
                             think=("files changed since the last run" if last_run
                                    else "files exist; nothing has been executed yet"),
                             reason="run the project's tests")

        # ---- 3. a passing run: publish real bytes, then ask the runtime to judge completion
        if last_run.ok:
            owed = list((obs.task or {}).get("produces") or []) or list(self.seed)
            promised = [q for q in self.seed if q in present]
            if not self._published:
                gaps = [q for q in owed if q not in promised]
                if gaps:
                    return Intent(control=Control.ESCALATE,
                                  reason=f"cannot publish yet: {gaps} are not on disk")
                self._published = True
                # ONE artifact carrying every promised path: publishing them one at a time would let
                # the kernel's "all promised outputs exist" rule close the task after the *first*
                # file, i.e. mark the worker done while it is still mid-publication.
                return Intent.of(ToolCall("publish_artifact",
                                          {"artifact": owed[0], "files": list(owed)}),
                                 think=f"tests pass; all {len(owed)} promised file(s) exist with "
                                       f"real digests",
                                 reason="publish verified output")
            return Intent(control=Control.COMPLETE,
                          think="the test command exited 0 and the promised files are published",
                          reason="verified work finished")

        # ---- 4. a failing run: read, correct what the reading supports, run again
        body = last_run.text or ""
        if self.specialist_trigger and not self._asked and self.specialist_trigger in body:
            self._asked = True
            return Intent(control=Control.SPAWN_REQUEST,
                          reason=self.specialist.get("reason", "capability gap observed in output"),
                          spawn=dict(self.specialist))
        if self._fixes >= self.max_fixes:
            return Intent(control=Control.ESCALATE,
                          reason=f"{self._fixes} correction(s) did not fix "
                                 f"{' '.join(self.test_command)}")
        # a refused proposal is information, not a failure to retry: the runtime told us the edit
        # does not match the file, so re-reading (or escalating) is the only honest next move
        if last is not None and last.refused:
            return Intent.of(ToolCall("read_file", {"path": self._last_edit_target(obs) or ""})
                             if self._last_edit_target(obs) else
                             ToolCall("list_files", {"prefix": ""}),
                             control=Control.ESCALATE if not self._last_edit_target(obs) else "",
                             think=f"the runtime refused my last call ({last.refused}); re-read "
                                   f"before proposing anything else",
                             reason=f"react to {last.refused}")
        # just read something? use it. Running the tests again here would be a spin, not progress.
        if last is not None and last.tool == "read_file":
            fixed = self._next_correction(obs)
            if fixed is not None:
                return fixed
            unread = self._unread(obs)
            if unread:
                return Intent.of(ToolCall("read_file", {"path": unread[0]}),
                                 reason=f"read {unread[0]}")
            return Intent.of(ToolCall("list_files", {"prefix": ""}), control=Control.ESCALATE,
                             reason="no file I have read carries a correction this failure supports"
                                    "; I have read " + ", ".join(sorted(self._already_read(obs))
                                                                  or ["nothing"]))
        unread = self._unread(obs)
        if unread:
            return Intent.of(ToolCall("read_file", {"path": unread[0]}),
                             think=f"{unread[0]} is implicated by the failure output; read it before "
                                   f"guessing at a cause",
                             reason=f"read {unread[0]}")
        fixed = self._next_correction(obs)
        if fixed is not None:
            return fixed
        return Intent.of(ToolCall("list_files", {"prefix": ""}), control=Control.ESCALATE,
                         reason="failure output names no file I can correct")

    def _last_edit_target(self, obs: Observation) -> str:
        for o in reversed(self._all(obs)):
            if o.tool == "edit_file":
                return str((o.data or {}).get("path") or "")
        return ""

    # ---------------------------------------------------------------- step 4's two questions
    def _unread(self, obs: Observation) -> list[str]:
        """Files the evidence points at that this agent has not opened yet."""
        already = self._already_read(obs)
        last_run = self._last_run(obs)
        cands = self._failure_candidates(obs, (last_run.text if last_run else "") or "")
        todo = [q for q in cands if q not in already]
        if not todo:
            for seen in cands + sorted(already):
                if seen:
                    extra = [q for q in self._referenced(obs, seen) if q not in already]
                    if extra:
                        return extra
        return todo

    def _next_correction(self, obs: Observation) -> Intent | None:
        """Apply the correction recorded in the most recently read file that actually carries one."""
        ev = self._evidence(obs)
        for i in range(len(ev) - 1, -1, -1):
            o = ev[i]
            if o.tool != "read_file":
                continue
            path = str((o.data or {}).get("path") or "")
            if not path:
                continue
            if self._last_mutation_at(ev, path) > i:
                continue      # mutated since that read: the bytes I remember are not the bytes now
            if self.fix_files and path not in self.fix_files:
                return Intent(control=Control.ESCALATE,
                              reason=f"{path} needs a correction but is outside my grant "
                                        f"{list(self.fix_files)}; the file that owns it must apply it")
            corr = self._correction(obs, path)
            if corr:
                self._fixes += 1
                return Intent.of(ToolCall("edit_file", {"path": path, "find": corr["find"],
                                                        "replace": corr["replace"]}),
                                 think=f"the failure and {path}'s own recorded correction agree; "
                                       f"applying it as read from disk",
                                 reason=f"correction {self._fixes} after a real failure")
        # nothing read carries a marker: the caller falls back to reading, then to escalating
        return None
