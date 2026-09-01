"""Cognition sources for `arena-code`: what an agent decides with.

Both classes below implement the same `decide(Observation) -> Intent` seam that a real model will
use later (`arena/cognition.py`), and neither can touch the kernel: they receive an Observation of
*facts* (files on disk, digests, exit codes, captured output) and return proposals. The runtime
validates, executes, journals. That is the whole contract, and it is identical for the tier that
runs today and the tier that needs an API key.

`PolicyCognition` (in `arena/cognition.py`) already adapts every Phase 1/2 policy onto this seam, so
"no LLM configured" is not a degraded mode of the runtime - it is a different, honest brain behind an
unchanged body.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..cognition import Control, Intent, Observation, ToolCall


def _fingerprint(kind: str, extra: dict[str, Any]) -> dict[str, Any]:
    blob = json.dumps(extra, sort_keys=True, default=str).encode("utf-8", "replace")
    return {"kind": kind, "prompt_sha256_16": hashlib.sha256(blob).hexdigest()[:16],
            "obj_id": f"{id(extra):x}", **extra}


# --------------------------------------------------------------------- scripted tier
@dataclass
class ScriptedCoding:
    """A plan replayed one step per turn. Deliberately dumb - but *only* in what it decides, never
    in how it acts: every step goes through the tool registry, the jail and the journal."""

    steps: tuple[dict[str, Any], ...] = ()
    role: str = "coder"
    name: str = "policy:scripted-coding"
    _turn: int = 0

    def fingerprint(self) -> dict[str, Any]:
        return _fingerprint("policy-script", {"source": self.name, "model": "deterministic",
                                              "steps": len(self.steps)})

    def decide(self, obs: Observation) -> Intent:
        if self._turn >= len(self.steps):
            return Intent(control=Control.WAIT, reason="script exhausted; parked for the runtime")
        step = self.steps[self._turn]
        self._turn += 1
        calls = tuple(ToolCall(c["tool"], dict(c.get("args") or {})) for c in step.get("calls", []))
        return Intent(calls=calls, control=step.get("control", ""),
                      think=step.get("think", ""), reason=step.get("reason", ""),
                      wait_for=step.get("wait_for", ""), publish=step.get("publish") or {},
                      spawn=step.get("spawn") or {})


# --------------------------------------------------------------------- reflective tier
@dataclass
class ReflectiveCoding:
    """The deterministic stand-in for a coding agent: it *reacts to what actually happened*.

    The branch points are all reads of real evidence - the workspace listing, the exit code and
    captured output of the last command, the digests of files it wrote - so swapping in a model
    changes only how the next `Intent` is produced. Nothing here is special-cased for the demo
    project beyond the data passed in (`test_command`, `seed`, `fix_markers`).

    Loop shape:  observe -> write what is missing -> run -> read the failure -> correct -> rerun ->
    publish -> complete (the runtime, not this class, decides whether "complete" is allowed).
    """

    #: files the runtime declared this task owes, as {path: purpose}; used only to decide *what to
    #: write*, never to fake a result
    seed: dict[str, str] = field(default_factory=dict)
    test_command: tuple[str, ...] = ("python3", "-m", "pytest", "-q", "tests")
    #: a failing test naming `path:line` plus an `ARENA-BUG:`/`ARENA-FIX:` pair is the "correction"
    #: step. These are strings the *project source* contains, read out of real pytest output.
    bug_marker: str = "ARENA-BUG:"
    fix_marker: str = "ARENA-FIX:"
    max_fixes: int = 3
    max_turns: int = 40
    ask_specialist_when: str = ""      # substring of failing output that triggers a spawn request
    specialist: dict[str, Any] = field(default_factory=dict)
    role: str = "coder"
    name: str = "policy:reflective-coding"
    #: per-instance on purpose: two agents must never share a turn counter or a transcript
    _turn: int = 0
    _fixes: int = 0
    _requested: bool = False
    _pass_seen: bool = False
    _log: list[str] = field(default_factory=list)

    def fingerprint(self) -> dict[str, Any]:
        return _fingerprint("policy-reflective", {
            "source": self.name, "model": "deterministic",
            "role": self.role, "test": " ".join(self.test_command),
            "seed": sorted(self.seed)})

    # -------------------------------------------------------------- helpers
    @staticmethod
    def _files_present(obs: Observation) -> set[str]:
        ws = obs.workspace or {}
        listed = set(ws.get("present") or ())
        if listed:
            return listed
        return set()

    def _last(self, obs: Observation, tool: str) -> Observation | Any:
        for o in reversed(obs.recent or ()):
            if o.tool == tool:
                return o
        return None

    @staticmethod
    def _succeeded(out: Any) -> bool:
        return bool(out) and out.ok and (out.exit_code in (0, None))

    @staticmethod
    def _failed(out: Any) -> bool:
        return bool(out) and not out.ok

    def _failure_target(self, text: str, obs: Observation) -> tuple[str, str] | None:
        """From real pytest output, find (path, the buggy line + its paired fix line).

        Returns the *edit* to make: the runtime will refuse it if the target string is not exactly
        there, so a wrong guess comes back as a refusal to reason over - not as silent success.
        """
        m = re.search(r"(?:^|\n)([A-Za-z0-9_./-]+\.py):(\d+):", text or "")
        if not m:
            return None
        path = m.group(1)
        if path not in (obs.workspace or {}).get("text_paths", []):
            # pytest often prints paths relative to its rootdir; only edit what the workspace lists
            cand = [p for p in (obs.workspace or {}).get("text_paths", [])
                    if p.endswith("/" + path) or p == path]
            if not cand:
                return None
            path = cand[0]
        return path, ""

    # -------------------------------------------------------------- the loop
    def decide(self, obs: Observation) -> Intent:
        self._turn += 1
        if self._turn > self.max_turns:
            return Intent(control=Control.ESCALATE,
                          reason=f"gave up after {self.max_turns} turns "
                                 f"({self._log[-1] if self._log else 'no activity'})")
        present = set((obs.workspace or {}).get("text_paths") or [])

        # 1. write what the task owes and does not have yet
        missing = [p for p in self.seed if p not in present]
        if missing:
            path = missing[0]
            return Intent.of(ToolCall("write_file", {"path": path, "content": self.seed[path]}),
                             think=f"{path} is not on disk yet",
                             reason=f"scaffolding {path}")

        last_run = self._last(obs, "run_command")

        # 2. no test run recorded yet -> run the tests
        if last_run is None:
            return Intent.of(ToolCall("run_tests", {"argv": list(self.test_command)}),
                             think="nothing has been executed yet; establish the real state",
                             reason="run the test suite")

        # 3. a run that passed: publish, then let the runtime judge completion
        if self._succeeded(last_run):
            self._pass_seen = True
            promised = [pt for pt in self.seed if pt in present]
            if promised:
                return Intent.of(ToolCall("publish_artifact",
                                          {"artifact": promised[0], "files": list(promised)}),
                                 think=f"tests pass on {' '.join(self.test_command)}; "
                                      f"publishing {len(promised)} real file(s)",
                                 reason="publish verified output")
            return Intent(control=Control.COMPLETE, think="tests pass and outputs are published",
                          reason="work finished with evidence")

        # 4. a run that failed: the interesting case. Fix what the failure points at.
        text = (last_run.text or "")
        if (self.ask_specialist_when and not self._requested
                and self.ask_specialist_when in text):
            self._requested = True
            return Intent(control=Control.SPAWN_REQUEST, reason=self.specialist.get(
                "reason", "need a capability this agent does not have"),
                spawn=dict(self.specialist))
        if self._fixes >= self.max_fixes:
            return Intent(control=Control.ESCALATE,
                          reason=f"{self._fixes} correction(s) did not make "
                                 f"{' '.join(self.test_command)} pass")
        target = self._failure_target(text, obs)
        if target is None:
            return Intent.of(ToolCall("list_files", {"prefix": ""}),
                             think="failure output names no editable file; look at the tree",
                             reason="inspect workspace")
        path, _ = target
        bug = self._bug_line(path, obs)
        if bug is None:
            return Intent.of(ToolCall("read_file", {"path": path}),
                             think=f"{path} is implicated but I have no fix to apply; read it",
                             reason="read failing file")
        self._fixes += 1
        self._log.append(f"fix {self._fixes} in {path}")
        return Intent.of(ToolCall("edit_file", {"path": path, "find": bug["find"],
                                                "replace": bug["replace"]}),
                         think=f"pytest pointed at {path}; the file carries a paired "
                              f"{self.fix_marker} annotation - apply it",
                         reason=f"correction {self._fixes} after a real failure")

    def _bug_line(self, path: str, obs: Observation) -> dict[str, str] | None:
        """Pair an `ARENA-BUG:` line with the `ARENA-FIX:` annotation directly under it.

        The *content* of the fix comes from the project's own source file, which the runtime read
        from disk - not from this policy's constants. That is the difference between a scripted agent
        and a scripted *outcome*.
        """
        text = ""
        for o in reversed(obs.recent or ()):
            if o.tool == "read_file" and (o.data or {}).get("path") == path:
                text = o.text or ""
                break
        if not text or "--- stdout ---" not in text:
            return None
        body = text.split("--- stdout ---", 1)[1]
        lines = body.splitlines()
        for i, ln in enumerate(lines):
            if self.bug_marker not in ln:
                continue
            m = re.search(r"(.*?)\s+#\s*" + re.escape(self.bug_marker), ln)
            if not m:
                continue
            fixed = ""
            for nxt in lines[i + 1:i + 4]:
                fm = re.search(r"^\s*#\s*" + re.escape(self.fix_marker) + r"\s*(.*)$", nxt)
                if fm:
                    fixed = fm.group(1).strip()
                    break
            if not fixed:
                continue
            return {"find": ln.rstrip(), "replace": fixed}
        return None
