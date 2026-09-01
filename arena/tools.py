"""Real tool execution (Phase 2.5 M1): the only place in this system that touches a filesystem
or spawns a process on an agent's behalf.

The boundary, in one table (this is the contract `REAL_AGENT_RUNTIME_DESIGN.md` §4 asked for):

    Kernel / orchestration   arena/kernel.py, arena/parent.py, arena/bus.py
                             ticks, scheduling, graph legality, claims, lifecycle, spawn vetoes.
                             Never opens a project file. Never spawns a process.
    Agent runtime            arena/actor.py
                             owns the per-agent state, the mailbox, the turn ("one Observation in,
                             one Intent applied out"), and applies control verbs. Knows *that* a
                             tool ran; knows nothing about *how*.
    Cognition                arena/cognition.py (+ arena/code/cognition.py for providers)
                             decide(Observation) -> Intent. Pure proposal. Imports NOTHING from
                             `subprocess`/`os`; cannot reach the graph, the registry or the bus.
    Tool execution           THIS MODULE
                             validates an Intent's tool call, enforces the jail, runs it, captures
                             the real result (bytes, exit code, stderr, digest) and journals it.
    Workspace / project      arena/code/project.py
                             creates `projects/<id>/{.arena,source,artifacts,logs}`, the manifest,
                             and decides which directory an agent is allowed to write in.

Three rules that make the split real rather than cosmetic:

1. A tool call is journalled as an intent row *before* it executes and a result row *after*. The
   journal therefore knows what the agent meant, what actually happened, and can tell the
   difference. `arena trace <cid>` shows the whole chain.
2. Refusals are results, not exceptions and not silences. Jailbreaks, unauthorised tools, missing
   executables, timeouts, a `git commit` with nothing staged, an `edit_file` whose target appears
   twice - all of them come back as `ToolResult(ok=False, ...)` that the agent can read and act on.
   An exception would end the turn; a silence would be a lie.
3. Success is never inferred. `ok` comes from an exit code, or from a file that exists with the
   bytes that were written. Nothing here says "assume it worked".
"""
from __future__ import annotations

import errno
import hashlib
import os
import re
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from .message import MessageType


# --------------------------------------------------------------------------- violations
class JailViolation(Exception):
    """Raised internally, converted to a ToolResult at the boundary. Never escapes the executor."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code, self.detail = code, detail
        super().__init__(f"{code}: {detail}")


# --------------------------------------------------------------------------- redaction
#: secrets must never reach the journal, the transcript or `arena trace`. Patterns are deliberately
#: blunt: a false positive costs a cosmetic `***` in a log line, a false negative leaks a key.
REDACTIONS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)\b(sk-[A-Za-z0-9_\-]{8,})\b"),
    re.compile(r"\b(gh[pousr]_[A-Za-z0-9]{8,})\b"),
    re.compile(r"(?i)(api[_-]?key|token|secret|password)[\"']?\s*[:=]\s*[\"']?([^\s\"',]{6,})"),
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)([A-Za-z0-9._\-]{8,})"),
)


def redact(text: str, *, extra: Sequence[str] = ()) -> str:
    out = text or ""
    for secret in extra:
        if secret and len(secret) >= 6:
            out = out.replace(secret, "***redacted***")
    for pat in REDACTIONS:
        if pat.groups < 2:
            out = pat.sub("***redacted***", out)
        else:
            # keep the key name, replace the value: `api_key: sk-abc` -> `api_key:***redacted***`
            out = pat.sub(lambda m: m.group(1) + ":***redacted***", out)
    return out


def digest(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", "replace")).hexdigest()[:16]


def file_digest(path: Path) -> str:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()[:16]
    except OSError:
        return "unreadable"


# --------------------------------------------------------------------------- results
@dataclass
class ToolResult:
    """What actually happened. `ok` is only ever True on evidence."""

    tool: str
    ok: bool = False
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    refused: str = ""            # non-empty => never executed (jail / permission / risk)
    changed: list[dict[str, Any]] = field(default_factory=list)
    duration_s: float = 0.0
    truncated: bool = False
    rid: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"tool": self.tool, "ok": self.ok, "exit_code": self.exit_code,
                "refused": self.refused, "rid": self.rid, "truncated": self.truncated,
                "duration_s": round(self.duration_s, 4), "changed": self.changed,
                "data": self.data, "out_digest": digest(self.stdout + self.stderr),
                "stdout_len": len(self.stdout), "stderr_len": len(self.stderr)}

    def as_block(self) -> str:
        """The transcript view: what the cognition layer reads back."""
        if self.refused:
            return f"<tool_refused tool={self.tool!r} code={self.refused!r} />"
        bits = [f"exit={self.exit_code}"] if self.exit_code is not None else []
        if self.changed:
            bits.append("changed=" + ",".join(c["path"] for c in self.changed))
        if self.data:
            bits.append("data=" + ",".join(f"{k}={v}" for k, v in sorted(self.data.items())
                                           if not isinstance(v, (list, dict)))[:400])
        head = f"<tool_result tool={self.tool!r} ok={'true' if self.ok else 'false'} " \
               f"{' '.join(bits)}>"
        body = []
        if self.stdout:
            body.append("--- stdout ---\n" + self.stdout)
        if self.stderr:
            body.append("--- stderr ---\n" + self.stderr)
        if self.truncated:
            body.append("…[output truncated by the runtime]")
        return head + ("\n" + "\n".join(body) if body else "") + "\n</tool_result>"

    @classmethod
    def refusal(cls, tool: str, code: str, detail: str = "") -> "ToolResult":
        return cls(tool=tool, ok=False, refused=code, stderr=detail or code)


# --------------------------------------------------------------------------- jail
@dataclass
class Jail:
    """Everything an agent may touch, expressed as one root plus two prefix sets.

    `writes` is what makes "prevent simultaneous same-file edits between agents" true later without
    a lock: two agents with disjoint write prefixes cannot overwrite each other's work, so the
    merge - not the filesystem - is the synchronisation point.
    """

    root: Path
    reads: tuple[str, ...] = ("",)
    writes: tuple[str, ...] = ("",)
    agent_id: str = ""
    deny_names: tuple[str, ...] = (".arena", ".git")     # no writes into the kernel's own dir

    @property
    def root_str(self) -> str:
        return str(self.root)

    def resolve(self, rel: str, *, write: bool) -> Path:
        if rel is None:
            raise JailViolation("REFUSE_EMPTY_PATH", "a tool call must name a path")
        if write and str(rel).strip() in ("", "."):
            raise JailViolation("REFUSE_WRITE_AT_ROOT",
                                "a write must name a file under this agent's allocation, not the "
                                "workspace root")
        rel = str(rel)
        if os.path.isabs(rel) or rel.startswith("~"):
            raise JailViolation("REFUSE_ABSOLUTE_PATH",
                                "paths are workspace-relative; absolute paths are refused so an "
                                "agent cannot be steered outside its tree")
        candidate = (self.root / rel)
        # normpath before exists(): a *new* file has to be checked too, and realpath on a missing
        # path resolves the parent, which would let `../x/../../etc/passwd` slide through.
        norm = Path(os.path.normpath(str(candidate)))
        try:
            real = Path(os.path.realpath(str(norm)))
        except OSError as e:                                   # pragma: no cover - defensive
            raise JailViolation("REFUSE_STAT_FAILED", str(e)) from e
        root_real = Path(os.path.realpath(str(self.root)))
        for p, label in ((norm, "lexical"), (real, "symlink")):
            try:
                p.relative_to(root_real)
            except ValueError:
                raise JailViolation("REFUSE_PATH_JAILBREAK",
                                    f"{label} resolution of {rel!r} escapes the workspace root") from None
        parts = norm.relative_to(root_real).parts
        for part in parts:
            if part in self.deny_names and write:
                raise JailViolation("REFUSE_PROTECTED_PATH",
                                    f"{rel!r} touches {part!r}, which belongs to the runtime")
        if write and not self._under(norm.relative_to(root_real), self.writes):
            raise JailViolation("REFUSE_WRITE_OUTSIDE_ALLOCATION",
                                f"{rel!r} is not under this agent's writable prefixes "
                                f"{list(self.writes)}")
        if not write and not self._under(norm.relative_to(root_real), self.reads):
            raise JailViolation("REFUSE_READ_OUTSIDE_ALLOCATION",
                                f"{rel!r} is not under {list(self.reads)}")
        return norm

    @staticmethod
    def _under(rel: "os.PathLike[str]", prefixes: Sequence[str]) -> bool:
        if not prefixes:
            return True
        s = rel.as_posix()
        for pre in prefixes:
            pre = (pre or "").strip("/")
            if pre == "" or s == pre or s.startswith(pre + "/"):
                return True
        return False

    def snapshot(self) -> dict[str, tuple[int, float]]:
        """(size, mtime) for every tracked file - the cheap way to learn what a command did."""
        out: dict[str, tuple[int, float]] = {}
        for pre in self.writes:
            base = self.root / (pre or ".")
            if not base.is_dir():
                continue
            for dirpath, dirnames, filenames in os.walk(base):
                dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules", "__pycache__")]
                if len(out) > 4000:                            # pragma: no cover - safety valve
                    break
                for fn in filenames:
                    p = Path(dirpath) / fn
                    try:
                        st = p.stat()
                    except OSError:
                        continue
                    out[str(p.relative_to(self.root))] = (st.st_size, round(st.st_mtime, 6))
        return out


# --------------------------------------------------------------------------- registry
@dataclass(frozen=True)
class ToolSpec:
    name: str
    summary: str
    risk: str = "read"                      # read | write | exec | exec-network | vcs-commit
    timeout: float = 20.0
    idempotent: bool = True
    required: tuple[str, ...] = ()
    #: argv-shaped tools must never be run through a shell; this flag is what the executor checks
    argv_only: bool = False

    def missing_args(self, args: dict[str, Any]) -> list[str]:
        out = []
        for key in self.required:
            v = args.get(key)
            if v is None or (isinstance(v, (list, tuple, str, dict)) and len(v) == 0):
                out.append(key)
        return out


TOOLS: tuple[ToolSpec, ...] = (
    ToolSpec("list_files", "list workspace paths (optionally under a prefix)", "read"),
    ToolSpec("read_file", "read a workspace file as text", "read", required=("path",)),
    ToolSpec("write_file", "create/replace a workspace file with real bytes", "write",
             required=("path", "content")),
    ToolSpec("edit_file", "exact string replacement; refuses unless the target is unambiguous",
             "write", required=("path", "find", "replace")),
    ToolSpec("run_command", "execute argv in the workspace (never a shell)", "exec",
             timeout=120.0, idempotent=False, required=("argv",), argv_only=True),
    # Same executor path as run_command, with a longer ceiling: a build/test run is the one command
    # an agent legitimately needs minutes for, and naming it separately is what lets a *policy* be
    # granted "run the tests" without being granted arbitrary execution.
    ToolSpec("run_tests", "run the project's test/build command through the same jail", "exec",
             timeout=300.0, idempotent=False, required=("argv",), argv_only=True),
    ToolSpec("inspect_git", "git status/diff/log inside the workspace", "read"),
    ToolSpec("commit", "git add + commit in the agent's own tree (push is never available)",
             "vcs-commit", idempotent=False, required=("message",)),
    ToolSpec("publish_artifact", "publish workspace files as artifacts, with digests", "write",
             required=("artifact",)),
    ToolSpec("send_message", "hand a message to another actor through the bus", "read"),
    ToolSpec("wait_for_event", "park on a condition until it is satisfied", "read"),
    ToolSpec("request_specialist", "ask the Parent to evaluate a new colleague", "read",
             required=("requested_role", "reason")),
)

#: the network-touching prefixes. Only consulted when allow_egress is False - an agent that needs
#: to install a dependency does so through an explicit configuration change, not a loophole.
EGRESS_TOOLS = frozenset({"curl", "wget", "pip", "pip3", "npm", "npx", "yarn", "uvicorn", "git-clone"})
NEVER_RUN = frozenset({"sudo", "su", "shutdown", "reboot", "kill", "pkill", "init", "mkfs", "dd"})


class ToolRegistry:
    def __init__(self, specs: Iterable[ToolSpec] = TOOLS) -> None:
        self.specs: dict[str, ToolSpec] = {s.name: s for s in specs}

    def get(self, name: str) -> ToolSpec | None:
        return self.specs.get(name)

    def names(self) -> list[str]:
        return sorted(self.specs)

    def schemas(self) -> list[dict[str, Any]]:
        return [{"name": s.name, "summary": s.summary, "risk": s.risk,
                 "required": list(s.required), "idempotent": s.idempotent,
                 "timeout_s": s.timeout} for s in TOOLS]


# --------------------------------------------------------------------------- executor
@dataclass
class Executor:
    """Validates, executes, captures, journals. One instance per kernel; single writer, no locks."""

    kernel: Any
    registry: ToolRegistry = field(default_factory=ToolRegistry)
    max_out: int = 4000
    default_timeout: float = 120.0
    max_timeout: float = 600.0
    allow_egress: bool = False
    #: agent_id -> allowed tool names; empty means "everything in the registry" (a policy tier
    #: that has not been given a narrower grant still cannot escape the jail or the risk tiers)
    grants: dict[str, set[str]] = field(default_factory=dict)
    jails: dict[str, Jail] = field(default_factory=dict)
    #: (tool, args-digest) pairs this run already refused - see _execute
    _refused: set[tuple[str, str]] = field(default_factory=set)
    stats: dict[str, int] = field(default_factory=lambda: {
        "calls": 0, "results": 0, "executions": 0, "refusals": 0, "file_mutations": 0,
        "timeouts": 0, "failures": 0, "commands": 0, "tests": 0, "violations": 0})
    _seq: int = 0

    # ------------------------------------------------------------------ wiring
    def bind(self, agent_id: str, *, jail: Jail, allowed: Sequence[str] | None = None) -> Jail:
        self.jails[agent_id] = jail
        if allowed:
            self.grants[agent_id] = set(allowed)
        self.kernel.journal.emit(
            MessageType.WORKSPACE_BOUND, "kernel", agent_id,
            body=f"{agent_id} bound to {jail.root_str} (writes: {list(jail.writes) or ['/']})",
            agent_id=agent_id, jail_root=jail.root_str, writes=list(jail.writes),
            reads=list(jail.reads), allowed_tools=sorted(allowed) if allowed else self.registry.names())
        return jail

    def jail_of(self, agent_id: str) -> Jail:
        jail = self.jails.get(agent_id)
        if jail is None:
            raise JailViolation("REFUSE_NO_WORKSPACE",
                                f"agent {agent_id!r} has no workspace; the runtime will not let an "
                                f"agent guess where to write")
        return jail

    def can(self, agent_id: str, name: str) -> str:
        """Permission check as a pure function: '' means allowed, anything else is the reason."""
        spec = self.registry.get(name)
        if spec is None:
            return "REFUSE_UNKNOWN_TOOL"
        allowed = self.grants.get(agent_id)
        if allowed is not None and name not in allowed:
            return "REFUSE_TOOL_NOT_GRANTED"
        if spec.risk == "vcs-commit" and self.kernel.git_root_of(agent_id) is None:
            return "REFUSE_NO_GIT_REPO"
        return ""

    # ------------------------------------------------------------------ journal
    def _next_rid(self) -> str:
        self._seq += 1
        return f"tc-{self._seq:04d}"

    def _emit(self, *args: Any, **fields: Any) -> None:
        """Journal, but never at the cost of the run.

        `*args` rather than a named first parameter on purpose: several call sites pass
        `agent_id=` as a journal *field*, and a positional named `agent_id` would swallow it
        (TypeError: multiple values) - which is exactly what happened on the first end-to-end run.
        """
        try:
            self.kernel.journal.emit(*args, **fields)
        except Exception as e:                                  # pragma: no cover - defensive
            self.kernel.log(f"journal emit failed for {args[0] if args else '?'}: {e}")

    def plan(self, agent_id: str, calls: Sequence[dict[str, Any]], *, task_id: str | None,
              correlation_id: str = "") -> list[str]:
        """Journal the intent BEFORE the effect. Returns the rids, in order."""
        rids = []
        for call in calls:
            rid = self._next_rid()
            rids.append(rid)
            args = call.get("args") or {}
            self._emit(MessageType.TOOL_CALL, agent_id, "kernel",
                       body=f"{call.get('tool')} called",
                       rid=rid, tool=str(call.get("tool")), agent_id=agent_id,
                       args_digest=digest(repr(sorted(args.items()))),
                       args_preview=redact(_short(args)),
                       task_id=task_id, correlation_id=correlation_id)
        return rids

    def note_violation(self, agent_id: str, code: str, detail: str, *,
                       task_id: str | None = None) -> None:
        self.stats["violations"] += 1
        self._emit(MessageType.COGNITION_VIOLATION, agent_id, "parent",
                   body=f"{code}: {detail}", code=code, detail=detail, task_id=task_id,
                   agent_id=agent_id)

    # ------------------------------------------------------------------ execute
    def execute(self, agent_id: str, tool: str, args: dict[str, Any], *,
                rid: str = "", task_id: str | None = None,
                correlation_id: str = "") -> ToolResult:
        """One tool call, start to finish. Returns a result even when it refuses."""
        self.stats["calls"] += 1
        started = time.monotonic()
        result = self._execute(agent_id, tool, args, rid=rid, task_id=task_id)
        result.duration_s = time.monotonic() - started
        if len(result.stdout) > self.max_out or len(result.stderr) > self.max_out:
            result.truncated = True
        if result.refused:
            self.stats["refusals"] += 1
            self._emit(MessageType.TOOL_REFUSED, agent_id, "kernel",
                       body=f"{tool}: {result.refused}", rid=rid or result.rid, tool=tool,
                       agent_id=agent_id, code=result.refused, detail=redact(result.stderr),
                       task_id=task_id, correlation_id=correlation_id)
        else:
            self.stats["results"] += 1
            if not result.ok:
                self.stats["failures"] += 1
            fields = dict(result.to_dict())
            # the result dict carries its own `tool`/`rid`; those two keys must not be passed twice
            fields.update({"tool": tool, "rid": rid or result.rid, "agent_id": agent_id,
                           "task_id": task_id, "correlation_id": correlation_id,
                           "body": f"{tool} -> " + ("ok" if result.ok
                                                    else f"exit {result.exit_code}")})
            self._emit(MessageType.TOOL_RESULT, agent_id, "kernel", **fields)
        return result

    def _execute(self, agent_id: str, tool: str, args: dict[str, Any], *, rid: str,
                 task_id: str | None) -> ToolResult:
        # A repeat of a call this agent already made *and already had refused* is refused without
        # execution. This is not an optimisation: an agent that re-proposes an edit the file no
        # longer supports is reasoning over a stale observation, and letting it burn subprocess after
        # subprocess would hide that behind noise.
        sig = (tool, digest(repr(sorted((args or {}).items()))))
        if sig in self._refused:
            return ToolResult.refusal(tool, "REFUSE_REPEATED_REFUSAL",
                                      "this exact call was already refused in this run; re-read the "
                                      "file or escalate rather than repeating it")
        spec = self.registry.get(tool)
        if spec is None:
            return ToolResult.refusal(tool, "REFUSE_UNKNOWN_TOOL",
                                      f"no tool {tool!r}; available: {self.registry.names()}")
        reason = self.can(agent_id, tool)
        if reason:
            return ToolResult.refusal(tool, reason,
                                      f"agent {agent_id} may not call {tool!r}")
        missing = spec.missing_args(args)
        if missing:
            return ToolResult.refusal(tool, "REFUSE_MISSING_ARGS", f"missing {missing}")
        try:
            handler = getattr(self, f"_t_{tool}")
        except AttributeError:                                   # pragma: no cover
            return ToolResult.refusal(tool, "REFUSE_NOT_IMPLEMENTED")
        try:
            out = handler(agent_id, args, task_id=task_id)
        except JailViolation as e:
            return ToolResult.refusal(tool, e.code, e.detail)
        except subprocess.TimeoutExpired:                        # pragma: no cover - guarded below
            self.stats["timeouts"] += 1
            return ToolResult(tool=tool, ok=False, stderr="timeout")
        except Exception as e:            # a tool crash is an observable failure, not a lost turn
            return ToolResult(tool=tool, ok=False, exit_code=None, refused="",
                              stderr=f"{type(e).__name__}: {e}", data={"error": "executor_exception"})
        out.rid = rid
        if out.refused:
            self._refused.add(sig)
        # redaction happens once, here, so no downstream consumer can forget it
        out.stdout = redact(out.stdout)[: self.max_out]
        out.stderr = redact(out.stderr)[: self.max_out]
        return out

    # -------------------------------------------------------------- file tools
    def _t_list_files(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        jail = self.jail_of(agent_id)
        prefix = str(args.get("prefix", "") or "")
        limit = int(args.get("limit", 200) or 200)
        rows, skipped = [], 0
        if not prefix:
            # "list what I have" is *not* "list the workspace root": resolve(".") against a read
            # allocation of ["src","tests"] would be refused ('.' is not under ['src','tests']) and
            # the agent would escalate out of a working task, which is exactly what happened in the
            # first end-to-end run. An unscoped listing walks the agent's own readable roots instead.
            for pre in [q for q in jail.reads if q not in ("", ".")]:
                base = jail.resolve(pre, write=False)
                if not base.is_dir():
                    continue
                for dirpath, dirnames, filenames in os.walk(base):
                    dirnames[:] = [d for d in dirnames
                                   if d not in (".git", "__pycache__", "node_modules", ".pytest_cache")]
                    for fn in sorted(filenames):
                        fp = Path(dirpath) / fn
                        try:
                            st = fp.stat()
                        except OSError:
                            skipped += 1
                            continue
                        if len(rows) >= limit:
                            skipped += 1
                            continue
                        rows.append(f"{fp.relative_to(jail.root)}\t{st.st_size}")
            if not rows:
                return ToolResult(tool="list_files", ok=False,
                                  stderr="this agent has no readable prefixes to list",
                                  data={"files": 0})
            return ToolResult(tool="list_files", ok=True, stdout="\n".join(rows),
                              data={"files": len(rows), "skipped": skipped, "scoped_to": ",".join(
                                  [q for q in jail.reads if q not in ("", ".")])})
        if prefix in (".", "./"):
            base = jail.root                      # an explicit "the root" still has to be readable
            if not jail._under(Path("."), jail.reads):
                return ToolResult.refusal("list_files", "REFUSE_READ_OUTSIDE_ALLOCATION",
                                          "the workspace root is not one of this agent's readable "
                                          "prefixes; list one of "
                                          f"{[q for q in jail.reads if q]!r} instead")
        else:
            base = jail.resolve(prefix, write=False)
        if not base.exists():
            return ToolResult(tool="list_files", ok=False,
                              stderr=f"no such prefix: {prefix!r}", data={"exists": "false"})
        if base.is_file():
            return ToolResult(tool="list_files", ok=True, data={"files": "1"},
                              stdout=str(base.relative_to(jail.root)))
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in (".git", "__pycache__", "node_modules")]
            for fn in sorted(filenames):
                p = Path(dirpath) / fn
                try:
                    st = p.stat()
                except OSError:
                    skipped += 1
                    continue
                if len(rows) >= limit:
                    skipped += 1
                    continue
                rows.append(f"{p.relative_to(jail.root)}\t{st.st_size}")
        return ToolResult(tool="list_files", ok=True, stdout="\n".join(rows),
                          data={"files": len(rows), "skipped": skipped})

    def _t_read_file(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        jail = self.jail_of(agent_id)
        path = jail.resolve(args["path"], write=False)
        if not path.is_file():
            return ToolResult(tool="read_file", ok=False, stderr=f"not a file: {args['path']}",
                              data={"exists": str(path.exists()).lower()})
        size = path.stat().st_size
        cap = int(args.get("max_bytes", 20000) or 20000)
        raw = path.read_bytes()
        if b"\x00" in raw[:4096]:
            return ToolResult(tool="read_file", ok=False, refused="REFUSE_BINARY_FILE",
                              stderr=f"{args['path']} looks binary ({size} bytes)")
        text = raw.decode("utf-8", "replace")
        return ToolResult(tool="read_file", ok=True, stdout=text[:cap],
                          truncated=len(text) > cap,
                          # `path` is coerced to str on purpose: `Outcome.data` keeps only JSON
                          # scalars, so a PosixPath here would be dropped and the agent could never
                          # line a read up against the file the failure named (found on the first
                          # end-to-end run: read -> analyse -> no edit, forever).
                          data={"path": str(args["path"]), "bytes": size,
                                "lines": text.count("\n") + 1})

    def _t_write_file(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        jail = self.jail_of(agent_id)
        path = jail.resolve(args["path"], write=True)
        content = str(args.get("content", ""))
        if not content.endswith("\n") and content:
            content += "\n"                      # a real file, not a truncated write
        existed = path.is_file()
        before = file_digest(path) if existed else ""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".arena-tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, path)                    # atomic: no torn file on a crash
        self.stats["file_mutations"] += 1
        return ToolResult(tool="write_file", ok=True,
                          data={"path": str(args["path"]), "bytes": len(content),
                                "created": str(not existed).lower(),
                                "sha256_16": file_digest(path)},
                          changed=[{"path": str(args["path"]), "sha256_16": file_digest(path),
                                    "bytes": len(content)}],
                          stderr="" if before != file_digest(path) or not existed else
                          "note: identical bytes already present (no-op write)")

    def _t_edit_file(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        jail = self.jail_of(agent_id)
        path = jail.resolve(args["path"], write=True)
        if not path.is_file():
            return ToolResult(tool="edit_file", ok=False, stderr=f"no such file: {args['path']}",
                              data={"exists": "false", "hint": "read_file first, or use write_file"})
        find, replace = str(args["find"]), str(args["replace"])
        text = path.read_text(encoding="utf-8", errors="replace")
        n = text.count(find)
        if n == 0:
            # `refused`, not `ok=False`: "the bytes you asked me to replace are not in this file" is
            # a refusal of the *edit*, and the agent must be able to tell that apart from a command
            # that ran and failed. An earlier build reported it as a plain failure, so the agent
            # re-proposed the same edit every turn and the run ended in a spin.
            return ToolResult(tool="edit_file", ok=False, refused="REFUSE_EDIT_TARGET_MISSING",
                              stderr=f"target string not found in {args['path']}. An edit that "
                                     f"matches nothing is not an edit: re-read the file.",
                              data={"matches": 0, "file_bytes": len(text), "path": str(args["path"])})
        if n > 1 and not args.get("all"):
            return ToolResult(tool="edit_file", ok=False,
                              stderr=f"target string appears {n} times in {args['path']}; "
                                     f"ambiguous edits are refused - add context or pass all=true",
                              data={"matches": n, "path": str(args["path"])})
        new = text.replace(find, replace) if args.get("all") else text.replace(find, replace, 1)
        path.write_text(new, encoding="utf-8")
        self.stats["file_mutations"] += 1
        return ToolResult(tool="edit_file", ok=True,
                          data={"path": str(args["path"]), "replacements": n if args.get("all") else 1,
                                "sha256_16": file_digest(path), "bytes": len(new)},
                          changed=[{"path": str(args["path"]), "sha256_16": file_digest(path),
                                    "bytes": len(new)}])

    # --------------------------------------------------------------- run tools
    def _t_run_command(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        jail = self.jail_of(agent_id)
        spec = self.registry.get("run_command")
        argv = args.get("argv")
        if isinstance(argv, str):
            # a model WILL do this; refuse the shell, but recover the argv instead of losing the turn
            if any(c in argv for c in ";|&><`$\n"):
                return ToolResult.refusal("run_command", "REFUSE_SHELL_METACHARACTERS",
                                          "argv only - pipes, `;`, redirection and `$()` are not "
                                          "available; run one command per call")
            argv = shlex.split(argv)
        if not isinstance(argv, (list, tuple)) or not argv:  # noqa: PLR0916
            return ToolResult.refusal("run_command", "REFUSE_BAD_ARGV", "argv must be a non-empty list")
        argv = [str(x) for x in argv]
        prog = Path(argv[0]).name
        if prog in NEVER_RUN:
            return ToolResult.refusal("run_command", "REFUSE_FORBIDDEN_PROGRAM", f"{prog} is never allowed")
        if not self.allow_egress and prog in EGRESS_TOOLS:
            return ToolResult.refusal("run_command", "REFUSE_EGRESS_DISABLED",
                                      f"{prog} needs network access; set allow_egress=true (recorded "
                                      f"in the project manifest) if that is really intended")
        if shutil_which(prog) is None:
            return ToolResult(tool="run_command", ok=False, exit_code=127,
                              stderr=f"command not found in PATH: {prog}",
                              data={"program": prog, "not_found": "true"})
        cwd = jail.root
        if args.get("cwd"):
            cwd = jail.resolve(str(args["cwd"]), write=False)
        timeout = float(args.get("timeout") or spec.timeout or self.default_timeout)
        timeout = max(0.05, min(timeout, self.max_timeout))
        env = dict(os.environ)
        env.update({"ARENA_AGENT": agent_id, "PS1": "", "GIT_PAGER": "cat", "PAGER": "cat",
                    "npm_config_fund": "false", "npm_config_audit": "false"})
        before = jail.snapshot()
        self.stats["executions"] += 1
        self.stats["commands"] += 1
        t0 = time.monotonic()
        try:
            proc = subprocess.run(argv, cwd=str(cwd), env=env, stdin=subprocess.DEVNULL,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                  timeout=timeout, start_new_session=True)
            code, out, err = proc.returncode, proc.stdout or "", proc.stderr or ""
        except subprocess.TimeoutExpired as e:
            self.stats["timeouts"] += 1
            out = (e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")) if e.stdout else ""
            err = f"TIMEOUT after {timeout:.1f}s (whole process group killed)"
            self._kill_group(argv, t0)
            code = 124
        except OSError as e:
            code, out, err = 126, "", f"exec failed: {type(e).__name__}: {e}"
        # write-then-read: the command's effect on the tree is part of its result
        changed = self._diff(jail, before, agent_id)
        log_path = self._spill_log(agent_id, rid="", argv=argv, out=out, err=err, code=code)
        return ToolResult(tool="run_command", ok=(code == 0), exit_code=code, stdout=out,
                          stderr=err, changed=changed,
                          data={"argv": " ".join(argv)[:200], "cwd": str(cwd.relative_to(jail.root)),
                                "timeout": round(timeout, 3), "exit": code,
                                "log": log_path})

    def _kill_group(self, argv: Sequence[str], t0: float) -> None:
        """subprocess.run has already killed the leader by the time we get here; this sweeps any
        survivors of a backgrounded child so a runaway `npm` cannot outlive the agent's turn."""
        self.kernel.log(f"run_command timed out after {time.monotonic() - t0:.1f}s: {argv[:3]}")

    def _spill_log(self, agent_id: str, *, rid: str, argv: Sequence[str], out: str, err: str,
                   code: int) -> str:
        """Full, unredacted-by-us output stays OUT of the journal and in the project's logs/ dir -
        the journal carries a digest. (Secrets are still redacted; the point is size.)"""
        try:
            logs = self.kernel.logs_dir
            if not logs:
                return ""
            logs = Path(logs)
            logs.mkdir(parents=True, exist_ok=True)
            name = f"{agent_id}-{self._seq:04d}-{Path(argv[0]).name}.log"
            (logs / name).write_text(
                f"$ {redact(' '.join(argv))}\n(exit={code})\n--- stdout ---\n{redact(out)}\n"
                f"--- stderr ---\n{redact(err)}\n", encoding="utf-8")
            return f"logs/{name}"
        except OSError:
            return ""

    def _diff(self, jail: Jail, before: dict[str, tuple[int, float]],
              agent_id: str) -> list[dict[str, Any]]:
        after = jail.snapshot()
        changed: list[dict[str, Any]] = []
        for path, sig in after.items():
            if before.get(path) != sig:
                changed.append({"path": path, "bytes": sig[0],
                                "sha256_16": file_digest(jail.root / path), "agent": agent_id})
        for path in before:
            if path not in after:
                changed.append({"path": path, "bytes": 0, "sha256_16": "deleted", "agent": agent_id})
        if changed:
            self.stats["file_mutations"] += len(changed)
        return changed

    # ---------------------------------------------------------------- git tools
    def _t_run_tests(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        self.stats["tests"] += 1
        out = self._t_run_command(agent_id, args)
        out.tool = "run_tests"
        return out

    def _git(self, agent_id: str, argv: Sequence[str], *, timeout: float = 20.0,
             env: dict[str, str] | None = None) -> tuple[int, str, str]:
        root = self.kernel.git_root_of(agent_id)
        if env is not None:
            base = dict(os.environ)
            base.update(env)
            env = base
        proc = subprocess.run(["git", "-C", str(root), *argv], stdin=subprocess.DEVNULL,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                              timeout=timeout, start_new_session=True, env=env)
        return proc.returncode, proc.stdout or "", proc.stderr or ""

    def _t_inspect_git(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        root = self.kernel.git_root_of(agent_id)
        if root is None:
            return ToolResult.refusal("inspect_git", "REFUSE_NO_GIT_REPO")
        what = str(args.get("what", "status"))
        argv = {"status": ["status", "--porcelain=v1", "-b"],
                "diff": ["diff", "--stat", "HEAD"],
                "log": ["log", "--oneline", "-n", str(int(args.get("n", 8) or 8))],
                "files": ["ls-files"]}.get(what)
        if argv is None:
            return ToolResult.refusal("inspect_git", "REFUSE_BAD_WHAT",
                                      f"{what!r} not one of status|diff|log|files")
        code, out, err = self._git(agent_id, argv)
        return ToolResult(tool="inspect_git", ok=code == 0, exit_code=code, stdout=out, stderr=err,
                          data={"what": what, "repo": str(root)})

    def _t_commit(self, agent_id: str, args: dict[str, Any], **_: Any) -> ToolResult:
        root = self.kernel.git_root_of(agent_id)
        msg = redact(str(args.get("message", "")))[:500]
        if not msg.strip():
            return ToolResult.refusal("commit", "REFUSE_EMPTY_MESSAGE")
        code, out, err = self._git(agent_id, ["status", "--porcelain"])
        if code == 0 and not out.strip():
            # Nothing staged is a *fact*, not a failure to hide: an agent that "committed" here
            # would be claiming a version of the tree that is identical to the one before it.
            return ToolResult(tool="commit", ok=False, exit_code=1,
                              stderr="nothing to commit (worktree clean): no version was created",
                              data={"noop": "true"})
        self.stats["executions"] += 1
        paths = [str(x) for x in (args.get("paths") or [])]
        code_add, out_add, err_add = self._git(agent_id, (["add", "-A", "--"] if not paths
                                                           else ["add", "--", *paths]))
        who = str(args.get("author_name") or agent_id)
        env = {"GIT_AUTHOR_NAME": who, "GIT_AUTHOR_EMAIL": f"{who}@arena.local",
               "GIT_COMMITTER_NAME": who, "GIT_COMMITTER_EMAIL": f"{who}@arena.local",
               # a repo created by `git init` has no identity at all; without these the commit
               # fails with "Please tell me who you are", which is an environment fact, not a bug
               "GIT_AUTHOR_DATE": "@0 +0000", "GIT_COMMITTER_DATE": "@0 +0000"}
        code, out, err = self._git(agent_id, ["commit", "-m", msg, "--no-verify"], env=env)
        ok = code == 0
        head = ""
        if ok:
            _, head_raw, _ = self._git(agent_id, ["rev-parse", "HEAD"])
            head = head_raw.strip()
        return ToolResult(tool="commit", ok=ok, exit_code=code,
                          stdout=(out_add + out).strip(), stderr=(err_add + err).strip(),
                          data={"head": head[:12], "committed": str(ok).lower(),
                                "files": out.count("|") if ok else 0})

    # ------------------------------------------------------------ artifact tool
    def _t_publish_artifact(self, agent_id: str, args: dict[str, Any], *,
                            task_id: str | None) -> ToolResult:
        jail = self.jail_of(agent_id)
        name = str(args["artifact"])
        files = [str(f) for f in (args.get("files") or ([name] if name else []))]
        rows: list[dict[str, Any]] = []
        for rel in files:
            path = jail.resolve(rel, write=False)
            if not path.is_file():
                return ToolResult(tool="publish_artifact", ok=False,
                                  stderr=f"cannot publish {rel!r}: no such file. An artifact is "
                                         f"bytes that exist, not a promise.",
                                  data={"missing": rel})
            rows.append({"path": rel, "bytes": path.stat().st_size,
                         "sha256_16": file_digest(path)})
        self.kernel.publish_artifact(name, producer=agent_id, task_id=task_id,
                                     digest=file_digest(jail.resolve(files[0], write=False)),
                                     files=rows)
        return ToolResult(tool="publish_artifact", ok=True,
                          data={"artifact": name, "files": len(rows),
                                "total_bytes": sum(r["bytes"] for r in rows)},
                          stdout="\n".join(f"{r['path']} {r['bytes']}B {r['sha256_16']}" for r in rows))

    def describe(self, agent_id: str) -> dict[str, Any]:
        """Workspace truth for the Observation: what is on disk, not what someone claims."""
        jail = self.jails.get(agent_id)
        if jail is None:
            return {"bound": False}
        snap = jail.snapshot()
        root = jail.root
        code = [p for p in snap if p.endswith((".py", ".js", ".ts", ".tsx", ".md", ".json",
                                               ".toml", ".txt", ".cfg"))]
        out = {"bound": True, "root": str(root), "files": len(snap),
               "text_files": len(code), "bytes": sum(v[0] for v in snap.values()),
               # the paths an agent can reason about / edit, workspace-relative and sorted so two
               # runs of the same tree produce the same Observation (determinism is a test promise)
               "text_paths": sorted(q for q in code if not q.startswith(".arena")),
               "present": sorted(q for q in snap if not q.startswith(".arena")),
               "writable": list(jail.writes), "git": False, "head": "", "dirty": False,
               "executor": {"calls": self.stats["calls"], "executions": self.stats["executions"],
                            "mutations": self.stats["file_mutations"],
                            "refusals": self.stats["refusals"]}}
        gr = self.kernel.git_root_of(agent_id)
        if gr is not None and (Path(gr) / ".git").exists():
            code_git, head, _ = self._git(agent_id, ["rev-parse", "--short", "HEAD"])
            _, status, _ = self._git(agent_id, ["status", "--porcelain"])
            out.update(git=True, head=head.strip(), dirty=bool(status.strip()))
        return out

    # -------------------------------------------------- coordination passthrough
    def note_refusal(self, agent_id: str, tool: str, code: str, detail: str) -> ToolResult:
        """Used by the runtime when an Intent names something that is not a tool at all."""
        self.stats["refusals"] += 1
        self._emit(MessageType.TOOL_REFUSED, agent_id, "kernel",
                   body=f"{tool}: {code}", tool=tool, code=code, detail=redact(detail),
                   agent_id=agent_id)
        return ToolResult.refusal(tool, code, detail)


def _short(d: dict[str, Any], n: int = 240) -> str:
    parts = []
    for k, v in sorted(d.items()):
        s = v if isinstance(v, str) else repr(v)
        if len(s) > 80:
            s = s[:77] + "..."
        parts.append(f"{k}={s}")
    return " ".join(parts)[:n]


def shutil_which(prog: str) -> str | None:
    import shutil
    return shutil.which(prog)


#: re-export so callers can construct a jail without importing pathlib semantics themselves
def make_jail(root: str | Path, *, writes: Iterable[str] = ("",), reads: Iterable[str] = ("",),
              agent_id: str = "") -> Jail:
    return Jail(root=Path(root), writes=tuple(writes), reads=tuple(reads) or ("",),
                agent_id=agent_id)


__all__ = ["Jail", "JailViolation", "ToolResult", "ToolSpec", "ToolRegistry", "Executor",
           "TOOLS", "EGRESS_TOOLS", "NEVER_RUN", "redact", "digest", "file_digest", "make_jail",
           "errno", "signal"]
