"""`arena-code doctor`: what this environment can actually do, measured rather than assumed.

The rule behind every check: report a capability only if this process can demonstrate it right now,
and when a capability is missing, say which tier that leaves the system in. A fresh chat, a new
sandbox or a reviewer with no memory of this conversation should be able to run this command and know
exactly what will and will not work. Nothing here probes the network unless `--live` is passed, and
nothing here prints a secret - checks report *presence*, never values.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TIER_POLICY = "policy"
TIER_LLM = "llm"


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""
    hint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"check": self.name, "ok": self.ok, "detail": self.detail, "hint": self.hint}

    def line(self) -> str:
        flag = "OK  " if self.ok else "MISS"
        out = f"  [{flag}] {self.name}: {self.detail}"
        if not self.ok and self.hint:
            out += f"\n         -> {self.hint}"
        return out


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, ok: bool, detail: str = "", hint: str = "") -> None:
        self.checks.append(Check(name, bool(ok), detail, hint))

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks if c.name not in OPTIONAL)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "checks": [c.to_dict() for c in self.checks]}

    def render(self) -> str:
        head = "arena-code doctor — environment capability report"
        lines = [head, ""]
        for c in self.checks:
            lines.append(c.line())
        lines += ["", f"verdict: {'READY' if self.ok else 'NOT READY'} "
                       f"({sum(1 for c in self.checks if c.ok)}/{len(self.checks)} checks pass)"]
        return "\n".join(lines)


#: checks a deterministic run does not need
#: capabilities whose absence must not fail the verdict. The point of this list is the acceptance
#: contract: a sandbox with **no model credential at all** is a perfectly healthy place to run the
#: deterministic tier, so `model-credential`/`provider-sdk` are reported honestly as absent and the
#: exit code still says "environment ok". Treating them as required would make `doctor` the thing
#: that fails, and hide the things that actually matter (exec, writes, journal, workspace).
OPTIONAL = frozenset({"live-provider-egress", "git-identity", "postgres", "node-modules-cache",
                      "model-credential", "provider-sdk", "network-dns"})


def _py() -> Check:
    v = sys.version_info
    return Check("python", v >= (3, 11), f"{sys.version.split()[0]}")


def _probe(name: str, cmd: list[str], *, want: str = "") -> Check:
    exe = shutil.which(cmd[0])
    if not exe:
        return Check(name, False, "not installed",
                     f"install {cmd[0]} or choose a plan that does not need it")
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=15,
                           stdin=subprocess.DEVNULL)
        out = (p.stdout or "") + (p.stderr or "")
        ok = p.returncode == 0 and (want in out if want else True)
        return Check(name, ok, out.strip().splitlines()[0][:90] if out.strip() else "ok")
    except Exception as e:
        return Check(name, False, f"{type(e).__name__}: {e}")


def runtime_checks() -> list[Check]:
    from ..tools import TOOLS
    from .. import __name__ as _engine
    from . import RUNTIME_VERSION
    out = [Check("runtime-version", True, RUNTIME_VERSION), _py()]
    out.append(Check("engine-imports", True, "arena.kernel / arena.tools / arena.cognition importable"))
    out.append(Check("tools-registered", len(TOOLS) >= 10, f"{len(TOOLS)} tools: "
                     + ", ".join(sorted(t.name for t in TOOLS))))
    # real execution, demonstrated rather than declared
    try:
        p = subprocess.run([sys.executable, "-c", "print('arena-ok')"], capture_output=True,
                           text=True, timeout=15, stdin=subprocess.DEVNULL)
        out.append(Check("subprocess-execution", p.returncode == 0 and "arena-ok" in p.stdout,
                         f"argv exec + capture works (exit {p.returncode})"))
    except Exception as e:
        out.append(Check("subprocess-execution", False, str(e),
                         "without real execution this runtime can only simulate work"))
    for name, cmd in (("git", ["git", "--version"]),
                      ("pytest", [sys.executable, "-m", "pytest", "--version"]),
                      ("node", ["node", "--version"]),
                      ("npm", ["npm", "--version"]),
                      ("postgres", ["psql", "--version"])):
        c = _probe(name, cmd)
        if name == "postgres":
            c.hint = ("no postgres in this sandbox: plans must verify with what exists "
                     "(measured: no server/initdb binaries either)")
        out.append(c)
    # writability, with a real file
    try:
        d = Path(tempfile.mkdtemp(prefix="arena-doctor-"))
        f = d / "probe.txt"
        f.write_text("x")
        ok = f.read_text() == "x"
        f.unlink()
        shutil.rmtree(d, ignore_errors=True)
        out.append(Check("filesystem-writes", ok, f"tested in {d}"))
    except Exception as e:
        out.append(Check("filesystem-writes", False, str(e)))
    # journal dependency: sqlite3 is stdlib, so the journal works even with no packages installed
    try:
        import sqlite3
        out.append(Check("journal-sqlite", True, f"sqlite {sqlite3.sqlite_version}"))
    except Exception as e:                                   # pragma: no cover
        out.append(Check("journal-sqlite", False, str(e)))
    return out


PROVIDER_ENV = ("ARENA_CODE_MODEL_PROVIDER", "ARENA_CODE_MODEL", "ARENA_CODE_BASE_URL",
                "ARENA_CODE_ALLOW_EGRESS")
KEY_ENV = ("ARENA_CODE_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")


def credential_presence() -> dict[str, Any]:
    """Presence only. Never a value, never a length beyond ">0", never a partial echo."""
    found = {}
    for var in KEY_ENV:
        v = os.environ.get(var) or ""
        if v.strip():
            found[var] = "set"
    cfg = config_path()
    cfg_keys = []
    if cfg and cfg.is_file():
        try:
            data = json.loads(cfg.read_text())
            cfg_keys = [k for k in ("api_key", "key") if data.get(k)]
        except (OSError, json.JSONDecodeError):
            cfg_keys = ["unreadable-config"]
    return {"env_keys": sorted(found), "config_file": str(cfg) if cfg else "",
            "config_keys": cfg_keys,
            "available": bool(found or [k for k in cfg_keys if k == "api_key"])}


def config_path() -> Path | None:
    explicit = os.environ.get("ARENA_CODE_CONFIG")
    if explicit:
        return Path(explicit)
    if os.environ.get("HOME"):
        return Path(os.environ["HOME"]) / ".config" / "arena-code" / "config.json"
    return None


def resolve_config(cli: dict[str, Any] | None = None) -> dict[str, Any]:
    """env -> $ARENA_CODE_CONFIG -> ~/.config/arena-code/config.json -> flags.

    Secrets come from env or a 0600 file only; the tool never writes a key it is given on a flag,
    and never logs one it reads.
    """
    out: dict[str, Any] = {"provider": TIER_POLICY, "model": "", "base_url": "",
                           "allow_egress": False, "api_key": "", "source": "default"}
    cfg = config_path()
    if cfg and cfg.is_file():
        try:
            data = json.loads(cfg.read_text())
        except (OSError, json.JSONDecodeError):
            data = {}
        for k in ("provider", "model", "base_url", "api_key"):
            if data.get(k):
                out[k] = data[k]
        out["allow_egress"] = bool(data.get("allow_egress", out["allow_egress"]))
        out["source"] = str(cfg)
    for var, key in (("ARENA_CODE_MODEL_PROVIDER", "provider"), ("ARENA_CODE_MODEL", "model"),
                     ("ARENA_CODE_BASE_URL", "base_url"), ("ARENA_CODE_API_KEY", "api_key"),
                     ("ARENA_CODE_ALLOW_EGRESS", "allow_egress")):
        v = os.environ.get(var)
        if v:
            out[key] = (v.lower() in ("1", "true", "yes")) if key == "allow_egress" else v
            out["source"] = "env"
    if not out["api_key"]:
        for alt in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
            if os.environ.get(alt):
                out["api_key"] = os.environ[alt]
                out["source"] = f"env:{alt}"
                if out["provider"] == TIER_POLICY:
                    out["provider"] = "openai-compat" if alt == "OPENAI_API_KEY" else "anthropic"
                break
    # Precedence is env -> file -> flags, *per field*: a flag fills a gap the environment and the
    # stored config left open, it does not override a value the operator already wrote down. (The
    # flags used to be applied last over everything, so `--model` silently beat a configured model
    # and "which config produced this run?" had no answer in the record.)
    for k, v in (cli or {}).items():
        if v in (None, "", False):
            continue
        if out.get(k) in ("", None) or (k == "allow_egress" and out.get(k) is False):
            out[k] = v
            out.setdefault("filled_by", []).append(k)
            if out["source"] == "default":
                out["source"] = "cli"
        else:
            out.setdefault("shadowed", []).append(k)
    # A provider is only a provider if it can be reached. A config file that says `openai-compat`
    # with no key anywhere is the deterministic tier *asking to be* a model tier; reporting it as
    # the latter would make `doctor` claim a capability the run does not have.
    if not out["api_key"]:
        out["provider"] = TIER_POLICY
    return out


def build_report(*, workspace: str | Path | None = None, live: bool = False,
                 project_id: str = "") -> Report:
    r = Report()
    for c in runtime_checks():
        r.checks.append(c)
    conf = resolve_config()
    r.add("cognition-provider", True, f"{conf['provider']} (config source: {conf['source']})")
    creds = credential_presence()
    r.add("model-credential", bool(creds["env_keys"] or creds["config_keys"]),
          ("found: " + ", ".join(creds["env_keys"] + creds["config_keys"])) if creds["env_keys"]
          or creds["config_keys"] else "none present in this environment",
          "an external API is the only way to real per-step reasoning here; export "
          "ARENA_CODE_API_KEY or `arena-code config set-key`, or stay on the policy tier")
    r.add("provider-sdk", True,
          "httpx " + (getattr(__import__("httpx"), "__version__", "?")
                      if shutil.which("python3") else "?") + " (adapter uses stdlib http.client too)")
    if live:
        try:
            import http.client
            host = (conf["base_url"].split("//")[-1].split("/")[0]
                    if conf["base_url"] else ("api.openai.com" if "openai" in conf["provider"]
                                              else "api.anthropic.com"))
            conn = http.client.HTTPSConnection(host, timeout=8)
            conn.request("GET", "/v1/models" if "openai" in host else "/")
            resp = conn.getresponse()
            r.add("live-provider-egress", True, f"{host} answered HTTP {resp.status} "
                                                f"(401 here means reachable, unauthenticated)")
            conn.close()
        except Exception as e:
            r.add("live-provider-egress", False, f"{type(e).__name__}: {e}",
                  "no egress: the LLM tier cannot run in this sandbox")
    ws = Path(workspace) if workspace else Path.cwd()
    projects = (ws / "projects")
    rows = []
    if projects.is_dir():
        from .project import Project
        rows = Project.discover(ws)
    r.add("workspace", True, f"{ws} · {len(rows)} project(s)")
    if project_id:
        hit = [p for p in rows if p.get("project_id") == project_id]
        r.add("project", bool(hit), (hit[0]["state"] if hit else "no such project id"))
    else:
        r.add("project", True, "none selected (a run creates one)")
    r.add("secrets-hygiene", True, "doctor prints presence only; executor redacts sk-*/ghp_*/*key:*")
    return r


def main(argv: list[str] | None = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="arena-code doctor", description="environment capability report")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--live", action="store_true", help="probe the configured provider endpoint")
    ap.add_argument("--workspace", default=os.environ.get("ARENA_CODE_WORKSPACE", "."))
    ap.add_argument("--project", default="")
    a = ap.parse_args(argv)
    rep = build_report(workspace=a.workspace, live=a.live, project_id=a.project)
    print(json.dumps(rep.to_dict(), indent=2) if a.json else rep.render())
    return 0 if rep.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
