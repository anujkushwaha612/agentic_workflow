"""Project workspaces: where a run lives, and the rules that keep it from clobbering anything.

Design rules this file enforces (from `REAL_AGENT_RUNTIME_DESIGN.md` §7):

* the framework and the generated project are separate trees - a run only ever writes under
  `projects/<id>/`;
* a directory that already exists and is *not* a project is never touched - refuse, don't guess;
* the journal lives **inside** the project (`.arena/`), because a journal for N projects in one file
  makes `verify` and replay ambiguous;
* `.arena` is a protected path name inside every agent's jail, so an agent cannot edit its own log.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import RUNTIME_VERSION

#: refuse to overwrite; these are the files that mean "this directory is ours"
MANIFEST = "project.json"
DIRS = (".arena", "source", "artifacts", "logs")
STATE_ORDER = ["created", "planned", "running", "awaiting-cortex", "verified", "completed", "failed"]


def project_id(goal: str = "", *, when: float | None = None) -> str:
    """`20260901-2135-task-tracker` - sortable, readable, and collision-checked by the caller."""
    ts = time.localtime(when if when is not None else time.time())
    slug = re.sub(r"[^a-z0-9]+", "-", (goal or "project").lower()).strip("-")[:32].strip("-")
    return f"{time.strftime('%Y%m%d-%H%M%S', ts)}-{slug or 'project'}"


@dataclass
class Project:
    """One workspace on disk, plus the manifest that describes it."""

    root: Path
    manifest: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------ paths
    @property
    def arena_dir(self) -> Path:
        return self.root / ".arena"

    @property
    def source_dir(self) -> Path:
        return self.root / "source"

    @property
    def artifacts_dir(self) -> Path:
        return self.root / "artifacts"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def journal_path(self) -> Path:
        return self.arena_dir / "j.db"

    @property
    def inject_path(self) -> Path:
        return self.arena_dir / "inject.jsonl"

    @property
    def decisions_path(self) -> Path:
        return self.arena_dir / "decisions.jsonl"

    @property
    def context_dir(self) -> Path:
        return self.arena_dir / "context"

    def is_project(self) -> bool:
        return (self.arena_dir / MANIFEST).is_file()

    # ------------------------------------------------------------ construction
    @classmethod
    def create(cls, workspace: str | Path, goal: str, *, project_id_hint: str = "",
               resume: bool = False, force_new: bool = False, meta: dict[str, Any] | None = None
               ) -> "Project":
        """Create `workspace/projects/<id>/`. Never overwrites; never reuses a foreign directory."""
        ws = Path(workspace).expanduser().resolve()
        pid = project_id_hint or project_id(goal)
        root = ws / "projects" / pid
        if (root / MANIFEST).is_file() or (root / ".git").exists():
            raise FileExistsError(
                f"{root} is an existing project or repository. `arena-code` will not overwrite it: "
                f"use `--resume {pid}` to continue it, or a different id.")
        if root.exists() and any(root.iterdir()) and not force_new:
            raise FileExistsError(
                f"{root} exists and is not a project (no {MANIFEST}). Refusing to write into a "
                f"directory we did not create; pass --force-new to start one inside it anyway.")
        if resume and not root.is_dir():
            raise FileNotFoundError(f"--resume {pid}: no such project at {root}")
        for d in DIRS:
            (root / d).mkdir(parents=True, exist_ok=True)
        (root / ".arena" / "context").mkdir(parents=True, exist_ok=True)
        (root / ".arena" / "tool-log").mkdir(parents=True, exist_ok=True)
        p = cls(root=root)
        p.manifest = {
            "project_id": pid,
            "prompt": goal,
            "created_at": time.time(),
            "created_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "runtime_version": RUNTIME_VERSION,
            "state": "created",
            "history": [{"state": "created", "at": time.time()}],
            "workspace": str(ws),
            "layout": {"journal": ".arena/j.db", "source": "source/", "artifacts": "artifacts/",
                       "logs": "logs/", "context": ".arena/context/"},
            "provenance": {"launched_by": (meta or {}).get("launched_by", "cli"),
                           "prompt_source": (meta or {}).get("prompt_source", "argv")},
            **(meta or {}),
        }
        p.write_manifest()
        p._emit_created()
        return p

    @classmethod
    def load(cls, workspace: str | Path, project_id_hint: str) -> "Project":
        ws = Path(workspace).expanduser().resolve()
        root = ws / "projects" / project_id_hint
        m = root / ".arena" / MANIFEST
        if not m.is_file():
            # a project can also have been created with the manifest at root by an older build
            m2 = root / MANIFEST
            if not m2.is_file():
                raise FileNotFoundError(f"no project {project_id_hint!r} under {ws / 'projects'} "
                                        f"(looked for .arena/{MANIFEST})")
            m = m2
        p = cls(root=root, manifest=json.loads(m.read_text()))
        return p

    @classmethod
    def discover(cls, workspace: str | Path) -> list[dict[str, Any]]:
        base = Path(workspace).expanduser() / "projects"
        out = []
        if not base.is_dir():
            return out
        for d in sorted(base.iterdir()):
            man = (d / ".arena" / MANIFEST)
            if not man.is_file():
                man = d / MANIFEST
            if man.is_file():
                try:
                    rows = json.loads(man.read_text())
                except (OSError, json.JSONDecodeError):
                    rows = {"project_id": d.name, "state": "unreadable-manifest"}
                rows.setdefault("project_id", d.name)
                rows["_path"] = str(d)
                out.append(rows)
        return out

    # ------------------------------------------------------------------- io
    def write_manifest(self) -> None:
        for target in (self.arena_dir / MANIFEST, self.root / MANIFEST):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(self.manifest, indent=2, default=str) + "\n")

    def set_state(self, state: str, **extra: Any) -> None:
        self.manifest["state"] = state
        self.manifest.setdefault("history", []).append({"state": state, "at": time.time(),
                                                         **extra})
        self.write_manifest()

    def update(self, **extra: Any) -> None:
        self.manifest.update(extra)
        self.write_manifest()

    def _emit_created(self) -> None:
        """`PROJECT_CREATED` is a journal row, not just a manifest field: the origin of a run must be
        reconstructible from the log alone (that is what "the journal is the truth" means)."""
        from ..journal import Journal
        from ..message import MessageType
        j = Journal(path=self.journal_path)
        try:
            j.emit(MessageType.PROJECT_CREATED, "launcher", "kernel",
                   body=f"project {self.manifest['project_id']} created for: "
                        f"{str(self.manifest['prompt'])[:120]}",
                   project_id=self.manifest["project_id"], prompt=self.manifest["prompt"],
                   runtime_version=RUNTIME_VERSION, layout=self.manifest["layout"],
                   provenance=self.manifest["provenance"])
        finally:
            j.close()

    # --------------------------------------------------------------- summary
    def status_row(self) -> dict[str, Any]:
        src = self.source_dir
        files = [p for p in (src.rglob("*") if src.is_dir() else []) if p.is_file()]
        tracked = [f for f in files if ".git/" not in str(f)]
        return {"project_id": self.manifest.get("project_id"), "state": self.manifest.get("state"),
                "prompt": (self.manifest.get("prompt") or "")[:80],
                "source_files": len(tracked),
                "source_bytes": sum(f.stat().st_size for f in tracked),
                "has_journal": self.journal_path.exists(),
                "path": str(self.root)}
