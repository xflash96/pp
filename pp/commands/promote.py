"""Snapshot a project into a release and switch to it

The working tree is dev. A promotion copies one exact state of it (git HEAD, or the tree) into an immutable
release directory, runs the project's gate ON THOSE BYTES, then flips `current` to it with one rename(2).
Tools and services are registered by stable links that point through `current`, so a later flip needs no
relink, and nothing registered ever runs a half-edited tree.

    pp promote [PATH]              promote the project containing PATH (default: cwd)
    pp promote -n [PATH]           preflight: the plan and EVERY blocker at once; changes nothing
    pp promote --dirty             git mode: snapshot the working tree as-is (tracked + untracked, minus ignored)
    pp promote --again             promote even when HEAD already is the current release
    pp promote status [--json]     every promoted target: current release, how far the source is ahead, links
    pp promote ls NAME             releases, newest first (* = current)
    pp promote rollback NAME [--to STAMP] [-y]   flip back to the previous release, rerun `after`; a release
                                   whose own `after`/`verify` failed is skipped (ls marks it); --to picks any
    pp promote diff NAME           what the source has that the current release does not
    pp promote link NAME           reassert the links           pp promote gc NAME [--keep N] [-n]   prune releases, after asking
    pp promote init [PATH]         write a starter .pp.toml
NAME is a promoted target's name, or a path into its project.

.pp.toml at the project root:
    name = "pp"            # optional, default the directory name; it names the releases dir: one path
                           # component, and unique across the projects you promote
    [promote]
    source = "git"         # git: HEAD via `git archive` (a dirty tracked tree refuses unless --dirty)
                           # tree: copy the working tree (stamp = content hash)
    exclude = []           # added to .git node_modules __pycache__ .venv *.pyc (tree mode and --dirty)
    build = "make"         # optional; cwd = the new release, before the check
    check = "python3 -m unittest discover -s tests -q"   # the gate; non-zero refuses, current untouched
    links = { "~/.local/bin/pp" = "bin/pp" }        # stable path -> releases/<name>/current/<rel>
    dev_links = { "~/.local/bin/ppd" = "bin/pp" }   # stable path -> <source>/<rel>: the dev side
    after = "systemctl --user restart foo"          # after the flip
    verify = "curl -fsS localhost:8100/ >/dev/null" # after `after`; non-zero (or a failed `after`) rolls back
    keep = 5               # releases kept; current, the one before it and the one a promote leaves always stay
`after` is ACTIVATION: make what is running pick up `current` (restart or reload a service). It must be
idempotent and correct for ANY release, because rolling back is "flip to the previous release, run `after`
again". That is also why a failed `after` rolls back: whatever the cause, the previous release is the last state
known to work, and leaving `current` on a release nothing runs would make the pointer lie. Irreversible steps
(schema migrations, data rewrites) do not belong in `after`, because no rollback can undo them; run them by
hand, and keep them compatible with the release before. `verify` is the health check of the activated release.
Commands run in the release dir with PP_NAME PP_SOURCE PP_REV PP_RELEASE (where the release's bytes are: a
staging dir during build/check) and PP_PREV (the previous release dir, or empty). Git submodules are not
included (git archive leaves them out).

Releases are ~/.local/share/pp/releases/<name>/<YYYYmmdd-HHMMSS>-<rev7>[-dirty]/; `current` is a relative
symlink; revisions.log there records every flip; each run's full output is kept in
~/.local/state/pp/promote/logs/. The gate's verdict is evidence, not scrollback.

Registering a service: point the unit at `current`, let `after` restart it.
    # ~/.config/systemd/user/notes.service
    [Service]
    ExecStart=%h/.local/share/pp/releases/notes/current/bin/serve --port 8100
    # .pp.toml
    [promote]
    check = "./test.sh"
    after = "systemctl --user restart notes"
    verify = "sleep 1; curl -fsS http://127.0.0.1:8100/ >/dev/null"
Dev runs from the working tree (another port); prod only ever runs promoted bytes.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import shutil
import signal
import sys
import tarfile
import threading
import time
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone

from pp.lib import config, lock, paths, proc, ui

VERBS = ("status", "ls", "rollback", "diff", "link", "gc", "init")
DEFAULT_EXCLUDE = [".git", "node_modules", "__pycache__", ".venv", "*.pyc"]
META = paths.RELEASE_META          # its fields and readers: pp/lib/paths.py
NOT_RELEASES = ("current", "revisions.log")


# ---- targets --------------------------------------------------------------------------------------

def _releases(name) -> str:
    """Where this target's releases live. A name becomes a path here and nowhere else, so this is where a name
    that is not one plain path component is refused: `gc` removes the directories under this one, and a name of
    `..` would make it the whole data directory. It holds for a registry key as much as for `name` in a
    .pp.toml, since both name the same directory.

    What cannot be one directory is all that is refused, because most names are names pp derived rather than
    names a user wrote: with no `name` key the project directory's own name is the target's, a dot in front and
    all (~/.dotfiles), and a name pp refused here would refuse a project it has always taken."""
    if not isinstance(name, str) or name in ("", ".", "..") or name != os.path.basename(name) or "\0" in name:
        raise config.ConfigError(f"name = {name!r}: one path component, since it names the releases directory: "
                                 f"no '/', not '.' or '..', and not empty")
    return paths.data_dir("releases", name, create=False)


class Target:
    def __init__(self, name: str, source: str, cfg: dict):
        self.name, self.source, self.cfg = name, source, cfg
        self.mode = cfg.get("source", "git")
        self.releases = _releases(name)

    def current(self) -> str | None:
        try:
            return os.readlink(os.path.join(self.releases, "current"))
        except OSError:
            return None

    def dir(self, stamp: str | None) -> str:
        return os.path.join(self.releases, stamp) if stamp else ""

    def info(self, stamp: str | None) -> dict:
        try:
            with open(os.path.join(self.releases, stamp, META)) as f:
                return json.load(f)
        except (OSError, ValueError, TypeError):
            return {}

    def stamps(self) -> list[str]:
        """Releases, oldest first, by creation time: two stamps in one second sort by rev, not age."""
        try:
            names = [n for n in os.listdir(self.releases)
                     if not n.startswith(".") and n not in NOT_RELEASES and os.path.isdir(self.dir(n))]
        except OSError:
            return []
        return sorted(names, key=lambda n: (self.info(n).get("created", 0), n))

    def previous(self, stamp: str | None) -> str | None:
        """The newest release before stamp that did not fail activation (`after`/`verify` rolled it back)."""
        s = self.stamps()
        if stamp not in s:
            return None
        return next((p for p in reversed(s[:s.index(stamp)]) if not self.info(p).get("failed")), None)

    def excludes(self) -> list[str]:
        return DEFAULT_EXCLUDE + list(self.cfg.get("exclude", []))

    def env(self, rev: str, release: str, prev: str) -> dict:
        return dict(os.environ, PP_NAME=self.name, PP_SOURCE=self.source, PP_REV=rev or "",
                    PP_RELEASE=release, PP_PREV=prev or "")


DEFAULT_KEEP = 5         # releases kept; current, the one before it and the one a promote leaves always stay
PROMOTE_KEYS = ("source", "exclude", "build", "check", "links", "dev_links", "after", "verify", "keep")


def _checked(data: dict, root: str) -> dict:
    """A project's [promote] table, or a ConfigError naming the file and the key: a misspelt `check` must not
    release with nothing gating it, a `name` that is not one path component must not become a path, and a value
    of the wrong type must not reach the code that uses it. Every key is checked here, so preflight lists what
    is wrong and nothing is created before it is read."""
    where = f"{root}/.pp.toml"
    name = data.get("name")
    if name is not None:
        try:
            _releases(name)             # the rule is where the name becomes a path; this names the file it is in
        except config.ConfigError as e:
            raise config.ConfigError(f"{where} {e}") from None
    cfg = data.get("promote", {})
    if not isinstance(cfg, dict):
        raise config.ConfigError(f"{where}: promote must be a table ([promote] check = ..., links = {{...}})")
    config.check_keys(cfg, f"{where} [promote]", PROMOTE_KEYS)
    if cfg.get("source", "git") not in ("git", "tree"):
        raise config.ConfigError(f"{where} [promote] source = {cfg['source']!r}: git or tree")
    for key in ("build", "check", "after", "verify"):
        if key in cfg and not isinstance(cfg[key], str):
            raise config.ConfigError(f"{where} [promote] {key} = {cfg[key]!r}: one shell command, as a string")
    if "keep" in cfg and (isinstance(cfg["keep"], bool) or not isinstance(cfg["keep"], int)):
        raise config.ConfigError(f"{where} [promote] keep = {cfg['keep']!r}: how many releases to keep, a number")
    ex = cfg.get("exclude", [])
    if not isinstance(ex, list) or not all(isinstance(p, str) for p in ex):
        raise config.ConfigError(f"{where} [promote] exclude = {ex!r}: a list of patterns, each a string")
    for key in ("links", "dev_links"):
        links = cfg.get(key, {})
        if not isinstance(links, dict) or not all(isinstance(p, str) and isinstance(r, str) for p, r in links.items()):
            raise config.ConfigError(f"{where} [promote] {key} = {links!r}: a table of link path = path in the "
                                     f"release ({{ \"~/.local/bin/x\" = \"bin/x\" }})")
        for p in links:
            if not (os.path.isabs(p) or p.startswith("~")):
                raise config.ConfigError(f"{where} [promote] {key}: the link path {p!r} is relative, so where the "
                                         f"link lands would follow the directory pp ran in: an absolute path, or ~")
    return cfg


def _project(path: str | None) -> Target:
    root, data = config.find_project(path)
    if not root or "promote" not in data:
        ui.die(f"no [promote] table in a .pp.toml at or above {paths.expand(path or '.')}; `pp promote init` writes one", 2)
    return Target(config.project_name(root, data), root, _checked(data, root))


def _registry_file(create: bool = False) -> str:
    return os.path.join(paths.state_dir("promote", create=create), "registry.json")


def _registry(unusable: list | None = None) -> dict:
    """Every promoted target: its name, and where its project is. A key no releases path can be built from is
    no target to act on, and 0.1.0 wrote such a key, so it is dropped here: a verb over the registry then acts
    on the targets around it, and `pp promote` in that project refuses naming the file and the key, which is
    where the name is fixed. `unusable` collects the keys dropped, for `status` to say once: it is the one verb
    that surveys the registry, and a target it never lists is one to say something about."""
    try:
        with open(_registry_file()) as f:
            reg = json.load(f)
    except (OSError, ValueError):
        return {}
    kept = {}
    for name, row in reg.items():
        try:
            _releases(name)
        except config.ConfigError:
            if unusable is not None:
                unusable.append(name)
            continue
        kept[name] = row
    return kept


def _registry_lock() -> str:
    """The registry's lock file. The registry is one file for every target, so a target's own .lock serialises
    nothing here: two promotes of different projects hold different locks, and the later write would otherwise
    forget what the earlier one recorded."""
    return os.path.join(paths.state_dir("promote", create=True), ".lock")


def _name_taken(reg: dict, t: Target) -> str | None:
    """Why this target cannot have its name: a project that is still there registered it first."""
    owner = reg.get(t.name, {}).get("source")
    if owner and owner != t.source and os.path.isdir(owner):
        return f"name '{t.name}' already belongs to {owner}; set a different `name` in .pp.toml"
    return None


def _register(t: Target):
    """Record this project as the owner of its name. The read and the write are one transaction, so a target
    registered meanwhile is still there afterwards; a name another project holds is a refusal, and leaves the
    registry as it was."""
    with paths.json_txn(_registry_file(create=True), lock=_registry_lock(), indent=2) as reg:
        taken = _name_taken(reg, t)             # listed with the other blockers too; this one decides
        if taken:
            ui.die(taken, 2)
        reg[t.name] = {"source": t.source}


def _resolve(arg: str | None) -> Target:
    """A NAME argument: a registered target, or a path into a project (default: cwd)."""
    reg = _registry()
    if arg is None:
        root, data = config.find_project(None)
        if not root or "promote" not in data:
            ui.die(f"not in a promoted project: name one ({', '.join(sorted(reg)) or 'none promoted yet'})", 2)
        return _project(None)
    if arg in reg:
        src = reg[arg]["source"]
        root, data = config.find_project(src) if os.path.isdir(src) else (None, {})
        cfg, bad = {}, ""
        if root == src:
            try:
                cfg = _checked(data, root)
            except config.ConfigError as e:
                # one target's bad value must not end a verb over the registry: `status` surveys every target,
                # and `pp promote` in that project is where the refusal belongs
                bad = str(e)
        if not os.path.isdir(src):
            ui.warn(f"{arg}: its source {src} is gone; releases only, no hooks or links")
        elif bad:
            ui.warn(f"bad config: {bad}; releases only, no hooks or links")
        elif not cfg:
            ui.warn(f"{src}: no readable [promote] config; releases only, no hooks")
        return Target(arg, src, cfg)
    if os.path.isdir(paths.expand(arg)):
        return _project(arg)
    ui.die(f"no promoted target '{arg}' (known: {', '.join(sorted(reg)) or 'none'})", 2)


# ---- snapshots ------------------------------------------------------------------------------------

def _git(src: str, *args: str) -> proc.Result:
    """git read in src. A git that did not answer is never read as an answer: a run whose time ran out stops the
    command here, since its empty output would read as a clean tree with no files in it."""
    r = proc.git("-C", src, *args, group="own")
    if r.timed_out:
        ui.die(f"git {args[0]} in {src} took more than {proc.GIT_WAIT / 60:g} minutes; nothing changed", 1)
    return r


def _excluded(rel: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(rel, p) for p in patterns) or \
        any(fnmatch.fnmatch(part, p) for part in rel.split(os.sep) for p in patterns)


def _unreadable(e: OSError, src: str) -> str:
    """What pp could not read in a tree project, said the one way: the hash raises an OSError, and each caller
    adds what it means for what it was doing."""
    return f"{e.filename or src}: {e.strerror}; pp cannot read it"


def _tree_files(src: str, patterns: list[str]) -> list[str]:
    """Every file a release of this tree holds: what `_copy` copies and no more, so a fifo or a socket is no
    file of a release and opening one to hash it never returns."""
    out = []
    for dirpath, dirnames, filenames in os.walk(src):
        rel_dir = os.path.relpath(dirpath, src)
        rel_dir = "" if rel_dir == "." else rel_dir
        keep = []
        for d in dirnames:
            rel = os.path.join(rel_dir, d)
            if _excluded(rel, patterns):
                continue
            if os.path.islink(os.path.join(dirpath, d)):
                out.append(rel)          # a symlinked dir is copied as the link, never followed
            else:
                keep.append(d)
        dirnames[:] = keep
        for f in filenames:
            rel, p = os.path.join(rel_dir, f), os.path.join(dirpath, f)
            if not _excluded(rel, patterns) and (os.path.islink(p) or os.path.isfile(p)):
                out.append(rel)
    return sorted(out)


def _git_files(src: str, patterns: list[str]) -> list[str]:
    """--dirty: tracked + untracked-not-ignored, as they are on disk now."""
    out = _git(src, "ls-files", "-z", "-co", "--exclude-standard").stdout.split("\0")
    return sorted(r for r in out if r and not _excluded(r, patterns) and os.path.lexists(os.path.join(src, r)))


def _tree_hash(src: str, rels: list[str]) -> str:
    h = hashlib.sha256()
    for rel in rels:
        p = os.path.join(src, rel)
        h.update(rel.encode() + b"\0")
        if os.path.islink(p):
            h.update(b"L" + os.readlink(p).encode())
        else:
            h.update(b"X" if os.access(p, os.X_OK) else b"F")
            with open(p, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
        h.update(b"\0")
    return h.hexdigest()


def _copy(src: str, rels: list[str], dest: str):
    os.makedirs(dest, exist_ok=True)
    for rel in rels:
        s, d = os.path.join(src, rel), os.path.join(dest, rel)
        os.makedirs(os.path.dirname(d), exist_ok=True)
        if os.path.islink(s):
            os.symlink(os.readlink(s), d)
        elif os.path.isfile(s):
            shutil.copy2(s, d)       # keeps the exec bit


def _archive(src: str, dest: str):
    """git HEAD of the project's subtree, extracted with the `data` filter (no absolute or escaping paths)."""
    top = _git(src, "rev-parse", "--show-toplevel").stdout.strip()
    prefix = _git(src, "rev-parse", "--show-prefix").stdout.strip()
    os.makedirs(dest)
    c = proc.git_child("-C", top, "archive", "--format=tar", f"HEAD:{prefix}" if prefix else "HEAD")
    # pp is the reader, so pp bounds the time: a git that says nothing would hold this promote, and the
    # target's lock with it, for good. It gets proc.GIT_WAIT, as every other git pp runs does, and the
    # deadline ends it, which brings the read below back.
    over = []
    bound = threading.Timer(proc.GIT_WAIT, lambda: (over.append(True), c.end(signal.SIGKILL)))
    bound.start()
    try:
        with tarfile.open(fileobj=c.stdout, mode="r|") as tf:
            tf.extractall(dest, filter="data")
        c.stdout.read()     # to the end of the stream, so git is never left writing the last of it
        # git's output is closed first. An extraction that stopped part way leaves git blocked writing into a
        # pipe nobody reads, and reading its stderr to the end would then wait for a process that cannot get on.
        c.stdout.close()
        err = c.stderr.read().decode(errors="replace").strip()
        c.close_pipes()
        rc = c.wait(proc.GIT_WAIT)
    except BaseException:
        c.end(signal.SIGKILL)   # an extraction that raised, or a Ctrl-C: nothing pp started is left behind
        c.close_pipes()         # nothing reads what is left: this archive is ending, and the read has no bound
        c.wait()
        if over:
            raise RuntimeError(f"git archive took more than {proc.GIT_WAIT:g}s and was ended") from None
        raise
    finally:
        bound.cancel()
    if rc:
        raise RuntimeError(f"git archive failed: {ui.printable(err)}")


class Facts:
    """What a promotion would snapshot, and everything standing in its way. Computed without mutating."""

    def __init__(self, t: Target, dirty_ok: bool):
        self.blockers: list[str] = []
        self.rev = ""
        self.dirty = False
        self.rels: list[str] | None = None     # files to copy (tree mode / --dirty); None = git archive
        self.untracked: list[str] = []         # git mode: files on disk that HEAD (so the release) lacks
        if t.mode == "git":
            if not shutil.which("git"):
                self.blockers.append("git is not installed")
            elif _git(t.source, "rev-parse", "--is-inside-work-tree").returncode:
                self.blockers.append(f'{t.source} is not a git repository (git init and commit, or set source = "tree")')
            else:
                head = _git(t.source, "rev-parse", "--verify", "-q", "HEAD^{commit}")
                if head.returncode:
                    self.blockers.append("the repository has no commits yet; commit first")
                else:
                    self.rev = head.stdout.strip()
                tracked = _git(t.source, "status", "--porcelain", "--untracked-files=no", "--", ".").stdout.strip()
                if dirty_ok:
                    self.dirty = bool(_git(t.source, "status", "--porcelain", "--", ".").stdout.strip())
                    self.rels = _git_files(t.source, t.excludes())
                elif tracked:
                    n = len(tracked.splitlines())
                    self.blockers.append(f"{n} tracked file(s) modified; commit, or --dirty to snapshot the tree as-is")
                if not dirty_ok:
                    out = _git(t.source, "ls-files", "-z", "-o", "--exclude-standard", "--", ".").stdout.split("\0")
                    self.untracked = sorted(r for r in out if r)
        elif t.mode == "tree":
            self.rels = _tree_files(t.source, t.excludes())
            try:
                self.rev = _tree_hash(t.source, self.rels)
            except OSError as e:
                # a file pp may not read is a blocker, listed with the others, and no release is made from it
                self.blockers.append(f"{_unreadable(e, t.source)}, so it cannot be in the release: add it to "
                                     f"`exclude`. pp stopped at this file, so another one it cannot read is "
                                     f"named by a later run")
        else:
            self.blockers.append(f'unknown source = "{t.mode}" (git or tree)')

    def untracked_note(self) -> str:
        if not self.untracked:
            return ""
        # the names come from git, so an escape sequence in one cannot redraw the terminal around the note
        shown = ui.printable(", ".join(self.untracked[:3])) \
            + (f" (+{len(self.untracked) - 3} more)" if len(self.untracked) > 3 else "")
        return (f"note: {len(self.untracked)} untracked file(s) are not in HEAD, so not in the release: {shown}"
                f"; git add + commit them, or --dirty")

    def stamp(self, t: Target) -> str:
        short = self.rev[:10] if t.mode == "tree" else self.rev[:7]
        base = f"{datetime.now():%Y%m%d-%H%M%S}-{short or 'norev'}" + ("-dirty" if self.dirty else "")
        stamp, n = base, 1
        while os.path.lexists(t.dir(stamp)) or os.path.lexists(os.path.join(t.releases, ".staging-" + stamp)):
            n += 1
            stamp = f"{base}-{n}"
        return stamp


# ---- links ----------------------------------------------------------------------------------------

def _inside(path: str, root: str) -> bool:
    for a, b in ((os.path.normpath(path), os.path.normpath(root)), (os.path.realpath(path), os.path.realpath(root))):
        if a == b or a.startswith(b.rstrip(os.sep) + os.sep):
            return True
    return False


def _link_plan(t: Target, force: bool) -> list[dict]:
    """Every link with its action: ok (already right), create, replace (pp's own, or forced), conflict."""
    plan = []
    rel_root = paths.data_dir("releases", create=False)
    for kind, base in (("link", os.path.join(t.releases, "current")), ("dev_link", t.source)):
        for path, rel in (t.cfg.get(kind + "s") or {}).items():
            path, target = paths.expand(path), os.path.join(base, rel)
            row = {"kind": kind, "path": path, "rel": rel, "target": target, "action": "create", "why": "", "foreign": False}
            if os.path.islink(path):
                have = os.readlink(path)
                abs_have = os.path.join(os.path.dirname(path), have)
                if have == target:
                    row["action"] = "ok"
                elif _inside(abs_have, rel_root) or _inside(abs_have, t.source):
                    row.update(action="replace", why=f"was -> {have}")
                else:
                    row.update(action="replace" if force else "conflict", why=f"a symlink to {have}, not pp's", foreign=True)
            elif os.path.isdir(path):
                row.update(action="conflict", why="a directory (never replaced, even with --force-links)")
            elif os.path.lexists(path):
                row.update(action="replace" if force else "conflict", why="a regular file", foreign=True)
            if not os.path.lexists(os.path.join(t.source, rel)) and row["action"] != "conflict":
                # a note here; cmd_run makes it a blocker when there is no build (_in_snapshot), and the release
                # is checked again after its build
                row["why"] = (row["why"] + "; " if row["why"] else "") + f"{rel} not in the source tree"
            plan.append(row)
    return plan


def _link_blockers(plan: list[dict]) -> list[str]:
    return [f"{l['path']} is {l['why']}" + ("; pass --force-links to replace it" if l["foreign"] else "")
            for l in plan if l["action"] == "conflict"]


def _apply_links(plan: list[dict], say=print):
    for l in plan:
        if l["action"] not in ("create", "replace"):
            continue
        os.makedirs(os.path.dirname(l["path"]), exist_ok=True)
        # a staging name of this writer's own, so no other applier (a promote, a `pp promote link`) can remove
        # it between the symlink and the rename and leave this one with nothing to rename
        tmp = paths.staging(os.path.dirname(l["path"]), "link")
        os.symlink(l["target"], tmp)
        os.replace(tmp, l["path"])
        say(f"  {l['kind']} {l['path']} -> {l['target']}")


def _confirm_foreign(plan: list[dict], yes: bool) -> bool:
    foreign = [l for l in plan if l["foreign"] and l["action"] == "replace"]
    if not foreign:
        return True
    for l in foreign:
        print(f"  will replace {l['path']} ({l['why']})")
    return ui.confirm("replace these with pp links?", yes)


# ---- the run log, the flip ------------------------------------------------------------------------

class Log:
    """Everything a run prints, also appended to its own log file: evidence that outlives the caller's pipe."""

    def __init__(self, path: str | None):
        self.path = path
        self.f = open(path, "a") if path else None

    def __enter__(self) -> "Log":
        return self

    def __exit__(self, *exc) -> None:
        if self.f:
            self.f.close()
            self.f = None

    def say(self, msg: str = ""):
        print(msg, flush=True)
        if self.f:
            self.f.write(msg + "\n")
            self.f.flush()

    def sh(self, label: str, cmd: str, cwd: str, env: dict) -> int:
        """Run the check, printing its output as it comes. In pp's own group, so a Ctrl-C in this terminal reaches
        the check as well: that is how a person stops a promotion."""
        self.say(f"--- {label}: {cmd}")
        c = proc.spawn(["/bin/sh", "-c", cmd], group="shared", stdin="none", env=env, cwd=cwd, stdout=proc.PIPE,
                       stderr=proc.STDOUT)
        for raw in iter(c.stdout.readline, b""):
            line = raw.decode(errors="replace")
            sys.stdout.write(line)
            sys.stdout.flush()
            if self.f:
                self.f.write(line)
        c.close_pipes()
        rc = c.wait()
        self.say(f"--- {label}: {'ok' if rc == 0 else f'FAILED (exit {rc})'}")
        return rc


def _flip(t: Target, stamp: str):
    """`current` -> stamp, atomically: a reader sees the old release or the new one, never neither."""
    tmp = paths.staging(t.releases, "current")
    os.symlink(stamp, tmp)
    os.replace(tmp, os.path.join(t.releases, "current"))


def _revlog(t: Target, line: str):
    with open(os.path.join(t.releases, "revisions.log"), "a") as f:
        f.write(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {line}\n")


@contextmanager
def _lock(t: Target):
    """A target's own lock: one promote, rollback, link or gc of it at a time. A second one refuses rather than
    waits, since a promotion runs a build and a gate and can take minutes. Ctrl-C reaches those, so the lock
    does not defer signals; what an interrupted promotion leaves is read from disk by the next one."""
    os.makedirs(t.releases, exist_ok=True)
    try:
        with lock.locked(os.path.join(t.releases, ".lock"), block=False):
            yield
    except lock.Busy:           # nothing inside asks for a lock without waiting, so it is this one
        ui.die(f"another promote/rollback of {t.name} is running", 1)


def _in_snapshot(t: Target, f: Facts, rel: str) -> bool:
    """Whether the release will hold rel: HEAD's tree in git mode, else what is on disk now."""
    if t.mode == "git" and not f.dirty:
        return _git(t.source, "cat-file", "-e", f"HEAD:./{rel}").returncode == 0
    return os.path.lexists(os.path.join(t.source, rel))


def _gc_victims(t: Target, keep: int, also: str | None = None) -> list[str]:
    """Releases to remove: the keep newest stay, and so do `current`, the one before it, and `also`. A promote
    passes the release it has just flipped away from, which is the one its user was running until that flip and
    the one they read as "the one before"; it is not the creation-order predecessor after a rollback."""
    stamps = t.stamps()
    cur = t.current()
    spare = set(stamps[-max(keep, 0):] if keep > 0 else []) | {cur, t.previous(cur), also}
    return [s for s in stamps if s not in spare]


def _gc(t: Target, gone: list[str], say=print) -> None:
    """Remove these releases (under the target's lock; current and the one before it are never in the list)."""
    for s in gone:
        shutil.rmtree(t.dir(s), ignore_errors=True)
    if gone:
        say(f"gc: removed {len(gone)} old release(s): {', '.join(gone)}")


# ---- verbs ----------------------------------------------------------------------------------------

def cmd_run(argv: list[str]) -> int:
    ap = ui.parser("promote", __doc__)
    ap.add_argument("path", nargs="?", help="a path in the project (default: cwd)")
    ap.add_argument("-n", "--dry-run", action="store_true", help="preflight: list every blocker and the plan, change nothing")
    ap.add_argument("--dirty", action="store_true", help="git mode: snapshot the working tree as it is")
    ap.add_argument("--no-check", action="store_true", help="skip the gate (recorded in the log)")
    ap.add_argument("--again", action="store_true", help="promote even if HEAD is already current")
    ap.add_argument("--keep-failed", action="store_true", help="leave a failed staging dir for inspection")
    ap.add_argument("--force-links", action="store_true", help="replace regular files / foreign symlinks at link paths")
    ap.add_argument("-y", "--yes", action="store_true", help="do not ask")
    a = ap.parse_args(argv)

    t = _project(a.path)
    f = Facts(t, a.dirty)
    plan = _link_plan(t, a.force_links)
    blockers = f.blockers + _link_blockers(plan)
    if not t.cfg.get("build"):       # with a build, the release may make it; without one it is what is snapshot
        blockers += [f"{l['rel']} is a link target but the release would not have it: commit it, or add a `build`"
                     for l in plan if l["kind"] == "link" and not _in_snapshot(t, f, l["rel"])]
    taken = _name_taken(_registry(), t)
    if taken:
        blockers.append(taken)
    cur = t.current()
    cur_info = t.info(cur)
    already = bool(f.rev) and cur_info.get("rev") == f.rev and not cur_info.get("dirty") and not f.dirty

    if a.dry_run:
        _print_plan(t, f, plan, cur, a, already)
        if f.untracked_note():
            print("\n" + f.untracked_note())
        if blockers:
            print(ui.c(f"\nblocked ({len(blockers)}):", "red"))
            for b in blockers:
                print(f"  - {b}")
            return 2
        print(ui.c("\npreflight: clear" + (" (already current; --again to re-release)" if already and not a.again else ""), "green"))
        return 0
    if blockers:
        ui.warn(f"promote {t.name}: REFUSED")
        for b in blockers:
            print(f"  - {b}", file=sys.stderr)
        return 2
    # both paths apply the same link plan, so both ask about the same files first: a run that found the
    # release already current replaces nothing of the user's without the answer a promotion needs
    if not _confirm_foreign(plan, a.yes):
        return 2
    if already and not a.again:
        with _lock(t):          # a promote applies the same links: one applier at a time
            _apply_links(plan)
        print(f"{t.name}: already current ({cur}, rev {f.rev[:12]}). --again re-releases it")
        if f.untracked_note():
            print(f.untracked_note())
        return 0

    _register(t)                # the registry first, then this target's own lock: that is the order
    with _lock(t), ExitStack() as scope:
        # `current` again, under the lock: PP_PREV, the revisions.log line and the rollback target all come from
        # it, and a promote that finished while this one was asking is the release it really flips away from
        cur = t.current()
        stamp = f.stamp(t)
        log = scope.enter_context(Log(os.path.join(paths.state_dir("promote", "logs"), f"{t.name}-{stamp}.log")))
        log.say(f"promote {t.name}: {t.source} ({t.mode}) rev {f.rev[:12]}{' DIRTY' if f.dirty else ''} -> {stamp}")
        log.say(f"log: {log.path}")
        if f.untracked_note():
            log.say(f.untracked_note())
        staging = os.path.join(t.releases, ".staging-" + stamp)

        def abandon(why: str) -> int:
            if a.keep_failed:
                log.say(f"kept the failed staging dir: {staging}")
            else:
                shutil.rmtree(staging, ignore_errors=True)
            log.say(ui.c(f"promote {t.name}: NOT PROMOTED: {why}; current stays {cur or '(none)'}", "red"))
            log.say(f"full output: {log.path}")
            return 1

        try:
            return _release(t, f, a, plan, log, stamp, staging, cur, abandon)
        except KeyboardInterrupt:
            if os.path.isdir(staging):
                abandon("interrupted")
            else:
                log.say(ui.c(f"promote {t.name}: interrupted after the flip; current is {t.current() or '(none)'} "
                             f"and `after`/`verify` may not have finished. `pp promote rollback {t.name}` goes "
                             f"back", "red"))
            return 130


def _release(t: Target, f: Facts, a, plan: list[dict], log: Log, stamp: str, staging: str, cur: str | None,
             abandon) -> int:
    """Snapshot, build, check, flip, activate: the part of a promotion that changes things."""
    final, prev_dir = t.dir(stamp), t.dir(cur)
    try:
        if f.rels is None:
            _archive(t.source, staging)
        else:
            _copy(t.source, f.rels, staging)
        with open(os.path.join(staging, META), "w") as fh:
            json.dump({"name": t.name, "rev": f.rev, "source": t.source, "mode": t.mode, "dirty": f.dirty,
                       "checked": bool(t.cfg.get("check")) and not a.no_check, "created": time.time(), "created_iso": datetime.now().isoformat(timespec="seconds")}, fh, indent=2)
    except (OSError, RuntimeError, tarfile.TarError) as e:
        return abandon(f"snapshot failed: {e}")
    env = t.env(f.rev, staging, prev_dir)
    if t.cfg.get("build") and log.sh("build", t.cfg["build"], staging, env):
        return abandon("build failed")
    missing = [l["rel"] for l in plan if l["kind"] == "link" and not os.path.lexists(os.path.join(staging, l["rel"]))]
    if missing:
        return abandon(f"link target(s) not in the release: {', '.join(missing)} (committed? built?)")
    if a.no_check:
        log.say("--- check: SKIPPED (--no-check)")
    elif t.cfg.get("check"):
        if log.sh("check", t.cfg["check"], staging, dict(env, PYTHONDONTWRITEBYTECODE="1")):
            return abandon("check failed")
    else:
        log.say("--- check: none configured; nothing gates this release")

    os.rename(staging, final)
    _flip(t, stamp)
    _revlog(t, f"promote {cur or '-'} -> {stamp} rev={f.rev}" + (" dirty" if f.dirty else ""))
    log.say(f"current -> {stamp}" + (f" (was {cur})" if cur else ""))
    try:
        _apply_links(plan, log.say)
        env = t.env(f.rev, final, prev_dir)
        failed = next((h for h in ("after", "verify") if t.cfg.get(h) and log.sh(h, t.cfg[h], final, env)), None)
    except OSError as e:
        # past the flip, so there is no abandoning it: say what is live and what to run, never a traceback
        log.say(ui.c(f"promote {t.name}: current is {stamp} but its links or hooks did not finish: {e}", "red"))
        log.say(ui.c(f"`pp promote link {t.name}` reasserts the links; `pp promote rollback {t.name}` goes back "
                     f"to {cur or '(nothing: `current` would be removed)'}", "red"))
        log.say(f"full output: {log.path}")
        return 1
    if failed:
        _auto_rollback(t, stamp, cur, failed, log)
        return 1
    _gc(t, _gc_victims(t, int(t.cfg.get("keep", DEFAULT_KEEP)), also=cur), log.say)
    log.say(ui.c(f"promoted {t.name} -> {stamp}", "green"))
    return 0


def _auto_rollback(t: Target, new: str, prev: str | None, hook: str, log: Log):
    _mark_failed(t, new, hook)
    if prev:
        _flip(t, prev)
        _revlog(t, f"rollback {new} -> {prev} (auto: {hook} failed)")
        log.say(ui.c(f"{hook} failed: rolled back current -> {prev}", "red"))
        if t.cfg.get("after") and log.sh("after (rollback)", t.cfg["after"], t.dir(prev),
                                         t.env(t.info(prev).get("rev", ""), t.dir(prev), t.dir(new))):
            log.say(ui.c(f"`after` failed for {prev} too: what is running may be neither release. Fix the cause, "
                         f"then `pp promote` again", "red"))
    else:
        os.remove(os.path.join(t.releases, "current"))
        _revlog(t, f"rollback {new} -> - (auto: {hook} failed; nothing before it)")
        log.say(ui.c(f"{hook} failed and there is no earlier release: `current` removed", "red"))
    log.say(f"the failed release stays on disk for inspection (rollback skips it): {t.dir(new)}")
    log.say(f"full output: {log.path}")


def _mark_failed(t: Target, stamp: str, hook: str):
    """Record in the release's own metadata that activating it failed, so `rollback` never lands on it."""
    info = dict(t.info(stamp), failed=hook)
    paths.write_json(os.path.join(t.dir(stamp), META), info, indent=2)


def _print_plan(t: Target, f: Facts, plan: list[dict], cur: str | None, a, already: bool):
    keep = int(t.cfg.get("keep", DEFAULT_KEEP))
    rev = (f.rev[:12] or "?") + (" (dirty)" if f.dirty else "")
    check = "SKIPPED (--no-check)" if a.no_check else (t.cfg.get("check") or "- (nothing gates this release)")
    rows = [["target", f"{t.name}  ({t.mode} from {t.source})"],
            ["rev", rev + ("  = current" if already else "")],
            ["current", cur or "(none)"],
            ["new", f.stamp(t) if f.rev else "?"],
            ["releases", t.releases],
            ["build", t.cfg.get("build") or "-"], ["check", check],
            ["after", t.cfg.get("after") or "-"], ["verify", t.cfg.get("verify") or "-"], ["keep", keep]]
    print(ui.table(rows, indent="  "))
    for l in plan:
        print(f"  {l['kind']:<9} {l['path']} -> {l['target']}  [{l['action']}{': ' + l['why'] if l['why'] else ''}]")


def _status_row(name: str, src: str) -> dict:
    t = _resolve(name)
    cur = t.current()
    info = t.info(cur)
    row = {"name": name, "source": src, "mode": t.mode, "current": cur, "rev": info.get("rev"),
           "created": info.get("created_iso"), "release_dirty": info.get("dirty", False),
           "checked": info.get("checked"), "head": None, "ahead": None, "dirty": None, "links_ok": True,
           "link_problems": []}
    if t.mode == "git" and os.path.isdir(src):
        head = _git(src, "rev-parse", "--verify", "-q", "HEAD").stdout.strip()
        row["head"] = head or None
        if head and info.get("rev"):
            n = _git(src, "rev-list", "--count", f"{info['rev']}..HEAD", "--", ".")
            row["ahead"] = int(n.stdout) if n.returncode == 0 else None
        row["dirty"] = bool(_git(src, "status", "--porcelain", "--untracked-files=no", "--", ".").stdout.strip())
    elif t.mode == "tree" and os.path.isdir(src):
        try:
            row["head"] = _tree_hash(src, _tree_files(src, t.excludes()))
        except OSError as e:            # this target's drift is unknown; the other targets' rows still print
            ui.warn(f"{name}: {_unreadable(e, src)}, so the source is not compared")
        else:
            row["ahead"] = 0 if row["head"] == info.get("rev") else None
            row["dirty"] = row["head"] != info.get("rev")
    cur_dir = os.path.join(t.releases, "current")
    for kind, base in (("links", cur_dir), ("dev_links", src)):
        for path, rel in (t.cfg.get(kind) or {}).items():
            p = paths.expand(path)
            if os.path.exists(p) and _inside(os.path.realpath(p), os.path.realpath(base)):
                continue
            if not os.path.lexists(p):
                problem = "missing"
            elif os.path.islink(p) and any(_inside(os.path.realpath(p), os.path.realpath(d)) for d in
                                           (paths.data_dir("releases", create=False), src)):
                problem = "stale"           # pp's own link, to something other than it should (`pp promote link`)
            else:
                problem = "conflict"        # a file or a link that is not pp's (preflight says the same)
            row["link_problems"].append({"path": p, "kind": kind[:-1], "problem": problem})
    row["links_ok"] = not row["link_problems"]
    return row


def cmd_status(argv: list[str]) -> int:
    ap = ui.parser("promote status", "every promoted target: current release, source drift, links")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    unusable: list[str] = []
    rows = [_status_row(n, r["source"]) for n, r in sorted(_registry(unusable).items())]
    if unusable:
        # what is wrong with the name, and where the releases of such a name went: a pp that built the path
        # without checking it wrote them wherever the name led, which for `.` and `..` is a directory pp uses
        # for every target. So the line names that path and what of it is this target's
        root = paths.data_dir("releases", create=False)
        where = ", ".join(sorted({os.path.normpath(os.path.join(root, n)) for n in unusable}))
        ui.warn(f"{len(unusable)} name(s) in {_registry_file()} are not one path component: "
                f"{', '.join(unusable)}. pp builds no releases path from such a name, so no verb acts on it: "
                f"remove its entry from that file. A pp that made releases for it put them in {where}, a "
                f"directory pp uses for other targets as well: what is this target's there is its release "
                f"directories, its `current` and its `revisions.log`")
    if a.json:
        ui.print_json(rows)
        return 0
    if not rows:
        print("nothing promoted yet: `pp promote` in a project with a [promote] table")
        return 0
    table = []
    for r in rows:
        ahead = "-" if r["ahead"] is None else (f"+{r['ahead']}" if r["ahead"] else "0")
        if r["mode"] == "tree":
            # `head` is None in exactly the cases pp did not read the source, and `-` is what the git rows
            # print for a drift that is not known
            ahead = "-" if r["head"] is None else ("same" if r["ahead"] == 0 else "differs")
        table.append([r["name"], r["current"] or "(none)", (r["rev"] or "")[:10] + ("*" if r["release_dirty"] else ""),
                      ahead, "dirty" if r["dirty"] else "", "ok" if r["links_ok"] else " ".join(f"{x['problem']} {x['path']}" for x in r["link_problems"]),
                      r["source"] + ("" if os.path.isdir(r["source"]) else "  (gone)")])
    print(ui.table(table, ["NAME", "CURRENT", "REV", "AHEAD", "TREE", "LINKS", "SOURCE"]))
    return 0


def cmd_ls(argv: list[str]) -> int:
    ap = ui.parser("promote ls", "releases of a target, newest first (* = current)")
    ap.add_argument("name", nargs="?")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    t = _resolve(a.name)
    cur = t.current()
    rows = [dict(t.info(s), stamp=s, current=(s == cur)) for s in reversed(t.stamps())]
    if a.json:
        ui.print_json(rows)
        return 0
    print(ui.table([["*" if r["current"] else "", r["stamp"], (r.get("rev") or "")[:12],
                     " ".join(w for w in ("dirty" if r.get("dirty") else "",
                                          "unchecked" if r.get("checked") is False else "",
                                          f"failed ({r['failed']})" if r.get("failed") else "") if w),
                     ui.human_age(r.get("created"))] for r in rows], ["", "STAMP", "REV", "", "CREATED"]))
    return 0


def cmd_rollback(argv: list[str]) -> int:
    ap = ui.parser("promote rollback", "flip `current` back to the previous release (or --to one), rerun `after`")
    ap.add_argument("name", nargs="?")
    ap.add_argument("--to", help="a stamp (or unique prefix of one)")
    ap.add_argument("-y", "--yes", action="store_true")
    a = ap.parse_args(argv)
    t = _resolve(a.name)
    cur, stamps = t.current(), t.stamps()
    if not cur:
        ui.die(f"{t.name}: nothing promoted", 1)
    if a.to:
        hits = [s for s in stamps if s == a.to] or [s for s in stamps if s.startswith(a.to)]
        if len(hits) != 1:
            ui.die(f"--to {a.to}: {'ambiguous: ' + ', '.join(hits) if hits else 'no such release'}", 2)
        target = hits[0]
        if target == cur:
            ui.die(f"{target} is already current", 2)
    else:
        target = t.previous(cur)
        if not target:
            ui.die(f"{t.name}: no release older than {cur} kept", 1)
    if not ui.confirm(f"roll {t.name} back {cur} -> {target}?", a.yes):
        return 2
    with _lock(t), ExitStack() as scope:
        now = t.current()
        if now != cur:       # a promote finished while this was asked: it would be undone without being seen
            ui.die(f"{t.name}: current is {now} now, not the {cur} this asked about; nothing was rolled back. "
                   f"`pp promote rollback {t.name}` asks again", 2)
        log = scope.enter_context(Log(os.path.join(paths.state_dir("promote", "logs"),
                                                   f"{t.name}-{datetime.now():%Y%m%d-%H%M%S}-rollback.log")))
        _flip(t, target)
        _revlog(t, f"rollback {cur} -> {target} (manual)")
        log.say(f"{t.name}: current {cur} -> {target}")
        rc = 0
        if t.cfg.get("after"):
            rc = log.sh("after", t.cfg["after"], t.dir(target),
                        t.env(t.info(target).get("rev", ""), t.dir(target), t.dir(cur)))
        log.say(f"the source is unchanged: the next `pp promote` releases it again; undo this: "
                f"pp promote rollback {t.name} --to {cur}")
    return 1 if rc else 0


def cmd_diff(argv: list[str]) -> int:
    ap = ui.parser("promote diff", "what the source has that the current release does not")
    ap.add_argument("name", nargs="?")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    t = _resolve(a.name)
    cur = t.current()
    if not cur:
        ui.die(f"{t.name}: nothing promoted", 1)
    rev = t.info(cur).get("rev", "")
    if t.mode == "git":
        got = {}
        for key, args in (("commits", ("log", "--oneline", f"{rev}..HEAD", "--", ".")),
                          ("files", ("diff", "--stat", rev, "--", "."))):
            out = _git(t.source, *args)
            if out.returncode:
                ui.die(f"promote diff: git {args[0]}: {ui.printable(out.stderr.strip())}", 1)
            got[key] = out.stdout.rstrip().splitlines()
    else:
        excl = [x for p in t.excludes() + [META] for x in ("-x", p)]
        got = {"files": proc.run(["diff", "-rq", *excl, t.dir(cur), t.source], group="own", stdin="none",
                                 env="inherit").stdout.rstrip().splitlines()}
    if a.json:
        ui.print_json({"name": t.name, "current": cur, "rev": rev, **got})
        return 0
    print(f"{t.name}: current {cur}" + (f", rev {rev[:12]}" if rev else ""))
    if not any(got.values()):
        print("nothing: the source is level with the release")
    for key, lines in got.items():
        if lines:
            print(f"{key}:")
            # commit subjects and file names, from git: an escape sequence in one would redraw the terminal
            print("\n".join("  " + ui.printable(ln) for ln in lines))
    return 0


def cmd_link(argv: list[str]) -> int:
    ap = ui.parser("promote link", "reassert a target's links and dev_links")
    ap.add_argument("name", nargs="?")
    ap.add_argument("--force-links", action="store_true")
    ap.add_argument("-y", "--yes", action="store_true")
    a = ap.parse_args(argv)
    t = _resolve(a.name)
    plan = _link_plan(t, a.force_links)
    blockers = _link_blockers(plan)
    if blockers:
        for b in blockers:
            print(f"  - {b}", file=sys.stderr)
        return 2
    if not _confirm_foreign(plan, a.yes):
        return 2
    with _lock(t):           # a promote applies the same links: one at a time, or they race for the same paths
        _apply_links(plan)
    print(f"{t.name}: {len(plan)} link(s) in place")
    return 0


def cmd_gc(argv: list[str]) -> int:
    ap = ui.parser("promote gc", "prune old releases (current and the one before it always stay)")
    ap.add_argument("name", nargs="?")
    ap.add_argument("--keep", type=int, default=None)
    ap.add_argument("-n", "--dry-run", action="store_true", help="only show what it would remove")
    ap.add_argument("-y", "--yes", action="store_true", help="do not ask")
    a = ap.parse_args(argv)
    t = _resolve(a.name)
    keep = a.keep if a.keep is not None else int(t.cfg.get("keep", DEFAULT_KEEP))
    gone = _gc_victims(t, keep)
    if not gone:
        print(f"gc: nothing to remove ({len(t.stamps())} release(s); current and the one before it always stay)")
        return 0
    print(f"gc: {len(gone)} old release(s) of {t.name} to remove: {', '.join(gone)}")
    if a.dry_run:
        return 0
    if not ui.confirm("remove them?", a.yes):
        return 2
    with _lock(t):
        _gc(t, [s for s in gone if s in _gc_victims(t, keep)])  # what was shown, if still old under the lock
    return 0


STARTER = """[promote]
source = "{source}"          # git: HEAD (commit first; --dirty for the tree as-is) | tree: copy the working tree
# exclude = ["data", "*.log"]
# build = "make"
# check = "python3 -m unittest discover -s tests -q"     # runs IN the release: the exact bytes being promoted
# links = {{ "~/.local/bin/{name}" = "bin/{name}" }}       # -> releases/{name}/current/bin/{name}
# dev_links = {{ "~/.local/bin/{name}d" = "bin/{name}" }}  # -> this working tree
# after = "systemctl --user restart {name}"
# verify = "curl -fsS http://127.0.0.1:PORT/ >/dev/null"  # non-zero rolls back automatically
keep = 5
"""


def cmd_init(argv: list[str]) -> int:
    ap = ui.parser("promote init", "write a starter [promote] table into PATH/.pp.toml")
    ap.add_argument("path", nargs="?", default=".")
    a = ap.parse_args(argv)
    root = paths.expand(a.path)
    f = os.path.join(root, config.PROJECT_FILE)
    have = ""
    if os.path.exists(f):
        with open(f) as fh:
            have = fh.read()
        found, data = config.find_project(root)
        if found == root and "promote" in data:
            ui.die(f"{f} already has a [promote] table", 2)
    name = config.project_name(root) if have else os.path.basename(root)
    source = "git" if config.git_root(root) else "tree"
    head = (f'# pp project config. `pp promote -n` previews, `pp promote` releases.\n# name = "{name}"\n\n'
            if not have else "" if have.endswith("\n\n") else "\n" if have.endswith("\n") else "\n\n")
    # in one step: what the file had, the gap and the new table land together, or the file stays as it was
    paths.write_text(f, have + head + STARTER.format(source=source, name=name))
    print(f"wrote [promote] to {f}; edit it, then `pp promote -n`")
    return 0


def main(argv: list[str]) -> int:
    if argv and argv[0] in VERBS:
        return globals()["cmd_" + argv[0]](argv[1:])
    return cmd_run(argv)
