"""Write commands, or add others' from a git repository

    pp cmd new NAME                 pp/commands/NAME.py in this pp's source tree, with tests/test_NAME.py and
                                    tests/test_journey_NAME.py (its help examples, run through the real CLI);
                                    run from a promoted release, that is the tree it was released from
    pp cmd new NAME --user          ~/.config/pp/commands/NAME.py, on this machine only
    pp cmd new NAME --user -s "One-line summary"
    pp cmd add github.com/OWNER/REPO      clone it, pin it to one commit, show what it adds, then ask
    pp cmd add URL -n --show        only show: its commands, and their code
    pp cmd add URL --ref v1.2 --name tools
    pp cmd update                   every plugin: fetch, show the diff, ask, then move the pin
    pp cmd update tools -n          only show what would change
    pp cmd rm tools                 remove a plugin and its commands
    pp cmd ls                       every command and where it comes from (--json)

A new command has a docstring whose first line is its summary, a main(argv), and only the standard library at
the top of the file (the rest: https://github.com/xflash96/pp/blob/main/CONTRIBUTING.md).

A plugin is a git repository of such command files: the *.py files in its commands/ folder, or at its top when
it has no commands/ folder. Each plugin is a clone in ~/.local/share/pp/plugins/NAME, checked out at the commit
you said yes to; only `pp cmd update` moves it. A plugin's command never hides a built-in or one of yours: add
and update refuse a name that is taken.

A plugin's code runs as you, with your files and keys. Read what add and update show before you say yes.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
import time

from pp import cli
from pp.lib import lock, paths, proc, ui

TEMPLATE = '''"""{summary}

    pp {name} ARG          what it does
    pp {name} -n ARG       show what it would do
"""
from __future__ import annotations

from pp.lib import ui


def main(argv: list[str]) -> int:
    ap = ui.parser("{name}", __doc__)
    ap.add_argument("arg")
    ap.add_argument("-n", "--dry-run", action="store_true", help="show the plan only")
    ap.add_argument("-y", "--yes", action="store_true", help="do not ask")
    a = ap.parse_args(argv)
    if a.dry_run:
        print(f"would do: {{a.arg}}")
        return 0
    print(a.arg)
    return 0
'''

TEST_TEMPLATE = '''"""Unit tests for `pp {name}`, in tests/sandbox.py's scope (CONTRIBUTING.md, Tests)."""
import io
import os
import sys
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sandbox  # noqa: E402
from pp.commands import {name}  # noqa: E402


class Test{cls}(unittest.TestCase):
    def setUp(self):
        # the scope owns the PP_* directories, HOME, the cwd and the session identity, so this test reads and
        # writes a tree of its own and never the machine's. First in setUp, so it closes last
        self.sb = sandbox.enter(self)

    def test_runs(self):
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual({name}.main(["hello"]), 0)
        self.assertIn("hello", out.getvalue())


if __name__ == "__main__":
    unittest.main()
'''

JOURNEY_TEMPLATE = '''"""Journeys through `pp {name}`: every example in its help, run the way a person would (tests/journey.py).

A step that uses a command other than this one is guarded with `needs` or `has`, imported from journey the
way `Journey` is, so a tree with that command removed still passes. A step that needs a state waits for it
with `self.until`.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from journey import Journey  # noqa: E402


class Test{cls}Journey(Journey):
    def test_the_help_examples(self):
        self.assertIn("usage: pp {name}", self.pp("{name}", "-h").stdout)
        self.assertIn("required: arg", self.pp("{name}", code=2).stderr)
        thing = self.write("work/thing.txt", "hello\\n")     # a file in the journey's own directory
        self.assertEqual(self.pp("{name}", "thing.txt").stdout, "thing.txt\\n")
        self.assertEqual(self.pp("{name}", "-n", "thing.txt").stdout, "would do: thing.txt\\n")
        self.assertEqual(self.read(thing), "hello\\n")       # `-n` showed the plan and changed nothing


if __name__ == "__main__":
    unittest.main()
'''


# ---- new ------------------------------------------------------------------------------------------

def cmd_new(argv: list[str]) -> int:
    ap = ui.parser("cmd new", "Write a new command, with its tests.")
    ap.add_argument("name")
    ap.add_argument("-s", "--summary", default="TODO: one-line summary")
    ap.add_argument("--user", action="store_true", help="write to ~/.config/pp/commands (machine-local)")
    ap.add_argument("--no-test", action="store_true")
    a = ap.parse_args(argv)
    if not re.fullmatch(r"[a-z][a-z0-9_]*", a.name):
        ui.die("a command name is lowercase letters, digits and _ (it is a Python module name)", 2)
    taken = _taken(a.name)
    if taken:
        ui.die(f"'{a.name}' is taken: {taken}; pick another name", 2)
    prog = os.path.basename(sys.argv[0]) or "pp"
    if a.user:
        d = os.path.join(paths.config_dir(), "commands")
        test_dir = None
    else:
        root = _source_tree()
        if root != paths.pp_root():
            prog = "ppd"
        d = os.path.join(root, "pp", "commands")
        test_dir = None if a.no_test else os.path.join(root, "tests")
    path = os.path.join(d, a.name + ".py")
    os.makedirs(d, exist_ok=True)
    try:
        with open(path, "x") as f:      # "x": the file is this writer's own, or there was one already
            f.write(TEMPLATE.format(name=a.name, summary=a.summary))
    except FileExistsError:
        ui.die(f"{path} exists", 2)
    print(f"wrote {path}")
    if test_dir and os.path.isdir(test_dir):
        cls = "".join(w.capitalize() for w in a.name.split("_"))
        for tmpl, fname in ((TEST_TEMPLATE, f"test_{a.name}.py"), (JOURNEY_TEMPLATE, f"test_journey_{a.name}.py")):
            tpath = os.path.join(test_dir, fname)
            # each template is written only where what it imports is: a tests/ without them is a published
            # subset or a contributor's own, and a test that cannot import is worse than no test
            needed = "journey.py" if tmpl is JOURNEY_TEMPLATE else "sandbox.py"
            if not os.path.exists(os.path.join(test_dir, needed)):
                continue
            try:
                with open(tpath, "x") as f:
                    f.write(tmpl.format(name=a.name, cls=cls))
            except FileExistsError:     # a test file of that name is the user's: never overwritten
                continue
            print(f"wrote {tpath}")
    print(f"try: {prog} {a.name} -h" + ("   (`pp` gets it at the next `pp promote` of that tree)" if prog == "ppd" else ""))
    return 0


def _taken(name: str, cmds: dict | None = None) -> str | None:
    """Why NAME cannot be a new command: an existing one would hide it, or it would hide an existing one. The
    answer is printed, and a path or a name in it may be someone else's, so it comes back escaped."""
    if name == "help":
        return "`pp help CMD` is pp's own"
    cmds = cli.discover() if cmds is None else cmds
    if name in cmds:
        c = cmds[name]
        return f"{c.origin} command {ui.printable(c.path)}"
    owner = next((c for c in cmds.values() if name in c.aliases), None)
    return f"an alias of '{ui.printable(owner.name)}' ({ui.printable(owner.path)})" if owner else None


def _source_tree() -> str:
    """The tree to add a built-in to: this pp's own, unless this pp is a promoted release (a snapshot replaced at
    the next promote), in which case the source it was released from. An installed package has no tree to add to."""
    root = paths.pp_root()
    if not os.path.isfile(os.path.join(root, "bin", "pp")):
        ui.die(f"this pp is an installed package ({root}); a command written into it would vanish at the next "
               "upgrade. `pp cmd new NAME --user` writes one of your own", 2)
    meta = paths.release_meta(root)
    if meta is None:
        return root
    src = meta.get("source")
    if not src or not os.path.isdir(os.path.join(src, "pp", "commands")):
        ui.die(f"this pp is a release ({root}) and its source tree ({src}) is gone; a command written here would "
               "vanish at the next promote. `pp cmd new NAME --user` writes a machine-local one", 2)
    return src


# ---- plugins --------------------------------------------------------------------------------------
#
# ~/.local/share/pp/plugins/NAME/ is a clone, detached at the approved commit; cli.discover reads its commands
# (the plugin origin, after builtin and user). .index.json beside the clones records {NAME: {url, ref, commit,
# added, updated}}: where it came from and which ref `update` follows (null: the remote's default branch). Its
# leading dot keeps it out of discovery and out of the names a plugin can have, and so does .lock's.
#
# What a plugin brings is read, never trusted to print: its commit subject, its file names, its summaries and
# the URL it came from all go through ui.printable, so the screen a person says yes to is pp's own lines and
# nothing else can write one. git's output reaches pp with \r read as a newline, which is a line of its own.
#
# add, update and rm each hold the plugins directory's lock (_holding) for the part that must not interleave
# with another `pp cmd`: the checks that say a name is free, the install, checkout or removal they allow, and
# the record of it (_changing, a transaction on .index.json under that same lock). The lock is taken after the
# clone and the question, not around them, so one plugin's slow remote or an unanswered prompt never holds up
# another verb; what was decided before it is decided again inside it. Only the record's own scope defers a
# signal, so the work a person waits for is always theirs to stop.

NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")


def _root(create: bool = False) -> str:
    return paths.data_dir("plugins", create=create)


def _index_file(create: bool = False) -> str:
    return os.path.join(_root(create=create), ".index.json")


def _index() -> dict:
    """What is installed, as it was last written. A verb that changes it goes through _changing()."""
    try:
        with open(_index_file()) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _plugins_lock() -> str:
    return os.path.join(_root(create=True), ".lock")


def _holding():
    """The plugins directory's lock, which one `pp cmd` verb holds at a time, with signals left as they are: a
    Ctrl-C ends the work in this scope at once. The slow part of a verb goes here, and _changing takes the same
    lock inside it, since the thread holding it may take it again."""
    return lock.locked(_plugins_lock())


def _changing():
    """The index read, changed and written back under the plugins directory's lock. Signals arriving in this
    scope are delivered when it ends, so what the verb did and the record of it both land, and what the verb
    says it did is printed in the scope. It holds no slow work, since Ctrl-C waits for it."""
    return paths.json_txn(_index_file(create=True), lock=_plugins_lock(), indent=1, sort_keys=True)


def _git(*args: str, group: str, cwd: str | None = None) -> str:
    """git, or the reason it could not be done: a plugin comes from a clone, so a failure here stops the verb.
    group is proc.git's, and it is stated here at every run: "shared" for the two that talk to a remote, so ssh
    can ask on the terminal; "own" for the reads and the checkout, which ask nothing."""
    r = proc.git(*args, group=group, cwd=cwd)
    if r.timed_out:
        ui.die(f"git {args[0]} took more than {proc.GIT_WAIT / 60:g} minutes; nothing changed", 1)
    if r.returncode is None:
        ui.die("git is not installed; pp cmd add and update need it", 1)
    if r.returncode:
        ui.die(ui.relayed(r.stderr.strip() or "failed", f"git {' '.join(args[:2])}"), 1)
    return r.stdout


def _url(spec: str) -> str:
    """github.com/OWNER/REPO means its https URL; a directory means its absolute path; anything else is given to
    git as it is."""
    if re.fullmatch(r"github\.com/[\w.-]+/[\w.-]+", spec):
        return "https://" + spec
    local = os.path.expanduser(spec)
    return os.path.abspath(local) if os.path.isdir(local) else spec


def _commands_in(clone: str) -> list[tuple[str, str, str]]:
    """(name, path, summary) of each command file the clone provides."""
    d = cli.command_dir(clone)
    out = []
    for fn in sorted(os.listdir(d)):
        if cli.is_command_file(fn):
            path = os.path.join(d, fn)
            summary, _, err = cli._scan(path)
            out.append((fn[:-3], path, err or summary))
    return out


def _lines(path: str) -> int:
    with open(path, "rb") as f:
        return f.read().count(b"\n")


def _names_at(clone: str, rev: str) -> list[str]:
    """The command names the clone provides at rev, read from git: nothing is checked out before a yes."""
    top = [line.split("\t", 1) for line in _git("ls-tree", rev, cwd=clone, group="own").splitlines()]
    sub = ["commands/"] if any(meta.split()[1] == "tree" and name == "commands" for meta, name in top) else []
    listed = _git("ls-tree", "--name-only", rev, *sub, cwd=clone, group="own").splitlines()
    files = [os.path.basename(f) for f in listed]
    return [f[:-3] for f in files if cli.is_command_file(f)]


def _clashes(names: list[str], skip: str | None = None) -> list[str]:
    """Which of names are taken, leaving out the plugin being updated (skip)."""
    mine = os.path.join(_root(), skip) + os.sep if skip else None
    cmds = {n: c for n, c in cli.discover().items() if not (mine and c.path.startswith(mine))}
    return [f"{ui.printable(name)}: {why}" for name in names for why in [_taken(name, cmds)] if why]


def _free(name: str, dest: str, index: dict) -> None:
    """Refuse unless the name and its directory are free. Asked before the clone, so a taken name costs no
    download, and asked again inside the transaction, where a plugin that landed in between is a refusal
    rather than a failed rename."""
    if name in index:
        ui.die(f"a plugin named {name} is there already; `pp cmd update {name}`, or --name", 2)
    if os.path.exists(dest):
        ui.die(f"{dest} is in the way and is not a plugin pp added; move it, or pass --name", 2)


def _unclashed(staging: str) -> None:
    """Refuse unless every command the clone provides still has a free name. Same two moments as _free."""
    clashes = _clashes([n for n, _, _ in _commands_in(staging)])
    if clashes:
        ui.die(["these names are taken; nothing was added:"] + ["  " + c for c in clashes], 2)


def _show_commands(clone: str, show: bool) -> None:
    """What a plugin adds, as the user reads it before saying yes: printable() throughout, since the files are
    someone else's and an escape sequence in them must not redraw what the user reads."""
    for name, path, summary in _commands_in(clone):
        print(ui.printable(f"  {name:<12} {summary}  ({os.path.relpath(path, clone)}, {_lines(path)} lines)"))
        if show:
            with open(path, errors="replace") as f:
                for line in f:
                    print("    | " + ui.printable(line.rstrip("\n")))


def _proceed(a, prompt: str) -> int | None:
    """None to go ahead; else the exit code (0 for -n, 2 for a refusal)."""
    if a.dry_run:
        print(ui.c("(dry run: nothing changed)", "dim"))
        return 0
    return None if ui.confirm(prompt, a.yes) else 2


def cmd_add(argv: list[str]) -> int:
    ap = ui.parser("cmd add", "Add a git repository of commands, pinned to one commit.")
    ap.add_argument("url", help="a git URL or path, or github.com/OWNER/REPO")
    ap.add_argument("--ref", help="a branch or tag to follow (default: the repository's default branch)")
    ap.add_argument("--name", help="the plugin's name (default: the repository's)")
    ap.add_argument("--show", action="store_true", help="print the code of every command it adds")
    ap.add_argument("-n", "--dry-run", action="store_true", help="show what it adds, change nothing")
    ap.add_argument("-y", "--yes", action="store_true", help="do not ask")
    a = ap.parse_args(argv)
    url = _url(a.url)
    name = a.name or re.sub(r"\.git$", "", url.rstrip("/").rsplit("/", 1)[-1])
    if not NAME_RE.fullmatch(name):
        ui.die(f"'{name}' cannot name a plugin (letters, digits, _ . -); pass --name", 2)
    dest = os.path.join(_root(), name)
    _free(name, dest, _index())
    if os.path.isdir(url) and not os.path.exists(os.path.join(url, ".git")):
        ui.die(f"{url} is not a git repository; a plugin is a git repository of command files", 2)
    staging = paths.staging(_root(create=True), name)
    try:
        _git("clone", "--quiet", *(["--branch", a.ref] if a.ref else []), "--", url, staging, group="shared")
        commit = _git("rev-parse", "HEAD", cwd=staging, group="own").strip()
        _git("checkout", "--quiet", "--detach", commit, cwd=staging, group="own")
        if not _commands_in(staging):
            ui.die(f"{url} has no command files (commands/*.py, or *.py at its top)", 2)
        print(ui.printable(f"plugin  {name}  from {url}" + (f"  ({a.ref})" if a.ref else "")))
        print("commit  " + ui.printable(_git("log", "-1", "--format=%h %ad %s", "--date=short", cwd=staging,
                                             group="own").strip()))
        print("adds")
        _show_commands(staging, a.show)
        _unclashed(staging)
        if not a.show:
            print(ui.c(f"its code runs as you: read it first (pp cmd add {ui.printable(a.url)} -n --show)",
                       "yellow"))
        rc = _proceed(a, f"add plugin {name}?")
        if rc is not None:
            return rc
        with _changing() as index:      # the rename is instant, so it belongs in the record's own scope
            _free(name, dest, index)
            _unclashed(staging)
            try:
                os.rename(staging, dest)
            except OSError as e:          # something outside pp put it there: say so, do not trace back
                ui.die(f"{name} could not be installed at {dest}: {e}", 1)
            index[name] = {"url": url, "ref": a.ref, "commit": commit, "added": time.time(), "updated": time.time()}
            print(f"added {name} at {commit[:7]}: "
                  + ui.printable(", ".join(n for n, _, _ in _commands_in(dest))))
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return 0


def _plugin(name: str) -> tuple[str, dict]:
    data = _index()
    if name not in data or not os.path.isdir(os.path.join(_root(), name)):
        ui.die(f"no plugin named {name} (pp cmd ls)", 2)
    return os.path.join(_root(), name), data[name]


def _target(clone: str, ref: str | None) -> str:
    """The commit `update` would move to: the fetched branch, or the tag, or the remote's default branch."""
    for rev in ([f"origin/{ref}", ref] if ref else ["origin/HEAD"]):
        r = proc.git("rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}", cwd=clone, group="own")
        if r.returncode == 0:
            return r.stdout.strip()
    ui.die(f"{clone}: {ref or 'the default branch'} is not on the remote any more", 1)


def cmd_update(argv: list[str]) -> int:
    ap = ui.parser("cmd update", "Fetch plugins, show what changed, and move each pin when you say yes.")
    ap.add_argument("names", nargs="*", help="default: every plugin")
    ap.add_argument("--show", action="store_true", help="print the whole diff, not just which files changed")
    ap.add_argument("-n", "--dry-run", action="store_true", help="show what would change, change nothing")
    ap.add_argument("-y", "--yes", action="store_true", help="do not ask")
    a = ap.parse_args(argv)
    names = a.names or sorted(_index())
    if not names:
        print("no plugins (pp cmd add URL)")
        return 0
    rc = 0

    def taken(name: str, clone: str, new: str, old: str) -> bool:
        """Names the commit would bring that something else already has. Printed, not fatal: the other plugins
        still update. Asked before the question and again under the lock."""
        clashes = _clashes(_names_at(clone, new), skip=name)
        if clashes:
            print(f"{name}: these names are taken, so it stays at {old[:7]}:\n  " + "\n  ".join(clashes),
                  file=sys.stderr)
        return bool(clashes)

    for name in names:
        clone, rec = _plugin(name)
        try:        # one plugin's remote being away must not keep the others from updating
            _git("fetch", "--quiet", "--tags", "--force", "origin", cwd=clone, group="shared")
            old, new = rec["commit"], _target(clone, rec.get("ref"))
        except SystemExit:
            print(f"{name}: could not fetch from {rec['url']}; it stays at {rec['commit'][:7]}", file=sys.stderr)
            rc = max(rc, 1)
            continue
        if new == old:
            print(f"{name}: up to date at {old[:7]}")
            continue
        local = _git("status", "--porcelain", "--untracked-files=no", cwd=clone, group="own")
        if local.strip():       # e.g. __pycache__ is no change
            print(f"{name}: {clone} has local changes, so it stays at {old[:7]}; discard them, or "
                  f"`pp cmd rm {name}` and add it again", file=sys.stderr)
            rc = 2
            continue
        print(f"{name}: {old[:7]} -> {new[:7]}")
        for out in (_git("log", "--format=  %h %s", f"{old}..{new}", cwd=clone, group="own"),
                    _git("diff", "--patch" if a.show else "--stat", old, new, cwd=clone, group="own")):
            print("\n".join(ui.printable(line) for line in out.splitlines()))
        go = (2 if taken(name, clone, new, old) else 0 if a.dry_run
              else None if ui.confirm(f"move {name} to {new[:7]}?", a.yes) else 2)
        if go is not None:
            rc = max(rc, go)
            continue
        with _holding():
            if name not in _index():    # a `pp cmd rm` of it finished while this was asked
                print(f"{name}: removed while this ran, so it stays as it is", file=sys.stderr)
                rc = max(rc, 1)
                continue
            if taken(name, clone, new, old):
                rc = 2
                continue
            # a Ctrl-C ends the checkout here. What it leaves is a clone at a commit the index does not record:
            # the next `pp cmd update` of this plugin moves the pin there, or says the clone has local changes.
            _git("checkout", "--quiet", "--detach", new, cwd=clone, group="own")
            with _changing() as index:
                index[name].update(commit=new, updated=time.time())
                print(f"{name}: now at {new[:7]}")
    if a.dry_run:
        print(ui.c("(dry run: nothing changed)", "dim"))
    return rc


def cmd_rm(argv: list[str]) -> int:
    ap = ui.parser("cmd rm", "Remove a plugin and its commands.")
    ap.add_argument("name")
    ap.add_argument("-n", "--dry-run", action="store_true", help="show what goes, change nothing")
    ap.add_argument("-y", "--yes", action="store_true", help="do not ask")
    a = ap.parse_args(argv)
    index = _index()
    if a.name not in index:
        ui.die(f"no plugin named {a.name} (pp cmd ls)", 2)
    clone, rec = os.path.join(_root(), a.name), index[a.name]
    print(ui.printable(f"plugin  {a.name}  at {rec['commit'][:7]} from {rec['url']}"))
    print("removes " + (ui.printable(", ".join(n for n, _, _ in _commands_in(clone))) + f"  ({clone})"
                        if os.path.isdir(clone) else f"its record alone: the clone {clone} is already gone"))
    rc = _proceed(a, f"remove plugin {a.name}?")
    if rc is not None:
        return rc
    with _holding():
        if a.name not in _index():      # another `pp cmd rm` of it finished while this was asked
            ui.die(f"no plugin named {a.name} (pp cmd ls)", 2)
        try:
            # a Ctrl-C ends the removal here. What it leaves is part of the clone, and the record with it, so
            # the next `pp cmd rm` of this plugin removes the rest.
            shutil.rmtree(clone)
        except FileNotFoundError:       # the clone is already gone; its record goes all the same
            pass
        with _changing() as record:
            record.pop(a.name)
            print(f"removed {a.name}")
    return 0


def cmd_ls(argv: list[str]) -> int:
    ap = ui.parser("cmd ls", "Every command, and where it comes from.")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    index, root = _index(), _root()
    rows = []
    for name, c in sorted(cli.discover().items()):
        row = {"name": name, "origin": c.origin, "path": c.path, "summary": c.summary, "error": c.error}
        if c.origin == "plugin":
            plugin = os.path.relpath(c.path, root).split(os.sep)[0]
            rec = index.get(plugin, {})
            row.update(plugin=plugin, url=rec.get("url"), ref=rec.get("ref"), commit=rec.get("commit"))
        rows.append(row)
    if a.json:
        ui.print_json(rows)
        return 0
    shown = []
    for r in rows:
        where = (f"{r['plugin']} at {(r['commit'] or '?')[:7]}" + (f" ({r['ref']})" if r.get("ref") else "")
                 + f" from {r['url']}") if r["origin"] == "plugin" else r["path"]
        shown.append([ui.printable(r["name"]), r["origin"], ui.printable(where)])
    print(ui.table(shown, ["command", "from", "where"]))
    return 0


VERBS = {"new": cmd_new, "add": cmd_add, "update": cmd_update, "rm": cmd_rm, "ls": cmd_ls}


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        ui.parser("cmd", __doc__).print_help()
        print("\nverbs: " + " ".join(VERBS))
        return 0
    verb = argv[0]
    if verb not in VERBS:
        cands = [v for v in VERBS if v.startswith(verb)]
        if len(cands) != 1:
            ui.die(f"unknown verb {verb!r}; one of: {', '.join(VERBS)}", 2)
        verb = cands[0]
    return VERBS[verb](argv[1:])
