"""Write commands, or add others' from a git repository

    pp cmd new NAME                 pp/commands/NAME.py in this pp's source tree, with tests/test_NAME.py and
                                    tests/test_journey_NAME.py (its help examples, run through the real CLI);
                                    run from a promoted release, that is the tree it was released from
    pp cmd new NAME --user          ~/.config/pp/commands/NAME.py, on this machine only
    pp cmd new NAME -n              the files it would write, writing none
    pp cmd new NAME --user -s "One-line summary"
    pp cmd add github.com/OWNER/REPO      clone it, pin it to one commit, show what it adds, then ask
    pp cmd add URL -n --show        only show: its commands, and their code
    pp cmd add URL --ref v1.2 --name tools
    pp cmd update                   every plugin: fetch, show the diff, ask, then move the pin
    pp cmd update tools -n          only show what would change
    pp cmd rm tools                 remove a plugin and its commands
    pp cmd ls                       every command and where it comes from (--json, with its aliases)
    pp cmd alias                    every alias, what it runs and where it is declared (--json)
    pp cmd alias st promote status  `pp st` runs `pp promote status`; pp cmd alias st --rm removes it
    pp cmd alias st -n promote      print the change, write nothing

A new command has a docstring whose first line is its summary, a main(argv), and only the standard library at
the top of the file (the rest: https://github.com/xflash96/pp/blob/main/CONTRIBUTING.md).

A command is named by its file, by its module's ALIASES, and by `[alias]` in ~/.config/pp/config.toml, which
names any command anything on this machine. `pp cmd alias` is the one place pp writes that file: it adds or
removes one line of the [alias] table and leaves every other byte of it as it was.

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
import shlex
import shutil
import sys
import time
import tomllib

from pp import cli
from pp.lib import config, lock, paths, proc, ui

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

def _new_files(name: str, d: str, test_dir: str | None) -> list[tuple[str, str]]:
    """Every file `cmd new` writes, in the order it writes them: the command first, then each test whose
    harness is in that tests directory. A tests/ without `sandbox.py` or `journey.py` is a published subset
    or a contributor's own, and a test that cannot import is worse than no test. The plan and the run read
    this one list, so both name the same files."""
    out = [(os.path.join(d, name + ".py"), TEMPLATE)]
    for tmpl, fname, needed in ((TEST_TEMPLATE, f"test_{name}.py", "sandbox.py"),
                                (JOURNEY_TEMPLATE, f"test_journey_{name}.py", "journey.py")):
        if test_dir and os.path.exists(os.path.join(test_dir, needed)):
            out.append((os.path.join(test_dir, fname), tmpl))
    return out


def cmd_new(argv: list[str]) -> int:
    ap = ui.parser("cmd new", "Write a new command, with its tests.")
    ap.add_argument("name")
    ap.add_argument("-s", "--summary", default="TODO: one-line summary")
    ap.add_argument("--user", action="store_true", help="write to ~/.config/pp/commands (machine-local)")
    ap.add_argument("--no-test", action="store_true")
    ap.add_argument("-n", "--dry-run", action="store_true", help="show the files it would write, write none")
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
        test_dir = os.path.join(root, "tests")
    files = _new_files(a.name, d, None if a.no_test else test_dir)
    cls = "".join(w.capitalize() for w in a.name.split("_"))
    hint = f"try: {prog} {a.name} -h" + ("   (`pp` gets it at the next `pp promote` of that tree)" if prog == "ppd" else "")
    if a.dry_run:
        for path, _ in files:
            print(f"{'exists already' if os.path.lexists(path) else 'would write'}: {path}")
        print(hint)
        return 2 if os.path.lexists(files[0][0]) else 0       # what the run itself would refuse
    os.makedirs(d, exist_ok=True)
    for n, (path, tmpl) in enumerate(files):
        try:
            with open(path, "x") as f:  # "x": the file is this writer's own, or there was one already
                f.write(tmpl.format(name=a.name, summary=a.summary, cls=cls))
        except FileExistsError:
            if n == 0:                  # the command file itself: the name is taken, and nothing is written
                ui.die(f"{path} exists", 2)
            continue                    # a test file of that name is the user's: never overwritten
        print(f"wrote {path}")
    print(hint)
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


# ---- aliases ------------------------------------------------------------------------------------
#
# `[alias]` in config.toml is pp's own naming table, read in pp/cli.py and written here and nowhere else. The
# file is a person's: their comments and their other tables are in it, so this writes one line of the `[alias]`
# table and leaves every other byte alone.
#
# The edit is checked before it lands: the result is parsed, and it must be the old parse with exactly that one
# key added or removed. So an edit this line edit cannot make (an `alias = { ... }` written inline, a value
# spread over lines) ends with the file named for the person to change by hand, never with pp rewriting it.

_TABLE = re.compile(r"\[\s*alias\s*\]\s*(#.*)?")    # the `[alias]` header line, with the comment TOML allows
_ESCAPES = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\t": "\\t", "\n": "\\n", "\f": "\\f", "\r": "\\r"}


def _toml_string(s: str) -> str:
    """s as a TOML basic string: the escapes TOML names, and \\uXXXX for every other control character."""
    out = "".join(_ESCAPES.get(ch) or (ch if ch >= " " and ch != "\x7f" else f"\\u{ord(ch):04X}") for ch in s)
    return f'"{out}"'


def _key_line(name: str) -> re.Pattern:
    """What the line holding this key of `[alias]` looks like: the bare key, or the key quoted, then `=`."""
    k = re.escape(name)
    return re.compile(rf"""\s*({k}|"{k}"|'{k}')\s*=""")


def _edited(text: str, name: str, value: str | None) -> str | None:
    """text with one line added to or removed from its `[alias]` table; None where that cannot be one line.

    An added key goes at the end of the table's own lines, or into a new `[alias]` table at the end of the
    file. A removed one is the line it is written on. Nothing else in the file is touched."""
    lines = text.splitlines(keepends=True)
    at = next((i for i, line in enumerate(lines) if _TABLE.fullmatch(line.strip())), None)
    if at is None:
        if value is None:
            return None
        pad = "" if not text or text.endswith("\n") else "\n"
        blank = "\n" if text else ""        # a table of its own, one blank line below whatever was there
        return text + pad + blank + "[alias]\n" + f"{name} = {_toml_string(value)}\n"
    end = next((i for i in range(at + 1, len(lines)) if lines[i].lstrip().startswith("[")), len(lines))
    if value is None:
        hit = [i for i in range(at + 1, end) if _key_line(name).match(lines[i])]
        if len(hit) != 1:
            return None
        del lines[hit[0]]
        return "".join(lines)
    while end > at + 1 and not lines[end - 1].strip():      # the table's own lines end before the blank ones
        end -= 1
    if not lines[end - 1].endswith("\n"):       # a file whose last line has no newline of its own
        lines[end - 1] += "\n"
    lines.insert(end, f"{name} = {_toml_string(value)}\n")
    return "".join(lines)


def _wanted(old: dict, name: str, value: str | None) -> dict | None:
    """The parse the edited file must come to: the old one with exactly that key of `[alias]` added or removed.
    None where the old file holds an `alias` that is no table, which this cannot edit."""
    table = old.get("alias", {})
    if not isinstance(table, dict):
        return None
    table = dict(table)
    if value is None:
        table.pop(name, None)
    else:
        table[name] = value
    return {**old, "alias": table}


def _write_alias(name: str, value: str | None, dry_run: bool) -> None:
    """Add or remove one key of `[alias]`, or refuse and leave the file byte for byte as it was.

    Under config.toml's own lock, through a temporary file in its directory renamed over it with its mode kept
    (0600 where there was no file), and only once the result parses as the old file with that one change. A dry
    run builds the edit and checks it too, so the change it prints is the change the run makes."""
    path = config.global_path()
    change = f"remove {name}" if value is None else f"write {name} = {_toml_string(value)}"
    with lock.locked(path + ".lock", defer=True):
        try:        # the bytes and the mode of one open file, so the mode kept is the mode of what was read
            with open(path, "rb") as f:
                raw, mode = f.read(), os.fstat(f.fileno()).st_mode & 0o7777
        except FileNotFoundError:       # no file at all: this writer makes one
            raw, mode = b"", 0o600
        else:
            if not raw:     # a file rewritten in place is empty for a moment, which is not "no settings"
                raise config.empty_error(path)
        try:
            old = tomllib.loads(raw.decode())
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as e:
            raise config.ConfigError(f"{path}: {e}") from None
        text, want = _edited(raw.decode(), name, value), _wanted(old, name, value)
        if text is None or want is None or _parse(text) != want:
            ui.die(f"pp cannot make this change one line of {ui.printable(path)}; {change} in its [alias] "
                   "table by hand", 2)
        if dry_run:     # the edit is made and checked first, so the plan is the run and not another answer
            print(f"would {change} in [alias] of {ui.printable(path)}")
            print(ui.c("(dry run: nothing changed)", "dim"))
            return
        paths.write_text(path, text, mode)
        print(f"{'removed' if value is None else 'wrote'} {name} in [alias] of {ui.printable(path)}")


def _parse(text: str) -> dict | None:
    """What TOML makes of the edited file; None where it makes nothing of it."""
    try:
        return tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return None


def _alias_rows(cmds: dict, table: dict) -> list[dict]:
    """Every alias of both kinds: the `[alias]` table's, then each module's own ALIASES."""
    path = config.global_path()
    rows = [{"name": n, "argv": words, "source": "config", "path": path, "error": why}
            for n, (words, why) in sorted(table.items())]
    return rows + [{"name": a, "argv": [c.name], "source": "command", "path": c.path, "error": c.error}
                   for c in sorted(cmds.values(), key=lambda c: c.name) for a in c.aliases]


def _own_flags(argv: list[str]) -> tuple[list[str], list[str]]:
    """This verb's own words, and the command line after NAME, which is read as nobody's flags but that
    command's: `pp cmd alias q queue -q big` gives `-q big` to `queue` and not to argparse here."""
    plain = [i for i, w in enumerate(argv) if not w.startswith("-")]
    cut = plain[1] if len(plain) > 1 else len(argv)
    return argv[:cut], argv[cut:]


def cmd_alias(argv: list[str]) -> int:
    ap = ui.parser("cmd alias", "Name a command whatever you like, on this machine.",
                   usage="pp cmd alias [-n] [NAME [CMD [ARGS...] | --rm]]  (bare: the listing, --json for data)")
    ap.add_argument("name", nargs="?", help="the name to give; without one, every alias is listed")
    ap.add_argument("--rm", action="store_true", help="take this name back")
    ap.add_argument("--json", action="store_true", help="the listing as data")
    ap.add_argument("-n", "--dry-run", action="store_true", help="print the change, write nothing")
    head, line = _own_flags(argv)
    a = ap.parse_args(head)
    cmds = cli.discover()
    table, unreadable = cli.alias_table(cmds)
    if unreadable:      # this verb's own file: the error every reader of a bad config file gives
        raise config.ConfigError(unreadable)
    if a.name is None:
        if a.rm:
            ui.die("usage: pp cmd alias NAME --rm", 2)
        rows = _alias_rows(cmds, table)
        if a.json:
            ui.print_json(rows)
            return 0
        if not rows:
            print("no aliases (pp cmd alias st promote status)")
            return 0
        print(ui.table([[ui.printable(r["name"]), ui.printable(" ".join(r["argv"])),
                         ui.printable(r["path"]) + (f"  ({ui.printable(r['error'])})" if r["error"] else "")]
                        for r in rows], ["alias", "runs", "declared in"]))
        return 0
    if a.rm:
        if line:
            ui.die("`pp cmd alias NAME --rm` takes no command after the name", 2)
        if a.name not in table:
            ui.die(f"no alias '{ui.printable(a.name)}' in [alias]; `pp cmd alias` lists them", 2)
        _write_alias(a.name, None, a.dry_run)
        return 0
    if not line:
        ui.die(f"usage: pp cmd alias {ui.printable(a.name)} CMD [ARGS...]   (or --rm)", 2)
    if not cli.ALIAS_KEY.fullmatch(a.name):
        ui.die("an alias is a letter, then letters, digits, _ and -", 2)
    taken = _taken(a.name, cmds)
    if taken:
        ui.die(f"'{ui.printable(a.name)}' is taken: {taken}; pick another name", 2)
    if a.name in table:
        ui.die(f"'{ui.printable(a.name)}' is an alias already; `pp cmd alias {ui.printable(a.name)} --rm` "
               "first", 2)
    if not isinstance(cli.find(line[0], cmds), cli.Command):
        ui.die(f"no command '{ui.printable(line[0])}'; `pp -h` lists them", 2)
    _write_alias(a.name, shlex.join(line), a.dry_run)
    return 0


def cmd_ls(argv: list[str]) -> int:
    ap = ui.parser("cmd ls", "Every command, and where it comes from.")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    index, root = _index(), _root()
    cmds = cli.discover()
    given = cli.alias_names(cmds)           # every name `[alias]` gives, with the words it runs
    named: dict[str, list[str]] = {}        # the names `[alias]` gives, per command, beside its own ALIASES
    for n, words in given:
        got = cli.find(words[0], cmds)
        if isinstance(got, cli.Command):
            named.setdefault(got.name, []).append(n)
    rows = []
    for name, c in sorted(cmds.items()):
        row = {"name": name, "origin": c.origin, "path": c.path, "summary": c.summary, "error": c.error,
               "aliases": cli.aliases_of(c, given), "config_aliases": named.get(name, [])}
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
        shown.append([ui.printable(r["name"]), r["origin"],
                      ui.printable(" ".join(r["aliases"] + r["config_aliases"])), ui.printable(where)])
    print(ui.table(shown, ["command", "from", "aliases", "where"]))
    return 0


VERBS = {"new": cmd_new, "add": cmd_add, "update": cmd_update, "rm": cmd_rm, "ls": cmd_ls, "alias": cmd_alias}


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
