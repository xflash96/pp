"""Where pp keeps things. XDG locations, each overridable by a PP_* variable (tests point them at a temp dir).

    config  ~/.config/pp        PP_CONFIG_DIR   config.toml, commands/ (user commands)
    state   ~/.local/state/pp   PP_STATE_DIR    queues, journals, logs, per command: state_dir("queue")
    data    ~/.local/share/pp   PP_DATA_DIR     releases made by `pp promote`, plugins added by `pp cmd add`

`json_txn` is the read-modify-write a JSON record goes through; its docstring is the contract. `write_json` and
`write_text` replace a whole file in one step, and `staging` hands out a name no other writer picks. `file_now`
and `stamp` are one clock: the time now and a file's time, as the file system keeps them.
"""
from __future__ import annotations

import copy
import itertools
import json
import os
import secrets
import sys
import tempfile
import threading
import time
from contextlib import contextmanager

from pp.lib.lock import locked


def expand(p: str) -> str:
    """~ and $VARS expanded, made absolute. Symlinks are kept (use os.path.realpath when identity matters)."""
    return os.path.abspath(os.path.expanduser(os.path.expandvars(p)))


def _xdg(env: str, xdg: str, fallback: str) -> str:
    if os.environ.get(env):
        return expand(os.environ[env])
    return os.path.join(expand(os.environ.get(xdg) or fallback), "pp")


def config_dir() -> str:
    return _xdg("PP_CONFIG_DIR", "XDG_CONFIG_HOME", "~/.config")


def state_dir(*sub: str, create: bool = True) -> str:
    d = os.path.join(_xdg("PP_STATE_DIR", "XDG_STATE_HOME", "~/.local/state"), *sub)
    if create:
        os.makedirs(d, exist_ok=True)
    return d


def data_dir(*sub: str, create: bool = True) -> str:
    d = os.path.join(_xdg("PP_DATA_DIR", "XDG_DATA_HOME", "~/.local/share"), *sub)
    if create:
        os.makedirs(d, exist_ok=True)
    return d


def pp_root() -> str:
    """The directory holding the running pp package (a source tree or a release)."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


RELEASE_META = ".pp-release.json"
# `pp promote` writes it into each release it makes: {name, rev, source, mode, dirty, checked, created,
# created_iso}, plus `failed` (the hook name) once activating that release failed. Readers: promote (rollback, ls
# and status skip or mark a failed or unchecked release), cmd new (writes a built-in into `source`, never into the
# snapshot).


def release_meta(root: str) -> dict | None:
    """The metadata `pp promote` left in the release at root; None when root is not a release (e.g. a source tree)."""
    try:
        with open(os.path.join(root, RELEASE_META)) as f:
            meta = json.load(f)
    except (OSError, ValueError):
        return None
    return meta if isinstance(meta, dict) else None


@contextmanager
def _replacing(path: str):
    """Yield a file open for writing, and put it in place of path when the block ends. A block that raises
    leaves path as it was and no temporary file behind."""
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=os.path.basename(path) + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            yield f
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def write_json(path: str, data, **dump) -> None:
    """Replace path with data as JSON in one step: a reader sees the old file or the new one, never part of one.
    The temporary file is this writer's own, so two writers at once cannot mix their halves; the later rename
    wins, so a read-modify-write goes through json_txn, which holds a lock."""
    with _replacing(path) as f:
        json.dump(data, f, **dump)


def write_text(path: str, s: str, mode: int | None = None) -> None:
    """Replace path with s in one step, the way write_json replaces a record. Without a mode the file keeps
    the one it had, so rewriting one a person edits does not change who can read it, and a new file is 0644;
    with one, the file is written and left with exactly that mode, which is how a file holding a job's own
    environment is readable by its owner alone."""
    if mode is None:
        try:
            mode = os.stat(path).st_mode & 0o7777
        except OSError:
            mode = 0o644
    with _replacing(path) as f:
        os.fchmod(f.fileno(), mode)
        f.write(s)


# CLOCK_REALTIME_COARSE, which no Python pp runs on names: the clock Linux stamps a file's times from
FILE_CLOCK = 5


def file_now() -> float:
    """Now, by the clock a file's times are stamped from. A file written after this read is stamped no earlier
    than it, which time.time() does not give: that runs ahead of the stamp by up to one tick of the kernel's
    clock, 1 to 10 ms. So a time read here and a file's `stamp` say which came first, and in the same tick the
    two can be equal. Whole nanoseconds over 1e9, as `stamp` reads them, so equal times compare equal."""
    return time.clock_gettime_ns(FILE_CLOCK) / 1e9


def stamp(fd: int) -> float:
    """When the open file fd was last written, on the clock `file_now` reads."""
    return os.fstat(fd).st_mtime_ns / 1e9


_counter = itertools.count(1)       # the number in a staging name; one process hands each out once
_counting = threading.Lock()


def staging(parent: str, prefix: str) -> str:
    """A path under parent that no other writer picks: `.staging-<prefix>-<pid>-<number>-<random>`.

    Two names from one process differ in the number, which is handed out once, so within a process no two can
    be the same. Another process has another pid. The random tail tells this pid from the same pid in another
    namespace or after a wrap. The caller creates the name itself, with a mkdir, an `open(..., "x")`, a symlink
    or a clone, each of which fails on a name that is there, so a name that did repeat would stop the command
    instead of writing over what holds it. Two callers replace the name instead of creating it: the store's
    `add` (jobq._stage_job writes it with write_json) and promote's `_discard` (it renames what it is removing
    onto the name). Each holds the lock of the thing it is staging for, the store's or the target's, so no
    other writer can hold the name.

    Nothing has to be removed before the name is used, and nobody else can remove it between its creation and
    its rename. The leading dot keeps it out of listings that read every entry as a real one."""
    with _counting:
        n = next(_counter)
    return os.path.join(parent, f".staging-{prefix}-{os.getpid()}-{n}-{secrets.token_hex(4)}")


@contextmanager
def json_txn(path: str, *, lock: str | None = None, **dump):
    """Read the JSON record at path, hand it over to change, and write it back, all under one lock, so a writer
    that read first never loses what another recorded meanwhile. The lock is `path + ".lock"` unless lock names
    another file. A file that is missing, unreadable, or not a JSON object reads as an empty one.

    The record is written only when the block ends without raising, and only when it changed: a `ui.die` inside
    is a refusal, and leaves the file byte for byte as it was. The signals `lock.deferring` holds back are
    delivered when the block ends, so a change the block makes elsewhere and the record it writes both land;
    keep the block short, since Ctrl-C waits for it."""
    with locked(lock or path + ".lock", defer=True):
        blank: dict = {}
        try:
            with open(path) as f:
                record = json.load(f)
        except (OSError, ValueError):
            record = blank
        if not isinstance(record, type(blank)):
            record = blank
        before = copy.deepcopy(record)
        yield record
        if record != before:
            write_json(path, record, **dump)


def self_cmd() -> list[str]:
    """argv that runs this same pp (same interpreter, same tree), for detached helpers like a queue's worker: the
    tree's bin/pp in a clone or a release; in an installed package, which has no bin/, `python -P -m pp` (-P: a
    pp/ folder in the current directory must not stand in for the installed one)."""
    launcher = os.path.join(pp_root(), "bin", "pp")
    return [sys.executable, launcher] if os.path.isfile(launcher) else [sys.executable, "-P", "-m", "pp"]
