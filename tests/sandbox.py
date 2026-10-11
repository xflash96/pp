"""The scope a test's own process holds: one object that owns everything a test would otherwise change for the
whole run. A test enters it once and changes none of it by hand (tests/test_constructs.py reads the source and
fails on a test that does).

What it owns, for as long as the test lasts:

    the PP_* directories    pointed at a temporary tree, so in-process pp works there and nowhere else
    HOME                    a directory in that tree, so a `~` path a test or pp expands stays in the tree
    git's environment       a global config file of the tree's, no system config, an author and a committer,
                            and no other GIT_* variable, so git reads nothing of the machine's
    the session identity    removed, so pp sees a person; `as_session` names one the test chose
    the cwd                 `chdir`, and the directory the test started in back afterwards
    signal dispositions     `on_signal`
    the warnings filter     `filter_warnings`
    threads and servers     `ticker` and `serving`, stopped on the way out
    what the test puts back `later`, run once those are stopped and before anything else goes back

Order on the way out is the point of the object: every thread and server it handed out is stopped first, then
the callbacks `later` registered, and only then do the environment, the cwd, the dispositions and the filter go
back. A thread still running with the PP_* variables restored would read and write the caller's own
~/.local/state/pp. A thread that will not stop is why `close` can raise and give nothing back.

Two ways in, one per test class:

    with sandbox.Sandbox() as sb:       ... for one test
    def setUp(self):                    ... for a class, as the first line, so the scope closes last
        self.sb = sandbox.enter(self)

The tree is `sb.root`, with `sb.state`, `sb.config`, `sb.data`, `sb.work` and `sb.home` in it. Only the root,
`work` and `home` are created: pp makes the directories it uses, so a test can still assert that a command
which must write nothing created nothing. `sb.gitconfig` is the global config git reads in the scope, and a
test that wants a global git setting writes it there.
"""
from __future__ import annotations

import os
import signal
import sys
import tempfile
import threading
import warnings

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from pp.lib import session  # noqa: E402

JOIN_WAIT = 30.0        # how long a thread of the scope's gets to end before the test is told it would not
POLL = 0.05             # how often a server of the scope's looks for the shutdown the scope asks for

GIT_WHO = "T"                   # who a test commits as, author and committer
GIT_EMAIL = "t@example.invalid"


def git_env(gitconfig: str) -> dict[str, str]:
    """What git is told inside a test, so that a test's repositories are the test's alone: who commits, that
    there is no system config, and that the global config is this file. A `core.hooksPath`, a
    `commit.template` or an identity on the machine then reaches no git a test runs.

    The one statement of it: tests/journey.py asks for it too, for the processes a journey starts, so a
    change to what git may read is made here alone."""
    return {"GIT_AUTHOR_NAME": GIT_WHO, "GIT_AUTHOR_EMAIL": GIT_EMAIL,
            "GIT_COMMITTER_NAME": GIT_WHO, "GIT_COMMITTER_EMAIL": GIT_EMAIL,
            "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": gitconfig}


class StillRunning(AssertionError):
    """A thread or a server the scope handed out would not stop within JOIN_WAIT.

    The scope raises this instead of giving anything back. A thread that is still running keeps reading and
    writing where the PP_* variables point, so they stay pointed at the sandbox's tree and the tree stays: what
    that thread writes next lands in a temporary directory nobody else uses, never in the machine's own state.
    The scope stays open, so closing it again once the thread has stopped gives everything back."""


class Ticker:
    """A thread the scope handed out: it calls fn, waits the period, and goes again, until it is stopped or fn
    raises. What fn raised comes back out of `stop`, so a ticker that broke cannot pass unnoticed.

    The wait is on the stop event, not a sleep, so stopping is not held up by a period that has just begun."""

    def __init__(self, fn, period: float, name: str | None = None):
        self.fn, self.period = fn, period
        self.error: BaseException | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=name or getattr(fn, "__name__", "ticker"),
                                        daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.fn()
            except BaseException as e:      # raised again in the test's own thread, by stop()
                self.error = e
                return
            self._stop.wait(self.period)

    @property
    def alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self) -> None:
        """Stop ticking and wait for the thread. Called again, it does nothing."""
        self._stop.set()
        self._thread.join(JOIN_WAIT)
        if self._thread.is_alive():
            raise StillRunning(f"the ticker {self._thread.name} would not stop within {JOIN_WAIT:g}s")
        if self.error is not None:
            err, self.error = self.error, None
            raise err


class Sandbox:
    """The scope itself. `enter` or `with` opens one. The one test that calls `open` and `close` by hand is the
    one that closes the scope to see what `close` raises, which `with` would raise past its own assertions."""

    def __init__(self):
        self.root = self.state = self.config = self.data = self.work = self.home = self.gitconfig = ""
        self._tmp: tempfile.TemporaryDirectory | None = None
        self._cwd = ""
        self._env: dict[str, str | None] = {}
        self._signals: dict[int, object] = {}
        self._warnings: warnings.catch_warnings | None = None
        self._stops: list = []
        self._laters: list = []
        self._open = False

    # -- the scope -------------------------------------------------------------------------------------------
    def open(self) -> Sandbox:
        """Take over: a temporary tree, the PP_* variables and HOME pointed at it, git told what to read and who
        commits, no session identity, and the warnings filter and the cwd remembered as they are."""
        assert not self._open, "a sandbox is entered once"
        self._open = True
        self._tmp = tempfile.TemporaryDirectory(prefix="pp-sandbox-", ignore_cleanup_errors=True)
        self.root = os.path.realpath(self._tmp.name)
        self.state, self.config, self.data = (os.path.join(self.root, n) for n in ("state", "config", "data"))
        self.work, self.home = self.dir("work"), self.dir("home")
        self.gitconfig = self.path("home", ".gitconfig")
        self._cwd = os.getcwd()
        self._warnings = warnings.catch_warnings()
        self._warnings.__enter__()
        # every PP_*, XDG_* and GIT_* the caller has goes, so a variable set on the machine cannot reach this
        # test: a GIT_CONFIG_GLOBAL or a GIT_DIR of the contributor's would reach every git a test runs
        self.setenv(**{k: None for k in os.environ if k.startswith(("PP_", "XDG_", "GIT_"))})
        self.setenv(**{k: None for k in session.SESSION_VARS})
        self.setenv(PP_STATE_DIR=self.state, PP_CONFIG_DIR=self.config, PP_DATA_DIR=self.data, NO_COLOR="1",
                    HOME=self.home, **git_env(self.gitconfig))
        return self

    def close(self) -> None:
        """Give it all back: threads and servers first, then what `later` registered, then the cwd, the
        dispositions, the filter, the environment and the tree. Called again, it does nothing.

        A thread or server that would not stop raises StillRunning and nothing is given back, so that thread
        goes on writing into this sandbox's tree. The scope stays open then, and closing it again once the
        thread has stopped gives everything back."""
        if not self._open:
            return
        failed, running = [], []
        for stop in reversed(self._stops):      # nothing of the test's is still running past this point
            try:
                stop()
            except StillRunning as e:
                running.append(e)
            except BaseException as e:          # the first is raised once the scope has been given back
                failed.append(e)
        if running:
            raise StillRunning(f"{running[0]}; the PP_* variables still point at {self.root}, and it stays. "
                               "Close the scope again once that thread has stopped.") from running[0]
        self._stops.clear()
        self._open = False
        for fn in reversed(self._laters):       # the test's own way out, with the tree and the store still its
            try:
                fn()
            except BaseException as e:
                failed.append(e)
        self._laters.clear()
        try:
            os.chdir(self._cwd)
        except OSError as e:
            failed.append(e)
        for sig, handler in self._signals.items():
            # getsignal answers None for a disposition no Python code set; the default comes back instead
            signal.signal(sig, handler if handler is not None else signal.SIG_DFL)
        self._signals.clear()
        self._warnings.__exit__(None, None, None)
        for name, value in self._env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self._env.clear()
        self._tmp.cleanup()
        if failed:
            raise failed[0]

    def __enter__(self) -> Sandbox:
        return self.open()

    def __exit__(self, *exc) -> None:
        self.close()

    # -- the tree --------------------------------------------------------------------------------------------
    def path(self, *parts: str) -> str:
        """A path in the tree. Nothing is created: the name is all a test needs to say where something goes."""
        return os.path.join(self.root, *parts)

    def dir(self, *parts: str) -> str:
        """A directory in the tree, created. For a test that writes a file (a config.toml) before pp runs."""
        p = self.path(*parts)
        os.makedirs(p, exist_ok=True)
        return p

    # -- the environment -------------------------------------------------------------------------------------
    def setenv(self, **named: str | None) -> None:
        """Set these variables for the scope; None removes one. Each goes back to what it was, set or unset."""
        for name, value in named.items():
            self._env.setdefault(name, os.environ.get(name))
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def as_session(self, sid: str) -> None:
        """Act as agent session sid from here on: the variables pp reads to name a queued job's owner."""
        self.setenv(CLAUDE_CODE_SESSION_ID=sid, CLAUDE_PID=str(os.getpid()))

    def chdir(self, path: str | None = None) -> str:
        """Work in path (the scope's `work` by default) until the scope ends. -> where it moved to."""
        p = path or self.work
        os.chdir(p)
        return p

    # -- dispositions and warnings ---------------------------------------------------------------------------
    def on_signal(self, sig: int, handler) -> None:
        """Handle sig this way for the scope. The disposition that was there comes back on the way out, so a
        handler a test installs cannot act in the next test."""
        self._signals.setdefault(sig, signal.getsignal(sig))
        signal.signal(sig, handler)

    def filter_warnings(self, action: str, category: type = Warning) -> None:
        """A warnings filter for the scope only: the suite's filters come back on the way out."""
        warnings.simplefilter(action, category)

    # -- what runs beside the test ---------------------------------------------------------------------------
    def ticker(self, fn, period: float = 0.05, name: str | None = None) -> Ticker:
        """A thread calling fn until the scope ends, or until `Ticker.stop` is called. For a verb that blocks
        while something else has to keep happening (a worker's tick under a `wait`)."""
        t = Ticker(fn, period, name)
        self._stops.append(t.stop)
        return t

    def later(self, fn) -> None:
        """Run fn on the way out, after every thread and server of the scope's has stopped and before anything
        is given back. For the test's own part of the way out: what fn puts back is then never seen by a thread
        of the test's, and what fn reads or writes is still this scope's."""
        self._laters.append(fn)

    def serving(self, server):
        """Serve `server` in a thread of the scope's. On the way out it is shut down, the thread joined and the
        port closed, in that order. -> server, so the caller reads its address from it."""
        t = threading.Thread(target=server.serve_forever, args=(POLL,), name="sandbox-server", daemon=True)
        t.start()

        def stop():
            server.shutdown()               # from this thread, never from the one serving
            t.join(JOIN_WAIT)
            server.server_close()
            if t.is_alive():
                raise StillRunning(f"the server thread {t.name} would not stop within {JOIN_WAIT:g}s")
        self._stops.append(stop)
        return server


def enter(case) -> Sandbox:
    """A sandbox for the whole of case, closed when it ends.

    Call it first in setUp, so the scope closes after everything the test registers inside it: a child a test
    cleans up ends while the PP_* variables are still the sandbox's. A class whose tests share one fixture
    calls it in setUpClass instead, with the class itself as `case`."""
    sb = Sandbox().open()
    (case.addClassCleanup if isinstance(case, type) else case.addCleanup)(sb.close)
    return sb
