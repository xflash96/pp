"""Harness for user-journey tests (tests/test_journey_*.py): drive the real `bin/pp` the way a person or an agent
does, one process per step, in a sandbox, so a journey can promote, link into ~/.local/bin, start workers and
serve ports without touching the real machine.

Isolated: HOME and the PP_* dirs (a temp dir); XDG_*, GIT_*, PP_* and the caller's agent-session variables
removed (a suite run inside a Claude Code session still sees a person); the git environment
tests/sandbox.py's `git_env` states, whose global config is a file in the temp dir, so git reads no config of
the machine's;
NO_COLOR, TERM=dumb; `nvidia-smi` is a fake first on PATH that sees no GPU (self.gpus replaces it).
test_journey_cli's TestTheHarness checks each of these.
Not isolated: the rest of PATH (git, sh, bash, zsh are the machine's), ports (whoever listens binds port 0 and
says which port it got, so no number is handed to something that binds it later), the process table (kill only
what a journey started; patterns carry a unique token).

Which tier: a journey is a story through the CLI (exit codes, what is printed, what lands on disk) and spawns
bin/pp per step on purpose, so the launcher, discovery, flushing, signals and the environment pp passes on are
exercised too. Logic with many cases (parsers, schedulers) is tested in-process in tests/test_NAME.py instead.
Cost: the journeys are most of the suite's time, and `pp promote` runs the suite."""
from __future__ import annotations

import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PP = os.path.join(ROOT, "bin", "pp")
sys.path.insert(0, ROOT)

import sandbox  # noqa: E402
from pp.lib import proc, session  # noqa: E402

# Publishing a subset of pp is deleting command files (with their tests): a journey step that uses a command other
# than its own is guarded by has()/needs(), so what remains still passes.
BUILTINS = tuple(sorted(f[:-3] for f in os.listdir(os.path.join(ROOT, "pp", "commands"))
                        if f.endswith(".py") and not f.startswith("_")))


# Every field `pp queue show --json` promises, read off one record. A reader ignores a field it does not
# know, so a field beside these is no failure; a field missing from a job in any of the five states is. Both
# tiers read this one list: the journey of an agent session, and the machine-surface test of tests/test_queue.py.
PROMISED = ("id", "state", "queue", "tag", "project", "cwd", "cmd", "runner", "needs", "prio", "owner",
            "timeout", "grace", "retries", "attempt", "rc", "reason", "not_retried", "added", "started",
            "ended", "seconds", "log", "log_at", "container", "envfile", "rcfile", "cards", "history", "notes",
            "reported", "hooks", "env", "shell_env", "stop_cmd")


def has(*cmds: str) -> bool:
    return all(c in BUILTINS for c in cmds)


def needs(*cmds: str):
    """Skip a test or class unless these built-ins are in this tree."""
    return unittest.skipUnless(has(*cmds), f"pp {', '.join(cmds)} not in this tree")


def copy_tree(dest: str) -> str:
    """A source tree of pp (bin, pp, tests) at dest, to change or delete files in instead of this checkout."""
    for d in ("bin", "pp", "tests"):
        shutil.copytree(os.path.join(ROOT, d), os.path.join(dest, d),
                        ignore=shutil.ignore_patterns("__pycache__", "test_journey_*.py"))
    return dest


class Child:
    """A process a test started beside itself: its ready line, the signals the test sends it, and its end.

    `ready` is the protocol between the two: the process prints one line once its own setup is done, such as a
    handler installed or a port bound, and the test acts on it only after that line. Before it the process has
    exec'd and done none of its own setup: a handler it installs is not installed, a port it binds is not
    bound, and its command line may still read empty. Signals go through the same handle pp signals with, and
    through a pidfd of the process, so a signal reaches that process or nobody even once its pid is handed
    on."""

    def __init__(self, p: subprocess.Popen, group: str):
        self.p = p
        self.pid = p.pid
        self.fd = proc.pidfd(p.pid)
        self.handle = proc.Handle.live(p.pid, fd=self.fd)
        self.group = proc.Handle.live(p.pid, group=True) if group == "own" else None

    # the pipes and the exit status are the Popen's
    stdin = property(lambda self: self.p.stdin)
    stdout = property(lambda self: self.p.stdout)
    stderr = property(lambda self: self.p.stderr)
    returncode = property(lambda self: self.p.returncode)

    def poll(self):
        return self.p.poll()

    def wait(self, timeout: float | None = None):
        return self.p.wait(timeout)

    def communicate(self, input=None, timeout: float | None = None):
        return self.p.communicate(input, timeout)

    def line(self) -> str:
        """The next line it prints, stripped of its newline; "" at end of output."""
        raw = self.p.stdout.readline()
        return (raw.decode(errors="replace") if isinstance(raw, bytes) else raw).strip()

    def signal(self, sig: int) -> str:
        """Signal the process itself. -> proc.signal_pid's outcome."""
        return proc.signal_pid(self.handle, sig)

    def signal_group(self, sig: int) -> str:
        """Signal its whole group, as a terminal does on Ctrl-C. Only a child started with group="own" leads one."""
        assert self.group is not None, "a child of its own group: start it with group='own'"
        return proc.signal_group(self.group, sig)

    def dead(self, timeout: float = 5.0) -> bool:
        """Whether it has ended within timeout."""
        try:
            self.p.wait(timeout)
            return True
        except subprocess.TimeoutExpired:
            return False

    def close(self):
        """Nothing it started outlives the test: its whole group when it leads one, then its pipes."""
        if self.poll() is None:
            if self.group is not None:
                proc.signal_group(self.group, signal.SIGKILL)
            else:
                proc.signal_pid(self.handle, signal.SIGKILL)
        self.p.wait()
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        for pipe in (self.p.stdin, self.p.stdout, self.p.stderr):
            if pipe is not None and not pipe.closed:
                pipe.close()


def start(argv: list, *, env: dict | None = None, cwd: str | None = None, group: str = "shared",
          ready: str | None = None, stdout=subprocess.PIPE, stderr=None, stdin=None, text: bool = True,
          bufsize: int = -1, pass_fds=(), close_fds: bool = True) -> Child:
    """A process beside the test. group "own": its own session, so the test can signal the whole group as a
    terminal does. ready: wait for that line before returning, so nothing acts on a child that is not up yet.
    The caller closes it (Child.close), or Journey.child does that for a story.

    close_fds False hands the child every descriptor it could inherit, which is what a test asking whether
    one of them survives an exec needs: with it on, the answer would be subprocess's and not the file's.

    The child's signals are put back the way pp puts a child's back: an ignored INT or TERM, and a block of
    either in the thread that starts it, both survive exec, so a suite run as a background job or a test that
    blocks them would hand that on and the child would never see the signal the test sends it."""
    p = subprocess.Popen(argv, env=env, cwd=cwd, stdout=stdout, stderr=stderr, stdin=stdin, text=text,
                         bufsize=bufsize, pass_fds=pass_fds, close_fds=close_fds,
                         start_new_session=group == "own", preexec_fn=proc._reset_ignored())
    c = Child(p, group)
    if ready is not None:
        said = c.line()
        if said != ready:
            c.close()
            raise AssertionError(f"{argv} said {said!r}, not {ready!r}: it never came up")
    return c


class Gate:
    """A step of a test that happens once, and the point one actor waits for another to pass.

    A test that runs a second actor at an exact step of a window needs two things, and neither can be left to
    a flag of the test's own: that the step happens once, however many times the function it is patched into
    is called and from however many threads, and that the condition it waits for and the step it then takes
    are one move, with nothing in between.

        gate = Gate()
        gate.once(requeue, when=lambda: (jobstore.find(jid) or {}).get("stop"))
        gate.passed("signalled")            # the point, said by one actor ...
        gate.wait("signalled")              # ... and waited for by another
        self.assertEqual(gate.ran, 1)       # how many times the step ran, in place of a flag to assert on

    `once(do, when=None)` takes the gate's lock, asks `when` under it, runs `do` where it holds it, and marks
    the gate done: every later call does nothing and says so. So the condition and the step are one move, and
    a call another thread makes meanwhile waits for the step rather than taking it too.

    A call the step itself makes, through the function the gate is patched into, is one of those later calls:
    the lock is this thread's already and the gate is done, so it does nothing and the step goes on. A step
    that reaches its own window again runs once, and never waits for itself.

    The rendezvous is the other half, and it keeps a lock of its own. `passed` waits for no step, and `wait`
    holds nothing a step needs, so the step of one actor may wait for another actor to pass a point: the
    shape above, with `do` waiting inside the gate while another thread passes it."""

    def __init__(self):
        self.ran = 0
        self._lock = threading.RLock()      # the step's, held across `when` and `do`; the step's own again
        self._mutex = threading.Lock()      # the rendezvous', which no step ever holds
        self._points: dict[str, threading.Event] = {}

    def once(self, do, when=None) -> bool:
        """Run `do` once, where this gate holds its lock and `when` holds. -> whether this call was the one
        that ran it."""
        with self._lock:
            if self.ran:
                return False
            if when is not None and not when():
                return False
            self.ran += 1
            do()
            return True

    def _point(self, name: str) -> threading.Event:
        with self._mutex:
            return self._points.setdefault(name, threading.Event())

    def passed(self, name: str = "") -> None:
        """Say the actor has reached this point, so a `wait` for it goes on. This waits for nothing."""
        self._point(name).set()

    def wait(self, name: str = "", timeout: float = 15) -> None:
        """Wait until another actor has passed that point, with `until`'s bound and its failure."""
        until(self._point(name).is_set, timeout=timeout, what=f"an actor to pass the gate's {name!r}")


@contextmanager
def interruptible():
    """SIGINT's default handler for the block, and the handler that was there back afterwards. What a test
    needs around a Ctrl-C it raises to itself (proc.raise_to_self), so that the signal raises
    KeyboardInterrupt.

    SIG_IGN survives exec, and CPython installs default_int_handler only where SIGINT was SIG_DFL at startup,
    so a suite run as a background job (`ci/check 3.11 & ci/check 3.14 & wait`) starts with SIGINT ignored.
    Without this the raised signal does nothing, and the test passes on nothing."""
    old = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, old)


def until(pred, timeout: float = 15, every: float = 0.1, what: str | None = None):
    """pred's first true value; a failure if it is not true within timeout, so a test never goes on from a state
    it only hoped for. The failure names what was waited for (`what`, or the predicate) and pred's last answer.

    This is the one place a test waits, and the sleep in it is the only one: waiting for a while instead says
    nothing when it is long enough on this machine and too short on a busier one."""
    end = time.monotonic() + timeout
    while True:
        v = pred()
        if v:
            return v
        if time.monotonic() > end:
            raise AssertionError(f"not true within {timeout:g}s: {what or pred.__qualname__} (last: {v!r})")
        time.sleep(every)


def kill_pid(pid: int) -> None:
    """End a process a test knows only by pid, one its own child forked, if it is still there."""
    h = proc.Handle.live(pid)
    if h is not None:
        proc.signal_pid(h, signal.SIGKILL)


def replace_file(path: str, text: str) -> str:
    """Write path in one step (a temporary file, then a rename): a worker reading it meanwhile, as pp's do every
    loop, sees the old file or the new one, never an empty or half-written one."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".new", "w") as f:
        f.write(text)
    os.replace(path + ".new", path)
    return path


def _pids() -> list[int]:
    return [int(n) for n in os.listdir("/proc") if n.isdigit() and int(n) != os.getpid()]


def has_env(pid: int, entry: bytes) -> bool:
    try:
        with open(f"/proc/{pid}/environ", "rb") as f:
            return entry in f.read().split(b"\0")
    except OSError:         # gone, or not ours
        return False


def _kill_if(pid: int, entry: bytes) -> None:
    """SIGKILL pid if it is still a process carrying entry. Through pp's own pidfd: the check and the kill reach
    the same process even if pid ends and its number is given to another in between (the kill then finds
    nobody)."""
    fd = proc.pidfd(pid)
    if fd is None:
        return
    try:
        if has_env(pid, entry):
            proc.pidfd_signal(fd, signal.SIGKILL)
    except OSError:
        pass
    finally:
        os.close(fd)


class Journey(unittest.TestCase):
    """self.home is the sandbox's HOME; self.work its cwd. self.pp(...) runs pp and, unless code=None, asserts
    the exit code, printing the whole exchange on a mismatch."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="ppj-")
        self.addCleanup(self._tmp.cleanup)
        t = self.tmp = os.path.realpath(self._tmp.name)
        self.home, self.work, self.bin = f"{t}/home", f"{t}/work", f"{t}/bin"
        for d in (self.home, self.work, self.bin):
            os.makedirs(d)
        env = session.scrubbed_env()
        for k in [k for k in env if k.startswith(("PP_", "XDG_", "GIT_"))]:
            del env[k]
        env.update(HOME=self.home, PATH=f"{self.bin}:{env['PATH']}", NO_COLOR="1", TERM="dumb",
                   PP_CONFIG_DIR=f"{self.home}/.config/pp", PP_STATE_DIR=f"{self.home}/.local/state/pp",
                   PP_DATA_DIR=f"{self.home}/.local/share/pp",
                   # the global config sits outside HOME, which a step may assert is empty; git reads nothing
                   # where there is no file
                   **sandbox.git_env(f"{t}/gitconfig"))
        self.env = env
        self.tool("nvidia-smi", "exit 9\n")         # no GPU, until self.gpus(...)
        self.addCleanup(self._stop_workers)

    # -- running things --------------------------------------------------------------------------------------
    def pp(self, *args, code: int | None = 0, input: str | None = None, cwd: str | None = None,
           env: dict | None = None, timeout: float = 60) -> subprocess.CompletedProcess:
        r = subprocess.run([sys.executable, PP, *args], capture_output=True, text=True, input=input,
                           cwd=cwd or self.work, env=env or self.env, timeout=timeout)
        if code is not None and r.returncode != code:
            self.fail(f"pp {shlex.join(args)} -> exit {r.returncode}, wanted {code}\n"
                      f"--- stdout\n{r.stdout}--- stderr\n{r.stderr}")
        return r

    def child(self, argv: list, *, cwd: str | None = None, env: dict | None = None, group: str = "shared",
              ready: str | None = None, **kw) -> Child:
        """A process beside the story, in its sandbox, gone when the story ends. ready: the line it prints when
        it is up, waited for here, so no step acts on it before it says so."""
        c = start(argv, env=env or self.env, cwd=cwd or self.work, group=group, ready=ready, **kw)
        self.addCleanup(c.close)
        return c

    def spawn(self, *args, cwd: str | None = None, env: dict | None = None) -> Child:
        """pp in the background (a server, a `watch`); gone at cleanup."""
        return self.child([sys.executable, PP, *args], cwd=cwd, env=env, stderr=subprocess.PIPE)

    def sh(self, cmd: str, cwd: str | None = None, check: bool = True) -> str:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd or self.work, env=self.env,
                           timeout=60)
        if check and r.returncode:
            self.fail(f"$ {cmd} -> exit {r.returncode}\n{r.stdout}{r.stderr}")
        return r.stdout

    # -- arranging the world ---------------------------------------------------------------------------------
    def write(self, rel: str, text: str, mode: int = 0o644) -> str:
        """Write a file; rel is under the sandbox root ("home/...", "work/...") unless absolute."""
        path = rel if os.path.isabs(rel) else os.path.join(self.tmp, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(text)
        os.chmod(path, mode)
        return path

    @staticmethod
    def read(path: str) -> str:
        with open(path) as f:
            return f.read()

    def config(self, text: str) -> str:
        """Add to the sandbox's ~/.config/pp/config.toml. Adding a table header that is already there (a second
        [http]) makes the file invalid TOML and every pp command fail: rewrite the file instead."""
        path = f"{self.env['PP_CONFIG_DIR']}/config.toml"
        old = self.read(path) if os.path.exists(path) else ""
        return replace_file(path, old + text)

    def tool(self, name: str, script: str) -> str:
        """A fake executable on the sandbox's PATH (nvidia-smi, ...)."""
        return self.write(f"bin/{name}", "#!/bin/sh\n" + script, 0o755)

    def gpus(self, *cards: tuple[int, int]) -> None:
        """Fake nvidia-smi: one (total_gb, used_gb) per card."""
        rows = "\n".join(f"{i}, Fake GPU, {t * 1024}, {u * 1024}, 0" for i, (t, u) in enumerate(cards))
        self.tool("nvidia-smi", f"cat <<'EOF'\n{rows}\nEOF\n")

    def git_repo(self, rel: str, files: dict[str, str]) -> str:
        path = os.path.join(self.tmp, rel)
        for name, text in files.items():
            self.write(os.path.join(path, name), text, 0o755 if text.startswith("#!") else 0o644)
        self.sh("git init -q -b main && git add -A && git commit -qm init", cwd=path)
        return path

    # -- waiting ---------------------------------------------------------------------------------------------
    @staticmethod
    def until(pred, timeout: float = 15, every: float = 0.1, what: str | None = None):
        """pred's first true value; a failure if it is not true within timeout, so a story never goes on from a
        state it only hoped for."""
        return until(pred, timeout, every, what)

    def _stop_workers(self):
        """Nothing the story started outlives it. Every process it started carries the sandbox's PP_STATE_DIR in
        its environment, and so does every process those start: queue workers (one still starting, before it has
        its claim, too), their jobs and hooks. Every process carrying the value is killed, and the look repeats
        until it finds none. Only those processes start more of them, so once a look finds none, none can appear
        while the sandbox is removed.

        A worker needs no stop of its own first: being up is the claim it holds, which the kernel lets go of
        when it dies, so one that is killed here leaves nothing behind that says it is still up."""
        mark = f"PP_STATE_DIR={self.env['PP_STATE_DIR']}".encode()

        def nothing_of_this_story_is_left():
            """One look: kill what carries the mark, and say whether the look found none."""
            left = [pid for pid in _pids() if has_env(pid, mark)]
            for pid in left:
                _kill_if(pid, mark)
            return not left
        self.until(nothing_of_this_story_is_left, timeout=10, every=0.05,
                   what=f"every process carrying {mark.decode()} ended")
