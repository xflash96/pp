"""Processes: reading them from /proc (Linux, no psutil/lsof/ss needed), starting them, and signalling them.

Signalling a process, starting one and judging one alive by its pid go through these, and nothing else in pp
does them (tests/test_constructs.py reads the source and fails on a call that does):

    Handle, signal_pid, signal_group, signal_and_wait, raise_to_self   the only way pp signals anything
    pidfd, pidfd_signal                                                the fd one of those signals goes through
    spawn, run, git, git_child, exec_replace                           the only way pp starts a process
    alive                                                              whether a pid is a live process

The rest are helpers, which any caller may use: `say_ready`, how a child of spawn(ready=True) says its own
setup is done; `read`, `processes`, `ancestors`, `descendants`, `members` and `uptime`, what /proc says about
a process; and `sockets`, `socket_owners` and `port_owners`, what it says about who holds a port.

The one rule that motivates the reading: a pattern match must never include the caller's own ancestry. `pkill -f
PAT` run from a shell matches that shell, because PAT is in its command line. ancestors() is the set every
pattern search here excludes.
"""
from __future__ import annotations

import os
import select
import signal
import socket
import subprocess
import time
from dataclasses import dataclass, replace

from pp.lib import session

CLK_TCK = os.sysconf("SC_CLK_TCK")


@dataclass
class Proc:
    pid: int
    ppid: int
    pgid: int           # its process group: the pid of the group's leader
    comm: str
    cmdline: list
    uid: int
    state: str
    start: int          # starttime in clock ticks since boot; with pid, a process identity that survives pid reuse
    rss_kb: int

    @property
    def cmd(self) -> str:
        return " ".join(self.cmdline) if self.cmdline else f"[{self.comm}]"

    def age(self) -> float:
        return uptime() - self.start / CLK_TCK


def uptime() -> float:
    with open("/proc/uptime") as f:
        return float(f.read().split()[0])


_BOOT: str | None = None


def boot_id() -> str:
    """This boot, as the kernel names it (/proc/sys/kernel/random/boot_id).

    A pid and the start time read beside it name a process within one boot and no further, so whatever keeps
    them past the life of the process that read them keeps this too. Read once; "" where the kernel does not
    say, which is then the same for everything this process records."""
    global _BOOT
    if _BOOT is None:
        try:
            with open("/proc/sys/kernel/random/boot_id") as f:
                _BOOT = f.read().strip()
        except OSError:
            _BOOT = ""
    return _BOOT


def _stat(pid: int) -> tuple[str, list[str]] | None:
    """/proc/<pid>/stat as (comm, the fields after it), or None when there is no such process. The one parser of
    that line: comm is parenthesised and may itself hold spaces and parens, so it is cut at the LAST ')'."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            stat = f.read().decode(errors="replace")
        lp, rp = stat.index("("), stat.rindex(")")
    except (FileNotFoundError, ProcessLookupError, PermissionError, NotADirectoryError, ValueError):
        return None
    return stat[lp + 1:rp], stat[rp + 2:].split()


# the fields of /proc/<pid>/stat after comm, counted as `man 5 proc` numbers them: state is field 3
_STATE, _PPID, _PGID, _START, _RSS = 0, 1, 2, 19, 21
DEAD = ("Z", "X")       # a zombie or a reaped process: a pid, no longer a process


def read(pid: int) -> Proc | None:
    got = _stat(pid)
    if got is None:
        return None
    comm, rest = got
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
        uid = os.stat(f"/proc/{pid}").st_uid
    except (FileNotFoundError, ProcessLookupError, PermissionError, NotADirectoryError):
        return None
    page_kb = os.sysconf("SC_PAGE_SIZE") // 1024
    cmdline = [a.decode(errors="replace") for a in raw.split(b"\0") if a] if raw else []
    return Proc(pid=pid, ppid=int(rest[_PPID]), pgid=int(rest[_PGID]), comm=comm, cmdline=cmdline, uid=uid,
                state=rest[_STATE], start=int(rest[_START]), rss_kb=int(rest[_RSS]) * page_kb)


def processes() -> dict[int, Proc]:
    out = {}
    for name in os.listdir("/proc"):
        if name.isdigit():
            p = read(int(name))
            if p:
                out[p.pid] = p
    return out


def ancestors(pid: int | None = None, procs: dict[int, Proc] | None = None) -> list[int]:
    """pid (default: this process) and every ancestor up to init."""
    pid = os.getpid() if pid is None else pid
    chain = []
    while pid > 0 and pid not in chain:
        chain.append(pid)
        p = procs.get(pid) if procs is not None else read(pid)
        if not p:
            break
        pid = p.ppid
    return chain


def descendants(pid: int, procs: dict[int, Proc] | None = None) -> list[int]:
    """Every descendant of pid (not pid itself), parents before children."""
    procs = procs if procs is not None else processes()
    kids: dict[int, list[int]] = {}
    for p in procs.values():
        kids.setdefault(p.ppid, []).append(p.pid)
    out, todo = [], list(kids.get(pid, []))
    while todo:
        q = todo.pop(0)
        if q in out:
            continue
        out.append(q)
        todo.extend(kids.get(q, []))
    return out


def alive(pid: int, start: int | None = None) -> bool:
    """pid exists, is not a zombie, and (when start is given) is still the same process."""
    p = read(pid)
    if not p or p.state in DEAD:
        return False
    return start is None or p.start == start


def members(pgid: int) -> list[int]:
    """The pids in process group pgid that are still running. A zombie is not one of them: it holds a pid, and no
    signal reaches it."""
    out = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        got = _stat(int(name))
        if got and int(got[1][_PGID]) == pgid and got[1][_STATE] not in DEAD:
            out.append(int(name))
    return out


# ---- sockets --------------------------------------------------------------------------------------

_TCP_LISTEN = "0A"


def _hex_addr(s: str) -> tuple[str, int]:
    host, port = s.split(":")
    if len(host) == 8:
        return socket.inet_ntop(socket.AF_INET, bytes.fromhex(host)[::-1]), int(port, 16)
    # IPv6: four 32-bit words, each in host (little-endian) byte order
    raw = b"".join(bytes.fromhex(host[i:i + 8])[::-1] for i in range(0, 32, 8))
    return socket.inet_ntop(socket.AF_INET6, raw), int(port, 16)


def sockets(port: int | None = None, listening_only: bool = True) -> list[dict]:
    """Sockets from /proc/net/{tcp,tcp6,udp,udp6}: [{proto, addr, port, inode}]. TCP: LISTEN only unless asked."""
    out = []
    for proto in ("tcp", "tcp6", "udp", "udp6"):
        try:
            with open(f"/proc/net/{proto}") as f:
                lines = f.read().splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            parts = line.split()
            addr, lport = _hex_addr(parts[1])
            if port is not None and lport != port:
                continue
            if proto.startswith("tcp") and listening_only and parts[3] != _TCP_LISTEN:
                continue
            if proto.startswith("udp") and listening_only and int(parts[2].split(":")[1], 16) != 0:
                continue    # a connected udp socket (remote port set) is a client, not a listener
            out.append({"proto": proto, "addr": addr, "port": lport, "inode": int(parts[9])})
    return out


def socket_owners(inodes: set[int]) -> dict[int, set[int]]:
    """inode -> pids holding it. Only processes we may read /proc/<pid>/fd of (our own, unless root)."""
    want = {f"socket:[{i}]": i for i in inodes}
    out: dict[int, set[int]] = {}
    if not want:
        return out
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        fd_dir = f"/proc/{name}/fd"
        try:
            fds = os.listdir(fd_dir)
        except OSError:
            continue
        for fd in fds:
            try:
                target = os.readlink(f"{fd_dir}/{fd}")
            except OSError:
                continue
            if target in want:
                out.setdefault(want[target], set()).add(int(name))
    return out


def port_owners(port: int) -> tuple[list[dict], set[int]]:
    """(listening sockets on port, pids holding them). Sockets with no visible pid belong to another user."""
    socks = sockets(port)
    owners = socket_owners({s["inode"] for s in socks})
    for s in socks:
        s["pids"] = sorted(owners.get(s["inode"], set()))
    return socks, {p for s in socks for p in s["pids"]}


# ---- signalling -----------------------------------------------------------------------------------
#
# Every signal pp sends goes through a handle, and nothing else in pp calls kill, killpg or pidfd_send_signal
# (tests/test_constructs.py keeps it that way). A pid on its own does not name a process: the kernel hands it out
# again once the process it named is reaped, so a pid read a moment ago can belong to something else by the time a
# signal is sent. A handle carries the start time read with the pid, and a signal goes out only while both match.

_LOOK = 0.05                # between looks, while waiting for a target to end
_KILL_WAIT = 2.0            # how long a SIGKILL is given before its target is reported alive
_WRAPPED = hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal")    # this Python wraps the calls
_PIDFD_OPEN, _PIDFD_SEND_SIGNAL = 434, 424      # their syscall numbers, the same on every Linux pp runs on


def _syscall(nr: int, *args) -> int:
    """A syscall by its number, for a Python built without the wrapper for it. A failure is an OSError, as the
    wrapper's would be."""
    import ctypes          # imported here: every command imports this module, and only this path needs ctypes
    r = ctypes.CDLL(None, use_errno=True).syscall(nr, *args)
    if r < 0:
        raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))
    return r


def pidfd(pid: int) -> int | None:
    """An fd that holds the process pid names now. The kernel keeps that fd on that process, so a signal sent
    through it reaches that process or nobody, whatever becomes of the pid. None where the kernel refuses one
    (a container whose seccomp profile has no pidfd_open). A Python built without the calls gets them from
    libc, so this is pp's path on every kernel pp runs on, not only on the builds that wrap them."""
    try:
        return os.pidfd_open(pid) if _WRAPPED else _syscall(_PIDFD_OPEN, pid, 0)
    except OSError:
        return None


def pidfd_signal(fd: int, sig: int) -> None:
    """Send sig through a pidfd, which pidfd() describes."""
    if _WRAPPED:
        signal.pidfd_send_signal(fd, sig)
    else:
        _syscall(_PIDFD_SEND_SIGNAL, fd, sig, None, 0)


@dataclass(frozen=True)
class Handle:
    """What pp signals: a pid, or a process group's id, with the start time read together with it.

    group: the id is a process group's. It is signalled with killpg, and it is gone when no member is left.
    fd: a pidfd for a process pp started itself. The kernel keeps that fd on that process, so a signal sent
    through it reaches that process or nobody, whatever became of the pid.
    """

    id: int
    start: int
    group: bool = False
    fd: int | None = None

    def __post_init__(self):
        if not isinstance(self.id, int) or self.id <= 0 or not isinstance(self.start, int):
            raise ValueError(f"a handle is a pid and the start time read with it, not ({self.id!r}, {self.start!r})")

    @classmethod
    def live(cls, pid: int, *, group: bool = False, fd: int | None = None) -> Handle | None:
        """A handle for a process that is there now, its start time read with it. None when it is not there."""
        p = read(pid)
        return None if p is None else cls(pid, p.start, group=group, fd=fd)

    @classmethod
    def recorded(cls, id: int | None, start: int | None, *, group: bool = False) -> Handle | None:
        """A handle from a record written earlier: a job's process group, a worker's pid file. None when either
        half is missing. Such a record names no process, so signalling it would mean signalling whatever holds
        that pid now."""
        if not id or start is None:
            return None
        return cls(int(id), int(start), group=group)

    @classmethod
    def running(cls, id: int | None, start: int | None) -> Handle | None:
        """A handle from such a record, where the process it names is running now: the same pid, the same
        start time, and not a zombie. None where the record names no process, or one that has gone.

        What a reader asks when the question is whether a record is a live process's own, rather than what to
        signal: a record that names a process that is gone is a record nobody is writing any more."""
        h = cls.recorded(id, start)
        return h if h is not None and alive(h.id, h.start) else None


def raise_to_self(sig: int) -> None:
    """Deliver sig to this process now: its handler runs, or its default action ends the process."""
    os.kill(os.getpid(), sig)


def _reused(h: Handle) -> bool:
    """A live process holds the handle's id with another start time: the number has been handed on."""
    p = read(h.id)
    return p is not None and p.start != h.start


def _there(h: Handle) -> bool:
    """The handle's target is still there: the process itself, or any member of the group."""
    return bool(members(h.id)) if h.group else alive(h.id, h.start)


def _send(h: Handle, sig: int) -> str | None:
    """Send sig to the handle's target: None when it was sent, else the outcome saying why it was not."""
    if h.group:
        # a group's id is the pid of its leader, and Linux never hands out a pid that is still a group id. A
        # process holding that pid with another start time therefore means the group is gone and the number
        # reused, while no process holding it at all leaves members of the group that may still be running.
        if _reused(h):
            return "gone"
    elif not alive(h.id, h.start):
        # with a pidfd the identity is read after that fd is open, and the fd holds one process, so nothing can
        # come between what was read here and where the signal goes
        return "gone"
    try:
        if h.group:
            os.killpg(h.id, sig)
        elif h.fd is not None:
            pidfd_signal(h.fd, sig)
        else:
            os.kill(h.id, sig)
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        return "denied"
    except OSError:
        if h.fd is None:
            raise
        return _send(Handle(h.id, h.start, group=h.group), sig)    # no pidfd here: the pinned pid instead
    return None


def _wait_gone(h: Handle, grace: float) -> bool:
    """Whether the target is gone within grace seconds."""
    deadline = time.monotonic() + max(grace, 0)
    while True:
        if not _there(h):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(_LOOK)


def signal_pid(h: Handle, sig: int = signal.SIGTERM) -> str:
    """Signal one process: it goes out, and this returns. -> one of three outcomes:

    alive   it was signalled and is still there    gone    it was not there to signal
    denied  pp is not allowed to signal it

    Nothing waits here. A caller that waits for its targets to end, and SIGKILLs the ones that do not, asks
    `signal_and_wait` for one window over all of them; a worker waits for its own jobs a tick at a time
    (`jobq.decide`), so no wait of its is inside a signal."""
    if h.group:
        raise ValueError("a group handle: signal_group signals a process group")
    return _send(h, sig) or "alive"


def signal_group(h: Handle, sig: int = signal.SIGTERM) -> str:
    """Signal every process in a group (a job, which pp starts in a session of its own), with signal_pid's
    outcomes. A group is gone when no member is left, so a leader that exited while its children run is alive."""
    if not h.group:
        raise ValueError("a process handle: signal_pid signals one process")
    return _send(h, sig) or "alive"


def signal_and_wait(pids: list[int], sig: int = signal.SIGTERM, grace: float = 3.0, escalate: bool = True,
                    starts: dict[int, int] | None = None) -> dict[int, str]:
    """Signal each pid, wait up to grace seconds for them together, then SIGKILL the survivors when escalate.
    -> {pid: outcome}: signal_pid's three, and two more for the waiting this does, `exited` for one that ended
    after the signal and `killed` for one that needed the SIGKILL. starts pins the identities; a pid with no
    start is read now, and one that is gone is never signalled. What `pp kill` sends: the grace is one window
    for every target, so a long list of them is not a long wait."""
    starts = starts or {}
    res: dict[int, str] = {}
    sent: list[Handle] = []
    for pid in pids:
        h = Handle.recorded(pid, starts.get(pid)) or Handle.live(pid)
        if h is None:
            res[pid] = "gone"
            continue
        r = _send(h, sig)
        if r is not None:
            res[pid] = r
        else:
            sent.append(h)
    deadline = time.monotonic() + max(grace, 0)
    pending = list(sent)
    while pending and time.monotonic() < deadline:
        pending = [h for h in pending if _there(h)]
        if pending:
            time.sleep(_LOOK)
    for h in sent:
        if h not in pending:
            res[h.id] = "exited"
    for h in pending:
        if not _there(h):
            res[h.id] = "exited"
        elif not escalate or sig == signal.SIGKILL:
            res[h.id] = "alive"
        else:
            r = _send(h, signal.SIGKILL)
            if r == "gone":
                res[h.id] = "exited"
            elif r is not None:
                res[h.id] = r
            else:
                res[h.id] = "killed" if _wait_gone(h, _KILL_WAIT) else "alive"
    return res


# ---- starting a process ---------------------------------------------------------------------------
#
# Every process pp starts goes through spawn, or through run, git, git_child and exec_replace, which are spawn
# with git's decisions or the waiting done. Nothing else in pp calls subprocess or os.exec
# (tests/test_constructs.py keeps it that way).
# spawn's first three keywords have no default, so no call site is silent about the group its child runs in, what
# that child can read, or the environment it is given.

PIPE, STDOUT, DEVNULL = subprocess.PIPE, subprocess.STDOUT, subprocess.DEVNULL
READY_FD = "PP_READY_FD"        # ready=True: the fd the child writes one byte to once its own setup is done
GIT_ENV = {"GIT_TERMINAL_PROMPT": "0"}      # git's own prompt is off; ssh's passphrase prompt is not git's
GIT_WAIT = 300.0                # how long any one git command may take
DRAIN_WAIT = 2.0                # how long an ended child's pipes are read before they are closed (run)
_STDIN = {"none": DEVNULL, "pipe": PIPE}


@dataclass
class Result:
    """What a finished process left behind. returncode is None when it never finished: its time ran out and pp
    ended it (timed_out), or it could not be started at all. stdout and stderr are what pp captured, as text
    unless the caller asked for bytes."""

    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool = False


_ENDING = {signal.SIGINT, signal.SIGTERM}


def _default_signals():
    """In the child, between fork and exec: INT and TERM back to their default action, and not blocked. SIG_IGN
    and the signal mask both survive exec, so a pp whose own caller ignores a signal (pp started from a
    background shell, or a suite run as a background job), or a thread that blocks them (`pp http` serves with
    both blocked), would hand that on, and the child would never see a Ctrl-C or a `pp kill`. A handler does not
    survive exec, so only an ignored or blocked signal has to be undone."""
    for s in _ENDING:
        signal.signal(s, signal.SIG_DFL)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, _ENDING)


def _reset_ignored():
    """_default_signals where this process ignores INT or TERM, or this thread blocks one, else None, so no code
    runs between fork and exec unless there is something to undo."""
    ignored = any(signal.getsignal(s) is signal.SIG_IGN for s in _ENDING)
    return _default_signals if ignored or _ENDING & signal.pthread_sigmask(signal.SIG_BLOCK, ()) else None


class Child:
    """A process pp started: its pipes, its exit status, and the handles that signal it.

    `handle` is the process; `group` is its whole process group, where it leads one of its own. end() prefers
    the group, so a shell's children end with the shell.
    """

    def __init__(self, p: subprocess.Popen, group: str):
        self.p = p
        self.pid = p.pid
        now = read(p.pid)       # read once, while the process is certainly there: pp holds it until it waits
        self.handle = Handle(p.pid, now.start) if now else None
        self.group = Handle(p.pid, now.start, group=True) if now and group == "own" else None

    stdin = property(lambda self: self.p.stdin)
    stdout = property(lambda self: self.p.stdout)
    stderr = property(lambda self: self.p.stderr)
    returncode = property(lambda self: self.p.returncode)

    def poll(self) -> int | None:
        return self.p.poll()

    def wait(self, timeout: float | None = None) -> int:
        return self.p.wait(timeout)

    def communicate(self, input=None, timeout: float | None = None):
        return self.p.communicate(input, timeout)

    def close_pipes(self):
        """Close pp's ends of its pipes, once pp has stopped reading them. communicate() does this itself."""
        for pipe in (self.p.stdin, self.p.stdout, self.p.stderr):
            if pipe is not None and not pipe.closed:
                pipe.close()

    def end(self, sig: int = signal.SIGTERM) -> str:
        """Signal it: its whole group where it leads one, so nothing it started runs on. -> signal_pid's
        outcome, which says nothing about whether it ended: `wait(timeout)` is how a caller waits for that.

        A process pp holds goes through a pidfd, opened for the signal and closed after it: the kernel keeps that
        fd on this process, so the signal cannot reach another that was given its pid."""
        if self.group is not None:
            return signal_group(self.group, sig)
        if self.handle is None:
            return "gone"
        fd = pidfd(self.pid)
        try:
            return signal_pid(self.handle if fd is None else replace(self.handle, fd=fd), sig)
        finally:
            if fd is not None:
                os.close(fd)


def spawn(argv: list, *, group: str, stdin: str, env, cwd: str | None = None, stdout=None, stderr=None,
          add_env: dict | None = None, text: bool = False, ready: bool = False,
          ready_wait: float = 60.0) -> Child:
    """Start argv. Three things every caller states, which is why they have no default:

    group   "own": a session of its own, so no Ctrl-C in pp's terminal reaches it and its whole group can be
            signalled together (a job, a hook, a worker, anything pp must be able to end itself). "shared": it
            stays in pp's group and terminal, so a Ctrl-C reaches it as well (a gate's check, a batch command).
    stdin   "none": /dev/null, so it can never read pp's input nor stop waiting for input. "pipe": pp writes
            to it. An open fd: that file, as the child's input. The child is given the same open file, so a
            lock held on it is the child's for as long as the child lives, and pp closes its own copy
            afterwards (a job's run claim). Nothing pp starts is given pp's own input.
    env     "scrubbed": pp's environment without the calling agent session, for anything pp starts on its own.
            "inherit": pp's environment as it is. A dict: exactly that environment. add_env goes on top.

    ready: return only once the child has written a byte to the fd named by $PP_READY_FD, which it does when its
    own setup is done, or once that pipe is at end of file because the child ended first. Without it spawn
    returns once the child has exec'd, which says nothing about what the child has done since: a handler it
    installs is not installed, a port it binds is not bound, and its command line may still read empty.
    """
    if group not in ("own", "shared"):
        raise ValueError(f'group {group!r}: "own" or "shared"')
    if not isinstance(stdin, int) and stdin not in _STDIN:
        raise ValueError(f'stdin {stdin!r}: "none", "pipe", or an open fd')
    if env == "scrubbed":
        envv = session.scrubbed_env()
    elif env == "inherit":
        envv = dict(os.environ)
    elif isinstance(env, dict):
        envv = dict(env)
    else:
        raise ValueError(f'env {env!r}: "scrubbed", "inherit", or the environment to give it')
    envv.update(add_env or {})
    r = w = None
    try:
        if ready:
            r, w = os.pipe()
            envv[READY_FD] = str(w)
        p = subprocess.Popen(argv, cwd=cwd, env=envv,
                             stdin=stdin if isinstance(stdin, int) else _STDIN[stdin], stdout=stdout, stderr=stderr,
                             text=text, start_new_session=group == "own",
                             preexec_fn=_reset_ignored(), pass_fds=() if w is None else (w,),
                             **({"errors": "replace"} if text else {}))
    except BaseException:
        for fd in (r, w):
            if fd is not None:
                os.close(fd)
        raise
    if w is not None:
        os.close(w)             # pp's copy: with only the child's left, the child's end closes the pipe
    c = Child(p, group)
    if r is not None:
        try:
            if not select.select([r], [], [], ready_wait)[0]:
                c.end(signal.SIGKILL)       # and waited for, as run() does: nothing pp killed is left a zombie
                c.wait()
                c.close_pipes()
                raise TimeoutError(f"{argv[0]} did not say it was ready within {ready_wait:g}s")
            os.read(r, 1)
        finally:
            os.close(r)
    return c


def _drained(c: Child, text: bool) -> tuple:
    """End the child and read what is left in its pipes, for at most DRAIN_WAIT, then close them. -> what was
    read by then.

    The bound is what a child sharing pp's group makes necessary: it is ended by itself, so the children it
    started run on, and they hold the write ends of these pipes. Reading to the end of them would wait for a
    process pp has no handle on (git's ssh, sitting on a passphrase prompt)."""
    c.end(signal.SIGKILL)
    try:
        out, err = c.communicate(timeout=DRAIN_WAIT)
    except subprocess.TimeoutExpired as late:
        out, err = late.output, late.stderr     # what was read, as bytes even where the caller asked for text
        if text:
            out, err = (s.decode(errors="replace") if isinstance(s, bytes) else s for s in (out, err))
        c.close_pipes()         # the rest stays unread: nothing pp may end holds the other side
        c.wait()                # the child itself took the SIGKILL
    return out or "", err or ""


def run(argv: list, *, group: str, stdin: str, env, cwd: str | None = None, add_env: dict | None = None,
        stdout=PIPE, stderr=PIPE, text: bool = True, timeout: float | None = None) -> Result:
    """Start argv, wait for it, and return what it left. spawn's keywords, and:

    timeout: when its time runs out the child is ended, and Result.timed_out says so instead of an exception.
    What is ended is its whole group, where it has one of its own, so a shell's children go with it; a child
    that shares pp's group is ended by itself, since that group holds pp as well. An interruption while pp
    waits, such as a Ctrl-C, ends the child the same way: nothing pp started is left behind.

    A shared child's own children are not ended with it, and they keep the write ends of its pipes: `pp cmd
    add <ssh-url>` runs git in pp's group so ssh can ask for a passphrase there, and that ssh outlives the
    git pp killed. The pipes are therefore read for at most DRAIN_WAIT seconds after the child is ended and
    then closed (_drained), so what a run that timed out returns is what pp had read by then, and no process
    holding a pipe can hold the run.
    """
    c = spawn(argv, group=group, stdin=stdin, env=env, cwd=cwd, add_env=add_env,
              stdout=stdout, stderr=stderr, text=text)
    try:
        out, err = c.communicate(timeout=timeout)
        return Result(c.returncode, out or "", err or "")
    except subprocess.TimeoutExpired:
        return Result(None, *_drained(c, text), timed_out=True)
    except BaseException:
        c.end(signal.SIGKILL)
        c.close_pipes()         # nothing reads what is left: this run is ending, and the read has no bound
        c.wait()
        raise


def git(*args: str, group: str, cwd: str | None = None, timeout: float = GIT_WAIT) -> Result:
    """git, the one way pp runs it: nothing on its stdin, git's own prompt off, and a bound on how long it may
    take. returncode None: it is not installed, or its time ran out (timed_out says which).

    group has no default, because it decides whether git can reach a terminal. "shared": git stays in pp's
    session, so ssh can ask there for a key passphrase or a host key, which is what a remote over ssh may need.
    "own": a session of its own, which has no terminal, so nothing it runs can ask the person anything; pp ends
    it on a timeout or an interruption rather than leaving it behind.
    """
    try:
        return run(["git", *args], group=group, stdin="none", env="inherit", add_env=GIT_ENV, cwd=cwd,
                   timeout=timeout)
    except FileNotFoundError:
        return Result(None, "", "git is not installed")


def git_child(*args: str, cwd: str | None = None) -> Child:
    """git whose output pp reads as it comes, an archive piped into tar. It decides what git() decides, bar one:
    nothing on its stdin, git's own prompt off, and a session of its own, so it reaches no terminal and pp can
    end it. How long it may take is not decided here: pp is reading this child, so the reader bounds the time
    and ends it."""
    return spawn(["git", *args], group="own", stdin="none", env="inherit", add_env=GIT_ENV, cwd=cwd,
                 stdout=PIPE, stderr=PIPE)


def say_ready() -> None:
    """Tell the pp that started this process with ready=True that this process's own setup is done: one byte to
    the fd it named in $PP_READY_FD. Nothing happens when nobody is waiting, so a process started by hand runs
    the same way. The variable is dropped afterwards, so nothing this process starts inherits it."""
    fd = os.environ.pop(READY_FD, "")
    if not fd.isdigit():
        return
    try:
        os.write(int(fd), b"1")
        os.close(int(fd))
    except OSError:
        pass            # whoever waited for it has gone; this process goes on


def exec_replace(path: str, argv: list) -> None:
    """Become another program: pp's pid, group and terminal stay as they are, and nothing after this runs. What
    an external pp-NAME command gets, so it behaves as if the user had run it."""
    os.execv(path, [path] + argv)
