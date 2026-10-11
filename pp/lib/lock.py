"""The two lock constructs, and nothing else in pp takes a lock (tests/test_constructs.py reads the source and
fails on a call that does):

    locked(path)        how pp keeps two processes out of each other's way for the length of a scope
    Claim.take(path)    how one process says it is the one doing something, for as long as it lives

`deferring()` is the part of `locked` that holds the signals back, for a scope that has to be whole where no lock
is involved. Each docstring is the contract.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
import signal
import struct
import threading
from contextlib import contextmanager


class Busy(Exception):
    """What `locked(block=False)` raises when another holder has the lock. The caller turns it into its own
    message, since only the caller knows what is held and what to tell the user."""


_files: dict[str, list] = {}        # lock file -> [thread lock, fd, depth, shared]
_mutex = threading.Lock()


@contextmanager
def locked(path: str, *, block: bool = True, defer: bool = False, shared: bool = False):
    """Hold the lock file at path for the block, and release it on the way out.

    The file is created if it is missing, and its directory with it; nothing is ever written in it. One holder
    at a time: another process waits for it, and with block=False gets `Busy` at once instead. The thread
    holding it may take it again, so a scope nested in another is safe; another thread of this process waits
    like another process does.

    With shared, several processes read at once, and a holder that wants it to itself waits for them. Two
    threads of one process still take turns: the thread lock above the file is taken whether the hold is
    shared or not, because the helper keeps one fd per path, and one fd holds one kind of lock. A scope that
    writes cannot start inside one that reads: the hold would have to be let go of to be made exclusive, and
    what was read before it would then be older than the store. That raises instead.

    With defer, the signals `deferring` holds back are delivered when the block ends, to the handlers in
    place before it, so a change made under the lock is never left half done. A Ctrl-C while waiting for the
    lock still stops the command.

    No code holds two of these locks at once, which is a convention and not a check: `pp promote` takes the
    registry's lock and releases it before it takes a target's own (`pp/commands/promote.py`), and one that
    ever has to hold both takes the registry's first. One lock is taken inside another in pp, and only one: a
    job's run claim inside the store's transaction, which `pp/lib/jobq.py`'s `txn` states, and a claim is
    taken without waiting.
    """
    path = os.path.abspath(path)
    with _mutex:
        state = _files.setdefault(path, [threading.RLock(), None, 0, False])
    if not state[0].acquire(blocking=block):
        raise Busy(path)
    try:
        if state[2] == 0:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
            how = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
            try:
                fcntl.flock(fd, how if block else how | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                raise Busy(path) from None
            except BaseException:
                os.close(fd)
                raise
            state[1], state[3] = fd, shared
        elif state[3] and not shared:
            raise RuntimeError(f"{path} is held for reading: a scope that writes cannot start inside one that "
                               "reads, since it would have to let the lock go to take it alone")
        state[2] += 1
        try:
            if defer:
                with deferring():
                    yield
            else:
                yield
        finally:
            state[2] -= 1
            if state[2] == 0:
                os.close(state[1])
                state[1] = None
    finally:
        state[0].release()


RECORD_MAX = 65536      # how much of a claim file is read: a record is a few hundred bytes
MAKE_TRIES = 3          # names `Claim.make` takes where a sweep of unheld claims forgets one as it takes it

# A claim is an open file description lock (fcntl(2), F_OFD_SETLK): like flock it belongs to the open file, is
# shared by every fd a fork or a dup made of it, and goes when the last of them closes. Unlike flock, F_OFD_GETLK
# asks whether someone holds one without taking anything, so asking can never stand in a taker's way. Python's
# fcntl names these only where its build knew them; the numbers are the same on every Linux.
_OFD_GETLK = getattr(fcntl, "F_OFD_GETLK", 36)
_OFD_SETLK = getattr(fcntl, "F_OFD_SETLK", 37)
_FLOCK = struct.Struct("=hh4xqqi4x")       # struct flock on 64-bit Linux: type, whence, start, len, pid


def _whole_file(kind: int) -> bytes:
    """A struct flock for the whole file, of kind F_WRLCK or F_UNLCK."""
    return _FLOCK.pack(kind, os.SEEK_SET, 0, 0, 0)


def _same_file(path: str, fd: int) -> bool:
    """Whether path still names the file this fd holds. A name removed while this fd was being locked leaves
    the lock on a file no name leads to, which no reader can ever find."""
    try:
        named, held = os.stat(path), os.fstat(fd)
    except OSError:
        return False
    return (named.st_dev, named.st_ino) == (held.st_dev, held.st_ino)


class Claim:
    """A claim on a path: one process says it is the one doing something, and holds the claim while it lives.

    `Claim.take(path, record)` takes it, or gives None back because another holder has it; the holder keeps the
    claim until it releases it or ends. `Claim.make(path, record)` takes one on a name nobody else ever takes,
    locked before the name is visible. `Claim.holder(path)` asks who holds it, without waiting and without
    taking it away. One claim per thing pp does at most once at a time: a queue's worker, one run of one job,
    one run of one probe.

        c = Claim.take(path, {"pid": os.getpid()})      # None: someone else is doing it
        if c is None:
            return
        with c:
            ...                                         # and the claim is let go of here

    The kernel holds it, as a lock on the open file, and lets go of it when the holder's last file description
    on it closes: at a clean exit, on an exception, on a SIGKILL, and when the machine loses power. So nothing
    has to be cleaned up after a holder that died, and no pid and no start time decide whether the holder is
    alive: getting the claim means there is no holder, whatever the file says, and failing to get it means
    there is one.

    What the file holds is the record of who holds it now. It is written under the claim: `take` locks the
    file, empties it and writes the record, in that order, and `release` empties it again. So an orderly
    release leaves no record, and the bytes a reader finds between a take's lock and its write are either
    none or the record of a holder that was killed. A reader that has to tell one holder from another looks
    again until the record names a process that is running. A holder that writes its record again writes over
    every byte of the one before it in one call, so a reader finds one record or the other, never half of
    each.

    The holder's file description is close-on-exec, so a process the holder starts does not keep the claim
    alive after the holder ends. A process that is MEANT to hold it is given the file description itself,
    which carries the lock: a job's wrapper gets it as its stdin, and from then on the job's run is what holds
    the job's claim.

    The file stays while the thing it is about exists, since a reader may be about to ask about it, and removing
    it would let the next taker create another file under the same name and hold a second claim on the same
    thing. A claim file left by a holder that has gone costs one small file and reads as free. `forget` removes
    the file of a thing that is gone for good, and is the only way a claim file is removed.

    `take` creates the file and then locks it, so for a moment a file exists that nobody holds yet. A name
    whose claim file is swept away when nobody holds it is taken with `make` instead, which locks the file
    before the name is visible.

    Over NFS this is as strong as the server's record locks, which may be weaker than they are locally; pp's
    state directory is a local directory.
    """

    def __init__(self, path: str, fd: int):
        self.path = path
        self.fd: int | None = fd
        self._held = 0          # bytes of the file this holder has written, which the next write covers whole

    @classmethod
    def take(cls, path: str, record: dict | None = None) -> Claim | None:
        """Take the claim on path and keep it; None when another holder has it. Nothing waits here.

        record says who holds it, for whoever asks. None leaves the file empty, for a claim whose holder writes
        its own record (a job's wrapper) or whose holder nobody asks about (a probe). A claim taken with no
        record is also the one a release leaves alone, which is what a handed-on file description needs."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.fcntl(fd, _OFD_SETLK, _whole_file(fcntl.F_WRLCK))
        except OSError as e:
            os.close(fd)
            if e.errno in (errno.EAGAIN, errno.EACCES):     # what F_OFD_SETLK says when another holder has it
                return None
            raise
        except BaseException:
            os.close(fd)
            raise
        c = cls(path, fd)
        try:
            os.ftruncate(fd, 0)         # what a holder that has gone wrote is not this holder's record
            if record is not None:
                c.write(record)
        except BaseException:
            c.release()
            raise
        return c

    @classmethod
    def make(cls, path: str, record: dict | None = None) -> Claim:
        """Take the claim on a name nobody else ever takes, locked before that name is visible.

        The file is created under a name of this call's own, locked, written, and renamed into place. So a
        file that exists at path is one its holder already holds, and a sweeper that finds one unheld knows
        its holder is gone: it can never take the claim away from a holder that is in the middle of taking
        it. `take` cannot promise that, since the name it takes is claimed again and again and its file has
        to outlive each holder.

        For a name that carries its holder's own identity, which nobody else ever takes: a `wait`'s lease.
        Nothing competes for it, so this gives back a claim or raises what the file system raised, and never
        None.

        The name it creates ends in `.claim`, like the name it renames to, so a sweep of unheld claims
        forgets what a holder killed before the rename left behind: nobody holds that file, and no name of
        pp's is ever claimed again. A sweep that forgets that name in the moment before the lock is taken is
        what the look after the lock is for: this takes another name where the one it locked has been
        forgotten, since a lock on a file no name leads to is a claim nobody can ever find."""
        d = os.path.dirname(path) or "."
        os.makedirs(d, exist_ok=True)
        for _ in range(MAKE_TRIES):
            tmp = os.path.join(d, f".{os.path.basename(path)}.{os.getpid()}.{os.urandom(4).hex()}.new.claim")
            c = cls(path, os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o644))
            try:
                fcntl.fcntl(c.fd, _OFD_SETLK, _whole_file(fcntl.F_WRLCK))   # this name is nobody else's: free
                if not _same_file(tmp, c.fd):
                    c.release()
                    continue            # a sweep found that name unheld and forgot it: take another
                if record is not None:
                    c.write(record)
                os.rename(tmp, path)        # and from here the name is this holder's, already locked
            except BaseException:
                c.release()
                try:
                    os.remove(tmp)
                except OSError:
                    pass
                raise
            return c
        raise FileNotFoundError(errno.ENOENT, f"{path}: every name this took was forgotten as it took it")

    def write(self, record: dict) -> None:
        """Replace the record in the claim file. Only the holder writes it, in one call that covers every byte it
        wrote before, so a reader finds one whole record. The record ends at its newline, and the spaces that may
        follow it are nothing to a reader of JSON."""
        data = json.dumps(record).encode() + b"\n"
        data += b" " * max(0, self._held - len(data))
        os.pwrite(self.fd, data, 0)
        self._held = len(data)

    def release(self) -> None:
        """Let the claim go, and take this holder's record with it: the file is emptied before the descriptor
        closes, so an orderly release leaves no record and the only record a reader ever finds is the one its
        holder wrote. A holder that was killed leaves its record behind, naming a process that is gone.

        A claim whose record is written by whoever is handed the file description (a job's wrapper) is taken
        with no record, so this holder has nothing to take away and that writer's record stays. Closing the
        file is what releases the claim, so an exit releases it too; releasing twice is not an error."""
        if self.fd is not None:
            try:
                if self._held:
                    os.ftruncate(self.fd, 0)
            finally:
                os.close(self.fd)
                self.fd = None
                self._held = 0

    def __enter__(self) -> Claim:
        return self

    def __exit__(self, *exc) -> None:
        self.release()

    @classmethod
    def forget(cls, path: str) -> None:
        """Remove the claim file of a thing that is gone and whose name is never claimed again: a job whose
        record has been taken away, a `wait` whose lease carries its own pid. Every removal of a claim file
        goes through here. A name that may be claimed again keeps its file, since removing it would let the
        next taker create another file under that name and hold a second claim on the same thing."""
        try:
            os.remove(path)
        except OSError:
            pass

    @classmethod
    def holder(cls, path: str) -> dict | None:
        """The record of whoever holds the claim on path, or None when nobody holds it. Asking takes nothing: it
        never waits, and never stands in the way of a holder or of a process taking the claim.

        An empty record means the claim is held by a holder that has not written one yet, which is a holder
        starting: it is alive, and it names nothing yet. A record naming a process that is gone is the record
        a holder that was killed left, read while the next holder is taking the claim."""
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return None
        try:
            held = _FLOCK.unpack(fcntl.fcntl(fd, _OFD_GETLK, _whole_file(fcntl.F_WRLCK)))[0]
            if held == fcntl.F_UNLCK:
                return None             # nobody holds it, whatever the file says
            try:
                rec = json.loads(os.pread(fd, RECORD_MAX, 0) or b"{}")
            except ValueError:
                return {}
            return rec if isinstance(rec, dict) else {}
        finally:
            os.close(fd)


@contextmanager
def deferring():
    """The four signals whose default action ends the process, namely INT, TERM, HUP and QUIT, are delivered
    when the block ends, to the handlers in place before it. This is the one place that set is named; every
    other scope says "the signals lock.deferring holds back". Python runs signal handlers in the main thread
    only, so only the main thread needs this (and may set them).

    What `locked(defer=True)` holds a change under the lock with, and what a scope with no lock of its own
    uses to be whole: a few renames that go through temporary names, say. Keep the block short, since a Ctrl-C
    waits for it."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    got: list[int] = []
    old = {s: signal.getsignal(s)
           for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT)}
    for s in old:
        signal.signal(s, lambda n, _f: got.append(n))
    try:
        yield
    finally:
        for s, h in old.items():
            signal.signal(s, h if h is not None else signal.SIG_DFL)
        if got:
            from pp.lib import proc      # here and not at the top of the file: pp.lib.paths imports this one
            for n in got:
                proc.raise_to_self(n)   # now: raises KeyboardInterrupt, ends the process, or sets a flag
