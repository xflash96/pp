"""What pp/lib/lock.py promises.

locked: one holder at a time, an answer at once when asked not to wait, the holder back in, and signals that
wait for the block to end. Claim: the kernel ends it with its holder, a record is its holder's own, asking
takes nothing, and a child the holder starts does not keep it."""
import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp.lib import lock, proc  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ASK = """
import sys
from pp.lib import lock
shared = len(sys.argv) > 2
try:
    with lock.locked(sys.argv[1], block=False, shared=shared):
        print("took it")
except lock.Busy:
    print("busy")
"""


class TestLocked(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        self.path = os.path.join(self._td.name, "sub", ".lock")

    def hold(self) -> None:
        """The lock held by another actor of this process (a thread of its own, since its holder may take it
        again), until this test is over."""
        got, done = threading.Event(), threading.Event()

        def holder():
            with lock.locked(self.path):
                got.set()
                done.wait(30)

        th = threading.Thread(target=holder)
        th.start()
        self.addCleanup(th.join, 30)
        self.addCleanup(done.set)
        self.assertTrue(got.wait(30), "the holder never took the lock")

    def test_a_process_that_will_not_wait_is_told_at_once(self):
        """The child asks with block=False while this process holds it, and says what it got. A lock that made
        it wait would hang here until the timeout."""
        with lock.locked(self.path):
            r = subprocess.run([sys.executable, "-c", ASK, self.path], cwd=ROOT, capture_output=True, text=True,
                               timeout=60)
        self.assertEqual((r.stdout, r.stderr), ("busy\n", ""))

    def test_another_thread_asking_without_waiting_is_told_too(self):
        self.hold()
        with self.assertRaises(lock.Busy):
            with lock.locked(self.path, block=False):
                self.fail("two holders at once")

    def test_the_holder_takes_it_again(self):
        with lock.locked(self.path):
            with lock.locked(self.path, block=False):       # a nested scope is the holder, not another one
                pass
            self.assertTrue(os.path.exists(self.path), "the lock file and its directory are made")

    def test_a_released_lock_is_free_again(self):
        with lock.locked(self.path):
            pass
        with lock.locked(self.path, block=False):
            pass
        r = subprocess.run([sys.executable, "-c", ASK, self.path], cwd=ROOT, capture_output=True, text=True,
                           timeout=60)
        self.assertEqual((r.stdout, r.stderr), ("took it\n", ""))

    def ask(self, *extra: str) -> str:
        """What another process gets when it asks for the lock without waiting."""
        r = subprocess.run([sys.executable, "-c", ASK, self.path, *extra], cwd=ROOT, capture_output=True,
                           text=True, timeout=60)
        self.assertEqual(r.stderr, "")
        return r.stdout.strip()

    def test_several_read_at_once_and_a_writer_waits_for_them(self):
        with lock.locked(self.path, shared=True):
            self.assertEqual(self.ask("shared"), "took it")
            self.assertEqual(self.ask(), "busy")

    def test_a_reader_waits_for_a_writer(self):
        with lock.locked(self.path):
            self.assertEqual(self.ask("shared"), "busy")

    def test_writing_inside_a_read_is_refused(self):
        """It would have to let the lock go to take it alone, and what it read before that would then be older
        than the file."""
        with lock.locked(self.path, shared=True):
            with self.assertRaises(RuntimeError):
                with lock.locked(self.path):
                    self.fail("the hold was made exclusive")
            with lock.locked(self.path, shared=True):       # another reading scope is the same holder
                pass
        with lock.locked(self.path):                        # and the lock is free again afterwards
            pass

    def test_each_signal_that_ends_a_process_lands_after_a_deferred_block(self):
        """The four the contract names: a terminal's Ctrl-C, a `pp kill`, the terminal closing, and a Ctrl-\\.
        Dropping one of them would leave a change under the lock cut in half."""
        sb = sandbox.enter(self)        # each handler is the scope's, and goes back before the next test
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT):
            with self.subTest(signal=signal.Signals(sig).name):
                got = []
                sb.on_signal(sig, lambda n, _f: got.append(n))
                with lock.locked(self.path, defer=True):
                    proc.raise_to_self(sig)
                    self.assertEqual(got, [], "the signal arrived inside the block")
                self.assertEqual(got, [sig])

    def test_without_defer_a_signal_arrives_in_the_block(self):
        got = []
        sandbox.enter(self).on_signal(signal.SIGTERM, lambda n, _f: got.append(n))
        with lock.locked(self.path):
            proc.raise_to_self(signal.SIGTERM)
            self.assertEqual(got, [signal.SIGTERM])


HOLD = """
import os, signal, sys
from pp.lib import lock
c = lock.Claim.take(sys.argv[1], {"pid": os.getpid(), "who": "the holder"})
print("held" if c is not None else "busy", flush=True)
signal.pause()
"""


TAKE = """
import sys
from pp.lib import lock
c = lock.Claim.take(sys.argv[1], {"pid": 0})
print("took it" if c else "held")
"""


WATCH = "import signal; print('up', flush=True); signal.pause()"


class TestClaim(unittest.TestCase):
    """A claim is liveness: the holder has it while it lives, and the kernel takes it back when the holder
    ends, however it ends."""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        self.path = os.path.join(self._td.name, "sub", "thing.claim")

    def holder_process(self) -> journey.Child:
        """Another process holding the claim, which has said so."""
        c = journey.start([sys.executable, "-c", HOLD, self.path], cwd=ROOT, ready="held")
        self.addCleanup(c.close)
        return c

    def test_a_second_take_is_refused_while_the_holder_lives(self):
        c = self.holder_process()
        self.assertIsNone(lock.Claim.take(self.path, {"pid": 0}))
        self.assertEqual(lock.Claim.holder(self.path)["pid"], c.pid)

    def test_a_holder_that_was_killed_leaves_the_claim_free_and_its_record_behind(self):
        """No cleanup step runs between the SIGKILL and the next ask: the kernel let the claim go with its
        holder, so nothing has to notice that the holder died. Its record is still in the file, and says
        nothing about whether anyone holds the claim: the claim answers that."""
        c = self.holder_process()
        self.assertEqual(proc.signal_pid(c.handle, signal.SIGKILL), "alive")
        self.assertTrue(c.dead(10), "the holder did not end")
        self.assertIsNone(lock.Claim.holder(self.path))
        with open(self.path) as f:
            self.assertIn("the holder", f.read())
        with lock.Claim.take(self.path, {"pid": os.getpid()}) as mine:
            self.assertIsNotNone(mine)

    def test_a_released_claim_leaves_no_record(self):
        """A release takes this holder's record with it, so the only record a reader ever finds in a claim
        file is one its holder wrote: a holder that is there now, or one that was killed. An orderly release
        leaves the file empty."""
        lock.Claim.take(self.path, {"pid": os.getpid(), "who": "gone now"}).release()
        with open(self.path) as f:
            self.assertEqual(f.read(), "")
        self.assertEqual(os.path.getsize(self.path), 0)
        self.assertIsNone(lock.Claim.holder(self.path))

    def test_a_take_leaves_only_this_holders_record(self):
        """The record a holder that was killed left is not the next holder's: a take empties the file, so a
        reader finds the new holder's own record or none."""
        c = self.holder_process()
        self.assertEqual(proc.signal_pid(c.handle, signal.SIGKILL), "alive")
        self.assertTrue(c.dead(10), "the holder did not end")
        with lock.Claim.take(self.path) as mine:        # a holder that writes its own record, later
            self.assertEqual(lock.Claim.holder(self.path), {})
            mine.write({"pid": 2})
            self.assertEqual(lock.Claim.holder(self.path), {"pid": 2})

    def test_a_record_written_again_covers_the_one_before_it(self):
        """A shorter record than the one before it still leaves one whole record to read, in one call: nothing
        of the longer one is left to be read with it."""
        with lock.Claim.take(self.path, {"pid": 1, "waiting": "a long reason, with its detail"}) as c:
            c.write({"pid": 1, "waiting": None})
            self.assertEqual(lock.Claim.holder(self.path), {"pid": 1, "waiting": None})
            c.write({"pid": 1, "waiting": "short"})
            self.assertEqual(lock.Claim.holder(self.path), {"pid": 1, "waiting": "short"})

    def test_asking_takes_nothing(self):
        """Asking who holds a claim only asks: it takes no lock of any kind, so it can never stand in the way of
        a holder, nor wait for one."""
        c = self.holder_process()
        real_fcntl, real_flock, calls = lock.fcntl.fcntl, lock.fcntl.flock, []

        def fcntl_(fd, cmd, arg=0):
            calls.append(cmd)
            return real_fcntl(fd, cmd, arg)

        def flock(fd, op):
            calls.append(("flock", op))
            return real_flock(fd, op)
        with mock.patch.object(lock.fcntl, "fcntl", fcntl_), mock.patch.object(lock.fcntl, "flock", flock):
            rec = lock.Claim.holder(self.path)
        self.assertEqual(rec, {"pid": c.pid, "who": "the holder"})
        self.assertEqual(calls, [lock._OFD_GETLK])

    def test_a_take_while_someone_asks_gets_the_claim(self):
        """The moment someone asks about a free claim is no different from any other: a process taking it then
        gets it. Here the take runs inside the asking call."""
        lock.Claim.take(self.path).release()           # the file exists and nobody holds it
        real_fcntl, real_flock, took = lock.fcntl.fcntl, lock.fcntl.flock, []

        def take_now():
            r = subprocess.run([sys.executable, "-c", TAKE, self.path], capture_output=True, text=True,
                               cwd=ROOT, timeout=60)
            took.append(r.stdout.strip())

        def fcntl_(fd, cmd, arg=0):
            out = real_fcntl(fd, cmd, arg)
            take_now()
            return out

        def flock(fd, op):
            out = real_flock(fd, op)
            take_now()
            return out
        with mock.patch.object(lock.fcntl, "fcntl", fcntl_), mock.patch.object(lock.fcntl, "flock", flock):
            self.assertIsNone(lock.Claim.holder(self.path))
        self.assertEqual(took, ["took it"])

    def test_a_claim_on_a_name_nobody_takes_twice_is_locked_before_it_is_named(self):
        """`make` is for a name that carries its own holder's identity, which nobody else ever takes: a
        `wait`'s lease. The file is locked and its record written under a name of this call's own, and only
        then renamed into place. So whoever finds that name finds a claim that is held, and a sweep of the
        names nobody holds can never take one from a holder in the middle of taking it."""
        real_rename, seen = lock.os.rename, []

        def rename(src, dst):
            seen.append((lock.Claim.holder(src), os.path.exists(dst)))
            return real_rename(src, dst)
        with mock.patch.object(lock.os, "rename", rename):
            c = lock.Claim.make(self.path, {"pid": os.getpid()})
        self.addCleanup(c.release)
        # under its own name it is already held and its record is whole; the name it takes is not there yet
        self.assertEqual(seen, [({"pid": os.getpid()}, False)])
        self.assertEqual(lock.Claim.holder(self.path), {"pid": os.getpid()})
        c.release()
        self.assertIsNone(lock.Claim.holder(self.path))

    def test_a_name_taken_with_make_leaves_nothing_of_its_own_behind(self):
        """The name it is taken under is this call's own and goes with the rename, so the directory holds the
        claim's file and nothing else: a lease that failed to be taken leaves no file either."""
        d = os.path.dirname(self.path)
        with lock.Claim.make(self.path, {"pid": os.getpid()}):
            self.assertEqual(os.listdir(d), [os.path.basename(self.path)])
        with mock.patch.object(lock.os, "rename", mock.Mock(side_effect=OSError("no"))):
            with self.assertRaises(OSError):
                lock.Claim.make(os.path.join(d, "other.claim"))
        self.assertEqual(os.listdir(d), [os.path.basename(self.path)])

    def test_a_child_of_the_holder_does_not_keep_the_claim(self):
        """The claim is the holder's own: a process it starts does not hold it on after the holder ends, so a
        job's own children never make it look as if the job were still running. The file description is
        close-on-exec, which is what sees to that, so a child that is given every descriptor it could inherit
        still does not have this one.

        The child is handed the holder's own descriptors (close_fds=False), since a child that was never
        given the description could not hold the claim whatever lock.py did."""
        c = lock.Claim.take(self.path, {"pid": os.getpid()})
        self.assertFalse(os.get_inheritable(c.fd), "the claim's own description survives an exec")
        kid = journey.start([sys.executable, "-c", WATCH], ready="up", close_fds=False)
        self.addCleanup(kid.close)
        c.release()
        self.assertIsNone(lock.Claim.holder(self.path))
        self.assertIsNone(kid.poll(), "the child this holds is gone, so it proves nothing")

    def test_a_child_that_is_handed_the_description_holds_the_claim(self):
        """The other side of it, which pp depends on: a process that is MEANT to hold the claim is given the
        open file description itself, and from then on that process is what holds the claim. A job's wrapper
        gets it as its stdin, so the attempt's own `sh` is what says the job is running, for exactly as long
        as it lives, and the worker's copy going makes no difference to it.

        The claim is taken with no record, since the record in it is to be that child's: a claim taken with
        one has it emptied as this copy is released, which is why a job's wrapper writes its own pid in there
        as its first act."""
        c = lock.Claim.take(self.path)
        kid = journey.start([sys.executable, "-c", WATCH], ready="up", stdin=c.fd)
        self.addCleanup(kid.close)
        c.release()                                     # pp's own copy; the child's keeps the claim
        # it is held, by the child, and names nothing until that child writes its own record
        self.assertEqual(lock.Claim.holder(self.path), {})
        self.assertIsNone(lock.Claim.take(self.path, {"pid": 0}), "the child does not hold it")
        kid.close()
        journey.until(lambda: lock.Claim.holder(self.path) is None, timeout=10,
                      what="the claim to go with the child that held it")


if __name__ == "__main__":
    unittest.main()
