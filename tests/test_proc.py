"""What pp/lib/proc.py's constructs promise, checked against real processes.

A handle is the only way pp signals anything, so these are the facts every call site relies on: a handle names
one process for as long as it exists, and never another that was given its pid; a record with half the identity
in it cannot be signalled at all; what came of the signal is an outcome, not an exception.

spawn is the only way pp starts one, and the facts here are the ones no call site should have to repeat: the
child's group, what it can read, the environment it is given, that an ignored signal does not reach it as
ignored, and that nothing it started outlives the time it was given.
"""
import inspect
import os
import signal
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp.lib import lock, proc  # noqa: E402

SLEEPER = "import time; print('ready', flush=True); time.sleep(60)"
DEAF = ("import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready', flush=True); "
        "time.sleep(60)")
# it writes into the file it was given as its input, and holds that file until it is ended
HOLDS_ITS_INPUT = ('import os, signal; os.write(0, b\'{"held": "by the child"}\'); '
                   "print('ready', flush=True); signal.pause()")


class HandleCase(unittest.TestCase):
    def child(self, code: str = SLEEPER, group: str = "shared") -> journey.Child:
        c = journey.start([sys.executable, "-c", code], group=group, ready="ready")
        self.addCleanup(c.close)
        return c


class TestWhatAHandleNames(HandleCase):
    def test_a_handle_needs_a_pid_and_the_start_time_read_with_it(self):
        with self.assertRaises(ValueError):
            proc.Handle(os.getpid(), None)
        with self.assertRaises(ValueError):
            proc.Handle(0, 1)

    def test_a_record_missing_either_half_is_no_handle(self):
        """What a job or a worker wrote earlier: with no start time there is nothing to pin the pid to, so the
        record yields no handle and the caller cannot signal whatever holds that pid now."""
        self.assertIsNone(proc.Handle.recorded(os.getpid(), None))
        self.assertIsNone(proc.Handle.recorded(None, 1234))
        self.assertIsNone(proc.Handle.recorded(0, 1234))
        h = proc.Handle.recorded(os.getpid(), 1234, group=True)
        self.assertEqual((h.id, h.start, h.group), (os.getpid(), 1234, True))

    def test_a_handle_for_a_process_that_is_not_there(self):
        with open("/proc/sys/kernel/pid_max") as f:
            nobody = int(f.read())          # one past the largest pid: no process can hold it
        self.assertIsNone(proc.Handle.live(nobody))

    def test_a_process_handle_and_a_group_handle_are_not_interchangeable(self):
        h = proc.Handle.live(os.getpid())
        with self.assertRaises(ValueError):
            proc.signal_group(h, signal.SIGUSR1)
        with self.assertRaises(ValueError):
            proc.signal_pid(proc.Handle.live(os.getpid(), group=True), signal.SIGUSR1)


class TestAStartThatDoesNotMatch(HandleCase):
    """A pid is handed out again once its process is reaped, so a signal sent on a pid alone can reach anything.
    A handle whose start time is not the live process's names a process that is gone: nothing is sent."""

    def test_a_process_is_never_signalled_through_a_handle_of_another_start(self):
        c = self.child()
        stale = proc.Handle(c.pid, c.handle.start + 1)
        self.assertEqual(proc.signal_pid(stale, signal.SIGKILL), "gone")
        self.assertIsNone(c.poll())
        self.assertTrue(proc.alive(c.pid, c.handle.start))

    def test_a_group_is_never_signalled_through_a_handle_of_another_start(self):
        c = self.child(group="own")
        stale = proc.Handle(c.pid, c.group.start + 1, group=True)
        self.assertEqual(proc.signal_group(stale, signal.SIGKILL), "gone")
        self.assertIsNone(c.poll())
        self.assertEqual(proc.members(c.pid), [c.pid])


class TestOutcomes(HandleCase):
    """What a signal says it did. `signal_pid` and `signal_group` send and return, so their answer is about
    the send alone: alive, gone, or denied. `signal_and_wait` is the one that waits for its targets and
    SIGKILLs the ones that outlive the grace, which is what `pp kill` does, and it says exited and killed."""

    def test_a_signal_is_sent_and_not_waited_for(self):
        c = self.child()
        self.assertEqual(proc.signal_pid(c.handle, signal.SIGKILL), "alive")
        self.assertTrue(c.dead())

    def test_a_process_that_takes_the_signal_exited(self):
        c = self.child()
        self.assertEqual(proc.signal_and_wait([c.pid], signal.SIGTERM, grace=10), {c.pid: "exited"})
        self.assertTrue(c.dead())

    def test_one_that_ignores_it_is_killed_after_the_grace(self):
        c = self.child(DEAF)        # 'ready' means SIGTERM is ignored now, not in a moment
        self.assertEqual(proc.signal_and_wait([c.pid], signal.SIGTERM, grace=0.3), {c.pid: "killed"})
        self.assertTrue(c.dead())

    def test_one_that_ignores_it_is_alive_when_nothing_escalates(self):
        c = self.child(DEAF)
        self.assertEqual(proc.signal_and_wait([c.pid], signal.SIGTERM, grace=0.3, escalate=False),
                         {c.pid: "alive"})
        self.assertIsNone(c.poll())

    def test_a_process_that_has_ended_is_gone(self):
        c = self.child()
        self.assertEqual(proc.signal_and_wait([c.pid], signal.SIGKILL, grace=10), {c.pid: "exited"})
        self.assertTrue(c.dead())
        self.assertEqual(proc.signal_pid(c.handle, signal.SIGTERM), "gone")


class TestAGroup(HandleCase):
    def test_a_group_is_its_leader_with_its_children_and_gone_with_the_last_of_them(self):
        """A job is a group, so what counts is whether any member is left: one signal reaches them all, and the
        group is gone only once none of them is there."""
        c = journey.start(["sh", "-c", "sleep 60 & sleep 60 & echo ready; wait"], group="own", ready="ready")
        self.addCleanup(c.close)
        kids = journey.until(lambda: proc.descendants(c.pid) if len(proc.descendants(c.pid)) == 2 else None)
        self.assertEqual(sorted(proc.members(c.pid)), sorted([c.pid] + kids))
        self.assertEqual(proc.signal_group(c.group, signal.SIGKILL), "alive")
        self.assertTrue(c.dead(10))
        journey.until(lambda: not proc.members(c.pid), what="the group to be gone")
        self.assertEqual(proc.members(c.pid), [])

    def test_a_zombie_is_not_a_member_and_is_not_alive(self):
        """A process that ended and has not been reaped still holds its pid. No signal reaches it, so counting it
        as a member would leave a group busy for as long as nobody waits for it."""
        c = journey.start([sys.executable, "-c", "pass"], group="own")
        self.addCleanup(c.close)
        journey.until(lambda: (proc.read(c.pid) and proc.read(c.pid).state in proc.DEAD))
        self.assertEqual(proc.members(c.pid), [])
        self.assertFalse(proc.alive(c.pid))
        # and it is no process a record names either: whoever wrote that record has ended
        self.assertIsNone(proc.Handle.running(c.pid, proc.read(c.pid).start))
        self.assertIsNotNone(proc.Handle.running(os.getpid(), proc.read(os.getpid()).start))


class TestThroughAPidfd(HandleCase):
    """Where pp holds the process, the kernel keeps the identity: the fd points at that process, so the signal
    cannot reach another that was given its pid. os.kill is made to fail for as long as the signal is on its
    way, so a run that takes the pid instead of the fd fails here rather than passing on the fallback."""

    def no_pidfd(self) -> bool:
        fd = proc.pidfd(os.getpid())
        if fd is None:
            return True
        os.close(fd)
        return False

    def through_the_fd(self, c) -> str:
        self.assertIsNotNone(c.handle.fd, "a process pp started with no fd of its own")

        def by_the_pid(*a, **kw):
            self.fail("the signal went by the pid, not through the fd")

        with mock.patch.object(proc.os, "kill", by_the_pid):
            out = proc.signal_pid(c.handle, signal.SIGTERM)
        self.assertTrue(c.dead(10), "the signal through the fd never reached it")
        return out

    def test_a_child_is_signalled_through_a_pidfd_of_itself(self):
        if self.no_pidfd():
            self.skipTest("this kernel gives no pidfd")
        self.assertEqual(self.through_the_fd(self.child()), "alive")

    def test_the_fd_is_pps_path_where_this_python_does_not_wrap_the_calls(self):
        """A Python built without os.pidfd_open and signal.pidfd_send_signal gets both from libc, so which
        build pp runs on does not decide whether a signal can miss."""
        with mock.patch.object(proc, "_WRAPPED", False):
            if self.no_pidfd():
                self.skipTest("this kernel gives no pidfd")
            self.assertEqual(self.through_the_fd(self.child()), "alive")


class TestToThisProcess(unittest.TestCase):
    def test_a_signal_raised_to_this_process_runs_its_handler(self):
        """What the store lock's deferral does when it lets go: the signal that arrived is delivered now, to the
        handler in place now."""
        got = []
        with sandbox.Sandbox() as sb:       # the handler is the scope's, so it cannot act in the next test
            sb.on_signal(signal.SIGUSR1, lambda n, _f: got.append(n))
            proc.raise_to_self(signal.SIGUSR1)
        self.assertEqual(got, [signal.SIGUSR1])


class TestProcReading(unittest.TestCase):
    def test_a_process_reads_as_itself(self):
        p = proc.read(os.getpid())
        self.assertEqual((p.pid, p.ppid, p.pgid), (os.getpid(), os.getppid(), os.getpgid(0)))
        self.assertGreater(p.start, 0)

    def test_a_child_of_its_own_session_leads_its_own_group(self):
        c = journey.start([sys.executable, "-c", SLEEPER], group="own", ready="ready")
        self.addCleanup(c.close)
        self.assertEqual(proc.read(c.pid).pgid, c.pid)


DISPOSITION = ("import signal, sys; print(' '.join('IGN' if signal.getsignal(getattr(signal, s)) is signal.SIG_IGN "
               "else 'DFL' for s in ('SIGINT', 'SIGTERM')), flush=True); sys.stdin.read()")
MASK = ("import signal, sys; print(' '.join(sorted(s.name for s in signal.pthread_sigmask(signal.SIG_BLOCK, ()) "
        "if s in (signal.SIGINT, signal.SIGTERM))), flush=True); sys.stdin.read()")
SAY_ENV ="import os, sys; print(','.join(sorted(k for k in os.environ if k.startswith('PP_T'))), flush=True)"
SAY_SID = "import os; print(os.getsid(0), flush=True)"
READY = 'echo up >&"$PP_READY_FD"; echo said; read x'


class SpawnCase(unittest.TestCase):
    def spawn(self, argv, **kw) -> proc.Child:
        kw.setdefault("stdout", proc.PIPE)
        kw.setdefault("text", True)
        c = proc.spawn(argv, **kw)
        self.addCleanup(self.ended, c)
        return c

    @staticmethod
    def ended(c: proc.Child):
        c.end(signal.SIGKILL)
        c.wait()
        c.close_pipes()


class TestEveryCallerStatesThree(SpawnCase):
    def test_the_three_decisions_have_no_default(self):
        """A call site cannot be silent about the group, the input or the environment of what it starts."""
        for f in (proc.spawn, proc.run):
            for name in ("group", "stdin", "env"):
                p = inspect.signature(f).parameters[name]
                self.assertEqual(p.kind, inspect.Parameter.KEYWORD_ONLY, f"{f.__name__} {name}")
                self.assertIs(p.default, inspect.Parameter.empty, f"{f.__name__} {name}")

    def test_a_git_run_states_its_group_too(self):
        """The group decides whether git reaches a terminal, so a site that runs git says which it wants."""
        p = inspect.signature(proc.git).parameters["group"]
        self.assertEqual(p.kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertIs(p.default, inspect.Parameter.empty)

    def test_a_decision_that_is_not_one_of_the_words_is_refused(self):
        for kw in ({"group": "session"}, {"stdin": "/dev/null"}, {"env": "clean"}):
            with self.assertRaises(ValueError):
                proc.spawn(["true"], **{"group": "own", "stdin": "none", "env": "inherit", **kw})

    def test_its_own_group_makes_it_the_leader_of_one(self):
        c = self.spawn([sys.executable, "-c", SLEEPER], group="own", stdin="none", env="inherit")
        self.assertEqual(c.stdout.readline().strip(), "ready")
        self.assertEqual(proc.read(c.pid).pgid, c.pid)
        self.assertEqual(c.group.id, c.pid)

    def test_a_shared_group_is_pps_own(self):
        c = self.spawn([sys.executable, "-c", SLEEPER], group="shared", stdin="none", env="inherit")
        self.assertEqual(c.stdout.readline().strip(), "ready")
        self.assertEqual(proc.read(c.pid).pgid, os.getpgid(0))
        self.assertIsNone(c.group)

    def test_a_shared_group_keeps_pps_session_and_its_own_leaves_it(self):
        """A session is what holds a terminal: ssh asks for a key passphrase on /dev/tty, which a process in a
        session of its own does not have. So the choice of group is the choice of whether a child can ask."""
        r = proc.run([sys.executable, "-c", SAY_SID], group="shared", stdin="none", env="inherit")
        self.assertEqual(r.stdout.strip(), str(os.getsid(0)))
        r = proc.run([sys.executable, "-c", SAY_SID], group="own", stdin="none", env="inherit")
        self.assertNotEqual(r.stdout.strip(), str(os.getsid(0)))

    def test_stdin_none_is_at_end_of_input(self):
        c = self.spawn(["sh", "-c", "read x; echo read $?"], group="own", stdin="none", env="inherit")
        self.assertEqual(c.stdout.readline().strip(), "read 1")

    def test_an_open_file_as_input_carries_the_claim_on_it_to_the_child(self):
        """What a job's run is: the child is given the same open file, so the claim held on it becomes the
        child's for as long as the child lives, and pp closing its own copy does not let it go."""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "thing.claim")
            mine = lock.Claim.take(path)
            c = self.spawn([sys.executable, "-c", HOLDS_ITS_INPUT], group="own", stdin=mine.fd, env="inherit")
            self.assertEqual(c.stdout.readline().strip(), "ready")
            mine.release()                                      # pp's own copy; the child's keeps the claim
            self.assertEqual(lock.Claim.holder(path), {"held": "by the child"})
            self.assertEqual(c.end(signal.SIGKILL), "alive")
            self.assertEqual(c.wait(10), -signal.SIGKILL)
            self.assertIsNone(lock.Claim.holder(path))          # it went with the child, with nothing to clean up

    def test_the_environment_is_the_one_asked_for(self):
        r = proc.run([sys.executable, "-c", SAY_ENV], group="own", stdin="none",
                     env={"PATH": os.environ["PATH"], "PP_T_GIVEN": "1"}, add_env={"PP_T_ON_TOP": "2"})
        self.assertEqual(r.stdout.strip(), "PP_T_GIVEN,PP_T_ON_TOP")


class TestAnIgnoredSignal(SpawnCase):
    """SIG_IGN survives exec, so a pp started from a shell that ignores a signal would hand that on, and the
    child would never see a Ctrl-C or a `pp kill`. spawn puts both back to their default action in the child."""

    def ignoring(self):
        """This process ignores INT and TERM, as a shell that started pp in the background does. The
        dispositions are the scope's: each goes back before the next test runs."""
        sb = sandbox.enter(self)
        for s in (signal.SIGINT, signal.SIGTERM):
            sb.on_signal(s, signal.SIG_IGN)

    def test_the_child_of_a_process_that_ignores_them_reports_its_own_default(self):
        self.ignoring()
        c = self.spawn([sys.executable, "-c", DISPOSITION], group="own", stdin="pipe", env="inherit")
        self.assertEqual(c.stdout.readline().strip(), "DFL DFL")

    def test_a_child_of_such_a_process_still_ends_on_the_signal(self):
        self.ignoring()
        c = self.spawn([sys.executable, "-c", SLEEPER], group="own", stdin="none", env="inherit")
        self.assertEqual(c.stdout.readline().strip(), "ready")
        self.assertEqual(c.end(signal.SIGTERM), "alive")
        self.assertEqual(c.wait(10), -signal.SIGTERM)

    def test_the_child_of_a_thread_that_blocks_them_has_them_unblocked(self):
        """The signal mask survives exec too: `pp http` serves with INT and TERM blocked in every thread."""
        ending = {signal.SIGINT, signal.SIGTERM}
        old = signal.pthread_sigmask(signal.SIG_BLOCK, ending)
        try:
            c = self.spawn([sys.executable, "-c", MASK], group="own", stdin="pipe", env="inherit")
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old)
        self.assertEqual(c.stdout.readline().strip(), "")


class TestAChildATestStarts(unittest.TestCase):
    """journey.start makes the same reset spawn makes, rather than a reset of its own. A test that blocks INT
    and TERM, as `pp http` does while it serves, would otherwise hand the block to the process it starts beside
    itself, and the signal it then sends that process would never arrive."""

    def test_a_signal_this_thread_blocks_is_not_handed_to_it(self):
        ending = {signal.SIGINT, signal.SIGTERM}
        old = signal.pthread_sigmask(signal.SIG_BLOCK, ending)
        try:
            c = journey.start([sys.executable, "-c", MASK], group="own", stdin=proc.DEVNULL)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old)
        self.addCleanup(c.close)
        self.assertEqual(c.line(), "")


class TestReady(SpawnCase):
    def test_it_returns_once_the_child_says_it_is_up(self):
        """The byte arrives after the child's own setup, so what the child printed is already there to read."""
        c = self.spawn(["sh", "-c", READY], group="own", stdin="pipe", env="inherit", ready=True)
        self.assertIsNone(c.poll())
        self.assertEqual(c.stdout.readline().strip(), "said")

    def test_a_child_that_never_says_it_is_ready_is_killed_and_reaped(self):
        """A child killed for not saying it is up is waited for as well, the way a run that times out waits
        for what it ended. Its one caller starts a queue's worker, so a machine loaded enough to miss the
        window would otherwise leave a zombie behind for the rest of pp's life.

        The handle the start made says which process to look at, so the answer is about that one child and
        not about whatever else the suite has running: the subclass records it on the way out of spawn."""
        made = []

        class Recorded(proc.Child):
            def __init__(self, p, group):
                super().__init__(p, group)
                made.append(self)

        with mock.patch.object(proc, "Child", Recorded):
            try:
                proc.spawn(["sleep", "60"], group="own", stdin="none", env="inherit", ready=True,
                           ready_wait=0.2)
            except TimeoutError:
                # asked from here, where the traceback still holds the handle: once that goes, CPython's own
                # __del__ reaps what is left and the zombie this is about would be gone unseen
                self.assertEqual(len(made), 1)
                left = proc.read(made[0].pid)
            else:
                self.fail("the start came back although the child never said it was ready")
        self.assertIsNone(left, "the child it killed is still a process")

    def test_it_returns_at_end_of_the_pipe_when_the_child_ends_first(self):
        """A child that ends without a word closes the pipe, so a start that would otherwise wait for a byte
        that is never written comes back with the child's exit status to read."""
        c = self.spawn(["sh", "-c", "exit 3"], group="own", stdin="none", env="inherit", ready=True)
        self.assertEqual(c.wait(), 3)


class TestWhatATimeoutEnds(SpawnCase):
    def test_a_shells_children_end_with_it(self):
        """Ending the shell alone leaves what it started running, holding the pipe pp reads: the group is what
        has to go. run() returning at all is that fact: a surviving child would hold the pipe open."""
        r = proc.run(["sh", "-c", "sleep 60 & echo started; wait"], group="own", stdin="none", env="inherit",
                     timeout=0.5)
        self.assertEqual((r.timed_out, r.returncode, r.stdout), (True, None, "started\n"))

    def test_a_child_that_shares_pps_group_is_ended_by_itself(self):
        """What is ended is the child, not the group it shares with pp: that group holds pp and whatever else
        the terminal started, so ending it would end this process too, and nothing would read the result."""
        r = proc.run(["sh", "-c", "sleep 60"], group="shared", stdin="none", env="inherit", timeout=0.5)
        self.assertEqual((r.timed_out, r.returncode), (True, None))
        self.assertTrue(proc.alive(os.getpid()))

    def test_a_shared_childs_own_child_cannot_hold_the_timeout_open(self):
        """The shell is ended by itself, so the sleep it started runs on in pp's group, holding the write end
        of the pipe pp reads. Reading to the end of that pipe would wait for the sleep, which is as long as
        git's ssh sitting on a passphrase prompt: the read is bounded and the pipes are closed instead."""
        started = time.monotonic()
        r = proc.run(["sh", "-c", "sleep 20 & echo $!; wait"], group="shared", stdin="none", env="inherit",
                     timeout=0.5)
        took = time.monotonic() - started
        self.assertEqual((r.timed_out, r.returncode), (True, None))
        self.assertLess(took, 10, f"run() waited {took:g}s for a process it cannot end")
        self.assertRegex(r.stdout.strip(), r"^\d+$")
        proc.signal_and_wait([int(r.stdout)], signal.SIGKILL)       # nothing this test started is left running

    def test_the_group_of_a_child_pp_ends_itself_is_gone_with_it(self):
        c = self.spawn(["sh", "-c", "sleep 60 & echo started; wait"], group="own", stdin="none", env="inherit")
        self.assertEqual(c.stdout.readline().strip(), "started")
        kids = journey.until(lambda: proc.descendants(c.pid) or None)
        self.assertEqual(c.end(signal.SIGKILL), "alive")
        self.assertEqual(c.wait(10), -signal.SIGKILL)
        self.assertEqual(proc.members(c.pid), [])
        self.assertFalse([k for k in kids if proc.alive(k)])

    def test_an_interruption_while_pp_waits_ends_the_child_too(self):
        """A Ctrl-C does not reach a child of its own session, so pp ends it: nothing it started is left over."""
        pid = []

        def interrupted(self, *a, **kw):
            pid.append(self.pid)
            raise KeyboardInterrupt
        with mock.patch.object(proc.Child, "communicate", interrupted), self.assertRaises(KeyboardInterrupt):
            proc.run(["sh", "-c", "sleep 60"], group="own", stdin="none", env="inherit")
        self.assertFalse(proc.alive(pid[0]))


if __name__ == "__main__":
    unittest.main()
