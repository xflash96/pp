import io
import os
import socket
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PP = os.path.join(ROOT, "bin", "pp")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
from pp.commands import kill  # noqa: E402
from pp.lib import proc  # noqa: E402


SLEEPER = "import time; print('ready', flush=True); time.sleep(60)"
STUBBORN = ("import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready', flush=True); "
            "time.sleep(60)")


class TestKill(unittest.TestCase):
    def victim(self, *args, code=SLEEPER):
        """A process carrying `args` in its command line that has said it is up (journey.start's ready line)."""
        c = journey.start([sys.executable, "-c", code, *args], ready="ready")
        self.addCleanup(c.close)
        return c

    def test_pattern_spares_the_calling_shell(self):
        """The pkill -f trap: the pattern is in the calling shell's command line; that shell must survive."""
        mark = f"pp-kill-test-{os.getpid()}"
        victim = self.victim(mark)
        r = subprocess.run(["sh", "-c", f"{sys.executable} {PP} kill -y --grace 2 {mark}; echo shell-survived"],
                           capture_output=True, text=True, timeout=20)
        self.assertIn("shell-survived", r.stdout, r.stderr)
        self.assertTrue(victim.dead())

    def test_pattern_refuses_without_terminal(self):
        mark = f"pp-kill-refuse-{os.getpid()}"
        victim = self.victim(mark)
        r = subprocess.run([sys.executable, PP, "kill", mark], capture_output=True, text=True, stdin=subprocess.DEVNULL)
        self.assertEqual(r.returncode, 2)
        self.assertIsNone(victim.poll())

    def test_dry_run_and_exact(self):
        mark = f"pp-kill-dry-{os.getpid()}"
        victim = self.victim(mark)
        r = subprocess.run([sys.executable, PP, "kill", "-n", mark], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0)
        self.assertIn(str(victim.pid), r.stdout)
        self.assertIsNone(victim.poll())
        # -x matches the process NAME only: the marker is an argument, not the name
        r = subprocess.run([sys.executable, PP, "kill", "-n", "-x", mark], capture_output=True, text=True)
        self.assertEqual(r.returncode, 1)

    def test_pid_and_tree(self):
        parent = journey.start(["sh", "-c", "sleep 60 & sleep 60 & wait"], stdout=None)
        self.addCleanup(parent.close)
        # sh forks its children after it has started, so wait for both of them to be there
        journey.until(lambda: len(proc.descendants(parent.pid)) == 2, what="sh forked both sleeps")
        kids = proc.descendants(parent.pid)
        r = subprocess.run([sys.executable, PP, "kill", "--tree", str(parent.pid)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertTrue(parent.dead())
        journey.until(lambda: not any(proc.alive(k) for k in kids), what="every child of the tree ended")

    def test_escalates_to_sigkill(self):
        p = self.victim(code=STUBBORN)       # 'ready' means SIGTERM is ignored now, not in a moment
        r = subprocess.run([sys.executable, PP, "kill", "--grace", "0.5", str(p.pid)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0)
        self.assertIn("killed", r.stdout)
        self.assertTrue(p.dead())

    def test_port(self):
        code = ("import socket, sys, time; s = socket.socket(); s.bind(('127.0.0.1', 0)); s.listen();"
                "print(s.getsockname()[1], flush=True); time.sleep(60)")
        p = journey.start([sys.executable, "-c", code])
        self.addCleanup(p.close)
        port = int(p.line())
        socks, pids = proc.port_owners(port)
        self.assertEqual(pids, {p.pid})
        r = subprocess.run([sys.executable, PP, "kill", f":{port}"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertTrue(p.dead())

    def test_never_kills_own_ancestry(self):
        r = subprocess.run([sys.executable, PP, "kill", str(os.getpid())], capture_output=True, text=True)
        self.assertEqual(r.returncode, 1)
        self.assertIn("caller", r.stderr)

    def test_a_target_that_names_nothing_is_a_usage_error(self):
        """`pp kill -y "$PAT"` with the variable unset hands pp an empty word, and an empty pattern matches
        every process of the user: the terminal multiplexer, the editor, the agent asking. So a word with no
        pid, no port and no pattern in it is refused where a target is read, before any process is chosen,
        and the refusal names the three things a target can be. `:` is the same word with the port left off
        (`pp kill ":$PORT"`). Only -n is run here: the refusal is what the test is about, and the form that
        signals must never be run against a pp that still takes the word."""
        for t in ("", " ", "\t", ":", ": "):
            r = subprocess.run([sys.executable, PP, "kill", "-n", t], capture_output=True, text=True)
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
            self.assertIn("write a pid, :PORT or a pattern", r.stderr)
            self.assertEqual(r.stdout, "")              # nothing was chosen, so no table was printed
        # a pattern that names a process nobody carries is still a pattern, and says so
        r = subprocess.run([sys.executable, PP, "kill", "-n", "-x", "nothing-carries-this-name"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("no process matches", r.stderr)

    def test_free_port_and_missing_pid(self):
        # held bound and never listened on, for as long as the check runs: nobody else can take the port, and
        # /proc/net shows no listener on it. A port let go of first could be anyone's by now.
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            r = subprocess.run([sys.executable, PP, "kill", f":{s.getsockname()[1]}"],
                               capture_output=True, text=True)
        self.assertEqual(r.returncode, 1)
        self.assertIn("nothing listens", r.stderr)


# a listener on a port it was handed as an open socket: no bind of its own, so nothing can be in the way
LATE_LISTENER = ("import socket, sys, time; s = socket.socket(fileno=int(sys.argv[1])); s.listen(); "
                 "print('ready', flush=True); time.sleep(60)")


class TestTargetsFoundAfterTheSnapshot(unittest.TestCase):
    """`kill :PORT` plans from a process snapshot but resolves the port's owners fresh, so an owner can be
    chosen that the snapshot never held. The gate starts the listener at exactly that point in pp."""

    def test_a_port_owner_younger_than_the_snapshot_is_read_not_a_traceback(self):
        holder = socket.socket()
        holder.bind(("127.0.0.1", 0))            # bound, never listened on: nobody else can take this port
        self.addCleanup(holder.close)
        port = holder.getsockname()[1]
        victim, real = [], proc.processes

        def snapshot_then_listen():
            procs = real()                       # the snapshot pp plans from
            p = journey.start([sys.executable, "-c", LATE_LISTENER, str(holder.fileno())],
                              pass_fds=(holder.fileno(),), ready="ready")
            self.addCleanup(p.close)
            victim.append(p)
            holder.close()                       # the listener alone holds the port now
            self.assertNotIn(p.pid, procs)       # the interleaving: a target from outside the snapshot
            return procs

        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(proc, "processes", snapshot_then_listen):
            with redirect_stdout(out), redirect_stderr(err):
                rc = kill.main([f":{port}", "--grace", "2"])
        self.assertEqual(rc, 0, out.getvalue() + err.getvalue())
        self.assertIn(f"{victim[0].pid}: exited", out.getvalue())
        self.assertTrue(victim[0].dead())


# a victim that forks another child on demand, and survives SIGTERM so it is still there to be asked. It
# catches SIGTERM rather than ignoring it: exec puts a caught signal back to its default, and Popen returns
# after the exec, so the child dies of SIGTERM from the moment its pid is printed. SIG_IGN would survive the
# exec and drop a SIGTERM sent before the child's own startup undid it.
FORKER = """
import signal, subprocess, sys
CHILD = "import time; time.sleep(60)"
signal.signal(signal.SIGTERM, lambda *a: None)
print("ready", flush=True)
while sys.stdin.readline():
    print(subprocess.Popen([sys.executable, "-c", CHILD]).pid, flush=True)
"""


class TestTreeGrowsUnderThePlan(unittest.TestCase):
    """--tree plans the descendants from one snapshot. These gates fork a child at the two points where the
    plan is already made: right after the snapshot, and right after the grace window."""

    def test_a_child_forked_after_the_snapshot_is_signalled_too(self):
        parent = journey.start(["sh", "-c", "sleep 60 & echo ready; read go; sleep 60 & echo forked $!; wait"],
                               stdin=subprocess.PIPE, ready="ready")
        self.addCleanup(parent.close)
        kid, real, gate = [], proc.processes, journey.Gate()

        def snapshot_then_fork():
            procs = real()

            def fork():                          # the competing step, once, at exactly this point
                parent.stdin.write("go\n")
                parent.stdin.flush()
                kid.append(int(parent.line().split()[1]))
                self.addCleanup(journey.kill_pid, kid[0])
                self.assertNotIn(kid[0], procs)
            gate.once(fork)
            return procs

        out = io.StringIO()
        with mock.patch.object(proc, "processes", snapshot_then_fork), redirect_stdout(out):
            rc = kill.main(["--tree", str(parent.pid), "--grace", "2"])
        self.assertEqual((rc, gate.ran), (0, 1), out.getvalue())
        self.assertTrue(parent.dead())
        self.assertIn(f"{kid[0]}: exited", out.getvalue())
        self.assertFalse(proc.alive(kid[0]), "the child forked after the snapshot was left running")

    def test_a_child_forked_while_the_targets_died_is_not_called_gone(self):
        parent = journey.start([sys.executable, "-c", FORKER], stdin=subprocess.PIPE, ready="ready")
        self.addCleanup(parent.close)
        kid, real, gate = [], proc.signal_and_wait, journey.Gate()

        def signal_then_fork(pids, *args, **kw):
            res = real(pids, *args, **kw)         # the grace window has passed, and the plan is finished with

            def fork():
                parent.stdin.write("go\n")
                parent.stdin.flush()
                kid.append(int(parent.line()))
                self.addCleanup(journey.kill_pid, kid[0])
            gate.once(fork)
            return res

        out = io.StringIO()
        with mock.patch.object(proc, "signal_and_wait", signal_then_fork), redirect_stdout(out):
            rc = kill.main(["--tree", "--no-escalate", "--grace", "0.5", str(parent.pid)])
        self.assertIn(f"{parent.pid}: signalled", out.getvalue())      # it ignores SIGTERM on purpose
        self.assertIn(f"{kid[0]}: exited", out.getvalue())
        self.assertFalse(proc.alive(kid[0]), "the child forked during the grace window was left running")
        self.assertEqual((rc, gate.ran), (0, 1), out.getvalue())


class TestProcParsing(unittest.TestCase):
    def test_comm_with_parens_and_spaces(self):
        p = proc.read(os.getpid())
        self.assertEqual(p.pid, os.getpid())
        self.assertGreater(p.start, 0)

    def test_hex_addr(self):
        self.assertEqual(proc._hex_addr("0100007F:1F90"), ("127.0.0.1", 8080))
        self.assertEqual(proc._hex_addr("00000000000000000000000001000000:0050"), ("::1", 80))



class TestShown(unittest.TestCase):
    def test_control_characters_are_escaped_and_long_lines_marked(self):
        self.assertEqual(kill._shown("a\nb\x1b[1Ac\td"), "a\\nb\\x1b[1Ac\\td")
        self.assertEqual(kill._shown("x" * 130), "x" * 119 + "…")
        self.assertEqual(kill._shown("x" * 120), "x" * 120)

if __name__ == "__main__":
    unittest.main()
