"""User journeys through `pp kill`: a person (or an agent) frees a port, clears out stray processes by pid, name
or pattern, and deals with ones that will not die. Every target here is a process the test started itself,
marked with a token no other process on the machine carries."""
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from journey import PP, Journey  # noqa: E402

SLEEPER = "import sys, time; print('ready', flush=True); time.sleep(120)"
STUBBORN = ("import signal, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "signal.signal(signal.SIGUSR1, lambda *a: print('usr1', flush=True)); print('ready', flush=True); "
            "time.sleep(120)")
# it binds port 0 and says which port it got: a number chosen for it to bind later could be taken in between
LISTENER = ("import socket, sys, time; s = socket.socket(); s.bind(('127.0.0.1', 0)); s.listen(); "
            "print(s.getsockname()[1], flush=True); time.sleep(120)")


class KillJourney(Journey):
    def setUp(self):
        super().setUp()
        self.tok = f"ppj-{secrets.token_hex(6)}"

    def start(self, argv, ready="ready"):
        return self.child(argv, stderr=subprocess.DEVNULL, ready=ready)

    def victim(self, code=SLEEPER, *extra):
        return self.start([sys.executable, "-c", code, self.tok, *extra])

    def merged(self, *args) -> subprocess.CompletedProcess:
        """pp with stderr folded into stdout, the order a person sees them in a terminal."""
        return subprocess.run([sys.executable, PP, *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              stdin=subprocess.DEVNULL, text=True, env=self.env, cwd=self.work, timeout=30)


class TestKillJourneys(KillJourney):
    def test_a_pid_looked_at_then_killed_then_gone(self):
        p = self.victim()
        r = self.pp("kill", "-n", str(p.pid))
        self.assertIn(f"pid {p.pid}", r.stdout)     # the command column stops at 120 characters, before the token
                                                     # when the interpreter's path is long (a uv-managed Python's)
        self.assertIsNone(p.poll())
        r = self.pp("kill", str(p.pid))                              # an explicit pid acts without asking
        self.assertIn(f"{p.pid}: exited", r.stdout)
        self.assertTrue(p.dead())
        self.assertIn(f"no process {p.pid}", self.pp("kill", str(p.pid), code=1).stderr)
        self.assertIn("not killing it", self.pp("kill", str(os.getpid()), code=1).stderr)    # the caller's ancestry

    def test_free_a_port(self):
        p = self.start([sys.executable, "-c", LISTENER, self.tok], ready=None)
        port = int(p.line())
        r = self.pp("kill", "--ports")
        line = next(ln for ln in r.stdout.splitlines() if f":{port} " in ln + " ")
        self.assertIn(f"{p.pid} ", line)
        js = json.loads(self.pp("kill", "--ports", "--json").stdout)
        self.assertIn(p.pid, next(s for s in js if s["port"] == port)["pids"])
        self.assertIn(f"{p.pid} ", self.pp("kill", "-n", f":{port}").stdout)
        self.assertIsNone(p.poll())
        self.assertIn("exited", self.pp("kill", f":{port}").stdout)
        self.assertTrue(p.dead())
        # a port this test holds bound and never listens on, for as long as pp runs: no process on the machine
        # can bind it, so pp's answer is about a port nobody can be listening on. Asking about the port just
        # freed would ask about one anything else may have taken in between.
        with socket.socket() as hold:
            hold.bind(("127.0.0.1", 0))
            r = self.pp("kill", f":{hold.getsockname()[1]}", code=1)
        self.assertIn("nothing listens", r.stderr)
        self.assertIn("1-65535", self.pp("kill", ":70000", code=2).stderr)

    def test_pattern_shows_asks_and_spares_the_shell_that_asked(self):
        a, b = self.victim(), self.victim()
        r = self.pp("kill", "-n", self.tok)
        self.assertIn(str(a.pid), r.stdout)
        self.assertIn(str(b.pid), r.stdout)
        # no terminal: the matches, then the refusal, in that order, and nobody dies
        r = self.merged("kill", self.tok)
        self.assertEqual(r.returncode, 2)
        self.assertLess(r.stdout.index(str(a.pid)), r.stdout.index("pass -y"))
        self.assertIsNone(a.poll())
        # the pkill -f trap: the token is in this shell's own command line, and the shell must live
        r = subprocess.run(["sh", "-c", f"{sys.executable} {PP} kill -y {self.tok}; echo shell-survived"],
                           capture_output=True, text=True, env=self.env, timeout=30)
        self.assertIn("shell-survived", r.stdout, r.stderr)
        self.assertTrue(a.dead() and b.dead())
        r = self.pp("kill", "-y", self.tok, code=1)
        self.assertIn("no process matches", r.stderr)

    def test_exact_name_even_past_the_kernels_15_characters(self):
        name = f"{self.tok}-long-worker"                         # comm keeps only the first 15 characters
        exe = shutil.copy(shutil.which("sleep"), os.path.join(self.bin, name))
        p = self.start([exe, "120"], ready=None)
        def execed():
            with open(f"/proc/{p.pid}/cmdline") as f:
                return f.read().startswith(exe)
        self.assertTrue(self.until(execed, 5))
        self.pp("kill", "-x", "-n", name[:15] + "zz", code=1)    # not a prefix match
        self.assertIn(str(p.pid), self.pp("kill", "-x", "-n", name).stdout)
        self.pp("kill", "-x", "-y", name)
        self.assertTrue(p.dead())

    def test_a_tree(self):
        p = self.start(["sh", "-c", f"sleep 120 & sleep 121 & sh -c 'sleep 122' & echo ready; wait # {self.tok}"])
        kids = self.until(lambda: len([ln for ln in self.pp("kill", "-n", "--tree", str(p.pid)).stdout.splitlines()
                                       if "child of" in ln]) == 4)
        self.assertTrue(kids)
        r = self.pp("kill", "--tree", str(p.pid))
        self.assertEqual(r.stdout.count(": exited"), 5, r.stdout)
        self.assertTrue(p.dead())
        self.pp("kill", "-n", self.tok, code=1)                   # nothing of it is left

    def test_stubborn_processes_and_signals_meant_to_be_survived(self):
        p = self.victim(STUBBORN)
        r = self.pp("kill", "--grace", "0.3", "--no-escalate", str(p.pid))
        self.assertIn("signalled", r.stdout)
        self.assertIsNone(p.poll())
        # a signal it is meant to survive is sent alone: no waiting, no SIGKILL afterwards
        t = time.monotonic()
        r = self.pp("kill", "-s", "USR1", str(p.pid))
        self.assertLess(time.monotonic() - t, 2.5)
        self.assertIn("signalled", r.stdout)
        self.assertEqual(p.line(), "usr1")
        self.assertIsNone(p.poll())
        r = self.pp("kill", "-s", "NOPE", str(p.pid), code=2)       # a typo fails before anything is shown
        self.assertEqual(r.stdout, "")
        self.assertIn("unknown signal", r.stderr)
        r = self.pp("kill", "--grace", "0.3", str(p.pid))            # TERM ignored -> SIGKILL after the grace
        self.assertIn("killed", r.stdout)
        self.assertTrue(p.dead())

    def test_the_help_says_what_the_exit_statuses_and_the_pkill_trap_really_are(self):
        """The help is what a person or an agent reads to decide what an exit status means, so it says what
        the code does: a signal meant to be survived exits 0, which the step above runs. The exit status of a
        shell that killed itself is 128 + SIGTERM."""
        out = self.pp("help", "kill").stdout
        self.assertIn("or signalled", out)                           # exit 0 with the target alive
        self.assertIn("exit 143", out)
        self.assertNotIn("144", out)
        p = self.victim(STUBBORN)
        self.assertIn("signalled", self.pp("kill", "-s", "USR1", str(p.pid), code=0).stdout)
        self.assertIsNone(p.poll())
        self.assertEqual(self.sh("sh -c 'kill $$'; echo $?").strip(), "143")

    def test_json_is_always_one_object(self):
        p = self.victim()
        js = json.loads(self.pp("kill", "-n", "--json", str(p.pid)).stdout)
        self.assertEqual(js["pids"], {str(p.pid): f"pid {p.pid}"})
        self.assertIn(self.tok, js["commands"][str(p.pid)])          # the whole command line, never cut
        js = json.loads(self.pp("kill", "--json", str(p.pid), "999999999", code=1).stdout)
        self.assertEqual(js["pids"], {str(p.pid): "exited"})
        self.assertEqual(js["problems"], ["no process 999999999"])
        js = json.loads(self.pp("kill", "--json", self.tok, code=1).stdout)    # nothing matched: still JSON
        self.assertEqual(js["pids"], {})

    def test_another_users_process_is_flagged_when_looked_at(self):
        if os.getuid() == 0:
            self.skipTest("root may signal anyone")
        mine = set()
        pid = os.getpid()
        while pid > 1:                                               # never even look at our own ancestry
            mine.add(pid)
            with open(f"/proc/{pid}/stat") as f:
                pid = int(f.read().rsplit(")", 1)[1].split()[1])
        def roots():
            for d in os.listdir("/proc"):
                try:
                    if d.isdigit() and int(d) > 1 and int(d) not in mine and os.stat(f"/proc/{d}").st_uid == 0:
                        with open(f"/proc/{d}/cmdline") as f:
                            if f.read():                             # a real program, not a kernel thread
                                yield int(d)
                except OSError:
                    pass
        root = next(roots(), None)
        if root is None:
            self.skipTest("no root process visible")
        r = self.pp("kill", "-n", str(root))                         # -n only: never signal it
        self.assertIn("signalling them is denied unless pp runs as root", r.stderr)


if __name__ == "__main__":
    import unittest
    unittest.main()
