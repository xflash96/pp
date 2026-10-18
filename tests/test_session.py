import json
import os
import subprocess
import sys
import unittest

from pp.lib import proc, session

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOOT = "0123456789abcdef-boot"
# a child that prints what caller() makes of the chain it is in, with no variable of its own to go by
SAYS_ITS_CALLER = "import json;from pp.lib import session;print(json.dumps(session.caller({})))"


class TestTheWalk(unittest.TestCase):
    """The rule for the process pp runs under, over the ancestry it is handed: one row per case."""

    def test_the_nearest_process_that_is_neither_a_shell_nor_a_wrapper_is_the_session(self):
        rows = (
            ("a shell is passed over", [("bash", 400, 11), ("agent", 300, 22)], "agent:300:01234567:22"),
            ("a wrapper is passed over", [("timeout", 400, 11), ("agent", 300, 22)], "agent:300:01234567:22"),
            ("only shells, up to pid 1", [("bash", 400, 11), ("zsh", 90, 7), ("systemd", 1, 0)], None),
            ("an ancestor that cannot be read", [("dash", 400, 11), None, ("agent", 300, 22)], None),
            ("the nearest one wins", [("python3", 400, 11), ("agent", 300, 22)], "python3:400:01234567:11"),
        )
        for what, ancestry, want in rows:
            with self.subTest(what):
                self.assertEqual(session.walk(ancestry, BOOT), want)

    def test_a_real_chain_through_a_shell_names_the_process_that_started_it(self):
        """This test process -> sh -c -> python: the shell is passed over and this process is the session. The
        `; :` keeps the shell in the chain, which `sh -c CMD` on its own is free to exec away."""
        me = proc.read(os.getpid())
        r = subprocess.run(["/bin/sh", "-c", f'{sys.executable} -c "$PY" ; :'], capture_output=True, text=True,
                           cwd=ROOT, timeout=60, env={"PYTHONPATH": ROOT, "PY": SAYS_ITS_CALLER})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)["session"], f"{me.comm}:{me.pid}:{proc.boot_id()[:8]}:{me.start}")


class TestSession(unittest.TestCase):
    def test_a_job_pp_started_is_no_session(self):
        self.assertIsNone(session.caller({"PP_QUEUE_JOB": "7"}))

    def test_an_agent_session_is_its_id_and_where_it_ran(self):
        env = {"CLAUDE_CODE_SESSION_ID": "abc-123", "CLAUDE_PID": str(os.getpid()), "TMUX_PANE": "%7"}
        self.assertEqual(session.caller(env), {"session": "abc-123", "cwd": os.getcwd()})

    def test_scrub_keeps_configuration(self):
        env = {"CLAUDECODE": "1", "CLAUDE_CODE_SESSION_ID": "s", "CLAUDE_CODE_MESSAGING_TOKEN": "t", "TMUX_PANE": "%1",
               "CLAUDE_CODE_USE_BEDROCK": "1", "TMUX": "/tmp/tmux-1/default,1,0", "PATH": "/bin"}
        out = session.scrubbed_env(env)
        self.assertEqual(out, {"CLAUDE_CODE_USE_BEDROCK": "1", "TMUX": "/tmp/tmux-1/default,1,0", "PATH": "/bin"})
        self.assertIn("CLAUDECODE", env)     # a copy, never the caller's dict


if __name__ == "__main__":
    unittest.main()
