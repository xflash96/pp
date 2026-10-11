import os
import unittest

from pp.lib import session


class TestSession(unittest.TestCase):
    def test_person_has_no_caller(self):
        self.assertIsNone(session.caller({"PATH": "/bin"}))

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
