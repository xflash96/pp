"""Who is calling pp: a person's shell, or an agent session.

Claude Code puts its session's identity in the environment of every command it runs (CLAUDE_CODE_SESSION_ID,
CLAUDE_PID, and TMUX_PANE when it runs inside tmux). pp records that identity as a queued job's owner, so
`pp queue wait --mine` finds the jobs a session queued. It strips it from anything pp starts on its own (workers,
jobs, hooks): a process that inherits a session's identity looks, to itself and to every tool it runs, as if it
were inside that session.

PP_SESSION is that identity said by any agent: where it is set and not empty it names the calling session, in
place of CLAUDE_CODE_SESSION_ID. It is a session's identity like the others, so pp strips it from what it
starts too, and a job does not inherit the identity of whoever queued it.
"""
from __future__ import annotations

import os

# a session's identity, never configuration: CLAUDE_CODE_USE_BEDROCK and friends are auth settings and stay
SESSION_VARS = ("PP_SESSION", "CLAUDECODE", "AI_AGENT", "CLAUDE_PID", "CLAUDE_EFFORT", "CLAUDE_CODE_SESSION_ID",
                "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ATTENDED", "CLAUDE_CODE_MESSAGING_SOCKET",
                "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_EXECPATH", "TMUX_PANE")


def caller(env: dict | None = None) -> dict | None:
    """The calling agent session, {session, cwd}, or None when a person (or a script) calls.

    PP_SESSION is the name an agent gives its own session, and it wins; an empty one is no name at all."""
    env = os.environ if env is None else env
    sid = env.get("PP_SESSION") or env.get("CLAUDE_CODE_SESSION_ID")
    return {"session": sid, "cwd": os.getcwd()} if sid else None


def scrubbed_env(env: dict | None = None) -> dict:
    """A copy of env without any session's identity."""
    env = dict(os.environ if env is None else env)
    for k in SESSION_VARS:
        env.pop(k, None)
    return env
