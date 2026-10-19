"""Who is calling pp: an agent session, a person's terminal, or nothing where pp started the process itself.

The caller is found once, and the first of these that answers is it:

1. PP_SESSION, set and not empty, which is the name an agent gives its own session;
2. CLAUDE_CODE_SESSION_ID, which one agent harness sets in the environment of every command it runs;
3. no session, where PP_QUEUE_JOB is in the environment: pp started this process (a job, its runner, a hook, a
   stop command), and a job is no session;
4. the process pp runs under: the nearest ancestor of pp that is neither a shell nor a wrapper, named
   `<comm>:<pid>:<boot>:<start>`, so a pid reused later, or reused after a reboot, is another session. pid 1, an
   init, and an ancestor pp cannot read, are no session. The ancestry is read once in a process, since pp's own
   parent can end while pp runs.

Rule 4 is what makes `--mine` work with nothing set: every command an agent runs is a child of the agent's own
process, and a person's terminal (the terminal program, the tmux server, the ssh connection) is a session the
same way. Its limit is that a program between the agent and pp, a script that runs `pp queue add`, is the
process pp finds, so that program's jobs are its own session's.

An init is no session because it called nothing: pp reaches one by being reparented, which the kernel does when
pp's own parent ends, and a pp left in the background by a shell that has returned is such a pp. `--mine` there
names nothing, and says so, instead of naming the init as the caller and answering for its jobs.

pp records the session it found as a queued job's owner, so `pp queue wait --mine` finds the jobs a session
queued. It strips every variable above from anything it starts (workers, jobs, hooks, stop commands): a process
that inherits a session's identity looks, to itself and to every tool it runs, as if it were inside that
session, and a job does not inherit the identity of whoever queued it.
"""
from __future__ import annotations

import functools
import os

# a session's identity, never configuration: CLAUDE_CODE_USE_BEDROCK and friends are auth settings and stay
SESSION_VARS = ("PP_SESSION", "CLAUDECODE", "AI_AGENT", "CLAUDE_PID", "CLAUDE_EFFORT", "CLAUDE_CODE_SESSION_ID",
                "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ATTENDED", "CLAUDE_CODE_MESSAGING_SOCKET",
                "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_EXECPATH", "TMUX_PANE")

# The processes a session runs a command through, which are therefore not the session: a shell it spells the
# command for, and a wrapper that only changes how the command runs. Named once, here.
SHELLS = ("sh", "bash", "zsh", "dash", "ksh", "mksh", "ash", "fish", "tcsh", "csh")
WRAPPERS = ("timeout", "env", "nice", "nohup", "stdbuf", "setsid", "ionice", "time")
# The processes that run a command nobody spelled: the init a process whose parent has ended is reparented to,
# whatever its pid is. The user's init is one of these and is not pid 1.
INITS = ("systemd", "init", "launchd")


def walk(ancestry: list, boot: str) -> str | None:
    """The session the process pp runs under names, or None where there is none.

    ancestry is (comm, pid, start) for each ancestor of pp from its parent upward, and None for an ancestor
    that cannot be read. A pure function of that list: the nearest entry that is neither a shell nor a wrapper,
    as `<comm>:<pid>:<boot>:<start>` with the first 8 characters of boot. pid 1, an init, an ancestor that
    cannot be read, and a chain that holds no such process are each no session."""
    for entry in ancestry:
        if entry is None:
            return None
        comm, pid, start = entry
        if pid <= 1 or comm in INITS:
            return None
        if comm not in SHELLS and comm not in WRAPPERS:
            return f"{comm}:{pid}:{boot[:8]}:{start}"
    return None


@functools.cache
def _found() -> str | None:
    """Rule 4 carried out, once in a process: the ancestors pp reads, handed to the walk. The reading is here
    and the rule is there, so the rule is a table and this is the one place /proc is read for a session.

    Once, because the process pp runs under can end while pp runs, and the kernel then gives pp an init as its
    parent: a `wait` an agent leaves running in the background outlives the shell that started it. The session
    it selected jobs by is the session it stamps their outcomes with, so every answer one pp process gives is
    one session's."""
    from pp.lib import proc       # proc reads this module, so the one import of it is here
    ancestry = [None if p is None else (p.comm, p.pid, p.start)
                for p in (proc.read(pid) for pid in proc.ancestors(os.getppid()))]
    return walk(ancestry, proc.boot_id())


def caller(env: dict | None = None) -> dict | None:
    """The calling session, {session, cwd}, or None where there is none.

    The four rules of this module's docstring, first match winning: the two variables, then the job pp started,
    then the process pp runs under."""
    env = os.environ if env is None else env
    sid = env.get("PP_SESSION") or env.get("CLAUDE_CODE_SESSION_ID")
    if not sid and "PP_QUEUE_JOB" not in env:
        sid = _found()
    return {"session": sid, "cwd": os.getcwd()} if sid else None


def scrubbed_env(env: dict | None = None) -> dict:
    """A copy of env without any session's identity."""
    env = dict(os.environ if env is None else env)
    for k in SESSION_VARS:
        env.pop(k, None)
    return env
