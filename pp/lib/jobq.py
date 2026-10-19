"""The job-queue engine behind `pp queue`, and its CLI (run_cli).

The store is pp/lib/jobstore.py (one file per job, and the transaction that changes one) and the rules are
pp/lib/jobpolicy.py (the facts in, one decision out). Here: the verbs, the views, the readings a limit is
checked against, the one declaration of every setting and the files each is read from, and the help.

One worker per queue reaps ended jobs, carries out the stops asked of them, re-queues retries, starts what
fits, and fires the job's on_fail / on_done hook on its FINAL outcome. Admission is strict head-of-line by
priority (a big job is never starved by small ones) unless the queue's own backfill is on; a job waiting out its
retry delay is skipped, not waited for.

A queue's worker is the one actor that signals that queue's jobs, and only a worker ever holds the queue's
worker claim. `pp queue kill` writes a stop on the job ({by, t, grace}) and waits for the record of how the job
ended. It makes sure the queue has a worker: it wakes the one that holds the claim, or starts one where nobody
does. A timeout is the worker writing a stop on the job itself. So a kill, a timeout and a reap never cross,
and one actor records how a job ended.

One construct makes sure a queue has a worker and says what that worker is (`worker_up`, and `worker_now` for a
verb that only reports). It is the one place that reads the claim's holder. Every verb prints from the one
answer it gets back, so no verb acts on a look it took outside the scope that acted.

One function says what is to be done about a running job: `jobpolicy.decide(job, facts)`, from the job's record and the
facts read with it, giving back the one action to carry out. It is pure. `Worker.carry_out` is the one pass
over the queue's running jobs that gathers those facts, asks it, and carries the action out. So a timeout, a
kill and a reap are facts about one job and one decision made from all of them.

Being alive is holding a claim (pp/lib/lock.py), never a pid in a file: the queue's worker holds the claim on
worker.lock while it runs, one run of a job is held by that run's own wrapper, and one run of a probe is held by
whoever runs it. The kernel lets a claim go when its holder ends, however it ends, so a worker or a job that was
killed leaves nothing to clean up and nothing that reads as alive.

The caller's session identity (pp.lib.session) is recorded as the job's owner, so `wait --mine` finds the jobs a
session queued, and stripped from the worker, the jobs and the hooks.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime

from pp.lib import config, jobcore, jobpolicy as policy, jobstore, lock, paths, proc, session, ui

CAPTURED_ENV = ("PATH", "VIRTUAL_ENV", "CONDA_PREFIX", "CONDA_DEFAULT_ENV")
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
LAUNCH_BANNER = re.compile(r"\[pp queue\] \S+ \S+ job \d+ \(")     # the line _spawn writes before each attempt
KILL_POLL = 0.1             # how often `kill` looks for the outcome that is recorded
# how long `kill` waits for that outcome, past the grace it asked for. Every pass of that wait wakes the
# worker that is up, so its next tick is that pass. What this bounds is the job's own exit time, and not the
# queue's poll period, whatever that is set to. Where no worker is up, a pass starts a round of them and
# waits for each start in it to say it is ready, so such a pass can carry the wait past this bound by that
# round's ready waits; `kill` says how long it really waited.
KILL_WAIT = 120.0

_PROG = "queue"             # the command running the CLI: names it in messages and starts its workers


def _by() -> str:
    """Who asked, as a record names them: the calling agent session, or the user."""
    who = session.caller()
    return who["session"] if who else "user"


def add_note(jid: int, text: str, by: str | None = None) -> dict | None:
    """Append a note to a job in any state; the updated job, or None when there is no such job."""
    note = {"t": time.time(), "text": text, "by": _by() if by is None else by}
    return jobcore.amend(jid, {"notes": [note]})


# ---- the worker, as this command wires it up -----------------------------------------------------------
#
# The core (pp/lib/jobcore.py) holds the claims, the processes and the loop, and it reads no config and writes
# no sentence. So three things are read and said here: the settings one pass of the loop runs under, the
# admission of one launch pass, and the words for what the worker did.

def _starting(q: str) -> str:
    """What a person is told while a worker holds q's claim and has not recorded itself yet."""
    return f"the {q} worker is starting and has not recorded itself yet"


def _read_settings(q: str):
    """What one pass of q's worker loop runs under: every per-queue row, read once per pass, so a queue's
    limits change under its running jobs with no restart. None of them has a flag, for that reason.

    A config that cannot be read comes back as `error` beside the defaults, and says nothing about backfill,
    so a worker keeps running the way it was until that file is fixed."""
    rows = [r for r in BY_SCOPE["queue"].values() if r.name != "resources"]

    def read():
        try:
            cfg = _qcfg(q)
        except config.ConfigError as e:
            return {"poll": policy.POLL, "idle_exit": policy.IDLE_EXIT, "error": str(e)}
        return {r.name: policy.resolve(r, config=cfg.get(r.name, policy.NOTSET)) for r in rows}
    return read


def _admit_for(q: str):
    """The admission of one launch pass: the readings are taken here, outside the store lock, and handed to
    the pass as a value (`policy.admit` is the arithmetic over them)."""
    def admit_for(kinds):
        return Resources(q, kinds, empty_ok=False).seen
    return admit_for


def _voice(q: str, log=None):
    """The sink the worker tells what it did, worded: one line of the queue's log per event, and for a retry
    one line in the job's own log too. Every sentence a worker says is here, and none of it in the core.

    log is where a line goes; a worker nobody gave one to says nothing, which is what a test that drives a
    tick by hand wants."""
    say = log or (lambda line: None)

    def voice(kind, job, f):
        jid = None if job is None else job["id"]
        tag = None if job is None else _name(job)
        if kind == "timeout":
            say(f"timeout {jid} {tag} after {ui.human_duration(float(job['timeout']))}: stopping it")
        elif kind == "again":
            _append_log(job, _retry_line(job))      # the job's own log, before the queue's
            say(f"retry {jid} {tag}: attempt {job['attempt']}/{int(job.get('retries') or 0) + 1} "
                f"in {_in_delay(job['not_before'])}")
        elif kind == "ended":
            why = f" ({job['reason']})" if job.get("reason") else ""
            say(f"{'done' if job['state'] == 'done' else 'FAILED'} {jid} {tag} "
                f"rc={job.get('rc')}{why} {ui.human_duration(job.get('seconds'))}")
        elif kind == "stop":
            word = (f"the project's stop command: {_text(f['cmd'])}" if f.get("cmd") else
                    "SIGTERM to its group" if f["sig"] == signal.SIGTERM else
                    f"SIGKILL after {ui.human_duration(float(f['grace'] or 0))}")
            say(f"stop {jid} {tag} ({(job.get('stop') or {}).get('by')}): "
                + (f"not allowed to signal its group {f['group']}" if f["outcome"] == "denied" else word))
        elif kind == "stop-failed":
            say(f"the stop command of {jid} {tag} could not start: {f['error']}")
        elif kind == "group-gone":
            say(f"nothing to signal in job {jid}'s group {f['group']}: it is gone")
        elif kind == "adopted":
            say(f"adopted {jid} {tag}: its run is pid {f['pid']}, from its claim")
        elif kind == "hook-failed":
            say(f"hook {f['event']} of {jid} could not start: {f['error']}")
        elif kind == "hook":
            say(f"hook {f['event']} of {jid}: {f['cmd']} (log {f['log']})")
        elif kind == "launch-failed":
            say(f"FAILED {jid} {tag}: launch failed: {f['error']}")
        elif kind == "ended-elsewhere":
            say(f"job {jid} ended elsewhere while it started; stopped its process {f['pid']}")
        elif kind == "start":
            retries = int(job.get("retries") or 0)
            attempt = f" attempt {job.get('attempt', 1)}/{retries + 1}" if retries else ""
            say(f"start {jid} {tag} [{job.get('project')}] {_needs_str(job.get('needs'))}"
                + (f" on GPU{f['card']}" if f["card"] is not None else "") + attempt)
        elif kind == "waiting":
            say(f"waiting: {jid} {tag} for {f['why']}")
        elif kind == "up":
            say(f"worker up, pid {f['pid']}" + (", backfill" if f["backfill"] else "")
                + (f", adopting {f['adopting']} running job(s)" if f["adopting"] else ""))
        elif kind == "backfill":
            say(f"backfill {'on' if f['on'] else 'off'} (config)")
        elif kind == "tick-failed":
            say("tick failed:\n" + f["trace"])
        elif kind == "config":
            say(f"bad config, nothing launches until it is fixed: {f['error']}" if f["error"] else "config fixed")
        elif kind == "idle":
            say(f"idle for {f['idle_exit']:g}s, exiting")
        elif kind == "stopped":
            say("stopped; running jobs keep running and the next worker adopts them")
        elif kind == "busy":    # another worker holds the claim, so this one is not the queue's
            print(f"pp {_PROG}: the {q} worker is already running (pid {f['pid']})" if f["how"] == jobcore.UP
                  else f"pp {_PROG}: {_starting(q)}, so this worker is not the one" if f["how"] == jobcore.STARTING
                  else f"pp {_PROG}: another worker held the {q} worker's claim and has let it go")
    return voice


def worker_for(q: str, log=None) -> jobcore.Worker:
    """Queue q's worker, wired: the admission of a pass, the tail a retry is matched against, and the sink it
    says what it did to. The one place a Worker is built."""
    return jobcore.Worker(q, False, _voice(q, log), lambda job, n: "\n".join(log_tail(job, n)), _admit_for(q))


def cmd_work(q: str) -> int:
    """`work`: queue q's worker in this process. The loop is the core's; the settings, the admission and the
    words are read and said here."""
    def log(msg):
        print(f"[{q} {datetime.now():%m-%d %H:%M:%S}] {msg}", flush=True)
    return jobcore.work(q, _read_settings(q), _admit_for(q), _voice(q, log),
                        lambda job, n: "\n".join(log_tail(job, n)))


def _worker_up(q: str, wake: bool = False, quiet: bool = False) -> jobcore.Holder:
    """Make sure q has a worker and say what came of it: the one place the core's answer is worded. A caller
    that asked quietly says it itself, from the answer it was given."""
    w = jobcore.worker_up(q, wake=wake)
    if quiet:
        return w
    if w.how == jobcore.STARTED:
        print(f"pp {_PROG}: started the {q} worker (pid {w.pid}; log {jobcore.worker_log(q)})", file=sys.stderr)
    elif w.how == jobcore.STARTING:
        print(f"pp {_PROG}: {_starting(q)}", file=sys.stderr)
    elif w.tried:
        ui.warn(f"{_PROG}: {w.why or f'the {q} worker did not come up'}; its log: {jobcore.worker_log(q)}")
    return w


# ---- the settings, one row each -----------------------------------------------------------------------
#
# Every setting a user can give is one row below, and every reading of that setting goes through its row. A
# row says its type and spelling, its default, its scope, its flag and its keys, and what it refuses
# (pp/lib/jobpolicy.py, Setting). The scope decides which sources a row has and nothing else does: a per-job
# row has a flag on `add` and a key in both config tables, a per-queue row the queue's key alone, the
# machine's rows the machine's table alone, and a per-project row its project's key and a flag for one job.
#
# One declaration feeds the flag's entry in the parser (`_flag`) and the checking and reading of every
# config table (`_read`), and one function reads a value for both (`jobpolicy.value`). The order the sources
# are read in is `jobpolicy.resolve`, the same order for every row. So a setting has one declaration, one
# default and one precedence, and a flag and a key cannot disagree about what a setting is.

PROBE_WAIT = 60.0   # how long a probe may take before pp ends it and its reading says so
PROBE_EVERY = 30.0
SETTLE = 30.0       # a reading (nvidia-smi, a probe) may not show a job launched less than this before it

_S = policy.Setting
SETTINGS = (
    # Per project: the nearest .pp.toml up from the job's directory. `name` is the one row with no flag,
    # because it names the project and not a job of it; `runner` and `stop` are the project's own commands.
    _S("name", "project", "text", top=True, flagged=False, refuses="not a string"),
    _S("runner", "project", "argv", metavar="CMD",
       says="run this job through it: RUNNER CMD... (a path holding / is relative to the project root)",
       refuses="a path that does not exist, is not executable, or is not on PATH, at add time"),
    _S("stop", "project", "shell", metavar="CMD",
       says='how a stop reaches the work the runner started; "group" where that work is in the job\'s own '
            "process group", refuses="an empty string; a non-string"),

    # Per job: `add`'s flags, `[queue]` in .pp.toml, `[queue.NAME]` in config.toml. The resolved value goes
    # on the record, so a later config edit cannot change a job already queued or running.
    _S("prio", "job", "whole", default=policy.PRIO, printed=True, metavar="N",
       says="lower runs first (default 10)", refuses="not an integer"),
    _S("gpu_gb", "job", "amount", field="needs", metavar="N",
       says="GPU memory to reserve on one card (the job's peak use, plus a margin)",
       refuses="0, a negative, nan, inf; a kind no pool provides"),
    _S("cpu", "job", "amount", field="needs", metavar="N", says="CPUs to reserve",
       refuses="0, a negative, nan, inf; a kind no pool provides"),
    _S("mem_gb", "job", "amount", field="needs", metavar="N", says="RAM to reserve, in GB",
       refuses="0, a negative, nan, inf; a kind no pool provides"),
    _S("need", "job", "counts", default={}, field="needs", merge=True, repeat=True, metavar="NAME=N",
       says="a counter from [resources] (every queue's) or [queue.Q.resources] (one queue's own)",
       refuses="0, a negative, nan, inf; a name no pool provides, named at add time"),
    _S("env", "job", "strings", default={}, merge=True, repeat=True, metavar="K=V",
       says="set in the job's environment; wins over the project's [queue] env",
       refuses="a name holding =, NUL or a newline; a value holding a newline"),
    _S("log", "job", "path", field="log_pattern", metavar="PATH",
       says="where this job's output goes: {tag} and {id} are filled in, under the project root",
       refuses="another placeholder; '..'; a relative path that leaves the project root"),
    _S("timeout", "job", "duration", none_ok=True, metavar="DUR",
       says="stop it after DUR (90s, 30m, 2h, 1d); none: no timeout",
       refuses='""; a non-finite value; over 1e9 seconds'),
    _S("retries", "job", "whole", default=0, printed=True, lo=0, hi=policy.RETRIES_CAP, metavar="N",
       says="automatic retries of a failure (default 0)",
       refuses="a negative, a non-integer, over 1000"),
    _S("retry_delay", "job", "duration", default=policy.RETRY_DELAY, printed=True,
       hi=policy.RETRY_DELAY_CAP, metavar="DUR",
       says="delay before the first retry, doubling after (default 30s, at most 1h)",
       refuses='""; a non-finite value; over the 1h cap'),
    _S("retry_if", "job", "regex", metavar="REGEX",
       says="retry only failures whose reason or log tail matches",
       refuses="a regex that does not compile"),
    _S("on_fail", "job", "command", metavar="CMD",
       says="shell command run on the final failure; none: no hook", refuses="a non-string"),
    _S("on_done", "job", "command", metavar="CMD",
       says="shell command run on success; none: no hook", refuses="a non-string"),
    _S("grace", "job", "duration", default=policy.GRACE, printed=True, metavar="DUR",
       says="between a stop of the job and the SIGKILL that follows it (default 10s)",
       refuses='""; a non-finite value'),

    # Per queue: `[queue.NAME]` in config.toml, and no flag. Its worker re-reads the table every loop, so a
    # limit changes under running jobs; a flag would freeze what config can otherwise change.
    _S("poll", "queue", "period", default=policy.POLL, printed=True, refuses="0 or less"),
    _S("idle_exit", "queue", "duration", default=policy.IDLE_EXIT, printed=True,
       refuses="a non-finite value"),
    _S("backfill", "queue", "boolean", default=False, printed=True, refuses="a non-boolean"),
    _S("resources", "queue", "pools", default={"slots": 3}, printed=True, merge=True,
       refuses="an unknown key in a pool table; probe on gpu_gb, since the cards are the machine's"),

    # The machine: `[resources]` in config.toml, and no flag. One machine has one set of limits, and every
    # queue counts against them. The four below are the keys of one pool's table.
    _S("resources", "machine", "pools", default={"cpu": "auto", "mem_gb": "auto", "gpu_gb": "auto"},
       printed=True, merge=True,
       refuses='an unknown key in the table; "auto" for a name that is not cpu, mem_gb or gpu_gb; '
               "a table with neither max nor probe, except gpu_gb"),
    _S("max", "machine", "number", default="auto", auto_ok=True, refuses="not a number"),
    # gpu_gb's probe prints the machine's cards in nvidia-smi's own format and stands in for it. One machine
    # has one set of cards, so [queue.NAME.resources] gpu_gb refuses a probe (jobpolicy.cards, section 16.1).
    _S("probe", "machine", "shell", refuses="an empty string; probe on a queue's own gpu_gb"),
    _S("every", "machine", "duration", default=PROBE_EVERY, printed=True, refuses="a non-finite value"),
    _S("settle", "machine", "duration", default=SETTLE, printed=True, refuses="a non-finite value"),
)

BY_SCOPE = {s: {r.name: r for r in SETTINGS if r.scope == s}
            for s in ("job", "queue", "machine", "project")}
JOB_ROWS = tuple(BY_SCOPE["job"].values())
POOL_KEYS = tuple(n for n in BY_SCOPE["machine"] if n != "resources")
QUEUE_KEYS = tuple(BY_SCOPE["job"]) + tuple(BY_SCOPE["queue"])
PROJECT_KEYS = tuple(BY_SCOPE["job"]) + tuple(n for n, r in BY_SCOPE["project"].items() if not r.top)
# the rows of the project's own table that have a flag on `add`, resolved by the command that carries them out
PROJECT_ROWS = tuple(r for r in BY_SCOPE["project"].values() if r.flagged)


def _read(t: dict, where: str, rows: dict) -> dict:
    """What one config table says, each key read through its own row: the typed value of every key it holds.

    A value a row refuses is a ConfigError naming the table and the key, never a silent default and never a
    traceback. A key no row of the declaration names is left to the one reader that knows it."""
    out = {}
    for k, v in t.items():
        if k not in rows:
            continue
        try:
            out[k] = policy.value(rows[k], v)
        except ValueError as e:
            raise config.ConfigError(f"{where} {k}: {e}") from None
    return out


def _typed(row: policy.Setting):
    """The type argparse reads this row's flag with: the row's own reading, and a usage error in the parser's
    own words where the row refuses it. So a flag and a key refuse the same values for the same reasons."""
    def read(raw):
        try:
            return policy.value(row, raw)
        except ValueError as e:
            raise argparse.ArgumentTypeError(str(e)) from None
    return read


def _dur_flag(unit: str = "s"):
    """The type of a duration flag that is a setting of nothing: a view's own bound (`wait --timeout`,
    `clean --older`). One reading of a duration, the same one every row takes."""
    def read(raw):
        try:
            return policy.duration(raw, unit)
        except ValueError as e:
            raise argparse.ArgumentTypeError(str(e)) from None
    return read


def _tag_flag(raw: str) -> str:
    """--tag's value, checked where it is read. A tag is an argument of `add` and not a setting (section 2),
    so it has one flag and no key, and `jobpolicy.tag` is its one rule."""
    try:
        return policy.tag(raw)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from None


def _flag_override(p, row: policy.Setting, says: str) -> None:
    """A row's flag on a verb other than `add`, where that verb overrides the resolved value on the record
    for the jobs it names. One row, so the flag takes exactly what the key takes."""
    p.add_argument(row.flag, metavar=row.metavar, type=_typed(row), default=policy.NOTSET, help=says)


def _flag(p, row: policy.Setting) -> None:
    """The parser entry this row's flag is, built from the row itself. A repeatable flag is read once the
    whole list is in, since argparse hands it one word at a time."""
    kw = {"help": row.says, "metavar": row.metavar} if row.metavar else {"help": row.says}
    if row.repeat:
        p.add_argument(row.flag, action="append", default=None, **kw)
    else:
        p.add_argument(row.flag, type=_typed(row), default=policy.NOTSET, **kw)


# ---- resources ------------------------------------------------------------------------------------
#
# The pool model as users see it (shared and per-queue pools, probes, settle) is `pp help queue`'s "Resources"; the
# defaults the rows above give are the ones it states. Here: a job's needs are a dict NAME -> amount; Resources.check
# asks every pool that provides a NAME, a probe's reading is cached in probes/ for `every`, and jobs launched less
# than `settle` before a reading are subtracted from it.


def _qcfg(q: str, empty_ok: bool = True) -> dict:
    """[queue.q], checked: a typo or a bad value is a ConfigError naming it, never a silent default or a traceback.
    Durations come back in seconds. empty_ok False: what a launch reads (config.load)."""
    t = config.load("queue", empty_ok).get(q, {})
    if not isinstance(t, dict):
        raise config.ConfigError(f"[queue.{q}] must be a table")
    where = f"[queue.{q}]"
    config.check_keys(t, where, QUEUE_KEYS)
    return _read(dict(t), where, {**BY_SCOPE["job"], **BY_SCOPE["queue"]})


@dataclass(frozen=True)
class Project:
    """The project one job belongs to: where its .pp.toml is, what that file's [queue] gives of the settings
    that have a key there, and the table itself for the keys whose own command reads them."""

    root: str | None = None
    where: str = ""
    table: dict = field(default_factory=dict)
    gives: dict = field(default_factory=dict)


def resolved(a, project: Project, name: str) -> tuple:
    """What one per-project row comes to for this job, and which source gave it: the flag it was given, then
    the project's own key, then the row's default, which is the order every row is read in. The command that
    carries the row out asks for it here, so the declaration and the precedence stay in one place (`runner`
    and `stop`, section 2). The source is for the sentence that refuses a value."""
    row = BY_SCOPE["project"][name]
    flag = _given(a, row)
    value = policy.resolve(row, flag, project.gives.get(name, policy.NOTSET))
    return value, row.flag if flag is not policy.NOTSET else f"{project.where} [queue]"


def _pcfg(cwd: str) -> Project:
    """The nearest .pp.toml up from cwd, read through the rows whose scope gives them a key in its [queue]:
    the project's own defaults for the jobs it adds. A project with no such file gives nothing."""
    root, data = config.find_project(cwd)
    if root is None:
        return Project()
    where = f"{root}/.pp.toml"
    t = data.get("queue", {})
    if not isinstance(t, dict):
        raise config.ConfigError(f"{where}: queue must be a table ([queue] runner = ..., env = {{...}})")
    config.check_keys(t, f"{where} [queue]", PROJECT_KEYS)
    rows = {**BY_SCOPE["job"], **{n: r for n, r in BY_SCOPE["project"].items() if not r.top}}
    return Project(root, where, dict(t), _read(dict(t), f"{where} [queue]", rows))


def _mem_total_gb() -> float:
    """What this machine's memory comes to, which `mem_gb = "auto"` is. libc counts it in pages, and a page
    count times the page size is /proc/meminfo's MemTotal to within one page, which a pool in whole GB cannot
    tell apart. 0.0 where it cannot be read, which `auto` then refuses on."""
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / 2 ** 30
    except (ValueError, OSError):
        return 0.0


CARDS_WHY = ""      # why the last _cards() gave None, which `card_reading` hands on to whoever reports it


def _cards() -> list[dict] | None:
    """[{index, name, total_gb, used_gb, free_gb, util}] from nvidia-smi; None when unreadable. Tests patch this."""
    global CARDS_WHY
    CARDS_WHY = ""
    try:
        out = proc.run(["nvidia-smi", "--query-gpu=index,name,memory.total,memory.used,utilization.gpu",
                        "--format=csv,noheader,nounits"], group="own", stdin="none", env="inherit", timeout=10)
    except OSError as e:
        CARDS_WHY = "nvidia-smi is not installed" if isinstance(e, FileNotFoundError) else f"nvidia-smi: {e}"
        return None
    if out.timed_out:
        CARDS_WHY = "nvidia-smi did not answer in 10s"
        return None
    if out.returncode != 0:
        said = (out.stderr.strip() or out.stdout.strip()).splitlines()
        CARDS_WHY = f"nvidia-smi exited {out.returncode}" + (f": {said[-1][:120]}" if said else "")
        return None
    cards, CARDS_WHY = policy.cards(out.stdout)      # the one card parser, which a probe's output goes through too
    return cards


def _pool(name: str, v, scope: str | None) -> dict | None:
    """One pool as its table declares it, read through the rows of the four keys a pool table takes. A bare
    number means `max`, and `false` removes a pool a default would otherwise give, in that scope."""
    where = (f"[queue.{scope}.resources] " if scope else "[resources] ") + name
    if v is False:           # cpu = false: no such pool here
        return None
    t = dict(v) if isinstance(v, dict) else {"max": v}
    bad = sorted(set(t) - set(POOL_KEYS))
    if bad:
        raise config.ConfigError(f"{where}: unknown {', '.join(bad)} (a table takes {', '.join(POOL_KEYS)})")
    read = _read(t, where, BY_SCOPE["machine"])
    mx = read.get("max")
    if mx == "auto":
        if name not in BY_SCOPE["machine"]["resources"].default:
            raise config.ConfigError(f'{where} = "auto": only cpu, mem_gb and gpu_gb know their own size')
        mx = {"cpu": os.cpu_count() or 1, "mem_gb": round(_mem_total_gb()), "gpu_gb": None}[name]   # gpu: per card
    p = {"name": name, "scope": scope, "where": where, "max": mx, "probe": read.get("probe"),
         "every": read.get("every", PROBE_EVERY), "settle": read.get("settle", SETTLE)}
    if p["probe"] is not None and name == "gpu_gb" and scope is not None:
        raise config.ConfigError(f"{where}: the cards are the machine's, so [resources] gpu_gb takes the probe")
    if p["max"] is None and p["probe"] is None and name != "gpu_gb":
        raise config.ConfigError(f"{where}: a number, or a table with max and/or probe")
    return p


def _machine_pools(empty_ok: bool = True) -> dict:
    """[resources] as its row reads it: the machine's pools, the row's default merged with what the file says."""
    return policy.resolve(BY_SCOPE["machine"]["resources"], config=config.load("resources", empty_ok))


def pools(q: str, empty_ok: bool = True) -> list[dict]:
    """The pools queue q's jobs draw from: its own ([queue.q.resources]), then the shared ones ([resources]).
    Each table is its row's default merged with what the file says, which is the one order of the sources."""
    own = _qcfg(q, empty_ok).get("resources", policy.NOTSET)
    if own is not policy.NOTSET and not isinstance(own, dict):
        raise config.ConfigError(f"[queue.{q}.resources] must be a table")
    own = policy.resolve(BY_SCOPE["queue"]["resources"], config=own)
    shared = _machine_pools(empty_ok)
    return [p for p in [_pool(k, v, q) for k, v in own.items()] + [_pool(k, v, None) for k, v in shared.items()] if p]


def card_pools() -> list[dict]:
    """Where the machine's card reading is declared: [resources] gpu_gb, as a pool list, or none where that
    table removes the pool. A config this cannot read leaves the reading to nvidia-smi and is named by the
    view that reads it, so the card line stands while a config error is being fixed."""
    try:
        return [p for p in [_pool("gpu_gb", _machine_pools().get("gpu_gb", False), None)] if p]
    except config.ConfigError:
        return []


def _probe_path(p: dict) -> str:
    return os.path.join(jobstore.top("probes"), f"{p['scope'] or '_shared'}.{p['name']}.json")


def _probe_claim(p: dict) -> str:
    return os.path.join(jobstore.top("probes"), f"{p['scope'] or '_shared'}.{p['name']}.lock")


def _cached(path: str) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def reading(p: dict, run: bool = True) -> dict | None:
    """A probe pool's latest reading {t, value, error}: the cached one while younger than `every`, which every
    worker and `status` share. Where that one is older, a fresh reading, which only `run` asks for.

    One run of a probe at a time: whoever takes the probe's claim runs it, and anyone else uses the reading
    that is there, which is what a cached reading is for. Nothing waits for a probe (they may take a minute),
    and the user's command is never run twice over at once.

    Where another holder is running the first reading there is, this gives back {t, value: None, running: True}:
    a job waits for that reading, and nobody is told the probe failed.

    A pool's value is the amount free, the first number the command prints. `gpu_gb`'s value is the machine's
    cards, one line each, since admission needs a fact per card (jobpolicy.cards)."""
    path = _probe_path(p)
    r = _cached(path)
    if not run or (r and time.time() - float(r.get("t", 0)) < p["every"]):
        return r
    claim = lock.Claim.take(_probe_claim(p))
    if claim is None:
        # someone is running it now: this reading is the one they are replacing, and there may be none yet
        return r or {"t": time.time(), "value": None, "running": True}
    with claim:
        t = time.time()
        try:
            # its own session, so the probe's own children end with it when its time runs out
            out = proc.run(["/bin/sh", "-c", p["probe"]], group="own", stdin="none", env="scrubbed",
                           cwd=os.path.expanduser("~"), timeout=PROBE_WAIT)
        except OSError as e:
            r = {"t": t, "value": None, "error": str(e)[:160]}
        else:
            if out.timed_out:
                r = {"t": t, "value": None, "error": f"it did not answer in {PROBE_WAIT:g}s"}
            elif out.returncode == 0 and p["name"] == "gpu_gb":
                got, why = policy.cards(out.stdout)
                r = {"t": t, "value": got, "error": why or None}
            elif out.returncode == 0 and (m := re.search(r"-?\d+(?:\.\d+)?", out.stdout)):
                r = {"t": t, "value": policy.num(m[0]), "error": None}
            else:
                said = (out.stderr.strip() or out.stdout.strip()).splitlines()
                r = {"t": t, "value": None, "error": (f"exit {out.returncode}" if out.returncode else "printed no number")
                     + (f": {said[-1][:120]}" if said else "")}
        paths.write_json(path, r)
    return r


def card_reading(ps: list[dict]) -> tuple[list[dict] | None, float, str]:
    """The machine's cards, the time they were read, and why there are none ("" where they were read).

    `ps` is every pool named gpu_gb. Where [resources] gpu_gb gives a probe, that probe is the reading: its
    cached reading is the one every reader shares, and a reader that finds it older than `every` runs the
    probe, so an add, a launch pass and `pp queue` read the cards the probe reads now. Without one pp runs
    nvidia-smi itself, uncached."""
    p = next((p for p in ps if p["probe"]), None)
    if p is None:
        t = time.time()     # before the reading runs, as `reading` takes a probe's own t
        return _cards(), t, CARDS_WHY
    r = reading(p) or {}
    if r.get("value") is None:
        return None, float(r.get("t") or 0.0), r.get("error") or policy.NO_READING
    return r["value"], float(r["t"]), ""


class Resources:
    """Queue q's pools with their live readings (the cards, probes), taken once and outside the store lock.

    What a pass is handed is `seen`, those readings as a value: the arithmetic over them is pure, so every
    rule of admission is a table (pp/lib/jobpolicy.py)."""

    def __init__(self, q: str, needed=(), live: bool = True, empty_ok: bool = True):
        self.q = q
        self.pools = pools(q, empty_ok)
        self.by_name: dict[str, list[dict]] = {}
        for p in self.pools:
            self.by_name.setdefault(p["name"], []).append(p)
        needed = set(needed)
        gpu = self.by_name.get("gpu_gb") if "gpu_gb" in needed else None
        # the cards, read once: a pass's settle window counts from this reading's own time, so the time
        # travels with it and so does the reason there are none
        self.cards, self.cards_t, self.cards_why = (card_reading(gpu) if gpu
                                                    else (None, time.time(), ""))
        self.readings = {(p["scope"], p["name"]): reading(p, run=live)
                         for p in self.pools if p["probe"] and p["name"] in needed and p["name"] != "gpu_gb"}

    @property
    def seen(self) -> dict:
        """These readings as the value a pass admits against, with the file a pool is declared in, which a
        refusal names."""
        return {"q": self.q, "pools": self.pools, "by_name": self.by_name, "cards": self.cards,
                "cards_t": self.cards_t, "probes": self.readings, "cards_why": self.cards_why,
                "where": config.global_path()}

    def check(self, needs: dict) -> str | None:
        """Why these needs can never fit; None when they might."""
        return policy.check(needs, self.seen)

    def usage(self, running: list[dict], scope: str | None, cards: list[dict] | None = None) -> list[dict]:
        """The pools of one scope (a queue name, or None = shared) with what running jobs hold: for status.

        The arithmetic is `policy.pool_usage`; this adds the cached reading a probe pool carries, which is a
        file. `cards` is the card reading its caller already took, so a gpu_gb pool's per-card numbers cost
        no second reading of the machine's cards."""
        out = []
        for p in self.pools:
            if p["scope"] != scope:
                continue
            r = reading(p, run=False) if p["probe"] and p["name"] != "gpu_gb" else None
            out.append({**policy.pool_usage(p, running, cards), "reading": r})
        return out


# ---- worker ---------------------------------------------------------------------------------------

def _tail_start(f, n: int, floor: int = 0) -> int:
    """Where the last n lines of open binary file f begin (its end when n is 0), without reading all of it,
    and never before `floor`, which is where this attempt's own output begins (`log_at`)."""
    end = pos = f.seek(0, 2)
    buf = b""
    while n > 0 and pos > 0 and buf.count(b"\n") <= n:
        pos = max(0, pos - 65536)
        f.seek(pos)
        buf = f.read(end - pos)
    start = end - sum(len(line) for line in buf.splitlines(keepends=True)[-n:]) if n > 0 else end
    return max(start, min(floor, end))


def _tail(src, n: int, floor: int = 0) -> list[str]:
    """Last n lines of a file (a path, or a file open for binary reading) without reading all of it, and none
    from before `floor`."""
    if isinstance(src, str):
        with open(src, "rb") as f:
            return _tail(f, n, floor)
    src.seek(_tail_start(src, n, floor))
    return src.read().decode(errors="replace").splitlines()[-n:] if n > 0 else []


def _name(j: dict) -> str:
    """How a view names one job: its own name, or the command it runs where it has none.

    A job added without --tag has no name (`jobpolicy.tag`): its id selects it, and what a person reads in
    that column is the work itself."""
    return _text(j["tag"]) if j.get("tag") else _text(shlex.join(list(j.get("cmd") or [])), 40)


def _text(v, cut: int | None = None) -> str:
    """A job's own text as pp prints it: its tag, its command, its project, a note, a line of its log.

    The one place a job's text is made printable: through ui.printable (CONTRIBUTING.md, The code), so an
    escape sequence a queued command put in it cannot redraw the terminal of whoever reads it, with tabs shown
    too, since one would break a table's columns. The cut comes after that, so no cut lands inside an
    escape."""
    out = ui.printable("" if v is None else str(v), tabs=False)
    return out if cut is None else out[:cut]


def failure_tail(job: dict, n: int) -> list[str]:
    """The last n lines of a job's log as a person reads them: colors and \\r-overwritten progress gone, every
    other control character shown instead of acted on, long lines cut, and without the launch banner ("job N
    (q) in CWD: CMD"), which only repeats what was queued."""
    lines = [_text(ANSI.sub("", ln.rsplit("\r", 1)[-1]).rstrip()) for ln in log_tail(job, n * 3)
             if not LAUNCH_BANNER.match(ln)]
    return [ln[:300] for ln in lines if ln][-n:]


def log_tail(job: dict, n: int = 40) -> list[str]:
    """The last n lines of this attempt's own output, from the offset the launch recorded (`log_at`): so a
    --retry-if pattern never matches an earlier attempt of the same job and never matches pp's own line.

    What the offset does not give is a job's own lines out of a file several jobs share: lines another job
    wrote since this attempt began are inside the region, because an offset bounds a start and not an author.
    That is the reason to give a retrying job a file of its own with {tag} or {id}."""
    try:
        return _tail(job["log"], n, job.get("log_at") or 0) if job.get("log") else []
    except OSError:
        return []


def _retry_line(job: dict) -> str:
    """What a requeue writes in the job's own log, read off the record it left: the attempt that ended, why,
    and the one to come."""
    last = (job.get("history") or [{}])[-1]
    why = last.get("reason") or f"exit {last.get('rc')}"
    return (f"[pp queue] {datetime.now():%Y-%m-%d %H:%M:%S} attempt {last.get('attempt')}/"
            f"{int(job.get('retries') or 0) + 1} after {why}; attempt {job.get('attempt')} in "
            f"{_in_delay(job.get('not_before'))}")


def _append_log(job: dict, line: str):
    try:
        os.makedirs(os.path.dirname(job["log"]), exist_ok=True)
        with open(job["log"], "a") as f:
            f.write(line.rstrip("\n") + "\n")
    except (OSError, KeyError):
        pass


def _of_session(verb: str) -> tuple:
    """What `--mine` means, for every verb that takes it: this session's name, and whether a job is its own.

    One answer for `wait`, `watch`, `status` and `ls`, so the four select the same jobs. Where there is no
    session (pp started the process it runs under, or that process has ended) `--mine` can name nothing, so it
    is a usage error."""
    me = session.caller()
    if not me:
        ui.die(f"{_PROG} {verb} --mine: there is no session, since pp started the process it runs under or that "
               f"process has ended (set PP_SESSION to name one)", 2)
    sid = me["session"]
    return sid, lambda j: isinstance(j.get("owner"), dict) and j["owner"].get("session") == sid


def _stamp_reported(jid: int, verb: str):
    who = session.caller()
    stamp = {"t": time.time(), "by": f"{verb} pid {os.getpid()}", "session": who["session"] if who else None}
    jobcore.amend(jid, {"reported": [stamp]}, must_be=jobstore.ENDED)


# ---- helpers for verbs ----------------------------------------------------------------------------

def _specs(a) -> list:
    """The selection this verb was given, as the five forms `jobpolicy.select` reads: the words it was given
    (an id, a range, a tag), then each --prefix, then --all, which names every job of the queue -q names and
    is a usage error without one.

    Empty where the verb was given no selection at all, which `wait`, `watch` and `clean` each have their own
    meaning for and every other verb refuses."""
    out: list = list(getattr(a, "ids", None) or [])
    one = getattr(a, "id", None)
    out += [one] if one else []
    out += [(policy.PREFIX, pre) for pre in getattr(a, "prefix", None) or []]
    if getattr(a, "all", False):
        if not a.scope:
            ui.die(f"{_PROG}: --all names every job of one queue, so it takes -q NAME", 2)
        out.append((policy.ALL, None))
    return out


def _said(spec) -> str:
    """One spec as the sentence that refuses it names it: the word the verb was given, or the flag it came
    from."""
    form = policy.kind(spec)
    if form == policy.PREFIX:
        return f"--prefix {spec[1]}"
    return "--all" if form == policy.ALL else f"'{spec}'"


def _named(a, verb: str) -> list[dict]:
    """The jobs this verb was given, with a refusal where it was given no selection: every verb that takes
    jobs takes the same five forms, and none of them acts on every job of the machine by default."""
    specs = _specs(a)
    if not specs:
        ui.die(f"{_PROG} {verb}: name the jobs: an id, a range (12-15), a tag, --prefix P, or --all with -q", 2)
    return _resolve(specs, a.scope)


def _resolve(specs: list, scope: list[str] | None = None) -> list[dict]:
    """The jobs these specs name, in the order given, without duplicates. Ids are machine-wide, so the default
    scope is every queue and -q narrows it.

    Each carries _bulk: it was named by a form other than its own id, so a verb leaves it as it is where it is
    in a state the verb does not suit, while a job named by its id there is an error."""
    records = jobcore.jobs(queue=scope)
    by_id = {j["id"]: j for j in records}
    out, seen = [], set()
    for spec in specs:
        form = policy.kind(spec)
        hits = [by_id[i] for i in policy.select(records, [spec])]    # the five forms, over these records
        if form in (policy.TAG, policy.PREFIX):
            qs = sorted({j["queue"] for j in hits})
            if len(qs) > 1:
                ui.die(f"{_PROG}: {_said(spec)} names jobs in queues {', '.join(qs)}: "
                       + ", ".join(f"{j['id']} ({j['queue']})" for j in hits) + "; use ids, or -q", 2)
        if not hits:
            ui.die(f"{_PROG}: no job matches {_said(spec)}", 2)
        for j in hits:
            if j["id"] not in seen:
                seen.add(j["id"])
                out.append({**j, "_bulk": form != policy.ID})
    return out


def _one(a, verb: str) -> dict:
    """The job show, note and log act on. A selection naming several jobs (a retry keeps the name): the newest,
    said so; note refuses, since a note on the wrong job misleads."""
    hits = _named(a, verb)
    if len(hits) > 1:
        ids = ", ".join(str(j["id"]) for j in hits)
        if verb == "note":
            ui.die(f"{_PROG} note: {_said(_specs(a)[0])} names jobs {ids}; note one by its id", 2)
        ui.warn(f"{_PROG} {verb}: {_said(_specs(a)[0])} names jobs {ids}; showing the newest, {hits[-1]['id']}")
    return hits[-1]


def _left(skipped: list[dict]) -> None:
    """Jobs a range or tag swept over that the verb does not apply to: listed once, not errors."""
    if skipped:
        print(ui.c("left as they are: " + ", ".join(f"{j['id']} {j['state']}" for j in skipped), "dim"))


# What `drop` and `clean` take away, in the one sentence both plans print: the record, and the files pp made
# for the job. A log at a path a `log` pattern or --log named belongs to the project, so it stays where it is.
REMOVES = "this removes the records and pp's own files for them; a log at a path `log` or --log named stays"


def _gate(a, jobs: list[dict], verb: str, ok: tuple, note: str = "") -> int | None:
    """kill, drop and retry: a selection naming one job is acted on at once; one naming more is shown first
    and asked about (-y: do not ask). -n only shows. None to go ahead, else the exit code.

    A plan over jobs none of which suits the verb asks nothing, since there is nothing to do. It is still
    the verb's own pass that answers for them: a job a selection swept over is left as it is (exit 0), and
    one named by its own id is refused with the verb that does suit it (exit 2).

    note is what the verb does to the jobs it names, for a verb that takes something away."""
    if not a.dry_run and len(jobs) < 2:
        return None
    hits = [j for j in jobs if j["state"] in ok]
    print(f"{verb} {len(hits)} job(s): "
          + (", ".join(f"{j['id']} {_name(j)}" for j in hits) or "none"))
    _left([j for j in jobs if j["state"] not in ok])
    if note and hits:
        print(note)
    if a.dry_run:
        return 0
    if not hits:
        return None if any(not j["_bulk"] for j in jobs) else 0
    return None if ui.confirm(f"{verb} them?", a.yes) else 2


def _ranges(ids: list[int]) -> str:
    """[1, 2, 3, 7] -> '1-3, 7' (sorted ids)."""
    out: list[list[int]] = []
    for i in ids:
        if out and i == out[-1][1] + 1:
            out[-1][1] = i
        else:
            out.append([i, i])
    return ", ".join(str(a) if a == b else f"{a}-{b}" for a, b in out)


def _needs_str(needs: dict | None) -> str:
    out = []
    for k, v in (needs or {}).items():
        if k == "slots" and v == 1:
            continue
        out.append({"gpu_gb": f"gpu {v:g}G", "mem_gb": f"mem {v:g}G"}.get(k, f"{k} {v:g}"))
    return " ".join(out)


def project_name(cwd: str) -> str:
    """The project a job of this directory belongs to, as that row says: `name` at the top of the nearest
    .pp.toml, else the name of the directory that file is in, else of the repository or the directory
    itself."""
    root, data = config.find_project(cwd)
    if root:
        named = _read({"name": data["name"]} if "name" in data else {}, root, BY_SCOPE["project"])
        return named.get("name") or os.path.basename(os.path.normpath(root))
    g = config.git_root(cwd)
    return os.path.basename(os.path.normpath(g or cwd))


def _under(spec: str, root: str) -> str:
    """Where a `log` pattern's file goes: an absolute path as it is, and a relative one under `root`, which is
    the project root (the directory holding the .pp.toml found from the job's directory) or the job's own
    directory where it is in no project. The same rule for the key and for the flag, so one path written one
    way means one thing. A pattern holding '..' is refused where it is read (`jobpolicy.pattern`), which is
    what keeps a relative one from leaving that root."""
    spec = os.path.expanduser(spec)
    return spec if os.path.isabs(spec) else os.path.join(root, spec)


def _from_lines(src: str, refusing) -> list[tuple[int, list[str], list[str]]]:
    """A campaign file read: one (line number, flags, command) per line that is a job.

    A line whose first non-blank character is `#` and a blank line are skipped, and nothing else is: a `#`
    anywhere else is an ordinary character, so there are no inline comments. Every other line is a job, and
    one that is refused is refused with its number before anything is queued."""
    try:
        text = sys.stdin.read() if src == "-" else open(paths.expand(src)).read()
    except OSError as e:
        ui.die(f"{_PROG} add --from: {e.strerror}: {src}", 2)
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            flags, cmd = policy.campaign(line)
        except ValueError as e:
            refusing(n)(f"{e}: {_text(line.strip(), 80)}")
        out.append((n, flags, cmd))
    return out


def _last_line(path: str | None, width: int) -> str:
    """The job's latest output line, never pp's own banner: a job that has printed nothing yet would show that
    banner as its line."""
    if not path:
        return ""
    try:
        lines = _tail(path, 5)
    except OSError:
        return ""
    for line in reversed(lines):
        line = _text(ANSI.sub("", line).strip())
        if line and not line.startswith("[pp queue] "):
            return line[:width]
    return ""


def _in_delay(not_before: float | None, now: float | None = None) -> str:
    """How long until a retry's delay is up, as a person reads it. Never negative: a worker notices an attempt
    ended a poll period after it did, so the delay of a job it is about to re-queue may be up already. The one
    place that remaining delay is put into words, for the three that print it."""
    return ui.human_duration(max(float(not_before or 0) - (now or time.time()), 0))


def _section(rows: list[list], tag: int, width: int) -> list[str]:
    """One section of `pp queue status` as its lines: one table, so every row of it lines up, with the column
    at `tag` cut until the widest row fits a terminal `width` wide.

    That column holds the job's own name, or its command where it has none, so it is the column with
    something to give; the rest of a row is pp's own, and a row whose other columns alone are wider than the
    terminal keeps eight characters of it and runs past the edge. Empty cells at the end of a row are left
    out, so no row ends in the padding of a column nothing filled."""
    rows = [_without_empty_tail(r) for r in rows]
    keep = max((ui.width(str(r[tag])) for r in rows), default=0)
    while True:
        out = ui.table([[ui.cut(str(v), keep) if i == tag else v for i, v in enumerate(r)] for r in rows],
                       indent="  ").splitlines()
        if keep <= 8 or max((ui.width(ln) for ln in out), default=0) <= width:
            return out
        keep -= 1


def _without_empty_tail(row: list) -> list:
    out = list(row)
    while len(out) > 1 and not str("" if out[-1] is None else out[-1]):
        out.pop()
    return out


def _latest_note(j: dict) -> str:
    """The last note on a job, as a row of `pp queue status` shows it: one line, cut to 60 characters."""
    notes = j.get("notes") or []
    return ("note: " + _text(notes[-1]["text"].replace("\n", " "), 60)) if notes else ""


# What a field of the machine surface is where the record does not hold it yet: a job that has not started
# has no exit code and no attempt of its own, and a reader of `--json` finds the field all the same. So every
# field below is on a job in each of the five states, and a reader tells "nothing yet" from "no such field".
NOT_YET = {"rc": None, "reason": None, "not_retried": None, "started": None, "ended": None, "seconds": None,
           "rcfile": None, "envfile": None, "container": None, "log_at": None, "cards": [], "stop_cmd": None,
           "history": [], "notes": [], "reported": [], "hooks": []}


def public(j: dict) -> dict:
    """A job as JSON consumers (`show/status --json`) see it: every stored field, with the job's environment
    masked, and every promised field a record of this state does not hold yet (NOT_YET).

    Fields others rely on: id (int), state, queue, tag (or null), project, cwd, cmd (argv), runner (argv,
    maybe []), stop_cmd ("group" or the command that reaches what the runner started), needs, prio, owner
    {session, cwd} or None, timeout, grace, retries, attempt, rc, reason, seconds, log, log_at (where this
    attempt's own output begins), cards (the card indices it reserved), container (the name whatever its
    runner started elsewhere carries), envfile (the file that runner reads its environment from),
    notes [{t, by, text}], reported [{t, by, session}], history (one entry per attempt that ended, each with
    that attempt's own rcfile, envfile, container, log_at and cards), stop {by, t, grace, timeout, sent,
    kill9, denied, was, cmd, rc} on a job asked to stop, after [id] on a job that names the jobs it waits
    for, and from `show` also jobcore.watchers [{pid, verb, session}].

    `env` and `shell_env` keep their keys and give every value as "***". A token or a secret is what the help
    says to pass with --env, and an agent reads `--json` to see how its jobs are getting on, so the values
    would land in a transcript and in whatever keeps one. The text `show` prints neither, and the record on
    disk, which is the job's own, holds what the job runs with.

    A stop's `timeout` is set on the one a worker wrote because the job's own time ran out, and on no other,
    so what a stop is never rests on the name of whoever asked. `sent` and `kill9` are when each signal went
    out, read just before it did, so an exit code the attempt wrote while that stop was going out is late
    against it and is that stop's outcome. `denied` is when pp was refused the job's process group. A stop with `denied` set is not in
    flight: nothing more goes out for it, and the job runs on until it ends on its own. `was` is who asked
    for the stop this one landed on, where a kill landed on a timeout's."""
    out = {**NOT_YET, **{k: v for k, v in j.items() if not k.startswith("_")}}
    for k in ("env", "shell_env"):
        if isinstance(out.get(k), dict):
            out[k] = dict.fromkeys(out[k], "***")
    return out


def _ended_line(j: dict | None, jid: int) -> str:
    if j is None:
        return f"dropped {jid}"
    word = "done" if j["state"] == "done" else ui.c("FAILED", "red")
    why = f" ({j['reason']})" if j.get("reason") else ""
    return f"{word} {jid} rc={_rc(j)}{why} {ui.human_duration(j.get('seconds'))}  {_name(j)}"


def _rc(j: dict):
    """A job's exit code as shown: '-' when there is none (killed, lost, never started)."""
    return "-" if j.get("rc") is None else j["rc"]


def _outcome_lines(j: dict | None, jid: int, tail: int) -> list[str]:
    """A job's final outcome as wait/watch print it: one line, and for a failure its log tail and what to do next."""
    out = [_ended_line(j, jid)]
    if j and j.get("not_retried"):
        out.append(f"  not retried: {j['not_retried']}")
    if j and j["state"] == "failed" and tail > 0:
        out += ["  | " + ln for ln in failure_tail(j, tail)]
        out.append(f"  next: pp {_PROG} log {jid} | pp {_PROG} retry {jid} | pp {_PROG} note {jid} \"why\"")
    return out


def _attempt_str(j: dict) -> str:
    retries = int(j.get("retries") or 0)
    return f"attempt {j.get('attempt', 1)}/{retries + 1}" if retries else ""


# ---- verbs ----------------------------------------------------------------------------------------

def _given(a, row: policy.Setting):
    """What this row's flag said on the command line, or NOTSET where it was not given. A flag that may be
    repeated is read here, since argparse hands it one word at a time and a row is read whole."""
    raw = getattr(a, row.name, policy.NOTSET)
    if not row.repeat:
        return raw
    if raw is None or raw is policy.NOTSET:
        return policy.NOTSET
    try:
        return policy.value(row, raw)
    except ValueError as e:
        ui.die(f"{_PROG}: {row.flag}: {e}", 2)


def _after(spec: str, refuse) -> list[int]:
    """The ids `--after SPEC` names, read as the job is queued. One flag's value, so it is one or more
    selections separated by commas rather than several argv words.

    Read once, here: the record keeps the ids, so a tag added to later is not picked up and a campaign file
    is reproducible. An id no job has is a typo, and a job that waits forever for it is the worst outcome,
    so one is a usage error and nothing is queued. That also makes a forward reference impossible, which is
    what lets a campaign file be read top to bottom."""
    specs = [w for w in (word.strip() for word in spec.split(",")) if w]
    if not specs:
        refuse("--after names the jobs to wait for: an id, a range (12-15) or a tag, separated by commas")
    try:
        return sorted({j["id"] for j in _resolve(specs)})    # machine-wide: an id names one job
    except SystemExit:
        refuse(f"--after {_text(spec, 80)}: it names a job that does not exist")


# What a job's `--after` is still waiting for, as a view says it: one of two sentences, so `status` and
# `show` say the same thing about the same record.
def _after_str(ids: list, live: set) -> str:
    """The jobs an `--after` named: the ids, and how many of them have not ended. A job whose record is gone
    counts as ended, so an id nothing names is satisfied."""
    left = [i for i in ids if i in live]
    return _ranges(sorted(ids)) + (f" ({len(left)} still to end)" if left else " (all ended)")


# The arguments of one `add`, which are the batch's and no line's of a campaign file: the queue the jobs go
# to, the file the lines were read from, whether they are parked, and whether a worker starts for them. A
# line that names one is refused, since a file cannot say it for the jobs of another line.
OF_THE_BATCH = ("queue", "from_", "hold", "no_start")


def cmd_add(a, cmd: list[str], q: str, prepare_job, ap) -> int:
    """Queue one job per command: the one this command line names, or one per line of a campaign file.

    A line of a campaign file is the rest of an `add` command line: its own flags, then `--`, then the
    command (`jobpolicy.campaign`). Its flags are read by `add`'s own parser, so every flag `add` ever gains
    works in a campaign file on the day it lands, and a line's own flag wins over the `add`'s own, per flag.
    A file one line of which is refused queues nothing, not even the lines before it: a half-queued campaign
    is the one state a plan script cannot see."""
    def refusing(n: int):
        """How a refusal of one line names where it came from, and that nothing was queued for it."""
        where, tail = (f" --from {a.from_}: line {n}", "  (nothing was queued)") if a.from_ else ("", "")
        return lambda msg: ui.die(f"{_PROG} add{where}: {msg}{tail}", 2)

    def settings(flags: list[str], refuse):
        """One line's own flags, over the `add`'s: the namespace the job of that line is read from. The
        parser's own defaults say which flags the line gave, so a flag it did not give stays the `add`'s."""
        try:
            got = ap.parse_args(["add", *flags])
        except SystemExit:
            refuse("a flag `add` does not take, or a value its row refuses")
        if got.cmd:
            refuse(f"'{_text(got.cmd[0], 40)}' is before the -- and is no flag; the command goes after it")
        out = argparse.Namespace(**vars(a))
        rows = {r.name: r for r in JOB_ROWS + PROJECT_ROWS}
        for k, v in vars(got).items():
            if v == getattr(base, k):
                continue            # the line did not give it
            if k in OF_THE_BATCH:
                refuse(f"--{k.rstrip('_').replace('_', '-')} is the `add`'s own and no line's")
            row = rows.get(k)
            setattr(out, k, policy.given(row, v, getattr(a, k)) if row else v)
        return out

    def record(la, c: list[str], refuse) -> dict:
        """One job, as this line or this command line asked for it. Every per-job row is resolved here and
        nowhere else: the flag, then this project's [queue], then the queue's table, then the row's own
        default. What they come to goes on the record, so a config edit after this never changes the job."""
        cwd = paths.expand(la.cwd) if la.cwd else os.getcwd()
        if not os.path.isdir(cwd):
            refuse(f"no directory {cwd}")
        project = _pcfg(cwd)
        gave = {r.name: policy.resolve(r, _given(la, r), project.gives.get(r.name, policy.NOTSET),
                                       qc.get(r.name, policy.NOTSET)) for r in JOB_ROWS}
        if gave["log"]:
            gave["log"] = _under(gave["log"], project.root or cwd)   # the store fills in {tag} and {id}
        needs = {"slots": 1, **{k: gave[k] for k in ("gpu_gb", "cpu", "mem_gb") if gave[k] is not None},
                 **gave["need"]}
        why = Resources(q, needs, live=False).check(needs)
        if why:
            refuse(why)
        job = {"queue": q, "tag": la.tag, "project": project_name(cwd), "cwd": cwd, "cmd": c, "runner": [],
               "shell_env": shell_env, "needs": needs, "owner": owner, "attempt": 1,
               **{r.on_record: gave[r.name] for r in JOB_ROWS if r.on_record != "needs"}}
        if la.after is not None:
            # every value the flag was given is read, the empty one too: `--after "$IDS"` over a variable a
            # shell never set names no job, and a job queued with no `after` runs before the stage it
            # follows. Read once per value, however many lines carry it: a campaign file whose lines all
            # name one stage reads the store once instead of once a line
            if la.after not in waits:
                waits[la.after] = _after(la.after, refuse)
            job["after"] = list(waits[la.after])
        if prepare_job:
            prepare_job(job, la, project)
        return job

    lines = _from_lines(a.from_, refusing) if a.from_ else [(0, [], cmd)]
    if not lines:
        ui.die(f"{_PROG} add --from {a.from_}: no command in it", 2)
    qc, owner = _qcfg(q), session.caller()
    shell_env = {k: os.environ[k] for k in CAPTURED_ENV if k in os.environ}
    state = "held" if a.hold else "queued"
    base = ap.parse_args(["add"])        # what the parser gives a line that gave a flag none of its own
    waits: dict = {}                    # the ids each --after value came to, read once per value
    records = []
    for n, flags, c in lines:
        refuse = refusing(n)
        if not c:
            refuse(f"no command. Put it after --  (pp {_PROG} add -- python x.py)")
        records.append(record(settings(flags, refuse) if flags else a, c, refuse))
    # The records and the ids they were given, whole: a batch lands in one transaction of the store, and the
    # signals that transaction holds back are delivered the moment it ends. They wait for these lines too, so
    # every job pp stored has its id on stdout and a Ctrl-C on a long `--from` file leaves a script the id of
    # everything it queued.
    with lock.deferring():
        ids = jobcore.add_jobs(records, state)      # n records, n ids, one scope
        for jid, job in zip(ids, records):
            print(jid)
            extra = " ".join(x for x in (f"timeout {ui.human_duration(job['timeout'])}" if job["timeout"] else "",
                                          f"retries {job['retries']}" if job["retries"] else "",
                                          f"on-fail {job['on_fail']}" if job["on_fail"] else "") if x)
            via = f"  (via {_text(os.path.basename(job['runner'][0]))})" if job["runner"] else ""
            print(f"pp {_PROG}: {state} {jid} ({q}) [{_text(job['project'])}] p{job['prio']} "
                  f"{_needs_str(job['needs'])} {extra}  {_text(shlex.join(job['cmd']), 100)}{via}", file=sys.stderr)
    if not a.hold and not a.no_start:
        _worker_up(q, wake=True)
    if owner:     # the pull side: an agent hears the outcome by waiting on it in the background
        some = _ranges(ids).replace(", ", " ")      # as range words, the form a verb takes: wait 8-13 20
        print(f"pp {_PROG}: to hear how it ends, run `pp {_PROG} wait {some}` in the background (it exits with the "
              f"outcome and, on failure, the log tail), or `pp {_PROG} watch {some}` for a line per event", file=sys.stderr)
    return 0


def _scope_list(scope: list[str] | None) -> list[str]:
    """The queues a verb works on: every one there is, which `queues` opens as it finds it, or the one it was
    given by name, which run_cli opened before any verb ran."""
    return jobcore.queues() if scope is None else scope


def cmd_status(a, scope: list[str] | None) -> int:
    """The state of every queue in scope, or of the one named with -q.

    `--here`/`--project` and `--mine` narrow the four lists of jobs, and apply together. The machine's own part
    of the view (the queues, the workers, the pools, the cards) counts every job, whoever queued it.

    `--json` is what an agent reads, and this is its shape, stated once:

        queues          [name], the queues this status covers: those that hold records, those a worker is up
                        for, and those config.toml names in a [queue.NAME] table
        workers         {queue: pid or null}, the pid of each queue's worker; null where none is up and while
                        one that holds the claim has not recorded itself yet
        starting        [queue] whose worker holds the claim and has not recorded itself yet, which is the
                        first moment of a worker: one is coming up, and it launches the queue's jobs
        waiting         {queue: {id, tag, why, t} or null}, what the head of each queue waits for
        running, queued, held    [job], each one as `public()` gives it
        recent          [job], the jobs that ended most recently, at most -n of them
        pools           {queue: [pool]}, each queue's own pools
        shared          [pool], the pools every queue draws from; a pool is {name, max, used, probe, reading},
                        with `max` null where the pool takes its number from elsewhere (gpu_gb = "auto": a
                        card's). A gpu_gb pool carries per_card besides, [{index, max, used}], that scope's
                        budget on each card of the reading and what is reserved on it, and [] where there is
                        no reading
        cards           [{index, name, total_gb, used_gb, free_gb, util}], the GPUs the card reading reports
        cards_why       why `cards` is null; "" where the cards were read
        config_errors   [message]; while there is one, workers launch nothing

    A field change goes in CHANGELOG.md (CONTRIBUTING.md, "Adding a command").
    """
    # before the listing: a card reading may take seconds, and a reader holds the store. The line this view
    # prints and the `cards` an agent reads are this one reading.
    cards, _t, cards_why = card_reading(card_pools())
    now = time.time()
    # one listing: the counts, the rows and what the shared pools hold are the same jobs, so no job is shown
    # twice or in two states. Every queue is listed, since a shared pool counts every queue's running jobs.
    all_jobs = jobcore.jobs()
    # every queue in scope: the ones that exist, and the ones config.toml names, which have nothing of their
    # own to read yet
    named = _configured_queues() if scope is None else []
    # what each queue's worker is, through the one construct, so the hints that nothing launches or records
    # this queue's jobs come back from the one answer it gave
    workers = {q: jobcore.worker_now(q) for q in sorted(set(_scope_list(scope)) | set(named))}
    # the queues those workers are of, so this view comes of one listing of the store: a queue another process
    # creates between two listings would otherwise be one this has no worker for
    qs = [q for q in workers
          if scope is not None or q in named or any(j["queue"] == q for j in all_jobs)
          or workers[q].how != jobcore.DOWN]
    # what the view narrows to, applied together: --here/--project one project's jobs, --mine this session's.
    # The machine's own part of the view (the queues, the workers, the pools, the cards) is every job's, so
    # these name the four lists of jobs and nothing else.
    tests = []
    if a.here or a.project:
        want = a.project or project_name(os.getcwd())
        tests.append(lambda j: j.get("project") == want)
    if a.mine:
        tests.append(_of_session(a.verb)[1])
    keep = lambda js: [j for j in js if all(t(j) for t in tests)]   # noqa: E731
    running_all = [j for j in all_jobs if j["state"] == "running"]
    live = {j["id"] for j in all_jobs if j["state"] in jobstore.LIVE}   # what an unmet `after` waits for
    per: dict = {}
    bad: list[str] = []         # a broken config stops launches, not this view of what is running

    def usage(q, sc):
        try:
            return Resources(q, live=False, empty_ok=False).usage(running_all, sc, cards)
        except config.ConfigError as e:
            if str(e) not in bad:
                bad.append(str(e))
            return []
    shared = usage(qs[0] if qs else "default", None)    # the same for every queue
    for q in qs:
        mine = [j for j in all_jobs if j["queue"] == q]
        data = {st: keep([j for j in mine if j["state"] == st]) for st in jobstore.STATES}
        own = usage(q, q)
        w = workers[q]
        waiting = (w.rec or {}).get("waiting") if w.up else None     # only a live worker waits for anything
        per[q] = {"data": data, "wpid": w.pid, "how": w.how, "pools": own, "waiting": waiting,
                  "recent": sorted(data["done"] + data["failed"], key=lambda j: -(j.get("ended") or 0))}
    if a.json:
        flat = lambda st: [public(j) for q in qs for j in per[q]["data"][st]]   # noqa: E731
        recent = sorted((j for q in qs for j in per[q]["recent"]), key=lambda j: -(j.get("ended") or 0))
        ui.print_json({"cards": cards, "cards_why": cards_why, "queues": qs,
                       "workers": {q: per[q]["wpid"] for q in qs},
                       "starting": [q for q in qs if per[q]["how"] == jobcore.STARTING],
                       "pools": {q: per[q]["pools"] for q in qs}, "shared": shared,
                       "waiting": {q: per[q]["waiting"] for q in qs},
                       **{st: flat(st) for st in ("running", "queued", "held")},
                       "recent": [public(j) for j in recent[:a.n]], "config_errors": bad})
        return 1 if bad else 0
    width = shutil.get_terminal_size((120, 20)).columns
    gpu = "  ".join(f"GPU{c['index']} {c['util']}% {c['used_gb']:.1f}/{c['total_gb']:.0f}G" for c in cards) if cards else (
        "GPU n/a" + (f" ({cards_why})" if cards_why else ""))
    print(f"{gpu}   {datetime.now():%Y-%m-%d %H:%M:%S}")
    # a shared pool is on this line where a job holds some of it, a probe reads it, or this machine named it:
    # a pool every machine has and nothing holds says nothing. gpu_gb is there where a job holds some or it has
    # a max of its own, since its reading is the card line above
    default_pools = BY_SCOPE["machine"]["resources"].default
    pool_strs = [_pool_str(p, now) for p in shared if p["used"] or (
        p["max"] is not None if p["name"] == "gpu_gb" else p["probe"] or p["name"] not in default_pools)]
    if pool_strs:
        print("shared: " + " · ".join(pool_strs))
    for e in bad:
        print(ui.c(f"bad config: {e}. Workers launch nothing until it is fixed", "yellow"))
    if not qs:
        print(f"no queues yet: `pp {_PROG} add -- CMD` starts one")
        return 1 if bad else 0
    if scope and scope[0] not in jobcore.queues() and not _configured(scope[0]):
        known = [q for q in jobcore.queues() if q != scope[0]]
        print(f"no queue '{scope[0]}' yet" + (f" (there are: {', '.join(known)})" if known else ""))
    if tests and not any(per[q]["data"][st] for q in qs for st in jobstore.STATES):
        narrowed = ([f"project '{want}'"] if a.here or a.project else []) + (["this session"] if a.mine else [])
        print(f"no jobs of {' and '.join(narrowed)}")
    for q in qs:
        s = per[q]
        data, waiting, wpid = s["data"], s["waiting"], s["wpid"]
        up = (f"worker up (pid {wpid})" if wpid is not None else
              ui.c("worker starting" if s["how"] == jobcore.STARTING else "worker down", "yellow"))
        usage = [_pool_str(p, now) for p in s["pools"]]
        head = ui.c(f"== {q}", "bold") + f"   {up}"
        counts = " · ".join(usage + [f"queued {len(data['queued'])}", f"held {len(data['held'])}",
                                     f"done {len(data['done'])}", f"failed {len(data['failed'])}"])
        # the counts go on a line of their own where the two do not fit the terminal side by side
        fits = ui.width(head) + 3 + ui.width(counts) <= width
        print("\n" + head + ("   " if fits else "\n  ") + counts)
        if data["running"]:
            print(ui.c("running", "bold"))
            rows, lasts = [], []
            for j in data["running"]:
                extra = " ".join(x for x in (_attempt_str(j),
                                             f"STOPPING ({_text(j['stop']['by'])})" if j.get("stop") else "",
                                             _latest_note(j)) if x)
                row = [j["id"], _name(j), f"[{_text(j.get('project'))}]",
                       ui.human_duration(now - (j.get("started") or now)),
                       _needs_str(j.get("needs"))
                       + (" on GPU" + ",".join(str(c) for c in j["cards"]) if j.get("cards") else "")]
                rows.append(row + [extra])
                lasts.append(_last_line(j.get("log"), max(width - 8, 20)))
            for line, last in zip(_section(rows, 1, width), lasts):
                print(line)
                if last:
                    print(ui.c("      " + last, "dim"))
            if s["how"] == jobcore.DOWN:
                print(ui.c(f"  nothing records how they end: `pp {_PROG} worker start -q {q}`", "yellow"))
            elif s["how"] == jobcore.STARTING:
                print(ui.c("  a worker is starting; it records how they end", "yellow"))
        if data["queued"]:
            print(ui.c("queued (run order)", "bold"))
            rows = []
            for j in data["queued"][:max(a.n, 1) * 3]:
                why = waiting["why"] if waiting and waiting.get("id") == j["id"] else ""
                nb = j.get("not_before") or 0
                extra = " ".join(x for x in (_attempt_str(j), f"retry in {_in_delay(nb, now)}" if nb > now else "",
                                             f"after {_after_str(j['after'], live)}" if j.get("after") else "",
                                             ui.c("waiting: " + why, "yellow") if why else "", _latest_note(j)) if x)
                row = [j["id"], f"p{j.get('prio')}", _name(j), f"[{_text(j.get('project'))}]",
                       _needs_str(j.get("needs"))]
                rows.append(row + [extra])
            print("\n".join(_section(rows, 2, width)))
            if len(data["queued"]) > len(rows):
                print(f"  ... {len(data['queued']) - len(rows)} more")
            if s["how"] == jobcore.DOWN:
                print(ui.c(f"  nothing launches them: `pp {_PROG} worker start -q {q}`", "yellow"))
            elif s["how"] == jobcore.STARTING:
                print(ui.c("  a worker is starting; it launches them", "yellow"))
        if data["held"]:
            print(ui.c("held", "bold"))
            rows = [[j["id"], f"p{j.get('prio')}", _name(j), f"[{_text(j.get('project'))}]",
                     _latest_note(j)] for j in data["held"]]
            print("\n".join(_section(rows, 2, width)))
        if s["recent"]:
            print(ui.c("recent", "bold"))
            rows = [[j["id"], "done" if j["state"] == "done" else ui.c("FAILED", "red"), f"rc={_rc(j)}",
                     ui.human_duration(j.get("seconds")), _name(j), f"[{_text(j.get('project'))}]",
                     ui.human_age(j.get("ended"), now) + (f"  {j['reason']}" if j.get("reason") else "")
                     + (f"  {_latest_note(j)}" if j.get("notes") else "")] for j in s["recent"][:a.n]]
            print("\n".join(_section(rows, 4, width)))
    return 1 if bad else 0


def _configured(q: str) -> bool:
    return q in _configured_queues()


def _configured_queues() -> list[str]:
    """Every queue config.toml names in a [queue.NAME] table, which `status` covers beside the queues that
    hold records and the ones a worker is up for. A lane the machine declared and nothing has run in yet is a
    section of the view with nothing in it, so a reader sees the lanes there are without being told their
    names, and reading them creates no directory and no file.

    A name no queue could have is left out, as it is wherever a queue is named. So is a key of [queue] whose
    value is no table: that is a setting of a project's own file, written here by mistake, and it names no
    queue. A config file that cannot be read names none here; the error is the view's own, from the pools it
    reads beside this."""
    try:
        names = config.load("queue")
    except config.ConfigError:
        return []
    return sorted(q for q, t in names.items()
                  if isinstance(t, dict) and jobstore.NAME_RE.fullmatch(q) and q not in jobstore.RESERVED)


def _pool_str(p: dict, now: float) -> str:
    if p.get("per_card"):
        # the cards are counted one by one, since a job runs on one of them: what this scope holds of each,
        # against that card's own budget, in the order the reading gave them
        return p["name"] + " " + ",".join(f"{c['used']:g}/{c['max']:g}" for c in p["per_card"])
    out = p["name"] + (f" {p['used']:g}/{p['max']:g}" if p["max"] is not None
                       else (f" {p['used']:g} in use" if p["used"] else ""))
    if p["probe"] and p["name"] != "gpu_gb":
        # a gpu_gb pool's reading is the cards, which the card line above reports, so this says the budget
        r = p["reading"]
        out += (" (probe: not run yet)" if not r else f" (probe: {r['value']:g} free, {ui.human_age(r['t'], now)})"
                if r.get("value") is not None else f" (probe failed {ui.human_age(r['t'], now)}: {r.get('error')})")
    return out


def cmd_show(a) -> int:
    j = _one(a, "show")
    watched = [{k: w[k] for k in ("pid", "verb", "session")} for w in jobcore.watchers(j)]
    if a.json:
        ui.print_json({**public(j), "watchers": watched})
        return 0
    state = j["state"] if j["state"] != "failed" else ui.c("FAILED", "red")
    head = f"job {j['id']}  ({j['queue']})  {state}"
    if j["state"] in jobstore.ENDED:
        head += f"  rc={_rc(j)}" + (f" ({j['reason']})" if j.get("reason") else "")
    print(ui.c(head, "bold"))
    owner = j.get("owner")
    rows = [["tag", _text(j.get("tag")) or "-"], ["project", _text(j.get("project"))], ["cwd", _text(j.get("cwd"))],
            ["command", _text(shlex.join(list(j.get("runner") or []) + list(j.get("cmd") or [])))],
            ["needs", _needs_str(j.get("needs")) or "slots 1"], ["prio", j.get("prio")],
            ["stop", _text(j.get("stop_cmd")) or "-"],
            ["container", _text(j.get("container")) or "-"],
            ["attempt", f"{j.get('attempt', 1)}/{int(j.get('retries') or 0) + 1}"
             + (f"  retry delay {ui.human_duration(j.get('retry_delay'))}" if j.get("retries") else "")
             + (f"  retry if /{j['retry_if']}/" if j.get("retry_if") else "")],
            ["timeout", ui.human_duration(j["timeout"]) if j.get("timeout") else "-"],
            ["on fail", _text(j.get("on_fail")) or "-"], ["on done", _text(j.get("on_done")) or "-"],
            ["owner", f"session {_text(owner['session'])}  in {_text(owner.get('cwd')) or '-'}" if owner else "-"],
            ["added", datetime.fromtimestamp(j["added"]).strftime("%Y-%m-%d %H:%M:%S") if j.get("added") else "-"]]
    if j.get("started"):
        rows.append(["started", datetime.fromtimestamp(j["started"]).strftime("%Y-%m-%d %H:%M:%S")])
    if j.get("ended"):
        rows.append(["ended", datetime.fromtimestamp(j["ended"]).strftime("%Y-%m-%d %H:%M:%S")
                     + f"  ({ui.human_duration(j.get('seconds'))})"])
    if j.get("stop"):
        st = j["stop"]
        # one stop per run carries one time, which is when the first ask landed. A kill that landed on a
        # timeout's stop is a kill, and the row says whose stop that time belongs to rather than reading as
        # the time the kill asked at
        when = f"{datetime.fromtimestamp(st['t']):%H:%M:%S}"
        rows.append(["stop", (f"{_text(st.get('by'))}, on the {_text(st['was'])}'s stop from {when}"
                              if st.get("was") else f"{_text(st.get('by'))} asked at {when}")
                     # what went out for it, off the record: the project's command where that is what went
                     # out, and pp's own signal where it is
                     + ((f", its stop command went out ({_text(st['cmd'])})" if st.get("cmd")
                         else ", SIGTERM sent") if st.get("sent") else ", nothing signalled")
                     + (", SIGKILL sent" if st.get("kill9") else "")
                     + (f", its command exited {st['rc']}" if st.get("rc") is not None else "")
                     # a denied stop is not in flight: say so, or it reads as one still being carried out
                     + (f"; pp is not allowed to signal its group {j.get('pgid')}, so nothing more goes out"
                        if st.get("denied") else "")])
    if j.get("after"):      # what it waits for besides a pool: the jobs it named when it was queued
        rows.append(["after", _after_str(j["after"], {x["id"] for x in jobcore.jobs(jobstore.LIVE)})])
    if j.get("not_retried"):
        rows.append(["not retried", j["not_retried"]])
    if j.get("retry_of"):
        rows.append(["retry of", f"job {j['retry_of']}"])
    if j.get("not_before") and j["state"] == "queued":
        rows.append(["retry at", datetime.fromtimestamp(j["not_before"]).strftime("%H:%M:%S")])
    if watched:
        rows.append(["watched by", ", ".join(f"{w['verb']} pid {w['pid']}" for w in watched)])
    if j.get("reported"):
        rows.append(["reported to", ", ".join(r["by"] for r in j["reported"])])
    print(ui.table(rows, indent="  "))
    for h in j.get("history") or []:
        print(f"  attempt {h['attempt']}: rc={_rc(h)}" + (f" ({h['reason']})" if h.get("reason") else "")
              + f" after {ui.human_duration(h.get('seconds'))}")
    if j.get("notes"):
        print(ui.c("notes", "bold"))
        for n in j["notes"]:
            print(f"  {datetime.fromtimestamp(n['t']):%m-%d %H:%M} {_text(n.get('by'))}: {_text(n['text'])}")
    if j.get("hooks"):
        print(ui.c("hooks", "bold"))
        for h in j["hooks"]:
            print(f"  {datetime.fromtimestamp(h['t']):%m-%d %H:%M} {h['event']}: {_text(h['cmd'])}  "
                  f"log {_text(h['log'])}")
    print(ui.c("log", "bold") + f" {_text(j.get('log'))}")
    for line in log_tail(j, 20):
        print("  " + _text(line))
    return 0


def cmd_note(a) -> int:
    text = " ".join(a.text).strip()
    if not text:
        ui.die(f"{_PROG} note: no text", 2)
    j = _one(a, "note")
    add_note(j["id"], text, by=a.by)
    print(f"noted on {j['id']}")
    return 0


def cmd_log(a) -> int:
    j = _one(a, "log")
    path = j.get("log")
    if a.follow and j["state"] == "held":
        print(f"[{_PROG} log] note: {j['id']} is held, so this waits forever. "
              f"`pp {_PROG} release {j['id']}` releases it", file=sys.stderr)
    if a.follow:
        while not (path and os.path.exists(path)):     # queued: the log appears when it starts
            cur = jobcore.find(j["id"])
            if not cur or cur["state"] not in jobstore.LIVE:
                break
            time.sleep(0.5)
    try:        # opened once and read from there: `clean` may remove it at any moment, before or after
        f = open(path or "", "rb")
    except OSError:
        ui.die(f"{_PROG}: job {j['id']} has no log yet ({j['state']})")
    if not a.follow:
        with f:
            for line in _tail(f, a.lines):
                print(line)
        return 0
    with f:     # the tail, then what follows it, from one open file: no line falls between
        f.seek(_tail_start(f, a.lines, j.get("log_at") or 0))
        while True:
            chunk = f.read()
            if chunk:
                sys.stdout.write(chunk.decode(errors="replace"))
                sys.stdout.flush()
                continue
            cur = jobcore.find(j["id"])
            if not cur or cur["state"] in jobstore.ENDED:     # a pending retry is queued, not ended: keep following
                sys.stdout.write(f.read().decode(errors="replace"))
                print(_ended_line(cur, j["id"]), file=sys.stderr)
                return 0 if cur and cur["state"] == "done" else 1     # like wait
            time.sleep(0.5)


def cmd_kill(a) -> int:
    """Ask for each job to be stopped, and report the outcome that is recorded for it.

    A kill signals no job itself. It writes a stop on the job in one transaction and waits for the record of
    how the job ended, the way `wait` waits for an outcome. The queue's worker carries that stop out, so this
    makes sure the queue has one: it wakes the worker that holds the claim, or starts one where nobody does.
    That worker signals the job's process group, escalates to SIGKILL after the grace, reaps it and records
    how it ended, so one actor ends a job and its outcome is recorded once. A worker this started launches the
    queue's queued jobs too, as the worker an `add` starts does.

    The grace is the job's own, resolved onto its record at add time, and `--grace DUR` overrides it for the
    jobs this kill names. A kill runs from any directory against jobs of any project, so the record is the
    only honest source for it.

    A job named as queued or held is refused, since it has no process: `drop` removes it. A job this kill read
    as running may be back in its queue for a retry by the time the stop is written. It has no process then,
    so the transaction ends it there."""
    rc = 0
    jobs = _named(a, "kill")
    go = _gate(a, jobs, "kill", ("running",))
    if go is not None:
        return go
    asked = []          # the jobs a worker has to stop, in the order they were named
    for j in jobs:
        jid = j["id"]
        if j["state"] != "running" and j["_bulk"]:
            continue        # listed by _gate
        if j["state"] != "running":
            hint = f"`pp {_PROG} drop {jid}` removes it" if j["state"] in ("queued", "held") else "it has ended"
            ui.warn(f"{_PROG}: job {jid} is {j['state']}, not running; {hint}")
            rc = 2
            continue
        grace = policy.grace(j) if a.grace is policy.NOTSET else a.grace
        out = jobcore.ask_stop(jid, _by(), grace)
        if out["did"] == "ended":
            print(f"job {jid} already {out['was']}")
            continue
        if out["did"] == "killed":
            # it went back to its queue for a retry: no process exists, so that is where it ended
            print(f"killed {jid} (it was {out['was']}, with no process to stop)  {_name(j)}")
            continue
        asked.append((j, out["t"], grace))
    for q in sorted({j["queue"] for j, _t, _g in asked}):
        _worker_up(q, wake=True)     # the queue's worker is what carries these stops out
    for j, t, grace in asked:
        rc = max(rc, _await_stop(j, grace, t))
    return rc


def _await_stop(j: dict, grace: float, t: float) -> int:
    """Wait for the stop written on this job at t to be carried out, then print how the job ended. -> this
    job's exit status: 0 once it has ended, 1 when pp may not signal it, when the record stops carrying that
    stop, or when the job outlives the wait.

    Every pass makes sure the queue has a worker. A pass that finds one up wakes it, so its next tick is this
    pass and what this waits for is the job's own exit time, not the queue's poll period. A pass that finds
    none starts a round of them, which `worker_up` does at most every START_EVERY, so a worker that cannot
    come up is not started on every pass. Such a round waits for each start in it to say it is ready, so a
    pass that makes one can take the wait past the job's exit time plus KILL_WAIT by those ready waits. The
    deadline is read again as soon as that round is over, and what this prints is the time it really waited,
    not the bound."""
    jid, started_at = j["id"], time.time()
    deadline = started_at + grace + KILL_WAIT
    while True:
        cur = jobcore.find(jid)
        if cur is None:
            print(f"dropped {jid}")
            return 1
        stop = cur.get("stop") or {}
        if cur["state"] in jobstore.ENDED:
            break
        if stop.get("t") != t:
            # the record no longer carries this stop, so nobody is going to carry it out: say where the job is
            where = f"it is {cur['state']}" + (f", attempt {cur['attempt']}" if cur.get("attempt", 1) > 1 else "")
            hint = f"`pp {_PROG} drop {jid}` removes it" if cur["state"] in ("queued", "held") else "ask again"
            ui.warn(f"{_PROG}: job {jid} no longer carries the stop it was asked for; {where}, so {hint}")
            return 1
        if stop.get("denied"):
            ui.warn(f"{_PROG}: job {jid}: not allowed to signal process group {cur.get('pgid')}; "
                    "it is still running")
            return 1
        if time.time() > deadline:
            # what the queue's worker is says who has the stop now, and the record says what has gone out for
            # it already, whoever sent it. Neither answer names a log that does not exist
            q, waited = j["queue"], ui.human_duration(time.time() - started_at)
            went = ("the SIGKILL went out" if stop.get("kill9") else
                    "the project's stop command went out" if stop.get("cmd") else
                    "the SIGTERM went out" if stop.get("sent") else "no signal of pp's has gone out")
            said = (f"{_PROG}: job {jid} is still running {waited} after it was asked to stop; {went}. "
                    f"`pp {_PROG} show {jid}` says what went out for it.")
            if jobcore.worker_now(q).up:
                ui.warn(f"{said} The stop went to the {q} worker on every pass, so this wait is not the "
                        f"queue's poll period. The {q} worker's log: {jobcore.worker_log(q)}")
            else:
                ui.warn(f"{said} No worker of the {q} queue is up now, so nothing is carrying it out. The "
                        f"{q} worker's log says why none came up: {jobcore.worker_log(q)}")
            return 1
        # a pass wakes the worker that is up, so its next tick is this pass and what this waits out is the
        # job's own exit time. Where none is up, worker_up starts one at most every START_EVERY
        _worker_up(j["queue"], quiet=True, wake=True)
        if time.time() > deadline:
            continue        # a round of starts took this pass past the deadline: look once more and report
        time.sleep(KILL_POLL)
    stop = cur.get("stop") or {}
    if not stop.get("sent"):
        print(f"job {jid} had already ended: {_ended_line(cur, jid)}")
    elif not cur.get("reason"):
        # the reason on the record says whose outcome this is: an exit code the attempt wrote after the stop
        # went out is that stop's outcome, with the code kept as `rc`, so a code alone names nobody
        print(f"job {jid} ended on its own as it was being killed: {_ended_line(cur, jid)}")
    else:
        # the grace on the record, which is the shorter of every ask (`jobpolicy.merge_stop`) and the one the SIGKILL
        # went out after, not the one this kill asked for
        # and what went out for it, off the record and not off the command the job carries: a command that
        # could not start ran nothing, and pp's own signal went out for that stop instead
        went = ("the project's stop command" if stop.get("cmd") else "SIGTERM")
        how = (f"SIGKILL after {float(stop.get('grace') or 0):g}s" if stop.get("kill9")
               else f"stopped on {went}")
        # a stop command that failed is said with its code: pp will not report a stop it did not achieve
        said = "" if not stop.get("rc") else f"; the project's stop command failed (exit {stop['rc']})"
        print(f"killed {jid} ({how}){said}  {_name(cur)}")
    return 0


def _transition(a, src: tuple, dst: str | None, verb: str, done: str) -> int:
    rc, skipped, started = 0, [], set()
    jobs = _named(a, verb)
    if verb == "drop":
        go = _gate(a, jobs, "drop", src, REMOVES)
        if go is not None:
            return go
    # one transaction per job, each told the states it may act on, so none of them acts on a copy that has
    # gone stale; the record it moved, or the state the store holds it in, is what this says
    moved, left = jobcore.forget(jobs, src) if dst is None else jobcore.set_state(jobs, src, dst)
    was = {cur["id"]: cur for cur in left}
    for j in jobs:
        if j["id"] in was:
            cur = was[j["id"]]
            if cur["state"] == dst:
                # already where the verb puts it: the state suits the verb, so the job is left as it is and
                # this is no refusal. A driver that holds a stage twice is not an error the second time
                print(f"{j['id']} is already {dst}  {_name(cur)}")
                continue
            if j["_bulk"]:
                if verb != "drop":          # drop's plan listed them already
                    skipped.append(cur)
                continue
            hint = f". `pp {_PROG} kill {j['id']}` stops it" if cur["state"] == "running" else ""
            ui.warn(f"{_PROG}: job {j['id']} is {cur['state']}; {verb} works on {'/'.join(src)} jobs{hint}")
            rc = 2
            continue
        print(f"{done} {j['id']}  {_name(j)}")
        if dst == "queued":
            started.add(j["queue"])
    _left(skipped)
    if not getattr(a, "no_start", False):          # a job added with --hold started no worker; one per queue
        for q in sorted(started):
            _worker_up(q, wake=True)
    return rc


def _reprio(a) -> int:
    rc, skipped = 0, []
    for j in _named(a, "reprio"):
        cur = jobcore.amend(j["id"], {"prio": a.prio}, must_be=("queued", "held"))
        if cur is None:
            got = jobcore.find(j["id"])
            if j["_bulk"]:
                skipped.append(got or {**j, "state": "gone"})
                continue
            ui.warn(f"{_PROG}: job {j['id']} is {got['state'] if got else 'gone'}; reprio works on queued/held jobs")
            rc = 2
            continue
        print(f"reprio {j['id']} -> p{a.prio}")
    _left(skipped)
    return rc


# What a retry copies onto the new job: what names the work, and every per-job setting the record holds, from
# the one declaration. So a row that lands on a record is carried over without a second list saying so. The
# owner is not carried: the session that asks for the retry owns the job it queues, so `wait --mine` covers it.
CARRY = ("queue", "tag", "project", "cwd", "cmd", "runner", "stop_cmd", "shell_env") \
    + tuple(dict.fromkeys(r.on_record for r in JOB_ROWS))


def cmd_retry(a) -> int:
    """Re-queue each ended job the selection named as a new job of the same work, with a new id.

    A job that succeeded has ended, so it suits `retry` and a selection over a family re-queues the finished
    runs with the failed ones. That is what the plan is for: a selection naming more than one job is shown
    and asked about, as `kill` and `drop` are, and `-n` is the form to type first. There is no selection by
    outcome; the recovery for the rest is to re-run the plan that writes the campaign file.

    The new job carries everything but the outcome, and its owner is the session asking for it, so that
    session hears how it ends."""
    started, rc = set(), 0
    jobs = _named(a, "retry")
    go = _gate(a, jobs, "retry", jobstore.ENDED)
    if go is not None:
        return go
    owner = session.caller()
    for j in jobs:
        if j["state"] not in jobstore.ENDED and j["_bulk"]:
            continue        # listed by _gate
        if j["state"] not in jobstore.ENDED:
            ui.warn(f"{_PROG}: job {j['id']} is {j['state']}; retry re-queues ended jobs")
            rc = 2
            continue
        new = {k: j[k] for k in CARRY if k in j}
        new.update(retry_of=j["id"], attempt=1, owner=owner)
        jid = jobcore.add_jobs([new], "queued")[0]
        print(jid)
        print(f"pp {_PROG}: retry of {j['id']} queued as {jid} ({j['queue']})", file=sys.stderr)
        started.add(j["queue"])
    if not a.no_start:
        for q in sorted(started):
            _worker_up(q, wake=True)
    return rc


def cmd_clean(a, scope: list[str] | None) -> int:
    cutoff = time.time() - a.older if a.older is not None else None
    specs = _specs(a)
    named = _resolve(specs, scope) if specs else jobcore.jobs(jobstore.ENDED, scope)
    for j in named:     # a job named by its own id must suit the verb; any other form leaves it as it is
        if j["state"] not in jobstore.ENDED and not j.get("_bulk"):
            ui.die(f"{_PROG}: job {j['id']} is {j['state']}; clean removes ended jobs. "
                   + (f"`pp {_PROG} kill {j['id']}` stops it" if j["state"] == "running"
                      else f"`pp {_PROG} drop {j['id']}` removes it"), 2)
    victims = [j for j in named
               if j["state"] in jobstore.ENDED and (cutoff is None or (j.get("ended") or 0) < cutoff)]
    if not victims:
        print("nothing to clean")
        return 0
    print(f"{len(victims)} ended job record(s)" + (f" older than {a.older}" if a.older else ""))
    for q in sorted({j["queue"] for j in victims}):
        print(f"  {q}: {_ranges(sorted(j['id'] for j in victims if j['queue'] == q))}")
    print(REMOVES)
    if a.dry_run:
        return 0
    if not ui.confirm("remove them?", a.yes):
        return 2
    gone, _left_as_they_were = jobcore.forget(victims, jobstore.ENDED)
    print(f"removed {len(gone)}")
    return 0


def _event(prev: dict | None, j: dict | None, jid: int) -> str | None:
    """watch: the lines for a change of a job (start, retry, hold, release), before its outcome; None when nothing
    changed. A poll can miss steps: a held job seen running or ended was released, and a job seen ended that was
    held or queued was started, so those lines come too."""
    if j is None or prev is None:
        return None
    st, k, was = j["state"], j.get("attempt", 1), prev["state"]
    if (st, k) == (was, prev.get("attempt", 1)):
        return None
    tag = _name(j)
    started = f"{jid} started{' ' + _attempt_str(j) if _attempt_str(j) else ''}  {tag}"
    lines = [f"{jid} released  {tag}"] if was == "held" and st != "held" else []
    if st in jobstore.ENDED:
        lines += [started] if was in ("held", "queued") else []
    elif st == "running":
        lines.append(started)
    elif st == "queued" and k > prev.get("attempt", 1):
        h = (j.get("history") or [{}])[-1]
        why = h.get("reason") or f"exit {h.get('rc')}"
        left = _in_delay(j.get("not_before"))
        lines.append(f"{jid} attempt {k - 1}/{int(j.get('retries') or 0) + 1} failed ({why}); attempt {k} in {left}  {tag}")
    elif not lines:
        lines.append(f"{jid} {st}  {tag}")
    return "\n".join(lines) or None


def cmd_wait(a, scope: list[str] | None, stream: bool = False) -> int:
    """wait: block until every job it named has ended, then print their outcomes. watch (stream): a line per
    event as it happens. Both hold a watcher lease while they run (`show` lists it) and stamp each outcome
    they print on the job.

    Four forms, and what each one names: a selection; nothing, which is every job that is queued or running;
    --idle, which is until nothing is queued or running; and --mine, which is this session's live jobs and
    the outcomes it never heard. A selection together with --idle or --mine is a usage error, since those two
    are the selection.

    A held job is not ended, so a wait that named one waits for it and says so in one line. The two forms
    that name no job name none held either: a bare wait takes the jobs that are queued or running and lets
    go of one that is parked while it waits, and --idle counts those two states, so a parked stage leaves
    neither waiting forever."""
    verb = "watch" if stream else "wait"
    specs = _specs(a)
    if specs and (a.idle or a.mine):
        ui.die(f"{_PROG} {verb}: --idle and --mine are the selection, so neither takes one", 2)
    t0 = time.time()

    def say(*lines):
        for line in lines:
            print(line, flush=True)

    def timed_out():
        return a.timeout is not None and time.time() - t0 >= a.timeout

    def until_the_next_look():
        """How long before the next pass: the poll, or what is left of the --timeout where that is less. So a
        --timeout is honoured whatever the --poll is, and a long poll never waits a deadline out."""
        left = a.poll if a.timeout is None else a.timeout - (time.time() - t0)
        return max(min(a.poll, left), 0.0)

    def footer():
        here = jobcore.jobs(jobstore.LIVE, scope)       # one listing: the three counts are the same jobs
        live = {st: len([j for j in here if j["state"] == st]) for st in jobstore.LIVE}
        say(f"[{_PROG} {verb}] {datetime.now():%H:%M:%S}: running {live['running']}, queued {live['queued']}, held {live['held']}")

    def tell(j, jid):
        say(*_outcome_lines(j, jid, a.tail))
        if j is not None:
            _stamp_reported(jid, verb)

    if a.idle:
        before = {j["id"] for j in jobcore.jobs(jobstore.ENDED, scope)}
        told: set = set()
        asked: set = set()      # the queues this loop has asked for a worker: it says what holds a claim once
        with jobcore.watching(None, scope, verb):
            while True:
                # one listing: the jobs still to come and the ones that ended are read together, so the last
                # look, the one that finds none live, holds every outcome there is to report
                here = jobcore.jobs(queue=scope)
                live = [j for j in here if j["state"] in ("queued", "running")]
                ended = [j for j in here if j["state"] in jobstore.ENDED and j["id"] not in before]
                if not live:
                    break
                if stream:
                    for j in sorted(ended, key=lambda j: j["id"]):
                        if j["id"] not in told:
                            told.add(j["id"])
                            tell(j, j["id"])
                if not a.no_start:      # running ones too: only a worker records how a job ended
                    for q in {j["queue"] for j in live}:
                        _worker_up(q, quiet=q in asked)
                        asked.add(q)
                if timed_out():
                    print(f"[{_PROG} {verb}] timeout after {a.timeout:g}s", file=sys.stderr)
                    return 3
                time.sleep(until_the_next_look())
            for j in sorted(ended, key=lambda j: j["id"]):
                if j["id"] not in told:
                    tell(j, j["id"])
        footer()
        return 1 if any(j["state"] == "failed" for j in ended) else 0
    if specs:
        targets = [j["id"] for j in _resolve(specs, scope)]
    elif a.mine:
        sid, mine = _of_session(verb)

        def unheard(j):     # ended before this session waited on it: a quick failure must not be missed
            return not any(isinstance(r, dict) and r.get("session") == sid for r in j.get("reported") or [])

        # one listing of both halves: a job is listed once, in one state, so a target is named once
        here = jobcore.jobs(queue=scope)
        targets = [j["id"] for j in here if j["state"] in jobstore.LIVE and mine(j)] \
            + [j["id"] for j in here if j["state"] in jobstore.ENDED and mine(j) and unheard(j)]
        if not targets:
            say("nothing of this session's is queued, held or running, and every outcome was reported to it")
            return 0
    else:
        # a bare wait: every job that is queued or running, and all of them, so a script that waits for
        # what it queued is not told the first outcome and left with the rest still to come. A held job is
        # named by no selection here, so none is waited for: a parked stage is normal
        targets = [j["id"] for j in jobcore.jobs(("queued", "running"), scope)]
        if not targets:
            say("nothing queued or running")
            return 0
    bare = not specs and not a.mine     # the form that names no job, so it names none held either
    noted: set = set()                  # the held jobs this wait has named, each named in one line once

    def held_now(cur: dict) -> set:
        """The jobs of this pass that are held, in one line the first time each one is seen. -> the ones a
        bare wait is no longer waiting for, which is all of them there: a selection is what asks for a job,
        so a wait that named one waits for it held, and a bare wait named none."""
        held = {t for t, j in cur.items() if j and j["state"] == "held"}
        new = sorted(held - noted)
        if new:
            noted.update(new)
            print(f"[{_PROG} {verb}] note: {', '.join(map(str, new))} held, so this "
                  + ("does not wait for them" if bare else "waits forever")
                  + f". `pp {_PROG} release` them", file=sys.stderr)
        return held if bare else set()

    with jobcore.watching(targets, None, verb):
        prev: dict = {}
        told: set = set()
        asked = set()           # as above: a loop says what holds a queue's claim on its first pass only
        blamed: set = set()     # the queues this loop has said no worker came up for, which it says once
        while True:
            cur = {t: jobcore.find(t) for t in targets}
            parked = held_now(cur)      # a job held while this waits: a bare wait drops it and returns
            if parked:
                targets = [t for t in targets if t not in parked]
                cur = {t: j for t, j in cur.items() if t not in parked}
            if stream:
                if not prev:
                    say(f"[{_PROG} watch] " + ", ".join(f"{t} {cur[t]['state'] if cur[t] else 'gone'}" for t in targets))
                for t in targets:
                    line = _event(prev.get(t), cur[t], t)
                    if line:
                        say(line)
                    if t not in told and (cur[t] is None or cur[t]["state"] in jobstore.ENDED):
                        told.add(t)
                        tell(cur[t], t)
                prev = {t: j for t, j in cur.items() if j}
            ended = {t: j for t, j in cur.items() if j is None or j["state"] in jobstore.ENDED}
            if len(ended) == len(targets):
                for t in sorted(ended):
                    if t not in told:
                        tell(ended[t], t)
                return 1 if any(j is None or j["state"] == "failed" for j in ended.values()) else 0
            if not a.no_start:      # a dead worker would otherwise make this wait forever, queued or running:
                for q in {j["queue"] for j in cur.values() if j and j["state"] in ("queued", "running")}:
                    quietly = stream or q in asked
                    w = _worker_up(q, quiet=quietly)             # only a worker records how a job ended
                    asked.add(q)
                    if w.why and q not in blamed:
                        # `watch` asks quietly on every pass, so a queue no worker can come up for would
                        # leave it printing events forever and never the reason none launches the job
                        blamed.add(q)
                        if quietly:
                            say(f"[{_PROG} {verb}] the {q} worker did not come up: {w.why}; "
                                f"its log: {jobcore.worker_log(q)}")
            if timed_out():
                print(f"[{_PROG} {verb}] timeout after {a.timeout:g}s: {len(targets) - len(ended)} of {len(targets)} not ended",
                      file=sys.stderr)
                return 3
            time.sleep(until_the_next_look())


def cmd_worker(a, q_given: str | None, default_queue: str, scope: list[str] | None) -> int:
    """`worker status`, `worker start` and `worker stop`, each through the one construct that says what a
    queue's worker is.

    `worker status --json` is a list, one entry per queue in scope, and this is its shape, stated once:

        queue   the queue's name
        pid     the pid of its worker; null where none is up and while one that is starting names nothing yet
        how     "up" (a worker holds the claim and has recorded itself), "down" (nobody holds it) or
                "starting" (a worker holds it and has not recorded itself yet, which is a worker's first
                moment). "started" is `worker start`'s answer and never appears here, since this starts none
        up      whether a worker is up, which is `pid` being a number
        log      where that worker writes

    A field change goes in CHANGELOG.md (CONTRIBUTING.md, "Adding a command").
    """
    if a.action == "status":
        qs = [q_given] if q_given else _scope_list(scope)
        rows = [{"queue": q, "pid": (w := jobcore.worker_now(q)).pid, "how": w.how, "log": jobcore.worker_log(q)} for q in qs]
        if a.json:
            ui.print_json([{**r, "up": r["pid"] is not None} for r in rows])
        elif not rows:
            print("no queues yet")
        for r in [] if a.json else rows:
            state = (f"worker up (pid {r['pid']})" if r["pid"] is not None
                     else "worker starting" if r["how"] == jobcore.STARTING else "worker down")
            print(f"{r['queue']}: {state}   log {r['log']}")
        return 0
    q = q_given or default_queue
    jobcore.writing_queue(q)    # `worker start -q NAME` opens that queue, so it goes through the same refusal
    logf = jobcore.worker_log(q)
    if a.action == "start":
        # one answer, from the scope that acted on it: nothing looks at the claim again, so a worker that came
        # up between two looks can never read as one that never came
        w = _worker_up(q, quiet=True)
        if w.how == jobcore.STARTED:
            print(f"{q}: started worker (pid {w.pid}; log {logf})")
        elif w.up:
            print(f"{q}: worker already up (pid {w.pid})")
        elif w.how == jobcore.STARTING:
            ui.die(f"{_PROG}: {_starting(q)}; ask again", 1)
        else:
            # this asked quietly, so the reason the answer carries is said here: a start that timed out
            # logged nothing of its own, and that reason is the only word there is about it
            why = f": {w.why}" if w.why else ""
            ui.die(f"{_PROG}: the {q} worker did not come up{why}; its log: {logf}")
        return 0
    # the stop is the core's: one look, the signal to the worker that look found, and the wait for that
    # holder. What it did is worded here, with the pid it signalled
    def slow(kind, job, f):
        print(f"{q}: worker {f['pid']} is finishing its tick (a probe or nvidia-smi); waiting", flush=True)
    out = jobcore.worker_stop(q, slow)
    pid = out["pid"]
    if out["did"] == "starting":        # there is nothing to signal yet: it has not recorded itself
        ui.die(f"{_PROG}: {_starting(q)}; ask again", 1)
    if out["did"] == "down":
        print(f"{q}: worker down")
        return 0
    if out["did"] == "denied":
        ui.die(f"{_PROG}: not allowed to stop the {q} worker (pid {pid}); it belongs to another user", 1)
    if out["did"] == "timeout":
        ui.die(f"{_PROG}: the {q} worker ({pid}) did not stop within {ui.human_duration(jobcore.STOP_WAIT)}; its log: {logf}")
    print(f"{q}: stopped worker {pid}; running jobs keep running and the next worker adopts them")
    return 0


# ---- the CLI ----------------------------------------------------------------------------------------

def _selectors(p) -> None:
    """The two of the five selection forms that are flags, on a verb that takes jobs: --prefix P, repeatable,
    and --all, which takes -q. The other three are words the verb is given, so every verb that takes jobs
    takes the same five (section 3's grammar, `jobpolicy.select`)."""
    p.add_argument("--prefix", action="append", metavar="P",
                   help="every job whose tag starts with P (repeatable)")
    p.add_argument("--all", action="store_true", help="every job of the queue -q names")


def _parser(prog: str, doc: str, add_args=None):
    ap = ui.parser(prog, doc)
    sub = ap.add_subparsers(dest="verb", metavar="VERB")

    def sp(name, **kw):
        p = sub.add_parser(name, **kw)
        p.add_argument("-q", "--queue", help="the named queue (see `pp help queue`)")
        return p

    p = sp("add", help="queue a command (after --)")
    p.add_argument("--tag", type=_tag_flag,
                   help="a name to select it by, alone or with the other jobs that share it: at most 64 of "
                        "letters, digits and . _ - : (default: none, and its id names it)")
    for row in JOB_ROWS + PROJECT_ROWS:     # one flag per row that has one, from the one declaration
        _flag(p, row)
    p.add_argument("--cwd", help="run there (default: here); also picks the project")
    p.add_argument("--after", metavar="SELECTION",
                   help="wait until every job this names has ended, whatever the outcome: ids, ranges and "
                        "tags separated by commas (41,42 / 41-66 / s1_eval,s1_report), read as the job is "
                        "added and kept as ids on its record")
    p.add_argument("--hold", action="store_true", help="park it as held until released")
    p.add_argument("--no-start", action="store_true", help="do not start a worker")
    p.add_argument("--from", dest="from_", metavar="FILE", help="one command per line; - for stdin")
    if add_args:
        add_args(p)
    p.add_argument("cmd", nargs="...")
    for name in ("status", "ls"):
        p = sp(name, help="running, queued, held, recent" if name == "status" else "same as status")
        p.add_argument("--json", action="store_true")
        p.add_argument("-n", type=int, default=10, help="recent jobs shown")
        g = p.add_mutually_exclusive_group()
        g.add_argument("--here", action="store_true", help="only this directory's project")
        g.add_argument("--project")
        p.add_argument("--mine", action="store_true",
                       help="only the jobs this session queued, in every state; applies with --here/--project")
    p = sp("show", help="every field of a job, and its log tail")
    p.add_argument("id", nargs="?", metavar="ID")
    _selectors(p)
    p.add_argument("--json", action="store_true")
    # note takes one job and then text of its own, so the job is a word and the two forms that are flags
    # are no use to it: a note on a family is refused, and `note one by its id` is what it says
    p = sp("note", help="attach a note to a job (any state)")
    p.add_argument("id", metavar="ID")
    p.add_argument("text", nargs="+")
    p.add_argument("--by", metavar="NAME", help="the author shown (default: the calling agent session, else user)")
    p = sp("log", help="tail a job's log")
    p.add_argument("id", nargs="?", metavar="ID")
    _selectors(p)
    p.add_argument("-f", "--follow", action="store_true")
    p.add_argument("-n", "--lines", type=int, default=40)
    p = sp("kill", help="stop running jobs")
    p.add_argument("ids", nargs="*", metavar="ID")
    _selectors(p)
    _flag_override(p, BY_SCOPE["job"]["grace"],
                   "the gap before the SIGKILL for the jobs this kill names (default: each job's own)")
    p.add_argument("-n", "--dry-run", action="store_true", help="only show which jobs it would stop")
    p.add_argument("-y", "--yes", action="store_true", help="do not ask about a range or a shared tag")
    for name, hlp in (("drop", "remove queued/held jobs"), ("hold", "park queued jobs"), ("release", "un-park held jobs")):
        p = sp(name, help=hlp)
        p.add_argument("ids", nargs="*", metavar="ID")
        _selectors(p)
        if name == "drop":
            p.add_argument("-n", "--dry-run", action="store_true", help="only show which jobs it would remove")
            p.add_argument("-y", "--yes", action="store_true", help="do not ask about a range or a shared tag")
        if name == "release":
            p.add_argument("--no-start", action="store_true", help="do not start a worker")
    p = sp("reprio", help="change a queued or held job's priority")
    p.add_argument("ids", nargs="*", metavar="ID")
    p.add_argument("prio", type=int)
    _selectors(p)
    p = sp("retry", help="re-queue ended jobs as new jobs")
    p.add_argument("ids", nargs="*", metavar="ID")
    _selectors(p)
    p.add_argument("-n", "--dry-run", action="store_true", help="only show which jobs it would re-queue")
    p.add_argument("-y", "--yes", action="store_true", help="do not ask about a range or a shared tag")
    p.add_argument("--no-start", action="store_true")
    p = sp("clean", help="remove ended job records and pp's own files for them")
    p.add_argument("ids", nargs="*", metavar="ID")
    _selectors(p)
    p.add_argument("--older", metavar="AGE", type=_dur_flag("d"),
                   help="only those ended before AGE ago (30m, 12h, 7d; a bare number is days)")
    p.add_argument("-n", "--dry-run", action="store_true")
    p.add_argument("-y", "--yes", action="store_true")
    for name, hlp in (("wait", "block until jobs end, then print each outcome"),
                      ("watch", "a line per event as it happens; exits like wait")):
        p = sp(name, help=hlp)
        p.add_argument("ids", nargs="*", metavar="ID")
        _selectors(p)
        g = p.add_mutually_exclusive_group()
        g.add_argument("--idle", action="store_true", help="until nothing is queued or running")
        g.add_argument("--mine", action="store_true", help="every job this agent session queued that is live, or ended without a wait/watch of this session reporting it")
        p.add_argument("--timeout", metavar="DUR", type=_dur_flag(),
                       help="give up after DUR (90s, 30m, 2h, 1d; a bare number is seconds) and exit 3")
        p.add_argument("--poll", metavar="DUR", type=_dur_flag(), default=2.0, help="between looks")
        p.add_argument("--tail", type=int, default=15, help="log lines shown with a failure")
        p.add_argument("--no-start", action="store_true", help="do not start a worker for queued jobs")
    p = sp("work", help="run a queue's worker in the foreground")
    p = sp("worker", help="start | stop one queue's worker (-q, else the default queue); status covers every queue")
    p.add_argument("action", choices=("start", "stop", "status"))
    p.add_argument("--json", action="store_true", help="status as JSON")
    return ap


def run_cli(argv: list[str], prog: str, default_queue: str, doc: str, prepare_job=None, add_args=None) -> int:
    """The queue CLI for `pp <prog>`. default_queue receives `add` and `work` without -q; status, wait --idle, clean
    and worker status cover every queue. prepare_job(job, args, project) amends each job at add time, with the
    project its [queue] table was read from; add_args(parser) adds options to `add`.

    The store's refusal of a queue another layout wrote is worded here, the one place it is said: every verb
    opens a queue, so the refusal can come from any of them, and the store writes no sentence."""
    global _PROG
    _PROG = jobcore.PROG = prog     # the worker the core starts is this command's own
    try:
        return _verb(argv, prog, default_queue, doc, prepare_job, add_args)
    except jobstore.Foreign as e:
        says = f"its FORMAT file says {e.got}" if e.got is not None else "it has no FORMAT file"
        ui.die(f"{_PROG}: queue '{e.queue}' holds jobs this pp cannot read: {says}. This pp reads format "
               f"{jobstore.FORMAT}. Drain that queue with the pp that wrote it, or "
               f"remove {jobstore.qroot(e.queue, create=False)}.", 2)


def _verb(argv: list[str], prog: str, default_queue: str, doc: str, prepare_job, add_args) -> int:
    """The verb the command line names, carried out: the parsing, the queue it is given, and the one row."""
    argv = list(argv)
    q_top = None
    if len(argv) >= 2 and argv[0] in ("-q", "--queue"):
        q_top, argv = argv[1], argv[2:]
    if not argv or (argv[0].startswith("-") and argv[0] not in ("-h", "--help")):
        argv = ["status"] + argv
    cmd: list[str] = []
    if argv[0] == "add" and "--" in argv:
        i = argv.index("--")
        argv, cmd = argv[:i], argv[i + 1:]
    ap = _parser(prog, doc, add_args)
    a = ap.parse_args(argv)
    q = getattr(a, "queue", None) or q_top
    if q is not None and (not jobstore.NAME_RE.fullmatch(q) or q in jobstore.RESERVED):
        ui.die(f"{prog}: '{q}' is not a queue name (lowercase letters, digits, - and _; not {' or '.join(jobstore.RESERVED)})", 2)
    scope = a.scope = [q] if q else None
    if q is not None:
        jobcore.open_queue(q)       # a queue named with -q is opened once here, whatever the verb does with it
    if a.verb == "add" and cmd and a.cmd:
        ui.die(f"{prog} add: put the whole command after --", 2)
    if a.verb == "add" and a.from_ and (cmd or a.cmd):
        ui.die(f"{prog} add: --from reads every command from that file, so this command line carries none", 2)
    # one row per verb: what it is called with is in the row, and nothing else is keyed on a verb's name. The
    # call is held back until the verb is chosen, so a verb's own options are read on that verb's path only
    verbs = {"add": lambda: cmd_add(a, cmd or a.cmd, q or default_queue, prepare_job, ap),
             "status": lambda: cmd_status(a, scope), "ls": lambda: cmd_status(a, scope),
             "show": lambda: cmd_show(a), "note": lambda: cmd_note(a), "log": lambda: cmd_log(a),
             "kill": lambda: cmd_kill(a), "reprio": lambda: _reprio(a), "retry": lambda: cmd_retry(a),
             "drop": lambda: _transition(a, ("queued", "held"), None, "drop", "dropped"),
             "hold": lambda: _transition(a, ("queued",), "held", "hold", "held"),
             "release": lambda: _transition(a, ("held",), "queued", "release", "released"),
             "clean": lambda: cmd_clean(a, scope),
             "wait": lambda: cmd_wait(a, scope), "watch": lambda: cmd_wait(a, scope, stream=True),
             "work": lambda: cmd_work(q or default_queue),
             "worker": lambda: cmd_worker(a, q, default_queue, scope)}
    return verbs[a.verb]() if a.verb in verbs else 2
