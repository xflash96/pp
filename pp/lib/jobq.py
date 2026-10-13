"""The job-queue engine behind `pp queue`, and its CLI (run_cli).

The store is pp/lib/jobstore.py (one file per job, and the transaction that changes one) and the rules are
pp/lib/jobpolicy.py (the facts in, one decision out). Here: the verbs, the views, the readings a limit is
checked against, the lines a setting is read from, and the help.

One worker per queue reaps ended jobs, carries out the stops asked of them, re-queues retries, starts what
fits, and fires the job's on_fail / on_done hook on its FINAL outcome. Admission is strict head-of-line by
priority (a big job is never starved by small ones) unless the worker runs with backfill; a job waiting out its
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

import json
import os
import re
import shlex
import shutil
import signal
import sys
import time
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


def _read_settings(q: str, backfill: bool | None, poll: float | None):
    """What one pass of q's worker loop runs under: {poll, idle_exit, backfill}, read once per pass, so a
    queue's limits change under its running jobs with no restart. A config that cannot be read comes back as
    `error` beside the defaults, which is what stops that pass launching anything."""
    def read():
        try:
            cfg = _qcfg(q)
        except config.ConfigError as e:
            return {"poll": float(poll or 2), "idle_exit": 600.0, "backfill": bool(backfill), "error": str(e)}
        return {"poll": float(poll or cfg.get("poll", 2)), "idle_exit": float(cfg.get("idle_exit", 600)),
                "backfill": bool(cfg.get("backfill", False)) if backfill is None else bool(backfill)}
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
        tag = None if job is None else job.get("tag")
        if kind == "timeout":
            say(f"timeout {jid} {tag} after {ui.human_duration(float(job['timeout']))}: stopping it")
        elif kind == "again":
            _append_log(job, _retry_line(f))        # the job's own log, before the queue's
            say(f"retry {jid} {tag}: attempt {job['attempt']}/{int(job.get('retries') or 0) + 1} "
                f"in {_in_delay(job['not_before'])}")
        elif kind == "ended":
            why = f" ({job['reason']})" if job.get("reason") else ""
            say(f"{'done' if job['state'] == 'done' else 'FAILED'} {jid} {tag} "
                f"rc={job.get('rc')}{why} {ui.human_duration(job.get('seconds'))}")
        elif kind == "stop":
            word = ("SIGTERM to its group" if f["sig"] == signal.SIGTERM else
                    f"SIGKILL after {ui.human_duration(float(f['grace'] or 0))}")
            say(f"stop {jid} {tag} ({(job.get('stop') or {}).get('by')}): "
                + (f"not allowed to signal its group {f['group']}" if f["outcome"] == "denied" else word))
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


def worker_for(q: str, backfill: bool = False, log=None) -> jobcore.Worker:
    """Queue q's worker, wired: the admission of a pass, the tail a retry is matched against, and the sink it
    says what it did to. The one place a Worker is built."""
    return jobcore.Worker(q, backfill, _voice(q, log), lambda job, n: "\n".join(log_tail(job, n)), _admit_for(q))


def cmd_work(q: str, backfill: bool | None = None, poll: float | None = None) -> int:
    """`work`: queue q's worker in this process. The loop is the core's; the settings, the admission and the
    words are read and said here."""
    def log(msg):
        print(f"[{q} {datetime.now():%m-%d %H:%M:%S}] {msg}", flush=True)
    return jobcore.work(q, _read_settings(q, backfill, poll), _admit_for(q), _voice(q, log),
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


# ---- durations, small parsing ------------------------------------------------------------------------

parse_duration = config.parse_duration


def _dur_arg(v, what: str, default_unit: str = "s") -> float:
    """The one reading of a duration a command line gave, in seconds: a usage error where it is not one, and
    where it is empty.

    A config key written as "" is a key that is not set, since the default behind it stands. A command line
    has no such key: `--timeout "$T"` with T unset is the mistake it looks like, and not a wait with no
    deadline or a `clean --older ""` with no bound. "no bound" has a spelling of its own where a flag offers
    one (`add --timeout none`), and leaving the flag out is the other."""
    try:
        secs = parse_duration(v, default_unit)
    except ValueError as e:
        ui.die(f"{_PROG}: {what}: {e}", 2)
    if secs is None:
        ui.die(f"{_PROG}: {what}: say how long, like 90s, 30m, 2h or 1d", 2)
    return secs


# ---- resources ------------------------------------------------------------------------------------
#
# The pool model as users see it (shared and per-queue pools, probes, settle) is `pp help queue`'s "Resources"; the
# defaults below are the ones it states. Here: a job's needs are a dict NAME -> amount; Resources.check asks every pool
# that provides a NAME, a probe's reading is cached in probes/ for `every`, and jobs launched less than `settle`
# before a reading are subtracted from it.

SHARED_DEFAULTS = {"cpu": "auto", "mem_gb": "auto", "gpu_gb": "auto"}
QUEUE_DEFAULTS = {"slots": 3}
POOL_KEYS = ("max", "probe", "every", "settle")
PROBE_EVERY = 30.0
PROBE_WAIT = 60.0   # how long a probe may take before pp ends it and its reading says so
SETTLE = 30.0       # a reading (nvidia-smi, a probe) may not show a job launched less than this before it


QUEUE_KEYS = ("timeout", "retries", "retry_delay", "retry_if", "on_fail", "on_done", "idle_exit", "poll", "backfill",
              "resources")


def _qcfg(q: str, empty_ok: bool = True) -> dict:
    """[queue.q], checked: a typo or a bad value is a ConfigError naming it, never a silent default or a traceback.
    Durations come back in seconds. empty_ok False: what a launch reads (config.load)."""
    t = config.load("queue", empty_ok).get(q, {})
    if not isinstance(t, dict):
        raise config.ConfigError(f"[queue.{q}] must be a table")
    t = dict(t)
    where = f"[queue.{q}]"
    config.check_keys(t, where, QUEUE_KEYS)
    for k in ("timeout", "retry_delay", "idle_exit", "poll"):
        if k in t:
            try:
                t[k] = parse_duration(t[k])
            except ValueError as e:
                raise config.ConfigError(f"{where} {k}: {e}") from None
    if "retries" in t and (isinstance(t["retries"], bool) or not isinstance(t["retries"], int) or t["retries"] < 0):
        raise config.ConfigError(f"{where} retries = {t['retries']!r}: a whole number >= 0")
    if "backfill" in t and not isinstance(t["backfill"], bool):
        raise config.ConfigError(f"{where} backfill = {t['backfill']!r}: true or false")
    for k in ("retry_if", "on_fail", "on_done"):
        if k in t and not isinstance(t[k], str):
            raise config.ConfigError(f"{where} {k} = {t[k]!r}: a string")
    if t.get("retry_if"):
        try:
            re.compile(t["retry_if"])
        except re.error as e:
            raise config.ConfigError(f"{where} retry_if {t['retry_if']!r}: {e}") from None
    return t


def _mem_total_gb() -> float:
    """What this machine's memory comes to, which `mem_gb = "auto"` is. libc counts it in pages, and a page
    count times the page size is /proc/meminfo's MemTotal to within one page, which a pool in whole GB cannot
    tell apart. 0.0 where it cannot be read, which `auto` then refuses on."""
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / 2 ** 30
    except (ValueError, OSError):
        return 0.0


CARDS_WHY = ""      # why the last _cards() gave None, for the messages that report it ("" when it had no card)


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
    cards = []
    for line in out.stdout.strip().splitlines():
        parts = [x.strip() for x in line.split(",")]
        try:     # the name may hold commas: index first, the three numbers last
            total, used = float(parts[-3]) / 1024, float(parts[-2]) / 1024
            cards.append({"index": int(parts[0]), "name": ",".join(parts[1:-3]), "total_gb": total,
                          "used_gb": used, "free_gb": total - used, "util": parts[-1]})
        except (ValueError, IndexError):
            continue
    return cards or None


def _pool(name: str, v, scope: str | None) -> dict | None:
    where = (f"[queue.{scope}.resources] " if scope else "[resources] ") + name
    if v is False:           # cpu = false: no such pool here
        return None
    t = dict(v) if isinstance(v, dict) else {"max": v}
    bad = sorted(set(t) - set(POOL_KEYS))
    if bad:
        raise config.ConfigError(f"{where}: unknown {', '.join(bad)} (a table takes {', '.join(POOL_KEYS)})")
    mx = t.get("max")
    if mx == "auto":
        if name not in SHARED_DEFAULTS:
            raise config.ConfigError(f'{where} = "auto": only cpu, mem_gb and gpu_gb know their own size')
        mx = {"cpu": os.cpu_count() or 1, "mem_gb": round(_mem_total_gb()), "gpu_gb": None}[name]   # gpu: per card
    elif mx is not None:
        try:
            mx = policy.num(mx)
        except (TypeError, ValueError):
            raise config.ConfigError(f"{where} = {mx!r}: not a number") from None
    p = {"name": name, "scope": scope, "where": where, "max": mx, "probe": None, "every": PROBE_EVERY,
         "settle": SETTLE}
    if t.get("probe") is not None:
        if name == "gpu_gb":
            raise config.ConfigError(f"{where}: gpu_gb is probed with nvidia-smi already")
        if not isinstance(t["probe"], str) or not t["probe"].strip():
            raise config.ConfigError(f"{where}: probe is a shell command that prints the amount free")
        p["probe"] = t["probe"]
    for k in ("every", "settle"):
        if k in t:
            try:
                p[k] = parse_duration(t[k]) or 0.0
            except ValueError as e:
                raise config.ConfigError(f"{where} {k}: {e}") from None
    if p["max"] is None and p["probe"] is None and name != "gpu_gb":
        raise config.ConfigError(f"{where}: a number, or a table with max and/or probe")
    return p


def pools(q: str, empty_ok: bool = True) -> list[dict]:
    """The pools queue q's jobs draw from: its own ([queue.q.resources]), then the shared ones ([resources])."""
    own = _qcfg(q, empty_ok).get("resources", {})
    if not isinstance(own, dict):
        raise config.ConfigError(f"[queue.{q}.resources] must be a table")
    own = {**QUEUE_DEFAULTS, **own}
    shared = {**SHARED_DEFAULTS, **config.load("resources", empty_ok)}
    return [p for p in [_pool(k, v, q) for k, v in own.items()] + [_pool(k, v, None) for k, v in shared.items()] if p]


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
    a job waits for that reading, and nobody is told the probe failed."""
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
            elif out.returncode == 0 and (m := re.search(r"-?\d+(?:\.\d+)?", out.stdout)):
                r = {"t": t, "value": policy.num(m[0]), "error": None}
            else:
                said = (out.stderr.strip() or out.stdout.strip()).splitlines()
                r = {"t": t, "value": None, "error": (f"exit {out.returncode}" if out.returncode else "printed no number")
                     + (f": {said[-1][:120]}" if said else "")}
        paths.write_json(path, r)
    return r


class Resources:
    """Queue q's pools with their live readings (nvidia-smi, probes), taken once and outside the store lock.

    What a pass is handed is `seen`, those readings as a value: the arithmetic over them is pure, so every
    rule of admission is a table (pp/lib/jobpolicy.py)."""

    def __init__(self, q: str, needed=(), live: bool = True, empty_ok: bool = True):
        self.q = q
        self.pools = pools(q, empty_ok)
        self.by_name: dict[str, list[dict]] = {}
        for p in self.pools:
            self.by_name.setdefault(p["name"], []).append(p)
        needed = set(needed)
        self.cards_t = time.time()
        self.cards = _cards() if "gpu_gb" in needed and "gpu_gb" in self.by_name else None
        self.readings = {(p["scope"], p["name"]): reading(p, run=live)
                         for p in self.pools if p["probe"] and p["name"] in needed}

    @property
    def seen(self) -> dict:
        """These readings as the value a pass admits against, with the file a pool is declared in, which a
        refusal names."""
        return {"q": self.q, "pools": self.pools, "by_name": self.by_name, "cards": self.cards,
                "cards_t": self.cards_t, "probes": self.readings, "cards_why": CARDS_WHY,
                "where": config.global_path()}

    def check(self, needs: dict) -> str | None:
        """Why these needs can never fit; None when they might."""
        return policy.check(needs, self.seen)

    def usage(self, running: list[dict], scope: str | None) -> list[dict]:
        """The pools of one scope (a queue name, or None = shared) with what running jobs hold: for status."""
        out = []
        for p in self.pools:
            if p["scope"] != scope or p["name"] == "gpu_gb":
                continue
            r = reading(p, run=False) if p["probe"] else None
            out.append({"name": p["name"], "max": p["max"],
                        "used": sum(policy.need(j, p["name"]) for j in policy.pool_jobs(p, running)),
                        "probe": p["probe"], "reading": r})
        return out


# ---- worker ---------------------------------------------------------------------------------------

def _tail_start(f, n: int) -> int:
    """Where the last n lines of open binary file f begin (its end when n is 0), without reading all of it."""
    end = pos = f.seek(0, 2)
    buf = b""
    while n > 0 and pos > 0 and buf.count(b"\n") <= n:
        pos = max(0, pos - 65536)
        f.seek(pos)
        buf = f.read(end - pos)
    return end - sum(len(line) for line in buf.splitlines(keepends=True)[-n:]) if n > 0 else end


def _tail(src, n: int) -> list[str]:
    """Last n lines of a file (a path, or a file open for binary reading) without reading all of it."""
    if isinstance(src, str):
        with open(src, "rb") as f:
            return _tail(f, n)
    src.seek(_tail_start(src, n))
    return src.read().decode(errors="replace").splitlines()[-n:] if n > 0 else []


def default_tag(cmd: list[str]) -> str:
    """The tag of a job added without --tag."""
    return shlex.join(cmd)[:40]


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
    try:
        return _tail(job["log"], n) if job.get("log") else []
    except OSError:
        return []


def _retry_line(said: dict) -> str:
    """What a requeue writes in the job's own log, from what the store wrote: the attempt that ended, why, and
    the one to come."""
    return (f"[pp queue] {datetime.now():%Y-%m-%d %H:%M:%S} attempt {said['attempt']}/{said['of']} after "
            f"{said['why']}; attempt {said['next']} in {ui.human_duration(said['delay'])}")


def _append_log(job: dict, line: str):
    try:
        os.makedirs(os.path.dirname(job["log"]), exist_ok=True)
        with open(job["log"], "a") as f:
            f.write(line.rstrip("\n") + "\n")
    except (OSError, KeyError):
        pass


def _stamp_reported(jid: int, verb: str):
    who = session.caller()
    stamp = {"t": time.time(), "by": f"{verb} pid {os.getpid()}", "session": who["session"] if who else None}
    jobcore.amend(jid, {"reported": [stamp]}, must_be=jobstore.ENDED)


# ---- helpers for verbs ----------------------------------------------------------------------------

def _resolve(specs: list[str], scope: list[str] | None = None) -> list[dict]:
    """Jobs named by ids, ranges (12-15) or exact tags, in the order given, without duplicates. Ids are global.
    Each carries _bulk: it came from a range or a tag naming several jobs, so a verb skips it when it is in a state
    the verb does not apply to, where a job named on its own is an error."""
    records = jobcore.jobs(queue=scope)
    by_id = {j["id"]: j for j in records}
    out, seen = [], set()
    for spec in specs:
        hits = [by_id[i] for i in policy.select(records, spec)]      # the five forms, over these records
        if policy.kind(spec) == "tag":
            qs = sorted({j["queue"] for j in hits})
            if len(qs) > 1:
                ui.die(f"{_PROG}: tag '{spec}' names jobs in queues {', '.join(qs)}: "
                       + ", ".join(f"{j['id']} ({j['queue']})" for j in hits) + "; use ids, or -q", 2)
        if not hits:
            ui.die(f"{_PROG}: no job matches '{spec}'", 2)
        for j in hits:
            if j["id"] not in seen:
                seen.add(j["id"])
                out.append({**j, "_bulk": policy.kind(spec) == "range" or len(hits) > 1})
    return out


def _one(spec: str, verb: str, scope: list[str] | None = None) -> dict:
    """The job show, note and log act on. A tag naming several jobs (a retry keeps the tag): the newest, said so;
    note refuses, since a note on the wrong job misleads."""
    hits = _resolve([spec], scope)
    if len(hits) > 1:
        ids = ", ".join(str(j["id"]) for j in hits)
        if verb == "note":
            ui.die(f"{_PROG} note: '{spec}' names jobs {ids}; note one by its id", 2)
        ui.warn(f"{_PROG} {verb}: '{spec}' names jobs {ids}; showing the newest, {hits[-1]['id']}")
    return hits[-1]


def _left(skipped: list[dict]) -> None:
    """Jobs a range or tag swept over that the verb does not apply to: listed once, not errors."""
    if skipped:
        print(ui.c("left as they are: " + ", ".join(f"{j['id']} {j['state']}" for j in skipped), "dim"))


def _gate(a, jobs: list[dict], verb: str, ok: tuple) -> int | None:
    """kill and drop: a job named by its id is acted on at once; a range or a tag naming several jobs is shown first
    and asked about (-y: do not ask). -n only shows. None to go ahead, else the exit code."""
    if not a.dry_run and not any(j["_bulk"] for j in jobs):
        return None
    hits = [j for j in jobs if j["state"] in ok]
    print(f"{verb} {len(hits)} job(s): "
          + (", ".join(f"{j['id']} {_text(j.get('tag'))}" for j in hits) or "none"))
    _left([j for j in jobs if j["state"] not in ok])
    if a.dry_run or not hits:
        return 0
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
    root, data = config.find_project(cwd)
    if root:
        return config.project_name(root, data)
    g = config.git_root(cwd)
    return os.path.basename(os.path.normpath(g or cwd))


def _kv(items: list[str] | None, flag: str, conv) -> dict:
    out = {}
    for it in items or []:
        k, sep, v = it.partition("=")
        if not sep or not k:
            ui.die(f"{_PROG}: {flag} takes NAME=VALUE, got '{it}'", 2)
        try:
            out[k] = conv(v)
        except ValueError:
            ui.die(f"{_PROG}: {flag} {k}: '{v}' is not a number", 2)
    return out


def _from_lines(src: str) -> list[list[str]]:
    try:
        text = sys.stdin.read() if src == "-" else open(paths.expand(src)).read()
    except OSError as e:
        ui.die(f"{_PROG} add --from: {e.strerror}: {src}", 2)
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        if line.strip() and not line.lstrip().startswith("#"):
            try:
                out.append(shlex.split(line))
            except ValueError as e:
                ui.die(f"{_PROG} add --from {src}: line {n}: {e}: {line.strip()[:80]}  (nothing was queued)", 2)
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

    A job's tag is its command cut to 40 characters unless it was named, so that is the column with something
    to give; the rest of a row is pp's own, and a row whose other columns alone are wider than the terminal
    keeps eight characters of tag and runs past the edge. Empty cells at the end of a row are left out, so no
    row ends in the padding of a column nothing filled."""
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


def public(j: dict) -> dict:
    """A job as JSON consumers (`show/status --json`) see it: every stored field, with the job's environment
    masked.

    Fields others rely on: id (int), state, queue, tag, project, cwd, cmd (argv), runner (argv, maybe []), needs,
    prio, owner {session, cwd} or None, timeout, retries, attempt, rc, reason, seconds, log,
    notes [{t, by, text}], reported [{t, by, session}], stop {by, t, grace, timeout, sent, kill9, denied, was}
    on a job asked to stop, and from `show` also jobcore.watchers [{pid, verb, session}].

    `env` and `shell_env` keep their keys and give every value as "***". A token or a secret is what the help
    says to pass with --env, and an agent reads `--json` to see how its jobs are getting on, so the values
    would land in a transcript and in whatever keeps one. The text `show` prints neither, and the record on
    disk, which is the job's own, holds what the job runs with.

    A stop's `timeout` is set on the one a worker wrote because the job's own time ran out, and on no other,
    so what a stop is never rests on the name of whoever asked. `sent` and `kill9` are when each signal went
    out, and `denied` is when pp was refused the job's process group. A stop with `denied` set is not in
    flight: nothing more goes out for it, and the job runs on until it ends on its own. `was` is who asked
    for the stop this one landed on, where a kill landed on a timeout's."""
    out = {k: v for k, v in j.items() if not k.startswith("_")}
    for k in ("env", "shell_env"):
        if isinstance(out.get(k), dict):
            out[k] = dict.fromkeys(out[k], "***")
    return out


def _ended_line(j: dict | None, jid: int) -> str:
    if j is None:
        return f"dropped {jid}"
    word = "done" if j["state"] == "done" else ui.c("FAILED", "red")
    why = f" ({j['reason']})" if j.get("reason") else ""
    return f"{word} {jid} rc={_rc(j)}{why} {ui.human_duration(j.get('seconds'))}  {_text(j.get('tag'))}"


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

def cmd_add(a, cmd: list[str], q: str, prepare_job) -> int:
    cmds = _from_lines(a.from_) if a.from_ else [cmd]
    if not cmds or not all(cmds):
        ui.die(f"{_PROG} add: no command. Put it after --  (pp {_PROG} add -- python x.py)", 2)
    cwd = paths.expand(a.cwd) if a.cwd else os.getcwd()
    if not os.path.isdir(cwd):
        ui.die(f"{_PROG} add: no directory {cwd}", 2)
    env = _kv(a.env, "--env", str)
    needs: dict = {"slots": 1}
    for k, v in (("gpu_gb", a.gpu_gb), ("cpu", a.cpu), ("mem_gb", a.mem_gb)):
        if v:
            needs[k] = policy.num(v)
    needs.update(_kv(a.need, "--need", policy.num))
    for k, v in needs.items():
        if not 0 < v < float("inf"):        # also nan: it compares false with everything, so it would never wait
            ui.die(f"{_PROG} add: {k}={v:g}: a need is a positive number", 2)
    why = Resources(q, needs, live=False).check(needs)
    if why:
        ui.die(f"{_PROG} add: {why}", 2)
    qc = _qcfg(q)
    if a.timeout is None:
        timeout = qc.get("timeout")
    else:       # none (or 0): no timeout, whatever the queue's default
        timeout = None if a.timeout.strip().lower() == "none" else _dur_arg(a.timeout, "--timeout")
    timeout = timeout or None
    retries = a.retries if a.retries is not None else qc.get("retries", 0)
    if retries < 0:
        ui.die(f"{_PROG} add: --retries must be >= 0", 2)
    retry_delay = _dur_arg(a.retry_delay, "--retry-delay") if a.retry_delay is not None else qc.get("retry_delay", policy.RETRY_DELAY)
    retry_if = a.retry_if if a.retry_if is not None else qc.get("retry_if")
    if retry_if:
        try:
            re.compile(retry_if)
        except re.error as e:
            ui.die(f"{_PROG} add: --retry-if {retry_if!r}: {e}", 2)
    if a.tag and re.fullmatch(r"\d+(-\d+)?", a.tag):
        ui.die(f"{_PROG} add: --tag {a.tag} reads as a job id or a range, so it could not select its jobs; "
               "use a word", 2)
    owner = session.caller()
    on_fail = policy.hook_spec(a.on_fail if a.on_fail is not None else qc.get("on_fail"))
    on_done = policy.hook_spec(a.on_done if a.on_done is not None else qc.get("on_done"))
    shell_env = {k: os.environ[k] for k in CAPTURED_ENV if k in os.environ}
    state = "held" if a.hold else "queued"
    project = project_name(cwd)
    records = []
    for c in cmds:
        job = {"queue": q, "tag": a.tag or default_tag(c), "project": project, "cwd": cwd, "cmd": c, "runner": [],
               "env": dict(env), "shell_env": shell_env, "needs": needs, "prio": a.prio, "owner": owner,
               "timeout": timeout, "retries": retries, "retry_delay": retry_delay, "retry_if": retry_if or None,
               "attempt": 1, "on_fail": on_fail, "on_done": on_done}
        if prepare_job:
            prepare_job(job, a)
        records.append(job)
    ids = jobcore.add_jobs(records, state)      # n records, n ids, one scope
    for jid, job in zip(ids, records):
        c = job["cmd"]
        print(jid)
        extra = " ".join(x for x in (f"timeout {ui.human_duration(timeout)}" if timeout else "",
                                      f"retries {retries}" if retries else "",
                                      f"on-fail {on_fail}" if on_fail else "") if x)
        via = f"  (via {_text(os.path.basename(job['runner'][0]))})" if job["runner"] else ""
        print(f"pp {_PROG}: {state} {jid} ({q}) [{_text(project)}] p{a.prio} {_needs_str(needs)} {extra}  "
              f"{_text(shlex.join(c), 100)}{via}", file=sys.stderr)
    if not a.hold and not a.no_start:
        _worker_up(q, wake=True)
    if owner:     # the pull side: an agent hears the outcome by waiting on it in the background
        some = " ".join(map(str, ids))
        print(f"pp {_PROG}: to hear how it ends, run `pp {_PROG} wait {some}` in the background (it exits with the "
              f"outcome and, on failure, the log tail), or `pp {_PROG} watch {some}` for a line per event", file=sys.stderr)
    return 0


def _scope_list(scope: list[str] | None) -> list[str]:
    """The queues a verb works on: every one there is, which `queues` opens as it finds it, or the one it was
    given by name, which run_cli opened before any verb ran."""
    return jobcore.queues() if scope is None else scope


def cmd_status(a, scope: list[str] | None) -> int:
    """The state of every queue in scope, or of the one named with -q.

    `--json` is what an agent reads, and this is its shape, stated once:

        queues          [name], the queues this status covers
        workers         {queue: pid or null}, the pid of each queue's worker; null where none is up and while
                        one that holds the claim has not recorded itself yet
        starting        [queue] whose worker holds the claim and has not recorded itself yet, which is the
                        first moment of a worker: one is coming up, and it launches the queue's jobs
        waiting         {queue: {id, tag, why, t} or null}, what the head of each queue waits for
        running, queued, held    [job], each one as `public()` gives it
        recent          [job], the jobs that ended most recently, at most -n of them
        pools           {queue: [pool]}, each queue's own pools
        shared          [pool], the pools every queue draws from; a pool is {name, max, used, probe, reading}
        cards           [{index, name, total_gb, used_gb, free_gb, util}], the GPUs nvidia-smi reports
        config_errors   [message]; while there is one, workers launch nothing

    A field change goes in CHANGELOG.md (CONTRIBUTING.md, "Adding a command").
    """
    cards = _cards()            # before the listing: nvidia-smi may take seconds, and a reader holds the store
    now = time.time()
    # one listing: the counts, the rows and what the shared pools hold are the same jobs, so no job is shown
    # twice or in two states. Every queue is listed, since a shared pool counts every queue's running jobs.
    all_jobs = jobcore.jobs()
    # what each queue's worker is, through the one construct, so the hints that nothing launches or records
    # this queue's jobs come back from the one answer it gave
    workers = {q: jobcore.worker_now(q) for q in _scope_list(scope)}
    # the queues those workers are of, so this view comes of one listing of the store: a queue another process
    # creates between two listings would otherwise be one this has no worker for
    qs = [q for q in workers
          if scope is not None or any(j["queue"] == q for j in all_jobs) or workers[q].how != jobcore.DOWN]
    if a.here or a.project:
        want = a.project or project_name(os.getcwd())
        keep = lambda js: [j for j in js if j.get("project") == want]   # noqa: E731
    else:
        keep = lambda js: js   # noqa: E731
    running_all = [j for j in all_jobs if j["state"] == "running"]
    per: dict = {}
    bad: list[str] = []         # a broken config stops launches, not this view of what is running

    def usage(q, sc):
        try:
            return Resources(q, live=False, empty_ok=False).usage(running_all, sc)
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
        ui.print_json({"cards": cards, "queues": qs, "workers": {q: per[q]["wpid"] for q in qs},
                       "starting": [q for q in qs if per[q]["how"] == jobcore.STARTING],
                       "pools": {q: per[q]["pools"] for q in qs}, "shared": shared,
                       "waiting": {q: per[q]["waiting"] for q in qs},
                       **{st: flat(st) for st in ("running", "queued", "held")},
                       "recent": [public(j) for j in recent[:a.n]], "config_errors": bad})
        return 1 if bad else 0
    width = shutil.get_terminal_size((120, 20)).columns
    gpu = "  ".join(f"GPU{c['index']} {c['util']}% {c['used_gb']:.1f}/{c['total_gb']:.0f}G" for c in cards) if cards else (
        "GPU n/a" + (f" ({CARDS_WHY})" if CARDS_WHY else ""))
    print(f"{gpu}   {datetime.now():%Y-%m-%d %H:%M:%S}")
    pool_strs = [_pool_str(p, now) for p in shared if p["used"] or p["probe"] or p["name"] not in SHARED_DEFAULTS]
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
    if (a.here or a.project) and not any(per[q]["data"][st] for q in qs for st in jobstore.STATES):
        print(f"no jobs of project '{want}'")
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
                row = [j["id"], _text(j.get("tag")), f"[{_text(j.get('project'))}]",
                       ui.human_duration(now - (j.get("started") or now)),
                       _needs_str(j.get("needs")) + (f" on GPU{j['gpu']}" if j.get("gpu") is not None else "")]
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
                                             ui.c("waiting: " + why, "yellow") if why else "", _latest_note(j)) if x)
                row = [j["id"], f"p{j.get('prio')}", _text(j.get("tag")), f"[{_text(j.get('project'))}]",
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
            rows = [[j["id"], f"p{j.get('prio')}", _text(j.get("tag")), f"[{_text(j.get('project'))}]",
                     _latest_note(j)] for j in data["held"]]
            print("\n".join(_section(rows, 2, width)))
        if s["recent"]:
            print(ui.c("recent", "bold"))
            rows = [[j["id"], "done" if j["state"] == "done" else ui.c("FAILED", "red"), f"rc={_rc(j)}",
                     ui.human_duration(j.get("seconds")), _text(j.get("tag")), f"[{_text(j.get('project'))}]",
                     ui.human_age(j.get("ended"), now) + (f"  {j['reason']}" if j.get("reason") else "")
                     + (f"  {_latest_note(j)}" if j.get("notes") else "")] for j in s["recent"][:a.n]]
            print("\n".join(_section(rows, 4, width)))
    return 1 if bad else 0


def _configured(q: str) -> bool:
    try:
        return q in config.load("queue")
    except config.ConfigError:
        return False


def _pool_str(p: dict, now: float) -> str:
    out = p["name"] + (f" {p['used']:g}/{p['max']:g}" if p["max"] is not None
                       else (f" {p['used']:g} in use" if p["used"] else ""))
    if p["probe"]:
        r = p["reading"]
        out += (" (probe: not run yet)" if not r else f" (probe: {r['value']:g} free, {ui.human_age(r['t'], now)})"
                if r.get("value") is not None else f" (probe failed {ui.human_age(r['t'], now)}: {r.get('error')})")
    return out


def cmd_show(a) -> int:
    j = _one(a.id, "show", a.scope)
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
    rows = [["tag", _text(j.get("tag"))], ["project", _text(j.get("project"))], ["cwd", _text(j.get("cwd"))],
            ["command", _text(shlex.join(list(j.get("runner") or []) + list(j.get("cmd") or [])))],
            ["needs", _needs_str(j.get("needs")) or "slots 1"], ["prio", j.get("prio")],
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
                     + (", SIGTERM sent" if st.get("sent") else ", nothing signalled")
                     + (", SIGKILL sent" if st.get("kill9") else "")
                     # a denied stop is not in flight: say so, or it reads as one still being carried out
                     + (f"; pp is not allowed to signal its group {j.get('pgid')}, so nothing more goes out"
                        if st.get("denied") else "")])
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
    j = _one(a.id, "note", a.scope)
    text = " ".join(a.text).strip()
    if not text:
        ui.die(f"{_PROG} note: no text", 2)
    add_note(j["id"], text, by=a.by)
    print(f"noted on {j['id']}")
    return 0


def cmd_log(a) -> int:
    j = _one(a.id, "log", a.scope)
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
        f.seek(_tail_start(f, a.lines))
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

    A job named as queued or held is refused, since it has no process: `drop` removes it. A job this kill read
    as running may be back in its queue for a retry by the time the stop is written. It has no process then,
    so the transaction ends it there."""
    rc = 0
    jobs = _resolve(a.ids, a.scope)
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
        out = jobcore.ask_stop(jid, _by(), float(a.grace))
        if out["did"] == "ended":
            print(f"job {jid} already {out['was']}")
            continue
        if out["did"] == "killed":
            # it went back to its queue for a retry: no process exists, so that is where it ended
            print(f"killed {jid} (it was {out['was']}, with no process to stop)  {_text(j.get('tag'))}")
            continue
        asked.append((j, out["t"]))
    for q in sorted({j["queue"] for j, _t in asked}):
        _worker_up(q, wake=True)     # the queue's worker is what carries these stops out
    for j, t in asked:
        rc = max(rc, _await_stop(j, float(a.grace), t))
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
    elif cur.get("rc") is not None:
        print(f"job {jid} ended on its own as it was being killed: {_ended_line(cur, jid)}")
    else:
        # the grace on the record, which is the shorter of every ask (`jobpolicy.merge_stop`) and the one the SIGKILL
        # went out after, not the one this kill asked for
        how = (f"SIGKILL after {float(stop.get('grace') or 0):g}s" if stop.get("kill9")
               else "stopped on SIGTERM")
        print(f"killed {jid} ({how})  {_text(cur.get('tag'))}")
    return 0


def _transition(a, src: tuple, dst: str | None, verb: str, done: str) -> int:
    rc, skipped, started = 0, [], set()
    jobs = _resolve(a.ids, a.scope)
    if verb == "drop":
        go = _gate(a, jobs, "drop", src)
        if go is not None:
            return go
    # one transaction per job, each told the states it may act on, so none of them acts on a copy that has
    # gone stale; the record it moved, or the state the store holds it in, is what this says
    moved, left = jobcore.forget(jobs, src) if dst is None else jobcore.set_state(jobs, src, dst)
    was = {cur["id"]: cur for cur in left}
    for j in jobs:
        if j["id"] in was:
            cur = was[j["id"]]
            if j["_bulk"]:
                if verb != "drop":          # drop's plan listed them already
                    skipped.append(cur)
                continue
            hint = f". `pp {_PROG} kill {j['id']}` stops it" if cur["state"] == "running" else ""
            ui.warn(f"{_PROG}: job {j['id']} is {cur['state']}; {verb} works on {'/'.join(src)} jobs{hint}")
            rc = 2
            continue
        print(f"{done} {j['id']}  {_text(j.get('tag'))}")
        if dst == "queued":
            started.add(j["queue"])
    _left(skipped)
    if not getattr(a, "no_start", False):          # a job added with --hold started no worker; one per queue
        for q in sorted(started):
            _worker_up(q, wake=True)
    return rc


def _reprio(a) -> int:
    rc, skipped = 0, []
    for j in _resolve(a.ids, a.scope):
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


CARRY = ("queue", "tag", "project", "cwd", "cmd", "runner", "env", "shell_env", "needs", "prio", "owner", "timeout",
         "retries", "retry_delay", "retry_if", "on_fail", "on_done")


def cmd_retry(a) -> int:
    started, rc, skipped = set(), 0, []
    for j in _resolve(a.ids, a.scope):
        if j["state"] not in jobstore.ENDED and j["_bulk"]:
            skipped.append(j)
            continue
        if j["state"] not in jobstore.ENDED:
            ui.warn(f"{_PROG}: job {j['id']} is {j['state']}; retry re-queues ended jobs")
            rc = 2
            continue
        new = {k: j[k] for k in CARRY if k in j}
        new.update(retry_of=j["id"], attempt=1)
        if a.prio is not None:
            new["prio"] = a.prio
        jid = jobcore.add_jobs([new], "queued")[0]
        print(jid)
        print(f"pp {_PROG}: retry of {j['id']} queued as {jid} ({j['queue']})", file=sys.stderr)
        started.add(j["queue"])
    _left(skipped)
    if not a.no_start:
        for q in sorted(started):
            _worker_up(q, wake=True)
    return rc


def cmd_clean(a, scope: list[str] | None) -> int:
    cutoff = time.time() - _dur_arg(a.older, "--older", "d") if a.older is not None else None
    victims = [j for j in jobcore.jobs(jobstore.ENDED, scope) if cutoff is None or (j.get("ended") or 0) < cutoff]
    if not victims:
        print("nothing to clean")
        return 0
    print(f"{len(victims)} ended job record(s)" + (f" older than {a.older}" if a.older else "")
          + ", with their logs")
    for q in sorted({j["queue"] for j in victims}):
        print(f"  {q}: {_ranges(sorted(j['id'] for j in victims if j['queue'] == q))}")
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
    tag = _text(j.get("tag"))
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
    """wait: block until jobs end, then print their outcomes. watch (stream): a line per event as it happens.
    Both hold a watcher lease while they run (`show` lists it) and stamp each outcome they print on the job."""
    verb = "watch" if stream else "wait"
    t0 = time.time()

    def say(*lines):
        for line in lines:
            print(line, flush=True)

    def timed_out():
        return a.timeout is not None and time.time() - t0 >= a.timeout

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
                time.sleep(a.poll)
            for j in sorted(ended, key=lambda j: j["id"]):
                if j["id"] not in told:
                    tell(j, j["id"])
        footer()
        return 1 if any(j["state"] == "failed" for j in ended) else 0
    if a.ids:
        targets, need_all = [j["id"] for j in _resolve(a.ids, scope)], True
    elif a.mine:
        me = session.caller()
        if not me:
            ui.die(f"{_PROG} {verb} --mine: not inside an agent session (no CLAUDE_CODE_SESSION_ID)", 2)

        def mine(j):
            return isinstance(j.get("owner"), dict) and j["owner"].get("session") == me["session"]

        def unheard(j):     # ended before this session waited on it: a quick failure must not be missed
            return not any(isinstance(r, dict) and r.get("session") == me["session"] for r in j.get("reported") or [])

        # one listing of both halves: a job is listed once, in one state, so a target is named once
        here = jobcore.jobs(queue=scope)
        targets = [j["id"] for j in here if j["state"] in jobstore.LIVE and mine(j)] \
            + [j["id"] for j in here if j["state"] in jobstore.ENDED and mine(j) and unheard(j)]
        need_all = True
        if not targets:
            say("nothing of this session's is queued, held or running, and every outcome was reported to it")
            return 0
    else:
        targets, need_all = [j["id"] for j in jobcore.jobs(("queued", "running"), scope)], False
        if not targets:
            say("nothing queued or running")
            return 0
    held = [j["id"] for j in (jobcore.find(t) for t in targets) if j and j["state"] == "held"]
    if held:
        print(f"[{_PROG} {verb}] note: {', '.join(map(str, held))} held, so this waits forever. "
              f"`pp {_PROG} release` them", file=sys.stderr)
    with jobcore.watching(targets, None, verb):
        prev: dict = {}
        told: set = set()
        asked = set()           # as above: a loop says what holds a queue's claim on its first pass only
        blamed: set = set()     # the queues this loop has said no worker came up for, which it says once
        while True:
            cur = {t: jobcore.find(t) for t in targets}
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
            if ended and (len(ended) == len(targets) or not need_all):
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
            time.sleep(a.poll)


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

def _parser(prog: str, doc: str, add_args=None):
    ap = ui.parser(prog, doc)
    sub = ap.add_subparsers(dest="verb", metavar="VERB")

    def sp(name, **kw):
        p = sub.add_parser(name, **kw)
        p.add_argument("-q", "--queue", help="the named queue (see `pp help queue`)")
        return p

    p = sp("add", help="queue a command (after --)")
    p.add_argument("--tag", help="a name to select it by, alone or with the other jobs that share it "
                                 "(default: the command, cut to 40 characters)")
    p.add_argument("--prio", type=int, default=10, help="lower runs first (default 10)")
    p.add_argument("--gpu-gb", type=float, help="GPU memory to reserve on one card (the job's peak use, plus a margin)")
    p.add_argument("--cpu", type=float, help="CPUs to reserve")
    p.add_argument("--mem-gb", type=float, help="RAM to reserve, in GB")
    p.add_argument("--need", action="append", metavar="NAME=N", help="a counter from [resources] (every queue's) or [queue.Q.resources] (one queue's own)")
    p.add_argument("--env", action="append", metavar="K=V",
                   help="set in the job's environment; wins over the project's [queue] env")
    p.add_argument("--cwd", help="run there (default: here); also picks the project")
    p.add_argument("--timeout", metavar="DUR", help="kill it after DUR (90s, 30m, 2h, 1d); none: no timeout")
    p.add_argument("--retries", type=int, help="automatic retries of a failure (default 0)")
    p.add_argument("--retry-delay", metavar="DUR", help="delay before the first retry, doubling after (default 30s)")
    p.add_argument("--retry-if", metavar="REGEX", help="retry only failures whose reason or log tail matches")
    p.add_argument("--on-fail", metavar="CMD", help="shell command run on the final failure; none: no hook")
    p.add_argument("--on-done", metavar="CMD", help="shell command run on success; none: no hook")
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
    p = sp("show", help="every field of a job, and its log tail")
    p.add_argument("id")
    p.add_argument("--json", action="store_true")
    p = sp("note", help="attach a note to a job (any state)")
    p.add_argument("id")
    p.add_argument("text", nargs="+")
    p.add_argument("--by", metavar="NAME", help="the author shown (default: the calling agent session, else user)")
    p = sp("log", help="tail a job's log")
    p.add_argument("id")
    p.add_argument("-f", "--follow", action="store_true")
    p.add_argument("-n", "--lines", type=int, default=40)
    p = sp("kill", help="stop running jobs: the whole process group is signalled; a job the signal stopped "
                        "fires no hook")
    p.add_argument("ids", nargs="+")
    p.add_argument("--grace", type=float, default=10, help="seconds between SIGTERM and SIGKILL")
    p.add_argument("-n", "--dry-run", action="store_true", help="only show which jobs it would stop")
    p.add_argument("-y", "--yes", action="store_true", help="do not ask about a range or a shared tag")
    for name, hlp in (("drop", "remove queued/held jobs"), ("hold", "park queued jobs"), ("release", "un-park held jobs")):
        p = sp(name, help=hlp)
        p.add_argument("ids", nargs="+")
        if name == "drop":
            p.add_argument("-n", "--dry-run", action="store_true", help="only show which jobs it would remove")
            p.add_argument("-y", "--yes", action="store_true", help="do not ask about a range or a shared tag")
        if name == "release":
            p.add_argument("--no-start", action="store_true", help="do not start a worker")
    p = sp("reprio", help="change a queued job's priority")
    p.add_argument("ids", nargs="+", metavar="ID")
    p.add_argument("prio", type=int)
    p = sp("retry", help="re-queue ended jobs as new jobs")
    p.add_argument("ids", nargs="+")
    p.add_argument("--prio", type=int)
    p.add_argument("--no-start", action="store_true")
    p = sp("clean", help="remove ended job records, with the logs that belong to them")
    p.add_argument("--older", metavar="AGE", help="only those ended before AGE ago (30m, 12h, 7d)")
    p.add_argument("-n", "--dry-run", action="store_true")
    p.add_argument("-y", "--yes", action="store_true")
    for name, hlp in (("wait", "block until jobs end, then print the outcomes (exit 0 ok, 1 failed or dropped, 3 timeout)"),
                      ("watch", "a line per event (start, retry, outcome) as it happens; exits like wait")):
        p = sp(name, help=hlp)
        p.add_argument("ids", nargs="*")
        g = p.add_mutually_exclusive_group()
        g.add_argument("--idle", action="store_true", help="until nothing is queued or running")
        g.add_argument("--mine", action="store_true", help="every job this agent session queued that is live, or ended without a wait/watch of this session reporting it")
        p.add_argument("--timeout", metavar="DUR", type=lambda v: _dur_arg(v, "--timeout"),
                       help="give up after DUR (90s, 30m, 2h, 1d; a bare number is seconds) and exit 3")
        p.add_argument("--poll", type=float, default=2.0, help="seconds between looks (default 2)")
        p.add_argument("--tail", type=int, default=15, help="log lines shown with a failure (default 15)")
        p.add_argument("--no-start", action="store_true", help="do not start a worker for queued jobs")
    p = sp("work", help="run a queue's worker in the foreground")
    p.add_argument("--backfill", action="store_true", default=None, help="start later jobs that fit while the head waits")
    p.add_argument("--poll", type=float)
    p = sp("worker", help="start | stop | status of the background workers")
    p.add_argument("action", choices=("start", "stop", "status"))
    p.add_argument("--json", action="store_true", help="status as JSON")
    return ap


def run_cli(argv: list[str], prog: str, default_queue: str, doc: str, prepare_job=None, add_args=None) -> int:
    """The queue CLI for `pp <prog>`. default_queue receives `add` and `work` without -q; status, wait --idle, clean
    and worker status cover every queue. prepare_job(job, args) amends each job at add time; add_args(parser) adds
    options to `add`.

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
    a = _parser(prog, doc, add_args).parse_args(argv)
    q = getattr(a, "queue", None) or q_top
    if q is not None and (not jobstore.NAME_RE.fullmatch(q) or q in jobstore.RESERVED):
        ui.die(f"{prog}: '{q}' is not a queue name (lowercase letters, digits, - and _; not {' or '.join(jobstore.RESERVED)})", 2)
    scope = a.scope = [q] if q else None
    if q is not None:
        jobcore.open_queue(q)       # a queue named with -q is opened once here, whatever the verb does with it
    if a.verb == "add" and cmd and a.cmd:
        ui.die(f"{prog} add: put the whole command after --", 2)
    # one row per verb: what it is called with is in the row, and nothing else is keyed on a verb's name. The
    # call is held back until the verb is chosen, so a verb's own options are read on that verb's path only
    verbs = {"add": lambda: cmd_add(a, cmd or a.cmd, q or default_queue, prepare_job),
             "status": lambda: cmd_status(a, scope), "ls": lambda: cmd_status(a, scope),
             "show": lambda: cmd_show(a), "note": lambda: cmd_note(a), "log": lambda: cmd_log(a),
             "kill": lambda: cmd_kill(a), "reprio": lambda: _reprio(a), "retry": lambda: cmd_retry(a),
             "drop": lambda: _transition(a, ("queued", "held"), None, "drop", "dropped"),
             "hold": lambda: _transition(a, ("queued",), "held", "hold", "held"),
             "release": lambda: _transition(a, ("held",), "queued", "release", "released"),
             "clean": lambda: cmd_clean(a, scope),
             "wait": lambda: cmd_wait(a, scope), "watch": lambda: cmd_wait(a, scope, stream=True),
             "work": lambda: cmd_work(q or default_queue, a.backfill, a.poll),
             "worker": lambda: cmd_worker(a, q, default_queue, scope)}
    return verbs[a.verb]() if a.verb in verbs else 2
