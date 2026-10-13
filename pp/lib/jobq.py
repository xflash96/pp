"""The job-queue engine behind `pp queue`, and its CLI (run_cli).

Named queues, one directory each: ~/.local/state/pp/queue/<name>/
    jobs/<id>.json  one file per job, holding every field, its `state` among them (queued, held, running, done,
                    failed): a change of state replaces that one file, and nothing else moves
    jobs/<id>.run   the claim one run of that job holds, from its own wrapper, for exactly as long as it runs
    rc/<id>.<attempt>.<token>   the exit code, written by the job's own `sh` under the name its record names,
                    so ANY worker can tell how it ended, and no attempt can read another's code
    logs/<id>.log  hooks/<id>-<event>.log   worker.lock (the worker's claim, and its record)  worker.log
    FORMAT          the number this layout is, written by the first writer that reaches the queue: a queue
                    whose jobs another pp wrote is refused where it is opened (open_queue), never read
and, shared by every queue, ~/.local/state/pp/queue/{counter,.lock,probes/,watchers/}: job ids are global (an id
names one job machine-wide), and the shared resource pools ([resources]) count every queue's running jobs.

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

One function says what is to be done about a running job: `decide(job, facts)`, from the job's record and the
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
import secrets
import select
import shlex
import shutil
import signal
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

from pp.lib import config, lock, paths, proc, session, ui

STATES = ("queued", "held", "running", "done", "failed")
LIVE = ("queued", "held", "running")
ENDED = ("done", "failed")
CAPTURED_ENV = ("PATH", "VIRTUAL_ENV", "CONDA_PREFIX", "CONDA_DEFAULT_ENV")
# What a job runs as. Its stdin is the job's run claim, which this `sh` holds for exactly as long as the attempt
# lives; it writes its own pid in there first, so the claim names it even if the worker that started it died
# before recording it. The command itself reads /dev/null, so neither it nor anything it leaves behind holds the
# claim: the attempt is over when this `sh` is over. The exit code goes to another name and is renamed into
# place, so whoever reads it finds a whole code or none.
WRAP = ('printf \'{"pid": %d}\' "$$" >&0; "$@" 0</dev/null; r=$?; '
        'echo "$r" > "$PP_QUEUE_RCFILE.part" && mv "$PP_QUEUE_RCFILE.part" "$PP_QUEUE_RCFILE"')
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
LAUNCH_BANNER = re.compile(r"\[pp queue\] \S+ \S+ job \d+ \(")     # the line _spawn writes before each attempt
NAME_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")
RECORD = re.compile(r"\d+\.json")     # what a job record is called: jobs/<id>.json, and nothing else is one
RESERVED = ("probes", "watchers")     # directories beside the queues
FORMAT = 1                  # what a queue's FORMAT file holds: the layout of everything under queue/<name>/
TIMEOUT_GRACE = 10.0        # seconds between a timeout's SIGTERM and its SIGKILL
WAKE = signal.SIGUSR1       # what a verb sends a worker to end its wait between ticks, so a stop is served now
STOP_PERIOD = 0.1           # how long a woken worker waits between ticks while a stop of its queue is in flight
KILL_POLL = 0.1             # how often `kill` looks for the outcome that is recorded
# how long `kill` waits for that outcome, past the grace it asked for. Every pass of that wait wakes the
# worker that is up, so its next tick is that pass. What this bounds is the job's own exit time, and not the
# queue's poll period, whatever that is set to. Where no worker is up, a pass starts a round of them and
# waits for each start in it to say it is ready, so such a pass can carry the wait past this bound by that
# round's ready waits; `kill` says how long it really waited.
KILL_WAIT = 120.0
# how often one process starts a worker again for a queue whose last round of starts brought none up
START_EVERY = 5.0
RETRY_DELAY = 30.0
RETRY_DELAY_CAP = 3600.0
STOP_WAIT = 300.0           # how long `worker stop` waits for a worker inside a slow tick
CLEAN_BATCH = 100           # job records `clean` removes in one transaction, so a reader never waits long

_PROG = "queue"             # the command running the CLI: names it in messages and starts its workers


# ---- store ------------------------------------------------------------------------------------------
#
# One file per job: jobs/<id>.json, with every field in it, `state` among them. A change of state replaces that
# one file, so a reader sees the job as it was before the change or as it is after it, never in two states and
# never in none. Reading and writing a record goes through txn() and listing(), or through find() for one.

def _top(*sub: str, create: bool = True) -> str:
    return paths.state_dir("queue", *sub, create=create)


def _qroot(q: str, *sub: str, create: bool = True) -> str:
    return paths.state_dir("queue", q, *sub, create=create)


def queues() -> list[str]:
    """Every queue that exists on disk, each one opened through open_queue."""
    top = _top(create=False)
    try:
        names = os.listdir(top)
    except OSError:
        return []
    found = sorted(n for n in names if NAME_RE.fullmatch(n) and os.path.isdir(os.path.join(top, n, "jobs")))
    for n in found:
        open_queue(n)
    return found


def _format_file(q: str, create: bool = False) -> str:
    return os.path.join(_qroot(q, create=create), "FORMAT")


def _format_of(q: str) -> int | None:
    """The number in queue q's FORMAT file, or None when it holds none."""
    try:
        with open(_format_file(q)) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _write_format(q: str) -> None:
    """Say what this queue's store looks like, where it does not say yet. Every writer that reaches a queue
    does it (writing_queue), so a queue that exists names the format its records are in."""
    path = _format_file(q, create=True)
    if not os.path.exists(path):
        paths.write_json(path, FORMAT)


def open_queue(q: str) -> str:
    """Queue q's directory, with the format of its store checked. The one place a queue is opened, so the one
    place that refuses a queue this pp cannot read.

    Every path that opens a queue comes through here: queues() for each queue it finds, the CLI for a queue it
    is given by name, and work() and `worker start` for the queue they fall back to. A queue whose jobs another
    pp wrote is refused rather than read, since this pp would report records it cannot see as gone, and launch
    jobs beside ones that are still running."""
    qdir = _qroot(q, create=False)
    got = _format_of(q)
    if got == FORMAT:
        return qdir
    # a job is a record, and a record is jobs/<id>.json wherever under jobs/ it lies, so the walk goes down:
    # a store laid out another way, with a directory per state, is refused like any other foreign format. A
    # run's claim and the name a write stopped halfway leaves are not records, so a queue whose records have
    # all been cleaned holds nothing this pp cannot read.
    has_jobs = any(RECORD.fullmatch(n) for _d, _subs, names in os.walk(os.path.join(qdir, "jobs"))
                   for n in names)
    if got is None and not has_jobs:
        return qdir             # no record yet: a writer says what format this queue is in (writing_queue)
    says = f"its FORMAT file says {got}" if got is not None else "it has no FORMAT file"
    ui.die(f"{_PROG}: queue '{q}' holds jobs this pp cannot read: {says}. This pp reads format {FORMAT}. "
           f"Drain that queue with the pp that wrote it, or remove {qdir}.", 2)


def writing_queue(q: str) -> str:
    """Queue q's directory, opened and ready to be written in: it exists, and it says what format its store is
    in. Every writer that may be the one creating a queue's directory comes through here, so a queue that
    exists says its format whether or not it holds a job."""
    qdir = open_queue(q)
    _write_format(q)
    return qdir


def _store_lock() -> str:
    return os.path.join(_top(), ".lock")


def _counter_file() -> str:
    return os.path.join(_top(), "counter")


def _jpath(q: str, jid: int, create: bool = False) -> str:
    """Where job jid's record is in queue q. create: make the queue's directories, which only a writer does, so
    a read of a job that is not there leaves nothing behind."""
    return os.path.join(_qroot(q, "jobs", create=create), f"{jid}.json")


def _job_ids(q: str) -> list[int]:
    """The ids in queue q, from one pass over its directory."""
    try:
        names = os.listdir(_qroot(q, "jobs", create=False))
    except OSError:
        return []
    return sorted(int(n[:-5]) for n in names if RECORD.fullmatch(n))


def _read_job(q: str, jid: int) -> dict | None:
    """Queue q's job jid, or None when it holds no readable record. Every write replaces the whole file, so a
    read sees one whole record. The queue and the id come from the path, which names them."""
    try:
        with open(_jpath(q, jid)) as f:
            j = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(j, dict) or j.get("state") not in STATES:
        return None
    j.update(id=jid, queue=q)
    return j


def _job_json(job: dict) -> dict:
    return {k: v for k, v in job.items() if not k.startswith("_")}


def _write_job(job: dict) -> None:
    paths.write_json(_jpath(job["queue"], job["id"], create=True), _job_json(job))


def _stage_job(job: dict, path: str) -> None:
    """The whole record, written under a name no other writer picks, ready to be linked to the name that takes
    its id."""
    paths.write_json(path, _job_json(job))


def _remove_job(job: dict) -> None:
    os.remove(_jpath(job["queue"], job["id"]))


def _counter() -> int:
    """Where to start looking for a free job id. Never the authority: a lost counter costs a few looks, never
    an id handed out twice."""
    try:
        with open(_counter_file()) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return 0


class Txn:
    """The store, for the length of one scope: the only way pp reads or writes a job record, apart from find().

    txn() holds the store lock, so nothing else reads or writes a record while the scope runs; listing() holds
    it shared, for a pass that writes nothing. Either way the scope hands out the records it read under the
    lock and takes back only those, so a copy read outside it can never be written over what another actor
    recorded meanwhile.

        with txn() as s:
            j = s.job(jid, must_be=("running",))        # that job in one of those states, else None
            if j:
                j.update(rc=0, state="done")
                s.save(j)                               # one replace of one file

    s.jobs(state=, queue=) lists records, s.add(record, state) stores a new one, s.remove(j) drops one. A verb
    that reports the state it found asks without must_be and reads j["state"].
    """

    def __init__(self, write: bool):
        self.write = write
        self._mine: dict[int, dict] = {}        # id -> the one record object this scope hands out for it
        self._ids: dict[str, list[int]] = {}    # queue -> its ids, for the queues this scope has read

    # -- reading

    def job(self, jid: int, must_be: tuple = ()) -> dict | None:
        """The job with this id, in whichever queue it is. None when there is none, and when must_be names the
        states it may be in and it is in another."""
        j = self._mine.get(jid)
        if j is None:
            for q in queues():
                j = self._keep(_read_job(q, jid))
                if j is not None:
                    break
        return None if j is None or (must_be and j["state"] not in must_be) else j

    def jobs(self, state=(), queue=None) -> list[dict]:
        """The jobs in these states (every state when state is empty), in run order (prio, id). One pass over
        each queue's directory, so a job is listed once, whole, and in one state."""
        want = (state,) if isinstance(state, str) else tuple(state)
        qs = queues() if queue is None else [queue] if isinstance(queue, str) else list(queue)
        out = [j for q in qs for j in (self._mine.get(i) for i in self._pass(q))
               if j is not None and (not want or j["state"] in want)]
        out.sort(key=lambda j: (j.get("prio", 10), j["id"]))
        return out

    # -- writing

    def save(self, job: dict) -> None:
        """Write the record back: one replace of its file."""
        self._writing(job)
        _write_job(job)

    def remove(self, job: dict) -> None:
        self._writing(job)
        _remove_job(job)
        self._mine.pop(job["id"], None)

    def add(self, job: dict, state: str) -> dict:
        """Store a new job of job["queue"], and hand it back with its id, state, log and added filled in.

        The whole record is written under a name of this writer's own and then linked to jobs/<id>.json, which
        fails if that name is there. So the id and the record that holds it land together: two adds at once
        never choose the same id, and an add stopped in between leaves no job file, never an empty one.

        An id names one job machine-wide, so one any queue holds is taken; `counter` says where to start
        looking and is never the authority, so a lost one costs a few looks, never an id handed out twice."""
        self._writing(job, new=True)
        q = job["queue"]
        writing_queue(q)        # the queue is opened, so a record never lands in one this pp cannot read
        others = [x for x in queues() if x != q]
        tmp = paths.staging(_qroot(q, "jobs"), "job")
        n = _counter()
        try:
            while True:
                n += 1
                if any(os.path.exists(_jpath(x, n)) for x in others):
                    continue
                job.update(id=n, state=state, log=os.path.join(_qroot(q, "logs"), f"{n}.log"), added=time.time())
                _stage_job(job, tmp)
                try:
                    os.link(tmp, _jpath(q, n))
                except FileExistsError:
                    continue
                break
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        paths.write_json(_counter_file(), n)
        self._mine[n] = job
        if q in self._ids:
            self._ids[q].append(n)
        return job

    # -- what the scope read, and what it may write

    def _keep(self, j: dict | None) -> dict | None:
        """One record object per id for the scope's lifetime: the one save() takes back."""
        if j is None:
            return None
        return self._mine.setdefault(j["id"], j)

    def _pass(self, q: str) -> list[int]:
        """Queue q's ids, read once per scope."""
        if q not in self._ids:
            self._ids[q] = [jid for jid in _job_ids(q)
                            if jid in self._mine or self._keep(_read_job(q, jid)) is not None]
        return self._ids[q]

    def _writing(self, job: dict, new: bool = False) -> None:
        if not self.write:
            raise RuntimeError("the store is open for reading: a listing writes nothing")
        if not new and self._mine.get(job.get("id")) is not job:
            raise RuntimeError(f"job {job.get('id')}: this record did not come from this scope, so what it holds "
                               "may be older than the store; read it again with s.job()")


@contextmanager
def txn():
    """The store to this scope alone: read records, and change them. Every write of a job file happens here.

    The scope holds the store lock (one for all the queues), so no other pp process reads or writes a record
    while it runs, and the signals `lock.deferring` holds back are delivered when it ends, so a change is
    never left half made. Keep it short: readers wait for it, so nothing that can block for long (a probe,
    nvidia-smi, starting a process) belongs inside one. `TestContracts` in tests/test_constructs.py reads the
    source and fails on a call to one of those inside a scope of this.

    One lock is taken inside it, and only this one: the claim on a job's run, which a worker takes before the
    record says the job is running (Worker.launch). A claim is taken without waiting, so the scope stays as
    short as the two calls it takes; the job itself starts after the scope ends."""
    with lock.locked(_store_lock(), defer=True):
        yield Txn(write=True)


@contextmanager
def listing():
    """The store for reading: one pass over each queue's directory, with nothing writing meanwhile.

    POSIX does not say what a directory read sees while entries in it are replaced, and a file system may
    report one entry twice or skip it, so a listing holds the store lock shared: a writer waits for it. Other
    pp processes list beside it; two threads of one pp take turns, since the lock helper keeps one open file
    per lock. A txn inside one is refused, since it would have to let the lock go to take it alone, and what it
    had read would then be older than the store."""
    with lock.locked(_store_lock(), shared=True):
        yield Txn(write=False)


def _jobs(state=(), queue=None) -> list[dict]:
    """One listing, for a caller that needs one list of records and nothing else from it."""
    with listing() as s:
        return s.jobs(state, queue)


def find(jid: int) -> dict | None:
    """The job with this id, in whichever queue it is, or None.

    Takes no lock: a job lives in one file, and every write of it is one replace, so one read sees one whole
    record. A caller that needs several records to agree with each other asks a listing for them together."""
    for q in queues():
        j = _read_job(q, jid)
        if j is not None:
            return j
    return None


def _run_claim(q: str, jid: int) -> str:
    """The file whose claim one run of job jid holds. Taking it is how a worker says it is starting that job,
    and the job's own wrapper holds it from then on, so a held claim means a run of this job is live."""
    return os.path.join(_qroot(q, "jobs"), f"{jid}.run")


def _new_rcfile(j: dict) -> str:
    """The file this attempt of job j writes its exit code to. One name per attempt, chosen as the attempt
    starts and kept on the record, so no attempt can read the code another wrote and nothing has to be removed
    before a launch."""
    return os.path.join(_qroot(j["queue"], "rc"), f"{j['id']}.{j.get('attempt', 1)}.{secrets.token_hex(4)}")


def _forget_job(s: Txn, job: dict) -> None:
    """In a transaction: take a job's record away, and with it every file that belongs to that job alone.

    The record goes first, so no reader finds a job whose files are gone. Then the exit code each attempt
    wrote, the job's log, the log of each hook that ran for it, and the claim its runs held. Only files under
    the queue's own directories, never one pp did not write. What `drop` and `clean` both take away, so
    neither leaves a file behind that nothing names any more: once the record is gone, nothing names them."""
    q, jid = job["queue"], job["id"]
    files = [p for p in [job.get("rcfile")] + [h.get("rcfile") for h in job.get("history") or []]
             if p and os.path.dirname(p) == _qroot(q, "rc", create=False)]
    if job.get("log") and os.path.dirname(job["log"]) == _qroot(q, "logs", create=False):
        files.append(job["log"])
    # a hook's log is named after the job and the event, so both names are this job's whether or not the
    # record got as far as naming them (a hook that could not start wrote its first line and no more)
    files += [os.path.join(_qroot(q, "hooks", create=False), f"{jid}-{e}.log") for e in ("failed", "done")]
    s.remove(job)
    for path in files:
        try:
            os.remove(path)
        except OSError:
            pass
    # its record is gone and its run is over, so nothing holds this claim and this id never takes that name again
    lock.Claim.forget(_run_claim(q, jid))


def ended_on_its_own(job: dict) -> tuple[int, float] | None:
    """(the exit code this attempt wrote, when it wrote it), or None when it wrote none. The one reader of an
    attempt's exit code.

    The job's own wrapper writes it, under the name the record names, after the command has returned and before
    the wrapper exits. So a code means the command ran to its end, whatever else was on its way, and its time
    is when the job really ended, however late a worker reaps it. A signal that ends the wrapper leaves no code.
    Both come from one open file, so the code and its time are the same attempt's."""
    path = job.get("rcfile")
    if not path:
        return None
    try:
        with open(path) as f:
            return int(f.read().strip()), os.fstat(f.fileno()).st_mtime
    except (OSError, ValueError):
        return None


def _finish(s: Txn, job: dict, state: str, rc: int | None, reason: str | None = None,
            ended: float | None = None, not_retried: str | None = None, hook: str | None = None):
    """In a transaction: the job is over, with this outcome. `state` is what it ended as, which `decide` says
    and nothing works out again.

    The outcome and the hook it owes are written together (`hook_due`), and that field is cleared once the
    hook has run (`Worker.fire`). So a worker that dies between the two leaves a record that says a hook is
    owed, and the next pass over the queue fires it: a hook fires once, and not never."""
    now = ended or time.time()
    job.update(ended=now, seconds=now - (job.get("started") or now), rc=rc, state=state)
    if reason:
        job["reason"] = reason
    if not_retried:
        job["not_retried"] = not_retried
    if hook:
        job["hook_due"] = hook
    s.save(job)


def _requeue(s: Txn, job: dict, a: Action) -> str:
    """In a transaction: the job goes back to its queue for the attempt the action names, with the same id.
    -> the line for its log, which the caller appends after the scope."""
    attempt, retries = int(job.get("attempt", 1)), int(job.get("retries") or 0)
    job.setdefault("history", []).append({"attempt": attempt, "rc": a.rc, "reason": a.reason,
                                          "started": job.get("started"), "ended": a.ended,
                                          "seconds": a.ended - (job.get("started") or a.ended),
                                          "rcfile": job.get("rcfile")})
    for k in ("pid", "pgid", "pstart", "boot", "started", "gpu", "stop", "rcfile"):
        job.pop(k, None)            # this attempt's, including the file it wrote its code to
    job.update(attempt=a.attempt, not_before=a.not_before, state="queued")
    s.save(job)
    return (f"[pp queue] {datetime.now():%Y-%m-%d %H:%M:%S} attempt {attempt}/{retries + 1} after {a.why}"
            f"; attempt {a.attempt} in {ui.human_duration(a.not_before - a.ended)}")


def _group_handle(j: dict) -> proc.Handle | None:
    """The job's process group, as the handle every signal to it goes through: the group id, the start time
    recorded with it, and the boot both were read in.

    None while the job has no process yet, for a record that holds no start time, and for a record from
    another boot, which names no process here. `Worker._signal_group` then falls back to the pid in the job's
    run claim, which is held by a live process and so is always this boot's."""
    if j.get("boot") != proc.boot_id():
        return None
    return proc.Handle.recorded(j.get("pgid"), j.get("pstart"), group=True)


def _by() -> str:
    """Who asked, as a record names them: the calling agent session, or the user."""
    who = session.caller()
    return who["session"] if who else "user"


def add_note(jid: int, text: str, by: str | None = None) -> dict | None:
    """Append a note to a job in any state; the updated job, or None when there is no such job."""
    if by is None:
        by = _by()
    with txn() as s:
        j = s.job(jid)
        if not j:
            return None
        j.setdefault("notes", []).append({"t": time.time(), "text": text, "by": by})
        s.save(j)
        return j


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


def _num(v) -> int | float:
    f = float(v)
    return int(f) if f.is_integer() else f


def _hook_spec(v) -> str | None:
    v = (v or "").strip() if isinstance(v, str) else v
    return None if not v or v == "none" else str(v)


def hook_argv(spec: str | None) -> list[str] | None:
    """argv a hook runs as: a shell command."""
    spec = _hook_spec(spec)
    return None if spec is None else ["/bin/sh", "-c", spec]


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
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) / 1024 / 1024
    except OSError:
        pass
    return 0.0


CARDS_WHY = ""      # why the last _cards() gave None, for the messages that report it ("" when it had no card)


def _cards() -> list[dict] | None:
    """[{index, name, total_gb, used_gb, free_gb, util}] from nvidia-smi; None when unreadable. Tests patch this."""
    global CARDS_WHY
    CARDS_WHY = ""
    try:
        out = proc.run(["nvidia-smi", "--query-gpu=index,name,memory.total,memory.used,utilization.gpu",
                        "--format=csv,noheader,nounits"], group="own", stdin="none", env="inherit", timeout=10)
    except FileNotFoundError:
        CARDS_WHY = "nvidia-smi is not installed"
        return None
    except OSError as e:
        CARDS_WHY = f"nvidia-smi: {e}"
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
    p = {"name": name, "scope": scope, "where": where, "max": None, "probe": None, "every": PROBE_EVERY,
         "settle": SETTLE}
    mx = t.get("max")
    if mx == "auto":
        if name not in SHARED_DEFAULTS:
            raise config.ConfigError(f'{where} = "auto": only cpu, mem_gb and gpu_gb know their own size')
        mx = {"cpu": os.cpu_count() or 1, "mem_gb": round(_mem_total_gb()), "gpu_gb": None}[name]   # gpu: per card
    elif mx is not None:
        try:
            mx = _num(mx)
        except (TypeError, ValueError):
            raise config.ConfigError(f"{where} = {mx!r}: not a number") from None
    p["max"] = mx
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
    return os.path.join(_top("probes"), f"{p['scope'] or '_shared'}.{p['name']}.json")


def _probe_claim(p: dict) -> str:
    return os.path.join(_top("probes"), f"{p['scope'] or '_shared'}.{p['name']}.lock")


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
            m = re.search(r"-?\d+(?:\.\d+)?", out.stdout)
            if out.timed_out:
                r = {"t": t, "value": None, "error": f"it did not answer in {PROBE_WAIT:g}s"}
            elif out.returncode == 0 and m:
                r = {"t": t, "value": _num(m[0]), "error": None}
            else:
                said = (out.stderr.strip() or out.stdout.strip()).splitlines()
                r = {"t": t, "value": None, "error": (f"exit {out.returncode}" if out.returncode else "printed no number")
                     + (f": {said[-1][:120]}" if said else "")}
        paths.write_json(path, r)
    return r


def _need(j: dict, k: str):
    return (j.get("needs") or {}).get(k, 0)


class Resources:
    """Queue q's pools with their live readings (nvidia-smi, probes), taken once and outside the store lock;
    admit() then runs under the lock against every queue's running jobs."""

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

    def _in(self, p: dict, running: list[dict]) -> list[dict]:
        return running if p["scope"] is None else [r for r in running if r["queue"] == p["scope"]]

    def gpu_budget(self, p: dict, card: dict) -> float:
        return p["max"] if p["max"] is not None else round(max(card["total_gb"] - 2, 0), 1)

    def check(self, needs: dict) -> str | None:
        """Why these needs can never fit (nothing provides one, or it exceeds a budget); None when they might."""
        for k, v in needs.items():
            ps = self.by_name.get(k)
            if not ps:
                return (f"nothing provides '{k}': set [resources] {k} (shared by every queue) or "
                        f"[queue.{self.q}.resources] {k} (this queue's own) in {config.global_path()}")
            if k == "gpu_gb":
                if not self.cards:
                    return "needs gpu_gb but nvidia-smi sees no GPU" + (f" ({CARDS_WHY})" if CARDS_WHY else "")
                top = max(min(self.gpu_budget(p, c) for p in ps) for c in self.cards)
                if v > top:
                    return f"needs gpu_gb={v:g} but no card's budget is that large ({top:g})"
                continue
            for p in ps:
                if p["max"] is not None and v > p["max"]:
                    return f"needs {k}={v:g} but {p['where']} = {p['max']:g}"
        return None

    def admit(self, job: dict, running: list[dict]) -> tuple[bool, str, int | None]:
        """(fits, why not, card). running = every queue's running jobs. `why` keeps its fluctuating detail after
        ';' so the worker notes a change of reason only."""
        needs = job.get("needs") or {}
        for k, v in needs.items():
            if k == "gpu_gb":
                continue
            ps = self.by_name.get(k)
            if not ps:
                return False, f"{k}: nothing provides it ([resources] or [queue.{self.q}.resources] {k})", None
            for p in ps:
                if p["max"] is not None:
                    used = sum(_need(r, k) for r in self._in(p, running))
                    if used + v > p["max"]:
                        return False, f"{k}: {p['where']}; {used:g}+{v:g} > {p['max']:g}", None
                if p["probe"]:
                    r = self.readings.get((p["scope"], p["name"]))
                    if r and r.get("running"):
                        return False, f"{k}: its probe is being read ({p['where']})", None
                    if not r or r.get("value") is None:
                        return False, f"{k}: probe failed ({p['where']}); {(r or {}).get('error') or 'no reading'}", None
                    pending = sum(_need(x, k) for x in running if (x.get("started") or 0) + p["settle"] > r["t"])
                    free = r["value"] - pending
                    if v > free:
                        return False, (f"{k}: not free ({p['where']}); probe says {r['value']:g}"
                                       + (f", {pending:g} of it launched since" if pending else "") + f", need {v:g}"), None
        need = needs.get("gpu_gb", 0)
        if not need:
            return True, "", None
        ps = self.by_name.get("gpu_gb")
        if not ps:
            return False, "gpu_gb: nothing provides it", None
        if not self.cards:
            return False, "gpu: nvidia-smi unreadable" + (f" ({CARDS_WHY})" if CARDS_WHY else ""), None
        settle = max(p["settle"] for p in ps)
        whys = []
        for c in sorted(self.cards, key=lambda c: -c["free_gb"]):
            i = c["index"]
            on = [r for r in running if r.get("gpu") == i]
            why = None
            for p in ps:
                reserved, budget = sum(_need(r, "gpu_gb") for r in self._in(p, on)), self.gpu_budget(p, c)
                if reserved + need > budget:
                    why = f"card {i} reserved {reserved:g}+{need:g} > {budget:g} ({p['where']})"
                    break
            if why is None:
                # what the reading cannot see yet: jobs launched on this card less than `settle` before it
                pending = sum(_need(r, "gpu_gb") for r in on if (r.get("started") or 0) + settle > self.cards_t)
                if c["free_gb"] - pending < need:
                    why = f"card {i} free {c['free_gb'] - pending:.1f}" + (
                        f" ({pending:g} launched in the last {settle:g}s, not allocated yet)" if pending else "")
            if why is None:
                return True, "", i
            whys.append(why)
        return False, f"gpu_gb {need:g} does not fit on any card; {', '.join(sorted(whys))}", None

    def usage(self, running: list[dict], scope: str | None) -> list[dict]:
        """The pools of one scope (a queue name, or None = shared) with what running jobs hold: for status."""
        out = []
        for p in self.pools:
            if p["scope"] != scope or p["name"] == "gpu_gb":
                continue
            r = reading(p, run=False) if p["probe"] else None
            out.append({"name": p["name"], "max": p["max"], "used": sum(_need(j, p["name"]) for j in self._in(p, running)),
                        "probe": p["probe"], "reading": r})
        return out


# ---- how a job ends: one decision ------------------------------------------------------------------
#
# Five things can be true of a running job at once: a run of it is live or over, it wrote an exit code or it
# did not, a stop is on its record or there is none, the grace that stop was given is up or it is not, and its
# own time has run out or it has not. `decide` is the one place that says what is to be done about it, from all
# of them together. It reads facts and the job's record, and writes nothing: no clock, no store, no process,
# which `TestContracts` in tests/test_constructs.py reads the source to keep.
#
# Around it, `Worker.carry_out` gathers the facts about each running job, calls this once, and carries the one
# action out: the signal through the job's Handle, the record in one transaction, the hook after it. So a
# timeout, a kill and a reap are not three actors with three views of one job; they are facts about it, and one
# decision made from all of them.

NOTHING = "nothing"         # leave it as it is
TIME_UP = "time-up"         # write a timeout's stop on it; from there it is a stop like any other
TERM = "term"               # SIGTERM to its group, recorded as the stop's `sent`
KILL9 = "kill9"             # SIGKILL to its group, recorded as the stop's `kill9`
ENDED_AS = "ended-as"       # record how it ended, and the hook that is owed for it
AGAIN = "again"             # back to its queue for another attempt


@dataclass(frozen=True)
class Facts:
    """What is true of one running job apart from its record, read before the decision and never inside it.

    held    a run of this attempt is live: its own wrapper holds the job's run claim, or this worker started
            that wrapper and has not reaped it yet. While it is held the
            attempt's exit code counts for nothing, since the command itself can write that file ($PP_QUEUE_RCFILE
            is in its environment); a code is the command's last word only once the run is over.
    rc, rc_at   the exit code this attempt wrote and when it wrote it (`ended_on_its_own`), or None for both.
    sig     the signal the attempt's process died of, where this worker started it and so holds the child.
    now     the time the facts were read at, which is the only clock the decision has.
    tail    the job's log as --retry-if matches against it, read where the job asks for one.
    """

    held: bool
    rc: int | None = None
    rc_at: float | None = None
    sig: int | None = None
    now: float = 0.0
    tail: str = ""


@dataclass(frozen=True)
class Action:
    """The one thing to be done about a job. `do` is which, and a field a kind does not use is None.

        nothing                                                      leave it alone
        time-up   t, grace                                  write {by: timeout, timeout: True, t, grace} on it
        term                                                         SIGTERM to its group
        kill9                                                        SIGKILL to its group
        ended-as  state, rc, reason, hook, ended, sweep, not_retried  record how it ended
        again     attempt, not_before, rc, reason, ended, sweep       back to its queue for another attempt

    hook is the event whose hook is owed ("failed", "done"), or None where the outcome owes none or the job
    carries no such hook. sweep says anything left in the job's process group goes with it.
    """

    do: str
    state: str | None = None
    rc: int | None = None
    reason: str | None = None
    hook: str | None = None
    ended: float | None = None
    sweep: bool = False
    attempt: int | None = None
    not_before: float | None = None
    not_retried: str | None = None
    t: float | None = None
    grace: float | None = None

    @property
    def why(self) -> str:
        """The failure as the job's log names it and --retry-if matches it: a reason, or the exit code."""
        return self.reason or f"exit {self.rc}"


def matches_retry_if(job: dict, why: str, tail: str) -> bool:
    """Whether --retry-if matches this failure: its reason, or the tail of the job's log."""
    pat = job.get("retry_if")
    if not pat:
        return True
    try:
        return re.search(pat, why + "\n" + tail, re.M) is not None   # ^ $ per line
    except re.error:
        return False


def decide(job: dict, facts: Facts) -> Action:
    """What is to be done about this running job: one action, from its record and the facts read with it.

    The rules are the ones `pp help queue` states under "How a job ends", in the order they are tried here.
    That help is their one statement; `TestTheDecision` in tests/test_queue.py is the table over every
    combination of the facts, and it checks each of its rules against that help.

    What a stop is does not rest on who asked for it: a timeout's carries `timeout`, so an asker called
    `timeout` writes a kill's stop like anyone else.
    """
    stop = job.get("stop") or {}
    asked = bool(stop) and not stop.get("timeout")      # a kill: someone asked for this job to stop
    out = bool(stop.get("sent")) or bool(stop.get("kill9"))     # a signal of pp's went out for this stop
    if facts.held:
        # a run of it is live, so nothing is recorded about how it ended and its exit code says nothing yet:
        # the one thing to do about it is to carry out the stop it carries, or to write a timeout's
        if stop.get("denied"):
            return Action(NOTHING)      # pp was refused this job's group: nothing more goes out for it
        if not stop:
            due = (job.get("timeout") and job.get("started")
                   and facts.now - float(job["started"]) >= float(job["timeout"]))
            return Action(TIME_UP, t=facts.now, grace=TIMEOUT_GRACE) if due else Action(NOTHING)
        if stop.get("sent") is None:
            return Action(TERM)
        if not stop.get("kill9") and facts.now - float(stop["sent"]) >= float(stop.get("grace") or 0):
            return Action(KILL9)
        return Action(NOTHING)      # signalled, and inside the grace it was given
    # its run is over: how it ended is recorded here, and nothing of it is signalled again. A stop that was
    # asked of it takes its group with it, so a command that held the signal back does not outlive the job.
    sweep = bool(stop) and not stop.get("denied")
    own = None if facts.rc is not None else (f"signal {facts.sig}" if facts.sig else "lost")
    if asked and out:
        # a kill's signal stopped it: "killed", and no hook, unless it wrote its own code as the kill arrived
        return _ends(job, facts.rc, None if facts.rc is not None else "killed", facts.rc_at, sweep,
                     None if facts.rc is None else ("done" if facts.rc == 0 else "failed"))
    if asked:
        # a kill wrote the stop and no signal of pp's ever went out, so the job ended as it ended: that
        # outcome, and the hook it owes. A job a kill has touched is never retried, whatever it had left
        return _ends(job, facts.rc, own, facts.rc_at, sweep, "done" if facts.rc == 0 else "failed")
    if facts.rc == 0:
        return _ends(job, 0, None, facts.rc_at, sweep, "done")
    if facts.rc is None and (out or stop.get("denied")):
        # a timeout's stop: pp's signal ended it, or pp was refused it and the attempt is over at its timeout
        return _or_again(job, facts, None, "timeout", None, sweep)
    # it ended on its own: the code it wrote, the signal it died of, or nothing at all
    return _or_again(job, facts, facts.rc, own, facts.rc_at, sweep)


def _hook_due(job: dict, event: str | None) -> str | None:
    """The hook event this outcome owes, or None: the outcome owes none, or the job carries no such hook."""
    if event is None:
        return None
    return event if _hook_spec(job.get("on_fail" if event == "failed" else "on_done")) else None


def _ends(job: dict, rc: int | None, reason: str | None, ended: float | None, sweep: bool, event: str | None,
          not_retried: str | None = None) -> Action:
    """The job is over: its state follows its exit code, and the hook that outcome owes is the job's own."""
    return Action(ENDED_AS, state="done" if rc == 0 and not reason else "failed", rc=rc, reason=reason,
                  hook=_hook_due(job, event), ended=ended, sweep=sweep, not_retried=not_retried)


def _or_again(job: dict, facts: Facts, rc: int | None, reason: str | None, ended: float | None,
              sweep: bool) -> Action:
    """A failure of the job's own: another attempt where it has one left and --retry-if matches it, else the
    end of it. A delay doubles per attempt, up to RETRY_DELAY_CAP."""
    attempt, retries = int(job.get("attempt", 1)), int(job.get("retries") or 0)
    why = reason or f"exit {rc}"
    left = attempt <= retries
    if left and matches_retry_if(job, why, facts.tail):
        now = ended or facts.now
        base = job.get("retry_delay")
        delay = min(float(RETRY_DELAY if base is None else base) * 2 ** (attempt - 1), RETRY_DELAY_CAP)
        return Action(AGAIN, rc=rc, reason=reason, ended=now, sweep=sweep, attempt=attempt + 1,
                      not_before=now + delay)
    # attempts were left, but retry_if says this failure is not worth one
    nr = f"neither the reason ({why}) nor the log tail matches /{job.get('retry_if')}/" if left else None
    return _ends(job, rc, reason, ended, sweep, "failed", nr)


def merge_stop(existing: dict | None, ask: dict) -> dict:
    """The one stop this run of the job carries, once this ask has landed on it. Pure: the caller names who
    asked and when.

    One stop per run: what went out is all the decision and the waiting verb have to go on, so a second ask
    never writes over `t`, `sent` or `kill9`, and keeps only the shorter of the two graces, since a second ask
    hastens the SIGKILL and never delays it. It clears `denied`, so a signal pp was refused is asked for again.

    A kill's ask on the stop a worker wrote for a timeout becomes the kill's: `by` is the asker, `timeout`
    goes, and the timeout stays in `was`. So a job a kill has touched is ended as a kill, never retried,
    however its stop began. A timeout landing on a kill's stop changes nothing: a kill stays a kill.

    Which of the two an ask is comes from its `timeout` field and never from who asked, so an asker called
    `timeout` asks as a kill."""
    if not existing:
        return dict(ask)
    out = dict(existing)
    out["grace"] = min(float(existing.get("grace") or 0), float(ask.get("grace") or 0))
    out.pop("denied", None)
    if existing.get("timeout") and not ask.get("timeout"):
        out["was"], out["by"] = existing["by"], ask["by"]
        out.pop("timeout")
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


def _append_log(job: dict, line: str):
    try:
        os.makedirs(os.path.dirname(job["log"]), exist_ok=True)
        with open(job["log"], "a") as f:
            f.write(line.rstrip("\n") + "\n")
    except (OSError, KeyError):
        pass


class Wait:
    """The wait a worker does between its ticks, and the one thing that shortens it. `waking()` hands one out.

    `wait(seconds)` waits that long, ends the moment a signal arrives, and says whether one did. Nothing in a
    worker's loop sleeps: a stop, a Ctrl-C and a `worker stop` all reach it through the same wait."""

    def __init__(self, fd: int):
        self.fd = fd

    def wait(self, seconds: float) -> bool:
        """-> whether a signal ended the wait, rather than the time being up."""
        if self._took():
            return True         # one arrived while the tick ran: it ends this wait, and is not waited out
        left = max(float(seconds), 0.0)
        if not left or not select.select([self.fd], [], [], left)[0]:
            return False
        self._took()
        return True

    def _took(self) -> bool:
        """Take whatever the wakeup pipe holds. -> whether it held anything."""
        try:
            return bool(os.read(self.fd, 4096))
        except BlockingIOError:
            return False


@contextmanager
def waking():
    """The scope a worker's loop runs in: inside it, a signal ends the wait `Wait.wait` does, at once.

    Python's own wakeup pipe is what ends it (signal.set_wakeup_fd): the C handler writes the number of every
    signal this process takes into that pipe, so a wake sent while a tick runs is a byte waiting in the pipe
    when the next wait begins, and no wake is lost. The Python handlers run as they always do, so the same
    SIGTERM that ends a wait sets the stop flag work() reads. The wake's own handler does nothing: it is there
    so that a wake cannot end a worker that is not waiting.

    Only the main thread may hold the wakeup pipe, and one process has one: this is the scope of the worker in
    it, entered before it claims its queue, so a wake can never reach a worker that is not in this scope."""
    r, w = os.pipe()
    os.set_blocking(r, False)
    os.set_blocking(w, False)
    was_handler = signal.signal(WAKE, lambda *_: None)
    was_fd = signal.set_wakeup_fd(w, warn_on_full_buffer=False)
    try:
        yield Wait(r)
    finally:
        signal.set_wakeup_fd(was_fd)
        signal.signal(WAKE, was_handler)
        os.close(r)
        os.close(w)


class Worker:
    """One queue's worker. One tick = one pass over its running jobs (`carry_out`: the facts about each, the
    decision, the action), then launch what fits. work() loops it; tests drive it tick by tick."""

    def __init__(self, queue: str, backfill: bool = False, log=None):
        me = proc.read(os.getpid())
        self.queue = queue
        self.children: dict[int, proc.Child] = {}    # jobs this worker launched; others are adopted
        self.hooks: list[proc.Child] = []            # kept only to reap them
        self.backfill = backfill
        self.pstart = me.start if me else None
        self.started = time.time()
        self.waiting: dict | None = None    # {id, tag, why, t}: what the head of the queue waits for
        self.said = None                    # (id, reason) last logged, without the detail after ';'
        self.serving = 0        # stops of this queue in flight at the last tick: while any is, the wait is short
        self.claim: lock.Claim | None = None    # where it records itself; work() sets it once it has taken it
        self.log = log or (lambda msg: None)
        self.stopping = lambda: False   # work() points it at its stop flag: a stopping worker launches nothing new

    def record(self) -> dict:
        """What this worker says about itself while it holds its claim, which is where `status` reads it."""
        return {"pid": os.getpid(), "pstart": self.pstart, "started": self.started, "backfill": self.backfill,
                "waiting": self.waiting}

    def _publish(self):
        if self.claim is not None:
            self.claim.write(self.record())

    def set_backfill(self, on: bool):
        self.backfill = on
        self._publish()

    def tick(self):
        self.carry_out()        # the one actor that signals this queue's jobs and records how one ended
        watchers()              # and the leases of the waits a signal ended, swept where the queue is read
        self.hooks = [h for h in self.hooks if h.poll() is None]
        if not self.stopping():
            self.launch()   # last: a broken config raises here, after ended jobs are recorded

    # -- one pass over the running jobs: the facts, the decision, the action

    def carry_out(self):
        """One pass over this queue's running jobs: gather the facts about each, ask `decide` once, and do the
        one thing it says. The whole of a tick apart from launching, and the worker's own: the worker is the
        one actor that signals this queue's jobs.

        A timeout, a kill and a reap are not three passes over the queue with three views of a job. They are
        facts about one job and one decision made from all of them, so no part of it can be wrong about what
        another part did."""
        now = time.time()
        with listing() as s:    # one pass: the jobs to decide about, and the hooks the queue still owes
            jobs = list(s.jobs(("running",) + ENDED, self.queue))
        owed = [j for j in jobs if j["state"] in ENDED and j.get("hook_due")]
        in_flight = 0
        for j in [x for x in jobs if x["state"] == "running"]:
            stop = j.get("stop") or {}
            action = self._step(j, self._facts(j, now))
            if action.do in (TERM, KILL9, TIME_UP) or (action.do == NOTHING and stop
                                                       and not stop.get("denied") and not stop.get("kill9")):
                in_flight += 1          # a stop with something still to go out for it
        # while a stop is in flight, and a verb is waking this worker for it, the loop's wait is STOP_PERIOD
        # and not the poll (work()). A stop whose SIGKILL has gone out has nothing left to carry out, so it is
        # not one: the worker waits for the kernel to end a job that is slow to die, at its poll or at the
        # next wake of whoever is waiting, and the reap sees the run claim go like any other
        self.serving = in_flight
        for j in owed:
            self.fire(j, j["hook_due"])     # owed by a worker that did not get to fire it
        # a child of this worker's whose record no longer says running: something outside pp ended the job
        # while it was starting, and `_spawn` stopped the process it had started. Reap the zombie
        for jid, p in list(self.children.items()):
            if p.poll() is not None and (find(jid) or {}).get("state") != "running":
                del self.children[jid]

    def _facts(self, j: dict, now: float) -> Facts:
        """What is true of this running job besides its record, all of it read here and nothing decided:
        whether a run of it is live, the exit code that run wrote and when, the signal its own child died of,
        and the log tail --retry-if matches against.

        Read before the transaction, since none of it is store state and a log tail can be long."""
        held = lock.Claim.holder(_run_claim(self.queue, j["id"]))
        p = self.children.get(j["id"])
        if held is not None or (p is not None and p.poll() is None):
            # a run of it is live, whatever its record holds: it is never lost, and never started again. Its
            # exit code is not read at all here: while the run is live it says nothing (Facts). A wrapper this
            # worker started is live until this worker reaps it: a process closes its files, the claim among
            # them, before its parent can read how it ended.
            if held is not None:
                self._adopt(j, held)
            return Facts(held=True, now=now)
        self.children.pop(j["id"], None)
        code = p.poll() if p is not None else None
        rc, ended = ended_on_its_own(j) or (None, None)
        tail = "\n".join(log_tail(j, 200)) if rc != 0 and j.get("retry_if") else ""
        return Facts(held=False, rc=rc, rc_at=ended, sig=-code if code is not None and code < 0 else None,
                     now=now, tail=tail)

    def _step(self, j: dict, facts: Facts) -> Action:
        """The one decision about one job, and the action it names, carried out: what is store state in one
        transaction, the signal through the job's Handle, the log line and the hook after the scope.
        -> the action, which says whether a stop of this job is still in flight.

        The decision is made inside the transaction, on the record the store holds and the facts read before
        it: a stop another actor wrote since those facts were read is part of it, so a job a kill has touched
        is never retried past its stop."""
        jid = j["id"]
        line = None
        with txn() as s:
            cur = s.job(jid, must_be=("running",))
            if cur is None:
                return Action(NOTHING)      # nothing else records a running job: there is nothing to record
            a = decide(cur, facts)
            if a.sweep:
                self._sweep_group(cur)      # its leader is gone; anything left in its group goes too
            if a.do == TIME_UP:
                cur["stop"] = {"by": "timeout", "timeout": True, "t": a.t, "grace": a.grace}
                s.save(cur)
            elif a.do == AGAIN:
                line = _requeue(s, cur, a)
            elif a.do == ENDED_AS:
                _finish(s, cur, a.state, a.rc, a.reason, a.ended, a.not_retried, hook=a.hook)
        if a.do in (TERM, KILL9):
            self._send(cur, a)
            return a
        if line:
            _append_log(cur, line)          # after the scope: a job's log is not the store
        if a.do == TIME_UP:
            self.log(f"timeout {jid} {cur.get('tag')} after {ui.human_duration(float(cur['timeout']))}: "
                     "stopping it")
        elif a.do == AGAIN:
            self.log(f"retry {jid} {cur.get('tag')}: attempt {cur['attempt']}/{int(cur.get('retries') or 0) + 1} "
                     f"in {_in_delay(cur['not_before'])}")
        elif a.do == ENDED_AS:
            why = f" ({cur['reason']})" if cur.get("reason") else ""
            self.log(f"{'done' if cur['state'] == 'done' else 'FAILED'} {jid} {cur.get('tag')} "
                     f"rc={cur.get('rc')}{why} {ui.human_duration(cur.get('seconds'))}")
            if a.hook:
                self.fire(cur, a.hook)
        return a

    def _send(self, job: dict, a: Action) -> None:
        """The signal one stop asks for next, and what it did on the record: a SIGTERM marked `sent`, a
        SIGKILL marked `kill9`, a refusal marked `denied`. The queue's worker is the one actor that signals a
        job's processes, so a kill, a timeout and a reap never cross.

        The signal goes out first and is recorded after, so a record that says a signal went out is one that
        did, which is all the decision about this job has to go on."""
        stop = job.get("stop") or {}
        sig, mark, word = ((signal.SIGTERM, "sent", "SIGTERM to its group") if a.do == TERM else
                           (signal.SIGKILL, "kill9",
                            f"SIGKILL after {ui.human_duration(float(stop.get('grace') or 0))}"))
        outcome = self._signal_group(job, sig)
        if outcome is None or outcome == "gone":
            return          # nothing to signal: its run is starting, or it is over and the next pass says how
        now = time.time()
        with txn() as s:
            cur = s.job(job["id"], must_be=("running",))
            if cur is None or not cur.get("stop"):
                return
            cur["stop"]["denied" if outcome == "denied" else mark] = now
            s.save(cur)
        self.log(f"stop {job['id']} {job.get('tag')} ({stop.get('by')}): "
                 + (f"not allowed to signal its group {cur.get('pgid')}" if outcome == "denied" else word))

    def _adopt(self, j: dict, held: dict):
        """Complete the record of a running job from its claim. The wrapper writes its own pid in there, so a
        job whose worker died between the start and the record is named again, and can be timed out, killed and
        reported like any other. The claim is held, so that pid is that wrapper's."""
        if j.get("pid") or not held.get("pid"):
            return
        p = proc.read(int(held["pid"]))
        if p is None:
            return              # it ended as this was read; the next look reaps it
        with txn() as s:
            cur = s.job(j["id"], must_be=("running",))
            if cur is None or cur.get("pid"):
                return
            cur.update(pid=p.pid, pgid=p.pgid, pstart=p.start, boot=proc.boot_id())
            s.save(cur)
        self.log(f"adopted {j['id']} {j.get('tag')}: its run is pid {p.pid}, from its claim")

    def fire(self, job: dict, event: str):
        """Run the job's on_fail / on_done hook, detached; record it on the job, and that the job no longer
        owes it.

        The outcome and the hook owed for it are written in one transaction (`_finish`); `hook_due` is cleared
        here, once the hook has run. A hook that could not start is reported and cleared too, so a queue is
        never left trying to start it again every tick."""
        spec = _hook_spec(job.get("on_fail" if event == "failed" else "on_done"))
        argv = hook_argv(spec)
        q, jid = job["queue"], job["id"]
        hlog = os.path.join(_qroot(q, "hooks"), f"{jid}-{event}.log")
        ran = False
        if argv:
            env = {"PP_QUEUE": q, "PP_QUEUE_JOB": str(jid), "PP_QUEUE_EVENT": event,
                   "PP_QUEUE_RC": "" if job.get("rc") is None else str(job["rc"]), "PP_QUEUE_REASON": job.get("reason") or (f"exit {job['rc']}" if event == "failed" and job.get("rc") is not None else ""),
                   "PP_QUEUE_LOG": job.get("log") or "", "PP_QUEUE_CWD": job.get("cwd") or "", "PP_QUEUE_TAG": job.get("tag") or ""}
            cwd = job.get("cwd") if job.get("cwd") and os.path.isdir(job["cwd"]) else "/"
            try:
                with open(hlog, "a") as f:
                    f.write(f"[pp queue] {datetime.now():%Y-%m-%d %H:%M:%S} {event} hook of job {jid}: {shlex.join(argv)}\n")
                    f.flush()
                    h = proc.spawn(argv, group="own", stdin="none", env="scrubbed", add_env=env, cwd=cwd, stdout=f,
                                   stderr=proc.STDOUT)
                self.hooks.append(h)
                ran = True
            except OSError as e:
                self.log(f"hook {event} of {jid} could not start: {e}")
        with txn() as s:        # it has run: the job owes it no longer, whether or not it could start
            cur = s.job(jid)
            if cur is not None:
                if ran:
                    cur.setdefault("hooks", []).append({"event": event, "cmd": spec, "t": time.time(), "log": hlog})
                if cur.pop("hook_due", None) is not None or ran:
                    s.save(cur)
        if ran:
            self.log(f"hook {event} of {jid}: {spec} (log {hlog})")

    # -- stopping a job: a `kill`, and a timeout, which is a stop the worker writes on the job itself

    @staticmethod
    def wake(h: proc.Handle | None) -> None:
        """End the wait of the worker this handle names, so it ticks now instead of when its period is up. The
        handle comes from the look `worker_up` has already taken (`Holder.handle`), so waking a worker costs no
        second look at the claim.

        The wake is a signal to a worker, never to a job: a worker inside `waking` takes it from its own
        wakeup pipe, so one sent while a tick runs ends the next wait rather than being lost. It is the
        worker's own, like the signalling above it, which is why it lives here and not in the verb that
        sends it. No verb holds the worker claim: a verb writes its stop, wakes the worker or starts one, and
        waits for the record."""
        if h is not None:
            proc.signal_pid(h, WAKE)

    def _sweep_group(self, job: dict):
        """The leader of a stopped job's group is gone; anything left in that group goes too, so a command that
        held the signal back does not outlive the job."""
        self._signal_group(job, signal.SIGKILL)

    def _signal_group(self, job: dict, sig: int) -> str | None:
        """Signal the job's process group, and say how that went (proc.signal_group's outcomes), or None while
        the job has no group to signal yet.

        The record names the group with the start time read beside it, so a group id that belongs to another
        process by now is not signalled. Where the record names none, or names one without that start time, the
        job's claim names its wrapper, which leads that group and holds the claim, so that pid is still its own.
        One of the two names the group, unless the wrapper has not written its pid yet, which is the first
        moment of a launch."""
        h = _group_handle(job)
        if h is None:
            held = lock.Claim.holder(_run_claim(job["queue"], job["id"])) or {}
            h = proc.Handle.live(int(held["pid"]), group=True) if held.get("pid") else None
        if h is None:
            return None
        outcome = proc.signal_group(h, sig)
        if outcome == "gone":
            self.log(f"nothing to signal in job {job['id']}'s group {h.id}: it is gone")
        return outcome

    # -- launching

    def launch(self):
        queued = _jobs("queued", self.queue)
        now = time.time()
        ready = [j for j in queued if (j.get("not_before") or 0) <= now]     # a retry's delay never blocks the line
        if not ready:
            self._note(None)
            return
        res = Resources(self.queue, {k for j in ready for k in (j.get("needs") or {})}, empty_ok=False)
        if self.stopping():
            return          # told to stop while the readings were taken
        taken: list = []        # (record, card, claim) for each job this pass admitted
        blocked = None          # the first job that did not fit, and why
        try:
            # one transaction for the whole pass: it reads the running jobs it admits against once, and reads
            # each job it is about to start again in that same scope, since another queue's worker may be
            # admitting from a shared pool and a verb may have changed the job since it was listed (prio, a note)
            with txn() as s:
                running = s.jobs("running")
                for j in ready:
                    cur = s.job(j["id"], must_be=("queued",))
                    if cur is None:
                        continue        # held, dropped or started meanwhile
                    ok, why, card = res.admit(cur, running)
                    if not ok:
                        if blocked is None:
                            blocked = (cur, why)
                        if self.backfill:
                            continue
                        break
                    # the run claim before the record says running, so a reader never sees a running job with
                    # nothing holding its claim unless its run has ended. Taking one is two calls that never
                    # wait, which is why it belongs in here; nothing under the store lock may wait either.
                    claim = lock.Claim.take(_run_claim(self.queue, cur["id"]))
                    if claim is None:
                        continue        # a run of this job is live already
                    taken.append((cur, card, claim))    # before the rest of it, which can raise
                    self._claim(s, cur, card)
                    running.append(cur)     # what this pass has taken counts against the next job in it
            for cur, card, claim in taken:  # outside the transaction: starting a process is not store work
                self._spawn(cur, card, claim)
        finally:
            # pp's own copy of every claim this pass took, however it ended: a wrapper that was started holds
            # the claim through the file description it was given, so releasing this copy frees nothing of
            # its; one for a job no `_spawn` reached is freed here, and that job is reaped like any other
            # whose run is over. Releasing twice is not an error.
            for _cur, _card, claim in taken:
                claim.release()
        if blocked is None:
            self._note(None)
        else:
            self._note(*blocked)

    @staticmethod
    def _claim(s: Txn, j: dict, gpu: int | None):
        """In a transaction: the job, as read in it, becomes running, with the file this attempt writes its exit
        code to. Its run claim is held before this, so no reader ever sees it running with nothing holding its
        claim unless its run has ended."""
        j.update(started=time.time(), gpu=gpu, state="running", rcfile=_new_rcfile(j))
        j.pop("not_before", None)
        s.save(j)

    def _spawn(self, j: dict, gpu: int | None, claim: lock.Claim) -> dict | None:
        """Start the attempt's wrapper, and hand it the run claim as its stdin: it holds that claim for exactly
        as long as the attempt lives. pp closes its own copy here, so nothing but the wrapper holds it.

        Whatever stopped this start is this job's launch failure, recorded on it: a directory that is gone, a
        log that cannot be opened, an environment name the kernel will not take. The pass that got here has
        marked every job it admitted `running` already, so one job that cannot start must not end the pass
        for the others: they would be left with no process, and read as lost."""
        jid, q = j["id"], self.queue
        argv = list(j.get("runner") or []) + list(j["cmd"])
        env = {**(j.get("shell_env") or {}), **(j.get("env") or {}),
               "PP_QUEUE": q, "PP_QUEUE_JOB": str(jid), "PP_QUEUE_RCFILE": j["rcfile"]}
        if gpu is not None:
            env["CUDA_VISIBLE_DEVICES"] = str(gpu)
        retries = int(j.get("retries") or 0)
        attempt = f" attempt {j.get('attempt', 1)}/{retries + 1}" if retries else ""
        try:
            os.makedirs(os.path.dirname(j["log"]), exist_ok=True)
            with open(j["log"], "a") as f:
                f.write(f"[pp queue] {datetime.now():%Y-%m-%d %H:%M:%S} job {jid} ({q}){attempt} in {j['cwd']}: {shlex.join(argv)}\n")
                f.flush()
                p = proc.spawn(["/bin/sh", "-c", WRAP, "pp-queue-job", *argv], group="own", stdin=claim.fd,
                               env="scrubbed", add_env=env, cwd=j["cwd"], stdout=f, stderr=proc.STDOUT)
        except Exception as e:
            with txn() as s:
                cur = s.job(jid, must_be=("running",))
                if cur:
                    _finish(s, cur, "failed", None, f"launch failed: {e}", hook=_hook_due(cur, "failed"))
            claim.release()     # after the record says it ended, so no reader sees it running with a free claim
            self.log(f"FAILED {jid} {j.get('tag')}: launch failed: {e}")
            if cur:
                self.fire(cur, "failed")
            return None
        claim.release()         # pp's own copy of it; the wrapper's keeps the claim
        with txn() as s:        # amend the record as it is now, not the copy launch read: a note added meanwhile stays
            cur = s.job(jid, must_be=("running",))
            if cur:
                cur.update(pid=p.pid, pgid=p.pid, pstart=p.handle.start if p.handle else None,
                           boot=proc.boot_id())
                s.save(cur)
        if cur is None:
            # its record no longer says running, which only something outside pp can do to a job this worker is
            # starting; its process must not run on under a record that has ended
            p.end(signal.SIGTERM)
            self.log(f"job {jid} ended elsewhere while it started; stopped its process {p.pid}")
        self.children[jid] = p
        self.log(f"start {jid} {j.get('tag')} [{j.get('project')}] {_needs_str(j.get('needs'))}"
                 + (f" on GPU{gpu}" if gpu is not None else "") + attempt)
        return cur

    def _note(self, j: dict | None, why: str = ""):
        """Record what the head of the queue waits for, in this worker's own claim where `status` reads it,
        whenever it changes; log it only when its reason changes, not its detail after ';'."""
        now = None if j is None else {"id": j["id"], "tag": j.get("tag"), "why": why, "t": time.time()}
        if (self.waiting or {}).get("id") == (now or {}).get("id") \
                and (self.waiting or {}).get("why") == (now or {}).get("why"):
            return              # the same job, waiting for the same thing
        self.waiting = now
        self._publish()
        key = None if j is None else (j["id"], why.split(";")[0])
        if key is not None and key != self.said:
            self.log(f"waiting: {j['id']} {j.get('tag')} for {why}")
        self.said = key


def _worker_claim(q: str) -> str:
    return os.path.join(_qroot(q, create=False), "worker.lock")


def _worker_log(q: str, create: bool = False) -> str:
    """Where queue q's worker writes what it does. Naming it creates nothing: a verb that only reports
    (`worker status`) leaves no directory behind for a queue name that was a typo. create: the worker
    about to open it for writing makes the queue's directories."""
    return os.path.join(_qroot(q, create=create), "worker.log")


def worker(q: str) -> dict | None:
    """Queue q's live worker as it records itself: {pid, pstart, started, backfill, waiting}. None when the
    queue has no live worker.

    Only a worker ever holds the claim on worker.lock, and it holds it for as long as it runs: that is what
    being up means. A worker that was killed leaves it free at once, and one that is alive holds it however
    long a tick takes. The record is what it is doing, written under the claim. A record that names no live
    process is a worker in its first moment, which has not recorded itself yet (`_said`). Asking never waits
    for it.

    `worker_now` is the one place that asks this, and every verb asks `worker_now` or `worker_up`."""
    return lock.Claim.holder(_worker_claim(q))


def _start_worker(q: str) -> proc.Child:
    """A detached worker for q, with no session identity in its environment (the caller may be an agent), once
    it is ready: it says so when it holds its claim, has recorded itself and handles a stop."""
    with open(_worker_log(q, create=True), "a") as log:
        return proc.spawn(paths.self_cmd() + [_PROG, "work", "-q", q], group="own", stdin="none", env="scrubbed",
                          cwd="/", stdout=log, stderr=proc.STDOUT, ready=True)


# ---- whether a queue's worker is up: one construct -------------------------------------------------
#
# Only a worker ever holds a queue's worker claim, and `work()` is the one place that takes it. So two things
# can be true of that claim, and one of them at a time: a worker holds it, or nobody does.
#
#   a worker holds the claim      it is up, and its record says what it is doing
#   nobody holds the claim        the queue has no worker, and a verb that needs one starts it here
#
# A worker in its first moment is neither: it has the claim and the record in it names no process, since a take
# writes that record in the call after the lock. Such a look is no answer, so the construct looks again, a
# bounded number of times, and says a worker is starting where the record still names nobody.
#
# A verb asks `worker_now`, which reports, or `worker_up`, which makes sure a worker is up, and prints from the
# one answer it gets back. So no verb acts on a look it took outside the scope that acted.

UP = "up"               # a worker holds the claim and has recorded itself
STARTED = "started"     # the claim was free, and the worker this start brought up holds it
DOWN = "down"           # nobody holds the claim
STARTING = "starting"   # a worker holds the claim and has not recorded itself yet

WORKER_STARTS = 3       # workers `worker_up` starts at most, each one asked about after it
STARTING_LOOKS = 25     # looks it takes at a claim whose holder has not recorded itself ...
STARTING_PAUSE = 0.02   # ... and how long apart, which is many times the one write that ends that moment


@dataclass(frozen=True)
class Holder:
    """What one look at a queue's worker claim came to, and the one thing every verb prints from.

    how     up, started, down or starting, as above.
    rec     the record of the worker that holds the claim. None where nobody holds it.
    why     why a round of starts brought no worker up, where this answer came from such a round. None
            otherwise. A start that times out is the one reason the worker's own log cannot carry, since a
            worker that hung before it said it was ready has logged nothing, so a caller that asked quietly
            says this itself.
    """

    how: str
    rec: dict | None = None
    why: str | None = None

    @property
    def up(self) -> bool:
        """Whether a worker is up: the one answer, so nothing has to read a record to find out."""
        return self.how in (UP, STARTED)

    @property
    def pid(self) -> int | None:
        """The pid of the queue's worker. None where nobody holds the claim, and while a worker that holds it
        has not recorded itself yet."""
        return (self.rec or {}).get("pid") if self.up else None

    @property
    def handle(self) -> proc.Handle | None:
        """The worker as the handle a signal to it goes through: a wake, or the SIGTERM of `worker stop`. None
        where this look found no worker. A worker that is up has recorded itself, so both halves are there."""
        return proc.Handle.recorded(self.pid, (self.rec or {}).get("pstart")) if self.up else None


def _starting(q: str) -> str:
    """What a person is told while a worker holds q's claim and has not recorded itself yet."""
    return f"the {q} worker is starting and has not recorded itself yet"


def _said(rec: dict | None) -> bool:
    """Whether the record in a worker claim is the record of the worker that holds it now.

    A take locks the claim file, empties it and writes the record in that order (`lock.Claim.take`), so for
    that moment a reader finds the file empty, or holding the record a worker that was killed left. A release
    empties the file, and a worker's record names itself, so those bytes name a process that is gone. Either
    way a record that names no process of its own, or one that names a process that is not running, is a
    worker in its first moment."""
    return proc.Handle.running((rec or {}).get("pid"), (rec or {}).get("pstart")) is not None


def worker_now(q: str) -> Holder:
    """What q's worker is: the one look at the claim's holder. It writes nothing and starts nothing.

    A worker that holds the claim and has not recorded itself yet is no state to report, so this looks again,
    a bounded number of times. A holder that never records itself comes back as a worker starting, for the
    verb to say so rather than name it as up."""
    rec: dict | None = None
    for _ in range(STARTING_LOOKS):
        rec = worker(q)
        if rec is None:
            return Holder(DOWN)
        if _said(rec):
            return Holder(UP, rec=rec)
        time.sleep(STARTING_PAUSE)      # its first moment: look again rather than decide from it
    return Holder(STARTING, rec=rec)


# When a round of starts of this process last brought no worker up, per queue. A loop that asks on every pass
# waits START_EVERY out before it starts another round, so a worker that cannot come up is not started over
# and over. The entry goes as soon as a worker is found up, so a queue that recovers is not held off.
_none_came_up: dict[str, float] = {}


def _no_worker_came_up(q: str, quiet: bool, said: bool, w: Holder, why: str | None = None) -> Holder:
    """This process's round of starts brought no worker up: hold off from starting another for START_EVERY,
    and say so unless the caller asked quietly. -> that answer, carrying why on it for a quiet caller to say.

    The clock is read here, after the starts, and not before them: a round can take longer than START_EVERY
    (a start waits for the worker to say it is ready), and a reading from before it would already be past."""
    _none_came_up[q] = time.time()
    if not quiet and not said:
        ui.warn(f"{_PROG}: the {q} worker did not come up; its log: {_worker_log(q)}")
    return Holder(w.how, rec=w.rec, why=why)


def worker_up(q: str, quiet: bool = False, wake: bool = False) -> Holder:
    """Make sure q has a worker, and say what it is: what `add`, `kill`, `release`, `retry`, a wait loop and
    `worker start` all ask, and the one answer they print from.

    A worker that is up comes back as it is, woken where wake asks, so a job this verb made ready launches now
    and not at the worker's next tick. Where nobody holds the claim this starts a worker and waits for it: the
    new worker says it is ready once it holds its claim, has recorded itself and has its stop handler in
    place, so a `worker stop` or a status right after this sees it. A start whose worker found the claim taken
    meanwhile brings up no worker, and then this asks again rather than guessing, WORKER_STARTS starts at
    most. Every answer is a decision taken after the last start this made, so no verb reports a state from
    before the act it reports on.

    A worker in its first moment holds the claim, so no start can take it: this says a worker is starting and
    leaves it at that. Where a round of starts brought no worker up, the answer is a queue with no worker, and
    this process starts another round only once START_EVERY has passed, so a loop that asks on every pass does
    not start workers it cannot bring up ten times a second. A caller that is not quiet is told when its own
    round brought no worker up. An ask that lands inside the hold-off of an earlier round starts nothing and
    says nothing. Every answer from a round that brought none up carries the reason in `why`, so a caller
    that asked quietly, `worker start` and a watch loop among them, says it itself."""
    starts: list[int | None] = []       # the workers this started, which the look after each asks about
    while True:
        w = worker_now(q)
        if w.up:
            _none_came_up.pop(q, None)      # it is up: nothing to hold the next start off for
            if w.pid is not None and w.pid in starts:
                if not quiet:
                    print(f"pp {_PROG}: started the {q} worker (pid {w.pid}; log {_worker_log(q)})",
                          file=sys.stderr)
                return Holder(STARTED, rec=w.rec)
            if wake:
                Worker.wake(w.handle)
            return w
        if w.how == STARTING:
            if not quiet:   # the same thing every verb says about this state, from the answer it got
                print(f"pp {_PROG}: {_starting(q)}", file=sys.stderr)
            return w
        if not starts and time.time() < _none_came_up.get(q, 0.0) + START_EVERY:
            return w        # a round of this process's brought none up that recently: not another yet
        if len(starts) == WORKER_STARTS:
            return _no_worker_came_up(q, quiet, False, w)    # the claim's next holder shut this many out
        try:
            starts.append(_start_worker(q).pid)     # starting a process is not store work: no lock is held
        except TimeoutError as e:
            if not quiet:
                ui.warn(f"{_PROG}: {e}; its log: {_worker_log(q)}")
            return _no_worker_came_up(q, quiet, True, w, why=str(e))


def work(q: str, backfill: bool | None = None, poll: float | None = None) -> int:
    writing_queue(q)    # a queue this pp cannot read is refused here too; the worker's own queue says its format
    from_cfg = backfill is None       # `work --backfill` fixes it; otherwise it follows the config, like the rest
    if from_cfg:
        try:
            backfill = bool(_qcfg(q).get("backfill", False))
        except config.ConfigError:
            backfill = False

    def log(msg):
        print(f"[{q} {datetime.now():%m-%d %H:%M:%S}] {msg}", flush=True)

    w = Worker(q, bool(backfill), log)
    # the wake is held for the whole of this worker's life, before it claims the queue: a wake can only ever
    # reach a worker that takes it from its own wakeup pipe
    with waking() as waits:
        claim = lock.Claim.take(_worker_claim(q), w.record())
        if claim is None:       # one worker per queue, and holding the claim is being that worker
            held = worker_now(q)
            if held.up:
                print(f"pp {_PROG}: the {q} worker is already running (pid {held.pid})")
            elif held.how == STARTING:
                print(f"pp {_PROG}: {_starting(q)}, so this worker is not the one")
            else:
                print(f"pp {_PROG}: another worker held the {q} worker's claim and has let it go")
            return 0
        w.claim = claim
        stop: list = []
        signal.signal(signal.SIGTERM, lambda *_: stop.append(1))    # a flag, never an exception mid-move
        proc.say_ready()    # it holds its claim, it is recorded, a stop is handled and a wake is taken
        w.stopping = lambda: bool(stop)
        adopted = _jobs("running", q)
        log(f"worker up, pid {os.getpid()}" + (", backfill" if backfill else "")
            + (f", adopting {len(adopted)} running job(s)" if adopted else ""))
        idle_since = None
        bad_cfg = None
        woken = False
        try:
            while not stop:
                period, idle_exit, bad = poll or 2.0, 600.0, None
                try:
                    cfg = _qcfg(q)
                    period = float(poll or cfg.get("poll", 2))
                    idle_exit = float(cfg.get("idle_exit", 600))
                    if from_cfg and bool(cfg.get("backfill", False)) != w.backfill:
                        w.set_backfill(bool(cfg.get("backfill", False)))
                        log(f"backfill {'on' if w.backfill else 'off'} (config)")
                except config.ConfigError as e:
                    bad = str(e)
                try:
                    w.tick()    # reaping and timeouts go on under a broken config; only launching needs it
                except config.ConfigError as e:
                    bad = str(e)
                except Exception:
                    log("tick failed:\n" + traceback.format_exc())
                if bad != bad_cfg:
                    log(f"bad config, nothing launches until it is fixed: {bad}" if bad else "config fixed")
                    bad_cfg = bad
                with listing() as s:
                    if s.jobs(("queued", "running"), q):
                        idle_since, spent = None, False
                    else:
                        idle_since = idle_since or time.time()
                        spent = time.time() - idle_since >= idle_exit
                        if spent:
                            # let the claim go inside the scope that read the store, which is the one order
                            # that leaves no job behind: a job stored after this finds the claim free and
                            # starts the next worker, and one stored before it was read here, so this worker
                            # stays
                            claim.release()
                if spent:
                    log(f"idle for {idle_exit:g}s, exiting")
                    break
                # one wait, ended by the period or by a signal: a stop in flight is carried out in the next
                # tick, and a wake or a SIGTERM is acted on as it arrives. The short period goes with the
                # wake: a verb waiting for a stop wakes this worker on every pass of its own wait, so the
                # tick after that wake is this worker's next one. A stop nobody waits for, a timeout's, is
                # carried out at the queue's poll, and the store lock is not taken ten times a second for a
                # job the kernel is slow to end
                woken = waits.wait(min(period, STOP_PERIOD) if w.serving and woken else period)
        except KeyboardInterrupt:
            stop.append(1)
        finally:
            if stop:
                log("stopped; running jobs keep running and the next worker adopts them")
            claim.release()
    return 0


# ---- watchers: the pull side ----------------------------------------------------------------------
#
# `wait` and `watch` hold a lease (watchers/<pid>-<pstart>.claim) while they run and stamp each outcome they print on the job
# (`reported`). An agent that runs one in the background hears the outcome from it.
#
# The stamp: job["reported"] = [{t, by: "wait pid N", session}], written only by _stamp_reported (wait/watch; never
# `log -f` or a hook). session is the caller's agent session, None for a person. Readers:
#   wait --mine    collects the session's ended jobs that carry no stamp of that session, so a job that fails
#                  between `add` and `wait --mine` is still reported and the wait does not exit 0. Cost: the first
#                  --mine of a session that never waited prints every outcome it never heard, old ones included; a
#                  per-session cursor would avoid that with more state.
#   show           "reported to" rows

def _watcher_claim(pid: int, pstart: int) -> str:
    """The file one `wait` or `watch` holds its lease on. The name is that process's own, so nobody else ever
    takes it and the file goes with the lease."""
    return os.path.join(_top("watchers"), f"{pid}-{pstart}.claim")


@contextmanager
def watching(ids: list[int] | None, scope: list[str] | None, verb: str):
    """Hold a lease on these jobs (ids None: every job of scope; scope None: every queue) while the block runs."""
    me, who = proc.read(os.getpid()), session.caller()
    # the lease is a claim this process holds while it waits, under a name that is this process's own: the
    # kernel lets it go when the process ends, however it ends, so no reader has to judge whether it is alive.
    # It is locked before that name is visible (lock.Claim.make), so the sweep below can never take it from a
    # wait that is taking it: a lease that is there is held, and one that is not held has lost its holder.
    claim = lock.Claim.make(_watcher_claim(os.getpid(), me.start if me else 0),
                            {"pid": os.getpid(), "ids": ids, "scope": scope, "verb": verb,
                             "session": who["session"] if who else None, "t": time.time()})
    try:
        yield
    finally:
        claim.release()
        lock.Claim.forget(claim.path)   # nobody else ever takes this name, so this frees nothing of theirs


def watchers(job: dict | None = None) -> list[dict]:
    """The live `wait` / `watch` processes holding a lease on this job, and none when job is None, which is
    the sweep on its own.

    A lease nobody holds is removed wherever this is asked. A wait ended by a signal runs no finally of its
    own: the kernel let its claim go as the process died, and the file it was holding goes here. `show` asks
    about one job, and a worker's tick asks about none, so the leases of an agent's interrupted waits are
    swept where the queue is being read anyway.

    A lease is locked before its name is visible (`watching`), so a lease that is there and unheld is one
    whose holder has gone, and this takes nothing from a wait that is taking its own."""
    d = _top("watchers")
    out = []
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".claim"):
            continue
        path = os.path.join(d, fn)
        rec = lock.Claim.holder(path)
        if rec is None:
            lock.Claim.forget(path)     # the process whose own name this is has gone, so the file goes too
            continue
        if job is None or not rec.get("pid"):
            continue        # a sweep, which asks about none; or a record that names nobody
        if (rec.get("ids") is None or job["id"] in rec["ids"]) and (rec.get("scope") is None or job["queue"] in rec["scope"]):
            out.append(rec)
    return out


def _stamp_reported(jid: int, verb: str):
    who = session.caller()
    with txn() as s:
        j = s.job(jid, must_be=ENDED)
        if j:
            j.setdefault("reported", []).append({"t": time.time(), "by": f"{verb} pid {os.getpid()}",
                                                 "session": who["session"] if who else None})
            s.save(j)


# ---- helpers for verbs ----------------------------------------------------------------------------

def _resolve(specs: list[str], scope: list[str] | None = None) -> list[dict]:
    """Jobs named by ids, ranges (12-15) or exact tags, in the order given, without duplicates. Ids are global.
    Each carries _bulk: it came from a range or a tag naming several jobs, so a verb skips it when it is in a state
    the verb does not apply to, where a job named on its own is an error."""
    by_id = {j["id"]: j for j in _jobs(queue=scope)}
    out, seen = [], set()
    for s in specs:
        m = re.fullmatch(r"(\d+)-(\d+)", s)
        if m:
            hits = [by_id[i] for i in range(int(m[1]), int(m[2]) + 1) if i in by_id]
        elif s.isdigit():
            hits = [by_id[int(s)]] if int(s) in by_id else []
        else:
            hits = sorted((j for j in by_id.values() if j.get("tag") == s), key=lambda j: j["id"])
            qs = sorted({j["queue"] for j in hits})
            if len(qs) > 1:
                ui.die(f"{_PROG}: tag '{s}' names jobs in queues {', '.join(qs)}: "
                       + ", ".join(f"{j['id']} ({j['queue']})" for j in hits) + "; use ids, or -q", 2)
        if not hits:
            ui.die(f"{_PROG}: no job matches '{s}'", 2)
        for j in hits:
            if j["id"] not in seen:
                seen.add(j["id"])
                out.append({**j, "_bulk": bool(m) or len(hits) > 1})
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


def _section(rows: list[list], tag: int, width: int, indent: str = "  ") -> list[str]:
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
                       indent=indent).splitlines()
        if keep <= 8 or max((ui.width(ln) for ln in out), default=0) <= width:
            return out
        keep -= 1


def _without_empty_tail(row: list) -> list:
    out = list(row)
    while len(out) > 1 and not str("" if out[-1] is None else out[-1]):
        out.pop()
    return out


def _latest_note(j: dict, width: int = 60) -> str:
    notes = j.get("notes") or []
    return ("note: " + _text(notes[-1]["text"].replace("\n", " "), width)) if notes else ""


def public(j: dict) -> dict:
    """A job as JSON consumers (`show/status --json`) see it: every stored field, with the job's environment
    masked.

    Fields others rely on: id (int), state, queue, tag, project, cwd, cmd (argv), runner (argv, maybe []), needs,
    prio, owner {session, cwd} or None, timeout, retries, attempt, rc, reason, seconds, log,
    notes [{t, by, text}], reported [{t, by, session}], stop {by, t, grace, timeout, sent, kill9, denied, was}
    on a job asked to stop, and from `show` also watchers [{pid, verb, session}].

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
            needs[k] = _num(v)
    needs.update(_kv(a.need, "--need", _num))
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
    retry_delay = _dur_arg(a.retry_delay, "--retry-delay") if a.retry_delay is not None else qc.get("retry_delay", RETRY_DELAY)
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
    on_fail = _hook_spec(a.on_fail if a.on_fail is not None else qc.get("on_fail"))
    on_done = _hook_spec(a.on_done if a.on_done is not None else qc.get("on_done"))
    shell_env = {k: os.environ[k] for k in CAPTURED_ENV if k in os.environ}
    state = "held" if a.hold else "queued"
    project = project_name(cwd)
    ids = []
    for c in cmds:
        job = {"queue": q, "tag": a.tag or default_tag(c), "project": project, "cwd": cwd, "cmd": c, "runner": [],
               "env": dict(env), "shell_env": shell_env, "needs": needs, "prio": a.prio, "owner": owner,
               "timeout": timeout, "retries": retries, "retry_delay": retry_delay, "retry_if": retry_if or None,
               "attempt": 1, "on_fail": on_fail, "on_done": on_done}
        if prepare_job:
            prepare_job(job, a)
        with txn() as s:
            jid = s.add(job, state)["id"]
        ids.append(jid)
        print(jid)
        extra = " ".join(x for x in (f"timeout {ui.human_duration(timeout)}" if timeout else "",
                                      f"retries {retries}" if retries else "",
                                      f"on-fail {on_fail}" if on_fail else "") if x)
        via = f"  (via {_text(os.path.basename(job['runner'][0]))})" if job["runner"] else ""
        print(f"pp {_PROG}: {state} {jid} ({q}) [{_text(project)}] p{a.prio} {_needs_str(needs)} {extra}  "
              f"{_text(shlex.join(c), 100)}{via}", file=sys.stderr)
    if not a.hold and not a.no_start:
        worker_up(q, wake=True)
    if owner:     # the pull side: an agent hears the outcome by waiting on it in the background
        some = " ".join(map(str, ids))
        print(f"pp {_PROG}: to hear how it ends, run `pp {_PROG} wait {some}` in the background (it exits with the "
              f"outcome and, on failure, the log tail), or `pp {_PROG} watch {some}` for a line per event", file=sys.stderr)
    return 0


def _scope_list(scope: list[str] | None) -> list[str]:
    """The queues a verb works on: every one there is, or the ones it was given by name, each one opened."""
    if scope is None:
        return queues()
    for q in scope:
        open_queue(q)
    return scope


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
    all_jobs = _jobs()
    # what each queue's worker is, through the one construct, so the hints that nothing launches or records
    # this queue's jobs come back from the one answer it gave
    workers = {q: worker_now(q) for q in _scope_list(scope)}
    qs = [q for q in _scope_list(scope)
          if scope is not None or any(j["queue"] == q for j in all_jobs) or workers[q].how != DOWN]
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
        data = {st: keep([j for j in mine if j["state"] == st]) for st in STATES}
        own = usage(q, q)
        w = workers[q]
        waiting = (w.rec or {}).get("waiting") if w.up else None     # only a live worker waits for anything
        per[q] = {"data": data, "wpid": w.pid, "how": w.how, "pools": own, "waiting": waiting,
                  "recent": sorted(data["done"] + data["failed"], key=lambda j: -(j.get("ended") or 0))}
    if a.json:
        flat = lambda st: [public(j) for q in qs for j in per[q]["data"][st]]   # noqa: E731
        recent = sorted((j for q in qs for j in per[q]["recent"]), key=lambda j: -(j.get("ended") or 0))
        ui.print_json({"cards": cards, "queues": qs, "workers": {q: per[q]["wpid"] for q in qs},
                       "starting": [q for q in qs if per[q]["how"] == STARTING],
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
    if scope and scope[0] not in queues() and not _configured(scope[0]):
        known = [q for q in queues() if q != scope[0]]
        print(f"no queue '{scope[0]}' yet" + (f" (there are: {', '.join(known)})" if known else ""))
    if (a.here or a.project) and not any(per[q]["data"][st] for q in qs for st in STATES):
        print(f"no jobs of project '{want}'")
    for q in qs:
        s = per[q]
        data, waiting, wpid = s["data"], s["waiting"], s["wpid"]
        up = (f"worker up (pid {wpid})" if wpid is not None else
              ui.c("worker starting" if s["how"] == STARTING else "worker down", "yellow"))
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
            if s["how"] == DOWN:
                print(ui.c(f"  nothing records how they end: `pp {_PROG} worker start -q {q}`", "yellow"))
            elif s["how"] == STARTING:
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
            if s["how"] == DOWN:
                print(ui.c(f"  nothing launches them: `pp {_PROG} worker start -q {q}`", "yellow"))
            elif s["how"] == STARTING:
                print(ui.c("  a worker is starting; it launches them", "yellow"))
        if data["held"]:
            print(ui.c("held", "bold"))
            rows = []
            for j in data["held"]:
                row = [j["id"], f"p{j.get('prio')}", _text(j.get("tag")), f"[{_text(j.get('project'))}]"]
                rows.append(row + [_latest_note(j)])
            print("\n".join(_section(rows, 2, width)))
        if s["recent"]:
            print(ui.c("recent", "bold"))
            rows = []
            for j in s["recent"][:a.n]:
                word = "done" if j["state"] == "done" else ui.c("FAILED", "red")
                rows.append([j["id"], word, f"rc={_rc(j)}", ui.human_duration(j.get("seconds")),
                             _text(j.get("tag")), f"[{_text(j.get('project'))}]", ui.human_age(j.get("ended"), now)
                             + (f"  {j['reason']}" if j.get("reason") else "") + (f"  {_latest_note(j)}" if j.get("notes") else "")])
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
    watched = [{k: w[k] for k in ("pid", "verb", "session")} for w in watchers(j)]
    if a.json:
        ui.print_json({**public(j), "watchers": watched})
        return 0
    state = j["state"] if j["state"] != "failed" else ui.c("FAILED", "red")
    head = f"job {j['id']}  ({j['queue']})  {state}"
    if j["state"] in ENDED:
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
            cur = find(j["id"])
            if not cur or cur["state"] not in LIVE:
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
            cur = find(j["id"])
            if not cur or cur["state"] in ENDED:     # a pending retry is queued, not ended: keep following
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
        with txn() as s:
            cur = s.job(jid)
            if cur is None or cur["state"] in ENDED:
                print(f"job {jid} already {cur['state'] if cur else 'gone'}")
                continue
            if cur["state"] in ("queued", "held"):
                # it went back to its queue for a retry: no process exists, so this is where it ends
                was = cur["state"]
                _finish(s, cur, "failed", None, "killed")
                print(f"killed {jid} (it was {was}, with no process to stop)  {_text(j.get('tag'))}")
                continue
            cur["stop"] = merge_stop(cur.get("stop"), {"by": _by(), "t": time.time(), "grace": float(a.grace)})
            s.save(cur)
        asked.append((j, cur["stop"]["t"]))
    for q in sorted({j["queue"] for j, _t in asked}):
        worker_up(q, wake=True)     # the queue's worker is what carries these stops out
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
        cur = find(jid)
        if cur is None:
            print(f"dropped {jid}")
            return 1
        stop = cur.get("stop") or {}
        if cur["state"] in ENDED:
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
            if worker_now(q).up:
                ui.warn(f"{said} The stop went to the {q} worker on every pass, so this wait is not the "
                        f"queue's poll period. The {q} worker's log: {_worker_log(q)}")
            else:
                ui.warn(f"{said} No worker of the {q} queue is up now, so nothing is carrying it out. The "
                        f"{q} worker's log says why none came up: {_worker_log(q)}")
            return 1
        # a pass wakes the worker that is up, so its next tick is this pass and what this waits out is the
        # job's own exit time. Where none is up, worker_up starts one at most every START_EVERY
        worker_up(j["queue"], quiet=True, wake=True)
        if time.time() > deadline:
            continue        # a round of starts took this pass past the deadline: look once more and report
        time.sleep(KILL_POLL)
    stop = cur.get("stop") or {}
    if not stop.get("sent"):
        print(f"job {jid} had already ended: {_ended_line(cur, jid)}")
    elif cur.get("rc") is not None:
        print(f"job {jid} ended on its own as it was being killed: {_ended_line(cur, jid)}")
    else:
        # the grace on the record, which is the shorter of every ask (`merge_stop`) and the one the SIGKILL
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
    for j in jobs:
        with txn() as s:
            cur = s.job(j["id"])
            if cur is None or cur["state"] not in src:
                if j["_bulk"]:
                    if verb != "drop":          # drop's plan listed them already
                        skipped.append(cur or {**j, "state": "gone"})
                    continue
                st = cur["state"] if cur else "gone"
                hint = f". `pp {_PROG} kill {j['id']}` stops it" if st == "running" else ""
                ui.warn(f"{_PROG}: job {j['id']} is {st}; {verb} works on {'/'.join(src)} jobs{hint}")
                rc = 2
                continue
            if dst is None:
                _forget_job(s, cur)         # the record, and every file that belongs to this job alone
            else:
                cur["state"] = dst
                s.save(cur)
        print(f"{done} {j['id']}  {_text(j.get('tag'))}")
        if dst == "queued":
            started.add(cur["queue"])
    _left(skipped)
    if not getattr(a, "no_start", False):          # a job added with --hold started no worker; one per queue
        for q in sorted(started):
            worker_up(q, wake=True)
    return rc


def _reprio(a) -> int:
    rc, skipped = 0, []
    for j in _resolve(a.ids, a.scope):
        with txn() as s:
            cur = s.job(j["id"])
            if cur is None or cur["state"] not in ("queued", "held"):
                if j["_bulk"]:
                    skipped.append(cur or {**j, "state": "gone"})
                    continue
                ui.warn(f"{_PROG}: job {j['id']} is {cur['state'] if cur else 'gone'}; reprio works on queued/held jobs")
                rc = 2
                continue
            cur["prio"] = a.prio
            s.save(cur)
        print(f"reprio {j['id']} -> p{a.prio}")
    _left(skipped)
    return rc


CARRY = ("queue", "tag", "project", "cwd", "cmd", "runner", "env", "shell_env", "needs", "prio", "owner", "timeout",
         "retries", "retry_delay", "retry_if", "on_fail", "on_done")


def cmd_retry(a) -> int:
    started, rc, skipped = set(), 0, []
    for j in _resolve(a.ids, a.scope):
        if j["state"] not in ENDED and j["_bulk"]:
            skipped.append(j)
            continue
        if j["state"] not in ENDED:
            ui.warn(f"{_PROG}: job {j['id']} is {j['state']}; retry re-queues ended jobs")
            rc = 2
            continue
        new = {k: j[k] for k in CARRY if k in j}
        new.update(retry_of=j["id"], attempt=1)
        if a.prio is not None:
            new["prio"] = a.prio
        with txn() as s:
            jid = s.add(new, "queued")["id"]
        print(jid)
        print(f"pp {_PROG}: retry of {j['id']} queued as {jid} ({j['queue']})", file=sys.stderr)
        started.add(j["queue"])
    _left(skipped)
    if not a.no_start:
        for q in sorted(started):
            worker_up(q, wake=True)
    return rc


def cmd_clean(a, scope: list[str] | None) -> int:
    cutoff = time.time() - _dur_arg(a.older, "--older", "d") if a.older is not None else None
    victims = [j for j in _jobs(ENDED, scope) if cutoff is None or (j.get("ended") or 0) < cutoff]
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
    removed = 0
    for batch in (victims[i:i + CLEAN_BATCH] for i in range(0, len(victims), CLEAN_BATCH)):
        with txn() as s:            # a batch at a time: a reader waits for the transaction, so it stays short
            for v in batch:
                j = s.job(v["id"], must_be=ENDED)
                if j is None:
                    continue
                _forget_job(s, j)
                removed += 1
    print(f"removed {removed}")
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
    if st in ENDED:
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
        with listing() as s:
            live = {st: len(s.jobs(st, scope)) for st in LIVE}
        say(f"[{_PROG} {verb}] {datetime.now():%H:%M:%S}: running {live['running']}, queued {live['queued']}, held {live['held']}")

    def tell(j, jid):
        say(*_outcome_lines(j, jid, a.tail))
        if j is not None:
            _stamp_reported(jid, verb)

    if a.idle:
        before = {j["id"] for j in _jobs(ENDED, scope)}
        told: set = set()
        asked: set = set()      # the queues this loop has asked for a worker: it says what holds a claim once
        with watching(None, scope, verb):
            while True:
                # one listing: the jobs still to come and the ones that ended are read together, so the last
                # look, the one that finds none live, holds every outcome there is to report
                with listing() as s:
                    live = s.jobs(("queued", "running"), scope)
                    ended = [j for j in s.jobs(ENDED, scope) if j["id"] not in before]
                if not live:
                    break
                if stream:
                    for j in sorted(ended, key=lambda j: j["id"]):
                        if j["id"] not in told:
                            told.add(j["id"])
                            tell(j, j["id"])
                if not a.no_start:      # running ones too: only a worker records how a job ended
                    for q in {j["queue"] for j in live}:
                        worker_up(q, quiet=q in asked)
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
        with listing() as s:
            targets = [j["id"] for j in s.jobs(LIVE, scope) if mine(j)] \
                + [j["id"] for j in s.jobs(ENDED, scope) if mine(j) and unheard(j)]
        need_all = True
        if not targets:
            say("nothing of this session's is queued, held or running, and every outcome was reported to it")
            return 0
    else:
        targets, need_all = [j["id"] for j in _jobs(("queued", "running"), scope)], False
        if not targets:
            say("nothing queued or running")
            return 0
    held = [j["id"] for j in (find(t) for t in targets) if j and j["state"] == "held"]
    if held:
        print(f"[{_PROG} {verb}] note: {', '.join(map(str, held))} held, so this waits forever. "
              f"`pp {_PROG} release` them", file=sys.stderr)
    with watching(targets, None, verb):
        prev: dict = {}
        told: set = set()
        asked = set()           # as above: a loop says what holds a queue's claim on its first pass only
        blamed: set = set()     # the queues this loop has said no worker came up for, which it says once
        while True:
            cur = {t: find(t) for t in targets}
            if stream:
                if not prev:
                    say(f"[{_PROG} watch] " + ", ".join(f"{t} {cur[t]['state'] if cur[t] else 'gone'}" for t in targets))
                for t in targets:
                    line = _event(prev.get(t), cur[t], t)
                    if line:
                        say(line)
                    if t not in told and (cur[t] is None or cur[t]["state"] in ENDED):
                        told.add(t)
                        tell(cur[t], t)
                prev = {t: j for t, j in cur.items() if j}
            ended = {t: j for t, j in cur.items() if j is None or j["state"] in ENDED}
            if ended and (len(ended) == len(targets) or not need_all):
                for t in sorted(ended):
                    if t not in told:
                        tell(ended[t], t)
                return 1 if any(j is None or j["state"] == "failed" for j in ended.values()) else 0
            if not a.no_start:      # a dead worker would otherwise make this wait forever, queued or running:
                for q in {j["queue"] for j in cur.values() if j and j["state"] in ("queued", "running")}:
                    quietly = stream or q in asked
                    w = worker_up(q, quiet=quietly)             # only a worker records how a job ended
                    asked.add(q)
                    if w.why and q not in blamed:
                        # `watch` asks quietly on every pass, so a queue no worker can come up for would
                        # leave it printing events forever and never the reason none launches the job
                        blamed.add(q)
                        if quietly:
                            say(f"[{_PROG} {verb}] the {q} worker did not come up: {w.why}; "
                                f"its log: {_worker_log(q)}")
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
        rows = []
        for q in qs:
            w = worker_now(q)
            rows.append({"queue": q, "pid": w.pid, "how": w.how, "log": _worker_log(q)})
        if a.json:
            ui.print_json([{**r, "up": r["pid"] is not None} for r in rows])
        elif not rows:
            print("no queues yet")
        for r in [] if a.json else rows:
            state = (f"worker up (pid {r['pid']})" if r["pid"] is not None
                     else "worker starting" if r["how"] == STARTING else "worker down")
            print(f"{r['queue']}: {state}   log {r['log']}")
        return 0
    q = q_given or default_queue
    writing_queue(q)    # `worker start -q NAME` opens that queue, so it goes through the same refusal
    logf = _worker_log(q)
    if a.action == "start":
        # one answer, from the scope that acted on it: nothing looks at the claim again, so a worker that came
        # up between two looks can never read as one that never came
        w = worker_up(q, quiet=True)
        if w.how == STARTED:
            print(f"{q}: started worker (pid {w.pid}; log {logf})")
        elif w.up:
            print(f"{q}: worker already up (pid {w.pid})")
        elif w.how == STARTING:
            ui.die(f"{_PROG}: {_starting(q)}; ask again", 1)
        else:
            # this asked quietly, so the reason the answer carries is said here: a start that timed out
            # logged nothing of its own, and that reason is the only word there is about it
            why = f": {w.why}" if w.why else ""
            ui.die(f"{_PROG}: the {q} worker did not come up{why}; its log: {logf}")
        return 0
    w = worker_now(q)           # one look, and this stop signals the worker that look found
    if w.how == STARTING:       # there is nothing to signal yet: it has not recorded itself
        ui.die(f"{_PROG}: {_starting(q)}; ask again", 1)
    if not w.up:                # nothing holds the claim: there is no worker, whatever is written in it
        print(f"{q}: worker down")
        return 0
    h = w.handle        # the handle this one look gave: no second look decides what is signalled
    pid = h.id
    outcome = proc.signal_pid(h, signal.SIGTERM)
    if outcome == "gone":           # it exited since we looked, or that pid is another process now
        print(f"{q}: worker down")
        return 0
    if outcome == "denied":
        ui.die(f"{_PROG}: not allowed to stop the {q} worker (pid {pid}); it belongs to another user", 1)
    # it stops between ticks, and one tick can wait on a probe (up to 60s each) or nvidia-smi; it is down when
    # the claim is no longer its own, which the kernel sees to even if it never reaches the end of its own stop.
    # What this waits for is that worker, not a free claim: an `add` or a background `wait` starts the next
    # worker as soon as the claim is free, and this stop is over when the one it signalled has let it go.
    def it_still_holds() -> bool:
        """Whether the worker this stop signalled still holds the claim, asked through the one construct. A
        record naming another worker, and nothing holding the claim, are both that worker gone."""
        held = worker_now(q).rec or {}
        return (held.get("pid"), held.get("pstart")) == (h.id, h.start)

    start, said = time.time(), False
    while it_still_holds():
        if not said and time.time() - start > 10:
            print(f"{q}: worker {pid} is finishing its tick (a probe or nvidia-smi); waiting", flush=True)
            said = True
        if time.time() - start > STOP_WAIT:
            ui.die(f"{_PROG}: the {q} worker ({pid}) did not stop within {ui.human_duration(STOP_WAIT)}; its log: {logf}")
        time.sleep(0.1)
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
    options to `add`."""
    global _PROG
    _PROG = prog
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
    if q is not None and (not NAME_RE.fullmatch(q) or q in RESERVED):
        ui.die(f"{prog}: '{q}' is not a queue name (lowercase letters, digits, - and _; not {' or '.join(RESERVED)})", 2)
    scope = a.scope = [q] if q else None
    if q is not None:
        open_queue(q)       # a queue named with -q is opened once here, whatever the verb does with it
    if a.verb == "add":
        if cmd and a.cmd:
            ui.die(f"{prog} add: put the whole command after --", 2)
        return cmd_add(a, cmd or a.cmd, q or default_queue, prepare_job)
    if a.verb in ("status", "ls"):
        return cmd_status(a, scope)
    verbs = {"show": cmd_show, "note": cmd_note, "log": cmd_log, "kill": cmd_kill, "reprio": _reprio, "retry": cmd_retry}
    if a.verb in verbs:
        return verbs[a.verb](a)
    if a.verb == "drop":
        return _transition(a, ("queued", "held"), None, "drop", "dropped")
    if a.verb == "hold":
        return _transition(a, ("queued",), "held", "hold", "held")
    if a.verb == "release":
        return _transition(a, ("held",), "queued", "release", "released")
    if a.verb == "clean":
        return cmd_clean(a, scope)
    if a.verb in ("wait", "watch"):
        return cmd_wait(a, scope, stream=a.verb == "watch")
    if a.verb == "work":
        return work(q or default_queue, a.backfill, a.poll)
    if a.verb == "worker":
        return cmd_worker(a, q, default_queue, scope)
    return 2
