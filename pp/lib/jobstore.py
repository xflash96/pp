"""The record store behind `pp queue`: one file per job, and the transaction that changes one.

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

One lock, and no timing, no process and no signal: this module reads and writes records, and that is all it
does. Reading and writing one goes through txn() and listing(), or through find() for one record. The writes
a verb asks for are the four at the end of the file, each one a whole transaction of its own, so no caller
holds a scope open across its own work (pp/lib/jobcore.py, which is the one door to them).

It words nothing: a queue another layout wrote is refused as `Foreign`, and the caller says the sentence.
"""
from __future__ import annotations

import json
import os
import re
import time
from contextlib import contextmanager

from pp.lib import jobpolicy as policy, lock, paths

STATES = ("queued", "held", "running", "done", "failed")
LIVE = ("queued", "held", "running")
ENDED = ("done", "failed")
NAME_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")
RECORD = re.compile(r"\d+\.json")     # what a job record is called: jobs/<id>.json, and nothing else is one
RESERVED = ("probes", "watchers")     # directories beside the queues
FORMAT = 1                  # what a queue's FORMAT file holds: the layout of everything under queue/<name>/
CLEAN_BATCH = 100           # job records `forget` removes in one transaction, so a reader never waits long


class Foreign(SystemExit):
    """Queue q holds jobs this pp cannot read: its name, and the format its FORMAT file says, or None where it
    says none.

    It is a refusal, so it carries the status one exits with; the sentence is the caller's to say, since the
    store writes none (pp/lib/jobq.py, run_cli)."""

    def __init__(self, queue: str, got: int | None):
        super().__init__(2)
        self.queue, self.got = queue, got


# One file per job: jobs/<id>.json, with every field in it, `state` among them. A change of state replaces that
# one file, so a reader sees the job as it was before the change or as it is after it, never in two states and
# never in none.

def top(*sub: str, create: bool = True) -> str:
    return paths.state_dir("queue", *sub, create=create)


def qroot(q: str, *sub: str, create: bool = True) -> str:
    return paths.state_dir("queue", q, *sub, create=create)


def queues() -> list[str]:
    """Every queue that exists on disk, each one opened through open_queue."""
    d = top(create=False)
    try:
        names = os.listdir(d)
    except OSError:
        return []
    found = sorted(n for n in names if NAME_RE.fullmatch(n) and os.path.isdir(os.path.join(d, n, "jobs")))
    for n in found:
        open_queue(n)
    return found


def _format_file(q: str, create: bool = False) -> str:
    return os.path.join(qroot(q, create=create), "FORMAT")


def _format_of(q: str) -> int | None:
    """The number in queue q's FORMAT file, or None when it holds none."""
    try:
        with open(_format_file(q)) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def open_queue(q: str) -> str:
    """Queue q's directory, with the format of its store checked. The one place a queue is opened, so the one
    place that refuses a queue this pp cannot read.

    Every path that opens a queue comes through here: queues() for each queue it finds, the CLI for a queue it
    is given by name, and work() and `worker start` for the queue they fall back to. A queue whose jobs another
    pp wrote is refused rather than read, since this pp would report records it cannot see as gone, and launch
    jobs beside ones that are still running. The refusal is `Foreign`, which the caller words."""
    qdir = qroot(q, create=False)
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
    raise Foreign(q, got)


def writing_queue(q: str) -> None:
    """Queue q, opened and ready to be written in: it exists, and it says what format its store is in. Every
    writer that may be the one creating a queue's directory comes through here, so a queue that exists says
    its format whether or not it holds a job.

    The directory it opens is bound by no caller, so it hands back nothing, and the refusal open_queue raises
    still comes first."""
    open_queue(q)
    path = _format_file(q, create=True)
    if not os.path.exists(path):
        paths.write_json(path, FORMAT)


def _store_lock() -> str:
    return os.path.join(top(), ".lock")


def _counter_file() -> str:
    return os.path.join(top(), "counter")


def _jpath(q: str, jid: int, create: bool = False) -> str:
    """Where job jid's record is in queue q. create: make the queue's directories, which only a writer does, so
    a read of a job that is not there leaves nothing behind."""
    return os.path.join(qroot(q, "jobs", create=create), f"{jid}.json")


def _job_ids(q: str) -> list[int]:
    """The ids in queue q, from one pass over its directory."""
    try:
        names = os.listdir(qroot(q, "jobs", create=False))
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
        tmp = paths.staging(qroot(q, "jobs"), "job")
        n = _counter()
        try:
            while True:
                n += 1
                if any(os.path.exists(_jpath(x, n)) for x in others):
                    continue
                job.update(id=n, state=state, log=os.path.join(qroot(q, "logs"), f"{n}.log"), added=time.time())
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


def jobs(state=(), queue=None) -> list[dict]:
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



def run_claim(q: str, jid: int) -> str:
    """The file whose claim one run of job jid holds. Taking it is how a worker says it is starting that job,
    and the job's own wrapper holds it from then on, so a held claim means a run of this job is live."""
    return os.path.join(qroot(q, "jobs"), f"{jid}.run")



def _forget_job(s: Txn, job: dict) -> None:
    """In a transaction: take a job's record away, and with it every file that belongs to that job alone.

    The record goes first, so no reader finds a job whose files are gone. Then the exit code each attempt
    wrote, the job's log, the log of each hook that ran for it, and the claim its runs held. Only files under
    the queue's own directories, never one pp did not write. What `drop` and `clean` both take away, so
    neither leaves a file behind that nothing names any more: once the record is gone, nothing names them."""
    q, jid = job["queue"], job["id"]
    files = [p for p in [job.get("rcfile")] + [h.get("rcfile") for h in job.get("history") or []]
             if p and os.path.dirname(p) == qroot(q, "rc", create=False)]
    if job.get("log") and os.path.dirname(job["log"]) == qroot(q, "logs", create=False):
        files.append(job["log"])
    # a hook's log is named after the job and the event, so both names are this job's whether or not the
    # record got as far as naming them (a hook that could not start wrote its first line and no more)
    files += [os.path.join(qroot(q, "hooks", create=False), f"{jid}-{e}.log") for e in ("failed", "done")]
    s.remove(job)
    for path in files:
        try:
            os.remove(path)
        except OSError:
            pass
    # its record is gone and its run is over, so nothing holds this claim and this id never takes that name again
    lock.Claim.forget(run_claim(q, jid))



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


def _requeue(s: Txn, job: dict, a: policy.Action) -> dict:
    """In a transaction: the job goes back to its queue for the attempt the action names, with the same id.
    -> what the caller words: the attempt that ended, how many there are, why it ended, the attempt to come
    and how long until it may start."""
    attempt, retries = int(job.get("attempt", 1)), int(job.get("retries") or 0)
    job.setdefault("history", []).append({"attempt": attempt, "rc": a.rc, "reason": a.reason,
                                          "started": job.get("started"), "ended": a.ended,
                                          "seconds": a.ended - (job.get("started") or a.ended),
                                          "rcfile": job.get("rcfile")})
    for k in ("pid", "pgid", "pstart", "boot", "started", "gpu", "stop", "rcfile"):
        job.pop(k, None)            # this attempt's, including the file it wrote its code to
    job.update(attempt=a.attempt, not_before=a.not_before, state="queued")
    s.save(job)
    return {"attempt": attempt, "of": retries + 1, "why": a.why, "next": a.attempt,
            "delay": a.not_before - a.ended}


# ---- the writes a verb hands over --------------------------------------------------------------------
#
# Each one is a whole transaction of its own, told the states it may act on, so no caller holds a scope open
# across its own work and none of them acts on a copy that has gone stale. They word nothing: each hands back
# the records it wrote and the ones it left, and the caller says what happened.

def add_jobs(records: list[dict], state: str) -> list[int]:
    """n records, n ids, one scope: each one stored in its queue with its id, state, log and added filled in.
    -> the ids, in the order the records came."""
    with txn() as s:
        return [s.add(r, state)["id"] for r in records]


def amend(jid: int, changes: dict, must_be: tuple = ()) -> dict | None:
    """One job with these changes on it: a priority, a note, a stamp that an outcome was reported. None where
    there is no such job, and where must_be names the states it may be in and it is in another.

    A list is appended to what the record holds; anything else replaces it. So a note and a stamp are one
    write each, and neither reads the record outside the scope that writes it."""
    with txn() as s:
        j = s.job(jid, must_be=must_be)
        if j is None:
            return None
        for k, v in changes.items():
            j[k] = (j.get(k) or []) + v if isinstance(v, list) else v
        s.save(j)
        return j


def _written(selection: list[dict], must_be: tuple, write, batch: int = 1) -> tuple[list[dict], list[dict]]:
    """Each job of the selection that the store holds in one of must_be, written by `write` inside a scope
    that holds `batch` of them at most, since a reader waits for the scope.

    -> the records it wrote, and for each one it did not, the record as the store holds it now (or the one
    handed in, as `gone`, where the store holds none). So the caller words what happened and reads no record
    outside the scope that wrote it."""
    done, left = [], []
    for i in range(0, len(selection), batch):
        with txn() as s:
            for j in selection[i:i + batch]:
                cur = s.job(j["id"], must_be=must_be)
                if cur is None:
                    left.append(s.job(j["id"]) or {**j, "state": "gone"})
                    continue
                write(s, cur)
                done.append(dict(cur))
    return done, left


def set_state(selection: list[dict], src: tuple, dst: str) -> tuple[list[dict], list[dict]]:
    """hold and release: each job of the selection that the store holds in one of src becomes dst, in a
    transaction of its own. -> as `_written`."""
    return _written(selection, src, lambda s, cur: (cur.update(state=dst), s.save(cur)))


def forget(selection: list[dict], must_be: tuple = ()) -> tuple[list[dict], list[dict]]:
    """drop and clean: each job's record, and with it every file that belongs to that job alone. -> as
    `_written`, CLEAN_BATCH records to a scope."""
    return _written(selection, must_be, _forget_job, CLEAN_BATCH)
