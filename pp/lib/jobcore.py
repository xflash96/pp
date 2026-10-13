"""The worker and the claims behind `pp queue`: the one actor that starts, stops, reaps and adopts a job.

This is the module where a bug loses a job rather than a file. It holds the claims (a job's run, a queue's
worker, a wait's lease), the handles a signal goes through, the worker's own loop and the one construct that
says what a queue's worker is. Every record it reads or writes goes through pp/lib/jobstore.py, and every rule
it carries out is a value pp/lib/jobpolicy.py gave it.

One worker per queue reaps ended jobs, carries out the stops asked of them, re-queues retries, starts what
fits, and fires the job's on_fail / on_done hook on its FINAL outcome. Admission is strict head-of-line by
priority (a big job is never starved by small ones) unless the worker runs with backfill; a job waiting out
its retry delay is skipped, not waited for.

A queue's worker is the one actor that signals that queue's jobs, and only a worker ever holds the queue's
worker claim. `ask_stop` writes a stop on the job ({by, t, grace}) and the queue's worker carries it out. A
timeout is the worker writing a stop on the job itself. So a kill, a timeout and a reap never cross, and one
actor records how a job ended.

Being alive is holding a claim (pp/lib/lock.py), never a pid in a file: the queue's worker holds the claim on
worker.lock while it runs, and one run of a job is held by that run's own wrapper. The kernel lets a claim go
when its holder ends, however it ends, so a worker or a job that was killed leaves nothing to clean up and
nothing that reads as alive.

What the outside may call is this file's own API: the reads below, the writes each of which is one whole
transaction, `worker_up`, `worker_stop`, `work` and `watching`. It reads no config file and takes no reading
of its own: a worker's settings arrive through `read_settings` and its admission through `admit_for`. It
writes no sentence: it tells a sink what it did, and the caller words it.
"""
from __future__ import annotations

import os
import secrets
import select
import shlex
import signal
import time
import traceback
from contextlib import contextmanager
from datetime import datetime

from pp.lib import config, jobpolicy as policy, jobstore, lock, paths, proc, session

# What a job runs as. Its stdin is the job's run claim, which this `sh` holds for exactly as long as the attempt
# lives; it writes its own pid in there first, so the claim names it even if the worker that started it died
# before recording it. The command itself reads /dev/null, so neither it nor anything it leaves behind holds the
# claim: the attempt is over when this `sh` is over. The exit code goes to another name and is renamed into
# place, so whoever reads it finds a whole code or none.
WRAP = ('printf \'{"pid": %d}\' "$$" >&0; "$@" 0</dev/null; r=$?; '
        'echo "$r" > "$PP_QUEUE_RCFILE.part" && mv "$PP_QUEUE_RCFILE.part" "$PP_QUEUE_RCFILE"')
WAKE = signal.SIGUSR1       # what a verb sends a worker to end its wait between ticks, so a stop is served now
START_EVERY = 5.0           # how often one process starts a worker again for a queue whose last round brought none up
STOP_WAIT = 300.0           # how long `worker_stop` waits for a worker inside a slow tick
SLOW_TICK = 10.0            # ... and how long before it says it is waiting on one
PROG = "queue"              # the command whose worker this starts, which its CLI names (jobq.run_cli)

# the reads the outside may take: one listing, one record, the queues there are, and the FORMAT gate
jobs, find, queues, open_queue, writing_queue = (jobstore.jobs, jobstore.find, jobstore.queues,
                                                 jobstore.open_queue, jobstore.writing_queue)
# the writes the outside hands over, each one a whole transaction of its own that is told the states it may
# act on: n records and n ids, a priority or a note or a stamp, a change of state, and a record and its files
add_jobs, amend, set_state, forget = jobstore.add_jobs, jobstore.amend, jobstore.set_state, jobstore.forget


def _new_rcfile(j: dict) -> str:
    """The file this attempt of job j writes its exit code to. One name per attempt, chosen as the attempt
    starts and kept on the record, so no attempt can read the code another wrote and nothing has to be removed
    before a launch."""
    return os.path.join(jobstore.qroot(j["queue"], "rc"), f"{j['id']}.{j.get('attempt', 1)}.{secrets.token_hex(4)}")


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


def _group_handle(j: dict) -> proc.Handle | None:
    """The job's process group, as the handle every signal to it goes through: the group id, the start time
    recorded with it, and the boot both were read in.

    None while the job has no process yet, for a record that holds no start time, and for a record from
    another boot, which names no process here. `Worker._signal_group` then falls back to the pid in the job's
    run claim, which is held by a live process and so is always this boot's."""
    if j.get("boot") != proc.boot_id():
        return None
    return proc.Handle.recorded(j.get("pgid"), j.get("pstart"), group=True)


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

    def __init__(self, queue: str, backfill: bool = False, sink=None, tail=None, admit_for=None):
        me = proc.read(os.getpid())
        self.queue = queue
        self.children: dict[int, proc.Child] = {}    # jobs this worker launched; others are adopted
        self.hooks: list[proc.Child] = []            # kept only to reap them
        self.stops: dict[int, proc.Child] = {}       # a stop command per job: reaped onto that stop's rc
        self.backfill = backfill
        self.pstart = me.start if me else None
        self.waiting: dict | None = None    # {id, tag, why, t}: what the head of the queue waits for
        self.said = None                    # (id, reason) last logged, without the detail after ';'
        self.serving = 0        # stops of this queue in flight at the last tick: while any is, the wait is short
        self.claim: lock.Claim | None = None    # where it records itself; work() sets it once it has taken it
        # what this worker is handed, because none of it is the core's: the sink it says what it did to, the
        # tail of a job's log that --retry-if matches against, and the readings of one launch pass. The
        # defaults say nothing, match nothing and provide nothing, so a worker nobody wired launches nothing
        self.say = sink or (lambda kind, job, fields: None)
        self.tail = tail or (lambda job, n: "")
        self.admit_for = admit_for or (lambda kinds: {"by_name": {}, "q": self.queue})
        self.stopping = lambda: False   # work() points it at its stop flag: a stopping worker launches nothing new

    def record(self) -> dict:
        """What this worker says about itself while it holds its claim, which is where `status` reads it."""
        return {"pid": os.getpid(), "pstart": self.pstart, "waiting": self.waiting}

    def _publish(self):
        if self.claim is not None:
            self.claim.write(self.record())

    def tick(self):
        self.carry_out()        # the one actor that signals this queue's jobs and records how one ended
        watchers()              # and the leases of the waits a signal ended, swept where the queue is read
        self.hooks = [h for h in self.hooks if h.poll() is None]
        self._reap_stops()      # and the exit code each stop command of its own had
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
        with jobstore.listing() as s:    # one pass: the jobs to decide about, and the hooks the queue still owes
            jobs = list(s.jobs(("running",) + jobstore.ENDED, self.queue))
        owed = [j for j in jobs if j["state"] in jobstore.ENDED and j.get("hook_due")]
        # while a stop is in flight, and a verb is waking this worker for it, the loop's wait is STOP_PERIOD
        # and not the poll (work()); `policy.in_flight` is what one of them is, over the record this pass
        # listed and the action it came to
        self.serving = sum(policy.in_flight(j, self._step(j, self._facts(j, now)))
                           for j in jobs if j["state"] == "running")
        for j in owed:
            self.fire(j, j["hook_due"])     # owed by a worker that did not get to fire it
        # a child of this worker's whose record no longer says running: something outside pp ended the job
        # while it was starting, and `_spawn` stopped the process it had started. Reap the zombie
        for jid, p in list(self.children.items()):
            if p.poll() is not None and (jobstore.find(jid) or {}).get("state") != "running":
                del self.children[jid]

    def _facts(self, j: dict, now: float) -> policy.Facts:
        """What is true of this running job besides its record, all of it read here and nothing decided:
        whether a run of it is live, the exit code that run wrote and when, the signal its own child died of,
        and the log tail --retry-if matches against.

        Read before the transaction, since none of it is store state and a log tail can be long."""
        held = lock.Claim.holder(jobstore.run_claim(self.queue, j["id"]))
        p = self.children.get(j["id"])
        if held is not None or (p is not None and p.poll() is None):
            # a run of it is live, whatever its record holds: it is never lost, and never started again. Its
            # exit code is not read at all here: while the run is live it says nothing (Facts). A wrapper this
            # worker started is live until this worker reaps it: a process closes its files, the claim among
            # them, before its parent can read how it ended.
            if held is not None:
                self._adopt(j, held)
            return policy.facts(True, now=now)
        self.children.pop(j["id"], None)
        rc, ended = ended_on_its_own(j) or (None, None)
        return policy.facts(False, p.poll() if p is not None else None, rc, ended, now=now,
                            tail=self.tail(j, 200) if policy.wants_tail(j, rc) else "")

    def _step(self, j: dict, facts: policy.Facts) -> policy.Action:
        """The one decision about one job, and the action it names, carried out: what is store state in one
        transaction, the signal through the job's Handle, the log line and the hook after the scope.
        -> the action, which says whether a stop of this job is still in flight.

        The decision is made inside the transaction, on the record the store holds and the facts read before
        it: a stop another actor wrote since those facts were read is part of it, so a job a kill has touched
        is never retried past its stop."""
        jid = j["id"]
        with jobstore.txn() as s:
            cur = s.job(jid, must_be=("running",))
            if cur is None:
                return policy.Action(policy.NOTHING)      # nothing else records a running job: there is nothing to record
            a = policy.decide(cur, facts)
            was = dict(cur)     # this attempt's own names, which a requeue below takes off the record
            if a.sweep:
                # the leader of this job's group is gone and its run is over, so anything left in that group
                # goes too: a command that held the signal back does not outlive the job
                self._signal_group(cur, signal.SIGKILL)
            if a.do == policy.TIME_UP:
                cur["stop"] = {"by": "timeout", "timeout": True, "t": a.t, "grace": a.grace}
                s.save(cur)
            elif a.do == policy.AGAIN:
                jobstore._requeue(s, cur, a)
            elif a.do == policy.ENDED_AS:
                jobstore._finish(s, cur, a.state, a.rc, a.reason, a.ended, a.not_retried, hook=a.hook)
        if cmd := policy.swept_stop(was, a):
            # after the scope, since starting a process in one would make every reader of the store wait for
            # it: nobody asked for this stop, so nothing is recorded for it and no outcome rests on it
            self.hooks += [h for h in [self._stop(was, cmd)] if h is not None]
        if a.do in (policy.TERM, policy.KILL9):
            self._send(cur, a)
            return a
        if a.do == policy.TIME_UP:
            self.say("timeout", cur, {})
        elif a.do == policy.AGAIN:
            self.say("again", cur, {})      # after the scope: the line it writes in the job's log is not the store
        elif a.do == policy.ENDED_AS:
            self.say("ended", cur, {})
            if a.hook:
                self.fire(cur, a.hook)
        return a

    def _send(self, job: dict, a: policy.Action) -> None:
        """The signal one stop asks for next, and what it did on the record: a SIGTERM marked `sent`, a
        SIGKILL marked `kill9`, a refusal marked `denied`. The queue's worker is the one actor that signals a
        job's processes, so a kill, a timeout and a reap never cross.

        The signal goes out first and is recorded after, so a record that says a signal went out is one that
        did, which is all the decision about this job has to go on."""
        sig, mark = (signal.SIGTERM, "sent") if a.do == policy.TERM else (signal.SIGKILL, "kill9")
        # the project's command in place of the SIGTERM, where the job carries one. pp's own grace and SIGKILL
        # follow it either way, and a command that could not start sent nothing: pp's own signal goes out for
        # that stop instead, so the job is left no worse off than 0.1.1
        cmd = policy.stop_command(job) if a.do == policy.TERM else None
        h = self._stop(job, cmd) if cmd else None
        self.stops.update({job["id"]: h} if h is not None else {})   # the worker reaps it onto this stop
        outcome = "alive" if h is not None else self._signal_group(job, sig)
        if outcome is None or outcome == "gone":
            return          # nothing to signal: its run is starting, or it is over and the next pass says how
        now = time.time()
        with jobstore.txn() as s:
            cur = s.job(job["id"], must_be=("running",))
            if cur is None or not cur.get("stop"):
                return
            cur["stop"]["denied" if outcome == "denied" else mark] = now
            if cmd and h is None:
                cur["stop"]["rc"] = 127     # the code a shell gives a command it could not run
            s.save(cur)
        self.say("stop", job, {"outcome": outcome, "sig": sig, "cmd": cmd if h is not None else None,
                               "grace": (job.get("stop") or {}).get("grace"), "group": cur.get("pgid")})

    def _stop(self, job: dict, cmd: str) -> proc.Child | None:
        """The project's stop command, started detached in the job's directory with the job's own environment,
        its output appended to the job's log: the one thing that reaches work the runner started elsewhere.
        -> the child, so its caller says what becomes of its exit code, or None where it could not start.

        It goes out in place of the SIGTERM to the job's group, and pp's own grace and SIGKILL follow it
        unchanged. It runs through the construct a hook uses, so it cannot hold up the one worker that serves
        the whole queue."""
        try:
            with open(job["log"], "a") as f:
                return proc.spawn(["/bin/sh", "-c", cmd], group="own", stdin="none", env="scrubbed",
                                  add_env=policy.job_env(job), cwd=job["cwd"], stdout=f, stderr=proc.STDOUT)
        except OSError as e:
            self.say("stop-failed", job, {"cmd": cmd, "error": str(e)})
            return None

    def _reap_stops(self):
        """The exit code each stop command of this worker's had, onto the stop it carried out, once it has
        ended. A stop pp has not seen end keeps `rc` null, so null never reads as "it worked"."""
        for jid in [jid for jid, h in self.stops.items() if h.poll() is not None]:
            h = self.stops.pop(jid)
            with jobstore.txn() as s:
                cur = s.job(jid)
                if cur and cur.get("stop"):
                    cur["stop"]["rc"] = h.poll()
                    s.save(cur)

    def _adopt(self, j: dict, held: dict):
        """Complete the record of a running job from its claim. The wrapper writes its own pid in there, so a
        job whose worker died between the start and the record is named again, and can be timed out, killed and
        reported like any other. The claim is held, so that pid is that wrapper's."""
        if j.get("pid") or not held.get("pid"):
            return
        p = proc.read(int(held["pid"]))
        if p is None:
            return              # it ended as this was read; the next look reaps it
        with jobstore.txn() as s:
            cur = s.job(j["id"], must_be=("running",))
            if cur is None or cur.get("pid"):
                return
            cur.update(pid=p.pid, pgid=p.pgid, pstart=p.start, boot=proc.boot_id())
            s.save(cur)
        self.say("adopted", cur, {"pid": p.pid})

    def fire(self, job: dict, event: str):
        """Run the job's on_fail / on_done hook, detached; record it on the job, and that the job no longer
        owes it.

        The outcome and the hook owed for it are written in one transaction (`_finish`); `hook_due` is cleared
        here, once the hook has run. A hook that could not start is reported and cleared too, so a queue is
        never left trying to start it again every tick."""
        spec = policy.hook_spec(job.get("on_fail" if event == "failed" else "on_done"))
        argv = policy.hook_argv(spec)
        q, jid = job["queue"], job["id"]
        hlog = os.path.join(jobstore.qroot(q, "hooks"), f"{jid}-{event}.log")
        ran = False
        if argv:
            env = policy.hook_env(job, event)
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
                self.say("hook-failed", job, {"event": event, "error": str(e)})
        with jobstore.txn() as s:        # it has run: the job owes it no longer, whether or not it could start
            cur = s.job(jid)
            if cur is not None:
                if ran:
                    cur.setdefault("hooks", []).append({"event": event, "cmd": spec, "t": time.time(), "log": hlog})
                if cur.pop("hook_due", None) is not None or ran:
                    s.save(cur)
        if ran:
            self.say("hook", job, {"event": event, "cmd": spec, "log": hlog})

    # -- stopping a job: a `kill`, and a timeout, which is a stop the worker writes on the job itself

    @staticmethod
    def wake(h: proc.Handle | None) -> None:
        """End the wait of the worker this handle names, so it ticks now instead of when its period is up. The
        handle comes from the look `worker_up` has already taken (`_handle`), so waking a worker costs no
        second look at the claim.

        The wake is a signal to a worker, never to a job: a worker inside `waking` takes it from its own
        wakeup pipe, so one sent while a tick runs ends the next wait rather than being lost. It is the
        worker's own, like the signalling above it, which is why it lives here and not in the verb that
        sends it. No verb holds the worker claim: a verb writes its stop, wakes the worker or starts one, and
        waits for the record."""
        if h is not None:
            proc.signal_pid(h, WAKE)

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
            held = lock.Claim.holder(jobstore.run_claim(job["queue"], job["id"])) or {}
            h = proc.Handle.live(int(held["pid"]), group=True) if held.get("pid") else None
        if h is None:
            return None
        outcome = proc.signal_group(h, sig)
        if outcome == "gone":
            self.say("group-gone", job, {"group": h.id})
        return outcome

    # -- launching

    def launch(self):
        now = time.time()
        queued = jobstore.jobs("queued", self.queue)
        if not policy.ready(queued, now):   # a retry's delay never blocks the line, and nothing else is due
            self._note(None)
            return
        seen = self.admit_for({k for j in policy.ready(queued, now) for k in (j.get("needs") or {})})
        if self.stopping():
            return          # told to stop while the readings were taken
        taken: list = []        # (record, card, claim) for each job this pass admitted
        p = policy.Pass()
        try:
            # one transaction for the whole pass: it reads the running jobs it admits against once, and reads
            # each job it is about to start again in that same scope, since another queue's worker may be
            # admitting from a shared pool and a verb may have changed the job since it was listed (prio, a note)
            with jobstore.txn() as s:
                here = [cur for j in queued if (cur := s.job(j["id"], must_be=("queued",))) is not None]
                p = policy.admit({"queued": here, "running": s.jobs("running")}, seen,
                                 {"backfill": self.backfill, "now": now})
                for cur, card in p.starts:
                    # the run claim before the record says running, so a reader never sees a running job with
                    # nothing holding its claim unless its run has ended. Taking one is two calls that never
                    # wait, which is why it belongs in here; nothing under the store lock may wait either.
                    claim = lock.Claim.take(jobstore.run_claim(self.queue, cur["id"]))
                    if claim is None:
                        continue        # a run of this job is live already
                    taken.append((cur, card, claim))    # before the rest of it, which can raise
                    self._claim(s, cur, card)
            for cur, card, claim in taken:  # outside the transaction: starting a process is not store work
                self._spawn(cur, card, claim)
        finally:
            # pp's own copy of every claim this pass took, however it ended: a wrapper that was started holds
            # the claim through the file description it was given, so releasing this copy frees nothing of
            # its; one for a job no `_spawn` reached is freed here, and that job is reaped like any other
            # whose run is over. Releasing twice is not an error.
            for _cur, _card, claim in taken:
                claim.release()
        self._note(*p.blocked)

    @staticmethod
    def _claim(s: jobstore.Txn, j: dict, card: int | None):
        """In a transaction: the job, as read in it, becomes running, with the names and files this attempt
        alone has: where it writes its exit code, where its runner reads its environment, the name whatever
        that runner starts elsewhere carries, and the cards pp reserved for it. Its run claim is held before
        this, so no reader ever sees it running with nothing holding its claim unless its run has ended."""
        rc = _new_rcfile(j)
        j.update(started=time.time(), cards=[] if card is None else [card], state="running", rcfile=rc,
                 envfile=rc + ".env", container=policy.container(j))
        j.pop("not_before", None)
        s.save(j)

    def _spawn(self, j: dict, card: int | None, claim: lock.Claim) -> dict | None:
        """Start the attempt's wrapper, and hand it the run claim as its stdin: it holds that claim for exactly
        as long as the attempt lives. pp closes its own copy here, so nothing but the wrapper holds it.

        Whatever stopped this start is this job's launch failure, recorded on it: a directory that is gone, a
        log that cannot be opened, an environment name the kernel will not take. The pass that got here has
        marked every job it admitted `running` already, so one job that cannot start must not end the pass
        for the others: they would be left with no process, and read as lost."""
        jid = j["id"]
        argv = list(j.get("runner") or []) + list(j["cmd"])
        try:
            # the environment file the runner is handed, readable by this user alone, before anything starts
            paths.write_text(j["envfile"], policy.env_file(j), mode=0o600)
            os.makedirs(os.path.dirname(j["log"]), exist_ok=True)
            with open(j["log"], "a") as f:
                f.write(policy.banner(j, shlex.join(argv), f"{datetime.now():%Y-%m-%d %H:%M:%S}"))
                f.flush()
                at = f.tell()   # where this attempt's own output begins, which is what --retry-if reads from
                p = proc.spawn(["/bin/sh", "-c", WRAP, "pp-queue-job", *argv], group="own", stdin=claim.fd,
                               env="scrubbed", add_env=policy.job_env(j), cwd=j["cwd"], stdout=f,
                               stderr=proc.STDOUT)
        except Exception as e:
            with jobstore.txn() as s:
                cur = s.job(jid, must_be=("running",))
                if cur:
                    jobstore._finish(s, cur, "failed", None, f"launch failed: {e}", hook=policy.hook_due(cur, "failed"))
            claim.release()     # after the record says it ended, so no reader sees it running with a free claim
            self.say("launch-failed", j, {"error": str(e)})
            if cur:
                self.fire(cur, "failed")
            return None
        claim.release()         # pp's own copy of it; the wrapper's keeps the claim
        with jobstore.txn() as s:        # amend the record as it is now, not the copy launch read: a note added meanwhile stays
            cur = s.job(jid, must_be=("running",))
            if cur:
                cur.update(pid=p.pid, pgid=p.pid, pstart=p.handle.start if p.handle else None,
                           boot=proc.boot_id(), log_at=at)
                s.save(cur)
        if cur is None:
            # its record no longer says running, which only something outside pp can do to a job this worker is
            # starting; its process must not run on under a record that has ended
            p.end(signal.SIGTERM)
            self.say("ended-elsewhere", j, {"pid": p.pid})
        self.children[jid] = p
        self.say("start", j, {"card": card})
        return cur

    def _note(self, j: dict | None, why: str = ""):
        """Record what the head of the queue waits for, in this worker's own claim where `status` reads it,
        whenever it changes; log it only when its reason changes, not its detail after ';'."""
        note = policy.noting(self.waiting, self.said, j, why, time.time())
        if not note.change:
            return              # the same job, waiting for the same thing
        self.waiting = note.rec
        self._publish()
        if note.say:
            self.say("waiting", j, {"why": why})
        self.said = note.key


def _worker_claim(q: str) -> str:
    return os.path.join(jobstore.qroot(q, create=False), "worker.lock")


def worker_log(q: str, create: bool = False) -> str:
    """Where queue q's worker writes what it does. Naming it creates nothing: a verb that only reports
    (`worker status`) leaves no directory behind for a queue name that was a typo. create: the worker
    about to open it for writing makes the queue's directories."""
    return os.path.join(jobstore.qroot(q, create=create), "worker.log")


def worker(q: str) -> dict | None:
    """Queue q's live worker as it records itself: {pid, pstart, waiting}. None when the queue has no live
    worker.

    Only a worker ever holds the claim on worker.lock, and it holds it for as long as it runs: that is what
    being up means. A worker that was killed leaves it free at once, and one that is alive holds it however
    long a tick takes. The record is what it is doing, written under the claim. A record that names no live
    process is a worker in its first moment, which has not recorded itself yet (`worker_now`). Asking never
    waits for it.

    `worker_now` is the one place that asks this, and every verb asks `worker_now` or `worker_up`."""
    return lock.Claim.holder(_worker_claim(q))


def _start_worker(q: str) -> proc.Child:
    """A detached worker for q, with no session identity in its environment (the caller may be an agent), once
    it is ready: it says so when it holds its claim, has recorded itself and handles a stop."""
    with open(worker_log(q, create=True), "a") as log:
        return proc.spawn(paths.self_cmd() + [PROG, "work", "-q", q], group="own", stdin="none", env="scrubbed",
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

# what one look at the claim says, and the value it comes to, which is the one thing every verb prints from
UP, STARTED, DOWN, STARTING = policy.UP, policy.STARTED, policy.DOWN, policy.STARTING
Holder = policy.Holder

WORKER_STARTS = 3       # workers `worker_up` starts at most, each one asked about after it
STARTING_LOOKS = 25     # looks it takes at a claim whose holder has not recorded itself ...
STARTING_PAUSE = 0.02   # ... and how long apart, which is many times the one write that ends that moment


def _handle(w: policy.Holder) -> proc.Handle | None:
    """The worker one look found, as the handle a signal to it goes through: a wake, or the SIGTERM of
    `worker stop`. None where that look found no worker. A worker that is up has recorded itself, so both
    halves of the handle are there."""
    return proc.Handle.recorded(w.pid, (w.rec or {}).get("pstart")) if w.up else None


def worker_now(q: str) -> Holder:
    """What q's worker is: the one look at the claim's holder. It writes nothing and starts nothing.

    A worker that holds the claim and has not recorded itself yet is no state to report, so this looks again,
    a bounded number of times. A holder that never records itself comes back as a worker starting, for the
    verb to say so rather than name it as up.

    Whether the record is the holder's own is the process it names being alive. A take locks the claim file,
    empties it and writes the record in that order (`lock.Claim.take`), so for that moment a reader finds the
    file empty, or holding the record a worker that was killed left; a release empties the file, and a
    worker's record names itself, so those bytes name a process that is gone. Either way a record that names
    no process of its own, or one that names a process that is not running, is a worker in its first
    moment."""
    for _ in range(STARTING_LOOKS):
        rec = worker(q)
        w = policy.look(rec, proc.Handle.running((rec or {}).get("pid"), (rec or {}).get("pstart")) is not None)
        if w.how != STARTING:
            return w
        time.sleep(STARTING_PAUSE)      # its first moment: look again rather than decide from it
    return w


# When a round of starts of this process last brought no worker up, per queue. A loop that asks on every pass
# waits START_EVERY out before it starts another round, so a worker that cannot come up is not started over
# and over. The entry goes as soon as a worker is found up, so a queue that recovers is not held off.
_none_came_up: dict[str, float] = {}


def _no_worker_came_up(q: str, w: Holder, why: str | None = None) -> Holder:
    """This process's round of starts brought no worker up: hold off from starting another for START_EVERY.
    -> that answer, marked as such and carrying why, which is the reason a start gave or none.

    The clock is read here, after the starts, and not before them: a round can take longer than START_EVERY
    (a start waits for the worker to say it is ready), and a reading from before it would already be past."""
    _none_came_up[q] = time.time()
    return Holder(w.how, rec=w.rec, why=why, tried=True)


def worker_up(q: str, wake: bool = False) -> Holder:
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
    not start workers it cannot bring up ten times a second. Such an answer is marked `tried` and carries the
    reason in `why`, and an ask that lands inside the hold-off of an earlier round carries neither, so the
    caller says one and not the other. The words are the caller's: this writes none."""
    starts: list[int | None] = []       # the workers this started, which the look after each asks about
    while True:
        w = worker_now(q)
        if w.up:
            _none_came_up.pop(q, None)      # it is up: nothing to hold the next start off for
            if w.pid is not None and w.pid in starts:
                return Holder(STARTED, rec=w.rec)
            if wake:
                Worker.wake(_handle(w))
            return w
        if w.how == STARTING:
            return w
        if not starts and time.time() < _none_came_up.get(q, 0.0) + START_EVERY:
            return w        # a round of this process's brought none up that recently: not another yet
        if len(starts) == WORKER_STARTS:
            return _no_worker_came_up(q, w)         # the claim's next holder shut this many out
        try:
            starts.append(_start_worker(q).pid)     # starting a process is not store work: no lock is held
        except TimeoutError as e:
            return _no_worker_came_up(q, w, why=str(e))


def work(q: str, read_settings, admit_for, sink, tail=None) -> int:
    """Queue q's worker, in this process, until it is stopped or its idle time is spent.

    Four callables, because none of what they answer is the core's: `read_settings()` gives back
    {poll, idle_exit, backfill} once per loop, so a queue's limits change under running jobs with no restart,
    and an `error` on it is a caller whose settings cannot be read, under which this keeps reaping, timing out
    and recording, and launches nothing; `admit_for(kinds)` gives back the readings of one launch pass, taken
    before that pass's transaction; `sink(kind, job, fields)` is told what this did, and words it;
    `tail(job, n)` is the log tail --retry-if matches against.

    So the core reads no config file and takes no reading of its own, and it writes no sentence."""
    jobstore.writing_queue(q)    # a queue this pp cannot read is refused here too; the worker's own queue says its format
    st = _settings(read_settings)
    w = Worker(q, bool(st.get("backfill")), sink, tail, admit_for)
    # the wake is held for the whole of this worker's life, before it claims the queue: a wake can only ever
    # reach a worker that takes it from its own wakeup pipe
    with waking() as waits:
        claim = lock.Claim.take(_worker_claim(q), w.record())
        if claim is None:       # one worker per queue, and holding the claim is being that worker
            held = worker_now(q)
            sink("busy", None, {"how": held.how, "pid": held.pid})
            return 0
        w.claim = claim
        stop: list = []
        signal.signal(signal.SIGTERM, lambda *_: stop.append(1))    # a flag, never an exception mid-move
        proc.say_ready()    # it holds its claim, it is recorded, a stop is handled and a wake is taken
        w.stopping = lambda: bool(stop)
        # the running jobs this worker adopts: it says how many, and its first pass reaps them like its own
        sink("up", None, {"pid": os.getpid(), "backfill": w.backfill,
                          "adopting": len(jobstore.jobs("running", q))})
        idle_since = None
        bad_cfg = None
        woken = False
        try:
            while not stop:
                st = _settings(read_settings)   # once per loop, so a limit changes under a running job
                bad = st.get("error")
                if st.get("backfill") is not None and bool(st["backfill"]) != w.backfill:
                    w.backfill = bool(st["backfill"])
                    sink("backfill", None, {"on": w.backfill})
                try:
                    w.tick()    # reaping and timeouts go on under a broken config; only launching needs it
                except config.ConfigError as e:
                    bad = str(e)
                except Exception:
                    sink("tick-failed", None, {"trace": traceback.format_exc()})
                if bad != bad_cfg:
                    sink("config", None, {"error": bad})
                    bad_cfg = bad
                with jobstore.listing() as s:
                    busy = bool(s.jobs(("queued", "running"), q))
                    idle_since = None if busy else (idle_since or time.time())
                    period, spent = policy.next_tick(st, w.serving, woken, idle_since, time.time())
                    if spent:
                        # let the claim go inside the scope that read the store, which is the one order that
                        # leaves no job behind: a job stored after this finds the claim free and starts the
                        # next worker, and one stored before it was read here, so this worker stays
                        claim.release()
                if spent:
                    sink("idle", None, {"idle_exit": st.get("idle_exit", policy.IDLE_EXIT)})
                    break
                # one wait, ended by the period or by a signal: a stop in flight is carried out in the next
                # tick, and a wake or a SIGTERM is acted on as it arrives. The short period goes with the
                # wake: a verb waiting for a stop wakes this worker on every pass of its own wait, so the
                # tick after that wake is this worker's next one. A stop nobody waits for, a timeout's, is
                # carried out at the queue's poll, and the store lock is not taken ten times a second for a
                # job the kernel is slow to end
                woken = waits.wait(period)
        except KeyboardInterrupt:
            stop.append(1)
        finally:
            if stop:
                sink("stopped", None, {})
            claim.release()
    return 0


def _settings(read_settings) -> dict:
    """The settings one pass of the loop runs under, as its caller reads them. A caller that raises is a
    caller whose settings cannot be read: the error comes back on the settings themselves, which is what
    stops that pass launching anything."""
    try:
        return read_settings()
    except Exception as e:
        return {"error": str(e)}


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
    return os.path.join(jobstore.top("watchers"), f"{pid}-{pstart}.claim")


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
    d = jobstore.top("watchers")
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


# ---- the two signals a verb hands over ---------------------------------------------------------------

def ask_stop(jid: int, by: str, grace: float) -> dict:
    """A kill, as one scope: the stop this run of the job carries, once this ask has landed on it (`decide`
    carries it out), or the end of a job that is back in its queue with no process of its own, which is where
    such a job ends, as failed with reason `killed`.

    -> which of the two it did (`did`: "asked", "killed" or "ended"), the record as the scope left it, the
    state it found the job in, and the time of the stop it wrote. The caller words it and records nothing."""
    with jobstore.txn() as s:
        cur = s.job(jid)
        if cur is None or cur["state"] in jobstore.ENDED:
            return {"did": "ended", "job": cur, "was": cur["state"] if cur else "gone"}
        if cur["state"] in ("queued", "held"):
            was = cur["state"]
            jobstore._finish(s, cur, "failed", None, "killed")
            return {"did": "killed", "job": cur, "was": was}
        cur["stop"] = policy.merge_stop(cur.get("stop"), {"by": by, "t": time.time(), "grace": grace})
        s.save(cur)
        return {"did": "asked", "job": cur, "was": "running", "t": cur["stop"]["t"]}


def worker_stop(q: str, sink=None) -> dict:
    """Stop queue q's worker: one look at the claim, the SIGTERM to the worker that look found, and the wait
    until that holder lets the claim go.

    -> what it did (`did`: "down", "stopped", "denied", "starting" or "timeout") and the pid it signalled,
    which three of those five sentences name. The caller words them, and hears through `sink`, once, that the
    wait is in a slow tick.

    What this waits for is that worker, not a free claim: an `add` or a background `wait` starts the next
    worker as soon as the claim is free, and this stop is over when the one it signalled has let it go. It
    stops between ticks, and one tick can wait on a probe or on nvidia-smi; it is down when the claim is no
    longer its own, which the kernel sees to even if it never reaches the end of its own stop."""
    w = worker_now(q)           # one look, and this stop signals the worker that look found
    if not w.up:                # nothing holds the claim, or a holder that has not recorded itself yet
        return {"did": "starting" if w.how == STARTING else "down", "pid": None}
    h = _handle(w)              # the handle this one look gave: no second look decides what is signalled
    outcome = proc.signal_pid(h, signal.SIGTERM)
    if outcome in ("gone", "denied"):   # it exited since we looked, or it is another user's
        return {"did": "down" if outcome == "gone" else "denied", "pid": h.id}
    start, told = time.time(), False
    while True:
        # whether the worker this stop signalled still holds the claim, asked through the one construct. A
        # record naming another worker, and nothing holding the claim, are both that worker gone
        held, waited = worker_now(q).rec or {}, time.time() - start
        if (held.get("pid"), held.get("pstart")) != (h.id, h.start):
            return {"did": "stopped", "pid": h.id}
        if waited > STOP_WAIT:
            return {"did": "timeout", "pid": h.id}
        if sink is not None and waited > SLOW_TICK and not told:
            told = sink("slow-stop", None, {"pid": h.id}) is None
        time.sleep(0.1)
