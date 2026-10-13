"""The rules of `pp queue`, as values: the facts in, one decision out.

Every rule a lab argues about lives here, and nothing else does: no clock, no store, no process, no signal,
no file. A function is handed records, readings, settings and `now`, and gives back a value. So each rule is
checked by a table over the whole of its input (tests/test_queue.py), and a table over a finite input is
exhaustive.

One direction only: pp/lib/jobcore.py and pp/lib/jobq.py call this, and this calls neither. Nothing here
imports a module that imports this, so "the rules never read the state a mechanism is in" is a property of
the import graph (tests/test_constructs.py).

Five things can be true of a running job at once: a run of it is live or over, it wrote an exit code or it
did not, a stop is on its record or there is none, the grace that stop was given is up or it is not, and its
own time has run out or it has not. `decide` is the one place that says what is to be done about it, from all
of them together.

Around it, `jobcore.Worker.carry_out` gathers the facts about each running job, calls `decide` once, and
carries the one action out: the signal through the job's Handle, the record in one transaction, the hook
after it. So a timeout, a kill and a reap are not three actors with three views of one job; they are facts
about it, and one decision made from all of them.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# The built-in defaults the declaration's rows point at (pp/lib/jobq.py, SETTINGS). A rule below falls back
# to one of them for a record written before that row was a setting of the job.
GRACE = 10.0            # between a stop of a job and the SIGKILL that follows, for a timeout and a kill alike
PRIO = 10
RETRY_DELAY = 30.0
RETRY_DELAY_CAP = 3600.0
RETRIES_CAP = 1000


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
        again     attempt, not_before, history, clear, ended, sweep   back to its queue for another attempt

    hook is the event whose hook is owed ("failed", "done"), or None where the outcome owes none or the job
    carries no such hook. sweep says anything left in the job's process group goes with it. history is the
    entry a requeue writes for the attempt that ended, and clear the keys it takes off the record, so the
    store writes what this says and works nothing out again.
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
    history: dict | None = None
    clear: tuple = ()

    @property
    def why(self) -> str:
        """The failure as the job's log names it and --retry-if matches it: a reason, or the exit code."""
        return self.reason or f"exit {self.rc}"


# What one attempt of a job holds that the next one does not: the process it ran as, the stop it carried, the
# files it wrote and the names it was given. A requeue takes each off the record and the history entry it
# writes keeps them, so a job that ran three times names three sets of files and claims none of them twice.
ATTEMPT = ("pid", "pgid", "pstart", "boot", "started", "gpu", "stop", "rcfile")
KEPT = ("rcfile",)      # of those, the ones the history entry carries by name


def attempt_of(job: dict, rc: int | None, reason: str | None, ended: float) -> dict:
    """The history entry the attempt that just ended leaves behind: how it ended, how long it ran, and the
    files and names that were its own."""
    started = job.get("started")
    return {"attempt": int(job.get("attempt", 1)), "rc": rc, "reason": reason, "started": started,
            "ended": ended, "seconds": ended - (started or ended),
            **{k: job.get(k) for k in KEPT}}


def matches(job: dict, why: str, tail: str) -> bool:
    """Whether --retry-if matches this failure: its reason, or the tail of the job's log."""
    pat = job.get("retry_if")
    if not pat:
        return True
    try:
        return re.search(pat, why + "\n" + tail, re.M) is not None   # ^ $ per line
    except re.error:
        return False


def grace(job: dict) -> float:
    """The gap between a stop of this job and the SIGKILL that follows it: one number of the job's own,
    resolved onto its record at add time, and the built-in default for a record that carries no such key,
    which is one written before that row was a setting. A grace of 0 is the job's own answer and not an
    absent one, so a job that asks to be SIGKILLed at once is.

    A timeout's stop and a kill of the job both take this number, and `kill --grace` overrides it for the
    jobs that kill names."""
    g = job.get("grace")
    return GRACE if g is None else float(g)


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
            return Action(TIME_UP, t=facts.now, grace=grace(job)) if due else Action(NOTHING)
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


def hook_due(job: dict, event: str | None) -> str | None:
    """The hook event this outcome owes, or None: the outcome owes none, or the job carries no such hook."""
    if event is None:
        return None
    return event if hook_spec(job.get("on_fail" if event == "failed" else "on_done")) else None


def _ends(job: dict, rc: int | None, reason: str | None, ended: float | None, sweep: bool, event: str | None,
          not_retried: str | None = None) -> Action:
    """The job is over: its state follows its exit code, and the hook that outcome owes is the job's own."""
    return Action(ENDED_AS, state="done" if rc == 0 and not reason else "failed", rc=rc, reason=reason,
                  hook=hook_due(job, event), ended=ended, sweep=sweep, not_retried=not_retried)


def _or_again(job: dict, facts: Facts, rc: int | None, reason: str | None, ended: float | None,
              sweep: bool) -> Action:
    """A failure of the job's own: another attempt where it has one left and --retry-if matches it, else the
    end of it. A delay doubles per attempt, up to RETRY_DELAY_CAP."""
    attempt, retries = int(job.get("attempt", 1)), int(job.get("retries") or 0)
    why = reason or f"exit {rc}"
    left = attempt <= retries
    if left and matches(job, why, facts.tail):
        now = ended or facts.now
        base = job.get("retry_delay")
        delay = min(float(RETRY_DELAY if base is None else base) * 2 ** (attempt - 1), RETRY_DELAY_CAP)
        return Action(AGAIN, rc=rc, reason=reason, ended=now, sweep=sweep, attempt=attempt + 1,
                      not_before=now + delay, history=attempt_of(job, rc, reason, now), clear=ATTEMPT)
    # attempts were left, but retry_if says this failure is not worth one
    nr = f"neither the reason ({why}) nor the log tail matches /{job.get('retry_if')}/" if left else None
    return _ends(job, rc, reason, ended, sweep, "failed", nr)


def in_flight(job: dict, action: Action) -> bool:
    """Whether a stop of this job still has something to go out for it, which is what shortens the worker's
    wait while a verb waits on one (`next_tick`).

    A stop whose SIGKILL has gone out has nothing left to carry out: the worker waits for the kernel to end a
    job that is slow to die, at its poll. A stop pp was refused has nothing more going out for it at all."""
    stop = job.get("stop") or {}
    if action.do in (TERM, KILL9, TIME_UP):
        return True
    return action.do == NOTHING and bool(stop) and not stop.get("denied") and not stop.get("kill9")


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


# ---- what one launch pass admits ---------------------------------------------------------------------
#
# A pass is handed the records it reads inside its transaction, the readings taken before it, and the
# settings it runs under. It gives back one value: the jobs to start, in the order they start, with the card
# each one takes, and the one (job, why) the head of the queue waited for. So every rule of admission is a
# table (tests/test_queue.py), and the core's own code is the claim, the record and the start.
#
# `seen` is one pass's readings as a value, taken outside the store lock by whoever runs the probes
# (pp/lib/jobq.py, Resources): {q, pools, by_name, cards, cards_t, probes, cards_why, where}.


@dataclass(frozen=True)
class Pass:
    """What one launch pass decided.

    starts      ((job, card), ...) in the order they start; card is None for a job that needs no GPU.
    blocked     (job, why) for the first job that did not fit, which is what the head of the queue waits
                for, and (None, "") for a pass that admitted everything it was given.
    """

    starts: tuple = ()
    blocked: tuple = (None, "")


def need(j: dict, k: str):
    """How much of pool k this job asks for; 0 where it asks for none."""
    return (j.get("needs") or {}).get(k, 0)


def pool_jobs(p: dict, running: list[dict]) -> list[dict]:
    """The running jobs a pool counts: every queue's, for a shared pool, and one queue's for its own."""
    return running if p["scope"] is None else [r for r in running if r["queue"] == p["scope"]]


def gpu_budget(p: dict, card: dict) -> float:
    """What a pool reserves on one card: the max it was read with, or the card's own total less 2 GB."""
    return p["max"] if p["max"] is not None else round(max(card["total_gb"] - 2, 0), 1)


def ready(queued: list[dict], now: float) -> list[dict]:
    """The queued jobs a pass may start. A job waiting out a retry's delay is skipped, not waited for, so it
    never becomes the head of its queue."""
    return [j for j in queued if (j.get("not_before") or 0) <= now]


def check(needs: dict, seen: dict) -> str | None:
    """Why these needs can never fit (nothing provides one, or it exceeds a budget); None when they might."""
    for k, v in needs.items():
        ps = seen["by_name"].get(k)
        if not ps:
            return (f"nothing provides '{k}': set [resources] {k} (shared by every queue) or "
                    f"[queue.{seen['q']}.resources] {k} (this queue's own) in {seen['where']}")
        if k == "gpu_gb":
            if not seen["cards"]:
                return "needs gpu_gb but nvidia-smi sees no GPU" + (f" ({seen['cards_why']})" if seen["cards_why"] else "")
            top = max(min(gpu_budget(p, c) for p in ps) for c in seen["cards"])
            if v > top:
                return f"needs gpu_gb={v:g} but no card's budget is that large ({top:g})"
            continue
        for p in ps:
            if p["max"] is not None and v > p["max"]:
                return f"needs {k}={v:g} but {p['where']} = {p['max']:g}"
    return None


def fits(job: dict, running: list[dict], seen: dict) -> tuple[bool, str, int | None]:
    """Whether one job fits now: (fits, why not, the card it takes). running = every queue's running jobs,
    which inside a pass holds the jobs that pass has taken already. `why` keeps its fluctuating detail after
    ';' so the worker notes a change of reason only."""
    needs = job.get("needs") or {}
    for k, v in needs.items():
        if k == "gpu_gb":
            continue
        ps = seen["by_name"].get(k)
        if not ps:
            return False, f"{k}: nothing provides it ([resources] or [queue.{seen['q']}.resources] {k})", None
        for p in ps:
            if p["max"] is not None:
                used = sum(need(r, k) for r in pool_jobs(p, running))
                if used + v > p["max"]:
                    return False, f"{k}: {p['where']}; {used:g}+{v:g} > {p['max']:g}", None
            if p["probe"]:
                r = seen["probes"].get((p["scope"], p["name"]))
                if r and r.get("running"):
                    return False, f"{k}: its probe is being read ({p['where']})", None
                if not r or r.get("value") is None:
                    return False, f"{k}: probe failed ({p['where']}); {(r or {}).get('error') or 'no reading'}", None
                pending = sum(need(x, k) for x in running if (x.get("started") or 0) + p["settle"] > r["t"])
                free = r["value"] - pending
                if v > free:
                    return False, (f"{k}: not free ({p['where']}); probe says {r['value']:g}"
                                   + (f", {pending:g} of it launched since" if pending else "") + f", need {v:g}"), None
    want = needs.get("gpu_gb", 0)
    if not want:
        return True, "", None
    ps = seen["by_name"].get("gpu_gb")
    if not ps:
        return False, "gpu_gb: nothing provides it", None
    if not seen["cards"]:
        return False, "gpu: nvidia-smi unreadable" + (f" ({seen['cards_why']})" if seen["cards_why"] else ""), None
    settle = max(p["settle"] for p in ps)
    whys = []
    for c in sorted(seen["cards"], key=lambda c: -c["free_gb"]):
        i = c["index"]
        on = [r for r in running if r.get("gpu") == i]
        why = None
        for p in ps:
            reserved, budget = sum(need(r, "gpu_gb") for r in pool_jobs(p, on)), gpu_budget(p, c)
            if reserved + want > budget:
                why = f"card {i} reserved {reserved:g}+{want:g} > {budget:g} ({p['where']})"
                break
        if why is None:
            # what the reading cannot see yet: jobs launched on this card less than `settle` before it
            pending = sum(need(r, "gpu_gb") for r in on if (r.get("started") or 0) + settle > seen["cards_t"])
            if c["free_gb"] - pending < want:
                why = f"card {i} free {c['free_gb'] - pending:.1f}" + (
                    f" ({pending:g} launched in the last {settle:g}s, not allocated yet)" if pending else "")
        if why is None:
            return True, "", i
        whys.append(why)
    return False, f"gpu_gb {want:g} does not fit on any card; {', '.join(sorted(whys))}", None


def admit(state: dict, seen: dict, settings: dict) -> Pass:
    """One launch pass: which of the queued jobs start, on which card, and what the head waited for.

    state is the records the pass read inside its own transaction: {queued, running}, the queued ones of this
    queue in run order (prio, then id) and every queue's running ones, since a shared pool counts them all.
    settings is {backfill, now}.

    Strict head-of-line by priority: the first job that does not fit ends the pass, so a big job is never
    starved by small ones. With backfill the pass goes on past it, and the job that did not fit is still the
    one the head waits for. A job waiting out a retry's delay is skipped, not waited for (`ready`), so it
    never becomes the head. What this pass has taken counts against the next job in it, as the record it is
    about to become: on the card it took, and launched now, so a reading older than its settle window does
    not show the next job room that is already taken."""
    running = list(state["running"])
    starts, blocked = [], (None, "")
    for job in ready(state["queued"], settings["now"]):
        ok, why, card = fits(job, running, seen)
        if not ok:
            if blocked[0] is None:
                blocked = (job, why)
            if settings["backfill"]:
                continue
            break
        starts.append((job, card))
        # what this pass has taken counts against the next job in it, as the record it is about to become:
        # on the card it took, and launched now, which is inside every settle window a reading has
        running.append({**job, "started": settings["now"], "gpu": card})
    return Pass(tuple(starts), blocked)


# ---- the worker's own two numbers, and one look at its claim -------------------------------------------

UP = "up"               # a worker holds the claim and has recorded itself
STARTED = "started"     # the claim was free, and the worker this start brought up holds it
DOWN = "down"           # nobody holds the claim
STARTING = "starting"   # a worker holds the claim and has not recorded itself yet

STOP_PERIOD = 0.1       # how long a woken worker waits between ticks while a stop of its queue is in flight
POLL = 2.0              # how long between ticks, and how long a worker waits idle before it exits, where
IDLE_EXIT = 600.0       # ... the settings it was handed say neither


@dataclass(frozen=True)
class Holder:
    """What one look at a queue's worker claim came to, and the one value every verb prints from.

    how     up, started, down or starting, as above.
    rec     the record of the worker that holds the claim. None where nobody holds it.
    tried   whether this answer came from a round of starts of this process's that brought no worker up,
            which is what a caller that is not quiet says. An ask inside the hold-off of an earlier round
            starts nothing and carries nothing, so it says nothing either.
    why     why a round of starts brought no worker up, where this answer came from such a round. None
            otherwise. A start that times out is the one reason the worker's own log cannot carry, since a
            worker that hung before it said it was ready has logged nothing, so a caller that asked quietly
            says this itself.
    """

    how: str
    rec: dict | None = None
    why: str | None = None
    tried: bool = False

    @property
    def up(self) -> bool:
        """Whether a worker is up: the one answer, so nothing has to read a record to find out."""
        return self.how in (UP, STARTED)

    @property
    def pid(self) -> int | None:
        """The pid of the queue's worker. None where nobody holds the claim, and while a worker that holds it
        has not recorded itself yet."""
        return (self.rec or {}).get("pid") if self.up else None


def look(rec: dict | None, alive: bool) -> Holder:
    """What one look at a queue's worker claim came to: `down` where nobody holds it, `up` where the record in
    it names a process of its own that is alive, and `starting` while the holder has not recorded itself yet.

    rec is the record the claim holds, or None where nobody holds it; alive is whether the process that
    record names is running, which only a look at a process can say."""
    if rec is None:
        return Holder(DOWN)
    return Holder(UP if alive else STARTING, rec=rec)


def next_tick(settings: dict, serving: int, woken: bool, idle_since: float | None, now: float):
    """How long the worker waits before its next tick, and whether its idle time is spent.

    The short period goes with the wake: a verb waiting for a stop wakes the worker on every pass of its own
    wait, so the tick after that wake is the worker's next one. A stop nobody waits for, a timeout's, is
    carried out at the queue's poll, and the store lock is not taken ten times a second for a job the kernel
    is slow to end.

    idle_since is when the queue last held a job that is queued or running, and None while it holds one, so
    an idle time that is spent is a worker with nothing left to do."""
    period = float(settings.get("poll", POLL))
    if serving and woken:
        period = min(period, STOP_PERIOD)
    return period, idle_since is not None and now - idle_since >= float(settings.get("idle_exit", IDLE_EXIT))


# ---- what a job and a hook run with -------------------------------------------------------------------

def job_env(job: dict, card: int | None) -> dict:
    """The environment one attempt of a job runs in, in the order it is merged: what the shell that queued it
    was in, the project's env and the job's own, pp's own names for this queue and this job, and the card it
    was given."""
    env = {**(job.get("shell_env") or {}), **(job.get("env") or {}),
           "PP_QUEUE": job["queue"], "PP_QUEUE_JOB": str(job["id"]), "PP_QUEUE_RCFILE": job["rcfile"]}
    if card is not None:
        env["CUDA_VISIBLE_DEVICES"] = str(card)
    return env


def banner(job: dict, cmd: str, stamp: str) -> str:
    """The line pp appends to a job's log before an attempt's output: whose output follows, which is what
    makes a file several jobs write to readable. The caller passes the time, since a rule reads no clock, and
    the command as one line, since a rule splits no words."""
    retries = int(job.get("retries") or 0)
    attempt = f" attempt {job.get('attempt', 1)}/{retries + 1}" if retries else ""
    return f"[pp queue] {stamp} job {job['id']} ({job['queue']}){attempt} in {job['cwd']}: {cmd}\n"


def hook_env(job: dict, event: str) -> dict:
    """What this job's on_fail / on_done hook runs with: the queue and the job, the event, and how the job
    ended, so a hook needs to read no record. A field the record does not hold is empty, never absent: a hook
    under `set -u` reads every one of them."""
    reason = job.get("reason") or (f"exit {job['rc']}" if event == "failed" and job.get("rc") is not None else "")
    return {"PP_QUEUE": job["queue"], "PP_QUEUE_JOB": str(job["id"]), "PP_QUEUE_EVENT": event,
            "PP_QUEUE_RC": "" if job.get("rc") is None else str(job["rc"]), "PP_QUEUE_REASON": reason,
            "PP_QUEUE_LOG": job.get("log") or "", "PP_QUEUE_CWD": job.get("cwd") or "",
            "PP_QUEUE_TAG": job.get("tag") or ""}


# ---- which jobs a selection names ----------------------------------------------------------------------

NAME = re.compile(r"[A-Za-z0-9._:-]{1,64}")     # what a job's own name may be made of, and how long


def tag(v: str) -> str:
    """A job's name, as it is read on the command line: at most 64 characters of letters, digits and . _ - :,
    never holding `..`, never `.` alone, and never a word that reads as an id or a range.

    A name that read as an id could not select its own jobs, and one holding `..` could not be put in a path
    (section 6's `{tag}`). A job added without --tag has no name: its id selects it and its command is what a
    view shows for it."""
    if not isinstance(v, str) or not NAME.fullmatch(v):
        raise ValueError(f"'{str(v)[:80]}': a name of at most 64 letters, digits, '.', '_', '-' and ':'")
    if ".." in v or v == ".":
        raise ValueError(f"'{v}': a name holds no '..' and is not '.' on its own")
    if kind(v) != "tag":
        raise ValueError(f"'{v}' reads as a job id or a range, so it could not select its jobs; use a word")
    return v


ID, RANGE, TAG = "id", "range", "tag"
PREFIX = "prefix"       # --prefix P: every job whose name starts with P, repeatable
ALL = "all"             # --all: every job of the queue -q names, and a usage error without one


def kind(spec) -> str:
    """Which form of the selection grammar this spec is: an id, a range (12-15), a tag matched whole, a
    prefix of a tag, or every job of one queue.

    The first three are a word the verb was given. The two that are flags are (form, value) pairs, since no
    word tells them apart from a tag."""
    if isinstance(spec, tuple):
        return spec[0]
    if re.fullmatch(r"\d+-\d+", spec):
        return RANGE
    return ID if spec.isdigit() else TAG


def select(records: list[dict], selection) -> list[int]:
    """The ids these specs name, over the records they are read against: in the order the specs were given,
    each id once, and the ids of one spec in id order. The five forms of the grammar and no others.

    One spec, or several. A job with no name is named by its id or by a range alone, so neither a tag nor a
    prefix ever matches one. A form that names no record gives back nothing, and the caller refuses: nothing
    here reads the store or writes a sentence."""
    out: list[int] = []
    for spec in ([selection] if isinstance(selection, (str, tuple)) else selection):
        form = kind(spec)
        value = spec[1] if isinstance(spec, tuple) else spec
        if form == RANGE:
            lo, hi = (int(x) for x in value.split("-"))
            hits = [j["id"] for j in records if lo <= j["id"] <= hi]
        elif form == ID:
            hits = [j["id"] for j in records if j["id"] == int(value)]
        elif form == TAG:
            hits = [j["id"] for j in records if j.get("tag") == value]
        elif form == PREFIX:
            hits = [j["id"] for j in records if j.get("tag") and j["tag"].startswith(value)]
        else:
            hits = [j["id"] for j in records]
        out += [jid for jid in sorted(hits) if jid not in out]
    return out


# ---- what a setting is, how a value of it is read, and the one order of its sources -------------------
#
# Every setting a user can give is one row of one declaration (pp/lib/jobq.py, SETTINGS), and a row says its
# type and spelling, its default, its scope, its flag, its keys and what it refuses. Three rules of a row are
# here, because each is pure and each is the same for every row: which sources a row has, the value one
# source gives, and the order those sources are read in. Two readers of one setting is a defect, so a flag's
# word and a key's value are read by the one function below.


class _NotSet:
    """What a source that does not give a setting at all is.

    `None` cannot say it: `--timeout none` and `on_done = "none"` are values of their rows, and a value has
    to win over the sources behind it the way any value does."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "NOTSET"


NOTSET = _NotSet()

UNITS = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
DURATION_CAP = 1e9      # seconds: past about thirty years a duration is a mistake and not a wait
HOW_LONG = "say how long, like 90s, 30m, 2h or 1d"
_DUR = re.compile(r"\s*(\d+(?:\.\d+)?|\.\d+)\s*([smhd]?)\s*")

# Which sources a row has follows from what the setting belongs to, and nothing else decides it: a per-job
# row has a flag and both keys, a per-queue row the queue's key, the machine's rows the machine's table, and
# a per-project row its project's key and a flag for one job. A row that cannot have all of its scope's
# sources is not a setting but an argument of `add`.
SOURCES = {"job": ("flag", "project", "queue"), "project": ("flag", "project"),
           "queue": ("queue",), "machine": ("machine",)}


@dataclass(frozen=True)
class Setting:
    """One row of the declaration, and everything that is true of that setting.

    name      the config key, which is the flag's name without the dashes and with `-` written `_`
    scope     job | queue | machine | project: what it belongs to, which decides its sources
    kind      its type and spelling, which `value` reads one source's word through
    default   the one built-in default, which is what every source being absent comes to
    printed   its default is printed in the help, so a release that changes it is a break
    field     the record field a per-job row lands in, where that is not the row's own name
    unit      what a bare number means, for a duration
    lo, hi    the bounds the row gives
    none_ok   `none` is a value of this row, and so is 0: no bound
    auto_ok   "auto" is a value of this row: the size the machine knows of itself
    merge     a table, merged key by key, instead of a scalar taken whole from one source
    repeat    its flag may be given more than once
    flagged   its scope has a flag and this row has it
    top       its key is at the top of .pp.toml and not in that file's [queue]
    metavar   what its flag's help calls the value
    says      the line its flag's help prints
    refuses   what it refuses, which the reader that met it words
    """

    name: str
    scope: str
    kind: str
    default: object = None
    printed: bool = False
    field: str = ""
    unit: str = "s"
    lo: float | None = None
    hi: float | None = None
    none_ok: bool = False
    auto_ok: bool = False
    merge: bool = False
    repeat: bool = False
    flagged: bool = True
    top: bool = False
    metavar: str = ""
    says: str = ""
    refuses: str = ""

    @property
    def flag(self) -> str:
        """The flag this row is given on a command line, or "" where it has none."""
        has = self.flagged and "flag" in SOURCES[self.scope]
        return "--" + self.name.replace("_", "-") if has else ""

    @property
    def tables(self) -> tuple:
        """The config tables this row is a key of, by scope: `project` is the project's `.pp.toml [queue]`,
        `queue` is `[queue.NAME]` and `machine` is `[resources]`."""
        return tuple(k for k in ("project", "queue", "machine") if k in SOURCES[self.scope])

    @property
    def on_record(self) -> str:
        """The record field a per-job row's resolved value lands in."""
        return self.field or self.name


def resolve(setting: Setting, flag=NOTSET, project=NOTSET, config=NOTSET):
    """One row's value: the first source that gives it, in the one order every row is read in.

        the flag  >  the project's .pp.toml [queue]  >  ~/.config/pp/config.toml [queue.NAME]  >  the default

    A source that does not give the setting is NOTSET, so a value of `none` is a value and not an absence. A
    scalar is taken whole from the first source that gives it; a table (env, need) merges key by key, with
    the same order per key, so a project may add one variable to the queue's without naming the rest."""
    if setting.merge:
        out = dict(setting.default or {})
        for src in (config, project, flag):
            if src is not NOTSET:
                out.update(src)
        return out
    for v in (flag, project, config):
        if v is not NOTSET:
            return v
    return setting.default


def value(setting: Setting, raw):
    """What one source says about this row, as the row's own type: the one reading of a setting, for a flag's
    word and for a key's value alike.

    ValueError where the row refuses it, worded so that whoever read it names the flag or the key it came
    from. A kind with no reading of its own comes back as it is, for the one reader that knows it (a pool)."""
    kind = setting.kind
    if kind in ("duration", "period"):
        secs = duration(raw, setting.unit, setting.none_ok)
        if secs is not None and setting.hi is not None and secs > setting.hi:
            raise ValueError(f"'{raw}' is longer than {setting.hi:g} seconds, which is this one's cap")
        if kind == "period" and not secs:
            raise ValueError(f"'{raw}' is not longer than 0")
        return secs
    if kind == "whole":
        return whole(raw, setting.lo, setting.hi)
    if kind == "amount":
        return amount(raw)
    if kind == "number":
        if setting.auto_ok and raw == "auto":
            return raw
        try:
            return num(raw)
        except (TypeError, ValueError):
            raise ValueError(f"{raw!r}: not a number") from None
    if kind == "boolean":
        return boolean(raw)
    if kind == "regex":
        return regex(raw)
    if kind == "command":
        return hook_spec(raw)
    if kind == "shell":
        return shell(raw)
    if kind == "text":
        return text(raw)
    if kind == "strings":
        return {k: one_line(k, v) for k, v in pairs(raw).items()}
    if kind == "counts":
        return {k: amount(v) for k, v in pairs(raw).items()}
    return raw


def duration(v, unit: str = "s", none_ok: bool = False) -> float | None:
    """The one reading of a duration, in seconds: 90s, 30m, 2h, 1d, .5 and 1.5h, and a bare number in `unit`.
    `none` is no bound where the row allows one, and so is 0.

    An empty value, a spelling this does not know, a value that is not finite and a value past DURATION_CAP
    seconds are each refused. A config key is written `""` by a shell that expanded a variable it had not
    set as much as a flag is, and reading that as "no bound" is a wait with no deadline; where a row means to
    offer one it offers the word."""
    if v is None or v == "" or isinstance(v, bool):
        raise ValueError(HOW_LONG + (", or none" if none_ok else ""))
    if isinstance(v, str) and v.strip().lower() == "none":
        if none_ok:
            return None
        raise ValueError(f"'{v}' is no duration, and this one takes no 'none': {HOW_LONG}")
    if isinstance(v, (int, float)):
        secs = float(v) * UNITS[unit]
    else:
        m = _DUR.fullmatch(str(v))
        if not m:
            raise ValueError(f"'{v}' is not a duration like 90s, 30m, 2h or 1d")
        secs = float(m[1]) * UNITS[m[2] or unit]
    if not 0 <= secs <= DURATION_CAP:       # nan and inf compare false with everything, so both land here
        raise ValueError(f"'{v}' is not a duration from 0 to {DURATION_CAP:g} seconds")
    return None if none_ok and secs == 0 else secs


def num(v) -> int | float:
    """A number, as a whole one where it is one: 3 rather than 3.0, since a count reads better that way."""
    f = float(v)
    return int(f) if f.is_integer() else f


def whole(v, lo: float | None = None, hi: float | None = None) -> int:
    """A whole number inside the bounds the row gives. A fraction is refused rather than rounded."""
    if isinstance(v, bool) or isinstance(v, float):
        raise ValueError(f"{v!r}: a whole number, not a fraction")
    try:
        n = int(str(v).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{v!r}: a whole number") from None
    if (lo is not None and n < lo) or (hi is not None and n > hi):
        bound = (f"from {lo:g} to {hi:g}" if lo is not None and hi is not None
                 else f"of at least {lo:g}" if lo is not None else f"of at most {hi:g}")
        raise ValueError(f"{n}: a whole number {bound}")
    return n


def amount(v) -> int | float:
    """How much of a pool a job asks for: a positive number. 0, a negative, nan and inf are refused, nan
    because it compares false with everything and so would wait for a pool that can never hold it."""
    if isinstance(v, bool):
        raise ValueError(f"{v!r}: a positive number")
    try:
        f = num(v)
    except (TypeError, ValueError):
        raise ValueError(f"'{v}' is not a number") from None
    if not 0 < f < float("inf"):
        raise ValueError(f"{f:g}: a positive number")
    return f


def boolean(v) -> bool:
    if not isinstance(v, bool):
        raise ValueError(f"{v!r}: true or false")
    return v


def text(v) -> str:
    if not isinstance(v, str):
        raise ValueError(f"{v!r}: a string")
    return v


def shell(v) -> str:
    """A shell command a setting carries: a string with something in it."""
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{v!r}: a shell command")
    return v


def regex(v) -> str | None:
    """A pattern that compiles, or None where it is empty: a row with no pattern matches every failure, so an
    empty one written on purpose turns a default off."""
    if not isinstance(v, str):
        raise ValueError(f"{v!r}: a string")
    if not v:
        return None
    try:
        re.compile(v)
    except re.error as e:
        raise ValueError(f"{v!r}: {e}") from None
    return v


def hook_spec(v) -> str | None:
    """A shell command, or None where it is `none` or empty: a hook the job does not carry."""
    if v is not None and not isinstance(v, str):
        raise ValueError(f"{v!r}: a shell command, or none")
    v = (v or "").strip()
    return None if not v or v == "none" else v


def hook_argv(spec: str | None) -> list[str] | None:
    """argv a hook runs as: a shell command."""
    spec = hook_spec(spec)
    return None if spec is None else ["/bin/sh", "-c", spec]


def pairs(v) -> dict:
    """A table row's value from either source: the table a config key holds, or the NAME=VALUE words a
    repeated flag gave."""
    if isinstance(v, dict):
        return {str(k): val for k, val in v.items()}
    if not isinstance(v, (list, tuple)):
        raise ValueError(f'{v!r}: a table, like {{ NAME = "value" }}')
    out = {}
    for it in (v if isinstance(v, (list, tuple)) else [v]):
        k, sep, val = str(it).partition("=")
        if not sep or not k:
            raise ValueError(f"'{it}': NAME=VALUE")
        out[k] = val
    return out


def one_line(name: str, v) -> str:
    """One variable of a job's environment: a name without `=` or NUL, and a value on one line, because the
    environment is handed to a runner as one line per variable."""
    if "=" in name or "\0" in name:
        raise ValueError(f"'{name}': a name holds neither = nor NUL")
    s = str(v)
    if "\n" in s:
        raise ValueError(f"'{name}': a value holds no newline")
    return s
