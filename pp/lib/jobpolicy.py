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

TIMEOUT_GRACE = 10.0        # seconds between a timeout's SIGTERM and its SIGKILL
RETRY_DELAY = 30.0
RETRY_DELAY_CAP = 3600.0


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


def matches(job: dict, why: str, tail: str) -> bool:
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

# ---- the settings a rule is handed ------------------------------------------------------------------

def num(v) -> int | float:
    f = float(v)
    return int(f) if f.is_integer() else f


def hook_spec(v) -> str | None:
    v = (v or "").strip() if isinstance(v, str) else v
    return None if not v or v == "none" else str(v)


def hook_argv(spec: str | None) -> list[str] | None:
    """argv a hook runs as: a shell command."""
    spec = hook_spec(spec)
    return None if spec is None else ["/bin/sh", "-c", spec]
