"""Run commands later, under resource limits, in named queues

Each named queue has its own limits, its own run order and its own worker; job ids are machine-wide. A job runs
when its declared needs fit the resource pools. A pool is slots, GPU memory (reserved per card AND read live
from nvidia-smi), CPUs, RAM, any counter you name, or anything a command can report as free (a cluster, an API
quota). A job is retried, timed out and reported as you ask.

    pp queue add -- make docs                          queue a command in the `default` queue, run in this directory
    pp queue add -q dl --need net=1 -- ./fetch.sh      another queue: its own limits, order and worker
    pp queue add --timeout 2h --retries 2 --retry-if 'Connection reset|CUDA out of memory' -- python x.py
    pp queue add --on-fail 'notify-send "job $PP_QUEUE_JOB failed"' -- ./nightly.sh
    pp queue add --from cmds.txt --prio 5              one job per line (# comments and blank lines skipped)
    pp queue add --gpu-gb 20 -- python x.py            in a project with a runner (below), run through it
    pp queue add --no-runner -- make docs              ... or not
    pp queue add --tag sweep -- python x.py --lr 0.1   a tag names a job, or a group of jobs that share it
    pp queue wait sweep                                every job tagged sweep (kill, drop, show and log take tags too)
    pp queue                                           status of every queue: running (last log line), queued, recent
    pp queue -q dl                                     one queue
    pp queue show 12            pp queue log 12 -f     every field + log tail / follow the log (across retries;
                                                       exits like wait)
    pp queue note 12 "OOM at bs=64; re-added as 15"    leave a note on a job (shown in status and show)
    pp queue wait 12-15                                block until they end, then print the outcomes (a failure
                                                       with its log tail); exit 0 ok, 1 any failed or dropped,
                                                       3 timeout
    pp queue wait 12 --timeout 30m                     ... giving up after 30 minutes (exit 3)
    pp queue watch 12                                  a line per event as it happens: start, retry, outcome
    pp queue wait --idle                               ... until nothing is queued or running
    pp queue wait --mine                               (an agent) every job this session queued that is live
                                                       or whose end no wait/watch in this session reported yet
    pp queue kill 12                                   waits while the worker stops it: killed 12 (stopped on SIGTERM)
    pp queue kill 12 --grace 2m                        ... giving it longer than its own grace to end
    pp queue drop 13    pp queue retry 11    pp queue reprio 14 0
    pp queue drop 20-29 -n                             a range or a shared tag: shows what it would do, then asks
    pp queue hold 15    pp queue release 15    pp queue clean --older 7d
    pp queue worker status | start -q NAME | stop -q NAME     `add` starts a queue's worker; stop leaves jobs running

Agents: run `pp queue wait ID` in the background (or `pp queue watch ID` under a monitor) and you are told
how the job ended, without polling and without anything typed into your terminal; `add` prints this hint to
an agent.

Selecting jobs: every verb that takes jobs takes the same five forms. 12 is one job by id (ids are
machine-wide), 12-15 a range of ids, NAME every job with that tag, --prefix P every job whose tag starts with
P (repeatable), and -q NAME --all every job of one queue (a usage error without -q). A tag is add --tag NAME,
at most 64 of letters, digits and . _ - : ; a job added without one has no tag and its id names it. -q
narrows any selection to one queue, and a tag or a prefix that names jobs in two queues is an error that
lists the ids. There is no glob. A job named by its own id must suit the verb, or it is an error; any other
form leaves the jobs it does not suit as they are and says which, so a driver that runs twice is no error the
second time, while a selection that matches no job at all is an error. kill, drop and clean show a selection
of more than one job and ask first (-y: do not ask, -n: only show).

Lower --prio runs first (default 10), ties by id. Each queue is strict head-of-line unless its own backfill
is on, so a big job is never starved by small ones.
PATH, VIRTUAL_ENV, CONDA_PREFIX and CONDA_DEFAULT_ENV are captured at add time, so `python` means what it
meant in your shell.

A job's environment, later entries winning: the worker's (that of the `pp queue add` that started it, without an
agent session's identity); PATH, VIRTUAL_ENV, CONDA_PREFIX and CONDA_DEFAULT_ENV from the shell that added the job;
the project's [queue] env; add's --env; then the six pp sets, which nothing overrides: PP_QUEUE,
PP_QUEUE_JOB, PP_QUEUE_RCFILE (where this attempt writes its exit code), PP_QUEUE_CONTAINER (a name for
whatever the runner starts elsewhere, one per attempt, beginning pp-<queue>-<id>-), PP_QUEUE_ENVFILE (a file
of KEY=VALUE lines, readable by you alone, for a runner to hand to a container) and CUDA_VISIBLE_DEVICES, the
cards --gpu-gb reserved as a comma-separated list, unset where the job reserved none. A variable you export
before `pp queue add` reaches the job only through the worker that add started, so pass a token or a secret
with --env or the project's env. `--json` gives the names of a job's own variables with every value as ***,
and `show` prints neither, so a secret you pass stays in the job's record and in the job.

PP_QUEUE_ENVFILE holds the project's [queue] env, the job's --env, CUDA_VISIBLE_DEVICES where a card was
reserved, PP_QUEUE, PP_QUEUE_JOB and PP_QUEUE_CONTAINER, and nothing else of this machine's: no PATH,
VIRTUAL_ENV, CONDA_PREFIX, CONDA_DEFAULT_ENV or PP_QUEUE_RCFILE, which would shadow an image's own python or
name a host path inside a container. A container runner passes it with --env-file and deletes any -e line for
a variable the file carries, since docker ranks -e above --env-file. It goes with the job's record.

Timeouts and retries
    --timeout DUR      the worker SIGTERMs the job's process group after DUR (90s, 30m, 2h, 1d) and SIGKILLs
                       it once the job's grace is up; the attempt fails with reason "timeout", and what that
                       comes to is under "How a job ends" below. `none` (or 0): no timeout, even when the
                       queue has a default one
    --grace DUR        between a stop of the job and the SIGKILL that follows it (default 10s), for a
                       timeout and for a kill of that job alike. `pp queue kill --grace DUR` overrides it
                       for the jobs that kill names.
    --retries N        re-run a failure up to N more times, with the SAME id: back to the queue after
    --retry-delay DUR  (default 30s, doubling each attempt, at most 1h). A job waiting out its delay never
                       holds up the queue. `wait` and `log -f` follow it across attempts.
    --retry-if REGEX   retry only failures whose reason ("exit 1", "timeout", "lost", ...) or last 200 log lines
                       match (^ and $ anchor per line); any other failure is final at once (wait, watch
                       and show say "not retried" and why). `--retry-if '^timeout$'` matches an attempt that
                       wrote no code of its own. Unset: every failure is retried.

Hooks, on the FINAL outcome of a job
    --on-fail CMD      a shell command run when the job has failed for good
    --on-done CMD      ... when it succeeded
    A hook never fires between retries, and never for a drop. Which hook an outcome owes is under "How a job
    ends" below.
    `none` turns a hook off. Hooks run detached in the job's directory with PP_QUEUE PP_QUEUE_JOB
    PP_QUEUE_EVENT (failed|done) PP_QUEUE_RC PP_QUEUE_REASON PP_QUEUE_LOG PP_QUEUE_CWD PP_QUEUE_TAG; their output
    goes to <queue>/hooks/<id>-<event>.log.

Settings: one default, one key, one flag, one order. Every setting has a built-in default, so nothing has to
be configured to start. Where one has more than one source, the first of these that gives it wins, and a key is
its flag's name without the dashes and with - written _ (retry_if, retry_delay, on_fail, on_done, gpu_gb,
mem_gb):

    add's flag  >  the project's .pp.toml [queue]  >  ~/.config/pp/config.toml [queue.NAME]  >  the default

A setting a JOB carries has a flag and a key in both tables, and what it comes to goes on the job's record, so
a later config edit never changes a job already queued. A setting a QUEUE carries has a key and no flag,
because its worker re-reads it every loop. env and need merge key by key; every other setting is taken whole
from one source. --tag, --hold and --cwd name one run, so they have no key: a shared default for any of them
would be wrong for every job that took it.

Per project, from the nearest .pp.toml up from the job's directory:

    [queue]
    runner = "docker/run.sh"         # put before every command added from this project (relative to the
                                     # project root); --no-runner skips it for one job
    stop = "group"                   # how a stop reaches the work the runner started: "group" where that
                                     # work stays in the job's own process group, else a shell command
    env = { HF_HUB_OFFLINE = "1" }   # added to each job's environment; add's --env wins
    timeout = "6h"                   # and any other per-job key, as this project's default

The job runs in the directory you queued it from (or --cwd), as `RUNNER CMD...`. The runner gets env and
CUDA_VISIBLE_DEVICES (the card --gpu-gb reserved) in its own environment. A container runner must pass them in
(docker run -e CUDA_VISIBLE_DEVICES -e HF_HUB_OFFLINE ...), or the job sees every GPU and none of env.

A project whose work runs through a runner says how a stop reaches that work, and `add` is refused where it
sets a runner and no stop: a runner that starts the work in a container, on a batch system or on another host
puts it where no signal of pp's reaches it. `--runner CMD` and `--stop CMD` say the same for one job, so a
single such job needs no .pp.toml. Both are read when the job is queued and kept on its record, so a config
edit does not change a running job and the worker that comes back after a restart carries out the same stop.
A job added with --no-runner has its work in its own process group, so its stop is that group whatever the
project says, and the record says so.

Resources: what a job declares (--gpu-gb, --cpu, --mem-gb, --need NAME=N; every job takes slots = 1) must be
provided by a pool and fit in EVERY pool that provides it. Shared pools count all queues' running jobs; a
queue's own pools count only its jobs, so a queue can be held to a share of a shared pool:

    [resources]                  # SHARED by every queue
    gpu_gb = "auto"              # per card: total - 2 GB, and live free memory from nvidia-smi   (default)
    cpu = "auto"                 # the machine's CPUs                                              (default)
    mem_gb = "auto"              # MemTotal                                                        (default)
    license = 4                  # a counter every queue draws from: --need license=1
    nodes = { probe = "sinfo -h -t idle -o %D", every = "1m", settle = "2m" }
                                 # a probe: the amount FREE now (first number the command prints), cached for
                                 # `every`; jobs launched less than `settle` before a reading are subtracted
                                 # from it. Add max = N to also cap what pp's jobs may hold.
    [queue.default.resources]    # this queue's OWN
    slots = 3                    #                                                                 (default)
    cpu = 16                     # this queue may use 16 of the shared CPUs
    gpu_gb = 40                  # ... and hold at most 40 GB per card
    [queue.dl.resources]
    net = 2                      # a counter only dl has: --need net=1
A pool is a number or { max, probe, every, settle }; `false` removes a default. settle is 30s unless set, for
gpu_gb and every probe: a job launched less than that before a reading may not show in it yet, so its
reservation is subtracted from the reading.

Config, ~/.config/pp/config.toml: a table per queue, re-read by its worker every loop, with no restart
needed. An unknown key or a bad value is an error, shown by `pp queue`; while it lasts the worker launches nothing, but
still records how running jobs end and enforces their timeouts. So does an empty file, which is what a file
rewritten in place is for a moment; remove it to use the defaults:

    [queue.default]
    timeout = "6h"       # defaults for jobs added to this queue (add's flags and the project's [queue] win)
    retries = 1
    retry_delay = "1m"
    retry_if = "Connection reset|503"
    on_fail = "notify-send 'pp job failed'"     # a shell command; default: none
    on_done = "none"
    grace = "30s"        # ... and every other per-job key: prio, env, need, gpu_gb, cpu, mem_gb
    idle_exit = 600      # the worker exits after this long with nothing queued or running
    poll = 2             # worker loop period, seconds
    backfill = false     # let later jobs that fit start while the head waits

A job runs in its own process group, inside an `sh` that writes the job's exit code to a file of that attempt's
own when the command ends. So the exit code outlives the worker, and a restarted worker adopts running jobs
instead of losing them. That `sh` is also what says the job is running, for as long as it runs; the command
itself reads /dev/null, so a process the command leaves behind does not keep the job running. The caller's
agent session (if any) is recorded as the job's owner and never passed to the job.

A queue's worker is what signals that queue's jobs, and only a worker holds the queue's worker claim. `pp
queue kill` writes a stop on the job and waits for the outcome that is recorded for it. It makes sure the
queue has a worker: it wakes the one that is up, or starts one where none is. So a kill with no worker up
starts one, and that worker carries out the stop and launches the queue's queued jobs. Two kills of one queue
come to the same thing: the one worker carries out both stops.

`pp queue worker stop` stops the worker that is up. It is not a pause: the next command that needs a worker
starts one.

A timeout is the worker writing a stop on the job itself, and nothing waits for it, so each step of it goes
out at the queue's poll. The worker signals the job's process group, escalates to SIGKILL once the grace is
up, and records how the job ended. Anything left in that group when the run is over goes with it, so a
command that held the signal back does not outlive its job. One actor ends a job, and a job that ended on its
own meanwhile keeps its exit code.

How a job ends, whatever reached it. The rules are tried in the order below, and the first that holds for a
job is what happens to it.

    While a run of the job is live its exit code says nothing about how the job ended: the job's own `sh`
    holds the run, and the command can write PP_QUEUE_RCFILE itself. What is done about such a job is what
    its stop asks for.

    A signal pp was refused is recorded as denied, and nothing more goes out for that stop.
    A stop nobody has signalled yet goes out as SIGTERM to the job's process group.
    A stop whose grace is up is escalated to SIGKILL, once.
    A stop inside its grace, or SIGKILLed already, is waited for.
    A job with no stop whose own timeout has run out gets a timeout's stop.
    A job with no stop whose time is not up is left as it is.

    Once the run is over the exit code the attempt wrote is the command's last word, and the job ends here. A
    stop someone asked for is never retried.

    A job a kill's signal stopped is recorded "killed" and fires no hook; an exit code the attempt wrote
    after that signal went out is kept as the attempt's code.
    A job that wrote its own exit code before a kill's signal went out, or that a kill wrote a stop for that
    no signal of pp's ever reached, ends with its own outcome, the exit code it wrote or the signal it died
    of, and fires the hook that outcome owes.
    An exit code of 0 is done, and on_done fires.
    A timeout's stop pp signalled or was refused is a timeout, which may be retried; an exit code the attempt
    wrote after that stop went out is kept as the attempt's code, and on a final timeout on_fail fires.
    Anything else is the job's own failure, the exit code it wrote or the signal it died of, retried while an
    attempt is left and --retry-if matches it, and on_fail fires on the last.

A stop decides the reason: the exit code an attempt wrote after its stop went out is that stop's outcome,
killed or timeout, and the code is kept as the attempt's own. A container runner writes the code of a
container the stop command ended, so without that rule a kill or a timeout of a container job would read as
the job's own failure.

One run of a job carries one stop. A kill's ask on a timeout's stop makes it a kill, a second ask keeps what
has gone out and only shortens the grace, and a refused stop is asked for again. `show` and --json say whose
stop it is, when it was asked for, what has gone out for it, its command's exit code where it had one, and
whether pp was refused, so nobody waits for a stop that is not going to be carried out.

Where a job's work is reached by the project's stop command, that command goes out in place of the SIGTERM to
the job's process group, detached, in the job's directory, with the job's own environment: its output goes to
the job's log and its exit code onto the stop's `rc`. The grace and the SIGKILL that follow are unchanged, so
a container that ignores its own shutdown is still ended on time, and `stop.rc: null` means pp has not seen
that command end rather than that it worked. Anything left in the job's process group when its run is over
goes with it, and a run that ended with no exit code of its own runs that stop command once more, so nothing
the runner started outlives the attempt that started it.

A job's record names the files that are its own: its log, the exit code each attempt wrote, and the log of
each hook that ran for it. `drop` and `clean` take those with the record, so neither leaves a file behind that
nothing names. State: ~/.local/state/pp/queue/<name>/.
"""
from __future__ import annotations

import os
import shlex
import shutil

from pp.lib import jobq, ui

def _add_args(p):
    p.add_argument("--no-runner", action="store_true", help="ignore the project's [queue] runner")


def _runner(spec, root: str, where: str, path: str | None) -> list[str]:
    """argv of the runner this job runs through; refuses (exit 2) one that could only fail at launch time,
    hours later. A path holding / is relative to the project root, and a bare name is looked up on PATH."""
    argv = shlex.split(spec) if isinstance(spec, str) else list(spec or [])
    if not argv:
        return []
    exe = os.path.expanduser(argv[0])
    if not os.path.isabs(exe) and os.sep in exe:
        exe = os.path.join(root, exe)
    if os.sep not in exe:
        if not shutil.which(exe, path=path):
            ui.die(f"queue: runner {exe} (from {where}) is not on PATH; or pass --no-runner", 2)
    elif not os.path.exists(exe):
        ui.die(f"queue: runner {exe} (from {where}) does not exist; or pass --no-runner", 2)
    elif not os.access(exe, os.X_OK):
        ui.die(f"queue: runner {exe} (from {where}) is not executable (chmod +x); or pass --no-runner", 2)
    return [exe] + argv[1:]


def _prepare(job: dict, a, project) -> None:
    """How this job's work is launched and stopped, onto the record: the runner put before its command, and
    the stop that reaches what that runner started. Both are rows of the settings declaration, so the flag
    wins over the project's key and nothing else gives either (pp/lib/jobq.py, SETTINGS).

    A job whose work runs through a runner needs a stop, because no signal of pp's reaches work the runner
    started elsewhere: such an add is refused here, where the runner is read. A job that skips the runner has
    its work in its own process group, so its stop is that group whatever its project says."""
    spec, where = (None, "") if getattr(a, "no_runner", False) else jobq.resolved(a, project, "runner")
    root = project.root or job["cwd"]
    job["runner"] = _runner(spec, root, where, (job.get("shell_env") or {}).get("PATH"))
    stop, _where = jobq.resolved(a, project, "stop")
    if not job["runner"]:
        job["stop_cmd"] = "group"       # its work is in its own process group, and pp's signals reach it
    elif stop:
        job["stop_cmd"] = stop
    else:
        ui.die("queue add: this job runs through a runner, so it needs a stop that reaches what the runner "
               f"starts: set [queue] stop in {root}/.pp.toml, or pass --stop CMD. Where the runner's work "
               'stays in the job\'s own process group, that is stop = "group".', 2)


def main(argv: list[str]) -> int:
    return jobq.run_cli(argv, "queue", "default", __doc__, prepare_job=_prepare, add_args=_add_args)
