r"""Run commands later, under resource limits, in named queues

One machine, one user's queues, any number of projects. Each named queue has its own limits, its own run order and its
own worker, and job ids are machine-wide, so the jobs of several projects share one card. A job is one command, and it
runs when the needs it declares fit the resource pools: slots, GPU memory (reserved per card AND read live from
nvidia-smi), CPUs, RAM, any counter you name, or anything a command reports as free. A job is retried, timed out,
stopped and reported as you ask.

Your first job needs no configuration at all, and there is no daemon to arrange and no worker to start by hand: `add`
starts the queue's worker itself.

    pp queue add -- make docs                       queue a command, run in this directory, in the `default` queue
    pp queue                                        every queue: running (with each job's last log line),
                                                    queued with what it waits for, held, recent
    pp queue wait                                   until every queued and running job has ended
    pp queue show 12       pp queue log 12 -f       every field and the log tail / follow the log across retries
    pp queue add --tag c3_recall.s0 --gpu-gb 36 -- python -m lab.run --cell c3 --seed 0
    pp queue add -q r1 --gpu-gb 38 --timeout 2h --retries 2 --retry-if 'CUDA out of memory' -- ./step.sh
    pp queue add --from stage1.txt --prio 1         one job per line, each line with its own flags
    pp queue add --after 41-66 -- python -m lab.report   once those jobs have ended, however they ended
    pp queue add --hold -- ./stage2.sh              queued, parked, until `release`; --no-runner skips the runner
    pp queue wait 12-15    pp queue wait --prefix c3_     block until they end, then print each outcome
    pp queue wait --idle   pp queue wait --mine     until nothing is left / (an agent) this session's own
    pp queue kill 12   pp queue drop 13   pp queue hold --prefix s2_   pp queue release --prefix s2_
    pp queue watch 12   pp queue note 12 "..."   pp queue reprio 14 0   pp queue retry 11   pp queue clean --older 7d

`status` is the verb the bare command runs: `pp queue` is `pp queue status`, and a flag goes there too, so `pp queue
--json` and `pp queue status --json` are one command. `status --here` is this directory's project only, `--project
NAME` one named project. -q narrows any verb to one queue, before or after the verb. A queue name begins with a
lowercase letter or a digit and holds letters, digits, - and _; `probes`, `watchers` and `counter` are taken, so
name a lane after its work (probe_runs). `pp help queue add`, or `pp -h queue add`, prints that verb's own flags.

A first project whose work runs in a container, in four steps

    1. Put three lines in the project root's .pp.toml:

           [queue]
           runner = "docker/run.sh"                  # put before every command added from this project
           stop = "docker rm -f $PP_QUEUE_CONTAINER" # how a stop reaches the work the runner started

       2. Copy the guarded arguments below into that runner's `docker run` line.
       3. Delete any -e line in the runner for a variable the project sets in [queue] env: docker ranks -e above
          --env-file, so a leftover -e wins over what pp hands the runner. A variable whose value in the image is
          not its value here (HOME, PYTHONPATH, a cache path of the image's) stays a -e and goes in no [queue]
          env, which the runner itself runs with: an image's HOME hides the docker client's own configuration.
       4. Prove the stop once: `pp queue add --tag stop_probe -- sleep 600`, then `pp queue kill stop_probe`, then
          look for what the runner started. It is gone. Then the same with `--tag stop_probe.t --timeout 15s`. An
          `add` is refused in a project that sets a runner and no stop.

Naming jobs
    --tag NAME is the job's name, and the name of the group of jobs that share it. A name is at most 64 characters of
    letters, digits and . _ - : , holds no `..`, is not `.` on its own, and may not read as an id or a range. Name a
    family by a common start (c3_recall.s0, c3_recall.s1, c3_shift.s0): --prefix c3_ is then the campaign and --prefix
    c3_recall one arm of it, and a prefix matches from the first character, so the part you select by goes first. A
    job added without --tag has no name, is selected by its id, and shows its command in a view. A tag is a name and
    never a key: adding a campaign file twice makes a second job per tag.

Selecting jobs
    Every verb that takes jobs takes the same five forms, and no others; `note` is the exception and takes
    one job, by its id or by a tag that names one:
        12              one job by id; ids are machine-wide, and `add` prints each one on stdout
        12-15           a range; one `add`'s ids are consecutive        c3_recall.s0   that tag, matched whole
        --prefix c3_    every job whose tag starts with c3_ (repeatable)  -q r1 --all    one queue's, whatever its tag
    A job named by its own id must suit the verb, or it is an error. A range, a tag, a prefix or --all leaves the jobs
    it does not suit as they are and says which, so a driver that runs twice is not an error the second time, while a
    selection that matches no job at all is an error. A tag or a prefix that spans two queues is an error that names
    the ids, so -q narrows it, and --all needs -q. There is no glob: 'c3_*' is your shell's business. kill, drop and
    retry show a plan and ask where a selection names more than one job, and clean asks for any; each of those four
    takes -y (do not ask) and -n (only show), so a driver with no terminal passes -y. To stop a whole set, `hold -q
    r1 --all` first, then `kill` and `drop` over it: a drop takes the held jobs too.

Order, and a stage after another
    Lower --prio runs first (default 10), ties by id, so in a lane (slots = 1) one campaign file's jobs run in its
    own order; `reprio` takes the new priority as its last word (`reprio --prefix s1_v2 0`). Each queue is strict
    head-of-line, so a big job is never starved by small ones; `backfill = true` in its config lets later jobs that
    fit start while the head waits. --after SELECTION holds a job back until every job that selection named has
    ended, in any outcome. Its value is ids, ranges and tags separated by commas (41,42,43 or 41-66 or
    s1_eval,s1_report), as many as a command line holds, a trailing or empty item ignored and a value that names no
    job refused. It is read when the job is added and the ids are kept, so nothing added later joins it, an id no job
    has is an error that queues nothing, and a dropped job counts as ended. A job waiting on --after is skipped
    rather than waited for, so it never becomes the head of its queue and never blocks the jobs behind it. `wait`
    counts a held job as not ended: a wait over a stage still held says so and then waits for ever, so wait for the
    stage before it and release this one.

        IDS=$(pp queue add --from stage1.txt)                         # one id per line on stdout
        pp queue add --from stage2.txt --after "$(echo "$IDS" | tr '\n' ',')"
        pp queue wait --prefix s1_ ; pp queue release --prefix s2_    # between stages: `;` whatever happened

Campaign files
    --from FILE (- for stdin) queues one job per line, and a line is the rest of an `add` command line: its own flags,
    then --, then the command.

        # one job per layer, 36 GB each: a comment is a line of its own, since a # inside a line is a word
        --tag s3_L00 --gpu-gb 36 -- python -m lab.run --layer 0
        # a line that is only a command still works
        python -m lab.smoke

    Flags on the `add` itself are the defaults for every line, and a line's own flags win: --env and --need merge key
    by key, every other flag is replaced. The queue, the file, --hold and --no-start are the `add`'s own, and a line
    that names one is refused, so a grid spread over two queues is one file and one `add` for each of them. A line is
    split into words the way a shell splits them, quotes and backslashes honoured, and then at its first bare --
    word; a line with no -- is the whole command, and one with no -- whose first word begins with - is refused.
    Nothing is expanded: no variable, no ~ and no glob, because a command is argv. An unquoted |, &, ;, <, >, (, ),
    ` or $( is refused with its line number; quoted they are ordinary, so a pipe is one argument of a shell you
    start: `-- sh -c 'python x.py | grep -v noise'`. A blank line and a line whose first character is # are skipped.
    Any refusal of any line queues nothing, an unknown flag and a bad duration and an --after id no job has alike: a
    campaign file is all or none.

Containers, and anything else a runner starts elsewhere
    A runner that starts the work in a container, on a batch system or on another host puts it outside the job's
    process group, where no signal of pp's reaches it, so a project with a runner says where its work is: `stop =
    "group"` where that work stays in the job's own group (a wrapper script, nice, uv run), else a command that ends
    it, which goes out first with pp's signals behind it. A container runner names its container $PP_QUEUE_CONTAINER
    and reads the job's environment from $PP_QUEUE_ENVFILE, one KEY=VALUE a line.

        #!/bin/bash                                   # the whole of a runner; safe with no PP_QUEUE_* set, so
        set -euo pipefail                             # the script's other callers keep working
        ROOT=$(cd "$(dirname "$0")/.." && pwd)        # the project root: the directory holding .pp.toml
        GPUS=${CUDA_VISIBLE_DEVICES:+device=$CUDA_VISIBLE_DEVICES}
        exec docker run --rm \
          ${PP_QUEUE_CONTAINER:+--name "$PP_QUEUE_CONTAINER"} \
          ${PP_QUEUE_ENVFILE:+--env-file "$PP_QUEUE_ENVFILE"} \
          --gpus "${GPUS:-all}" ${CUDA_VISIBLE_DEVICES:+-e CUDA_VISIBLE_DEVICES=0} \
          -v "$ROOT:/work" -w /work IMAGE "$@"

    Mount the project root worked out from the script's own place, not $PWD, or a job queued from a subdirectory gets
    that as /work. The GPUS line and the --gpus that reads it REPLACE a fixed --gpus all rather than joining it, since
    a second --gpus adds a device request instead of narrowing the first; the -e is there because the card docker
    gives a container is numbered 0 inside it; and the guard keeps a card-less job on --gpus all, where
    ${GPUS:+--gpus "$GPUS"} in its place gives a job that wants the image and no card none at all (--gpu-gb 0 is
    refused: a job declares an amount, or declares nothing). The runner process is the job, so it is given the job's
    whole environment and may read [queue] env and --env out of its own. --name is how a stop finds the container
    and how `docker ps` says which job it is: $PP_QUEUE_CONTAINER is one attempt's name, not one job's, so a retry
    never meets the container the last attempt left, and it begins pp-<queue>-<id>-, which is what to match; `pp
    queue show` prints the current one. --env-file is how the reserved card and the project's variables get inside.
    $PP_QUEUE_ENVFILE holds the project's [queue] env, the job's --env, CUDA_VISIBLE_DEVICES where a card was
    reserved, PP_QUEUE, PP_QUEUE_JOB and PP_QUEUE_CONTAINER, and no PATH, VIRTUAL_ENV, CONDA_PREFIX,
    CONDA_DEFAULT_ENV or PP_QUEUE_RCFILE of this machine's, which would name a python or a path outside the image.
    It is readable by you alone.
        A stop command runs detached in the job's directory with the job's environment, its output in the job's log.
    `docker stop -t 10 $PP_QUEUE_CONTAINER` waits for the container and `docker rm -f` does not, so keep a -t inside
    the job's grace (--grace, 10s by default): the SIGKILL follows the grace whatever the stop command is doing. The
    command is read when the job is queued and kept on the record, so a worker that comes back carries out the same
    stop on the same container. A --no-runner job's stop is its own process group whatever the project says, and
    nothing in pp names docker: a runner and a stop are commands.

Settings: one default, one key, one flag, one order
    Every setting has a built-in default. Where one has more than one source, the first of these that gives it wins,
    and a key is its flag's name without the dashes and with - written _ (retry_if, retry_delay, on_fail, on_done,
    gpu_gb, mem_gb):

        add's flag  >  the project's .pp.toml [queue]  >  ~/.config/pp/config.toml [queue.NAME]  >  the default

    A setting a JOB carries has a flag and a key in both tables, and its value goes on the job's record, so a later
    config edit never changes a job already queued. A setting a QUEUE or the MACHINE carries has a key and no flag,
    because its worker re-reads it every loop. env and need merge key by key; every other setting is taken whole from
    one source. gpu_gb, cpu and mem_gb are per-job keys of [queue.NAME], what a job declares, and pool keys of
    [resources] or [queue.NAME.resources], what is provided. Every per-job flag but --tag, --hold, --after, --cwd,
    --no-start, --from and --no-runner is also a key in both tables; those seven name one run or one add, and
    --runner and --stop have the project's key alone.
        Per project, in the nearest .pp.toml up from the job's directory, where a relative path is relative to that
    file's own directory: `name = "lab"` at the top level names the project in every status row, and [queue] takes any
    per-job key as this project's default, `log = "logs/{tag}.log"` among them, and two of its own: `runner =
    "docker/run.sh"` (a path with a / is in the project, a bare name is on PATH) and `stop = "docker rm -f
    $PP_QUEUE_CONTAINER"` (a command, or "group"; required where there is a runner). --runner CMD and --stop CMD on
    `add` say both for one job, with no .pp.toml at all. Per machine and per queue, in config.toml:

        [queue.NAME]            poll = 2 (the worker's loop period in seconds), idle_exit = 600 (it exits after
                                this long with nothing queued or running), backfill = false, and any per-job key
        [queue.NAME.resources]  this queue's OWN pools: slots = 3 by default, so slots = 1 makes a lane, and
                                gpu_gb = 76 holds this queue to 76 GB of a card, leaving room for another tenant
        [resources]             SHARED by every queue: gpu_gb = "auto" (per card, total - 2 GB, with live free
                                memory from nvidia-smi), cpu = "auto" (the machine's CPUs), mem_gb = "auto"
                                (MemTotal), solo = 1 (a counter every queue draws from: --need solo=1), and
                                nodes = { probe = "sinfo -h -t idle -o %D", every = "1m", settle = "2m" }

    What a job declares (--gpu-gb N, --cpu N, --mem-gb N, --need NAME=N; every job takes slots = 1) must be provided
    by a pool and must fit in EVERY pool that provides it. Shared pools count every queue's running jobs; a queue's
    own count only its own. A pool is a number, "auto", false, or a table of max, probe, every and settle; a bare
    number means max, and `false` removes a pool a default would otherwise give, in that scope. A probe is a command
    printing the amount FREE now, the first number it prints, cached for `every` (30s unless set); max = N beside it
    also caps what pp's own jobs may hold. gpu_gb takes no probe, since nvidia-smi is its reading: one on it is
    refused, gpu_gb = { settle = "2m" } is legal, and a max of its own is what that scope's jobs may reserve on one
    card, which the live free reading has to allow besides. settle (30s unless set, for gpu_gb and every probe) holds
    a job's reservation against a reading it may not show in yet. An unknown key or a bad value is an error that `pp
    queue` names; while it stands the worker launches nothing, but still records how running jobs end and enforces
    timeouts. A counter is how a job gets a card to itself: `solo = 1` above, and --need solo=1 on every job that
    must not share.

Where a job's output goes
    `log` in a project's [queue], or --log PATH on one job, is a path in which {tag} and {id} are filled in, those two
    and no others. {tag} is the job's tag, or its id where the job has none, so nothing is refused for a missing tag.
    A path holding `..` is refused, which keeps a relative one inside the project root; otherwise it is relative to
    that root, or absolute. The job's whole output goes there, appended, every attempt in that one file, with a `[pp
    queue]` line before each naming the job and the command, and several jobs may name one file. Without `log` it is
    <state>/queue/<name>/logs/<id>.log. Either way the record names it, and `log`, `show`, a failure's tail and
    PP_QUEUE_LOG follow the record, never the layout. --retry-if, a failure's tail and `log -f` read from where the
    attempt began, so a pattern never matches an earlier attempt or pp's own line, and a job with --retry-if wants a
    file of its own. `drop` and `clean` remove what pp put in its store, never your log.

Timeouts and retries
    --timeout DUR (90s, 30m, 2h, 1d, or `none` and 0 for no timeout even where the queue has a default one) stops the
    job after DUR: its stop, then SIGKILL to its process group after the job's grace, and the attempt fails with
    reason "timeout". --grace DUR on `add` is that gap (default 10s), for a timeout and for a kill alike, and `pp
    queue kill --grace DUR` overrides it for that kill. --retries N (default 0) re-runs a failure up to N more times
    with the SAME id, back to the queue after --retry-delay DUR (default 30s, doubling each attempt, at most 1h); a
    job waiting out its delay never holds up the queue, and `wait` and `log -f` follow it across attempts. --retry-if
    REGEX retries only failures whose reason ("exit 1", "timeout", "lost") or whose own attempt's last 200 log lines
    match: a python regular expression, searched case sensitively, ^ and $ anchoring per line. One pattern is tried
    against both, so write one a log line will not match, and alternation puts two patterns in one: ^timeout$, CUDA
    out of memory|^lost$. Any other failure is final at once, and wait, watch and show say "not retried" and why;
    with no pattern, every failure is retried.
        `retry` re-runs an ended job by hand: it queues a new job with a new id, carrying everything but the outcome,
    and over a selection of more than one job it shows a plan and asks. A job that succeeded has ended, so it suits
    retry too and `retry --prefix s1_` re-queues the finished runs with the failed ones: type -n first. A priority is
    set with `reprio` on the job the retry queued. There is no selection by outcome, so the jobs a reboot lost are
    the records of `pp queue --json` whose state is failed and whose reason is lost, and `retry` takes their ids as
    words.

Hooks, on the FINAL outcome of a job
    --on-fail CMD      a shell command run when the job has failed for good
    --on-done CMD      ... when it succeeded. Set both to one command for "whatever happens, do this next".
    A hook never fires between retries, and never for a drop or a kill; `none` turns one off. No runner is put
    before a hook, and a hook runs in the worker's own environment with the names below and nothing else of the
    job's, so one that needs the image calls the runner itself. Hooks run detached in the job's directory with
    PP_QUEUE PP_QUEUE_JOB PP_QUEUE_EVENT (failed|done) PP_QUEUE_RC PP_QUEUE_REASON PP_QUEUE_LOG PP_QUEUE_CWD
    PP_QUEUE_TAG, their output goes to <queue>/hooks/<id>-<event>.log, and the record names each hook that ran. A
    hook runs at least once: a worker killed while starting one leaves it owed and the next worker runs it, so write
    one a second run does no harm.

How a job ends, whatever reached it. The rules are tried in the order below, and the first that holds for a job is
what happens to it. While a run of the job is live its exit code says nothing, since the command can write
PP_QUEUE_RCFILE itself: what is done about such a job is what its stop asks for.

    A signal pp was refused is recorded as denied, and nothing more goes out for that stop.
    A stop nobody has signalled yet goes out as SIGTERM to the job's process group.
    A stop whose grace is up is escalated to SIGKILL, once.
    A stop inside its grace, or SIGKILLed already, is waited for.
    A job with no stop whose own timeout has run out gets a timeout's stop.
    A job with no stop whose time is not up is left as it is.

    Once the run is over the exit code the attempt wrote is the command's last word, and the job ends here. A stop
    someone asked for is never retried.

    A job a kill's signal stopped is recorded "killed" and fires no hook; an exit code the attempt wrote after that
    signal went out is kept as the attempt's code.
    A job that wrote its own exit code before a kill's signal went out, or that a kill wrote a stop for that no signal
    of pp's ever reached, ends with its own outcome, the exit code it wrote or the signal it died of, and fires the
    hook that outcome owes.
    An exit code of 0 is done, and on_done fires.
    A timeout's stop pp signalled or was refused is a timeout, which may be retried; an exit code the attempt wrote
    after that stop went out is kept as the attempt's code, and on a final timeout on_fail fires.
    Anything else is the job's own failure, the exit code it wrote or the signal it died of, retried while an attempt
    is left and --retry-if matches it, and on_fail fires on the last.

One run of a job carries one stop: a kill's ask on a timeout's makes it a kill, a second ask keeps what has gone out
and only shortens the grace, a refused stop is asked for again, and `show` and --json say whose stop it is, what has
gone out for it, its command's exit code where it had one, and whether pp was refused. Where a job's work is reached
by the project's stop command, that command goes out in place of the SIGTERM, and `stop.rc: null` means pp has not
seen it end, not that it worked. Anything left in the job's group when its run is over goes with it, and a run that
ended with no exit code of its own, or that a stop was asked of, runs that stop command once more, so nothing the
runner started outlives its attempt.

Workers, and after a reboot
    One worker per queue starts what fits, reaps what ended, carries out stops, re-queues retries and fires hooks.
    `add`, `release`, `retry`, `kill` and `wait` start one where none is up, so there is no nohup recipe: only a
    worker holds the queue's claim, so two cannot run on one queue and double its limits. A job runs in its own
    process group inside an `sh` that writes the attempt's exit code to a file of its own, so that code outlives the
    worker and a worker that comes back adopts running jobs instead of losing them. `pp queue worker stop` is not a
    pause, since the next command that needs a worker starts one; `pp queue work -q NAME` runs one in the foreground
    for a supervisor or a unit file; and --no-start on add, release, retry, wait and watch does the verb's work and
    starts none. After a reboot the records need nothing arranged: a job that was running when the machine went
    down has no exit code and no live run, so the first worker up records it failed with reason "lost", and retries
    it where --retry-if matches "lost". A job left queued waits for a worker, which one of the five verbs above
    starts, or `pp queue worker start -q NAME` does, for the one queue it names.

A job's environment, later entries winning: the worker's own, which is that of the `pp queue add` that started it,
without an agent session's identity;
PATH, VIRTUAL_ENV, CONDA_PREFIX and CONDA_DEFAULT_ENV from the shell that added the job; the project's [queue] env;
add's --env K=V; and then PP_QUEUE, PP_QUEUE_JOB, PP_QUEUE_RCFILE, PP_QUEUE_CONTAINER, PP_QUEUE_ENVFILE and
CUDA_VISIBLE_DEVICES, the cards --gpu-gb reserved, unset where it reserved none.
PATH, VIRTUAL_ENV, CONDA_PREFIX and CONDA_DEFAULT_ENV are captured at add time, so `python` means what it meant where
you typed it, and a variable you export before `pp queue add` reaches the job only through the worker that add
started: pass a token with --env or [queue] env. An env name holds no =, NUL or newline, a value no newline.
--json gives the names of a job's own variables with every value as ***, and `show` neither.

Agents
    `add` prints each job's id on stdout, one per line, with everything else on stderr, and for an agent session the
    one command to run next. Run `pp queue wait ID` in the background (or `pp queue watch ID` under a monitor) and you
    are told how the job ended, with a failure's log tail, without polling. `wait --mine` is every job this session
    queued that is live, or that ended without a wait or watch of this session reporting it; `wait --idle` waits until
    nothing is queued or running, which a held job does not hold. `wait --timeout DUR` and `watch --timeout DUR` take
    the duration spellings --timeout on add takes (90s, 30m, 2h, 1d, and a bare number is seconds) but no `none`,
    and have no default: without one, a wait waits. A wait over a stage with a job still held is the one that needs
    it, since that wait never returns on its own, and exit 3 is a real outcome there. Exit statuses: 0 it worked; 1
    a job failed or was dropped, or a config file is wrong; 2 a usage error or a refusal; 3 wait or watch gave up at
    its --timeout; 130 interrupted. So `wait --prefix s1_ && ./next.sh` is a whole driver.
        --json on status, show and worker status is the machine surface, and the only one: read fields from it and not
    from the text, and ignore a field you do not know. One object per job, out of that job's record. The fields to
    count on: id, state (queued, held, running, done, failed), queue, tag, project, cwd, cmd and runner (each an
    argv), stop_cmd ("group", or the command that reaches what the runner started), prio, needs, cards, after, grace,
    timeout, retries, attempt, rc, reason, not_retried, added, started, ended, seconds, log, log_at, container,
    envfile, rcfile, owner, history (one entry per attempt that ended, with its own files and cards), notes, reported,
    hooks, stop (by, t, grace, timeout, sent, kill9, denied, was, cmd, rc), env and shell_env (their names, every
    value ***), and from `show` also watchers. Every one of those is on a job in every state, null or empty where the
    record does not hold it yet, but `after` is there only on a job that names the jobs it waits for and `stop` only
    on a job asked to stop. A field is added and never renamed or retyped inside a minor version.

What is left to you, on purpose
    A dependency graph: --after is ids that already exist, with no cycles and no forward references. A log read as a
    result: make the command exit non-zero, or read its log in --on-done. Progress, a percentage and a finish time:
    `pp queue` shows elapsed time and the last log line, and `show --json` has every attempt's duration. Results,
    which the job writes where its project wants them; `pp queue note` puts a reading beside the job. Skipping work
    already done, which the plan that writes your campaign file knows and pp does not. More than one machine: a
    submitting runner (stop = "scancel $PP_QUEUE_JOB"), or `ssh host ...`.

State: ~/.local/state/pp/queue/<name>/, all plain files. A job's record names every file that is its own: its log, the
exit code each attempt wrote, the environment file its runner was handed, and each hook's log. Read a path from the
record, not from the layout; `drop` and `clean` take those files with the record, and PP_STATE_DIR moves the store.

What may change. Verb and flag names, the values a flag takes, config keys and their defaults, the five selection
forms, the defaults printed above, --json field names and types, the exit statuses, and the variables a job, a runner
and a hook are given are the interface: within a minor version they keep their names and their meaning, and anything
new is added beside them. A message's wording, a view's layout, row order and widths, a plan a -n prints, and the
names of store files no record names are not.
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
        exe = os.path.normpath(os.path.join(root, exe))     # one path, however the project wrote it
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
