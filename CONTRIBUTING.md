# Contributing

## Issues and pull requests

- Report a bug with what you ran, what it printed, `pp --version` and your
  Python version.
- Open an issue before a pull request that changes behaviour, so the change
  is agreed first. A typo or a small fix can go straight to a pull request.
- Keep a pull request to one change.

## Code written with AI

Code written with an AI assistant is welcome, if you have read and
understood all of it. Issues, pull request descriptions and your answers
to comments are written by you, not by an agent: you must be able to
explain why the change is right. A pull request that looks generated, and
not understood, is closed.

## Running pp from a clone

```
git clone https://github.com/xflash96/pp ~/src/pp
mkdir -p ~/.local/bin && ln -s ~/src/pp/bin/pp ~/.local/bin/pp
```

If you installed pp as a tool, `uv tool uninstall ppkit` (or
`pipx uninstall ppkit`) first: it uses the same `~/.local/bin/pp`.

## Building and testing

You need Python 3.11.4 or later, git, sh and bash. The tests that need zsh
skip without it. `ci/check` uses [uv](https://docs.astral.sh/uv/)
for the linter and for other Python versions.

```
ci/check                                 the linter, then every test, on python3
ci/check 3.11.4                          the same on Python 3.11.4, which uv installs the first time
ci/check --cpus 2 3.11.4                 the same on 2 CPUs, like a CI runner
python3 -m unittest discover -s tests    the tests alone
ci/package                               build ppkit into dist/, install it fresh, and try it
```

CI runs `ci/check 3.11.4`, `ci/check 3.14` and `ci/package` on every push to
main and every pull request. The whole suite takes a few minutes. Most of
that is the journeys (`tests/test_journey_*.py`), which run the real
`bin/pp` once per step in a throwaway home directory.

A test that fails only sometimes has a race in it. Fix the race rather
than running the test again (see Tests). `ci/check --cpus 2` runs the
suite on 2 CPUs, with a process table of its own (`unshare`). A test
that passes only because a worker or a server always gets a CPU at once
fails there, as it would on a CI runner. So does one that passes only
because a scan of `/proc` is slow: a scan of the thousand or so processes
on a developer's machine takes tens of milliseconds, and a runner's few
hundred take a few. The test that needs another user's process is skipped
there.

A uv-managed CPython has no `os.pidfd_open`, so `ci/check 3.11.4`, `ci/check
3.14` and CI run `pp/lib/proc.py`'s ctypes syscall path, while the distro
`python3` of a plain `ci/check` runs the wrapped calls. Both are tested only
by running both.

## The code

- `bin/pp` finds the tree it lives in, wherever it is linked from, and runs
  `pp.cli`.
- `pp/cli.py` finds the commands, runs one, and prints help and completion.
  It knows no command by name.
- `pp/lib/` holds what commands share. "How pp works" below lists those
  modules and what each one owns.
- `pp/commands/NAME.py` is one command.
- `tests/` has the unit tests, and the journeys with their harness,
  `tests/journey.py`.
- `ci/check` and `ci/package` are what CI runs; `.github/workflows/ci.yml`
  runs them, and `release.yml` publishes a tagged version.
- `pyproject.toml` builds the ppkit package, and `.pp.toml` lets
  `pp promote` release pp itself.

## Adding a command

`pp cmd new NAME` writes `pp/commands/NAME.py`, `tests/test_NAME.py` and
`tests/test_journey_NAME.py`. A command file has:

1. A docstring. Its first line is the summary that `pp` lists, in about 60
   characters or fewer. The whole docstring is what `pp help NAME` prints,
   so put examples in it.
2. `def main(argv: list[str]) -> int`, which builds its parser with
   `ui.parser("NAME", __doc__)`.
3. If you want short names for it, `ALIASES = ["x"]`, written as a literal
   list. A name one machine wants is `[alias]` in its own `config.toml`
   instead (`pp cmd alias x NAME`), which beats both `ALIASES` and a
   prefix and is checked against the command names before it is written.

These rules keep pp working as commands come and go:

- Import only the standard library at the top of the file. A third-party
  import goes inside the function that needs it, with a clear message when
  it is missing. `tests/test_cli.py` checks this.
- Commands do not import each other. Shared code goes in `pp/lib/`. Any
  command file can be deleted and the rest keep working.
- State goes under `paths.state_dir("NAME")`. Settings come from
  `config.load("NAME")`, the `[NAME]` table of `~/.config/pp/config.toml`,
  and for a project from the nearest `.pp.toml` (`config.find_project()`).
- Settings are checked. An unknown key or a bad value raises
  `config.ConfigError` with its name (`config.check_keys`,
  `config.parse_duration`), and pp prints it and exits 1. The docstring of
  `pp/lib/config.py` says which command owns each table.
- A command that changes or deletes things shows its plan first, unless the
  user named each thing exactly (a pid, a job id): those are acted on at
  once. `-n` shows the plan only, and `-y` goes ahead without asking.
  Otherwise it asks with `ui.confirm()`, which refuses when there is no
  terminal. Agents run without one, so they pass `-n` to look and then `-y`
  to act.
- Anything that lists or reports status has `--json`. Agents read it, so a
  change to a field goes in `CHANGELOG.md`.
- Exit statuses follow the table under "Exit status" below. Errors go
  through `ui.die(msg, code)`.
- Text from elsewhere (another program's command line, a plugin's file,
  git's output) goes through `ui.printable()` before it is printed, so an
  escape sequence in it cannot redraw the terminal.
- Anything two things could do at once goes through its construct: start a
  process with `proc.spawn`, signal one through `proc.Handle`, lock with
  `lock.locked`, read-modify-write a record with `paths.json_txn`. "The
  constructs" below is the list, and `tests/test_constructs.py` fails on a
  call that does one of those jobs itself.
- A process pattern must not match the caller or the processes that started
  it (`proc.ancestors()`). `pkill -f PAT` kills the shell that runs it,
  because PAT is in that shell's command line.
- Long work goes through `pp queue`, and the caller finds out how it ended
  with `pp queue wait` or `pp queue watch`. pp does not type into terminals
  to report back.
- Completion reads the docstring. TAB offers the verbs of its `pp NAME VERB`
  example lines and the flags argparse knows, so give every verb an example.

## Tests

- Unit tests go in `tests/test_NAME.py` and enter `tests/sandbox.py`'s
  scope: `self.sb = sandbox.enter(self)`, first in `setUp`. The docstring of
  `tests/sandbox.py` says what that scope owns and in what order it gives it
  back, so a test reads and writes a tree of its own and never yours. `pp
  cmd new` writes a test that enters it.
- Journeys go in `tests/test_journey_NAME.py` and use `journey.Journey`.
  The docstring of `tests/journey.py` says what its sandbox keeps away from
  your machine. Every example in a command's docstring runs in its journey,
  mistakes included. The help text is the spec: `tests/test_journey_cli.py`
  fails when an example's verb does not appear in its journey file. pp
  itself and `completion` are walked in that file.
- A journey step that uses a command other than its own is guarded with
  `has()` or `needs()`, so a tree with some commands removed still passes.
- Two things that touch the same job, file or port at once (a worker and a
  command, two workers, a signal and a write) go through a construct that
  is written once. "The constructs" below is the list of them, and a check
  added at one call site is not a fix.
- A test checks a contract, not a wording. Every sentence pp prints may be
  reworded in a patch, so a test asserts an exit status, a `--json` field, a
  record field, a file's existence or its mode, an id, or a state, and nothing
  else. Where a test has to know that pp said something, it asserts the field
  the sentence was made from. Two exceptions are written down where they are
  made: a journey types the examples of `pp help NAME`, and one journey step
  holds the headings and the line width of `pp help queue`.
- A test of a construct checks what the construct promises. A test that
  runs a second actor at an exact step keeps one fix from coming back
  (`TestTwoAtOnce` in `tests/test_queue.py`). That step goes through
  `journey.Gate`, never a flag of the test's own, so the condition and the
  step are one move; `Gate`'s docstring is the contract.
- A test waits for the state it needs (`journey.until`, or a file the job
  waits for), never for a while. A sleep that is long enough on this
  machine is not long enough on a CI runner: 2 CPUs, a few hundred other
  processes, and tens of milliseconds before a worker that was just started
  gets one.
- A process a test starts beside itself (a server, a victim, a worker)
  comes from `journey.start` or `Journey.child`, and nothing acts on it
  before it says it is up: the `ready` line those wait for, or a state a
  bounded `until` waits for. It has exec'd by then, but a handler it
  installs is not installed, a port it binds is not bound, and its command
  line may still read empty.

## Exit status

| status | meaning |
|---|---|
| 0 | it worked |
| 1 | it failed, or a config file is wrong (pp names the file and the key) |
| 2 | a usage error, or a refusal: something it checks first is wrong, or it will not act without asking |
| 3 | `pp queue wait` or `watch` gave up at its `--timeout` |
| 130 | interrupted (Ctrl-C) |

A `pp-NAME` program's own exit status passes through unchanged.

## How pp works

Each module's docstring is the reference for its part, and this is the one
list of the parts:

- `pp/cli.py`: finding commands, the order of their origins, help and
  completion.
- `pp/lib/jobstore.py`: a job's record, the files it lives in, and the
  transaction that changes one. `pp/lib/jobcore.py`: the worker, the claims
  and the processes of a queue. `pp/lib/jobpolicy.py`: every rule of a queue
  as a value, so each one is a table. `pp/lib/jobq.py`: the verbs, the views,
  the resources and the CLI; `pp help queue` is the user's view of the same.
- `pp/commands/promote.py`: releases. `pp/commands/cmd.py`: plugins.
  `pp/commands/http.py`: serving a folder to a browser, with its cache and
  its server. `pp/commands/batch.py`: bulk file changes, and the journal
  that undoes them.
- `pp/lib/session.py`: agent sessions. `pp/lib/paths.py`: where pp keeps
  things, and the transaction a record goes through. `pp/lib/config.py`:
  settings, and who owns each table.
- `pp/lib/proc.py`: what `/proc` says about a process, the handle every
  signal goes through, and the spawn every process start goes through.
- `pp/lib/lock.py`: the lock two processes go through, what a scope that
  holds a signal back promises, and the claim that says who is alive.
- `pp/lib/ui.py`: parsers, questions, tables, and the escaping every piece
  of text from elsewhere goes through.

### The constructs

`tests/test_constructs.py` reads the source and fails on a call that does a
construct's job anywhere else, named by file and function. Its `RULES` is
this table, and each rule's own docstring is its contract.

| rule | what it forbids elsewhere | the construct | its home |
|---|---|---|---|
| `signal` | signalling a process | `Handle`, `signal_pid`, `signal_group`, `signal_and_wait` | `pp/lib/proc.py` |
| `spawn` | starting a process | `spawn`, `run`, `git`, `exec_replace` | `pp/lib/proc.py`, `tests/journey.py` |
| `alive` | judging a process alive by its pid | a claim's holder, or `Handle` | `pp/lib/proc.py` |
| `lock` | taking a lock | `locked`, and `deferring` for a scope with no lock | `pp/lib/lock.py` |
| `claim` | opening a claim's file | `Claim` | `pp/lib/lock.py` |
| `job` | reading or writing a job's file | `txn`, `listing`, `find` | `pp/lib/jobstore.py` |
| `worker-claim` | taking a queue's worker claim, or reading its holder | `worker`, `worker_now`, `work` | `pp/lib/jobcore.py` |
| `job-process` | signalling or reaping a job's processes | the `Worker` class, `worker_stop`, `work` | `pp/lib/jobcore.py`, `pp/lib/proc.py`, `pp/commands/kill.py` |
| `by-name` | deciding on a served file by its name | act on the open file | `pp/commands/http.py` |
| `sleep` | sleeping in a test | `until` | `tests/journey.py` |
| `once` | a once-flag of a test's own | `Gate` | `tests/journey.py` |
| `environ` | changing `os.environ` in a test | the scope | `tests/sandbox.py` |

`KNOWN` in that file is the exceptions a rule allows, each with its reason
beside it. An exception that no longer does the thing fails too, so it is
taken off rather than left standing. The only ones are two `TestStore` tests
that write a half-finished name and a foreign layout on purpose.

Two more have no rule, so a command author follows them from here: a
read-modify-write of a JSON record goes through `paths.json_txn`, and
replacing a whole file in one step goes through `paths.write_json`.

### Three ways pp runs

- **From a clone.** `bin/pp` puts the clone on `sys.path`, and `pp cmd new`
  writes into the clone.
- **From a release made by `pp promote`.** The same, from a copy that holds
  `.pp-release.json`. `pp cmd new` writes into the tree that release came
  from, since the copy is replaced at the next promote.
- **From an installed package** (`uv tool install ppkit`). There is no
  `bin/pp`, so a queue's worker starts as `python -P -m pp`, and
  `pp cmd new` writes only with `--user`.

`paths.self_cmd` and `cmd._source_tree` tell the three apart.
`tests/test_cmd.py` and `ci/package` keep the installed case working, and
the journeys in `tests/test_journey_cmd.py` the other two.

### The queue's four modules, and what may go where

The line between them is mechanism against policy, and a change goes to the
module whose job it is:

- `pp/lib/jobstore.py` is the record store and its transaction: one lock, no
  timing, no process, no signal. A field a record gains and a write a verb
  hands over go here.
- `pp/lib/jobcore.py` is the worker and the claims: starting, stopping,
  reaping and adopting a job's processes, the worker's loop, and the watchers.
  A bug here loses a job, which is why it is the module held still.
- `pp/lib/jobpolicy.py` is every rule that is values in and one value out: how
  a job ends, what a selection names, what a setting comes to, what a job's
  environment is. A rule goes here, and a table tests it.
- `pp/lib/jobq.py` is the verbs, the views, the readings, the one settings
  declaration and the CLI, and `pp/commands/queue.py` is the command and its
  help. Every sentence a user reads is worded here.

The store and the worker together are the core. These rules hold that line,
and `tests/test_constructs.py` reads the source for each:

- The boundary rule. Outside the core no function takes the store lock, and
  none starts a job, a runner, a stop command, a hook or a worker, or signals
  a job, a worker or a process group: a verb goes through the core's API, where
  every write is one call and one whole transaction. Two sites outside it run a
  process of their own, and both are named exemptions: `reading`, which runs a
  user's probe under that probe's own claim, and `_cards`, which runs
  nvidia-smi where `[resources] gpu_gb` gives no probe of its own.
- The purity rule. A function of `jobpolicy.py` reaches no clock, no store, no
  process, no signal and no file. It is handed records, readings, settings and
  `now`, and gives back a value, so a rule that could be raced is a rule no
  table tests.
- The import rule. `jobpolicy.py` imports nothing of pp's, the store imports
  nothing of the worker, and neither imports the verbs above them. The
  direction is a line, so no cycle can hide a rule that reads a mechanism.
- The core reads no config file and takes no reading of its own: `read_settings`
  hands a worker the settings of one pass of its loop, and `admit_for` the
  readings of one launch pass. It writes no sentence either: it tells a sink
  what it did, and `jobq` words every one of them.
- The budgets, in code lines that are neither blank, comment nor docstring:
  250 for the store, 490 for the worker, 730 for the two together. A feature
  that cannot be built without going over one of them waits until something
  leaves the core, and moving a rule out into `jobpolicy.py` is the first thing
  to try.
- Every function of the store and of the worker is named in one of the two
  lists in that file, so a construct is placed when it lands, and a name that
  has gone comes off the list in the same change.

### Where a queue's jobs live, and how one says it is alive

The docstring of `pp/lib/jobstore.py` is the reference for the files a queue
is made of, and `pp/lib/jobcore.py`'s for how a worker and a job say they are
alive and which actor ends a job. Each mechanism has one owner, and these are
they:

- the store: `Txn`, `txn`, `listing` and `find`, in `pp/lib/jobstore.py`.
  `open_queue` is where a queue's `FORMAT` is checked, and `Txn.add` is where
  an id and the record that holds it land together. The writes a verb hands
  over are the four at the end of that file, each one a whole transaction of
  its own that is told the states it may act on.
- being alive: `lock.Claim`. A claim is held for as long as its holder lives,
  so nothing is cleaned up after a holder that was killed and no record is
  ever read as a live holder. A queue's worker holds one on `worker.lock`,
  one run of a job on `jobs/<id>.run`, a probe and a `wait` lease likewise.
- whether a queue's worker is up: the block comment above `worker_up`, for
  the two things that can be true of that claim and the first moment that is
  neither.
- how a job ends: `decide` and `merge_stop` in `pp/lib/jobpolicy.py`, which
  are pure, as every rule in that module is, and `Worker.carry_out`, the one
  pass that gathers the facts and carries the action out.
- what a worker is handed: it reads no config file and takes no reading of
  its own, so `read_settings` gives it the settings of one pass of its loop
  and `admit_for` the readings of one launch pass. It writes no sentence
  either: it tells a sink what it did, and `jobq` words every one of them.
- what a stop carries: `public()`, where every field of a job record that
  others rely on is named.

### Who stops a job

A queue's worker signals that queue's jobs, and nothing else does. Only a
worker ever holds the queue's worker claim, and `work()` is the one place
that takes it. So the worker is the one owner of a stop, whoever asked for
it: a verb writes the stop on the job in one transaction, makes sure the
queue has a worker, and waits for the record of how the job ended. Two kills
of one queue, a kill and a timeout, and a kill with no worker up all come to
the same place.

A kill, pass by pass:

```
kill:   ask_stop(merge_stop) -> worker_up(wake=True) -> SIGUSR1 to the worker
worker: carry_out: the facts -> decide = TERM -> the project's stop command,
        or the SIGTERM to the group, then txn marks `sent`
     -> (the grace) -> decide = KILL9 -> signal
     -> the wrapper ends, so the job's run claim is free
     -> decide = ENDED_AS -> _finish records the outcome and the hook it owes
     -> fire
kill:   _await_stop reads done or failed, and prints it
```

A job's states are `queued` or `held`, then `running`, then `done` or
`failed`; a retry takes it from `running` back to `queued`. Every pass of the
kill's wait wakes the worker again, so what the kill waits out is the job's
own exit time and not the queue's poll period.

The rules the worker follows are the ones `pp help queue` states under "How a
job ends, whatever reached it", and that help text is their one statement.
Two more are for contributors: which of the two a stop is comes from the
`timeout` field a worker writes on the stop it wrote itself, never from the
name of whoever asked; and an exit code the attempt wrote after its stop went
out is that stop's outcome, with the code kept, since a container runner writes
the code of a container the stop command ended.

`TestTheDecision` in `tests/test_queue.py` is a table over every combination
of those facts. Each row carries the rule from that help as its own label,
and the table checks that every label is a sentence of the help and that
their order there is the order the rules are tried in. A combination no rule
covers fails, and the table needs no process, thread or clock. What stays
process-level is that the facts are gathered right (the claim, the exit code
file, the child's status) and that the actions are applied (the signal
reaches the group, one transaction records the outcome, the hook fires once).

### How an agent hears about its job

1. `pp queue add` records the calling session as the job's owner, and
   starts the queue's worker if none is running.
2. The worker starts the job when what it needs is free, in its own process
   group, inside an `sh` that holds the job's run and writes the attempt's
   exit code when the command ends.
3. That `sh` writes the exit code, so any worker can tell how it ended, even
   one started after it. Each attempt writes to a file of its own, so no
   attempt reads another's code.
4. The worker records the job as done or failed, retries it if asked, and
   runs its hook on the final outcome.
5. A `pp queue wait` or `watch` holds a lease on the job while it runs,
   prints the outcome, and stamps the job as reported to its session.
6. `pp queue wait --mine` takes the session's jobs that are live, or that
   ended with no stamp from that session.

### Choices, and why

- pp does not type into an agent's terminal to report back. A paste can
  race a person typing, or answer a prompt. The agent runs
  `pp queue wait` in the background and is told when it exits.
- A plugin is a git clone pinned to a commit, not a package from an index.
  Its code runs as you, so you read the exact commit before it runs, and
  only `pp cmd update` moves it, after showing the diff.
- Each queue has its own worker, started by the first `add` and stopped
  after a while with nothing to do. There is no daemon to install.
- A kill never takes the worker claim, even where no worker is up: it starts
  one. A second kind of holder would be a second actor that signals the
  queue's jobs, and every stop would then have to be handed between the two.
- One store lock covers every queue, not one lock per queue. Job ids are
  machine-wide, and a shared pool is counted against every queue's running
  jobs, so a launch reads other queues' records.
- `pp queue worker stop` is not a pause: the next command that needs a
  worker starts one. A durable pause would be a record the worker obeys, and
  then a stopped worker and a paused queue would be two states to tell
  apart.
- A pidfd stands for a process only while pp holds that child. A record on
  disk names a process by pid, start time and boot id instead, since a pidfd
  closes with the process that opened it and says nothing to the next pp.
- A queue starts jobs strictly in priority order unless `backfill` is on,
  so a big job is not starved by small ones.
- `wait --mine` keeps no cursor per session: the stamps on the jobs say
  what a session has heard. A session's first `--mine` lists every outcome
  it never heard.
- `wait` and `watch` exit 3 on a timeout, so a script can tell "still
  running" from "failed".
- Options between several positional targets, as in `pp kill 1 -n 2`, are
  left to argparse, which rejects them. Put options first or last.

## Running your changes

`pp promote` in your clone copies HEAD into
`~/.local/share/pp/releases/pp/STAMP/`, runs `ci/check` inside that copy,
then points `current` at it. `~/.local/bin/pp` then links to that release,
in place of the link to the clone, and `~/.local/bin/ppd` links to the
clone. So `pp` runs code that passed its tests, and `ppd` runs what you are
editing. `pp promote rollback pp` goes back to the release before.

## Releases

pp is published on PyPI as ppkit. Before 1.0, the middle number goes up
only for a change that breaks a config or a command line that worked
before. Anything else, new features too, raises the last number.

Changes collect in `CHANGELOG.md` under `## Unreleased`. A maintainer sets
the version in `pp/__init__.py` and `pyproject.toml`, renames that heading
to the tag, such as `## v0.1.1`, and pushes the tag. The release job runs
CI, builds and tries the package, publishes it to PyPI, and makes a GitHub
release with that section as its notes. `tests/test_cli.py` checks that the
two version strings agree and that `CHANGELOG.md` has a section for them.

PyPI trusts this repository's `.github/workflows/release.yml` running in
its `pypi` environment (Trusted Publishing), and no token is stored.
Renaming either needs the same change on PyPI.

The tools are pinned, and move together: uv in both workflows and in
`pyproject.toml`'s `uv_build` range, ruff in `ci/check`, and the GitHub
actions by commit. After changing a workflow, run
`go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/*.yml`.
