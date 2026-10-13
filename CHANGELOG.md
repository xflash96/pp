# Changelog

## Unreleased

`pp queue`: a job is named and selected the same way by every verb.

- A job added without `--tag` has no name. Views show its command, and a
  selection by tag never matches it.
- A `--tag` is at most 64 characters of letters, digits and `.`, `_`, `-`
  and `:`. It may not hold `..`, be `.` on its own, or read as an id or a
  range, because a tag can be part of a log file's name.
- Every verb that takes jobs takes the same five forms: an id, a range
  (`12-15`), a tag matched whole, `--prefix P` (repeatable), and `--all`
  with `-q NAME`. `--all` without `-q` is a usage error, and a tag or a
  prefix that names jobs in two queues is an error that lists the ids.
- `pp queue clean` takes a selection as well as `--older AGE`.

`pp queue`: a project says how its work is launched and stopped, so a kill
and a timeout reach work that is not in the job's process group.

- `[queue] stop` in a project's `.pp.toml`, and `--stop CMD` on `add`:
  `"group"` where the runner's work stays in the job's own process group,
  else a shell command that ends it. `pp queue add` is refused in a project
  that sets `[queue] runner` and no stop, naming both spellings.
  `--runner CMD` on `add` sets the runner for one job.
- In a project with a command stop, `pp queue kill` and a `--timeout` run
  that command in place of the SIGTERM to the job's process group. The
  grace, the SIGKILL and the sweep of the group are unchanged, so a project
  with `stop = "group"` behaves as it did. The kill line says which ran, and
  a stop command that failed is reported with its exit code.
- A job added with `--no-runner` carries `stop = "group"` whatever its
  project says.
- The sweep that runs when a job's run is over also runs the project's stop
  command, where there is one and the attempt wrote no exit code of its own,
  and it takes the job's process group with it. So nothing the runner
  started outlives the attempt that started it, and a leaked process no
  longer holds a reservation the next job is told it may use.
- A job and its runner are given `PP_QUEUE_CONTAINER`, a name no other run
  on the machine has, one per attempt, beginning `pp-<queue>-<id>-`; and
  `PP_QUEUE_ENVFILE`, the path of a file of `KEY=VALUE` lines readable by
  its owner alone. That file holds the project's `[queue] env`, the job's
  `--env`, `CUDA_VISIBLE_DEVICES` where a card was reserved, `PP_QUEUE`,
  `PP_QUEUE_JOB` and `PP_QUEUE_CONTAINER`, and nothing else of the
  machine's: no `PATH`, `VIRTUAL_ENV`, `CONDA_PREFIX`, `CONDA_DEFAULT_ENV`
  or `PP_QUEUE_RCFILE`. The record names it, and it goes with the record.
- An `--env` value, or a `[queue] env` value, holding a newline is refused,
  and so is a name holding `=` or NUL.
- An exit code an attempt wrote while a stop of it was going out is that
  stop's outcome, killed or timeout, with the code kept as `rc`.
- `pp queue kill --grace DUR` takes a duration and overrides the job's own
  grace for the jobs that kill names.

`pp queue`: a job's output goes where the project says.

- `log` in a project's `[queue]`, and `--log PATH` on `add`: a path with
  `{tag}` or `{id}` in it, relative to the project root or absolute. Any
  other placeholder, a path holding `..`, and a relative path that leaves
  the project root are refused. `{tag}` on a job with no tag is filled in
  with the job's id.
- Every attempt appends to that one file, after a `[pp queue]` line naming
  the job and its command, and several jobs may name one file. The record
  names the file; `log`, `show`, a failure's tail and `PP_QUEUE_LOG` all
  read it from there. `drop` and `clean` remove the files pp put in its own
  store, and never a log at a path you named.
- `--retry-if` matches from the byte offset where the failed attempt's own
  output begins, so it no longer matches an earlier attempt's lines or pp's
  own line.

`pp queue`: a campaign file carries one job's flags per line, and a job can
wait for the jobs it names.

- A line of a `--from` file is the rest of an `add` command line: its own
  flags, then `--`, then the command. A flag on the `add` is the default for
  every line and a line's own flag wins over it; `--env` and `--need` merge
  key by key. A line that is only a command behaves as before.
- A line is split into words the way a shell splits them, with quotes and
  backslashes honoured. A line holding `|`, `&`, `;`, `<`, `>`, `(`, `)`,
  `` ` `` or `$(` outside quotes is refused with its line number, and so is a
  line with no `--` whose first word begins with `-`. A file one line of
  which is refused queues nothing. A command that wants a shell operator
  passes it quoted: `-- sh -c 'python x.py | grep -v noise'`.
- `--from FILE` together with a command on the same command line is a usage
  error, and so is a line that names the queue, the file, `--hold` or
  `--no-start`, which are the batch's and no line's.
- `--after SELECTION` on `add`: the job waits until every job that selection
  named has ended, whatever the outcome. The value is one or more selections
  separated by commas (`41,42`, `41-66`, `s1_eval,s1_report`), read as the
  job is added and kept as ids on the record. An id no job has is a usage
  error and nothing is queued. A job that is waiting is skipped rather than
  waited for, so it never becomes the head of its queue, and a job whose
  record is gone counts as ended.
- `pp queue retry` over a range, a tag, a prefix or `--all` shows a plan and
  asks, as `kill` and `drop` do; `-n` shows the plan and `-y` does not ask.
  A job that succeeded has ended, so it suits `retry`. The new job is owned
  by the session that asked for it, and a priority is set with
  `pp queue reprio` on the job the retry queued.
- A bare `pp queue wait` waits for every job that is queued or running
  instead of returning at the first outcome. It names no held job; a
  selection that names one waits for it and says so. `--timeout` is honoured
  whatever `--poll` is, and ids together with `--idle` or `--mine` is a
  usage error.
- A worker that adopts a running job takes the offset its output begins at
  from the job's own run claim, so `--retry-if` reads that attempt's lines
  and never pp's own line.

`pp queue`: every setting has one declaration, one default and one order of
its sources.

- Each setting of a job is a key of a project's `.pp.toml [queue]` as well
  as of `[queue.NAME]`: `timeout`, `retries`, `retry_delay`, `retry_if`,
  `on_fail`, `on_done`, `prio`, `gpu_gb`, `cpu`, `mem_gb`, `need`, `env`,
  `log` and `grace`. The first source that gives a setting wins, in this
  order: `add`'s flag, the project's `[queue]`, the queue's `[queue.NAME]`,
  the built-in default. `env` and `need` merge key by key.
- `grace` is one setting of the job: `--grace DUR` on `add`, the `grace`
  key, the resolved value on the record, and 10s by default. A timeout's
  stop and a kill of that job both take it.
- Durations are read one way everywhere: `90s`, `30m`, `2h`, `1d`, a bare
  number of seconds, `.5` and `1.5h`, and `none` where a setting offers it.
  An empty value, a value that is not finite and one over 1e9 seconds are
  refused, in a config key as on a command line.
- Numbers are bounded: `retries` at most 1000, `retry_delay` at most the
  one-hour cap, a queue's `poll` longer than 0. A need that is 0, negative
  or nan is refused.
- `pp queue work` takes no `--backfill` and no `--poll`: both are keys of
  `[queue.NAME]`, which the queue's worker re-reads every loop, so they
  change under running jobs with no restart.

`pp queue`: the store and the machine surface.

- A queue's `FORMAT` is 2. A pp that reads another format refuses the queue
  where it opens it, naming the format the queue says and the one that pp
  reads, rather than reading it.
- The `--json` field `gpu`, an integer, becomes `cards`, a list of card
  indices; a job that reserved no card has `cards: []`.
- New `--json` fields: `stop_cmd`, `grace`, `after`, `container`, `envfile`,
  `log_at`, and `cards`, `container`, `envfile` and `log_at` in each
  `history` entry. `show --json` carries every promised field on a job in
  each of the five states: a field the record does not hold yet is null, or
  an empty list.
- `pp help queue` is one page: a first job, a project whose work runs in a
  container in four steps, names and selection, campaign files, the settings
  table, how a job ends, workers, and what is left to you.

`pp kill`: `--grace DUR` takes a duration in the spelling every duration pp
takes has. A bare number is still seconds.

`pp promote`, `pp cmd` and `pp http`: plans under `-n`, help by flag, and
links one target holds.

- `pp -h NAME` and `pp --help NAME` print that command's help, as
  `pp help NAME` does. `pp -h NAME VERB` prints the verb's.
- `pp promote rollback`, `pp promote link`, `pp promote init` and
  `pp cmd new` take `-n`: each one shows its plan and changes nothing.
- `pp promote` does not take over a link path another target holds: the
  refusal names that target, and `--force-links` moves the link.
- `pp promote status --json` has `failed`: the current release's
  activation failed, or nothing gated it.
- `pp promote diff --json` has `source_gone`. A target whose project
  directory is gone exits 1 there, in place of git's error.
- `pp cmd ls --json` has `aliases`, each command's short names.
- An unknown key in `[[http.roots]]` or `[[http.discover]]` is a config
  error naming the file and the key, so a misspelt `marker` stops the
  server instead of serving every folder under `under`.
- `pp help http` documents every `[http]` key with its default.

## v0.1.1

Fixes. A few command lines and config values that did harm are refused.

- `pp batch run` refuses a file-name field where a shell would read the
  name again as code: inside `$( )`, backticks, `${ }`, arithmetic, a
  `[[ ]]` test, a case pattern, a comment, `eval`, `trap`, a shell's `-c`
  script, or a template with a heredoc or a function. The message names the
  spelling that works: `sh -c 'cmd "$1"' sh {}`. Its plan lists every
  command.
- A `pp batch rename` that is interrupted leaves a journal of what landed,
  and `pp batch undo` takes that journal and says so.
- `pp promote WORD` releases nothing when the word is neither a verb nor a
  path. It used to release the project in the current directory.
- A wrong value in `[promote]` is refused, naming the file and the key.
  `name` must be one path component: `name = ".."` let `gc` remove every
  release and plugin.
- `pp promote` keeps the release it flips away from, so the one you rolled
  back to is still there after the next promote.
- A socket or fifo in a tree project no longer hangs `pp promote`, and a
  file pp cannot read is a blocker that offers `exclude`.
- A hook that leaves a process running no longer holds `pp promote`. Each
  hook's output is a file beside the run's log.
- A release that `pp promote gc` cannot remove stays whole, and gc exits 1.
- `pp queue`: a job that cannot start fails on its own and the rest start.
  A job whose wrapper is still ending reads running, not lost.
- `pp queue --json` and `pp queue show --json` give a job's variables by
  name, with each value `***`.
- An empty duration such as `--timeout ""` is a usage error, and so is an
  empty `pp kill` target, which selected every process you own.
- `pp http` never serves `.git`, serves a file whose name is not UTF-8, and
  sorts by name when `?sort=` names anything else.
- pp's messages and the `pp cmd add` and `update` screens escape the text
  they name. TAB completion runs nothing in a command's name: run
  `pp completion bash` or `zsh` again to replace a saved script.
- A command file whose name is not UTF-8 no longer breaks `pp -h`.
- pp needs Python 3.11.4 or later, which `pp promote` always needed.

## v0.1.0

The first release.

- `pp queue` runs commands later, in named queues, when the GPU memory,
  CPUs, RAM or counters they ask for are free. Jobs have timeouts, retries
  for the failures you name, and hooks. Tags and ranges select groups of
  jobs. A project's `.pp.toml` can run its jobs through a runner, such as a
  container.
- `pp queue wait`, `watch` and `wait --mine` tell a person or an agent how
  jobs ended, with the end of the log for a failure.
- `pp promote` releases a project from a git commit when its check passes
  on that copy, and rolls back.
- `pp kill`, `pp batch` and `pp http` stop processes, rename or convert
  files, and show project folders in a browser.
- `pp cmd` writes commands, and adds other people's from git, pinned to the
  commit you read.
- Completion for zsh and bash.
