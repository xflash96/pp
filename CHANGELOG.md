# Changelog

## Unreleased

`pp queue`: every setting has one declaration, one default and one order of
its sources.

- Each setting of a job is a key of a project's `.pp.toml [queue]` as well
  as of `[queue.NAME]`: `timeout`, `retries`, `retry_delay`, `retry_if`,
  `on_fail`, `on_done`, `prio`, `gpu_gb`, `cpu`, `mem_gb`, `need`, `env`
  and `grace`. The first source that gives a setting wins, in this order:
  `add`'s flag, the project's `[queue]`, the queue's `[queue.NAME]`, the
  built-in default. `env` and `need` merge key by key.
- `grace` is one setting of the job: `--grace DUR` on `add`, the `grace`
  key, the resolved value on the record, and 10s by default. A timeout's
  stop and a kill of that job both take it, and `pp queue kill --grace DUR`
  overrides it for the jobs that kill names.
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
