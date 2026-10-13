# Changelog

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
