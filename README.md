# pp

[![ci](https://github.com/xflash96/pp/actions/workflows/ci.yml/badge.svg)](https://github.com/xflash96/pp/actions/workflows/ci.yml)
[![pypi](https://img.shields.io/pypi/v/ppkit)](https://pypi.org/project/ppkit/)
[![license](https://img.shields.io/github/license/xflash96/pp)](https://github.com/xflash96/pp/blob/main/LICENSE)

A CLI kit for user & agents.

pp gathers the shell chores that come up when you work next to coding
agents such as Claude Code on one Linux machine. Long jobs wait their turn
for the GPU. An agent finds out how its job ended without anyone watching a
terminal. The copy of a tool you use every day is one that passed its tests,
not the one being edited.

Every command shows what it will do before it does it, and works the same
when a person or an agent runs it. [Try it](#try-it) queues a first job.

## Features

- [`pp queue`](#queues) runs commands later, in named queues. A job starts
  when the GPU memory, CPUs, RAM or counters it asks for are free, and it
  can have a timeout, retries and a hook for when it ends.
- [`pp queue wait`](#working-with-agents) blocks until jobs end, then says
  how each one ended. An agent runs it in the background instead of polling.
- [`pp promote`](#releases) releases a project from a git commit, tests the
  copy, and switches to it only if the tests pass.
- [`pp kill`](#everyday-tools) stops processes by pid, port or pattern, and
  never the shell that asked.
- [`pp batch`](#everyday-tools) renames files, or runs a command on each one,
  with a preview and undo.
- [`pp http`](#everyday-tools) shows project folders in a browser, with
  markdown and code rendered.
- [`pp cmd`](#your-own-commands) writes your own commands, and adds other
  people's from git.

pp needs no packages beyond Python itself, and it completes in zsh and bash.

## How it works

Each command is one Python file. pp looks for them when it runs: its own in
`pp/commands/`, yours in `~/.config/pp/commands/`, the plugins you added
with `pp cmd add`, the folders listed in `PP_PATH`, and any program named
`pp-NAME` on your `PATH`. The first one found wins, so a plugin cannot hide
a built-in or one of yours. `pp` lists commands without loading them, so
one broken command cannot break the others.
`pp help NAME` prints a command's manual, with examples.

Everything pp keeps is in plain files:

```
~/.config/pp/config.toml       settings, a table per command
~/.config/pp/commands/         your own commands
.pp.toml                       a project's settings, at its root
~/.local/state/pp/             queues, job logs, the rename journal
~/.local/share/pp/releases/    what pp promote released
~/.local/share/pp/plugins/     plugins, one git clone each
```

A queue has a worker, a background process that `pp queue add` starts when
it is not running. It runs each job in its own process group, in the
directory the job was added from, with the `PATH` and virtualenv of the
shell that added it.

## Install

pp runs on Linux, with Python 3.11.4 or later. Install it as a tool, in an
environment of its own:

```
uv tool install ppkit        # or: pipx install ppkit
pp --version
```

For completion, add `source <(pp completion zsh)` to `~/.zshrc` after
`compinit`, or `source <(pp completion bash)` to `~/.bashrc`. To change pp
itself, run it from a clone instead ([CONTRIBUTING.md](https://github.com/xflash96/pp/blob/main/CONTRIBUTING.md)).

## Try it

```
pp queue add -- sh -c 'echo working; sleep 3; echo hello'   # job 1, in the default queue; its worker starts
pp queue                                       # job 1 running, with its last line of output: working
pp queue wait 1                                # waits, then: done 1 rc=0 3s
pp queue log 1                                 # working, then hello
pp queue add --retries 1 --retry-delay 1s -- sh -c 'echo trying; exit 3'
pp queue wait 2                                # FAILED 2 rc=3, after two tries, with the end of its log
```

`wait` exits with 0 when every job succeeded and 1 when one failed, so it
fits in a script: `pp queue wait 2 && ./next-step.sh`.

A job that needs a GPU says how much memory it takes:

```
pp queue add --gpu-gb 20 -- python train.py
```

It starts when one card has 20 GB free, counting what nvidia-smi reports
and what running jobs have reserved. The job sees only that card, through
`CUDA_VISIBLE_DEVICES`.

## Queues

Each queue has its own worker, limits and order. Job ids are unique across
queues. A job with a lower `--prio` runs first, and the default is 10. A big
job at the head of a queue is not overtaken by smaller ones.

```
pp queue add -q dl --need net=1 -- ./fetch.sh            the dl queue, taking one of a counter named net
pp queue add --timeout 2h -- python x.py                 stopped after two hours
pp queue add --retries 2 --retry-if 'Connection reset' -- ./sync.sh
pp queue add --on-fail 'notify-send "job $PP_QUEUE_JOB failed"' -- ./nightly.sh
pp queue add --tag sweep -- python x.py --lr 0.1         tagged, to act on the group later
pp queue wait sweep                                      every job tagged sweep
pp queue watch 12                                        a line for each start, retry and outcome
pp queue drop 20-29                                      a range: it shows the jobs and asks first
pp queue kill 12                                         waits while the worker stops it, then:
                                                         killed 12 (stopped on SIGTERM)
pp queue retry 11        pp queue hold 15        pp queue release 15
```

Limits go in `~/.config/pp/config.toml`. Every queue draws on the shared
GPU memory, CPUs and RAM; a queue's own pool caps what it may take of them.
A counter is a number, or a command that prints how much is free:

```toml
[resources]                  # shared by every queue
license = 4                  # --need license=1 takes one
nodes = { probe = "sinfo -h -t idle -o %D", every = "1m" }

[queue.default]
retries = 1
retry_if = "Connection reset|503"

[queue.dl.resources]         # only the dl queue has these
net = 2
```

A project's `.pp.toml` can wrap every job queued from it, for example in a
container, and add to the jobs' environment:

```toml
[queue]
runner = "docker/run.sh"         # the job runs as docker/run.sh CMD...
env = { HF_HUB_OFFLINE = "1" }
```

`pp queue add --no-runner` skips the runner for one job. The runner gets the
job's environment, including `CUDA_VISIBLE_DEVICES`, and a container runner
has to pass it in. Give a job a token or a secret with `--env` or the
project's `env`: a variable exported in your shell reaches it only through
the worker. `--json` gives a job's variables by name, with each value `***`.
`pp help queue` has the examples, and `pp help queue add` every option of
`add`.

## Working with agents

An agent that starts a long job should not sit in a loop checking on it.
With pp it runs `pp queue wait ID` in the background. Claude Code tells the
agent when a background command exits, and the output says how the job
ended, with the end of the log if it failed. `pp queue watch ID` prints a
line per event instead, for a monitor. `pp queue add` prints this hint when
an agent runs it.

pp notes which session queued each job. `pp queue wait --mine` waits for
the jobs this session queued, and also reports the ones that ended before it
started waiting, so an outcome is not missed:

```
pp queue add -- ./train.sh       # prints the job's id, and the hint to wait in the background
pp queue wait --mine             # every job of this session: how it ended, with the log's end on failure
```

Nothing is typed into the agent's terminal. pp leaves the session's identity
out of the jobs it runs, so a job does not think it is inside that session.

The commands follow the same rules, so an agent can use them without a
terminal. A command that changes or deletes things shows what it will do
and asks, unless you named each thing exactly; `-n` only shows, `-y` goes
ahead, and without a terminal it refuses instead of asking. Lists and
status reports have `--json`. The exit status is 0 when it worked, 1 when
it failed and 2 when it refused; [CONTRIBUTING.md](https://github.com/xflash96/pp/blob/main/CONTRIBUTING.md#exit-status) has
all of them.

## Releases

`pp promote` keeps the copy of a tool or service you use apart from the
tree you edit. It copies a git commit into
`~/.local/share/pp/releases/NAME/STAMP/`, runs the project's check inside
that copy, and points the `current` link at it only if the check passes.
The links you register go through `current`, so they never change.

```toml
# .pp.toml, at the project root
[promote]
check = "./test.sh"                              # must pass, or nothing changes
links = { "~/.local/bin/notes" = "bin/notes" }
after = "systemctl --user restart notes"         # makes the running service pick up the release
verify = "curl -fsS http://127.0.0.1:8100/ >/dev/null"   # if this fails, pp goes back
```

```
pp promote -n                  the plan and every problem, without changing anything
pp promote                     release HEAD
pp promote status              each project, its release, and how far its source is ahead
pp promote rollback notes      back to the release before
```

`after` runs again on a rollback, so it has to work for any release. A
database migration that cannot be undone does not belong in it. pp keeps the
last five releases, and always keeps the current one and the one before.

Each hook's output goes to a file beside the run's log, and pp shows it as it
comes. pp waits for the hook itself, not for what the hook leaves running, so
redirect the output of a process an `after` starts.

A systemd service runs the release by pointing at `current`:

```
ExecStart=%h/.local/share/pp/releases/notes/current/bin/serve --port 8100
```

## Everyday tools

```
pp kill :8080                  whatever listens on port 8080
pp kill 'vite --port'          processes whose command line matches; it shows them and asks
pp kill -n :8080               only show what it would stop
pp kill --ports                every listening port and what owns it
```

`pkill -f PATTERN` can kill the shell that runs it, because the pattern is
in that shell's command line. `pp kill` leaves out itself and the processes
that started it.

```
pp batch rename ' ' '_' '*'                                  spaces to underscores, after a preview
pp batch rename -t '{parent}_{i:03}{ext}' 'photos/**/*.jpg'   trip_001.jpg, trip_002.jpg, ... in each folder
pp batch run 'ffmpeg -i {} {out}' '*.wav' --out 'mp3/{stem}.mp3' -j 4
pp batch undo                                                put the last rename back
```

```
pp http ~/notes .              a tab for each folder, at http://127.0.0.1:8000
```

`pp http` listens on 127.0.0.1 unless you pass `--bind`, and does not serve
`.git` or files such as `.env` or private keys.

## Your own commands

```
pp cmd new hello --user -s "Say hello"     writes ~/.config/pp/commands/hello.py, a working start
```

A command is a Python file with a docstring and a `main` function. The
first line of the docstring is what `pp` lists, and the rest is its help.
Edit it into what you need. After this edit, `pp hello world` prints
`hello, world`:

```python
"""Say hello

    pp hello NAME          say hello to NAME
"""
from pp.lib import ui


def main(argv: list[str]) -> int:
    ap = ui.parser("hello", __doc__)
    ap.add_argument("name")
    a = ap.parse_args(argv)
    print(f"hello, {a.name}")
    return 0
```

Commands that someone else wrote come as a plugin: a git repository with
command files in its `commands/` folder, or at its top.

```
pp cmd add github.com/OWNER/REPO            shows the commands it adds, then asks
pp cmd add github.com/OWNER/REPO -n --show  only shows, with their code
pp cmd update                               fetches, shows the diff, then asks
pp cmd rm REPO
pp cmd ls                                   every command, and where it comes from
```

pp clones the plugin into `~/.local/share/pp/plugins/` and checks out the
commit you said yes to. Only `pp cmd update` moves it, after showing you
the diff. A plugin's code runs as you, so read it before you say yes.

A program named `pp-NAME` on your `PATH`, in any language, is a command
too. To add a command to pp itself, see [CONTRIBUTING.md](https://github.com/xflash96/pp/blob/main/CONTRIBUTING.md).

## Status

pp is new, and its version is 0.x. A command or a config key may still
change, and so may a field of `--json` output; the
[changelog](https://github.com/xflash96/pp/blob/main/CHANGELOG.md) says when one does. CI runs the tests on
Python 3.11.4 and 3.14, on Ubuntu 24.04.

## Contributing

[CONTRIBUTING.md](https://github.com/xflash96/pp/blob/main/CONTRIBUTING.md) says how to test and send a change, and
what a command needs to fit in. Bugs go to the
[issue tracker](https://github.com/xflash96/pp/issues).

Developed with agent assistance.

## License

Copyright 2026 The pp Authors. Licensed under the Apache License 2.0; see
[LICENSE](https://github.com/xflash96/pp/blob/main/LICENSE).
