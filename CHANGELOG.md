# Changelog

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
