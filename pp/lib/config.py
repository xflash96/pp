"""Configuration: one global TOML file plus an optional per-project `.pp.toml`.

Global   ~/.config/pp/config.toml holds a table per command:  [queue.NAME] ... [http] ... [batch.presets]
Project  .pp.toml in the project root (nearest one walking up from cwd) holds [promote], [queue], ...

Who owns which table, and what rejects an unknown key in it:

    config.toml  [resources], [queue.NAME]   queue    jobq (a queue's table and [resources])
                 [http]                      http     http's KEYS
                 [batch]                     batch    batch._presets
                 [alias]                     cli      cli.alias_table (a name pp gives a command)
    .pp.toml     name                        promote and queue (the releases dir, a job's project)
                 [promote]                   promote  promote._checked
                 [queue]                     queue    queue._prepare
                 [NAME] of your own          your command; pp ignores tables it does not own

Both are edited by hand. pp writes one line of one of them: `pp cmd alias` adds or removes a key of
config.toml's [alias], and that verb is the only writer of either file. A malformed file is a loud error naming
the file, not a silent default.
"""
from __future__ import annotations

import os
import tomllib

from pp.lib import paths, proc

PROJECT_FILE = ".pp.toml"


class ConfigError(Exception):
    pass


def empty_error(path: str) -> ConfigError:
    """The error for a file of 0 bytes, in the same words wherever a reader meets one."""
    return ConfigError(f"{path} is empty: being saved, or remove it to use the defaults")


def _read(path: str, empty_ok: bool = True) -> dict:
    """The parsed file; {} when it is absent. With empty_ok False an empty file is an error: rewritten in place
    (`cat >`, some editors), a file is empty for a moment, and a reader acting on what it says must not take that
    moment for "no settings"."""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        return {}
    if not raw and not empty_ok:
        raise empty_error(path)
    try:
        return tomllib.loads(raw.decode())
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as e:
        raise ConfigError(f"{path}: {e}") from None


def global_path() -> str:
    return os.path.join(paths.config_dir(), "config.toml")


def load(section: str | None = None, empty_ok: bool = True) -> dict:
    """The global config, or one table of it ({} when absent; a ConfigError when it is there but not a table)."""
    data = _read(global_path(), empty_ok)
    if not section:
        return data
    v = data.get(section, {})
    if not isinstance(v, dict):
        raise ConfigError(f"{global_path()}: {section} = {v!r}: [{section}] must be a table")
    return v


def check_keys(table: dict, where: str, known) -> None:
    """A key pp does not read is a typo until proven otherwise: a ConfigError naming it, never a silent default."""
    bad = sorted(set(table) - set(known))
    if bad:
        raise ConfigError(f"{where}: unknown {', '.join(bad)} (known: {', '.join(known)})")


def find_project(start: str | None = None) -> tuple[str | None, dict]:
    """(root, parsed .pp.toml) of the nearest project file walking up from start; (None, {}) when there is none."""
    d = os.path.abspath(start or os.getcwd())
    while True:
        f = os.path.join(d, PROJECT_FILE)
        if os.path.isfile(f):
            return d, _read(f)
        parent = os.path.dirname(d)
        if parent == d:
            return None, {}
        d = parent


def git_root(start: str | None = None) -> str | None:
    try:
        out = proc.git("-C", start or os.getcwd(), "rev-parse", "--show-toplevel", group="own", timeout=10)
    except OSError:
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


def project_name(root: str, data: dict | None = None) -> str:
    """A project's name: `name` at the top of its .pp.toml, else its directory name."""
    data = data if data is not None else _read(os.path.join(root, PROJECT_FILE))
    return str(data.get("name") or os.path.basename(os.path.normpath(root)))
