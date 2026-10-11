"""Configuration: one global TOML file plus an optional per-project `.pp.toml`.

Global   ~/.config/pp/config.toml holds a table per command:  [queue.NAME] ... [http] ... [batch.presets]
Project  .pp.toml in the project root (nearest one walking up from cwd) holds [promote], [queue], ...

Who owns which table, and what rejects an unknown key in it:

    config.toml  [resources], [queue.NAME]   queue    jobq (a queue's table and [resources])
                 [http]                      http     http's KEYS
                 [batch]                     batch    batch._presets
    .pp.toml     name                        promote and queue (the releases dir, a job's project)
                 [promote]                   promote  promote._checked
                 [queue]                     queue    queue._prepare
                 [NAME] of your own          your command; pp ignores tables it does not own

Both are read-only to pp; edit them by hand. A malformed file is a loud error naming the file, not a silent default.
"""
from __future__ import annotations

import os
import re
import tomllib

from pp.lib import paths, proc

PROJECT_FILE = ".pp.toml"


class ConfigError(Exception):
    pass


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
        raise ConfigError(f"{path} is empty: being saved, or remove it to use the defaults")
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


def parse_duration(v, default_unit: str = "s") -> float | None:
    """90 / 90s / 30m / 2h / 1d (a bare number is default_unit) -> seconds; None for None/''; ValueError otherwise."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[default_unit]
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*", str(v))
    if not m:
        raise ValueError(f"'{v}' is not a duration like 90s, 30m, 2h, 1d")
    return float(m[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[m[2] or default_unit]


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
