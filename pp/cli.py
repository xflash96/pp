"""Discovery and dispatch. Knows nothing about any particular command.

A command is found in one of five places, first match wins (so a plugin cannot silently replace a built-in):

    builtin    pp/commands/<name>.py           shipped with pp
    user       ~/.config/pp/commands/<name>.py  this machine's own, never published
    plugin     ~/.local/share/pp/plugins/<plugin>/[commands/]<name>.py   added by `pp cmd add`, pinned to a commit
    path       <dir>/<name>.py for dir in $PP_PATH (os.pathsep-separated)
    external   pp-<name> executable on $PATH   any language, git-style

A Python command is a module whose docstring's first line is its summary and which defines main(argv) -> int.
Listing reads modules with `ast` and never imports them, so a command whose file is absent, unparsable or whose
imports fail cannot break `pp -h` or any other command. Publishing a subset of pp is deleting files.
"""
from __future__ import annotations

import ast
import contextlib
import difflib
import importlib
import importlib.util
import io
import os
import re
import sys
import traceback
from dataclasses import dataclass, field

# pp is 3.11.4's: tarfile's data filter, which `pp promote` unpacks a release with, arrived there. Every way of
# running pp comes through this module, bin/pp and `python -m pp` and the ppkit command alike, so the floor is
# stated here, before the first import that may need it. bin/pp states it again, since it must refuse before
# importing pp at all.
if sys.version_info < (3, 11, 4):
    print(f"pp needs Python >= 3.11.4 (tarfile's data filter); this is {sys.version.split()[0]} at "
          f"{sys.executable}", file=sys.stderr)
    raise SystemExit(2)

from pp import TAGLINE, __version__      # noqa: E402
from pp.lib import config, paths, proc, ui      # noqa: E402

BUILTIN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "commands")
ORIGINS = ("builtin", "user", "plugin", "path", "external")


@dataclass
class Command:
    name: str
    origin: str
    path: str
    summary: str = ""
    aliases: tuple = ()
    error: str | None = None
    shadows: list = field(default_factory=list)   # same-named commands from later origins, hidden by this one


def _scan(path: str) -> tuple[str, tuple, str | None]:
    """(summary, aliases, error) of a command module. The module is parsed, never imported."""
    try:
        with open(path, encoding="utf-8") as f:
            tree = ast.parse(f.read(), path)
    except (OSError, SyntaxError, ValueError) as e:
        return "", (), f"{type(e).__name__}: {e}"
    doc = (ast.get_docstring(tree) or "").strip()
    summary = doc.splitlines()[0] if doc else ""
    aliases: tuple = ()
    has_main = False
    for node in tree.body:
        names = [t.id for t in getattr(node, "targets", ()) if isinstance(t, ast.Name)]
        if isinstance(node, ast.FunctionDef) and node.name == "main" or "main" in names:
            has_main = True
        elif isinstance(node, ast.ImportFrom) and any((a.asname or a.name) == "main" for a in node.names):
            has_main = True     # `from mytool.cli import main`: a thin wrapper around a package
        elif "ALIASES" in names:
            try:
                v = ast.literal_eval(node.value)
            except (ValueError, TypeError, SyntaxError):
                continue
            if isinstance(v, str):
                v = [v]         # ALIASES = "x" means one alias, not three single letters
            if isinstance(v, (list, tuple)):
                aliases = tuple(str(a) for a in v if a)
    return summary, aliases, (None if has_main else "defines no main(argv)")


def is_command_file(fn: str) -> bool:
    """A command file's name: NAME.py, not starting with _ or ."""
    return fn.endswith(".py") and not fn.startswith(("_", "."))


def command_dir(clone: str) -> str:
    """Where a plugin keeps its command files: its commands/ folder, or its top when it has none."""
    d = os.path.join(clone, "commands")
    return d if os.path.isdir(d) else clone


def _plugin_dirs() -> list[str]:
    root = paths.data_dir("plugins", create=False)
    try:
        clones = [os.path.join(root, n) for n in sorted(os.listdir(root)) if not n.startswith(".")]
    except OSError:
        return []
    return [command_dir(c) for c in clones if os.path.isdir(c)]


def _py_dirs() -> list[tuple[str, str]]:
    dirs = [("builtin", BUILTIN_DIR), ("user", os.path.join(paths.config_dir(), "commands"))]
    dirs += [("plugin", d) for d in _plugin_dirs()]
    dirs += [("path", paths.expand(d)) for d in os.environ.get("PP_PATH", "").split(os.pathsep) if d]
    return dirs


def discover() -> dict[str, Command]:
    found: dict[str, Command] = {}

    def add(cmd: Command):
        if cmd.name in found:
            found[cmd.name].shadows.append(cmd)
        else:
            found[cmd.name] = cmd

    for origin, d in _py_dirs():
        try:
            names = sorted(os.listdir(d))
        except OSError:
            continue
        for fn in names:
            if not is_command_file(fn):
                continue
            path = os.path.join(d, fn)
            summary, aliases, err = _scan(path)
            add(Command(fn[:-3], origin, path, summary, aliases, err))
    for d in os.environ.get("PATH", "").split(os.pathsep):
        try:
            names = sorted(os.listdir(d or "."))
        except OSError:
            continue
        for fn in names:
            path = os.path.join(d, fn)
            if fn.startswith("pp-") and len(fn) > 3 and os.access(path, os.X_OK) and not os.path.isdir(path):
                add(Command(fn[3:], "external", path, "(external: " + path + ")"))
    return found


def find(name: str, cmds: dict[str, Command]) -> Command | list[str] | None:
    """Exact name, then alias, then unique prefix: the command; several prefix matches: their names; else None."""
    if name in cmds:
        return cmds[name]
    for cmd in cmds.values():
        if name in cmd.aliases:
            return cmd
    pre = sorted(n for n in cmds if name and n.startswith(name))
    if len(pre) == 1:
        return cmds[pre[0]]
    return pre or None


def resolve(name: str, cmds: dict[str, Command]) -> Command:
    """find(), or exit with suggestions."""
    got = find(name, cmds)
    if isinstance(got, Command):
        return got
    if got:
        ui.die(f"'{name}' is ambiguous: {', '.join(got)}", 2)
    close = difflib.get_close_matches(name, list(cmds) + [a for c in cmds.values() for a in c.aliases], n=3, cutoff=0.6)
    hint = f": did you mean {' or '.join(close)}?" if close else "; `pp -h` lists commands"
    ui.die(f"no command '{name}'{hint}", 2)


def _load(cmd: Command):
    if cmd.origin == "builtin":
        return importlib.import_module(f"pp.commands.{cmd.name}")
    # Compiled from the file each time, never from a .pyc: a cached one is checked by size and mtime in whole
    # seconds, so a file changed within the second it was cached, to the same size, would run its old code.
    spec = importlib.util.spec_from_file_location(f"pp_{cmd.origin}_{cmd.name}", cmd.path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    with open(cmd.path, "rb") as f:
        exec(compile(f.read(), cmd.path, "exec"), mod.__dict__)
    return mod


def run(cmd: Command, argv: list[str]) -> int:
    if cmd.origin == "external":
        try:
            proc.exec_replace(cmd.path, argv)
        except OSError as e:
            # the kernel will not run this file: no shebang, a shebang naming an interpreter that is not
            # there, a format it cannot read. Every other broken command says what is wrong, so this one does
            ui.die(f"command '{cmd.name}' ({cmd.path}) could not run: {e.strerror}")
    if cmd.error:
        ui.die(f"command '{cmd.name}' ({cmd.path}) is broken: {cmd.error}")
    try:
        mod = _load(cmd)
    except Exception as e:      # an import failure stays this command's problem
        debug = os.environ.get("PP_DEBUG")
        if debug:
            traceback.print_exc()
        hint = "" if debug else "; PP_DEBUG=1 for the traceback"
        ui.die(f"command '{cmd.name}' failed to load: {type(e).__name__}: {e}  ({cmd.path}{hint})")
    rc = mod.main(argv)
    if rc is None or isinstance(rc, int):
        return int(rc or 0)
    # like sys.exit("message"): a message is a failure, and a command's words are its own, lines and all
    ui.warn(ui.relayed(str(rc)))
    return 1


def help_text(cmds: dict[str, Command], show_all: bool = False) -> str:
    out = [f"{ui.c('pp', 'bold')} {__version__}: {TAGLINE}",
           "",
           "usage: pp <command> [args...]     pp help <command> [verb]     pp -h --all (where each command lives)",
           ""]
    heads = {"builtin": "commands:", "user": f"user commands ({os.path.join(paths.config_dir(), 'commands')}):",
             "plugin": f"plugins ({paths.data_dir('plugins', create=False)}):", "path": "PP_PATH commands:",
             "external": "external (pp-* on PATH):"}
    for origin in ORIGINS:
        group = sorted((c for c in cmds.values() if c.origin == origin and not c.error), key=lambda c: c.name)
        if not group:
            continue
        out.append(heads[origin])
        rows = []
        for c in group:
            label = c.name + (f" ({', '.join(c.aliases)})" if c.aliases else "")
            extra = f"  [{ui.printable(c.path)}]" if show_all else ""
            if show_all and c.shadows:
                extra += "  shadows: " + ", ".join(f"{s.origin}:{ui.printable(s.path)}" for s in c.shadows)
            rows.append([ui.printable(label), ui.printable(c.summary) + extra])
        out.append(ui.table(rows, indent="  "))
        out.append("")
    hidden = [s for c in cmds.values() for s in c.shadows if s.origin != "external"]
    if hidden and not show_all:      # a file someone wrote that can never run: say so where they look
        out.append(ui.c("hidden by a same-named command found first (rename to use):", "yellow"))
        out.append(ui.table([[ui.printable(h.name), f"{h.origin}  [{ui.printable(h.path)}]"]
                             for h in sorted(hidden, key=lambda h: h.name)], indent="  "))
        out.append("")
    broken = sorted((c for c in cmds.values() if c.error), key=lambda c: c.name)
    if broken:
        out.append(ui.c("broken (skipped):", "yellow"))
        out.append(ui.table([[ui.printable(c.name), f"{ui.printable(c.error)}  [{ui.printable(c.path)}]"]
                             for c in broken], indent="  "))
        out.append("")
    out.append("Unique prefixes work (`pp pro` = `pp promote`). `pp cmd new NAME --user` writes one of your own.")
    return "\n".join(out)


def _cwd_gone() -> bool:
    try:
        os.getcwd()
        return False
    except FileNotFoundError:
        return True


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    try:
        return _main(argv)
    except config.ConfigError as e:
        ui.die(f"bad config: {e}")
    except KeyboardInterrupt:
        print(file=sys.stderr)
        return 130
    except BrokenPipeError:      # `pp ... | head`
        return 0
    except FileNotFoundError:
        if _cwd_gone():
            ui.die("the current directory no longer exists; cd somewhere that does")
        raise
    finally:
        # flush here, where a closed pipe is still ours to catch: at exit it becomes "Exception ignored" + exit 120
        try:
            sys.stdout.flush()
        except BrokenPipeError:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except (OSError, ValueError):
            pass


def _main(argv: list[str]) -> int:
    cmds = discover()
    if not argv or set(argv) <= {"-h", "--help", "--all"}:
        print(help_text(cmds, show_all="--all" in argv))
        return 0
    if argv[0] == "--commands":      # for shell completion: name<TAB>summary, aliases included
        _for_a_shell([[n, c.summary] for c in sorted(cmds.values(), key=lambda c: c.name) if not c.error
                      for n in (c.name, *c.aliases)])
        return 0
    if argv[0] == "--complete":      # for shell completion: candidates for the last of the words after `pp`
        _for_a_shell([[cand] for cand in complete(argv[1:], cmds)])
        return 0
    if argv[0] in ("-V", "--version"):
        print(f"pp {__version__} ({paths.pp_root()})")
        return 0
    if argv[0] == "help":
        rest = [w for w in argv[1:] if w not in ("-h", "--help", "--all", "help")]
        if not rest:
            print(help_text(cmds, show_all="--all" in argv))
            return 0
        return run(resolve(rest[0], cmds), rest[1:] + ["--help"])     # `pp help queue add` = `pp queue add -h`
    if argv[0].startswith("-"):
        ui.die(f"unknown option {argv[0]}; `pp -h` for usage", 2)
    return run(resolve(argv[0], cmds), argv[1:])


# ---- completion -------------------------------------------------------------------------------------------
# Verbs and flags come from what a command already prints: its docstring's `pp CMD VERB ...` examples and
# argparse's option lines. No command carries completion code, and commands of every origin complete the same way. Only built-in
# commands are run (with --help, output captured); a plugin's docstring is read without running anything, since
# pressing TAB must never execute someone's half-written script.


def _for_a_shell(rows: list[list[str]]) -> None:
    """What `--commands` and `--complete` hand a completion script: one record a line, its fields separated by
    a tab, each field escaped with tabs off. So a name is one field of one record whatever it holds, and a
    name no terminal can print (a byte that is not valid UTF-8) is printed as the escape of that byte rather
    than raising where a shell reads it with its errors hidden.

    Nothing at all is printed for no records: a shell reads an empty line as one empty candidate, and
    `pp queue add -- <TAB>` would complete that instead of the file names it falls back to. The script the
    shell runs is in pp/commands/completion.py, and it reads these lines as data: it never lets the shell
    expand a candidate, since a command's name is any file name."""
    if rows:
        print("\n".join("\t".join(ui.printable(f, tabs=False) for f in r) for r in rows))


def _help_of(cmd: Command, argv: list[str]) -> str:
    if cmd.origin != "builtin":
        if argv or cmd.error or cmd.origin == "external":
            return ""
        try:
            with open(cmd.path, encoding="utf-8") as f:
                return ast.get_docstring(ast.parse(f.read())) or ""
        except (OSError, SyntaxError, ValueError):
            return ""
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            _load(cmd).main(argv + ["--help"])
    except BaseException:       # SystemExit from argparse is the normal end; anything else just means no help
        pass
    return out.getvalue()


def _verbs(name: str, text: str) -> list[str]:
    """Verbs: every `pp NAME VERB` in the examples, and argparse sub-commands (a 4-space indented word in the
    positional-arguments section)."""
    found = re.findall(rf"\bpp {re.escape(name)} ([a-z][a-z0-9-]*)\b(?![./])", text)
    if "positional arguments:" in text:
        section = text.split("positional arguments:", 1)[1].split("\noptions:", 1)[0]
        found += re.findall(r"^    ([a-z][a-z0-9-]*)(?:\s{2,}|$)", section, re.M)
    return sorted(set(found))


def _flags(text: str) -> list[str]:
    """Flags from argparse's options section: `  -p PORT, --port PORT  help` -> -p, --port."""
    flags = []
    for line in text.rsplit("\noptions:", 1)[-1].splitlines() if "\noptions:" in text else ():
        if line.startswith(" ") and line.lstrip().startswith("-"):
            flags += [part.split()[0] for part in line.strip().split("  ")[0].split(", ")]
    return sorted(set(flags))


def complete(words: list[str], cmds: dict[str, Command]) -> list[str]:
    """Candidates for words[-1] (possibly "") in `pp WORDS...`; none means: complete file names."""
    *before, cur = words or [""]
    if before[:1] == ["help"]:
        before = before[1:]
    if not before:
        names = {n for c in cmds.values() if not c.error for n in (c.name, *c.aliases)}
        if words[:1] != ["help"]:
            names.add("help")
        return sorted(n for n in names if n.startswith(cur))
    if "--" in before:          # past `--`: the rest is someone else's command line
        return []
    cmd = find(before[0], cmds)
    if not isinstance(cmd, Command):
        return []
    text = _help_of(cmd, [])
    verbs = _verbs(cmd.name, text)
    verb = next((w for w in before[1:] if w in verbs), None)
    if cur.startswith("-"):
        flags = _flags(_help_of(cmd, [verb]) if verb else text) or re.findall(r"(?<![\w-])(--?[a-zA-Z][\w-]*)", text)
        return sorted({f for f in flags if f.startswith(cur)})
    if verb is None and not any(not w.startswith("-") for w in before[1:]):
        return [v for v in verbs if v.startswith(cur)]
    return []
