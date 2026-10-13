"""Terminal helpers shared by commands: argument parsers, confirmation, tables, colour, human units.
A parser's usage error and a question are escaped here like any message, so no site that writes text to a
terminal decides it."""
from __future__ import annotations

import argparse
import codecs
import re
import json
import os
import sys
import time
import unicodedata
from contextlib import contextmanager


class _Parser(argparse.ArgumentParser):
    """argparse's parser, with the words of its usage errors escaped. A word on pp's command line is text from
    elsewhere as much as a file's name is: a glob hands pp the names of the files in a directory, and
    "unrecognized arguments" names them as they are. The help text itself is printed as it is, since a
    command's docstring is pp's own and its tables are meant to line up.

    add_subparsers passes the parser's own class on, so a verb's parser escapes the same way."""

    def error(self, message: str):
        self.print_usage(sys.stderr)
        print(f"{self.prog}: error: {printable(message)}", file=sys.stderr)
        raise SystemExit(2)


def parser(cmd: str, doc: str | None, **kw) -> argparse.ArgumentParser:
    """An ArgumentParser for `pp <cmd>`: the module docstring is the help text, kept verbatim."""
    return _Parser(prog=f"pp {cmd}", description=doc,
                   formatter_class=argparse.RawDescriptionHelpFormatter, **kw)


def parse_intermixed(p: argparse.ArgumentParser, argv: list[str]) -> argparse.Namespace:
    """p.parse_intermixed_args(argv), the same on every Python pp supports: options may sit between positionals
    (`pp batch rename ' ' _ -r .`, which plain parse_args gets wrong on 3.11 and early 3.12), and after `--` every
    argument is a positional. Those versions' intermixed parse also lost `--` and read a later '-x' as an
    option, so such arguments pass through argparse as placeholders that cannot look like options."""
    if "--" not in argv:
        return p.parse_intermixed_args(argv)
    i = argv.index("--")
    held = {f"\0{n}": a for n, a in enumerate(argv[i + 1:]) if a.startswith("-")}
    tail = [f"\0{n}" if a.startswith("-") else a for n, a in enumerate(argv[i + 1:])]
    ns = p.parse_intermixed_args(argv[:i + 1] + tail)
    for k, v in vars(ns).items():
        if isinstance(v, str):
            setattr(ns, k, held.get(v, v))
        elif isinstance(v, list):
            setattr(ns, k, [held.get(x, x) if isinstance(x, str) else x for x in v])
    return ns


def _said(msg: str | list[str]) -> str:
    """`pp: ` and the message, each line of it through printable(): a message is pp's own words with text from
    elsewhere in them (a file's name, a path, an exception's words, git's output), and the escaping is here, so
    no site that reports an error decides it and none can be missed. A piece of that text is no line of its
    own either, the way a table's cell is no row of its own; pp's own lines are the items of a list. A colour
    from c() belongs outside a message, since its bytes are escaped here like any others."""
    lines = [msg] if isinstance(msg, str) else list(msg)
    return "pp: " + "\n".join(printable(line) for line in lines)


def die(msg: str | list[str], code: int = 1):
    """Print `pp: msg` to stderr and exit with code (1 failure, 2 usage/refusal)."""
    print(_said(msg), file=sys.stderr)
    raise SystemExit(code)


def warn(msg: str | list[str]):
    print(_said(msg), file=sys.stderr)


def relayed(text: str, head: str = "") -> list[str]:
    """Another program's output as the lines die() and warn() take: its first line after head (pp's own words
    about it), and its other lines indented under them.

    Output is several lines at run time where no literal is (git says four when a clone finds no key), and a
    message of several lines passed as one string would print them with their newlines written out. Each line
    here is a line pp wrote, so the escaping still leaves nothing from elsewhere able to start a line of pp's
    own shape."""
    lines = text.strip().splitlines() or [""]
    return [f"{head}: {lines[0]}" if head else lines[0]] + ["  " + ln if ln else ln for ln in lines[1:]]


def confirm(prompt: str, yes: bool = False) -> bool:
    """True to proceed. -y always proceeds; without a terminal to ask on we refuse rather than guess."""
    sys.stdout.flush()      # the plan printed so far comes before the question or the refusal, even into a pipe
    if yes:
        return True
    prompt = printable(prompt)      # it names a project, a job or a plugin, so it is escaped like a message
    if not sys.stdin.isatty():
        print(f"{prompt} There is no terminal to ask at; pass -y to proceed.", file=sys.stderr)
        return False
    try:
        return input(f"{prompt} [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


# ---- colour ---------------------------------------------------------------------------------------

_STYLES = {"bold": "1", "dim": "2", "red": "31", "green": "32", "yellow": "33", "blue": "34", "magenta": "35", "cyan": "36"}


def color_on(stream=None) -> bool:
    stream = stream or sys.stdout
    return stream.isatty() and not os.environ.get("NO_COLOR") and os.environ.get("TERM") != "dumb"


def c(text: str, *styles: str, stream=None) -> str:
    if not styles or not color_on(stream):
        return text
    return "\033[" + ";".join(_STYLES[s] for s in styles) + "m" + text + "\033[0m"


# ---- output ---------------------------------------------------------------------------------------

# every control character but tab, the C1 range (\x80-\x9f) among them: a terminal reads \x9b as a
# control sequence introducer, the way it reads \x1b[
_CTRL = re.compile(r"[\x00-\x08\x0a-\x1f\x7f-\x9f]")
_CTRL_TAB = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_SGR = re.compile(r"\x1b\[[0-9;]*m")                  # what c() wraps a cell in: bytes, no columns


def printable(text: str, tabs: bool = True) -> str:
    """Text from elsewhere (another program's argv, a plugin's file, git's output, a requested URL) made safe
    to print: control characters shown as \\n, \\x1b and so on, so an escape sequence cannot redraw the
    terminal around what the user is reading. A tab stays a tab unless tabs is False (in a table, where it
    would break the columns). The one escaper in pp: CONTRIBUTING.md, "Adding a command".

    A file name that is not valid UTF-8 is such text too: os.listdir hands it back with a lone surrogate per
    byte it could not read, and a surrogate cannot be sent to a terminal at all (a UTF-8 locale's stdout
    raises on one). It is shown as \\udcff, so what names it prints it."""
    text = text.encode("utf-8", "backslashreplace").decode("utf-8")
    return (_CTRL if tabs else _CTRL_TAB).sub(lambda m: repr(m.group())[1:-1], text)


def _char_width(ch: str) -> int:
    """How many columns one character takes, which width() and cut() both count in, so a cut and the measure
    that decided on it can never disagree.

    A combining mark is drawn on the character before it, so it takes no column of its own: an accent in an
    NFD name (a copy from a Mac, an unzipped archive, some input methods) is such a mark, and so are the
    other zero-width characters, a joiner and a variation selector among them. A character East Asian width
    calls wide or fullwidth (a CJK name, a fullwidth form) takes two columns. Anything else takes one."""
    if unicodedata.category(ch) in ("Mn", "Me", "Cf"):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def width(text: str) -> int:
    """How many columns text takes on a terminal, which is what anything lining text up has to count.

    A colour from c() is escape bytes the terminal prints nothing for, so they count for nothing; every other
    character counts what _char_width says. Counting characters instead pads a coloured cell nine columns too
    far, a CJK name half as far as it needs, and an NFD name one column per accent too far."""
    return sum(_char_width(ch) for ch in _SGR.sub("", text))


def pad(text: str, to: int) -> str:
    """text with spaces after it up to `to` printed columns, as str.ljust does with characters."""
    return text + " " * max(to - width(text), 0)


def cut(text: str, to: int) -> str:
    """text, no wider than `to` printed columns, with an ellipsis where it was cut. Cut by columns, so a wide
    character is kept whole or left out, never split across the edge, and a combining mark stays with the
    character it is drawn on. For text without colours: a colour cut in half would run on past the cell."""
    if width(text) <= to:
        return text
    out, used = [], 0
    for ch in text:
        w = _char_width(ch)
        if used + w > to - 1:
            break
        out.append(ch)
        used += w
    return "".join(out) + "…"


def table(rows: list[list], headers: list[str] | None = None, indent: str = "") -> str:
    """Left-aligned columns; the last column is never padded. Cells are str()'d, with a newline or carriage return
    shown as \\n or \\r, so no cell starts a row of its own. Cells may carry c()'s colours; text from elsewhere
    goes through printable() first.

    Columns are counted and padded in printed columns (width()), so a coloured cell and a CJK name line up
    like any other and no call site has to know what its cells hold."""
    rows = [[("" if v is None else str(v)).replace("\n", "\\n").replace("\r", "\\r") for v in r] for r in rows]
    allr = ([headers] if headers else []) + rows
    if not allr:
        return ""
    n = max(len(r) for r in allr)
    widths = [max((width(r[i]) for r in allr if i < len(r)), default=0) for i in range(n)]
    out = []
    for k, r in enumerate(allr):
        cells = [pad(cell, widths[i]) if i < len(r) - 1 else cell for i, cell in enumerate(r)]
        line = indent + "  ".join(cells)
        out.append(c(line, "bold") if headers and k == 0 else line)
    return "\n".join(out)


def print_json(obj) -> None:
    json.dump(obj, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def human_size(n: float) -> str:
    for unit in ("B", "K", "M", "G", "T"):
        if abs(n) < 1024 or unit == "T":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}T"


def human_duration(s: float | None) -> str:
    if s is None:
        return "-"
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    if s < 86400:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    return f"{s // 86400}d{(s % 86400) // 3600:02d}h"


def human_age(ts: float | None, now: float | None = None) -> str:
    """'3m ago' style age of a unix timestamp."""
    if not ts:
        return "-"
    return human_duration((now or time.time()) - ts) + " ago"


# ---- a file something else is writing -------------------------------------------------------------

_BLOCK = 1 << 16        # how much of a followed file one read takes


@contextmanager
def follow(fd: int, *streams, at: int = 0):
    """Show a growing file's new bytes on each of streams: a hook's output file while the hook runs
    (`promote.Log.sh`). The block yields `show`.

    `show(end=None)` writes what the file holds beyond what it has shown already, and answers with how many
    bytes that was. Those reads are bounded, so what a writer dumped between two passes decides how long one
    takes and never how much memory it needs: a program that prints a 200 MB build log is read in blocks like
    any other. `end` is an offset to stop at, for a pass that must not follow a writer pp is not waiting for.

    The bytes go through one incremental UTF-8 decoder for the whole follow, so a character a read ended in
    the middle of is shown whole, and not as a replacement on each side of the split. Where a read lands is
    the writer's own flushing and nothing a reader decides, which is why joining them is the reader's job.

    `at` is where in the file the showing starts. The file's own offset is never used, so pp may be holding
    that same descriptor open for the writer, and a reader of its own needs no second open."""
    dec = codecs.getincrementaldecoder("utf-8")("replace")

    def write(text: str) -> None:
        for s in streams if text else ():       # a read that was half a character shows nothing yet
            s.write(text)
            s.flush()

    def show(end: int | None = None) -> int:
        nonlocal at
        shown = 0
        while end is None or at < end:
            raw = os.pread(fd, _BLOCK if end is None else min(_BLOCK, end - at), at)
            if not raw:
                break
            at, shown = at + len(raw), shown + len(raw)
            write(dec.decode(raw))
        return shown

    try:
        yield show
    finally:
        # the file ends in the middle of a character where its writer wrote half of one: that is shown, as
        # one replacement, since nothing is coming to finish it
        write(dec.decode(b"", True))
