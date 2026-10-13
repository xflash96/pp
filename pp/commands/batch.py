r"""Rename files in bulk or run a command per file

Everything previews first: nothing is renamed or run until you say yes (or pass -y); -n only shows the plan.
pp expands globs itself, so quote them and they behave the same in zsh, bash or a script; '**' searches
subfolders too.

RENAME is a regex FIND applied to each name, replaced by REPLACE:
    pp batch rename '\.jpeg$' '.jpg' '*.jpeg'               change an extension
    pp batch rename ' ' '_' '*'                             spaces -> underscores
    pp batch rename '^IMG_(\d+)' 'photo-\1' '*.jpg'         \1 is whatever (\d+) matched
    pp batch rename -i 'draft' 'final' --stem '*.md'        ignore case; never touch the extension
...or build the whole new name from a template (-t):
    pp batch rename -t '{parent}_{i:03}{ext}' 'photos/**/*.jpg'   trip_001.jpg, trip_002.jpg, ... in each folder
    pp batch rename -t '{stem|slug}{ext}' '*.md'                  "My Notes (v2).md" -> my-notes-v2.md
A REPLACE that starts with - goes after --, and so does everything after it; a template that starts with - is
written --template=-... or -t-...:
    pp batch rename -- '^' '-draft-' '*.md'                       a.md -> -draft-a.md
    pp batch undo                                                 put the last rename back (again: the one before)
    pp batch undo --list                                          what undo can see
    pp batch undo --partial                                       some were moved or deleted since: restore the rest

RUN is one command per file. Each field is quoted for you where it sits, so a name with spaces, quotes or
semicolons in it is one word; the rest of the command is ordinary shell (pipes and redirections work). A
field whose value is a name sits in a word of a command the template itself runs, or in quoting of your own
there ('{}' and "{}"). Any other position is refused, and the refusal names it: inside $( ), backticks,
${ }, $'...', arithmetic ($(( )), (( )), $[ ], let), a [[ ]] test, a case pattern, a comment, the target of
>& or <&, right after a \ or a $, and in eval, trap or the script of a shell's -c. The -c pp reads is the
template's own sh, bash, dash, ksh or zsh, written plain or behind env, VAR=value, nice, stdbuf, timeout,
nohup, command, exec or builtin. The word after that script is an argument of the shell again, which is
where such a field goes: sh -c 'cmd "$1"' sh {}. A template that opens a heredoc (any <<) or defines a
function (NAME ( ), or the word function where a command's words start) is refused a name anywhere in it:
put the script in a file and hand the name to it as an argument (sh script.sh {}). A shell pp cannot see
there (setsid sh -c, sudo sh -c, $SHELL -c) and a program that reads an argument of its own as code (xargs,
ssh, python -c, find -exec) read a name as code, as they would any argument. {n} {N} {i} {mtime} are digits
pp writes, so they sit anywhere. The plan lists every command it will run:
    pp batch run 'magick {} {base}.png' '*.webp'
    pp batch run 'ffmpeg -i {} {out}' '*.wav' --out 'mp3/{stem}.mp3' -j 4   skips files already converted
                                        (a command that fails has the {out} it started removed, so a rerun redoes it)
    pp batch run 'wc -l {}' 'src/**/*.py'
    pp batch run @webp '*.png'          a preset from [batch.presets] in ~/.config/pp/config.toml
    pp batch presets                    list presets
Presets:   [batch.presets]
           webp = { cmd = "cwebp -q 85 {} -o {out}", out = "{base}.webp" }
           lines = "wc -l {}"

TEMPLATES are {field}, {field:format}, {field|filter|filter}; write {{ and }} for literal braces
    fields   path name stem ext dir parent base n N i mtime    (+ {out} with --out; + {1} {2} {name} regex groups)
             for photos/trip/IMG 7.JPG:  name=IMG 7.JPG  stem=IMG 7  ext=.JPG  dir=photos/trip  parent=trip
             base=photos/trip/IMG 7   n=position in the list (1, 2, ...; --start K)   N=how many
             i=position among the items in the same folder   mtime=20261008
    formats  {n:03} -> 007    {stem:.10} -> first 10 characters
    filters  lower upper title strip slug (my-notes-v2) snake (my_notes_v2)
    {} alone means {path}.

PATHS are files, folders, or quoted globs. A folder is an item itself; -r means everything inside it instead
(hidden entries only with --hidden; .git node_modules __pycache__ .venv never). Narrow with --type f|d and
--ext jpg,png.

Rename refuses when two items would get the same name or a new name is already taken, and lists every problem
at once. Run refuses the same way when two items would write the same {out}, since the one that finishes last
would win. Swaps (a<->b) and renaming a folder together with what is inside it are handled. Every applied
rename is journaled under ~/.local/state/pp/batch/journal; that journal is what `undo` reverses. A rename a
Ctrl-C ends journals the renames that landed, so `undo` puts those back and asks about nothing else. A rename
killed outright, or ended by a reboot, says nothing about what landed: `undo` takes that journal all the same,
names every item it cannot put back, and `--partial` restores the rest.
"""
from __future__ import annotations

import glob
import json
import os
import re
import shlex
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime
from typing import NamedTuple

from pp.lib import config, lock, paths, proc, ui

PRUNE = {".git", "node_modules", "__pycache__", ".venv"}
FIELDS = {"path", "name", "stem", "ext", "dir", "parent", "base", "n", "N", "i", "mtime"}
COUNTS = {"n", "N", "i", "mtime"}             # pp's own numbers: digits pp makes, not text off the disk
PATHLIKE = {"path", "dir", "base", "out"}     # quoted into a command: one starting with "-" gets "./" in front


class TemplateError(Exception):
    pass


def _shown(text: str) -> str:
    """A name off the disk, a command built from one, or a command's own output, as batch prints it.

    Every name here comes from somewhere else, and batch prints the plan it is about to carry out and then
    what became of each item. A control character in a name would redraw the terminal the plan is read in, so
    `\\x1b[2J` in a file name clears the screen holding the plan the user is about to say yes to. ui.printable
    shows it instead. Tabs are shown too, since a tab would break the columns a preview lines up in."""
    return ui.printable(text, tabs=False)


# ---- collecting items -----------------------------------------------------------------------------

def _natkey(s: str) -> list:
    # alternating str/int, always str first, so keys compare position by position without type clashes
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]


def _pruned(path: str, arg: str) -> bool:
    """A glob match that is or sits inside node_modules & co., unless the pattern itself names that directory."""
    return any(part in PRUNE and part not in arg for part in os.path.normpath(path).split(os.sep))


def _walk(top: str, hidden: bool) -> list[str]:
    out = []
    for d, dirs, files in os.walk(top):
        dirs[:] = sorted((x for x in dirs if x not in PRUNE and (hidden or not x.startswith("."))), key=_natkey)
        out += [os.path.join(d, x) for x in dirs]
        out += [os.path.join(d, x) for x in sorted(files, key=_natkey) if hidden or not x.startswith(".")]
    return sorted(out, key=_natkey)


def collect(args: list[str], recursive=False, hidden=False, type_=None, exts=None) -> list[str]:
    """Items from file/dir/glob args, in argument order, each glob naturally sorted, duplicates dropped."""
    exts = {e.lower().lstrip(".") for e in exts.split(",") if e} if exts else None
    items, seen = [], set()
    for arg in args:
        if os.path.lexists(arg):
            matches = [arg]
        elif set("*?[") & set(arg):
            matches = [m for m in sorted(glob.glob(arg, recursive=True, include_hidden=hidden), key=_natkey)
                       if not _pruned(m, arg)]
            if not matches:
                ui.warn(f"nothing matches {_shown(arg)}")
        else:
            ui.warn(f"no such file or folder: {_shown(arg)}")
            matches = []
        for m in matches:
            for p in (_walk(m, hidden) if recursive and os.path.isdir(m) and not os.path.islink(m) else [m]):
                key = os.path.abspath(p)
                if key in seen:
                    continue
                if type_ == "f" and os.path.isdir(p) or type_ == "d" and not os.path.isdir(p):
                    continue
                if exts is not None and (os.path.isdir(p) or os.path.splitext(p)[1].lower().lstrip(".") not in exts):
                    continue
                seen.add(key)
                items.append(p)
    return items


def _fields(path: str, n: int, N: int, i: int) -> dict:
    norm = os.path.normpath(path)
    name = os.path.basename(norm)
    stem, ext = os.path.splitext(name)
    d = os.path.dirname(norm)
    try:
        mtime = time.strftime("%Y%m%d", time.localtime(os.lstat(path).st_mtime))
    except OSError:
        mtime = ""
    return {"path": path, "name": name, "stem": stem, "ext": ext, "dir": d or ".",
            "parent": os.path.basename(os.path.abspath(d or ".")), "base": os.path.join(d, stem) if d else stem,
            "n": n, "N": N, "i": i, "mtime": mtime}


def _positions(items: list[str], start: int) -> list[int]:
    """{i} for each item: its position among the items in the same folder (start, start+1, ...)."""
    seen: dict = {}
    out = []
    for p in items:
        d = os.path.dirname(os.path.abspath(os.path.normpath(p)))
        seen[d] = seen.get(d, start - 1) + 1
        out.append(seen[d])
    return out


# ---- templates ------------------------------------------------------------------------------------

def _slug(s: str, sep: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", sep, s.lower()).strip(sep)


FILTERS = {"lower": str.lower, "upper": str.upper, "title": str.title, "strip": str.strip,
           "slug": lambda s: _slug(s, "-"), "snake": lambda s: _slug(s, "_")}
_TOK = re.compile(r"\{\{|\}\}|\{([^{}]*)\}|[{}]")


def compile_template(text: str, allowed: set[str], shell: bool = False) -> list:
    """Parse once, validating every field and filter up front: [literal str | (field, [filters], fmt, quoting)].

    quoting is where each field sits, which only a shell command has: with shell, the text is read once as a
    shell reads it (_reading), and a field whose value comes off the disk is accepted in a word of a command
    the template itself runs, or in quoting of your own there, and refused anywhere else (_place). It is
    decided here, once, because the text is the same for every item."""
    toks = list(_TOK.finditer(text))
    sits = _reading(_masked(text, toks)) if shell else []
    parts, buf, pos = [], [], 0
    for m in toks:
        buf.append(text[pos:m.start()])
        pos = m.end()
        tok = m.group(0)
        if tok in ("{{", "}}"):
            buf.append(tok[0])
        elif m.group(1) is None:
            raise TemplateError(f"unmatched '{tok}' in {text!r} (write {tok * 2} for a literal brace)")
        else:
            if buf:
                parts.append("".join(buf))
                buf = []
            head, _, fmt = m.group(1).partition(":")
            field, *filters = [x.strip() for x in head.split("|")]
            field = field or "path"
            if field not in allowed:
                hint = (" (regex groups exist only in FIND REPLACE mode)" if field.isdigit()
                        else " ({out} exists only with --out)" if field == "out" else "")
                raise TemplateError(f"unknown field {{{field}}}{hint}; fields: {' '.join(sorted(allowed, key=lambda f: (f.isdigit(), f.lower(), f)))}")
            for fl in filters:
                if fl not in FILTERS:
                    raise TemplateError(f"unknown filter |{fl} in {{{m.group(1)}}}; filters: {' '.join(FILTERS)}")
            parts.append((field, filters, fmt,
                          _place(sits[m.start()], m.group(1), field in COUNTS) if shell else None))
    buf.append(text[pos:])
    if "".join(buf):
        parts.append("".join(buf))
    return parts


def _masked(text: str, toks: list) -> str:
    """text as a shell reads it: every {field} written as the one word the command is handed there, and every
    {{ or }} as the one brace it stands for, both the same length as what they replace, so every index still
    names the same place.

    A quote or a `#` inside a field's own format ({stem:'^10}) is no quoting and no comment of the command's,
    and `{{ cmd; }}` is the group a shell runs, not a brace of pp's."""
    out = list(text)
    for m in toks:
        if m.group(1) is not None:
            out[m.start():m.end()] = "x" * (m.end() - m.start())
        elif m.group(0) in ("{{", "}}"):
            out[m.start():m.end()] = m.group(0)[0] + " "
    return "".join(out)


class _No(NamedTuple):
    """A position no name field may sit in: how a refusal names it, and the text a shell closes the position
    with ("" where the command it stands in closes it, None where only the end of the template does)."""
    why: str
    close: str | None = None


# Every position the reading knows a name cannot sit in, by what opens one. A value is written into the
# command as one quoted word, and these are the positions where one word is not what arrives: a second
# reading of the text (arithmetic, a command of its own, eval, the script of a shell's -c), a word whose own
# meaning the reading cannot tell (${ }, a [[ ]] test, a case pattern, a descriptor), and a place where a
# character beside the word decides what the quoting means (a backslash, a $, a comment a newline ends).
EXPANDS = {"$((": _No("inside $(( )), where a shell reads a value as arithmetic", "))"),
           "$[": _No("inside $[ ], where a shell reads a value as arithmetic", "]"),
           "$(": _No("inside $( ), which is a command of its own", ")"),
           "${": _No("inside ${ }, where a shell reads what the braces hold", "}"),
           "$'": _No("inside $'...', where a shell reads a backslash in the value", "'"),
           "`": _No("inside backticks, which are a command of its own", "`")}
ARITH = _No("inside (( )), where a shell reads a value as arithmetic", "))")
TEST = _No("inside a [[ ]] test, where a shell reads a word as a pattern", "]]")
PATTERN = _No("in a case pattern, which a shell matches a word against")
FD = _No("in the target of a >& or a <&, which a shell reads as a descriptor", "")
SCRIPT = _No("in the script of a shell's -c, which that shell reads as a command", "")
ESCAPED = _No("right after a \\, where a shell reads the next character as itself")
DOLLAR = _No("right after a $, where a shell reads the value as an expansion of its own")
COMMENT = _No("in a comment, which ends where its line does whatever a name holds")
RUNS = {"eval": _No("inside eval, which reads its argument as a command", ""),
        "trap": _No("inside trap, which reads its argument as a command when the signal arrives", ""),
        "let": _No("inside let, which reads its argument as arithmetic", "")}
# A heredoc and a function definition are facts about the whole template, not positions in it: where either
# body starts cannot be told from the text a field sits in, and a shell of its own reads that body. So a
# template holding one takes pp's own numbers and no name.
HEREDOC = _No("in a template holding a <<, whose body a shell can read as a command")
FUNCTION = _No("in a template that defines a function, whose body has a $1 of its own")
REMEDY = ("a name sits in a word of a command this template runs, or in quoting of your own there. To let a "
          """shell read the name, hand it over as an argument: sh -c 'cmd "$1"' sh {}""")
ONE_WORD = (SCRIPT, FD, "target")             # positions one word long: the word read there ends them
SHELLS = {"sh", "bash", "dash", "ksh", "zsh"}
# words before the command they run: a prefix pp reads through to the program, whose own options it does not
PREFIX = {"env", "command", "exec", "nohup", "nice", "stdbuf", "timeout", "builtin"}
KEYWORDS = {"!", "{", "}", "if", "then", "elif", "else", "do", "while", "until", "in", "time"}
_EMPTYPAREN = re.compile(r"\(\s*\)")
_REDIR = re.compile(r"(>>|<>|>&|<&|>\||>|<)[ \t]*")
_ASSIGN = re.compile(r"[A-Za-z_][A-Za-z_0-9]*=")


def _cmd_name(words: list[str]) -> str:
    """The program a command's words name so far, without its directory, or "" while none is named: the
    first word that is neither an assignment nor a word that runs the rest, so `env A=1 /bin/sh` names sh."""
    for w in words:
        if not _ASSIGN.match(w) and os.path.basename(w) not in PREFIX:
            return os.path.basename(w)
    return ""


def _shell(words: list[str]) -> bool:
    """Whether a command's words name a shell, which reads text pp writes for it as a command of its own: the
    program itself, or any word behind a prefix, since a prefix takes words of its own that pp does not read
    (`timeout 10 sh`, `nice -n 5 sh`). So `grep sh` names grep and the -c after it is grep's own option.

    A shell these words do not name reads a value as code all the same, and the help says which those are:
    one behind a word that is no prefix of pp's (setsid, sudo), one an expansion names ($SHELL), and a
    program that reads an argument of its own as code (xargs, ssh, python -c)."""
    prefixed = False
    for w in words:
        if _ASSIGN.match(w) or os.path.basename(w) in PREFIX:
            prefixed = True             # the program comes later, and pp does not read what the prefix takes
        elif os.path.basename(w) in SHELLS:
            return True
        elif not prefixed:
            return False                # this word is the program, and it is no shell
    return False


def _word_read(words: list[str], word: str, frames: tuple) -> tuple:
    """The words of the command after one more was read, and the stack after it: a word that hands the next
    one to a second reading opens a position, and a position one word long ends with the word read in it. A
    keyword names no command, so the word after it still can."""
    top = frames[-1] if frames else None
    if top in ONE_WORD:
        return words, frames[:-1]           # a script, or a redirection's target, names no command
    if word == "esac" and "case" in frames:
        while frames[-1] != "case":
            frames = frames[:-1]
        return [], frames[:-1]
    if top == "case" and word == "in" and len(words) == 1:
        return [], frames + (PATTERN,)      # the words up to the branch's ) are a pattern
    if not word or word in KEYWORDS:
        return words, frames
    if not words and word == "case":
        return [], frames + ("case",)
    name, named = _cmd_name(words), _cmd_name(words + [word])
    if not name and named in RUNS:
        frames += (RUNS[named],)
    elif _shell(words) and word.startswith("-") and not word.startswith("--") and "c" in word:
        frames += (SCRIPT,)                 # -c, or any cluster holding one: the next word is the script
    return words + [word], frames


def _line(text: str, i: int) -> str:
    """The line of text that starts at i, without its newline."""
    end = text.find("\n", i)
    return text[i:len(text) if end < 0 else end]


def _reading(text: str) -> list[tuple]:
    """Where every character of text sits, as the stack of what the text before it left open: "'" or '"' for
    quoting of the user's own, "case" for a case waiting for its patterns, "target" for a redirection's
    target, and a _No for a position no name may sit in. Entry k is the stack before text[k], and the top of
    it is where a field written at k sits.

    One reading of the template decides where every field sits, so the position a refusal names and the
    quoting a value is written in cannot disagree. The positions a name is accepted in are a word of a
    command the template itself runs and quoting of the user's own there, which is where quoting the value
    makes it one word and nothing else; every other position the reading meets opens a _No that names it,
    and a position it does not recognise at all is no position to stand in either. A stack, and not one
    state, because a position holds positions: a bracket inside arithmetic is not the bracket that ends it,
    and quoting inside $( ) is quoting of its own. The words of the command being read are kept as well,
    since eval, trap and a shell's -c are known by the words that name them. A heredoc (any << outside
    quoting) and a function definition stand at the bottom of every stack, since each is a fact about the
    whole text and no position in it. A comment ends where its line does, and a shell reads no quoting in
    it, so an apostrophe in a comment opens nothing."""
    at: list[tuple] = []
    frames: tuple = ()
    whole: _No | None = None            # the construct the whole text holds, where it holds one
    words, word = [], ""                # the words of the command being read, and the one being read now
    i = 0
    while i < len(text):
        at += [frames] * (i + 1 - len(at))
        ch, top = text[i], frames[-1] if frames else None
        cmd = (top is None or top in ("case", "target")
               or isinstance(top, _No) and top.close in ("", None))     # where the words are read
        if not words and word == "function" and ch in " \t\n;|&()<>":
            whole = whole or FUNCTION                   # the word a definition starts with, body or no body
        if text[i:i + 2] == "<<" and top not in ("'", '"'):
            whole = whole or HEREDOC                    # the operator, wherever it stands
        if top == "'":
            if ch == "'":                               # nothing else ends a single-quoted word
                frames = frames[:-1]
        elif ch == "\\":
            at += [frames + (ESCAPED,)] * (i + 2 - len(at))     # what it spells out is a place of its own
            i += 1                                      # the next character is escaped
        elif isinstance(top, _No) and top.close and text.startswith(top.close, i):
            frames, i = frames[:-1], i + len(top.close) - 1
        elif ch == top == '"':
            frames = frames[:-1]
        elif (o := next((o for o in EXPANDS if text.startswith(o, i)), None)):
            frames, i = frames + (EXPANDS[o],), i + len(o) - 1
        elif ch == '"' or ch == "'" and top != '"':
            frames += (ch,)
        elif ch == "$":
            at += [frames + (DOLLAR,)] * (i + 2 - len(at))      # a value there is read as an expansion
        elif ch in "([{" and isinstance(top, _No) and top.close in (")", "))", "]", "}"):
            frames += (_No(top.why, {"(": ")", "[": "]", "{": "}"}[ch]),)   # a bracket of its own
        elif not cmd:
            pass                                        # inside a position, only what ends it is read
        elif ch == "#" and text[i - 1:i] in ("", " ", "\t", "\n", ";", "&", "|", "("):
            line = _line(text, i)
            at += [frames + (COMMENT,)] * (i + len(line) - len(at))
            i += len(line) - 1                          # a comment is the rest of its line, read as nothing
        elif text[i:i + 2] == "((" and not words and not word:
            frames, i = frames + (ARITH,), i + 1
        elif text[i:i + 2] == "[[" and not word:
            frames, i = frames + (TEST,), i + 1
        elif ch in "<>" and (m := _REDIR.match(text, i)):
            if not (word.isascii() and word.isdecimal()):       # else the digits are the descriptor's own
                words, frames = _word_read(words, word, frames)
            frames += (FD if m[1] in (">&", "<&") else "target",)
            word, i = "", m.end() - 1                   # the target is the next word, whatever lies between
        elif ch in ";|&\n()":
            if ch == "(" and _EMPTYPAREN.match(text, i) and (word or words):
                whole = whole or FUNCTION               # `name ( )`: a definition, whatever holds the body
            words, frames = _word_read(words, word, frames)
            words, word = [], ""                        # the next word names a command of its own
            while frames and (frames[-1] in ONE_WORD
                              or isinstance(frames[-1], _No) and frames[-1].close == ""):
                frames = frames[:-1]                    # a position the command ends ends here
            back = frames[-1] if frames else None
            if ch == ")" and back == PATTERN:
                frames = frames[:-1]                    # a branch's pattern ends at its )
            elif ch == ";" and text[i + 1:i + 2] == ";" and back == "case":
                frames, i = frames + (PATTERN,), i + 1  # and the word after ;; is a pattern again
        elif ch in " \t":
            words, frames = _word_read(words, word, frames)
            word = ""
        else:
            word += ch
        i += 1
    at += [frames] * (len(text) + 1 - len(at))
    return [(whole,) + f for f in at] if whole else at


def _place(frames: tuple, shown: str, number: bool) -> str | None:
    """Where a field sits, as _shell_quote reads it: "'" or '"' for quoting of the user's own, None for a
    word of a command. A position no name may sit in is a refusal, which names the position and the spelling
    that works.

    number says the value is one of pp's own ({n}, {N}, {i}, {mtime}): what is in it is pp's own digits and
    the template's own format, and nothing off the disk, so every position takes it as 0.1.0 took it."""
    no = None if number else next((f for f in frames if isinstance(f, _No)), None)
    if no:
        raise TemplateError(f"{{{shown}}} sits {no.why}; {REMEDY}")
    return next((f for f in reversed(frames) if f in ("'", '"')), None)


def _shell_quote(s: str, state: str | None) -> str:
    """s as one literal word where the command has it: bare, or inside the user's own '...' or "...". So
    `rm '{}'` (the find -exec habit) removes `my file`, not `my` and `file`."""
    if state == "'":
        return s.replace("'", "'\\''")
    if state == '"':
        return re.sub(r'([\\"$`])', r"\\\1", s)
    return shlex.quote(s)


def render(parts: list, fields: dict, quote: bool = False, expand=None) -> str:
    """Fill a compiled template. quote: shell-quote each substituted value, for the quoting compile_template
    read it in. expand: applied to literal parts (rename passes match.expand, so \\1 and \\g<name> work
    between the {fields})."""
    out = []
    for p in parts:
        if isinstance(p, str):
            try:
                out.append(expand(p) if expand else p)
            except (re.error, IndexError) as e:
                raise TemplateError(f"in {p!r}: {e}") from None
            continue
        field, filters, fmt, state = p
        v = fields[field]
        if filters:
            v = str(v)
            for fl in filters:
                v = FILTERS[fl](v)
        try:
            s = format(v, fmt)
        except (ValueError, TypeError) as e:
            raise TemplateError(f"{{{field}:{fmt}}}: {e}") from None
        if quote and field in PATHLIKE and s.startswith("-"):
            s = "./" + s        # else `cat {}` on a file named -n.txt is `cat -n.txt`: an option, not a file
        out.append(_shell_quote(s, state) if quote else s)
    return "".join(out)


# ---- rename: plan ---------------------------------------------------------------------------------

def _hl(a: str, b: str) -> tuple[str, str]:
    """Colour the part that differs between two strings (common prefix and suffix stay plain)."""
    i = 0
    while i < min(len(a), len(b)) and a[i] == b[i]:
        i += 1
    j = 0
    while j < min(len(a), len(b)) - i and a[-1 - j] == b[-1 - j]:
        j += 1
    mid = lambda s, col: s[:i] + (ui.c(s[i:len(s) - j], col) if s[i:len(s) - j] else "") + s[len(s) - j:]
    return mid(a, "red"), mid(b, "green")


def _print_pairs(pairs: list[tuple[str, str]]):
    # the names as they are printed, and then in printed columns, so a CJK or emoji name, which takes two
    # columns per character, lines up with an ASCII one: this is the preview the user approves before
    # anything is renamed, and `pp batch undo` previews the way back the same way
    pairs = [(_shown(a), _shown(b)) for a, b in pairs]
    w = min(max((ui.width(a) for a, _ in pairs), default=0), 60)
    for a, b in pairs:
        ha, hb = _hl(a, b)
        print(f"  {ui.pad(ha, w)}  ->  {hb}")


def prepare_rename(find: str | None, replace: str, ignore_case=False):
    """(regex or None, compiled REPLACE), checked before any path is looked at, so a typo fails first."""
    rx = None
    allowed = FIELDS
    if find is not None:
        try:
            rx = re.compile(find, re.IGNORECASE if ignore_case else 0)
        except re.error as e:
            raise TemplateError(f"bad regex {find!r}: {e}") from None
        allowed = FIELDS | {str(i) for i in range(rx.groups + 1)} | set(rx.groupindex)
    return rx, compile_template(replace, allowed)


def plan_rename(items: list[str], rx, parts: list, stem_only=False, start=1):
    """-> (changes, unchanged_count, problems). A change: {path, target, parent, old, new, depth}."""
    changes, unchanged = [], 0
    for i, (path, k) in enumerate(zip(items, _positions(items, start))):
        f = _fields(path, i + start, len(items), k)
        subject = f["stem"] if stem_only else f["name"]
        if rx is None:
            new = render(parts, f)
        else:
            def sub(m, f=f):
                groups = {str(k): m.group(k) or "" for k in range(rx.groups + 1)}
                groups.update({k: m.group(k) or "" for k in rx.groupindex})
                return render(parts, {**f, **groups}, expand=m.expand)
            new = rx.sub(sub, subject)
        if stem_only:
            new += f["ext"]
        if new == f["name"]:
            unchanged += 1
            continue
        parent = os.path.dirname(os.path.abspath(path))
        changes.append({"path": path, "target": os.path.join(os.path.dirname(os.path.normpath(path)), new),
                        "parent": parent, "old": f["name"], "new": new, "depth": parent.count(os.sep)})
    problems = []
    sources = {(c["parent"], c["old"]) for c in changes}
    by_target: dict = {}
    for c in changes:
        if not c["new"] or "/" in c["new"] or "\0" in c["new"] or c["new"] in (".", ".."):
            problems.append(f"{c['path']}: {c['new']!r} is not a valid name")
            continue
        by_target.setdefault((c["parent"], c["new"]), []).append(c)
    for (parent, new), cs in by_target.items():
        if len(cs) > 1:
            problems.append(f"{', '.join(c['path'] for c in cs)} would all become {cs[0]['target']}")
        elif os.path.lexists(os.path.join(parent, new)) and (parent, new) not in sources:
            problems.append(f"{cs[0]['target']} already exists (wanted by {cs[0]['path']})")
    return changes, unchanged, problems


# ---- rename: execution ----------------------------------------------------------------------------

def _mv(src: str, dst: str, done: list):
    # os.rename silently replaces an existing file; the plan checked, but the disk may have moved on since
    if os.path.lexists(dst):
        raise FileExistsError(f"{dst} appeared after the plan was made")
    os.rename(src, dst)
    done.append((src, dst))


def _roll_back(done: list) -> list:
    """Put every step in done back, the last one first, and take the ones that went back off the list, so what
    is left on it is what is still on disk. -> the steps that could not go back.

    The signals `lock.deferring` holds back wait for this, since a rollback stopped half way would leave a file at
    a temporary name. It is as long as what the run has already done."""
    left = []
    with lock.deferring():
        for src, dst in reversed(done):
            try:
                os.rename(dst, src)
            except OSError as e:
                left.append((src, dst, f"{dst} -> {src}: {e}"))
    done[:] = [(src, dst) for src, dst, _ in reversed(left)]
    return [why for _, _, why in left]


def _rename_group(parent: str, g: list[dict], src_key: str, dst_key: str, done: list) -> None:
    """One parent directory's renames, which land together or not at all: an error puts every step so far back
    and re-raises with `rollback_failed` on it.

    A group whose targets include its own sources (a swap, a cycle, a chain) goes through temporary names, in
    two passes, and the signals `lock.deferring` holds back wait for both of them (_roll_back waits for the
    rollback as well). So no file is left at a `.staging-` name: such a name is in no journal and in no
    listing, and the next `pp batch undo` would call the file gone. The deferral is one group long, a few
    renames. A group that renames straight through is not deferred: each of its renames is whole on its own,
    so a Ctrl-C between two of them ends the run where it stands."""
    srcs = {e[src_key] for e in g}
    through_temp = any(e[dst_key] in srcs for e in g)
    with lock.deferring() if through_temp else nullcontext():
        try:
            if through_temp:
                tmp = []
                for e in g:
                    t = paths.staging(parent, "batch")       # a name no other run of pp can be using
                    _mv(os.path.join(parent, e[src_key]), t, done)
                    tmp.append((t, e[dst_key]))
                for t, dst in tmp:
                    _mv(t, os.path.join(parent, dst), done)
            else:
                for e in g:
                    _mv(os.path.join(parent, e[src_key]), os.path.join(parent, e[dst_key]), done)
        except OSError as err:
            err.rollback_failed = _roll_back(done)
            raise


def apply_groups(entries: list[dict], src_key: str, dst_key: str, deepest_first: bool,
                 done: list | None = None) -> list:
    """Rename entries ({parent, <src_key>, <dst_key>, depth}) one group of a parent dir at a time
    (_rename_group). Deepest groups first going forward, so a folder is renamed after its contents (their
    recorded parent path is still valid); shallowest first when undoing.
    Returns the (src, dst) steps done; on error rolls them back and re-raises.

    done is a list of the caller's own to append the steps to, so a caller that is interrupted part way still
    has the ones that landed, and only those: a step that went back comes off the list again. The same list is
    the return value."""
    groups: dict = {}
    for e in entries:
        groups.setdefault(e["parent"], []).append(e)
    order = sorted(groups, key=lambda p: (-groups[p][0]["depth"] if deepest_first else groups[p][0]["depth"], p))
    done = [] if done is None else done
    for parent in order:
        _rename_group(parent, groups[parent], src_key, dst_key, done)
    return done


def _journal_dir() -> str:
    return paths.state_dir("batch", "journal")


def _journal_lock() -> str:
    """One lock for the journals: an undo takes a journal and records what it did without another undo of the
    same one in between."""
    return os.path.join(paths.state_dir("batch"), ".lock")


def _write_journal(path: str, data: dict):
    """A journal of a rename in flight, which only the run that created it writes (undo goes through
    _closing)."""
    paths.write_json(path, data, indent=1)


def _undoing():
    """The journals' lock, with signals left as they are: the renames of an undo go here, so a Ctrl-C ends them
    where it lands, between two groups (_rename_group holds it back inside one). _closing takes the same lock
    inside it, since the thread holding it may take it again."""
    return lock.locked(_journal_lock())


def _closing(path: str):
    """A journal read, changed and written back under the journals' lock. Signals arriving in this scope are
    delivered when it ends, so what the undo did and the journal saying so both land, and what the undo says
    it did is printed in the scope. The renames themselves are not in it, since Ctrl-C waits for it."""
    return paths.json_txn(path, lock=_journal_lock(), indent=1)


def _journal(path: str) -> dict:
    """One journal as it was last written; {} when there is none to read."""
    try:
        with open(path) as f:
            j = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return j if isinstance(j, dict) else {}


def _journals() -> list[tuple[str, dict]]:
    out = []
    d = _journal_dir()
    for fn in sorted(os.listdir(d), reverse=True):
        if fn.endswith(".json"):
            j = _journal(os.path.join(d, fn))
            if j:
                out.append((os.path.join(d, fn), j))
    return out


def _refuse(problems: list[str], hint: str = "") -> int:
    sys.stdout.flush()      # the plan first, then why it is refused (stderr is unbuffered, stdout may not be)
    print(ui.c(f"refusing, {len(problems)} problem(s):", "red"), file=sys.stderr)
    for p in problems:
        print(f"  {_shown(p)}", file=sys.stderr)
    if hint:
        print(hint, file=sys.stderr)
    return 2


def _proceed(a, prompt: str) -> int | None:
    """None to go ahead; else the exit code (0 for -n, 2 for a refusal)."""
    if a.dry_run:
        print(ui.c("(dry run: nothing changed)", "dim"))
        return 0
    if not ui.confirm(prompt, a.yes):
        return 2
    return None


def cmd_rename(a) -> int:
    if a.template is not None:
        find, replace, args = None, a.template, a.args
    else:
        if len(a.args) < 3:
            ui.die("rename needs FIND REPLACE PATHS... (or -t TEMPLATE PATHS...)", 2)
        find, replace, args = a.args[0], a.args[1], a.args[2:]
    try:
        rx, parts = prepare_rename(find, replace, a.ignore_case)
        items = collect(args, a.recursive, a.hidden, a.type, a.ext)
        if not items:
            ui.die("nothing to rename: no paths matched", 1)
        changes, unchanged, problems = plan_rename(items, rx, parts, a.stem, a.start)
    except TemplateError as e:
        ui.die(_shown(str(e)), 2)
    _print_pairs([(c["path"], c["target"]) for c in changes])
    print(f"{len(changes)} to rename" + (f", {unchanged} unchanged" if unchanged else ""))
    if problems:
        return _refuse(problems)
    if not changes:
        return 0
    rc = _proceed(a, f"rename {len(changes)} item(s)?")
    if rc is not None:
        return rc
    jpath = os.path.join(_journal_dir(), datetime.now().strftime("%Y%m%d-%H%M%S-%f") + ".json")
    entries = [{k: c[k] for k in ("parent", "old", "new", "depth")} for c in changes]
    journal = {"created": time.time(), "cwd": os.getcwd(), "argv": a.argv, "status": "pending", "entries": entries}
    _write_journal(jpath, journal)     # before touching anything: a crash mid-way still leaves a record
    done: list = []
    try:
        apply_groups(entries, "old", "new", deepest_first=True, done=done)
    except OSError as e:
        bad = getattr(e, "rollback_failed", [])
        journal["status"] = "failed" if bad else "rolled-back"
        journal["error"] = str(e)
        _write_journal(jpath, journal)
        print(ui.c(f"rename failed: {_shown(str(e))}", "red"), file=sys.stderr)
        if bad:
            print(f"rollback incomplete; fix by hand ({jpath}):", file=sys.stderr)
            for b in bad:
                print(f"  {_shown(b)}", file=sys.stderr)
        else:
            print("rolled back: nothing changed", file=sys.stderr)
        return 1
    except BaseException:
        # a Ctrl-C ends the renames between groups (_rename_group holds it back inside one). The journal keeps
        # the ones that landed and nothing else, so `pp batch undo` puts back exactly what is on disk and asks
        # about nothing else; a journal with nothing on it never was applied.
        landed = {d for _, d in done}
        with _closing(jpath) as j:
            j["entries"] = [e for e in entries if os.path.join(e["parent"], e["new"]) in landed]
            j["status"] = "applied" if j["entries"] else "rolled-back"
            print(f"stopped with {len(j['entries'])} of {len(entries)} item(s) renamed"
                  + ("; undo with: pp batch undo" if j["entries"] else ""), file=sys.stderr)
        raise
    journal["status"] = "applied"
    _write_journal(jpath, journal)
    print(f"renamed {len(changes)} item(s); undo with: pp batch undo")
    return 0


# ---- undo -----------------------------------------------------------------------------------------

def _now(p: str, ren: dict) -> str:
    """Where an original path lives after the journal's renames: its ancestors may have been renamed too."""
    parent, name = os.path.split(p)
    if not name:
        return p
    return os.path.join(_now(parent, ren), ren.get(p, name))


def _rel(p: str) -> str:
    r = os.path.relpath(p)
    return p if r.startswith("..") else r


def cmd_undo(a) -> int:
    js = _journals()
    if a.list:
        rows = []
        for path, j in js[:a.n]:
            e = j.get("entries", [])
            ex = f"{_shown(e[0]['old'])} -> {_shown(e[0]['new'])}" + (" ..." if len(e) > 1 else "") if e else ""
            status = j.get("status", "") + (f", {len(j['left'])} left" if j.get("left") else "")
            rows.append([os.path.basename(path)[:-5], ui.human_age(j.get("created")), status, len(e), ex])
        if a.json:
            ui.print_json([{"id": os.path.basename(p)[:-5], **j} for p, j in js[:a.n]])
        elif rows:
            print(ui.table(rows, ["journal", "when", "status", "items", "example"]))
        else:
            print("no rename journals yet")
        return 0
    target = None
    for p, j in js:
        if j.get("status") in ("applied", "pending", "failed"):
            target = (p, j)
            if j["status"] != "applied":
                # a journal that does not say which of its items landed: a rename ended by a kill, a closed
                # session or a reboot, or one whose own rollback stopped part way. It is the rename the
                # directory is half way through, so it is the one undo takes: every item is tested on disk,
                # the ones it cannot put back are listed, and --partial restores the rest. Reversing the
                # rename before it would mix a third generation of names into the directory instead. A
                # rolled-back journal is silent: nothing of it is on disk.
                why = ("ended before it said what it renamed" if j["status"] == "pending" else
                       "stopped part way through its own rollback")
                ui.warn(f"{os.path.basename(p)[:-5]} {why}, so undo takes it and tests each item on disk. "
                        f"The journal is {p}")
            break
    if not target:
        ui.die("nothing to undo (no rename journal to reverse)", 1)
    jpath, journal = target
    status = journal.get("status")       # what undo took the journal as; another undo of it writes "undone"
    entries = journal["entries"]
    ren = {os.path.join(e["parent"], e["old"]): os.path.join(e["parent"], e["new"]) for e in entries}
    ren_names = {k: os.path.basename(v) for k, v in ren.items()}
    problems, pairs, stuck = [], [], []
    news_by_parent: dict = {}
    for e in entries:
        news_by_parent.setdefault(e["parent"], set()).add(e["new"])
    for e in entries:
        here = os.path.join(_now(e["parent"], ren_names), e["new"])
        back = os.path.join(_now(e["parent"], ren_names), e["old"])
        # whether the old name is on disk holding something no entry of this journal was renamed to
        old_there = os.path.lexists(back) and e["old"] not in news_by_parent[e["parent"]]
        if not os.path.lexists(here):
            # where an item is comes from disk, not from what the journal's status would suggest: a rename
            # that ended before it said what it renamed, and one whose own rollback stopped part way, both
            # leave some items back at their old name and some at neither name
            problems.append(f"{_rel(back)} is at its old name already, so there is nothing to put back"
                            if old_there else
                            f"{_rel(here)} is gone: it was moved or deleted since, or a rename that stopped "
                            "left it at a .staging-batch- name")
        elif old_there:
            problems.append(f"{_rel(back)} exists now, so {_rel(here)} cannot go back")
        else:
            pairs.append((_rel(here), _rel(os.path.join(e["parent"], e["old"]))))
            continue
        stuck.append(os.path.join(e["parent"], e["old"]))
    if problems and a.partial:
        # what is inside a folder that stays renamed stays too: its recorded parent will not exist
        inside = lambda e: any(e["parent"] == d or e["parent"].startswith(d + os.sep) for d in stuck)
        keep = [e for e in entries if os.path.join(e["parent"], e["old"]) not in stuck and not inside(e)]
        pairs = [(_rel(os.path.join(_now(e["parent"], ren_names), e["new"])), _rel(os.path.join(e["parent"], e["old"])))
                 for e in keep]
    else:
        keep = entries
    print(f"undo {os.path.basename(jpath)[:-5]} ({ui.human_age(journal.get('created'))}, {len(entries)} item(s)):")
    _print_pairs(pairs)
    if problems and not a.partial:
        return _refuse(problems, "pp batch undo --partial restores the rest and leaves these where they are")
    if problems:
        print(f"leaving {len(entries) - len(keep)} where they are:")
        for p in problems:
            print(f"  {_shown(p)}")
    rc = _proceed(a, f"undo {len(keep)} rename(s)?" if keep else
                  "nothing in it can be restored: close this journal, so undo moves on to the one before?")
    if rc is not None:
        return rc
    with _undoing():
        if _journal(jpath).get("status") != status:
            # another `pp batch undo` took this journal while this one was asking; it has had its renames
            ui.die(f"{os.path.basename(jpath)[:-5]} was undone while this asked; `pp batch undo` again", 1)
        done: list = []
        try:
            apply_groups(keep, "new", "old", deepest_first=False, done=done)
        except OSError as e:
            bad = getattr(e, "rollback_failed", [])
            print(ui.c(f"undo failed: {_shown(str(e))}", "red") + ("" if bad else "; rolled back, nothing changed"),
                  file=sys.stderr)
            for b in bad:
                print(f"  rollback incomplete: {_shown(b)}", file=sys.stderr)
            raise SystemExit(1)     # the journal keeps what it said: nothing was restored
        except BaseException:
            # a Ctrl-C ends the renames between groups. The journal keeps the ones still in place and the
            # files that are back are off it, so the command named here restores the rest and asks about
            # nothing else; a journal with nothing left on it is undone.
            back = [e for e in keep if os.path.join(e["parent"], e["old"]) in {d for _, d in done}]
            with _closing(jpath) as journal:
                rest = [e for e in journal["entries"] if e not in back]
                journal["entries"] = rest
                journal["restored"] = journal.get("restored", []) + back
                if rest:
                    again = "`pp batch undo --partial`" if problems else "`pp batch undo`"
                    print(f"stopped with {len(back)} of {len(keep)} item(s) back; {again} restores the rest",
                          file=sys.stderr)
                else:
                    journal["status"] = "undone"
                    journal["undone"] = time.time()
                    print(f"restored {len(back)} item(s)")
            raise
        with _closing(jpath) as journal:
            journal["status"] = "undone"
            journal["undone"] = time.time()
            if len(keep) < len(entries):
                journal["left"] = [e for e in entries if e not in keep]
            print(f"restored {len(keep)} item(s)"
                  + (f"; {len(entries) - len(keep)} left as they are" if len(keep) < len(entries) else ""))
    return 0


# ---- run ------------------------------------------------------------------------------------------

def _presets() -> dict[str, dict]:
    table = config.load("batch")
    config.check_keys(table, f"{config.global_path()} [batch]", ("presets",))
    out = {}
    for name, v in (table.get("presets") or {}).items():
        if isinstance(v, str):
            out[name] = {"cmd": v}
        elif isinstance(v, dict) and isinstance(v.get("cmd"), str) and set(v) <= {"cmd", "out"}:
            out[name] = {"cmd": v["cmd"], **({"out": v["out"]} if v.get("out") else {})}
        else:
            raise config.ConfigError(f"{config.global_path()} [batch.presets] {name}: a command line, or "
                                     "{ cmd = \"...\", out = \"...\" }")
    return out


def cmd_presets(a) -> int:
    ps = _presets()
    if a.json:
        ui.print_json(ps)
    elif ps:
        print(ui.table([[_shown(f"@{n}"), _shown(p["cmd"]), _shown(p.get("out", ""))] for n, p in sorted(ps.items())],
                       ["preset", "cmd", "out"]))
    else:
        print(f"no presets. Add some to {config.global_path()}:\n\n[batch.presets]\n"
              'webp = { cmd = "cwebp -q 85 {} -o {out}", out = "{base}.webp" }\nlines = "wc -l {}"')
    return 0


def cmd_run(a) -> int:
    tmpl, out_t = a.template, a.out
    if tmpl.startswith("@"):
        ps = _presets()
        if tmpl[1:] not in ps:
            have = ', '.join('@' + _shown(n) for n in sorted(ps)) or 'none (pp batch presets)'
            ui.die(f"no preset {tmpl!r}; have: {have}", 2)
        tmpl, out_t = ps[tmpl[1:]]["cmd"], out_t or ps[tmpl[1:]].get("out")
    try:
        parts = compile_template(tmpl, FIELDS | ({"out"} if out_t else set()), shell=True)
        out_parts = compile_template(out_t, FIELDS) if out_t else None
        items = collect(a.paths, a.recursive, a.hidden, a.type, a.ext)
        if not items:
            ui.die("nothing to run on: no paths matched", 1)
        jobs, skipped = [], []
        for i, (path, k) in enumerate(zip(items, _positions(items, a.start))):
            f = _fields(path, i + a.start, len(items), k)
            out = None
            if out_parts:
                out = f["out"] = render(out_parts, f)
                if os.path.lexists(out) and not a.overwrite:
                    skipped.append(path)
                    continue
            jobs.append((path, render(parts, f, quote=True), out))
    except TemplateError as e:
        ui.die(_shown(str(e)), 2)
    if not any(isinstance(p, tuple) for p in parts) and len(jobs) > 1:
        ui.warn(f"the command uses no {{field}}: the same command would run {len(jobs)} times")
    # every command, as rename shows every pair: a plan is what the user says yes to, and a command the plan
    # leaves out is one nobody approved. A name decides how its command reads, so no ten of them stand for
    # the rest
    for _, cmd, _ in jobs:
        print(f"  $ {_shown(cmd)}")
    print(f"{len(jobs)} command(s) to run" + (f", {len(skipped)} skipped (output exists; --overwrite to redo)" if skipped else ""))
    by_out: dict = {}
    for path, _, out in jobs:
        if out:
            by_out.setdefault(out, []).append(path)
    problems = [f"{', '.join(ps)} would all write {out}" for out, ps in by_out.items() if len(ps) > 1]
    if problems:
        # with -j above 1 both jobs find the path missing and count it as theirs, so the one that finishes last
        # wins and a failing one removes what a finished one wrote. rename refuses two items over one name.
        return _refuse(problems, "give --out a template that differs per item, such as one with {stem} or {n}")
    if not jobs:
        return 0
    rc = _proceed(a, f"run {len(jobs)} command(s)" + (f", {a.jobs} at a time" if a.jobs > 1 else "") + "?")
    if rc is not None:
        return rc
    counts = {"ok": 0, "failed": 0, "not run": 0}
    stop, lock = threading.Event(), threading.Lock()

    def one(job):
        path, cmd, out = job
        if stop.is_set():
            with lock:
                counts["not run"] += 1
            return
        fresh = bool(out) and not os.path.lexists(out)
        try:
            if out and os.path.dirname(out):
                os.makedirs(os.path.dirname(out), exist_ok=True)
            # pp's own group: a Ctrl-C in this terminal reaches the command too, as it would from a shell
            p = proc.run(["/bin/sh", "-c", cmd], group="shared", stdin="none", env="inherit", stderr=proc.STDOUT)
            rc_, output = p.returncode, p.stdout
        except OSError as e:
            rc_, output = None, str(e)
        ok = rc_ == 0
        # a failed command's half-written output would pass for "already converted" next time and be skipped.
        # No two jobs render one {out} (the plan refuses that), so an output that was not there when this job
        # started is this job's own.
        gone, left = False, ""
        if not ok and fresh and os.path.isfile(out) and not os.path.islink(out):
            try:
                os.remove(out)
                gone = True
            except OSError as e:        # said with this item, so the run still reports every other one
                left = _shown(f"    its partial output {out} is still there: {e}")
        with lock:
            counts["ok" if ok else "failed"] += 1
            print(f"{ui.c('ok', 'green')}  {_shown(path)}" if ok else
                  f"{ui.c('FAILED', 'red')} rc={rc_}  {_shown(path)}\n    $ {_shown(cmd)}")
            if output.strip() and (not ok or not a.quiet):
                print("\n".join("    " + _shown(ln) for ln in output.rstrip("\n").splitlines()))
            if gone:
                print(f"    removed its partial output {_shown(out)}")
            if left:
                print(left)
            sys.stdout.flush()
            if not ok and not a.keep_going:
                stop.set()

    if a.jobs > 1:
        with ThreadPoolExecutor(a.jobs) as ex:
            list(ex.map(one, jobs))
    else:
        for job in jobs:
            one(job)
    summary = f"{counts['ok']} ok, {counts['failed']} failed"
    summary += f", {counts['not run']} not run (stopped at the first failure; --keep-going to continue)" if counts["not run"] else ""
    summary += f", {len(skipped)} skipped" if skipped else ""
    print(ui.c(summary, "red" if counts["failed"] else "green"))
    return 1 if counts["failed"] else 0


# ---- cli ------------------------------------------------------------------------------------------

VERBS = {"rename": "rename by regex or template", "run": "run a command per item",
         "undo": "reverse the last applied rename", "presets": "list run presets from config"}


def _verb_parser(verb: str):
    usage = ("%(prog)s [options] FIND REPLACE PATHS...   or   %(prog)s [options] -t TEMPLATE PATHS..."
             if verb == "rename" else None)
    p = ui.parser(f"batch {verb}", __doc__ if verb in ("rename", "run") else VERBS[verb], usage=usage)

    def items_opts():
        p.add_argument("-r", "--recursive", action="store_true", help="folders mean everything inside them")
        p.add_argument("--hidden", action="store_true", help="include dot-files when expanding globs/folders")
        p.add_argument("--type", choices=("f", "d"), help="only files (f) or only folders (d)")
        p.add_argument("--ext", help="only these extensions, e.g. jpg,png")
        p.add_argument("--start", type=int, default=1, help="first value of {n} and {i} (default 1)")
        p.add_argument("-n", "--dry-run", action="store_true", help="show the plan, change nothing")
        p.add_argument("-y", "--yes", action="store_true", help="do not ask")

    if verb == "rename":
        p.add_argument("args", nargs="+", metavar="FIND REPLACE PATHS", help="or, with -t: PATHS")
        p.add_argument("-t", "--template", help="the whole new name, e.g. '{parent}_{i:03}{ext}' "
                                                 "(one starting with -: --template=-...)")
        p.add_argument("-i", "--ignore-case", action="store_true")
        p.add_argument("--stem", action="store_true", help="work on the name without its extension; keep the extension")
        items_opts()
    elif verb == "run":
        p.add_argument("template", help="command template, or @preset")
        p.add_argument("paths", nargs="+")
        p.add_argument("-j", "--jobs", type=int, default=1, help="run N at a time")
        p.add_argument("--out", help="output path template; defines {out}; existing outputs are skipped, "
                                     "and a failed command's new output is removed")
        p.add_argument("--overwrite", action="store_true", help="run even when {out} exists")
        p.add_argument("--keep-going", action="store_true", help="do not stop at the first failure")
        p.add_argument("-q", "--quiet", action="store_true", help="hide output of commands that succeed")
        items_opts()
    elif verb == "undo":
        p.add_argument("--list", action="store_true", help="show recent rename journals")
        p.add_argument("--partial", action="store_true",
                       help="restore what still can be; leave items that are gone or blocked where they are")
        p.add_argument("--json", action="store_true")
        p.add_argument("-N", dest="n", type=int, default=10, help="how many journals --list shows")
        p.add_argument("-n", "--dry-run", action="store_true")
        p.add_argument("-y", "--yes", action="store_true")
    else:
        p.add_argument("--json", action="store_true")
    return p


def main(argv: list[str]) -> int:
    # Verbs are dispatched by hand rather than with subparsers so each can use an intermixed parse:
    # `pp batch rename ' ' _ -r .` puts an option between positionals, which plain argparse rejects.
    if not argv or argv[0] in ("-h", "--help"):
        ui.parser("batch", __doc__).print_help()
        print("\nverbs: " + "   ".join(f"{v} ({h})" for v, h in VERBS.items()))
        return 0
    verb = argv[0]
    if verb not in VERBS:
        cands = [v for v in VERBS if v.startswith(verb)]
        if len(cands) > 1:
            ui.die(f"{verb!r} is ambiguous: {', '.join(cands)}", 2)
        if not cands:
            ui.die(f"unknown verb {verb!r}; one of: {', '.join(VERBS)}", 2)
        verb = cands[0]
    a = ui.parse_intermixed(_verb_parser(verb), argv[1:])
    a.argv = argv
    return {"rename": cmd_rename, "run": cmd_run, "undo": cmd_undo, "presets": cmd_presets}[verb](a)
