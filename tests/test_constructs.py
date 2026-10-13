"""Shared state goes through a few constructs, each written once (CONTRIBUTING.md, Tests). These checks read the
source: a call that does one of these things outside its construct fails here, named by file and function, in
the run that adds it.

KNOWN is the exceptions a rule allows, each with its reason beside it. An exception that no longer does the
thing fails too, so it is taken off rather than left standing."""
import ast
import os
import re
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import unittest
import warnings
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp.lib import session  # noqa: E402


def _sources(top: str) -> list[str]:
    return sorted(os.path.relpath(os.path.join(d, f), ROOT) for d, _, fs in os.walk(os.path.join(ROOT, top))
                  for f in fs if f.endswith(".py"))


def _dotted(node) -> str:
    """`os.path.isfile` for the callee os.path.isfile(...), `p.kill` for p.kill(), `open` for open()."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _last(name: str) -> str:
    """The last name of a dotted path: `jobq` for pp.lib.jobq and for jobq itself."""
    return name.rsplit(".", 1)[-1]


def _walk(path: str):
    """(qualname, node) for every node in the file, qualname being the enclosing class and function names."""
    with open(os.path.join(ROOT, path)) as f:
        tree = ast.parse(f.read(), path)

    def visit(node, names):
        for child in ast.iter_child_nodes(node):
            inner = names + [child.name] if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                                                ast.ClassDef)) else names
            yield ".".join(inner) or "<module>", child
            yield from visit(child, inner)
    yield from visit(tree, [])


class Rule:
    """What a construct alone may do: `match(node, path, qualname)` says a node does it, `home` lists the files
    where the construct lives.

    A home is let off the rule, so it is a file that really does the thing: `test_every_home_does_the_thing`
    fails on one that does not, and the home is taken off rather than left as a licence."""

    def __init__(self, what, match, tops, home=()):
        self.what, self.match, self.tops, self.home = what, match, tops, set(home)

    def sites(self) -> dict[str, int]:
        found = {}
        for top in self.tops:
            for path in _sources(top):
                if path in self.home:
                    continue
                for qual, node in _walk(path):
                    if self.match(node, path, qual):
                        found.setdefault(f"{path}:{qual}", node.lineno)
        return found


def _call(names=(), last=(), prefixes=()):
    def match(node, path, qual):
        if not isinstance(node, ast.Call):
            return False
        name = _dotted(node.func)
        return name in names or name.rsplit(".", 1)[-1] in last or name.startswith(tuple(prefixes))
    return match


def _through(mods: dict[str, tuple[str, ...]], starts: dict[str, tuple[str, ...]] | None = None,
             vias: tuple[str, ...] = ()):
    """A matcher for a construct whose work is functions of named modules: `subprocess.Popen`, `fcntl.flock`,
    `time.sleep`, `proc.alive`. One matcher for every such rule, with the arms the job and job-process rules
    have, since every spelling counts: each one reaches the same function.

    `mods` maps a module's own name to the names in it, `starts` to name prefixes (os.exec*, os.spawn*). Every
    way to one counts: the function through its module, however that module is spelled (`os.fork`,
    `pp.lib.proc.alive`); the function imported out of it, by its name or under an alias; the module itself
    imported under another name, which hides every call through it; the name as a string in a getattr; and the
    function put in a name of its own, which is why an attribute counts whether or not it is called.

    `vias` names the construct's own modules, and reaching the primitive through one is a test's business
    alone: `mock.patch.object(lock.fcntl, "flock", fake)` and the read beside it are how lock's own test
    watches the primitive, so under tests/ they are not a way around the construct. In pp the same attribute
    takes the same lock as a call through fcntl itself, so there it counts like any other way to it."""
    starts = starts or {}

    def wanted(mod: str, name: str) -> bool:
        return name in mods.get(mod, ()) or name.startswith(starts.get(mod, ()))

    def match(node, path, qual):
        if isinstance(node, ast.Attribute):
            through = _dotted(node.value).split(".")
            if len(through) > 1 and through[-2] in vias and path.startswith("tests/"):
                return False
            return wanted(through[-1], node.attr)
        if isinstance(node, ast.Import):
            return any(a.asname and _last(a.name) in mods for a in node.names)
        if isinstance(node, ast.ImportFrom):
            mod = _last(node.module or "")
            return any(wanted(mod, a.name) or (_last(a.name) in mods and a.asname) for a in node.names)
        if isinstance(node, ast.Call) and _dotted(node.func) == "getattr" and len(node.args) == 2:
            return isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str) \
                and wanted(_last(_dotted(node.args[0])), node.args[1].value)
        return False
    return match


_ANY_START = _through({"subprocess": ("Popen", "run", "call", "check_call", "check_output"),
                       "os": ("system", "fork", "posix_spawn", "posix_spawnp")},
                      starts={"os": ("exec", "spawn")})
_LIVES_BESIDE = _through({"subprocess": ("Popen",), "os": ("fork", "posix_spawn", "posix_spawnp")})


def _spawn(node, path, qual):
    """In pp every start counts: each decides the child's group, stdin and environment. A test's subprocess.run
    is over before the test goes on, so there only a process that runs beside the test counts."""
    return (_LIVES_BESIDE if path.startswith("tests/") else _ANY_START)(node, path, qual)


_STORE = "pp/lib/jobstore.py"       # the store's own code, which is the `job` rule's home
_CORE = "pp/lib/jobcore.py"         # the worker, the claims and the processes: the core, and three rules' home
_JOB_FILES = {"_jpath", "_job_ids", "_read_job", "_write_job", "_stage_job", "_remove_job"}
_JOB_MODULES = ("jobq", "jobstore", "store")    # the store's module, however the file that reads it names it
_RECORD_FILE = {"open", "os.open", "os.replace", "os.remove", "os.rename", "os.unlink"}
_PATH_CALLS = ("os.path.join",)                     # builds a path out of its parts
_PATH_HELPERS = {"qroot", "top", "state_dir"}       # ... and so do the store's own directory helpers


def _parts(node) -> list[str] | None:
    """The string parts of an expression that builds a path (os.path.join, an f-string, a sum of strings, one
    of the store's directory helpers), or None when the expression builds no path. A path is flagged where it
    is built, so putting it in a name first is no way past a rule."""
    if isinstance(node, ast.Call):
        name = _dotted(node.func)
        if name not in _PATH_CALLS and _last(name) not in _PATH_HELPERS:
            return None
    elif not isinstance(node, (ast.JoinedStr, ast.BinOp)):
        return None
    return [n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _says_jobs(v: str) -> bool:
    """A path part that names a queue's jobs directory."""
    return v == "jobs" or v.startswith("jobs/") or v.endswith("/jobs") or "/jobs/" in v


def _job_file(node, path, qual):
    """Reading or writing a job's file: only the store's transaction does that, and find(), which reads one
    record without a lock. Everything else asks one of them.

    Every way in counts, not only the one a bypass would be written in first: the primitive called by name in
    the store, through the module (`jobstore._write_job`, `getattr(jobstore, "_write_job")`), imported out of
    it by name, the module itself under another name, which hides the calls through it, a path with "jobs" in
    it opened, replaced or removed by hand, and the jobs directory named anywhere at all, which is where such
    a path comes from."""
    if isinstance(node, ast.Name):
        return node.id in _JOB_FILES
    if isinstance(node, ast.Attribute):
        return node.attr in _JOB_FILES and _last(_dotted(node.value)) in _JOB_MODULES
    if isinstance(node, ast.Import):
        return any(a.asname and _last(a.name) in ("jobq", "jobstore") for a in node.names)
    if isinstance(node, ast.ImportFrom):
        if _last(node.module or "") not in ("lib", "jobq", "jobstore"):
            return False
        return any(a.name in _JOB_FILES or (_last(a.name) in ("jobq", "jobstore") and a.asname)
                   for a in node.names)
    if isinstance(node, ast.Call):
        name = _dotted(node.func)
        if name == "getattr" and len(node.args) == 2:
            return _last(_dotted(node.args[0])) in _JOB_MODULES and isinstance(node.args[1], ast.Constant) \
                and node.args[1].value in _JOB_FILES
        if name in _RECORD_FILE and any(isinstance(n, ast.Constant) and isinstance(n.value, str)
                                        and _says_jobs(n.value) for a in node.args for n in ast.walk(a)):
            return True
    return any(_says_jobs(v) for v in _parts(node) or [])


_CLAIM_PATHS = {"_worker_claim", "run_claim", "_probe_claim", "_watcher_claim"}
_CLAIM_FILES = (".run", ".claim", "worker.lock")    # what a claim file is called, beside a probe's own lock
_handed_to_a_claim: dict[str, set] = {}     # path -> where in it an argument of a lock.Claim call is


def _claim_args_in(nodes) -> set:
    """(line, column) of every expression a lock.Claim call among these nodes is given, by argument or by
    keyword: the one shape a claim's path may be used in."""
    got = set()
    for node in nodes:
        if isinstance(node, ast.Call) and _dotted(node.func).split(".")[-2:-1] == ["Claim"]:
            got.update((a.lineno, a.col_offset) for a in node.args)
            got.update((k.value.lineno, k.value.col_offset) for k in node.keywords)
    return got


def _claim_args(path: str) -> set:
    """Where in this file an expression is handed to a lock.Claim call."""
    if path not in _handed_to_a_claim:
        try:
            nodes = [node for _qual, node in _walk(path)]
        except OSError:
            nodes = []
        _handed_to_a_claim[path] = _claim_args_in(nodes)
    return _handed_to_a_claim[path]


def _names_a_claim(parts: list[str]) -> bool:
    """Whether these path parts name a claim's file: one run of a job, a watcher's lease, a queue's worker, or
    a probe, whose lock is the only .lock under probes/."""
    if any(v.endswith(_CLAIM_FILES) for v in parts):
        return True
    return any("probes" in v for v in parts) and any(v.endswith(".lock") for v in parts)


def _claim_file(node, path, qual):
    """A claim's own file goes to lock.Claim and nowhere else: nothing opens it, reads it or writes it on its
    own. So the record in it is the holder's own, and whether that holder is alive is the claim's answer.

    Two ways to reach one: the helper that returns its path, called anywhere but inside a lock.Claim call, and
    the name of a claim file written out by hand, which is flagged where that path is built. In pp a claim's
    path comes from one of the helpers, so a path spelled out there is a bypass; a test makes claims of its own
    under its own names, and the helper arm is what covers pp's claims in a test.

    `pp/lib/lock.py` is the rule's home, since that is where lock.Claim opens these files: `make` builds the
    name it locks before the claim's own name is visible, which ends the way a claim file's name ends."""
    if isinstance(node, ast.Call) and _dotted(node.func).split(".")[-1] in _CLAIM_PATHS:
        if qual.split(".")[-1] in _CLAIM_PATHS:     # the one that builds the path, returning it
            return False
        return (node.lineno, node.col_offset) not in _claim_args(path)
    if path.startswith("tests/") or qual.split(".")[-1] in _CLAIM_PATHS | {"_store_lock"}:
        return False
    return _names_a_claim(_parts(node) or []) and (node.lineno, node.col_offset) not in _claim_args(path)


_CLAIM_READS = ("worker",)          # the read of the worker claim's holder: the construct's own, nobody's else
_THROUGH_THE_CORE = _through({"jobq": _CLAIM_READS, "jobcore": _CLAIM_READS})


def _names_the_worker_claim(node) -> bool:
    """Whether this expression is a queue's worker claim: the helper that returns its path, or a path built by
    hand that ends in worker.lock, the way `_names_a_claim` reads one."""
    if isinstance(node, ast.Call) and _last(_dotted(node.func)) == "_worker_claim":
        return True
    return any(v.endswith("worker.lock") for v in _parts(node) or [])


def _the_worker_claim(node, path, qual):
    """A queue's worker claim: `work()` takes it, and the construct beside it in the core reads its holder.
    Nothing outside the core does either. A verb asks the core and prints from the one answer it gets, so no
    verb acts on a look it took outside the scope that acted, and only a worker ever holds the claim
    (CONTRIBUTING.md, "Who stops a job").

    The home is the core's own file, pp/lib/jobcore.py: `worker` reads the holder, `worker_now` says what
    that holder is, and `work` takes the claim.

    Every way in counts, as for a job's record: the read called by name in the core, through the module
    however that is spelled, imported out of it, under an alias, through the module renamed, and as a string
    in a getattr. A take or a read through lock.Claim counts too, where the path it is given is a worker
    claim's: `lock.Claim.holder(_worker_claim(q))` is the same look taken by hand, and a path spelled out is
    the same claim."""
    if isinstance(node, ast.Name):
        return node.id in _CLAIM_READS
    if isinstance(node, ast.Call) and _dotted(node.func).split(".")[-2:-1] == ["Claim"]:
        args = list(node.args) + [k.value for k in node.keywords]
        if any(_names_the_worker_claim(a) for a in args):
            return True
    return _THROUGH_THE_CORE(node, path, qual)


# where a job's signal and its reap live: proc defines the signalling, `pp kill` signals the processes a
# person names, and the core is the queue's worker, which is the one actor that signals that queue's jobs
_SIGNAL_FILES = ("pp/lib/proc.py", "pp/commands/kill.py", _CORE)


# the Worker's own way into a job's signal and its reap: the tick that runs a pass, the pass itself, the step
# it takes per job, the stop that asks for a signal, and the signal itself. `tick` is among them: one call to
# it signals this queue's jobs, escalates, reaps them and records how each ended, so `work` is its one home.
_WORKER_OWN = ("tick", "carry_out", "_step", "_send", "_signal_group")


def _signals_a_job(name: str) -> bool:
    """Whether this name signals a job's processes or reaps them: one of proc's signalling functions, or one
    of the Worker's own entry points into them."""
    return name.startswith("signal_") or name in _WORKER_OWN


def _job_process(node, path, qual):
    """Signalling a job's processes, or reaping them: the queue's worker does that, inside the Worker's own
    methods, and nothing else does. A verb writes a stop on the job and wakes that worker, so one actor ends
    a job and records how it ended (CONTRIBUTING.md, "Who stops a job").

    Three homes under `pp/`, and no fourth. `pp/lib/proc.py` defines the signalling. `pp/commands/kill.py`
    is `pp kill`, which signals the processes a person names. `pp/lib/jobcore.py` is the queue's worker: the
    Worker class is where a job's signal and its reap live, `carry_out` is the worker's own pass over its
    running jobs, the wake a verb sends a worker is `Worker.wake`, `worker_stop` signals the worker itself,
    and `work` is the one caller of `Worker.tick`. So a verb that would signal a job, or the worker, goes
    through the core's API instead, which is the boundary this rule holds.

    Every way in counts, as for a job's record: the function through the module, out of an import, under an
    alias, through the module renamed, and as a string in a getattr. A Worker method reached on anything but
    `self` is one of those ways, so another class's method of the same name is not."""
    if isinstance(node, ast.Name):
        return _signals_a_job(node.id)
    if isinstance(node, ast.Attribute):
        if node.attr in _WORKER_OWN and _dotted(node.value) == "self":
            return False        # a class's own method, called on itself: that class is the Worker or no Worker
        return _signals_a_job(node.attr)
    if isinstance(node, ast.Import):
        return any(a.asname and _last(a.name) == "proc" for a in node.names)
    if isinstance(node, ast.ImportFrom):
        if _last(node.module or "") not in ("lib", "proc"):
            return False
        return any(_signals_a_job(a.name) or (_last(a.name) == "proc" and a.asname) for a in node.names)
    if isinstance(node, ast.Call) and _dotted(node.func) == "getattr" and len(node.args) == 2:
        return isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str) \
            and _signals_a_job(node.args[1].value)
    return False


_NAMED = {"open", "os.open", "os.stat", "os.lstat", "os.listdir", "os.scandir", "os.path.isfile",
          "os.path.isdir", "os.path.exists", "os.path.getsize", "os.path.getmtime"}


def _by_name_in_a_request(node, path, qual):
    return path == "pp/commands/http.py" and (qual == "Handler" or qual.startswith("Handler.")) \
        and isinstance(node, ast.Call) and _dotted(node.func) in _NAMED


_CHANGES_A_MAP = ("pop", "update", "setdefault", "clear")


_MARKS = ("append", "add", "insert", "appendleft", "set")        # ways a test marks a flag of its own
_EMPTY = ("list", "set", "dict", "Event")                       # ... and ways it starts one off unset
_LEAVES = (ast.Return, ast.Raise, ast.Continue, ast.Break)      # ... and ways its body leaves the frame
_ASKS = ("is_set",)                                             # ... and ways it asks one, through a call


def _spelling(node) -> str | None:
    """How a test spells the thing a condition tests or a statement marks: `done`, `self.done`, `done[0]`.
    None for anything else, such as a call or an expression, which is no flag."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        inner = _spelling(node.value)
        return f"{inner}.{node.attr}" if inner else None
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        inner = _spelling(node.value)
        return f"{inner}[{node.slice.value!r}]" if inner else None
    return None


def _starts_empty(value) -> bool:
    """Whether an initialiser starts a flag off unset: False, None, 0, "", an empty list, tuple, set or dict,
    one holding only those, or a fresh Event. This is what tells a flag from a value a test normalises."""
    if isinstance(value, ast.Constant):
        return value.value in (False, None, 0, "")
    if isinstance(value, (ast.List, ast.Tuple, ast.Set)):
        return all(_starts_empty(v) for v in value.elts)
    if isinstance(value, ast.Dict):
        return not value.keys
    if isinstance(value, ast.Call):
        return not value.args and not value.keywords and _last(_dotted(value.func)) in _EMPTY
    return False


def _pairs(stmt) -> list:
    """(target, value) for each thing an assignment sets, taking a tuple apart: `a, b = [], x` starts a off
    empty and says nothing about b."""
    targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
    out = []
    for t in targets:
        if isinstance(t, (ast.Tuple, ast.List)) and isinstance(stmt.value, (ast.Tuple, ast.List)) \
                and len(t.elts) == len(stmt.value.elts):
            out += list(zip(t.elts, stmt.value.elts))
        else:
            out.append((t, stmt.value))
    return out


def _starts_unset(stmts: list) -> set:
    """The spellings these statements, and the blocks inside them, start off unset."""
    out = set()
    for stmt in stmts:
        if isinstance(stmt, (ast.Assign, ast.AnnAssign)) and stmt.value is not None:
            out |= {sp for t, v in _pairs(stmt) if _starts_empty(v) for sp in [_spelling(t)] if sp}
        for sub in _blocks(stmt):
            out |= _starts_unset(sub)
    return out


def _self_fields(cls) -> set:
    """The fields a class starts off unset in any of its own methods, such as a `self.done = False` in
    setUp, since the `if` that tests one is in another method."""
    out = set()
    for node in ast.walk(cls):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            out |= {sp for t, v in _pairs(node) if _starts_empty(v)
                    for sp in [_spelling(t)] if sp and sp.startswith("self.")}
    return out


def _blocks(stmt) -> list:
    """Every statement list inside this statement, except a function's or a class's own body, which gets a
    pass of its own with the flags the code around it starts."""
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return []
    out = [b for b in (getattr(stmt, name, None) for name in ("body", "orelse", "finalbody"))
           if isinstance(b, list)]
    return out + [h.body for h in getattr(stmt, "handlers", []) or []]


def _tested(test, negated: bool = False) -> tuple[set, set]:
    """(the spellings this condition tests as unset, the ones it tests as set).

    `x` and `not x` as truth values of their own, either as an operand of `and` or `or`, `len(x) == 0` and
    `x.is_set()`. A thing the condition compares with something else, or calls with arguments, is neither:
    that is a condition about a value, not about a flag."""
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        return _tested(test.operand, not negated)
    if isinstance(test, ast.BoolOp):
        unset, been = set(), set()
        for v in test.values:
            u, b = _tested(v, negated)
            unset, been = unset | u, been | b
        return unset, been
    if isinstance(test, ast.Compare):
        right = test.comparators[0]
        if len(test.ops) == 1 and isinstance(test.left, ast.Call) and _dotted(test.left.func) == "len" \
                and len(test.left.args) == 1 and isinstance(right, ast.Constant) and right.value == 0:
            sp = _spelling(test.left.args[0])
            empty = isinstance(test.ops[0], (ast.Eq, ast.LtE)) != negated
            return ({sp}, set()) if sp and empty else ((set(), {sp}) if sp else (set(), set()))
        return set(), set()
    if isinstance(test, ast.Call):
        sp = (_spelling(test.func.value) if isinstance(test.func, ast.Attribute)
              and test.func.attr in _ASKS and not test.args else None)
        if sp is None:
            return set(), set()
        return ({sp}, set()) if negated else (set(), {sp})
    sp = _spelling(test)
    if sp is None:
        return set(), set()
    return ({sp}, set()) if negated else (set(), {sp})


def _flags_of(spellings: set, flags: set) -> set:
    """Those of these spellings that are flags: the spelling itself, or an element of one (`done[0]` where
    the code starts `done` off empty)."""
    return {sp for sp in spellings if sp in flags or sp.split("[")[0] in flags}


def _marked(stmts: list, flags: set) -> bool:
    """Whether any of these statements marks one of those flags: appending or adding to it, setting an
    event, or assigning it."""
    for stmt in stmts:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr in _MARKS and _spelling(node.func.value) in flags:
                return True
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                if any(_spelling(t) in flags for t, _v in _pairs(node)):
                    return True
            if isinstance(node, ast.AugAssign) and _spelling(node.target) in flags:
                return True
    return False


def _once_flags_in(tree) -> set:
    """(line, column) of every `if` or `while` in this tree that is a flag a test tests and then marks.

    Two shapes, which are the same race written two ways: the condition tests the flag unset and the body
    marks it, or the condition tests it set and the body leaves the frame, with the mark after the `if` in
    the same block, which is the guard clause. What makes the thing tested a flag is that the code starts
    it off unset, which is what tells one from a value a test normalises or a default it fills in."""
    found: set = set()

    def scan(stmts: list, flags: set):
        flags = flags | _starts_unset(stmts)
        for i, stmt in enumerate(stmts):
            if isinstance(stmt, (ast.If, ast.While)):
                unset, been = (_flags_of(got, flags) for got in _tested(stmt.test))
                if _marked(stmt.body, unset) or (been and stmt.body
                                                 and isinstance(stmt.body[-1], _LEAVES)
                                                 and _marked(stmts[i + 1:], been)):
                    found.add((stmt.lineno, stmt.col_offset))
            for sub in _blocks(stmt):
                scan(sub, flags)
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                scan(stmt.body, flags)
            elif isinstance(stmt, ast.ClassDef):
                scan(stmt.body, flags | _self_fields(stmt))
    scan(tree.body, set())
    return found


_once_sites: dict[str, set] = {}


def _once_flags(path: str) -> set:
    """Where the once-flags in this file are. Read from the whole file, as the claim rule reads where a path
    is handed to a Claim: the shape takes in the statements after an `if` and what the code around it starts
    the flag off as, neither of which one node can say."""
    if path not in _once_sites:
        try:
            with open(os.path.join(ROOT, path)) as f:
                _once_sites[path] = _once_flags_in(ast.parse(f.read(), path))
        except OSError:
            _once_sites[path] = set()
    return _once_sites[path]


def _once_flag(node, path, qual):
    """A step of a test that must happen once, kept with a flag of the test's own: `if not done:` with
    `done.append(1)` in the body, `if done: return` with the mark after it, and every other spelling of
    that. `journey.Gate` does it, under one lock, so the check and the step cannot be split and nothing
    slips between them.

    What makes a name a flag and not a value: the code around it starts it off unset (`done = []`,
    `done = False`, `self.done = False`, `ev = threading.Event()`). So an `if` that normalises a value or
    fills in a default is none of these, and a guard clause that keeps a step to one run is."""
    return isinstance(node, (ast.If, ast.While)) and (node.lineno, node.col_offset) in _once_flags(path)


def _environ(node, path, qual):
    """Changing the environment in a test: the sandbox owns it, and gives every variable back.

    Reading it is nobody's business but the reader's, so only a change counts, and every way to the map
    counts, as every way to a job's record does: through os, through os under another name (`o.environ`),
    imported out of it by name (`environ`), or as a string in a getattr."""
    def env(n):
        if isinstance(n, ast.Attribute):
            return n.attr == "environ"
        if isinstance(n, ast.Name):
            return n.id == "environ"
        if isinstance(n, ast.Call) and _dotted(n.func) == "getattr" and len(n.args) == 2:
            return isinstance(n.args[1], ast.Constant) and n.args[1].value == "environ"
        return False
    if isinstance(node, (ast.Assign, ast.AugAssign, ast.Delete)):
        targets = node.targets if isinstance(node, (ast.Assign, ast.Delete)) else [node.target]
        return any(isinstance(t, ast.Subscript) and env(t.value) for t in targets)
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute) and node.func.attr in _CHANGES_A_MAP and env(node.func.value):
            return True
        if _dotted(node.func).endswith("patch.dict") and node.args:
            a = node.args[0]
            return env(a) or isinstance(a, ast.Constant) and a.value == "os.environ"
    return False


RULES = {
    "signal": Rule("sends a signal: proc's handle does that",
                   _call(last=("kill", "killpg", "pidfd_send_signal", "send_signal", "terminate", "raise_signal",
                               "pthread_kill")),
                   ("pp", "tests"), home=("pp/lib/proc.py",)),
    "spawn": Rule("starts a process: proc.spawn does that", _spawn, ("pp", "tests"),
                  home=("pp/lib/proc.py", "tests/journey.py")),
    "job": Rule("reads or writes a job file: the store's transaction does that", _job_file, ("pp", "tests"),
                home=(_STORE,)),
    "lock": Rule("takes a lock: pp/lib/lock.py does that",
                 _through({"fcntl": ("flock", "lockf", "fcntl")}, vias=("lock",)),
                 ("pp", "tests"), home=("pp/lib/lock.py",)),
    "alive": Rule("judges a process alive by its pid: a claim or a handle does that",
                  _through({"proc": ("alive",)}), ("pp",)),
    "job-process": Rule("signals or reaps a job's processes: the queue's worker does that",
                        _job_process, ("pp",), home=_SIGNAL_FILES),
    "claim": Rule("opens a claim's file itself: lock.Claim does that", _claim_file, ("pp", "tests"),
                  home=("pp/lib/lock.py",)),
    "worker-claim": Rule("takes a queue's worker claim or reads its holder: work() takes it, and one construct "
                         "reads it", _the_worker_claim, ("pp",), home=(_CORE,)),
    "by-name": Rule("decides on a served file by its name: act on the open file", _by_name_in_a_request, ("pp",)),
    "sleep": Rule("sleeps in a test: wait for the state instead", _through({"time": ("sleep",)}), ("tests",),
                  home=("tests/journey.py",)),
    "environ": Rule("changes os.environ in a test: the sandbox does that", _environ, ("tests",),
                    home=("tests/sandbox.py",)),
    "once": Rule("keeps a once-flag of its own: journey.Gate does that", _once_flag, ("tests",),
                 home=("tests/journey.py",)),
}

KNOWN: dict[str, set[str]] = {
    "signal": set(),
    "spawn": set(),
    "job": {
        # two files the store never writes, so no store call can make them: the name a write stopped halfway
        # leaves, and a store laid out another way, which this pp refuses
        "tests/test_queue.py:TestStore.test_a_queue_whose_store_another_pp_wrote_is_refused",
        "tests/test_queue.py:TestStore.test_an_id_and_the_record_that_holds_it_land_together",
    },
    "lock": set(),
    "alive": set(),
    "job-process": set(),
    "claim": set(),
    "worker-claim": set(),
    "by-name": set(),
    "sleep": set(),
    "environ": set(),
    "once": set(),
}


# What may not be called inside a `with txn()` scope: readers wait for that scope, so nothing that can block
# for long belongs in one (pp/lib/jobq.py, `txn`). A probe and nvidia-smi take seconds, a process start waits
# for the child, and a second store scope would wait for a lock this one holds.
_SLOW_IN_A_TXN = ("spawn", "run", "git", "git_child", "signal_and_wait", "reading", "_cards", "sleep",
                  "txn", "listing")
_OPENS_A_TXN = (_STORE, _CORE)      # the two files that open a scope of the store's transaction

# The decision about a job, and the helpers it is made of: pure, so the table that covers every combination
# of the facts needs no process, no thread and no clock (pp/lib/jobpolicy.py, `decide`).
_POLICY = "pp/lib/jobpolicy.py"
_PURE = ("decide", "merge_stop", "_ends", "_or_again", "hook_due", "matches")
_IMPURE_MODULES = ("time", "os", "proc", "lock", "paths", "signal", "sys", "subprocess", "shutil")
_IMPURE_CALLS = ("txn", "listing", "find", "write_json", "sleep", "json_txn")


def _calls_under(node):
    """(line, the dotted callee) for every call under this node."""
    return [(n.lineno, _dotted(n.func)) for n in ast.walk(node) if isinstance(n, ast.Call)]


class TestContracts(unittest.TestCase):
    """Two contracts that pp/lib/jobq.py states and that a reading alone cannot keep: what may run inside the
    store's transaction, and that the decision about a job is pure."""

    def test_nothing_slow_runs_inside_the_stores_transaction(self):
        """`txn` holds the one store lock, so every reader of every queue waits for the scope. A probe,
        nvidia-smi, a process start, a sleep or a second store scope inside one would hold them all."""
        bad = []
        for path in _OPENS_A_TXN:
            for qual, node in _walk(path):
                if not isinstance(node, ast.With):
                    continue
                if not any(isinstance(i.context_expr, ast.Call) and _last(_dotted(i.context_expr.func)) == "txn"
                           for i in node.items):
                    continue
                bad += [f"{path}:{line} ({qual}) {name}" for stmt in node.body
                        for line, name in _calls_under(stmt) if _last(name) in _SLOW_IN_A_TXN]
        self.assertEqual(bad, [], "these can block, and a reader of any queue waits for the scope")

    def test_the_decision_reaches_no_clock_store_or_process(self):
        """`decide` and `merge_stop` are pure: the record and the facts in, one action out. So a reading of
        the clock, of the store or of a process inside them, or inside the helpers they are made of, fails
        here, and the facts stay the one thing a decision is made from."""
        bad = []
        for qual, node in _walk(_POLICY):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or _last(qual) not in _PURE:
                continue
            bad += [f"{_POLICY}:{line} ({qual}) {name}" for line, name in _calls_under(node)
                    if name.split(".")[0] in _IMPURE_MODULES or _last(name) in _IMPURE_CALLS]
        self.assertEqual(bad, [], "a pure decision reads the record and the facts, and nothing else")

    def test_these_checks_see_what_they_look_for(self):
        """Each is tried on the call that breaks it, so neither can pass by matching nothing."""
        slow = ast.parse("with txn() as s:\n    proc.spawn(argv, group='own')\n    time.sleep(1)\n")
        with_node = slow.body[0]
        found = [name for stmt in with_node.body for _line, name in _calls_under(stmt)
                 if _last(name) in _SLOW_IN_A_TXN]
        self.assertEqual(found, ["proc.spawn", "time.sleep"])
        impure = ast.parse("def decide(job, facts):\n    return time.time()\n").body[0]
        self.assertEqual([name for _line, name in _calls_under(impure)
                          if name.split(".")[0] in _IMPURE_MODULES], ["time.time"])


def _source(path: str) -> ast.Module:
    with open(os.path.join(ROOT, path)) as f:
        return ast.parse(f.read(), path)


def _functions(tree) -> dict:
    """Every function of this tree by its own name, methods and nested ones included: the ones a call written
    inside it can reach."""
    return {n.name: n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _in_a_txn(tree) -> list:
    """Every statement written inside a `with txn()` scope of this tree."""
    return [stmt for node in ast.walk(tree) if isinstance(node, ast.With)
            and any(isinstance(i.context_expr, ast.Call) and _last(_dotted(i.context_expr.func)) == "txn"
                    for i in node.items)
            for stmt in node.body]


def _reached(tree, nodes: list) -> list:
    """(line, the dotted callee) for every call these nodes make, and for every call made by a function of
    this tree that they reach. A function is followed once, so two that call each other end the walk.

    A call is followed into this file where it names a function of this file: a bare name, or one on `self`.
    A call through another module is not, since a name it happens to share with one here is another
    function (`lock.Claim.forget` and the store's own `forget`)."""
    fns, out, todo, seen = _functions(tree), [], list(nodes), set()
    while todo:
        for line, name in _calls_under(todo.pop()):
            out.append((line, name))
            root = name.split(".")[0]
            fn = fns.get(_last(name)) if root in (name, "self") else None
            if fn is not None and fn.name not in seen:
                seen.add(fn.name)
                todo.append(fn)
    return out


class TestWhatAScopeAndADecisionReach(unittest.TestCase):
    """The same two contracts, read one step further in: a helper called inside the store's transaction runs
    inside it, and a helper the decision about a job is made of is part of that decision.

    `TestContracts` reads the calls written in the scope itself. These follow every function of the file that
    those calls reach, so a probe, a process start, a second scope or a reading of the clock moved into a
    helper is one of these as well."""

    def test_nothing_slow_runs_inside_the_transaction(self):
        """`txn` holds the one store lock, so every reader of every queue waits for the scope. Nothing it
        reaches may block: a probe, nvidia-smi, a process start, a sleep or a second scope."""
        bad = []
        for path in _OPENS_A_TXN:
            tree = _source(path)
            inside = _in_a_txn(tree)
            self.assertGreater(len(inside), 0, f"{path} opens no scope of the store's transaction")
            bad += [f"{path}:{line} {name}" for line, name in _reached(tree, inside)
                    if _last(name) in _SLOW_IN_A_TXN]
        self.assertEqual(bad, [], "these can block, and a reader of every queue waits for the scope")

    def test_the_decision_and_every_helper_it_reaches_are_pure(self):
        """`decide` and `merge_stop` are handed the record and the facts and give back one action. No clock,
        no store and no process is reached from inside them, in their own code or in the helpers they are
        made of, so the facts stay the one thing a decision is made from."""
        tree = _source(_POLICY)
        fns = _functions(tree)
        pure = [fns[name] for name in _PURE if name in fns]
        self.assertEqual(len(pure), len(_PURE), f"a pure function of {_PURE} is not in the file")
        bad = [f"{_POLICY}:{line} {name}" for line, name in _reached(tree, pure)
               if name.split(".")[0] in _IMPURE_MODULES or _last(name) in _IMPURE_CALLS]
        self.assertEqual(bad, [], "a pure decision reads the record and the facts, and nothing else")

    def test_these_checks_see_what_they_look_for(self):
        """Each is tried on the call that breaks it one step further in than the scope it is written in, so
        neither can pass by matching nothing."""
        slow = ast.parse("def helper(j):\n    proc.spawn(argv, group='own')\n\n"
                         "def act(jid):\n    with txn() as s:\n        helper(s.job(jid))\n")
        self.assertEqual([name for _line, name in _reached(slow, _in_a_txn(slow))
                          if _last(name) in _SLOW_IN_A_TXN], ["proc.spawn"])
        impure = ast.parse("def hook_due(job, event):\n    return stamp(job)\n\n"
                           "def stamp(job):\n    return time.time()\n")
        self.assertEqual([name for _line, name in _reached(impure, [_functions(impure)["hook_due"]])
                          if name.split(".")[0] in _IMPURE_MODULES], ["time.time"])


class TestConstructs(unittest.TestCase):
    def test_every_rule_has_a_known_list(self):
        self.assertEqual(set(KNOWN), set(RULES))

    def test_contributing_lists_every_construct_and_no_other(self):
        """CONTRIBUTING's "The constructs" table is the one public list of them, so it is this one: a rule
        added here with no row there, or a row left behind by a rule that went, fails. Its last column is the
        rule's home, so a file let off with no word in that table fails too. A rule whose homes are functions
        of one file has no file of its own let off, and names those functions in the construct column."""
        with open(os.path.join(ROOT, "CONTRIBUTING.md")) as f:
            rows = re.findall(r"^\| `([a-z][a-z-]*)` \|(.*)\|(.*)\|$", f.read(), re.M)
        self.assertEqual(sorted(k for k, _construct, _homes in rows), sorted(RULES))
        for key, _construct, homes in rows:
            with self.subTest(key):
                said = set(re.findall(r"`([^`]+\.py)`", homes))
                if RULES[key].home:
                    self.assertEqual(said, RULES[key].home)

    def test_nothing_new_happens_outside_a_construct(self):
        for key, rule in RULES.items():
            with self.subTest(key):
                found = rule.sites()
                new = [f"{site.rsplit(':', 1)[0]}:{line} ({site.rsplit(':', 1)[1]}) {rule.what}"
                       for site, line in sorted(found.items()) if site not in KNOWN[key]]
                self.assertEqual(new, [])

    def test_every_exemption_still_does_the_thing(self):
        for key, rule in RULES.items():
            with self.subTest(key):
                gone = sorted(KNOWN[key] - set(rule.sites()))
                self.assertEqual(gone, [], f"these no longer {rule.what.split(':')[0]}: take them off KNOWN")

    def test_the_rules_see_what_they_look_for(self):
        """A rule that matched nothing would pass forever: each is tried on a line that breaks it."""
        lines = {"signal": ("os.kill(1, 15)", "os.killpg(g, 15)", "signal.raise_signal(signal.SIGTERM)",
                            "signal.pthread_kill(t, 15)"),
                 "spawn": ("subprocess.Popen(['true'])",),
                 "job": ("jobstore._write_job(j)", "d = os.path.join(st, 'queue', 'default', 'jobs')",
                         "d = qroot(q, 'jobs')", "d = paths.state_dir('queue', q, 'jobs')",
                         "d = f'{st}/queue/default/jobs'", "d = st + '/queue/default/jobs'"),
                 "lock": ("fcntl.flock(fd, 2)", "fcntl.lockf(fd, 2)", "fcntl.fcntl(fd, 37, b)"),
                 "alive": ("proc.alive(1)",),
                 "sleep": ("time.sleep(1)", "time.sleep(0.05 * n)"),
                 "job-process": ("Worker(q).carry_out()", "w._signal_group(j, 15)", "proc.signal_group(h, 15)",
                                 "proc.signal_pid(h, 15)", "proc.signal_and_wait([p], 15)"),
                 "environ": ("os.environ['X'] = '1'", "del os.environ['X']", "os.environ.pop('X', None)",
                             "os.environ.update(env)", "os.environ.setdefault('X', '1')",
                             "mock.patch.dict(os.environ, env)", "mock.patch.dict('os.environ', env)"),
                 "worker-claim": ("jobcore.worker(q)", "lock.Claim.holder(_worker_claim(q))",
                                  "lock.Claim.take(os.path.join(qroot(q), 'worker.lock'), rec)"),
                 "claim": ("open(_worker_claim(q))", "paths.write_json(run_claim(q, jid), rec)",
                           "p = os.path.join(qroot(q), 'worker.lock')",
                           "os.remove(os.path.join(top('watchers'), f'{pid}.claim'))",
                           "p = os.path.join(qroot(q, 'jobs'), f'{jid}.run')",
                           "p = os.path.join(top('probes'), '_shared.nodes.lock')"),
                 # every spelling of a flag a test tests and then marks, with the line that starts it off
                 # unset, which is what makes it one
                 "once": ("done = []\nif not done:\n    done.append(1)\n    go()",
                          "done = []\nif not done and ready():\n    done.append(1)",
                          "done = []\nif ready() and not done:\n    done.add(1)",
                          "done = False\nif not done:\n    done = True",
                          "n = 0\nif not n:\n    n += 1",
                          "done = []\nif done:\n    return\ndone.append(1)",
                          "done = [False]\nif not done[0]:\n    done[0] = True",
                          "ev = threading.Event()\nif not ev.is_set():\n    ev.set()",
                          "done = []\nif len(done) == 0:\n    done.append(1)",
                          "done = []\nwhile not done:\n    done.append(1)",
                          "def test_x(self):\n    done = []\n\n    def step():\n        if not done:\n"
                          "            done.append(1)",
                          "class T:\n    def setUp(self):\n        self.done = False\n"
                          "    def test_x(self):\n        if not self.done:\n            self.done = True")}
        where = {"sleep": "tests/x.py", "environ": "tests/x.py", "job-process": "pp/lib/jobq.py",
                 "once": "tests/x.py"}
        for key, ways in lines.items():
            for line in ways:
                with self.subTest(key, line=line):
                    path = where.get(key, "pp/x.py")
                    tree = ast.parse(line)
                    # the once rule reads a whole file, as the claim rule reads where a path is handed to a
                    # Claim, so the line under test stands in for that file's own
                    _once_sites[path] = _once_flags_in(tree)
                    self.assertTrue(any(RULES[key].match(n, path, "f") for n in ast.walk(tree)))
        inside = ast.parse("_write_job(j)")        # the same write outside the store, which is its home
        self.assertTrue(any(RULES["job"].match(n, "pp/lib/jobq.py", "cmd_add") for n in ast.walk(inside)))
        self.assertEqual(RULES["job"].home, {_STORE})
        self.assertNotIn(_STORE, {s.split(":")[0] for s in RULES["job"].sites()})
        for line in ("os.path.isfile(p)", "os.scandir(p)"):
            call = ast.parse(line).body[0].value
            self.assertTrue(RULES["by-name"].match(call, "pp/commands/http.py", "Handler._file"), line)
            self.assertFalse(RULES["by-name"].match(call, "pp/commands/http.py", "App.resolve"), line)
        # the one shape a claim's path may be used in, by argument or by keyword, and the functions that
        # return one; the store's own lock file is not a claim's
        for held in (ast.parse("lock.Claim.take(_worker_claim(q), rec)"),
                     ast.parse("lock.Claim.take(path=os.path.join(qroot(q), 'worker.lock'))")):
            _handed_to_a_claim["pp/held.py"] = _claim_args_in(ast.walk(held))
            self.assertFalse(any(RULES["claim"].match(n, "pp/held.py", "f") for n in ast.walk(held)),
                             ast.unparse(held))
        for qual, line in (("_worker_claim", "return _worker_claim(q)"),
                           ("_watcher_claim", "return os.path.join(_top('watchers'), f'{pid}.claim')"),
                           ("_store_lock", "return os.path.join(_top(), '.lock')")):
            self.assertFalse(any(RULES["claim"].match(n, "pp/x.py", qual) for n in ast.walk(ast.parse(line))), line)
        for ok in ("p = os.path.join(d, 'record.json.lock')", "p = _probe_path(p)", "p = path + '.lock'"):
            self.assertFalse(any(RULES["claim"].match(n, "pp/x.py", "f") for n in ast.walk(ast.parse(ok))), ok)
        # ordinary code is no once-flag: a value a test normalises, a default it fills in, a row added to a
        # list that already has some, a wait that marks nothing, a comparison, a bound, a lookup, a filter
        for ok in ("out = ''\nout = read()\nif out:\n    out = out.strip()",
                   "if not env:\n    env = dict(os.environ)",
                   'rows = []\nif rows:\n    rows.append("total")',
                   "done = []\nif not done:\n    return",
                   "if args[:2] == ['queue', 'wait']:\n    args += ['--poll']",
                   "ticks = []\nif len(ticks) > 500:\n    raise SystemExit('it never got there')",
                   "seen = {}\nif seen.get(k):\n    seen[k] = 1",
                   "out = []\nfor x in xs:\n    if cond(x):\n        out.append(x)"):
            tree = ast.parse(ok)
            _once_sites["tests/x.py"] = _once_flags_in(tree)
            self.assertFalse(any(RULES["once"].match(n, "tests/x.py", "f") for n in ast.walk(tree)), ok)

    def test_the_module_rules_see_an_alias_a_from_import_and_a_getattr(self):
        """The rules built on a module's own functions, tried on every way to one. The most ordinary bypass is
        not a clever one: it is `from subprocess import Popen`, which states no group, no stdin and no
        environment, or `from time import sleep` in a test that should wait for a state. So each is tried on
        the module under an alias, the function imported by its name and under an alias, the name as a string
        in a getattr, and the function put in a name of its own."""
        ways = {"spawn": ("from subprocess import Popen", "from subprocess import run as r",
                          "import subprocess as sp", "from os import execv", "import os as o",
                          "getattr(subprocess, 'Popen')(['x'])", "getattr(os, 'execv')(p, a)",
                          "start = subprocess.Popen", "os.posix_spawn(p, a, e)", "os.system('x')"),
                "lock": ("from fcntl import flock", "from fcntl import lockf as lf", "import fcntl as f",
                         "getattr(fcntl, 'flock')(fd, 2)", "take = fcntl.flock",
                         "lock.fcntl.flock(fd, 2)"),
                "sleep": ("from time import sleep", "from time import sleep as nap", "import time as t",
                          "getattr(time, 'sleep')(1)", "wait = time.sleep"),
                "alive": ("from pp.lib.proc import alive", "from pp.lib import proc as p",
                          "import pp.lib.proc as p", "getattr(proc, 'alive')(1)", "ask = proc.alive",
                          "pp.lib.proc.alive(1)"),
                "environ": ("environ['X'] = '1'", "del environ['X']", "environ.pop('X', None)",
                            "o.environ['X'] = '1'", "o.environ.update(env)", "o.environ.clear()",
                            "getattr(os, 'environ')['X'] = '1'", "getattr(os, 'environ').update(env)")}
        # and what each must not see: a read, a name that merely ends the same way, and the import that is
        # the right one
        ok = {"spawn": ("proc.spawn(a, group='own', stdin='none', env='inherit')", "import os", "import subprocess",
                        "from subprocess import PIPE", "r = subprocess.PIPE", "os.path.join(a, b)",
                        "raise subprocess.TimeoutExpired(a, b)"),
              "lock": ("p = path + '.lock'", "import fcntl"),
              "sleep": ("from time import monotonic", "import time", "t = time.monotonic()", "self.sleep(1)"),
              "alive": ("from pp.lib import proc", "claim.alive", "if t.alive: pass", "proc.read(pid)"),
              "environ": ("os.environ.get('X')", "d = dict(os.environ)", "v = os.environ['X']",
                          "env.update(more)", "env['X'] = '1'", "self.environ = env")}
        where = {"sleep": "tests/x.py", "environ": "tests/x.py"}
        for key, lines in ways.items():
            for line in lines:
                path = where.get(key, "pp/x.py")
                with self.subTest(key, line=line):
                    self.assertTrue(any(RULES[key].match(n, path, "f") for n in ast.walk(ast.parse(line))))
            for line in ok[key]:
                path = where.get(key, "pp/x.py")
                with self.subTest(key, allowed=line):
                    self.assertFalse(any(RULES[key].match(n, path, "f") for n in ast.walk(ast.parse(line))))
        # the primitive reached through pp's own lock module: in lock's own test it is the read that keeps the
        # real one and the primitive the patch beside it puts back, and in pp it is a lock taken outside
        # pp/lib/lock.py, which is the whole of the rule
        for line in ("real = lock.fcntl.flock", "lock.fcntl.flock(fd, 2)"):
            with self.subTest(via=line):
                self.assertFalse(any(RULES["lock"].match(n, "tests/test_lock.py", "f")
                                     for n in ast.walk(ast.parse(line))))
                self.assertTrue(any(RULES["lock"].match(n, "pp/x.py", "f") for n in ast.walk(ast.parse(line))))
        # the patch itself hands mock a module and a name and takes no lock, so no rule is about it
        patch = ast.parse("mock.patch.object(lock.fcntl, 'flock', fake)")
        for path in ("tests/test_lock.py", "pp/x.py"):
            with self.subTest(patch=path):
                self.assertFalse(any(RULES["lock"].match(n, path, "f") for n in ast.walk(patch)))
        # in a test only a process that lives beside it counts, and the module under an alias hides one
        for line in ("subprocess.Popen(['x'])", "import subprocess as sp", "from subprocess import Popen"):
            with self.subTest(beside=line):
                self.assertTrue(any(RULES["spawn"].match(n, "tests/x.py", "f") for n in ast.walk(ast.parse(line))))
        for line in ("subprocess.run(['x'])", "from subprocess import run", "subprocess.check_output(['x'])"):
            with self.subTest(over=line):
                self.assertFalse(any(RULES["spawn"].match(n, "tests/x.py", "f") for n in ast.walk(ast.parse(line))))

    def test_the_job_rule_sees_every_way_to_a_record(self):
        """A bypass is written whichever way is at hand, so each way is tried on the rule: the primitive under
        another name, out of an import, through the module under another name or as a string, the file opened
        by hand, and the jobs directory put in a name first, which is how such a path is usually written."""
        bypasses = ("w = _write_job",                               # an alias of the primitive
                    "from pp.lib.jobstore import _write_job",       # or out of it, into any file
                    "from pp.lib import jobstore as q",             # the module renamed: q._write_job is hidden
                    "import pp.lib.jobq as q",
                    "getattr(jobq, '_write_job')(j)",               # the name as a string
                    "open(os.path.join(st, 'queue', 'default', 'jobs', '1.json'), 'w')",
                    "os.replace(tmp, os.path.join(qroot(q, 'jobs'), '1.json'))",
                    "os.remove(qroot(q, 'jobs') + '/1.json')",
                    "jobs = os.path.join(st, 'queue', 'default', 'jobs')",       # the directory in a name
                    "d = qroot(q, 'jobs')",
                    "d = paths.state_dir('queue', q, 'jobs')",
                    "d = f'{st}/queue/{q}/jobs'",
                    "for f in os.listdir(os.path.join(st, 'queue', q, 'jobs')): pass")
        for line in bypasses:
            for path in ("pp/lib/jobq.py", "pp/commands/queue.py", "tests/test_queue.py"):
                with self.subTest(line=line, path=path):
                    self.assertTrue(any(RULES["job"].match(n, path, "f") for n in ast.walk(ast.parse(line))))
        # and none of them is reported in the store's own file, which is the rule's home
        self.assertNotIn(_STORE, {s.split(":")[0] for s in RULES["job"].sites()})
        for ok in ("from pp.lib import jobq", "open(os.path.join(d, 'rc', jid))", "jobstore.find(jid)"):
            with self.subTest(ok=ok):
                self.assertFalse(any(RULES["job"].match(n, "pp/commands/queue.py", "f")
                                     for n in ast.walk(ast.parse(ok))))

    def test_the_job_process_rule_sees_every_way_to_a_signal(self):
        """A shortcut past the one actor that ends a job is written whichever way is at hand, so each way is
        tried on the rule, in the file where `pp queue` lives as well as in jobq.py: the function through the
        module, the module renamed, the function imported by name, an alias of it, the name as a string, and
        the same for each of the Worker's own entry points into a job's signal and its reap."""
        bypasses = ("proc.signal_group(h, 15)",                 # the signalling through the module
                    "proc.signal_pid(h, 15)",
                    "proc.signal_and_wait([p], 15)",
                    "from pp.lib.proc import signal_group",     # out of it, into any file
                    "from pp.lib import proc as p",             # the module renamed: p.signal_group is hidden
                    "import pp.lib.proc as p",
                    "p.signal_group(h, 15)",
                    "sig = proc.signal_group",                  # an alias of it
                    "getattr(proc, 'signal_group')(h, 15)",     # the name as a string
                    "signal_group(h, 15)",                      # the function on its own, once imported
                    "Worker(q).carry_out()",                    # and the Worker's own, every way too
                    "jobcore.Worker(q).carry_out()",
                    "w.carry_out()",
                    "again = w.carry_out",
                    "getattr(w, 'carry_out')()",
                    "w._step(j, facts)",
                    "w._send(j, a)",
                    "w._signal_group(j, 15)",
                    "w.tick()",                             # one tick does the whole of the reserved work
                    "jobcore.Worker(q).tick()",
                    "getattr(w, 'tick')()")
        for line in bypasses:
            for path in ("pp/lib/jobq.py", "pp/commands/queue.py"):
                with self.subTest(line=line, path=path):
                    self.assertTrue(any(RULES["job-process"].match(n, path, "cmd_kill")
                                        for n in ast.walk(ast.parse(line))))
        # and none of them is reported in the three homes: proc defines the signalling, `pp kill` signals
        # the processes a person names, and the core is the worker's own pass over its jobs, the stop of a
        # worker and the loop that runs a tick
        self.assertEqual(RULES["job-process"].home, set(_SIGNAL_FILES))
        self.assertFalse({x.split(":")[0] for x in RULES["job-process"].sites()} & set(_SIGNAL_FILES))
        for ok in ("from pp.lib import proc", "from . import proc", "proc.read(pid)", "signal.signal(15, h)",
                   "p.wait()", "getattr(proc, 'read')(pid)", "Worker.wake(w.handle)",
                   "self._send(404, body, 'text/html')"):     # another class's method of the same name
            with self.subTest(ok=ok):
                self.assertFalse(any(RULES["job-process"].match(n, "pp/commands/queue.py", "f")
                                     for n in ast.walk(ast.parse(ok))))

    def test_the_three_homes_of_a_jobs_signal_are_the_ones_that_signal(self):
        """The home set itself is pinned here, so letting the rule off a fourth file fails this test, and
        each of the three is read for what it is let off for. In the core, `_signal_group` is where a signal
        to a job goes out, `wake` signals a worker, `worker_stop` stops one, and `work` is where a tick is
        run; nothing outside the core does any of it, which is what the rule holds."""
        self.assertEqual(set(_SIGNAL_FILES), {"pp/lib/proc.py", "pp/commands/kill.py", _CORE})
        did: dict[str, list[str]] = {}
        for path in _SIGNAL_FILES:
            for qual, node in _walk(path):
                if _job_process(node, "pp/x.py", "f"):      # the same node, read outside every home
                    did.setdefault(path, []).append(qual)
        self.assertTrue(did.get("pp/lib/proc.py"), "proc no longer signals: take it off the rule's homes")
        self.assertTrue(did.get("pp/commands/kill.py"), "`pp kill` no longer signals: take it off the homes")
        for method in ("Worker._signal_group", "Worker.wake", "worker_stop", "work"):
            self.assertIn(method, did[_CORE])

    def test_the_worker_claim_rule_sees_every_way_to_the_claim(self):
        """A second look at a queue's worker claim, and a second holder of it, are what the one construct
        keeps out, so each way to the claim is tried on the rule: the read through the
        module, the module renamed, the read imported by name, an alias of it, the name as a string, the bare
        name in the file that defines it, and a take or a read through lock.Claim with a worker claim's path,
        spelled out as well as from the helper."""
        bypasses = ("jobcore.worker(q)",                        # the read through the module
                    "from pp.lib.jobcore import worker",        # out of it, into any file
                    "from pp.lib.jobcore import worker as live",
                    "from pp.lib import jobcore as q",          # the module renamed: q.worker is hidden
                    "import pp.lib.jobcore as q",
                    "ask = jobcore.worker",                     # an alias of it
                    "getattr(jobcore, 'worker')(q)",            # the name as a string
                    "worker(q)",                                # the function on its own, once imported
                    "lock.Claim.holder(_worker_claim(q))",      # and the same look taken by hand
                    "jobq.lock.Claim.holder(jobcore._worker_claim(q))",
                    "lock.Claim.holder(os.path.join(qroot(q), 'worker.lock'))",
                    "lock.Claim.take(_worker_claim(q), rec)",   # or a second holder of it
                    "lock.Claim.take(path=os.path.join(qroot(q), 'worker.lock'))")
        for line in bypasses:
            for path in ("pp/lib/jobq.py", "pp/commands/queue.py"):
                with self.subTest(line=line, path=path):
                    self.assertTrue(any(RULES["worker-claim"].match(n, path, "cmd_add")
                                        for n in ast.walk(ast.parse(line))))
        # and none of them is reported in the core's own file, which is the rule's home
        self.assertNotIn(_CORE, {x.split(":")[0] for x in RULES["worker-claim"].sites()})
        for ok in ("from pp.lib import jobcore", "jobcore.worker_now(q)", "jobcore.worker_up(q)",
                   "lock.Claim.holder(run_claim(q, jid))",
                   "lock.Claim.take(_probe_claim(p))", "w.worker"):
            with self.subTest(ok=ok):
                self.assertFalse(any(RULES["worker-claim"].match(n, "pp/commands/queue.py", "f")
                                     for n in ast.walk(ast.parse(ok))))

    def test_the_home_of_the_worker_claim_rule_holds_the_ones_that_use_it(self):
        """The rule's one home is the core's file, and these are the functions in it that do what it is let
        off for: `worker` reads the claim's holder, `worker_now` says what that holder is, and `work` takes
        the claim. Nothing outside that file does any of the three, which is what the rule holds."""
        self.assertEqual(RULES["worker-claim"].home, {_CORE})
        did = {qual for qual, node in _walk(_CORE) if _the_worker_claim(node, "pp/x.py", "f")}
        self.assertEqual({q.split(".")[0] for q in did}, {"worker", "worker_now", "work"})

    def test_every_home_does_the_thing(self):
        """A home is let off its rule, so it is no exemption on paper: each file a rule lets off does what it
        is let off for. A home that stops doing it fails here and is taken off, rather than standing as a
        licence for the next call written there. The two rules that name their homes by function rather than
        by file have a test of their own above."""
        for key, rule in RULES.items():
            for path in sorted(rule.home):
                with self.subTest(key, home=path):
                    did = [qual for qual, node in _walk(path) if rule.match(node, path, qual)]
                    self.assertTrue(did, f"{path} no longer {rule.what.split(':')[0]}: take it off the home")

    def test_the_one_wait_under_tests_is_journey_until(self):
        """What the home of the sleep rule is let off for, and all of it: tests/journey.py sleeps in `until`
        and nowhere else, so every other test asks for a state."""
        waits = [qual for qual, node in _walk("tests/journey.py")
                 if RULES["sleep"].match(node, "tests/journey.py", qual)]
        self.assertEqual(waits, ["until"])


class _Echo(socketserver.StreamRequestHandler):
    def handle(self):
        self.wfile.write(b"ok\n")


def _git(key: str, cwd: str) -> str:
    """What git in this environment answers for a config key; "" when it has none."""
    r = subprocess.run(["git", "config", "--get", key], capture_output=True, text=True, cwd=cwd, timeout=60)
    return r.stdout.strip()


class TestTheSandbox(unittest.TestCase):
    """What tests/sandbox.py promises: what a test changes is the scope's for the test's length, and nothing
    the test started is still running when the scope gives it back."""

    def test_a_ticker_never_sees_the_machines_own_state_dir(self):
        """The reason the scope exists: a thread of a test ticks on while the test ends. Every tick here is
        the sandbox's state directory, because the scope stops the thread before putting the real one back."""
        before, cwd = dict(os.environ), os.getcwd()
        seen: list = []

        def tick():
            seen.append(os.environ.get("PP_STATE_DIR"))

        def ticked_a_few_times():
            return len(seen) > 2

        with sandbox.Sandbox() as sb:
            t = sb.ticker(tick, period=0.001)
            sb.chdir()
            journey.until(ticked_a_few_times, timeout=10)
            inside = sb.state
            self.assertEqual(os.getcwd(), sb.work)
        self.assertEqual(set(seen), {inside})       # the sandbox's own on every tick: never the machine's
        self.assertFalse(t.alive)
        self.assertEqual(dict(os.environ), before)
        self.assertEqual(os.getcwd(), cwd)

    def test_an_identity_a_disposition_and_a_filter_all_go_back(self):
        before, filters = dict(os.environ), warnings.filters[:]
        was = signal.getsignal(signal.SIGUSR2)

        def handler(_sig, _frame):
            pass

        with sandbox.Sandbox() as sb:
            sb.as_session("sandbox-contract")
            sb.on_signal(signal.SIGUSR2, handler)
            sb.filter_warnings("ignore", ResourceWarning)
            self.assertEqual(session.caller()["session"], "sandbox-contract")   # what pp reads is the test's
            self.assertEqual(warnings.filters[0][:3], ("ignore", None, ResourceWarning))
            self.assertIs(signal.getsignal(signal.SIGUSR2), handler)
        self.assertEqual(dict(os.environ), before)
        self.assertEqual(signal.getsignal(signal.SIGUSR2), was)
        self.assertEqual(warnings.filters, filters)

    def test_a_tests_git_reads_no_config_of_the_machines(self):
        """What the scope owns of git, so that a test's repositories are the test's alone. A global config on
        the machine reaches every repository a test makes: a `core.hooksPath` (the ordinary pre-commit setup)
        runs that hook on the test's own commits, and an identity there is who the test commits as.

        The outer scope here stands for the machine, with a HOME and a global config of its own. The inner
        scope is a test's: git in it reads neither of those, commits as the scope's own identity, and the
        machine's config is back once it closes."""
        with sandbox.Sandbox() as machine:
            elsewhere = machine.dir("machine")
            leaked = os.path.join(elsewhere, "gitconfig")
            with open(leaked, "w") as f:
                f.write("[core]\n\thooksPath = /nowhere\n[user]\n\tname = The Machine\n\temail = m@m.invalid\n")
            machine.setenv(HOME=elsewhere, GIT_CONFIG_GLOBAL=leaked)
            self.assertEqual(_git("core.hooksPath", machine.work), "/nowhere")   # what a test would read
            with sandbox.Sandbox() as sb:
                self.assertEqual(_git("core.hooksPath", sb.work), "")            # and what it reads instead
                self.assertEqual(_git("user.name", sb.work), "")
                self.assertEqual(os.environ["HOME"], sb.home)
                self.assertNotEqual(sb.home, elsewhere)
                repo = sb.dir("repo")
                for args in (["init", "-q"], ["commit", "-qm", "c", "--allow-empty"]):
                    subprocess.run(["git", *args], check=True, capture_output=True, cwd=repo, timeout=60)
                who = subprocess.run(["git", "log", "-1", "--format=%an <%ae>"], capture_output=True, text=True,
                                     check=True, cwd=repo, timeout=60).stdout.strip()
                self.assertEqual(who, "T <t@example.invalid>")      # the scope's identity, not the machine's
            self.assertEqual(_git("core.hooksPath", machine.work), "/nowhere")   # the machine's, given back

    def test_a_server_it_hands_out_stops_and_lets_go_of_its_port(self):
        with sandbox.Sandbox() as sb:
            server = sb.serving(socketserver.ThreadingTCPServer(("127.0.0.1", 0), _Echo))
            with socket.create_connection(server.server_address, timeout=10) as c:
                self.assertEqual(c.recv(16), b"ok\n")
        self.assertEqual(server.socket.fileno(), -1)

    def test_a_ticker_that_raised_says_so_once_the_scope_is_back(self):
        """A thread that died of an exception would take the reason with it, and the test would pass on it."""
        before = os.environ.get("PP_STATE_DIR")

        def boom():
            raise ValueError("the ticker broke")

        sb = sandbox.Sandbox().open()
        t = sb.ticker(boom, period=0.001)
        journey.until(lambda: not t.alive, timeout=10)
        with self.assertRaises(ValueError):
            sb.close()
        self.assertEqual(os.environ.get("PP_STATE_DIR"), before)

    def test_a_thread_that_will_not_stop_keeps_the_environment_in_the_sandbox(self):
        """The reason the scope exists, where it cannot be kept: a thread that outlasts the bound. The machine's
        own state directory is not given back under it, the tree it is writing in stays, and the test is told
        which thread it was. Closing the scope again, once that thread has stopped, gives everything back."""
        before, go = os.environ.get("PP_STATE_DIR"), threading.Event()
        wrote: list = []
        sb = sandbox.Sandbox().open()
        with mock.patch.object(sandbox, "JOIN_WAIT", 0.2):
            t = sb.ticker(lambda: wrote.append(os.environ["PP_STATE_DIR"]) or go.wait(30), period=0,
                          name="stuck-tick")
            journey.until(lambda: wrote, timeout=10, what="the thread to be inside its tick")
            with self.assertRaises(sandbox.StillRunning) as caught:
                sb.close()
        self.assertIn("stuck-tick", str(caught.exception))
        self.assertIn(sb.root, str(caught.exception))
        self.assertEqual(os.environ["PP_STATE_DIR"], sb.state)      # never the machine's, under a live thread
        self.assertTrue(os.path.isdir(sb.root))
        go.set()                                                    # it stops, and now the scope can close
        journey.until(lambda: not t.alive, timeout=10)
        sb.close()
        self.assertEqual(os.environ.get("PP_STATE_DIR"), before)
        self.assertFalse(os.path.exists(sb.root))
        self.assertEqual(set(wrote), {sb.state})

    def test_what_a_test_puts_back_later_waits_for_its_threads(self):
        """`later` is for the test's own way out: it runs once every thread of the scope's has stopped, so no
        tick of theirs ever sees what it put back, and before the scope gives the environment back, so what it
        reads is still the scope's."""
        seen, state = [], ["the scope's"]
        with sandbox.Sandbox() as sb:
            sb.later(lambda: state.append("put back"))
            sb.later(lambda: seen.append(os.environ["PP_STATE_DIR"]))
            sb.ticker(lambda: seen.append(state[-1]), period=0.001)
            journey.until(lambda: len(seen) > 2, timeout=10)
            inside = sb.state
        self.assertEqual(state[-1], "put back")
        self.assertNotIn("put back", seen[:-1])     # no tick saw it
        self.assertEqual(seen[-1], inside)          # and `later` itself read the scope's own state directory


if __name__ == "__main__":
    unittest.main()
