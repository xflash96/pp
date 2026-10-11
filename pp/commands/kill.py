"""Kill processes by pid, port or pattern, never the shell that asked

    pp kill 12345              that pid (SIGTERM, then SIGKILL after --grace seconds if it is still there)
    pp kill -s HUP 12345       another signal; only TERM, INT and QUIT are followed up with SIGKILL
    pp kill :8080              whatever listens on port 8080 (tcp/udp, IPv4/IPv6)
    pp kill 'vite --port'      every process whose command line matches the regex; shows them and asks first
    pp kill -x node            processes NAMED exactly node (not ones merely mentioning it; names past the
                               kernel's 15-character cut match by their program's file name)
    pp kill -n :8080           only show what would be killed
    pp kill --tree 12345       the pid and all its descendants
    pp kill --ports            list every listening port with its owner (or docker container)

Patterns never match pp itself or any of its ancestors. That is the `pkill -f PATTERN` trap: the pattern is in the
command line of the shell running pkill, so pkill kills its own shell (exit 143 from `sh -c "pkill -f foo"`,
which is the SIGTERM it sent itself).
Patterns only match your own processes unless --any-user.

Asking: pid and port targets are explicit and act at once; a pattern shows its matches and asks. Without a
terminal it refuses unless -y (agents: `-n` to look, then `-y`). A port published by docker is held by root's
docker-proxy: pp names the container and offers `docker stop` instead of killing the proxy.

--tree: pp takes each target's descendants again when it signals, and once more after the grace window, so a
child forked while it was looking or asking is signalled too. A child started after that last look is out of
scope. One still alive at the end is listed with the others and the exit status is 1.

Exit: 0 every target is gone, or signalled where the signal is not followed up with SIGKILL (`-s` naming
anything but TERM, INT, QUIT or KILL, or `--no-escalate`): a target meant to survive the signal is a success.
1 something survived a signal meant to end it, was denied, or nothing matched. 2 refused or bad usage.
"""
from __future__ import annotations

import os
import pwd
import re
import shutil
import signal

from pp.lib import proc, ui


def _user(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def _sig(s: str) -> int:
    s = s.upper()
    if s.isdigit():
        return int(s)
    try:
        return int(getattr(signal, s if s.startswith("SIG") else "SIG" + s))
    except AttributeError:
        ui.die(f"unknown signal {s}", 2)


def docker_ports() -> dict[int, list[str]]:
    """host port -> container names publishing it ({} without docker or when it does not answer quickly)."""
    if not shutil.which("docker"):
        return {}
    try:
        r = proc.run(["docker", "ps", "--format", "{{.Names}}\t{{.Ports}}"], group="own", stdin="none",
                     env="inherit", timeout=5)
    except OSError:
        return {}
    if r.returncode != 0:       # it did not answer in time, or it complained: no container to name
        return {}
    res: dict[int, list[str]] = {}
    for line in r.stdout.splitlines():
        name, _, ports = line.partition("\t")
        # "0.0.0.0:8080->80/tcp, [::]:8080->80/tcp, 0.0.0.0:9000-9002->9000-9002/tcp"
        for m in re.finditer(r":(\d+)(?:-(\d+))?->", ports):
            lo, hi = int(m.group(1)), int(m.group(2) or m.group(1))
            for p in range(lo, hi + 1):
                if name not in res.setdefault(p, []):
                    res[p].append(name)
    return res


# a signal sent to end a process: one that survives it gets SIGKILL after --grace. Any other (-s HUP to reload,
# USR1, STOP, 0) is sent alone, because the process is meant to survive it.
ENDING = {signal.SIGTERM, signal.SIGINT, signal.SIGQUIT, signal.SIGKILL}


def _names(p: proc.Proc) -> set[str]:
    """What -x compares: comm, and argv[0]'s file name; comm is cut at 15 characters (systemd-journald is
    "systemd-journal"), so a longer name only matches the program's own."""
    return {p.comm, os.path.basename(p.cmdline[0])} if p.cmdline else {p.comm}


def _shown(cmd: str, width: int = 120) -> str:
    """A command line as the table shows it: control characters escaped (another program chooses its argv, and an
    escape sequence or newline in it must not redraw what you are asked to confirm), cut with … past width."""
    cmd = ui.printable(cmd, tabs=False)
    return cmd if len(cmd) <= width else cmd[:width - 1] + "…"


def _row(p: proc.Proc, why: str) -> list:
    return [p.pid, _user(p.uid), ui.human_duration(p.age()), ui.human_size(p.rss_kb * 1024), why, _shown(p.cmd)]


def _tree_now(trees: list[int], known: dict[int, int], me: set[int]) -> dict[int, proc.Proc]:
    """The descendants of each --tree target that are not in `known` yet, read now. The plan comes from one
    snapshot, so a tree is taken again before pp signals and again after the grace window: a child forked in
    between is in scope, and nothing is reported gone that pp never looked for."""
    if not trees:
        return {}
    procs = proc.processes()
    return {d: procs[d] for pid in trees for d in proc.descendants(pid, procs)
            if d not in known and d not in me}


def cmd_ports(a) -> int:
    socks = proc.sockets()
    owners = proc.socket_owners({s["inode"] for s in socks})
    dports = docker_ports()
    procs = {}
    rows, js = [], []
    for s in sorted(socks, key=lambda s: (s["port"], s["proto"])):
        pids = sorted(owners.get(s["inode"], set()))
        who = []
        for pid in pids:
            p = procs.setdefault(pid, proc.read(pid))
            if p:
                who.append(f"{pid} {_shown(p.cmd, 70)}")
        if not who and s["port"] in dports:
            who = ["docker: " + ", ".join(dports[s["port"]])]
        if not who:
            who = ["? (another user's)"]
        addr = f"[{s['addr']}]" if ":" in s["addr"] else s["addr"]
        rows.append([s["proto"], f"{addr}:{s['port']}", "; ".join(who)])
        js.append({**s, "pids": pids, "docker": dports.get(s["port"], [])})
    if a.json:
        ui.print_json(js)
    else:
        print(ui.table(rows, ["proto", "address", "owner"]))
    return 0


def main(argv: list[str]) -> int:
    ap = ui.parser("kill", __doc__)
    ap.add_argument("targets", nargs="*", metavar="TARGET", help="PID, :PORT, or a regex over the command line")
    ap.add_argument("-n", "--dry-run", action="store_true", help="show what would be signalled, do nothing")
    ap.add_argument("-y", "--yes", action="store_true", help="do not ask (patterns, docker stop)")
    ap.add_argument("-s", "--signal", default="TERM", help="signal to send first (default TERM)")
    ap.add_argument("-9", dest="nine", action="store_true", help="SIGKILL straight away")
    ap.add_argument("--grace", type=float, default=3.0,
                    help="seconds before escalating to SIGKILL (default 3; after TERM, INT or QUIT only)")
    ap.add_argument("--no-escalate", action="store_true", help="send the signal only; never SIGKILL")
    ap.add_argument("--tree", action="store_true", help="also every descendant of each target")
    ap.add_argument("-x", "--exact", action="store_true", help="pattern is the exact process name (comm, or its program's file name)")
    ap.add_argument("-F", "--fixed", action="store_true", help="pattern is a plain string, not a regex")
    ap.add_argument("-i", "--ignore-case", action="store_true")
    ap.add_argument("--any-user", action="store_true", help="patterns may match other users' processes")
    ap.add_argument("--ports", action="store_true", help="list listening ports and their owners")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)

    if a.ports:
        return cmd_ports(a)
    if not a.targets:
        ap.print_usage()
        return 2
    sig = signal.SIGKILL if a.nine else _sig(a.signal)      # a typo fails before anything is shown or asked

    procs = proc.processes()
    me = set(proc.ancestors(procs=procs))
    uid = os.getuid()
    chosen: dict[int, str] = {}          # pid -> why it was chosen
    ask = False
    containers: list[tuple[int, list[str]]] = []
    problems: list[str] = []

    for t in a.targets:
        if t.isdigit():
            pid = int(t)
            if pid not in procs:
                problems.append(f"no process {pid}")
            elif pid in me:
                problems.append(f"{pid} is pp itself, its caller or an ancestor of both; not killing it")
            else:
                chosen.setdefault(pid, f"pid {pid}")
        elif re.fullmatch(r":\d+", t):
            port = int(t[1:])
            if not 0 < port < 65536:
                ui.die(f"{t}: a port is 1-65535", 2)
            socks, pids = proc.port_owners(port)
            if not socks:
                problems.append(f"nothing listens on port {port}")
                continue
            for pid in sorted(pids):
                if pid in me:
                    problems.append(f"port {port} is held by pp's own caller ({pid}); not killing it")
                else:
                    chosen.setdefault(pid, f"port {port}")
            if not pids:
                names = docker_ports().get(port)
                if names:
                    containers.append((port, names))
                else:
                    problems.append(f"port {port} is held by another user's process; try: sudo pp kill :{port}")
        else:
            ask = True
            flags = re.I if a.ignore_case else 0
            try:
                rx = re.compile(re.escape(t) if a.fixed else t, flags)
            except re.error:
                rx = re.compile(re.escape(t), flags)
            hits = 0
            for p in procs.values():
                if p.pid in me or (p.uid != uid and not a.any_user) or not p.cmdline:
                    continue        # never the caller; kernel threads have no cmdline
                if (any(rx.fullmatch(n) for n in _names(p)) if a.exact else rx.search(p.cmd)):
                    chosen.setdefault(p.pid, f"/{t}/")
                    hits += 1
            if not hits:
                problems.append(f"no process matches {t!r}" + ("" if a.any_user else " (own processes only; --any-user widens)"))

    for pid in list(chosen):
        # a `:PORT` owner is read fresh and can be younger than the snapshot: put it in procs, or drop it, so
        # every later use (the table, the uid warning, the JSON, the start times) reads a Proc that exists
        p = procs.get(pid) or proc.read(pid)
        if p:
            procs[pid] = p
        else:
            del chosen[pid]
            problems.append(f"no process {pid} any more")

    trees = list(chosen) if a.tree else []       # the targets --tree expands, before their descendants join
    for pid in trees:
        for d in proc.descendants(pid, procs):
            if d not in me:
                chosen.setdefault(d, f"child of {pid}")

    for msg in problems:
        ui.warn(msg)
    others = sorted({_user(procs[pid].uid) for pid in chosen if procs[pid].uid != uid})
    if others and uid != 0:
        ui.warn(f"{sum(procs[pid].uid != uid for pid in chosen)} of these belong to {', '.join(others)}:"
                f" signalling them is denied unless pp runs as root")
    if a.json and (a.dry_run or not chosen and not containers):
        ui.print_json({"pids": {str(k): v for k, v in chosen.items()}, "containers": dict(containers),
                       "commands": {str(k): procs[k].cmd for k in chosen}, "problems": problems})
    if not chosen and not containers:
        return 1

    if chosen and not a.json:
        print(ui.table([_row(procs[pid], why) for pid, why in chosen.items()],
                       ["pid", "user", "age", "rss", "matched", "command"]))
    for port, names in containers:
        print(f"port {port} is published by docker container {', '.join(names)}"
              f"; `docker stop {' '.join(names)}` stops it")

    if a.dry_run:
        return 0
    if (ask or containers) and not ui.confirm(f"signal {len(chosen)} process(es)" + (" and stop the container(s)" if containers else "") + "?", a.yes):
        return 2

    starts = {pid: procs[pid].start for pid in chosen}
    for pid, p in _tree_now(trees, starts, me).items():
        procs[pid], starts[pid] = p, p.start       # a child forked while pp looked, or while it asked
    escalate = sig in ENDING and not a.no_escalate
    grace = a.grace if sig in ENDING else 0        # nothing to wait for after a signal it is meant to survive
    res = proc.signal_and_wait(list(starts), sig, grace, escalate=escalate, starts=starts) if starts else {}
    late = _tree_now(trees, starts, me)            # and one forked while the targets were dying
    if late:
        for pid, p in late.items():
            procs[pid], starts[pid] = p, p.start
        res.update(proc.signal_and_wait(list(late), sig, grace, escalate=escalate, starts=starts))
    if not escalate and sig != signal.SIGKILL:      # e.g. -s HUP to make a daemon reload: surviving is the point
        res = {k: ("signalled" if v == "alive" else v) for k, v in res.items()}
    stopped = {}
    for port, names in containers:
        # docker stop gives each container 10s of its own to stop, so pp's own bound has to be the looser one
        wait = 30.0 + 15 * len(names)
        try:
            r = proc.run(["docker", "stop", *names], group="own", stdin="none", env="inherit", timeout=wait)
        except OSError as e:
            stopped[",".join(names)] = f"failed: {e}"
            continue
        why = r.stderr.strip() or (f"it did not answer in {wait:g}s" if r.timed_out else "")
        stopped[",".join(names)] = "stopped" if r.returncode == 0 else "failed: " + why

    if a.json:
        ui.print_json({"pids": {str(k): v for k, v in res.items()}, "containers": stopped,
                       "commands": {str(k): procs[k].cmd for k in res if k in procs}, "problems": problems})
    else:
        colour = {"exited": "green", "killed": "yellow", "gone": "dim", "stopped": "green", "signalled": "green"}
        for k, v in [*res.items(), *((f"docker:{n}", v) for n, v in stopped.items())]:
            print(f"  {k}: {ui.c(v, colour.get(v, 'red'))}")
    ok = {"exited", "killed", "gone", "stopped", "signalled"}
    return 0 if all(v in ok for v in [*res.values(), *stopped.values()]) and not problems else 1
