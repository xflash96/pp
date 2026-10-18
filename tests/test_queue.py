import dataclasses
import inspect
import io
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp.commands import queue as queue_cmd  # noqa: E402     the help text, which states the rules jobq keeps
from pp.lib import config, jobpolicy, jobcore, jobq, jobstore, proc  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def card(index=0, total=24.0, used=0.0):
    return {"index": index, "name": "fake", "total_gb": total, "used_gb": used, "free_gb": total - used, "util": "0"}


def worker_pid(q: str = "default") -> int | None:
    """The pid of queue q's worker, as `worker_now` answers it: None where nobody holds the claim, and while a
    worker that holds it has not recorded itself. What every verb prints from, read here for a test."""
    return jobcore.worker_now(q).pid


def _sentences(doc: str) -> list[str]:
    """The sentences of a help text, each with its runs of whitespace made one space, so a sentence the help
    wraps over two lines is one sentence here."""
    return [s for s in re.split(r"(?<=\.) +", " ".join(doc.split())) if s]


def _is_job_file(path) -> bool:
    path = str(path)
    return path.endswith(".json") and os.path.basename(os.path.dirname(path)) == "jobs"


def _state_in(path) -> str | None:
    """The state the job file at path holds, or None when there is no readable record in it. Read the way pp
    reads one, by the id the file's name gives."""
    j = jobstore.find(int(os.path.basename(str(path)).split(".")[0]))
    return j["state"] if j else None


ASK_FOR_THE_LOCK = """
import sys
from pp.lib import lock
try:
    with lock.locked(sys.argv[1], block=False, shared=sys.argv[2] == "shared"):
        print("took it")
except lock.Busy:
    print("busy")
"""


LEFT_A_RECORD = """
import json
import os
import sys
from pp.lib import jobpolicy, jobcore, jobq, jobstore, lock
lock.Claim.take(jobcore._worker_claim(sys.argv[1]), json.loads(sys.argv[2]))
os._exit(0)     # as a worker that was killed goes: no way out of its own runs, so its record stays behind
"""


KILLED_TAKING_A_LEASE = """
import signal
import sys
from pp.lib import lock, proc


def rename(*a):
    proc.raise_to_self(signal.SIGKILL)      # between the create and the rename, where no way out of its own runs


lock.os.rename = rename
lock.Claim.make(sys.argv[1], {"pid": 1})
"""


def text(path) -> str:
    """What a file holds, read and closed in one step. A file a test leaves open is a ResourceWarning of its
    own, and the only one these tests ignore is the live child of a worker they drop on purpose."""
    with open(path) as f:
        return f.read()


class QueueCase(unittest.TestCase):
    """The scope these tests run in is the sandbox's (tests/sandbox.py): a temporary state and config tree, no
    agent session in the environment (a test may run inside one, and --mine must see only the sessions it
    names), and every worker it ticks beside itself stopped before any of that goes back."""

    def setUp(self):
        self.sb = sandbox.enter(self)
        self.sb.filter_warnings("ignore", ResourceWarning)  # tests drop Worker objects with live children on purpose
        self.work, self.cfg = self.sb.dir("work"), self.sb.dir("config")
        self.addCleanup(self._restore)      # after entering the scope, so it runs while the store is still this one
        cards0 = jobq._cards
        jobq._cards = lambda: None
        # the card reader goes back after every worker of this test has stopped, so no tick of theirs ever asks
        # the machine's own cards
        self.sb.later(lambda: setattr(jobq, "_cards", cards0))
        # when this process last failed to bring a worker up, per queue: it holds a start off for START_EVERY,
        # and these tests are one process, so one test's queue must not hold the next test's off
        jobcore._none_came_up.clear()
        self.tables: dict = {}
        self.shared = ""
        self.config("slots = 3")

    def _restore(self):
        """The queue's own part of the way out: no job of this test's is left running."""
        for j in jobstore.jobs("running"):         # never leave a test job behind
            h = jobcore._group_handle(j)
            if h is not None:
                proc.signal_group(h, signal.SIGKILL)

    def config(self, resources: str, table: str = "", q: str = "default"):
        self.tables[q] = (table, resources)
        self._write_config()

    def resources(self, text: str):
        """The shared pools: [resources]."""
        self.shared = text
        self._write_config()

    def _write_config(self):
        with open(os.path.join(self.cfg, "config.toml"), "w") as f:
            f.write(f"[resources]\n{self.shared}\n")
            for name, (t, r) in self.tables.items():
                f.write(f"[queue.{name}]\n{t}\n[queue.{name}.resources]\n{r}\n")

    def why(self, w) -> str:
        """What the head of worker w's queue waits for, with its detail, as w records it."""
        self.assertIsNotNone(w.waiting, "the worker waits for nothing")
        return w.waiting["why"]

    def as_the_worker(self, w=None, q: str = "default") -> jobcore.Worker:
        """This test's worker, holding queue q's worker claim: the worker a verb finds, so a kill wakes it
        and nothing starts a worker beside it.

        The test enters jobcore.waking() with it, so a verb's wake lands in this process's own wakeup pipe
        instead of ending the test. `self.waits` is that pipe, which a test asks whether a wake arrived on."""
        w = w or jobq.worker_for(q)
        wake = jobcore.waking()
        self.waits = wake.__enter__()
        self.addCleanup(wake.__exit__, None, None, None)
        claim = jobq.lock.Claim.take(jobcore._worker_claim(q), w.record())
        self.assertIsNotNone(claim, f"something else holds the {q} worker claim")
        self.addCleanup(claim.release)
        return w

    def worker_beside(self, w=None, q: str = "default") -> jobcore.Worker:
        """A worker of queue q ticking beside the test until it ends, in place of the one a verb would start.
        What `kill` and `wait` need: only the queue's worker stops a job and records how it ended."""
        w = self.as_the_worker(w, q)
        self.sb.ticker(w.tick)
        return w

    def start_a_worker_here(self, q: str = "default"):
        """Stand in for the worker a verb starts, as the real one is by the time it says it is ready: it
        holds the queue's claim and has recorded itself. It is this process, so the test can tick it, and
        `self.worker` is it."""
        self.worker = w = jobq.worker_for(q)
        claim = jobq.lock.Claim.take(jobcore._worker_claim(q), w.record())
        self.assertIsNotNone(claim, f"the {q} worker claim was not free for the worker it started")
        self.addCleanup(claim.release)
        w.claim = claim
        self.started.append(q)
        return mock.Mock(pid=os.getpid())

    def tick_here(self):
        """One tick of that worker, in place of one pass of a verb's own wait."""
        self.ticks.append(1)
        if len(self.ticks) > 500:
            raise SystemExit("the worker never came to the end of what it was handed")
        if self.worker is not None:
            self.worker.tick()

    @contextmanager
    def one_thread(self, each_pass=None):
        """The scope a kill or a wait drives its queue in: the worker the verb starts is this process, and
        the verb's own wait is what ticks it, since every pass of it makes sure the queue has a worker. So
        the verb, the worker and every tick are this one thread's, in that order, and nothing waits for a
        clock. each_pass: a step to take at the start of every pass, before the worker is asked about.

        A wake the verb sends that worker lands in this process's own wakeup pipe, so no wake of a verb's
        ends the test."""
        self.worker, self.started, self.ticks = None, [], []
        real = jobcore.worker_up

        def worker_up(q, **kw):
            if each_pass is not None:
                each_pass()
            out = real(q, **kw)
            self.tick_here()
            return out
        with jobcore.waking(), mock.patch.object(jobcore, "_start_worker", self.start_a_worker_here), \
                mock.patch.object(jobcore, "worker_up", worker_up), \
                mock.patch.object(jobq, "KILL_POLL", 0.0):
            yield

    def call(self, *argv) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                rc = jobq.run_cli(list(argv), "queue", "default", "test doc")
            except SystemExit as e:
                rc = e.code
        return rc, out.getvalue(), err.getvalue()

    def add(self, *args, cmd="true", cwd=None, q=None) -> int:
        qa = ["-q", q] if q else []
        rc, out, err = self.call("add", *qa, "--no-start", "--cwd", cwd or self.work, *args, "--", "sh", "-c", cmd)
        self.assertEqual(rc, 0, err)
        return int(out.split()[-1])

    def state(self, jid):
        j = jobstore.find(jid)
        return j["state"] if j else None

    def files(self, q: str = "default") -> set:
        """Every file under queue q's directory, by its path relative to that directory."""
        root = os.path.join(self.sb.state, "queue", q)
        return {os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs}

    def asking_for_the_store(self, how: str = "exclusive") -> str:
        """What another process is told when it asks for the store lock without waiting."""
        r = subprocess.run([sys.executable, "-c", ASK_FOR_THE_LOCK, jobstore._store_lock(), how], cwd=ROOT,
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(r.stderr, "")
        return r.stdout.strip()

    def started_earlier(self, jid, seconds: float):
        """Move job jid's start time that far back, which is all the clock does to a running job. A pool's
        `settle` and a job's timeout are counted from that time, so a test says which side of the window it
        wants instead of waiting for the window to pass."""
        with jobstore.txn() as s:
            cur = s.job(jid, must_be=("running",))
            self.assertIsNotNone(cur, f"job {jid} is not running")
            cur["started"] -= seconds
            s.save(cur)

    def run_until(self, w, cond, timeout=10.0, what="what the ticks of this test's worker lead to"):
        """Tick worker w (or each of several) until cond holds: how a test drives a queue with no worker
        process of its own. A failure names the state every job was left in."""
        ws = w if isinstance(w, (list, tuple)) else [w]

        def ticked():
            for x in ws:
                x.tick()
            return cond()
        try:
            return journey.until(ticked, timeout=timeout, every=0.03, what=what)
        except AssertionError as e:
            self.fail(f"{e}; states: "
                      + json.dumps({st: [j["id"] for j in jobstore.jobs(st)] for st in jobstore.STATES}))


class TestStore(QueueCase):
    def test_ids_ranges_tags(self):
        a = self.add("--tag", "a")
        b = self.add("--tag", "b")
        c = self.add("--tag", "b")
        self.assertEqual((a, b, c), (1, 2, 3))
        self.assertEqual([j["id"] for j in jobq._resolve(["1-2"])], [1, 2])
        self.assertEqual([j["id"] for j in jobq._resolve(["b"])], [2, 3])
        self.assertEqual([j["id"] for j in jobq._resolve(["3", "1-3"])], [3, 1, 2])
        with self.assertRaises(SystemExit) as cm, redirect_stderr(io.StringIO()):
            jobq._resolve(["9"])
        self.assertEqual(cm.exception.code, 2)

    def test_global_ids_across_queues(self):
        a = self.add("--tag", "same", q="alpha")
        b = self.add("--tag", "same", q="beta")
        c = self.add("--tag", "solo", q="beta")
        self.assertEqual((a, b, c), (1, 2, 3))
        self.assertEqual((jobstore.find(a)["queue"], jobstore.find(b)["queue"]), ("alpha", "beta"))
        self.assertEqual([j["id"] for j in jobq._resolve(["1-3"])], [1, 2, 3])
        self.assertEqual(jobq._resolve(["solo"])[0]["id"], c)
        with self.assertRaises(SystemExit) as cm, redirect_stderr(io.StringIO()) as err:
            jobq._resolve(["same"])
        self.assertEqual(cm.exception.code, 2)
        self.assertIn("alpha, beta", err.getvalue())
        self.assertEqual(jobstore.queues(), ["alpha", "beta"])

    def test_a_state_change_is_one_replace_of_one_file(self):
        """A job's state is a field of its own file, so every change of it replaces that one file once: a
        reader sees the job as it was before the change or as it is after it, never in two states and never in
        none, and a change cut in half leaves one of the two. A new job arrives the same way, as one link of a
        record that is already whole."""
        moves: list = []
        real, real_link = os.replace, os.link

        def spy(src, dst, *a, **k):
            if not _is_job_file(dst):
                return real(src, dst, *a, **k)
            was = _state_in(dst)
            out = real(src, dst, *a, **k)
            moves.append((was, _state_in(dst)))
            return out

        def spy_link(src, dst, *a, **k):
            out = real_link(src, dst, *a, **k)      # it fails on a name that is there, so there was none
            moves.append((None, _state_in(dst)))
            return out

        def changes(step) -> list:
            """The state changes one step made, with one entry per step that changed a job's state."""
            moves.clear()
            with mock.patch.object(jobq.paths.os, "replace", spy), mock.patch.object(jobq.os, "link", spy_link):
                step()
            return [m for m in moves if m[0] != m[1]]

        ids: list = []
        self.assertEqual(changes(lambda: ids.append(self.add("--hold", "--retries", "1", "--retry-delay", "0.05",
                                                             cmd="exit 1"))), [(None, "held")])
        j = ids[0]
        self.assertEqual(changes(lambda: self.call("release", "--no-start", str(j))), [("held", "queued")])
        self.assertEqual(changes(lambda: self.call("hold", str(j))), [("queued", "held")])
        self.call("release", "--no-start", str(j))
        w = jobq.worker_for("default")
        self.assertEqual(changes(w.tick), [("queued", "running")])       # claimed, then its pid amended in place
        self.assertEqual(changes(lambda: self.run_until(w, lambda: self.state(j) == "queued")),
                         [("running", "queued")])                       # the retry keeps the id
        self.assertEqual(changes(lambda: self.run_until(w, lambda: self.state(j) == "failed")),
                         [("queued", "running"), ("running", "failed")])

    def test_an_id_and_the_record_that_holds_it_land_together(self):
        """The record is written whole under a name of the writer's own and then linked to jobs/<id>.json, so
        an add stopped in between leaves no job file at all, never an empty one, and takes no id."""
        real = jobq.paths.write_json

        def stop_after_staging(path, data, **kw):
            real(path, data, **kw)
            if os.path.basename(str(path)).startswith(".staging-job-"):
                raise KeyboardInterrupt     # a Ctrl-C between the record and the name that takes its id
        jobs = os.path.join(self.sb.state, "queue", "default", "jobs")
        os.makedirs(jobs)
        with mock.patch.object(jobq.paths, "write_json", stop_after_staging), self.assertRaises(KeyboardInterrupt):
            self.add()
        self.assertEqual(os.listdir(jobs), [])          # no record, no leftover, and nothing holding id 1
        # what a write killed halfway through leaves is the name write_json renames from: no job, to a reader
        open(os.path.join(jobs, "1.json.deadbeef.tmp"), "w").close()
        with jobstore.listing() as s:
            self.assertEqual(s.jobs(), [])
        self.assertEqual(self.add(), 1)

    def test_a_queue_that_holds_no_record_is_opened_and_says_its_format(self):
        """A record is jobs/<id>.json. A run's claim is not one, so a queue whose records have all been
        cleaned holds nothing this pp cannot read: it is opened like any other, and the first writer to reach
        it says what format its store is in."""
        jobq.lock.Claim.take(jobstore.run_claim("stray", 1)).release()     # what a run of a cleaned job left
        self.assertIsNone(jobstore._format_of("stray"))
        self.assertEqual(self.call("status", "-q", "stray")[0], 0)
        self.assertEqual(self.state(self.add(q="stray")), "queued")
        self.assertEqual(jobstore._format_of("stray"), jobstore.FORMAT)
        jobstore.writing_queue("fresh")                 # a writer that stores no job says the format too
        self.assertEqual(jobstore._format_of("fresh"), jobstore.FORMAT)

    def test_a_queue_whose_store_another_pp_wrote_is_refused(self):
        """A queue says what its store looks like in its FORMAT file, written by the first writer that
        reaches it. A queue whose jobs carry another number, or no number at all, holds records this pp cannot
        read: every path that opens a queue names it and refuses, rather than reporting it as empty and
        launching jobs beside ones that are still running."""
        self.config("slots = 3", table="idle_exit = 0\npoll = 0.01")
        jid = self.add(cmd="true")              # a queue with a job in it, and the format of its store
        fmt = os.path.join(self.sb.state, "queue", "default", "FORMAT")
        self.assertEqual(text(fmt).strip(), str(jobstore.FORMAT))
        self.addCleanup(jobq.paths.write_json, fmt, jobstore.FORMAT)     # before this case's own cleanup reads it
        opens = (["status"], ["status", "-q", "default"], ["add", "--no-start", "--cwd", self.work, "--", "true"],
                 ["wait", "--idle"], ["kill", str(jid)], ["show", str(jid)], ["clean", "-y"],
                 ["worker", "start", "-q", "default"], ["worker", "status", "-q", "default"],
                 ["work", "-q", "default"])
        for how in ("no FORMAT file", "another number"):
            if how == "no FORMAT file":
                os.remove(fmt)
            else:
                jobq.paths.write_json(fmt, jobstore.FORMAT + 1)
            for argv in opens:
                rc, _, err = self.call(*argv)
                self.assertEqual(rc, 2, (how, argv, err))
                self.assertIn("queue 'default' holds jobs this pp cannot read", err)
                self.assertIn("Drain that queue with the pp that wrote it", err)
        jobq.paths.write_json(fmt, jobstore.FORMAT)
        self.assertEqual(self.state(jid), "queued")     # nothing launched it, and nothing reported it
        # a store laid out another way, a directory per state under jobs/: it has no FORMAT file either
        old = os.path.join(self.sb.state, "queue", "old", "jobs", "running")
        self.addCleanup(shutil.rmtree, os.path.dirname(os.path.dirname(old)), ignore_errors=True)
        os.makedirs(old)
        with open(os.path.join(old, "7.json"), "w") as f:
            json.dump({"queue": "old", "state": "running", "cmd": ["sleep", "30"]}, f)
        rc, _, err = self.call("status", "-q", "old")
        self.assertEqual(rc, 2, err)
        self.assertIn("queue 'old' holds jobs this pp cannot read", err)

    def test_a_record_from_outside_the_transaction_is_not_written_back(self):
        """What a copy read outside the lock holds may be older than the store, so saving it would undo what
        another actor recorded meanwhile. Only the record the scope handed out goes back."""
        jid = self.add()
        outside = jobstore.find(jid)                        # read with no lock
        with jobstore.txn() as s:
            with self.assertRaises(RuntimeError):
                s.save(outside)
            with self.assertRaises(RuntimeError):
                s.save(dict(s.job(jid)))                # a copy of this scope's own record is a copy too
            s.job(jid)["prio"] = 3
            s.save(s.job(jid))                          # the record itself
        self.assertEqual(jobstore.find(jid)["prio"], 3)
        with jobstore.listing() as s:
            with self.assertRaises(RuntimeError):
                s.save(s.job(jid))                      # a listing writes nothing

    def test_a_listing_sees_each_job_once_and_whole_and_keeps_a_writer_out(self):
        """POSIX does not say what a directory read sees while entries in it are replaced, and a file system
        may report one entry twice or skip it, so a listing holds the store lock shared."""
        self.config("slots = 1")
        w = jobq.worker_for("default")
        done = self.add(cmd="true")
        self.run_until(w, lambda: self.state(done) == "done")
        failed = self.add(cmd="exit 1")
        self.run_until(w, lambda: self.state(failed) == "failed")
        running = self.add(cmd="sleep 30")
        w.tick()
        queued = self.add(cmd="true")
        held = self.add("--hold", cmd="true")
        want = {done: "done", failed: "failed", running: "running", queued: "queued", held: "held"}
        with jobstore.listing() as s:
            seen = s.jobs()
            self.assertEqual(sorted(j["id"] for j in seen), sorted(want))        # once each
            self.assertEqual({j["id"]: j["state"] for j in seen}, want)
            for j in seen:                                                       # whole: the fields of its state
                self.assertEqual(j["id"] in (done, failed), "rc" in j and "ended" in j)
                self.assertEqual(j["id"] in (queued, held), "pid" not in j)
            # the two halves of one listing that `wait --mine` takes name a job once between them
            live, ended = s.jobs(jobstore.LIVE), s.jobs(jobstore.ENDED)
            self.assertEqual(len({j["id"] for j in live + ended}), len(live) + len(ended))
            self.assertEqual(self.asking_for_the_store(), "busy")                # a writer waits for it
            self.assertEqual(self.asking_for_the_store("shared"), "took it")     # another reader does not

    def test_status_counts_and_rows_come_from_one_listing(self):
        """status reads the store once, so it cannot show one job as both running and done, nor count more
        jobs than it lists."""
        self.config("slots = 1")
        w = jobq.worker_for("default")
        done = self.add(cmd="true")
        self.run_until(w, lambda: self.state(done) == "done")
        self.add(cmd="sleep 30")
        w.tick()
        self.add(cmd="true")
        self.add("--hold", cmd="true")
        self.add(cmd="true", q="other")
        real, passes = os.listdir, []

        def spy(path):
            if os.path.basename(str(path)) == "jobs":
                passes.append(str(path))
            return real(path)
        with mock.patch.object(jobq.os, "listdir", spy):
            data = json.loads(self.call("status", "--json")[1])
        self.assertEqual(sorted(passes), sorted(set(passes)))       # one pass over each queue's directory
        self.assertEqual(len(passes), 2)
        sections = {st: {j["id"] for j in data[st]} for st in ("running", "queued", "held", "recent")}
        self.assertEqual(sum(len(ids) for ids in sections.values()), len(set().union(*sections.values())))
        rows = {st: [j for j in data["recent"] if j["state"] == st] for st in ("done", "failed")}
        rows.update(queued=data["queued"], held=data["held"])
        out = self.call("status")[1]
        head = next(ln for ln in out.splitlines() if ln.startswith("== default"))
        for st, js in rows.items():
            self.assertIn(f"{st} {len([j for j in js if j['queue'] == 'default'])}", head)

    def test_log_follow_misses_no_line_written_as_it_starts(self):
        """log -f prints the tail, then follows: a line written between the two must still be printed, once."""
        jid = self.add()
        j = jobstore.find(jid)
        os.makedirs(os.path.dirname(j["log"]), exist_ok=True)
        with open(j["log"], "w") as f:
            f.write("first\n")
        with jobstore.txn() as s:
            cur = s.job(jid)
            cur.update(rc=0, state="done")
            s.save(cur)
        start = jobq._tail_start

        def late(f, n, floor=0):
            pos = start(f, n, floor)
            with open(j["log"], "a") as g:
                g.write("late\n")
            return pos
        with mock.patch.object(jobq, "_tail_start", late):
            rc, out, err = self.call("log", str(jid), "-f")
        self.assertEqual((rc, out), (0, "first\nlate\n"), err)

    def test_bad_queue_name(self):
        rc, _, err = self.call("add", "-q", "Bad/Name", "--no-start", "--", "true")
        self.assertEqual(rc, 2)
        self.assertIn("not a queue name", err)

    def test_counter_never_reuses_ids_on_disk(self):
        """`counter` says where to start looking for a free id; making the job's file is what takes it, so a
        lost counter costs a few looks and never an id handed out twice."""
        self.add()
        self.add()
        os.remove(os.path.join(self.sb.state, "queue", "counter"))
        self.assertEqual(self.add(), 3)

    def test_refusals(self):
        rc, _, err = self.call("add", "--no-start", "--cwd", self.work, "--need", "foo=1", "--", "true")
        self.assertEqual(rc, 2)
        self.assertIn("[queue.default.resources] foo", err)
        rc, _, err = self.call("add", "--no-start", "--cwd", self.work, "--need", "slots=5", "--", "true")
        self.assertEqual(rc, 2)
        rc, _, err = self.call("add", "--no-start", "--cwd", self.work, "--gpu-gb", "1", "--", "true")
        self.assertEqual(rc, 2)
        self.assertIn("no GPU", err)
        rc, _, _ = self.call("add", "--no-start", "--cwd", self.work)
        self.assertEqual(rc, 2)
        rc, _, err = self.call("add", "--no-start", "--cwd", self.work, "--timeout", "soon", "--", "true")
        self.assertEqual(rc, 2)
        rc, _, err = self.call("add", "--no-start", "--cwd", self.work, "--retry-if", "(", "--", "true")
        self.assertEqual(rc, 2)

    def test_from_file(self):
        f = self.sb.path("cmds.txt")
        with open(f, "w") as fh:
            fh.write("# comment\necho one\n\necho 'two words'\n")
        rc, out, _ = self.call("add", "--no-start", "--cwd", self.work, "--from", f, "--tag", "batch")
        self.assertEqual(rc, 0)
        self.assertEqual(out.split(), ["1", "2"])
        self.assertEqual(jobstore.find(2)["cmd"], ["echo", "two words"])
        self.assertEqual([j["id"] for j in jobq._resolve(["batch"])], [1, 2])

    def plan(self, text: str, name: str = "plan.txt") -> str:
        """A campaign file of this test's, and its path."""
        path = self.sb.path(name)
        with open(path, "w") as fh:
            fh.write(text)
        return path

    def test_a_campaign_files_lines_carry_their_own_flags(self):
        """A line is the rest of an `add` command line: its own flags, then --, then the command. The flags
        are read by add's own parser, so every per-job row works on a line. A flag on the `add` is the
        default for every line and a line's own wins over it, per flag; a flag that carries a table merges
        key by key. A line that is only a command is the whole command, as it always was."""
        f = self.plan("# the lines this campaign covers\n\n"
                      "--tag a --prio 1 --env K=line -- ./one.sh\n"
                      "--timeout 90s -- ./two.sh\n"
                      "./three.sh\n")
        rc, out, err = self.call("add", "--no-start", "--cwd", self.work, "--from", f,
                                 "--prio", "5", "--tag", "fam", "--env", "K=add", "--env", "J=add")
        self.assertEqual(rc, 0, err)
        ids = [int(x) for x in out.split()]
        one, two, three = (jobstore.find(i) for i in ids)
        self.assertEqual((one["tag"], one["prio"], one["env"]), ("a", 1, {"J": "add", "K": "line"}))
        self.assertEqual((one["timeout"], one["cmd"]), (None, ["./one.sh"]))
        self.assertEqual((two["tag"], two["prio"], two["timeout"]), ("fam", 5, 90.0))
        self.assertEqual((two["env"], two["cmd"]), ({"K": "add", "J": "add"}, ["./two.sh"]))
        self.assertEqual((three["tag"], three["prio"], three["cmd"]), ("fam", 5, ["./three.sh"]))

    def test_a_file_one_line_of_which_is_refused_queues_nothing(self):
        """Every refusal of every line names its line number and queues nothing, not even the lines before
        it: a half-queued campaign is the one state a plan script cannot see. The arguments of the `add`
        itself are the batch's and no line's, so a line that names one is refused too."""
        before = sorted(j["id"] for j in jobstore.jobs())
        bad = ("echo a\necho b > out.txt\n",                # a shell operator outside quotes
               "echo a\n--timeout soon -- echo b\n",        # a value its row refuses
               "echo a\n--tag ../x -- echo b\n",            # a name its row refuses
               "echo a\n--bogus 1 -- echo b\n",             # a flag add does not take
               "echo a\n--tag t --\n",                      # flags and no command after the --
               "echo a\n--tag t oops -- echo b\n",          # a word before the -- that is no flag
               "echo a\n-q other -- echo b\n",              # the queue is the add's
               "echo a\n--hold -- echo b\n",                # and so is whether the batch is parked
               "echo a\n--from more.txt -- echo b\n")       # and the file the lines came from
        for text in bad:
            with self.subTest(line=text.splitlines()[-1]):
                rc, out, err = self.call("add", "--no-start", "--cwd", self.work, "--from", self.plan(text))
                self.assertEqual(rc, 2, out)
                self.assertIn("line 2", err)
                self.assertNotIn("Traceback", err)
                self.assertEqual(sorted(j["id"] for j in jobstore.jobs()), before)
        rc, _out, err = self.call("add", "--no-start", "--cwd", self.work, "--from", self.plan("echo a\n"),
                                  "--", "echo", "b")
        self.assertEqual(rc, 2)
        self.assertEqual(sorted(j["id"] for j in jobstore.jobs()), before)

    def test_an_interrupted_batch_names_every_job_it_stored(self):
        """A batch lands in one transaction, and that transaction holds the signals back for its whole length.
        A Ctrl-C in it is raised once it ends, and the ids are on stdout by then: one line per record the store
        holds, so a script reads the id of every job it queued and can name and wait on each one.

        journey.interruptible is what makes the raised SIGINT arrive."""
        f = self.sb.path("cmds.txt")
        with open(f, "w") as fh:
            fh.write("".join(f"echo {n}\n" for n in range(5)))
        real = jobstore.Txn.add

        def add_then_ctrl_c(s, job, state):
            out = real(s, job, state)
            if out["id"] == 2:
                proc.raise_to_self(signal.SIGINT)   # what a terminal sends pp while the batch is landing
            return out

        out, err = io.StringIO(), io.StringIO()
        with journey.interruptible(), redirect_stdout(out), redirect_stderr(err), \
                mock.patch.object(jobstore.Txn, "add", add_then_ctrl_c):
            with self.assertRaises(KeyboardInterrupt):       # which pp exits 130 on (pp/cli.py)
                jobq.run_cli(["add", "--hold", "--cwd", self.work, "--from", f], "queue", "default", "doc")
        stored = sorted(j["id"] for j in jobstore.jobs("held"))
        self.assertEqual(stored, [1, 2, 3, 4, 5])
        self.assertEqual(sorted(int(line) for line in out.getvalue().split()), stored)

    def test_durations(self):
        """One reading of a duration, for every row and every flag that takes one: the spellings it knows,
        the unit a bare number is in, and what it refuses. A value no row can hold is refused where it is
        read, so nothing turns into a wait with no deadline further in."""
        for v, secs in (("90", 90.0), (90, 90.0), ("90s", 90.0), ("30m", 1800.0), ("2h", 7200.0),
                        ("1d", 86400.0), ("1.5h", 5400.0), (".5", 0.5), (" 2h ", 7200.0), ("0", 0.0)):
            with self.subTest(duration=v):
                self.assertEqual(jobpolicy.duration(v), secs)
        self.assertEqual(jobpolicy.duration(7, "d"), 7 * 86400.0)       # a bare number in the row's unit
        self.assertEqual(jobpolicy.duration("1.5d"), 129600.0)
        self.assertIsNone(jobpolicy.duration("none", none_ok=True))     # no bound, where the row offers one
        self.assertIsNone(jobpolicy.duration("NONE", none_ok=True))
        self.assertIsNone(jobpolicy.duration(0, none_ok=True))
        for v in ("", None, "none", "2 weeks", "-5", "lots", True, float("nan"), float("inf"), 1e10, "1e10"):
            with self.subTest(refused=v), self.assertRaises(ValueError):
                jobpolicy.duration(v)

    def test_an_empty_duration_on_a_command_line_is_a_usage_error(self):
        """A config key written as "" is a key that is not set, and the default behind it stands. A command
        line has no such key, so one duration reader refuses an empty value for every duration flag: a shell
        wrote `--timeout "$T"` with T unset, and taking that for "no bound" is a wait with no deadline, a
        job with no timeout, and a `clean` that takes every record there is.

        "no bound" has a spelling where a flag offers one (`add --timeout none`), and leaving the flag out is
        the other, so nothing a user writes on purpose is refused here."""
        ap = jobq._parser("queue", "doc")
        for argv in (["wait", "1", "--timeout", ""], ["watch", "1", "--timeout", ""]):
            with redirect_stderr(io.StringIO()) as err, self.assertRaises(SystemExit, msg=argv) as exit_:
                ap.parse_args(argv)
            self.assertEqual(exit_.exception.code, 2, argv)
            self.assertIn("--timeout", err.getvalue())      # the flag it is about, and how to write one
            self.assertIn("say how long", err.getvalue())
        ended = self.add(cmd="true")
        self.run_until(jobq.worker_for("default"), lambda: self.state(ended) in jobstore.ENDED)
        for argv in (["add", "--no-start", "--cwd", self.work, "--timeout", "", "--", "true"],
                     ["add", "--no-start", "--cwd", self.work, "--retry-delay", "", "--", "true"],
                     ["clean", "--older", "", "-n"]):
            rc, out, err = self.call(*argv)
            self.assertEqual(rc, 2, (argv, out, err))
            self.assertIn("say how long", err)
            self.assertNotIn("Traceback", err)
        self.assertEqual(self.state(ended), "done")         # nothing was queued, and no record was taken
        # the spellings that do mean "no bound", and the flag left out
        self.assertIsNone(jobstore.find(self.add("--timeout", "none"))["timeout"])
        self.assertIsNone(jobstore.find(self.add("--timeout", "0"))["timeout"])
        self.assertEqual(self.call("clean", "-n")[0], 0)

    def test_config_defaults_apply(self):
        self.config("slots = 3", 'timeout = "2h"\nretries = 2\nretry_delay = "1m"\nretry_if = "OOM"\non_done = "true"')
        j = jobstore.find(self.add())
        self.assertEqual((j["timeout"], j["retries"], j["retry_delay"], j["retry_if"], j["on_done"]),
                         (7200, 2, 60, "OOM", "true"))
        j = jobstore.find(self.add("--timeout", "5m", "--retries", "0", "--on-done", "none"))
        self.assertEqual((j["timeout"], j["retries"], j["on_done"]), (300, 0, None))


class TestWorker(QueueCase):
    def test_runs_and_records_rc(self):
        ok = self.add(cmd="echo hello")
        bad = self.add(cmd="exit 3")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(ok) == "done" and self.state(bad) == "failed")
        self.assertEqual(jobstore.find(bad)["rc"], 3)
        with open(jobstore.find(ok)["log"]) as f:
            log = f.read()
        self.assertIn("hello", log)
        self.assertIn("[pp queue]", log)

    def test_slots_serialize(self):
        self.config("slots = 1")
        a = self.add(cmd="sleep 0.3")
        b = self.add(cmd="sleep 0.1")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.run_until(w, lambda: self.state(b) == "done")
        self.assertGreaterEqual(jobstore.find(b)["started"], jobstore.find(a)["ended"])

    def test_custom_counter_head_of_line_and_backfill(self):
        self.config("slots = 5\nquota = 1")
        a = self.add("--need", "quota=1", cmd="sleep 2")
        b = self.add("--need", "quota=1", cmd="sleep 2")
        c = self.add(cmd="sleep 2")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual([self.state(x) for x in (a, b, c)], ["running", "queued", "queued"])   # strict: c waits behind b
        self.assertEqual(w.waiting["id"], b)
        w.backfill = True
        w.tick()
        self.assertEqual([self.state(x) for x in (a, b, c)], ["running", "queued", "running"])

    def test_a_launch_pass_reads_the_store_once(self):
        """One transaction admits and claims everything a pass starts: the store is read once over, however
        many jobs the pass looks at, and every one of them is admitted against the same running jobs."""
        self.config("slots = 5")
        ids = [self.add(cmd="sleep 30") for _ in range(4)]
        real, passes = os.listdir, []

        def spy(path):
            if os.path.basename(str(path)) == "jobs":
                passes.append(str(path))
            return real(path)
        with mock.patch.object(jobq.os, "listdir", spy):
            jobq.worker_for("default").launch()
        self.assertEqual([self.state(i) for i in ids], ["running"] * 4)
        self.assertEqual(len(passes), 2)        # the queued jobs, then the transaction that claims them

    def test_config_change_takes_effect_without_restart(self):
        self.config("slots = 1")
        a = self.add(cmd="sleep 2")
        b = self.add(cmd="sleep 2")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual(self.state(b), "queued")
        self.config("slots = 2")
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "running"))

    def test_hold_release(self):
        """A held job is one the worker leaves alone. A job is launched inside a pass, so two passes that leave
        it held are two chances it did not take. A launch writes the job's log first, so a job with no log has
        not run."""
        h = self.add("--hold", cmd="true")
        self.assertEqual(self.state(h), "held")
        w = jobq.worker_for("default")
        w.tick()
        w.tick()
        self.assertEqual(self.state(h), "held")
        self.assertFalse(os.path.exists(jobstore.find(h)["log"]))
        self.assertEqual(self.call("release", "--no-start", str(h))[0], 0)
        self.run_until(w, lambda: self.state(h) == "done")

    def test_reprio_order(self):
        self.config("slots = 1")
        ids = [self.add(cmd="sleep 0.1") for _ in range(3)]
        self.assertEqual(self.call("reprio", str(ids[2]), "0")[0], 0)
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual(self.state(ids[2]), "running")
        self.assertEqual(self.state(ids[0]), "queued")

    def test_retry_verb_carries_the_job_over(self):
        j = self.add("--tag", "flaky", "--prio", "4", "--timeout", "1h", "--retries", "0", "--on-fail", "true", cmd="exit 1")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "failed")
        rc, out, _ = self.call("retry", "--no-start", str(j))
        self.assertEqual(rc, 0)
        self.assertEqual(len(out.split()), 1)       # the new id is the only stdout
        new = jobstore.find(int(out.split()[0]))
        self.assertEqual((new["state"], new["cmd"], new["prio"], new["retry_of"], new["tag"], new["timeout"],
                          new["on_fail"], new["attempt"], new["queue"]),
                         ("queued", ["sh", "-c", "exit 1"], 4, j, "flaky", 3600, "true", 1, "default"))

    def test_a_retry_over_a_selection_shows_its_plan_and_asks(self):
        """A selection naming more than one job is shown first and asked about, as a kill and a drop are. -n
        only shows the plan, no terminal to ask at refuses, and -y goes ahead. A job that succeeded has
        ended, so it suits retry and a family's finished runs are re-queued with its failed ones."""
        ok, bad = self.add(cmd="true"), self.add(cmd="exit 1")
        held = self.add("--hold", cmd="true")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(ok) == "done" and self.state(bad) == "failed")
        before = sorted(j["id"] for j in jobstore.jobs())
        rc, out, _ = self.call("retry", f"{ok}-{held}", "-n", "--no-start")
        self.assertEqual(rc, 0)
        self.assertEqual(sorted(j["id"] for j in jobstore.jobs()), before)      # only the plan
        with mock.patch("sys.stdin", io.StringIO()):         # nothing to ask at, so nothing is re-queued
            rc, _out, _err = self.call("retry", f"{ok}-{held}", "--no-start")
        self.assertEqual(rc, 2)
        self.assertEqual(sorted(j["id"] for j in jobstore.jobs()), before)
        rc, _out, err = self.call("retry", f"{ok}-{held}", "-y", "--no-start")
        self.assertEqual(rc, 0, err)
        new = sorted(set(j["id"] for j in jobstore.jobs()) - set(before))
        self.assertEqual([jobstore.find(i)["retry_of"] for i in new], [ok, bad])
        self.assertEqual(self.state(held), "held")      # the one the verb does not suit is left as it is

    def test_several_ids_in_a_state_the_verb_does_not_suit_are_refused(self):
        """A job named by its own id and in a state the verb does not suit is exit 2, whether it was named
        alone or beside another one. A plan over ids none of which suits the verb re-queues, stops and
        removes nothing, so an `&&` chain never reads exit 0 as the verb having done its work. A selection
        that swept those jobs up is exit 0 instead, since a driver that re-runs it must not stop the second
        time. retry, kill and drop answer this through the one construct the three share."""
        a, b = self.add(cmd="true"), self.add(cmd="true")
        before = sorted(j["id"] for j in jobstore.jobs())
        self.assertEqual(self.call("retry", str(a), str(b), "--no-start", "-y")[0], 2)
        self.assertEqual(self.call("kill", str(a), str(b), "-y")[0], 2)
        self.assertEqual(self.call("retry", f"{a}-{b}", "--no-start", "-y")[0], 0)      # swept up, not named
        self.assertEqual(self.call("kill", f"{a}-{b}", "-y")[0], 0)
        self.assertEqual(sorted(j["id"] for j in jobstore.jobs()), before)
        self.assertEqual([self.state(x) for x in (a, b)], ["queued", "queued"])
        self.run_until(jobq.worker_for("default"), lambda: self.state(a) == "done" and self.state(b) == "done")
        self.assertEqual(self.call("drop", str(a), str(b), "-y")[0], 2)         # ended: clean removes them
        self.assertEqual(self.call("drop", f"{a}-{b}", "-y")[0], 0)
        self.assertEqual(sorted(j["id"] for j in jobstore.jobs()), before)

    def test_a_job_already_in_the_state_a_verb_moves_it_to_is_left_as_it_is(self):
        """A held job counts as already held under `hold`, and a queued job as already queued under
        `release`: the state suits the verb, so the job is left as it is and the exit is 0. A driver that
        holds a parked stage again, or releases a stage it released before, is not an error the second time.
        The states these two do refuse, running and ended, are exit 2 as before."""
        j = self.add(cmd="true")
        self.assertEqual((self.call("release", "--no-start", str(j))[0], self.state(j)), (0, "queued"))
        self.assertEqual((self.call("hold", str(j))[0], self.state(j)), (0, "held"))
        rc, _out, err = self.call("hold", str(j))
        self.assertEqual((rc, self.state(j)), (0, "held"), err)
        self.assertEqual((self.call("release", "--no-start", str(j))[0], self.state(j)), (0, "queued"))
        done = self.add(cmd="true")
        self.run_until(jobq.worker_for("default"), lambda: self.state(done) == "done")
        self.assertEqual(self.call("hold", str(done))[0], 2)
        self.assertEqual(self.call("release", "--no-start", str(done))[0], 2)

    def test_a_retry_is_owned_by_the_session_that_asked_for_it(self):
        """The new job is this session's, not the session's that queued the job it repeats, so `wait --mine`
        in the session that asked for the retry hears how it ends."""
        j = self.add(cmd="exit 1")
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "failed")
        self.assertIsNone(jobstore.find(j)["owner"])
        self.sb.as_session("sess-retry")
        rc, out, err = self.call("retry", str(j), "--no-start")
        self.assertEqual(rc, 0, err)
        self.assertEqual(jobstore.find(int(out.split()[0]))["owner"]["session"], "sess-retry")

    def test_drop_and_clean(self):
        q = self.add(cmd="true")
        self.assertEqual(self.call("drop", str(q))[0], 0)
        self.assertIsNone(jobstore.find(q))
        d = self.add(cmd="true")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(d) == "done")
        rc, out, _ = self.call("clean", "-n")
        self.assertIn("1 ended", out)
        self.assertEqual(self.state(d), "done")
        log = jobstore.find(d)["log"]
        self.assertEqual(self.call("clean", "-y")[0], 0)
        self.assertIsNone(jobstore.find(d))
        self.assertFalse(os.path.exists(log))

    def test_drop_and_clean_take_every_file_that_belongs_to_the_job(self):
        """A job waiting out a retry has written an exit code, a log and a run claim, and a job that fired a
        hook has that hook's log. Both `drop` and `clean` take the record with every file that is the job's
        own, so a queue of flaky jobs does not fill up with files nothing names: once the record is gone,
        nothing can name them."""
        before = self.files()
        f = self.sb.path("hook")
        j = self.add("--retries", "1", "--retry-delay", "30", cmd="exit 1")
        done = self.add("--on-done", f"echo x >> {f}", cmd="true")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "queued" and jobstore.find(j)["attempt"] == 2
                       and self.state(done) == "done")
        journey.until(lambda: jobstore.find(done).get("hooks"), timeout=10, what="the hook on the record")
        left = self.files() - before
        self.assertTrue([p for p in left if p.endswith(f"{j}.run")], left)              # its run claim
        self.assertTrue([p for p in left if p.startswith(f"rc{os.sep}")], left)         # attempt 1's exit code
        self.assertTrue([p for p in left if p.startswith(f"logs{os.sep}")], left)       # and its log
        # the environment file that attempt's runner was handed is the attempt's too, and nobody else's to read
        was = jobstore.find(j)["history"][0]["envfile"]
        self.assertEqual(os.stat(was).st_mode & 0o777, 0o600)
        hlog = jobstore.find(done)["hooks"][0]["log"]
        self.assertTrue(os.path.exists(hlog))
        self.assertEqual(self.call("drop", str(j))[0], 0)
        self.assertEqual(self.call("clean", "-y")[0], 0)
        self.assertEqual((jobstore.find(j), jobstore.find(done)), (None, None))
        self.assertEqual({p for p in self.files() - before if p != "FORMAT"}, set())
        self.assertFalse(os.path.exists(hlog))      # the hook's log too: the record was the last to name it
        self.assertFalse(os.path.exists(was))       # and the environment file of an attempt that is over

    def test_adoption_reads_rc_from_file(self):
        j = self.add(cmd="sleep 0.3; exit 4")
        w1 = jobq.worker_for("default")
        w1.tick()
        self.assertEqual(self.state(j), "running")
        del w1                                  # the worker dies; the job does not
        w2 = jobq.worker_for("default")
        self.run_until(w2, lambda: self.state(j) == "failed")
        rec = jobstore.find(j)
        self.assertEqual(rec["rc"], 4)
        self.assertNotIn("reason", rec)

    def test_adoption_without_rc_is_lost(self):
        j = self.add(cmd="sleep 30")
        w1 = jobq.worker_for("default")
        w1.tick()
        proc.signal_group(jobcore._group_handle(jobstore.find(j)), signal.SIGKILL)   # it dies before writing an rc
        w2 = jobq.worker_for("default")
        self.run_until(w2, lambda: self.state(j) == "failed")
        self.assertEqual(jobstore.find(j)["reason"], "lost")

    def test_kill_takes_the_process_group(self):
        """A kill writes a stop on the job; its worker signals the whole group, so nothing the job started
        runs on, and records how it ended, which is what the kill prints."""
        j = self.add(cmd="sleep 30 & sleep 30; wait")
        w = jobq.worker_for("default")
        w.tick()
        pid = jobstore.find(j)["pid"]
        journey.until(lambda: len([p for p in proc.descendants(pid)
                                   if proc.read(p) and proc.read(p).comm == "sleep"]) >= 2,
                      timeout=5, what="the job's children")
        kids = proc.descendants(pid)
        self.worker_beside(w)
        rc, out, _ = self.call("kill", str(j), "--grace", "2")
        self.assertEqual(rc, 0)
        self.assertIn(f"killed {j} (stopped on SIGTERM)", out)
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["reason"]), ("failed", "killed"))
        self.assertFalse(any(proc.alive(k) for k in kids))
        self.assertNotIn(j, w.children)          # the worker reaped its zombie too
        shown = self.call("show", str(j))[1]
        self.assertIn("user asked at", shown)    # who asked for the stop, and what went out
        self.assertIn("SIGTERM sent", shown)

    def test_a_job_whose_record_has_no_start_time_is_stopped_through_its_claim(self):
        """A group id with no start time beside it names whatever holds that id now, so pp will not signal it.
        The job's claim names its wrapper, which leads that group and holds the claim, so the worker reaches
        the group without pp ever signalling a bare id."""
        j = self.add(cmd="sleep 30 & sleep 30; wait")
        w = jobq.worker_for("default")
        w.tick()
        pid = jobstore.find(j)["pid"]
        journey.until(lambda: len(proc.descendants(pid)) >= 2, timeout=10, what="the job's children")
        kids = proc.descendants(pid)
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur.pop("pstart")
            s.save(cur)
        self.worker_beside(w)
        rc, out, err = self.call("kill", str(j), "--grace", "2")
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"killed {j}", out)
        self.assertEqual((self.state(j), jobstore.find(j)["reason"]), ("failed", "killed"))
        self.assertFalse(any(proc.alive(k) for k in kids))

    def test_a_kill_clears_the_group_of_a_command_that_holds_the_signal_back(self):
        """The wrapper stops on the SIGTERM, so the run is over and the job is recorded as killed. Anything
        left in its group goes with it, as it does after a timeout, so a command that ignores the signal does
        not outlive the job."""
        j = self.add(cmd="trap '' TERM; while :; do sleep 0.05; done")
        w = jobq.worker_for("default")
        w.tick()
        pid = jobstore.find(j)["pid"]
        journey.until(lambda: proc.descendants(pid), timeout=5, what="the job's command")
        kids = proc.descendants(pid)
        self.worker_beside(w)
        rc, out, _ = self.call("kill", str(j), "--grace", "0.2")
        self.assertEqual(rc, 0)
        self.assertIn(f"killed {j}", out)
        self.assertEqual(jobstore.find(j)["reason"], "killed")
        journey.until(lambda: not any(proc.alive(k) for k in kids), timeout=5, what="its group to be clear")

    def test_a_kill_whose_stop_command_ended_the_job_reports_that_stop(self):
        """A kill reports the outcome recorded for the job. The project's stop command ends what the runner
        started, and the attempt writes the code of the work that stop ended, so the code is kept as `rc`
        and the stop is the reason: the job did not end on its own, and the kill says the stop it achieved."""
        gate = os.path.join(self.work, "the-stop-command-ran")
        j = self.add(cmd=f"while [ ! -f {gate} ]; do sleep 0.02; done; exit 137")
        w = jobq.worker_for("default")
        w.tick()
        with jobstore.txn() as s:       # the project's command, which ends the work and leaves the code 137
            cur = s.job(j, must_be=("running",))
            # it asks the work to end a beat later, as a command that waits for a container to stop does, so
            # the code the attempt writes lands after the stop went out by more than a file time can differ by
            cur["stop_cmd"] = f"sleep 0.2; : > {gate}"
            s.save(cur)
        self.worker_beside(w)
        rc, out, err = self.call("kill", str(j), "--grace", "60")
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["reason"], rec["rc"]), ("failed", "killed", 137))
        self.assertIsNotNone(rec["stop"]["sent"])
        self.assertIsNone(rec["stop"].get("kill9"))         # it ended inside its grace, on that stop
        self.assertEqual((rc, out.split()[:2]), (0, ["killed", str(j)]), err)

    def test_a_run_is_live_until_its_worker_reaps_it(self):
        """A dying process closes its files, the run claim among them, before its parent can read how it
        ended. So a tick can find the claim free and the wrapper not yet reapable: the run is still live then,
        and its end is read once the worker reaps it, never guessed as lost."""
        j = self.add(cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        rec = jobstore.find(j)
        journey.until(lambda: proc.descendants(rec["pid"]), timeout=5, what="the job's command")
        for k in proc.descendants(rec["pid"]):
            self.addCleanup(journey.kill_pid, k)
        real = jobq.lock.Claim.holder.__func__

        def holder(cls, path):          # the wrapper's claim as the kernel shows it once its files are closed
            r = real(cls, path)
            return None if r and r.get("pid") == rec["pid"] else r

        with mock.patch.object(jobq.lock.Claim, "holder", classmethod(holder)):
            w.tick()
        self.assertEqual(self.state(j), "running")
        wrapper = proc.Handle.recorded(rec["pid"], rec["pstart"])
        self.assertEqual(proc.signal_pid(wrapper, signal.SIGKILL), "alive")
        self.run_until(w, lambda: self.state(j) == "failed")
        self.assertEqual(jobstore.find(j)["reason"], "signal 9")

    def test_kill_over_a_range_asks_first_then_stops_each(self):
        """A range is a selection: it is shown first, -n only shows, -y goes ahead, and every job it named
        that is running is stopped and reported."""
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        held = self.add("--hold", cmd="true")
        w = jobq.worker_for("default")
        w.tick()
        self.worker_beside(w)
        rc, out, _ = self.call("kill", f"{a}-{held}", "-n")
        self.assertEqual(rc, 0)
        self.assertIn(f"kill 2 job(s): {a}", out)
        self.assertIn(f"left as they are: {held} held", out)
        self.assertEqual([self.state(x) for x in (a, b)], ["running", "running"])
        rc, out, _ = self.call("kill", f"{a}-{held}", "-y")
        self.assertEqual(rc, 0)
        self.assertIn(f"killed {a} (stopped on SIGTERM)", out)
        self.assertIn(f"killed {b} (stopped on SIGTERM)", out)
        self.assertEqual([jobstore.find(x)["reason"] for x in (a, b)], ["killed", "killed"])
        self.assertEqual(self.state(held), "held")

    def test_a_second_kill_hastens_the_signal_and_keeps_what_went_out(self):
        """A job whose SIGTERM is out with five minutes of grace before the SIGKILL: a second kill with a
        short grace SIGKILLs it now. The record still says when the SIGTERM went out, so the job is not
        signalled from the start again and the grace does not begin a second time.

        One stop carries one grace, the shorter of every ask, and that is the one the SIGKILL goes out after.
        It is also the one `kill` prints, whichever ask it came from: a kill that asks for two minutes inside
        a grace of a fifth of a second does not report a two-minute grace."""
        w = jobq.worker_for("default")
        self.worker_beside(w)       # one worker for the whole test, ticking beside it
        rows = [("a shorter ask hastens the SIGKILL", 300.0, 0.0, "0.2"),
                ("a longer ask leaves the grace the record kept", 0.2, 1.0, "120")]
        for what, grace, ago, asked in rows:
            with self.subTest(what):
                j = self.add(cmd="sleep 30")
                journey.until(lambda: self.state(j) == "running", timeout=10, what="the job to start")
                sent = time.time() - ago
                with jobstore.txn() as s:       # a kill that asked for that grace, its SIGTERM out
                    cur = s.job(j, must_be=("running",))
                    cur["stop"] = {"by": "user", "t": sent, "grace": grace, "sent": sent}
                    s.save(cur)
                rc, out, err = self.call("kill", str(j), "--grace", asked)
                self.assertEqual((rc, err), (0, ""))
                kept = min(grace, float(asked))
                self.assertIn(f"killed {j} (SIGKILL after {kept:g}s)", out)
                st = jobstore.find(j)["stop"]
                self.assertEqual((st["by"], st["t"], st["sent"], st["grace"]), ("user", sent, sent, kept))
                self.assertTrue(st["kill9"])
                self.assertEqual(jobstore.find(j)["reason"], "killed")

    def test_a_stop_pp_may_not_signal_says_so_where_a_person_looks(self):
        """A stop pp was refused is not in flight: nothing more goes out for it. `show` says that, and the
        field is in the list JSON consumers read, so a second reader does not wait for a stop that is never
        carried out."""
        j = self.add(cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        with mock.patch.object(jobq.proc, "signal_group", lambda *a, **k: "denied"):
            w.carry_out()                       # nothing in flight yet
            with jobstore.txn() as s:
                cur = s.job(j, must_be=("running",))
                cur["stop"] = {"by": "user", "t": time.time(), "grace": 10.0}
                s.save(cur)
            w.carry_out()
        st = jobstore.find(j)["stop"]
        self.assertTrue(st["denied"])
        self.assertIsNone(st.get("sent"))
        shown = self.call("show", str(j))[1]
        self.assertIn("user asked at", shown)
        self.assertIn("nothing signalled", shown)
        self.assertIn("pp is not allowed to signal its group", shown)
        self.assertIn("so nothing more goes out", shown)
        self.assertIn("denied", jobq.public.__doc__)    # and it is named where the stop's fields are
        self.assertEqual(json.loads(self.call("show", str(j), "--json")[1])["stop"]["denied"], st["denied"])

    def test_kill_queued_is_refused(self):
        q = self.add(cmd="true")
        rc, _, err = self.call("kill", str(q))
        self.assertEqual(rc, 2)
        self.assertIn("drop", err)

    def test_a_kill_of_a_job_back_in_its_queue_ends_it_with_no_worker(self):
        """A job the kill named as running may be back in its queue for a retry by the time the kill writes
        its stop. No process exists then, so the transaction ends it, and no worker is asked for."""
        j = self.add("--retries", "1", "--retry-delay", "30", cmd="exit 1")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "queued" and jobstore.find(j)["attempt"] == 2)
        as_running = [{**jobstore.find(j), "state": "running", "_bulk": False}]      # as a kill read it before
        with mock.patch.object(jobq, "_resolve", lambda *a, **k: as_running):
            rc, out, err = self.call("kill", str(j))
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"killed {j} (it was queued, with no process to stop)", out)
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["reason"], rec["attempt"]), ("failed", "killed", 2))
        self.assertIsNone(jobcore.worker("default"))       # nothing was asked of a worker, and none was started

    def test_a_run_claim_the_launch_took_is_never_left_held(self):
        """A launch takes each admitted job's run claim inside its transaction and hands it to the spawn after
        it. However that pass ends, pp's own copy of every claim it took is let go: a job whose spawn was
        never reached then reads as a run that is over, and the next pass records it, instead of a record that
        says running with a claim nothing will ever free."""
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        w = jobq.worker_for("default")
        real, seen = jobcore._new_rcfile, []

        def new_rcfile(j):          # the second job of the pass: the rest of its transaction cannot be done
            seen.append(j["id"])
            if len(seen) == 2:
                raise OSError(28, "No space left on device")
            return real(j)
        with mock.patch.object(jobcore, "_new_rcfile", new_rcfile), self.assertRaises(OSError):
            w.tick()
        self.assertEqual(seen, [a, b])
        for x in (a, b):
            self.assertIsNone(jobq.lock.Claim.holder(jobstore.run_claim("default", x)),
                              f"job {x} keeps a run claim, so nothing ever reaps it")
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        w.tick()                    # the next pass: a's run is over and is recorded, and b starts
        self.assertEqual((self.state(a), jobstore.find(a).get("reason")), ("failed", "lost"))
        self.run_until(w, lambda: self.state(b) == "running")

    def test_launch_failure(self):
        j = self.add(cmd="true")
        shutil.rmtree(self.work)
        w = jobq.worker_for("default")
        w.tick()
        rec = jobstore.find(j)
        self.assertEqual(rec["state"], "failed")
        self.assertIn("launch failed", rec["reason"])

    def test_a_launch_that_cannot_happen_is_one_jobs_failure_and_not_the_passs(self):
        """Whatever stopped a start is recorded on the job it belongs to, and the pass goes on launching the
        rest. A pass has marked every job it admitted `running` before it starts any of them, so a job that
        takes the pass down with it leaves the others with no process, and the next tick reads them as lost:
        jobs of other projects, which asked for none of it, end FAILED without ever running.

        An environment name holding a `=` is such a start: no execve takes it, and a project's [queue] env
        can hold one."""
        bad = self.add("--prio", "1", cmd="echo bad")
        good = self.add("--prio", "2", cmd="echo good")
        with jobstore.txn() as s:
            cur = s.job(bad)
            cur["env"] = {"PP_IS=NO_NAME": "1"}         # what no execve takes, so this job can never start
            s.save(cur)
        w = jobq.worker_for("default")
        w.tick()
        rec = jobstore.find(bad)
        self.assertEqual(rec["state"], "failed", rec)
        self.assertIn("launch failed", rec["reason"])
        self.assertEqual(self.state(good), "running")   # the pass went on, so this one has its process
        self.run_until(w, lambda: self.state(good) == "done")


class TestNamedQueues(QueueCase):
    def test_independent_workers_and_head_of_line(self):
        self.config("slots = 1", q="alpha")
        self.config("slots = 1", q="beta")
        a1 = self.add(cmd="sleep 2", q="alpha")
        a2 = self.add(cmd="true", q="alpha")
        b1 = self.add(cmd="true", q="beta")
        wa, wb = jobq.worker_for("alpha"), jobq.worker_for("beta")
        wa.tick()
        self.assertEqual((self.state(a1), self.state(a2), self.state(b1)), ("running", "queued", "queued"))
        self.run_until(wb, lambda: self.state(b1) == "done")       # alpha's blocked head never holds up beta
        self.assertEqual(self.state(a2), "queued")
        wa.tick()
        self.assertEqual(self.state(a2), "queued")                 # a worker only runs its own queue

    def test_status_sections_and_scope(self):
        self.add(cmd="true", q="alpha")
        self.add(cmd="true", q="beta")
        rc, out, _ = self.call()
        self.assertEqual(rc, 0)
        self.assertIn("== alpha", out)
        self.assertIn("== beta", out)
        rc, out, _ = self.call("-q", "beta")
        self.assertNotIn("== alpha", out)
        self.assertIn("== beta", out)
        data = json.loads(self.call("status", "--json")[1])
        self.assertEqual(sorted(j["queue"] for j in data["queued"]), ["alpha", "beta"])
        # the two that hold records, and `default`, which this scope's config names and nothing has run in
        self.assertEqual(data["queues"], ["alpha", "beta", "default"])

    def test_a_queue_the_config_names_is_a_section_with_nothing_in_it(self):
        """A lane the machine declares is in the view before anything is queued in it, so a reader sees the
        lanes there are instead of having to be told their names. Reading one creates nothing: the store
        holds the queues that exist, and a name in a table is not one of them."""
        self.config("slots = 1", q="lane")
        rc, out, _ = self.call()
        self.assertEqual(rc, 0)
        self.assertIn("== lane", out)
        data = json.loads(self.call("status", "--json")[1])
        self.assertIn("lane", data["queues"])
        self.assertIsNone(data["workers"]["lane"])
        self.assertIsNone(data["waiting"]["lane"])
        self.assertNotIn("lane", data["starting"])
        self.assertEqual([p["name"] for p in data["pools"]["lane"]], ["slots"])
        self.assertEqual(jobstore.queues(), [])
        self.assertFalse(os.path.exists(os.path.join(self.sb.state, "queue", "lane")),
                         "reading the status of a queue a table names created it")

    def test_a_key_of_the_queue_table_that_is_no_table_names_no_queue(self):
        """`[queue] retries = 2` is the .pp.toml spelling of a job's setting, written in config.toml by
        mistake. It names no queue, so the view covers none for it: a queue is a [queue.NAME] table."""
        with open(os.path.join(self.cfg, "config.toml"), "w") as f:
            f.write("[queue]\nretries = 2\n")
        rc, _out, _ = self.call()
        data = json.loads(self.call("status", "--json")[1])
        self.assertEqual((rc, data["queues"], data["config_errors"]), (0, [], []))
        self.assertEqual((data["workers"], data["waiting"], data["pools"]), ({}, {}, {}))

    def test_worker_status_lists_every_queue(self):
        self.add(cmd="true", q="alpha")
        self.add(cmd="true", q="beta")
        rc, out, _ = self.call("worker", "status")
        self.assertEqual(rc, 0)               # none up: still a report that worked
        self.assertIn("alpha: worker down", out)
        self.assertIn("beta: worker down", out)

    def test_worker_status_of_a_queue_that_is_not_there_creates_nothing(self):
        """A verb that only reports writes nothing. `worker status -q NAME` for a name that is a typo says
        the worker is down and leaves no directory behind, so the state tree holds the queues that exist."""
        top = os.path.join(self.sb.state, "queue")
        rc, out, _ = self.call("worker", "status", "-q", "typoqueue")
        self.assertEqual((rc, out.split()[:3]), (0, ["typoqueue:", "worker", "down"]))
        self.assertFalse(os.path.exists(os.path.join(top, "typoqueue")),
                         "reading the status of a queue that is not there created it")
        self.assertEqual(jobstore.queues(), [])
        self.assertEqual(json.loads(self.call("worker", "status", "-q", "typoqueue", "--json")[1])[0]["up"], False)
        self.assertFalse(os.path.exists(os.path.join(top, "typoqueue")))


class TestGpu(QueueCase):
    def test_reservation(self):
        jobq._cards = lambda: [card(total=24)]           # budget: 24 - 2 = 22
        a = self.add("--gpu-gb", "12", cmd="sleep 2")
        b = self.add("--gpu-gb", "12", cmd="sleep 2")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("does not fit", self.why(w))
        self.assertEqual(jobstore.find(a)["cards"], [0])

    def test_reservation_counts_every_queue(self):
        jobq._cards = lambda: [card(total=24)]
        self.config("slots = 3", q="alpha")
        self.config("slots = 3", q="beta")
        a = self.add("--gpu-gb", "12", cmd="sleep 2", q="alpha")
        b = self.add("--gpu-gb", "12", cmd="sleep 2", q="beta")
        wa, wb = jobq.worker_for("alpha"), jobq.worker_for("beta")
        wa.tick()
        wb.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("reserved 12", self.why(wb))            # alpha's reservation, seen from beta

    def test_live_free_memory(self):
        cards = [card(total=24, used=19)]                # another tenant holds 19 GB
        jobq._cards = lambda: cards
        j = self.add("--gpu-gb", "10", cmd="true")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual(self.state(j), "queued")
        cards[0] = card(total=24, used=0)
        self.run_until(w, lambda: self.state(j) == "done")

    def test_recent_launches_count_against_free_memory(self):
        self.config("slots = 3", q="alpha")
        self.config("slots = 3", q="beta")
        self.resources('gpu_gb = { max = "auto", settle = "0.5s" }')
        cards = [card(total=24, used=10)]                # another tenant holds 10 GB
        jobq._cards = lambda: cards
        a = self.add("--gpu-gb", "8", cmd="sleep 3", q="alpha")
        b = self.add("--gpu-gb", "8", cmd="sleep 3", q="beta")
        wa, wb = jobq.worker_for("alpha"), jobq.worker_for("beta")
        wa.tick()
        wb.tick()
        # nvidia-smi still says 14 free, but alpha's job has not allocated its 8 yet: 6 left, from beta's view too
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("not allocated yet", self.why(wb))
        self.started_earlier(a, 1)                       # alpha's job was launched before the settle window
        cards[0] = card(total=24, used=18)               # the reading is trusted now: it shows alpha's 8
        wb.tick()
        self.assertEqual(self.state(b), "queued")
        self.assertNotIn("not allocated yet", self.why(wb))
        cards[0] = card(total=24, used=8)                # the other tenant left
        wb.tick()
        self.assertEqual(self.state(b), "running")

    def test_status_publishes_the_gpu_pool_of_every_scope_per_card(self):
        """A planner sizes --gpu-gb against the smallest budget of every scope its queue draws from, so each
        scope's gpu_gb pool is an entry of `status --json` like any other pool, with one field of its own:
        its budget on each card of the one reading `status` took, and what is reserved on that card."""
        jobq._cards = lambda: [card(0, total=24), card(1, total=24)]
        self.resources("gpu_gb = 20")
        self.config("slots = 3\ngpu_gb = 16", q="alpha")
        j = self.add("--gpu-gb", "8", cmd="sleep 30", q="alpha")
        jobq.worker_for("alpha").tick()
        self.assertEqual((self.state(j), jobstore.find(j)["cards"]), ("running", [0]))
        data = json.loads(self.call("status", "--json")[1])
        own = next(p for p in data["pools"]["alpha"] if p["name"] == "gpu_gb")
        self.assertEqual((own["max"], own["used"], own["probe"], own["reading"]), (16, 8, None, None))
        self.assertEqual(own["per_card"], [{"index": 0, "max": 16, "used": 8},
                                           {"index": 1, "max": 16, "used": 0}])
        shared = next(p for p in data["shared"] if p["name"] == "gpu_gb")
        self.assertEqual((shared["max"], shared["used"]), (20, 8))
        self.assertEqual(shared["per_card"], [{"index": 0, "max": 20, "used": 8},
                                              {"index": 1, "max": 20, "used": 0}])
        self.assertEqual(self.call("status")[0], 0)      # and the text view of the same, per card

    def test_the_gpu_pool_of_a_scope_with_no_card_reading_has_no_cards(self):
        """Without a reading there is nothing to say per card, so per_card is empty and the pool's own max is
        what a planner reads. `pp queue` says what there is either way."""
        self.resources("gpu_gb = 20")
        data = json.loads(self.call("status", "--json")[1])
        self.assertIsNone(data["cards"])
        self.assertEqual([p["per_card"] for p in data["shared"] if p["name"] == "gpu_gb"], [[]])
        self.assertEqual(self.call("status")[0], 0)

    def test_picks_the_card_that_fits(self):
        jobq._cards = lambda: [card(0, total=24, used=14), card(1, total=24, used=4)]
        j = self.add("--gpu-gb", "15", cmd='echo "dev=$CUDA_VISIBLE_DEVICES"')
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "done")
        rec = jobstore.find(j)
        self.assertEqual(rec["cards"], [1])
        with open(rec["log"]) as f:
            self.assertIn("dev=1", f.read())


class TestPools(QueueCase):
    def test_shared_pool_counts_every_queue_and_a_queue_can_take_less(self):
        self.resources("cpu = 8")
        self.config("slots = 5\ncpu = 6", q="alpha")
        self.config("slots = 5", q="beta")
        a1 = self.add("--cpu", "4", cmd="sleep 3", q="alpha")
        a2 = self.add("--cpu", "4", cmd="sleep 3", q="alpha")
        b1 = self.add("--cpu", "4", cmd="sleep 3", q="beta")
        b2 = self.add("--cpu", "1", cmd="sleep 3", q="beta")
        wa, wb = jobq.worker_for("alpha"), jobq.worker_for("beta")
        wa.tick()
        self.assertEqual((self.state(a1), self.state(a2)), ("running", "queued"))
        self.assertIn("[queue.alpha.resources] cpu", self.why(wa))          # alpha's own share: 6
        wb.tick()
        self.assertEqual((self.state(b1), self.state(b2)), ("running", "queued"))
        self.assertIn("[resources] cpu", self.why(wb))                      # the machine's 8: 4 alpha + 4 beta

    def test_defaults(self):
        ps = {(p["name"], p["scope"]): p for p in jobq.pools("default")}
        self.assertEqual(ps[("slots", "default")]["max"], 3)
        self.assertEqual(ps[("cpu", None)]["max"], os.cpu_count())
        self.assertIn(("mem_gb", None), ps)
        self.assertIsNone(ps[("gpu_gb", None)]["max"])        # auto: per card
        rc, _, err = self.call("add", "--no-start", "--cpu", str(os.cpu_count() + 1), "--", "true")
        self.assertEqual(rc, 2)
        self.assertIn("[resources] cpu", err)

    def test_false_removes_a_default(self):
        self.resources("cpu = false")
        rc, _, err = self.call("add", "--no-start", "--cpu", "1", "--", "true")
        self.assertEqual(rc, 2)
        self.assertIn("nothing provides 'cpu'", err)

    def test_config_errors(self):
        for text, msg in (("nodes = { maxx = 3 }", "unknown maxx"), ('lic = "auto"', "know their own size"),
                          ("nodes = { every = 3 }", "max and/or probe"),
                          ('nodes = "lots"', "not a number"), ('nodes = { probe = "x", every = "soon" }', "every")):
            self.resources(text)
            with self.assertRaises(config.ConfigError, msg=text) as cm:
                jobq.pools("default")
            self.assertIn(msg, str(cm.exception))

    def test_a_card_probe_belongs_to_the_machine_and_not_to_one_queue(self):
        """One machine has one set of cards, so [resources] gpu_gb takes the probe and a queue's own table
        refuses it."""
        self.resources('gpu_gb = { probe = "x" }')
        self.assertEqual([p["probe"] for p in jobq.pools("default") if p["name"] == "gpu_gb"], ["x"])
        self.resources("")
        self.config('gpu_gb = { probe = "x" }')
        with self.assertRaises(config.ConfigError) as cm:
            jobq.pools("default")
        self.assertIn("[queue.default.resources] gpu_gb", str(cm.exception))

    def test_reserved_queue_names(self):
        rc, _, err = self.call("add", "-q", "probes", "--no-start", "--", "true")
        self.assertEqual(rc, 2)
        self.assertIn("not a queue name", err)

    def test_a_queue_named_like_the_id_counter_is_refused_and_every_other_add_still_works(self):
        """The id counter is a file beside the queues, so a queue of that name would take its place."""
        rc, _, _ = self.call("add", "-q", "counter", "--no-start", "--", "true")
        self.assertEqual(rc, 2)
        self.assertNotIn("counter", jobstore.queues())
        first = self.add(cmd="true")
        self.assertEqual(self.add(cmd="true"), first + 1)


class TestProbe(QueueCase):
    def test_free_amount_minus_what_was_launched_since(self):
        free = self.sb.path("free")
        with open(free, "w") as f:
            f.write("3\n")
        self.resources(f'nodes = {{ probe = "cat {free}", every = "0s", settle = "0.5s" }}')
        a = self.add("--need", "nodes=2", cmd="sleep 5")
        b = self.add("--need", "nodes=2", cmd="sleep 5")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("2 of it launched since", self.why(w))    # the probe said 3, but a took 2 after it
        with open(free, "w") as f:
            f.write("1\n")                                     # the cluster now shows a's 2 nodes as used
        self.started_earlier(a, 1)                             # launched before the settle window
        w.tick()
        self.assertEqual(self.state(b), "queued")
        self.assertIn("probe says 1", self.why(w))
        self.assertNotIn("launched since", self.why(w))
        with open(free, "w") as f:
            f.write("idle nodes: 4\n")                         # the first number counts
        w.tick()
        self.assertEqual(self.state(b), "running")

    def test_a_failing_probe_holds_the_job_and_says_why(self):
        self.resources('nodes = { probe = "echo cluster down >&2; exit 4", every = "0s" }')
        j = self.add("--need", "nodes=1")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual(self.state(j), "queued")
        self.assertIn("probe failed", self.why(w))
        self.assertIn("exit 4: cluster down", self.why(w))
        self.assertIn("probe failed", self.call("status")[1])

    def test_one_run_of_a_probe_at_a_time_and_nobody_waits_for_it(self):
        """Whoever takes the probe's claim runs it. Anyone else uses the reading that is there, and where
        there is none yet is told a probe is being read, not that the probe failed. The user's command is
        never run twice over at once, and nothing waits for a holder."""
        ran = self.sb.path("ran")
        self.resources(f'nodes = {{ probe = "echo x >> {ran}; echo 1", every = "0s" }}')
        p = next(x for x in jobq.pools("default") if x["name"] == "nodes")
        with jobq.lock.Claim.take(jobq._probe_claim(p)):
            t0 = time.time()
            self.assertEqual(jobq.reading(p), {"t": mock.ANY, "value": None, "running": True})
            self.assertLess(time.time() - t0, 5)        # it did not wait for the holder
            self.assertFalse(os.path.exists(ran))       # and it did not run the user's command
            j = self.add("--need", "nodes=1", cmd="true")
            w = jobq.worker_for("default")
            w.tick()
            self.assertEqual(self.state(j), "queued")
            self.assertIn("its probe is being read", self.why(w))
            self.assertNotIn("failed", self.why(w))
            self.assertNotIn("probe failed", self.call("status")[1])
        r = jobq.reading(p)                             # the claim is free now, so this caller runs it
        self.assertEqual(r["value"], 1)
        with open(ran) as f:
            self.assertEqual(f.read().count("x"), 1)
        with jobq.lock.Claim.take(jobq._probe_claim(p)):
            self.assertEqual(jobq.reading(p)["value"], 1)       # the reading that is there, not a second run
        with open(ran) as f:
            self.assertEqual(f.read().count("x"), 1)

    def test_readings_are_cached_for_every(self):
        calls = self.sb.path("calls")
        self.resources(f'nodes = {{ probe = "echo x >> {calls}; echo 5", every = "1h" }}')
        self.add("--need", "nodes=9")                           # never fits: every tick asks for a reading
        w = jobq.worker_for("default")
        w.tick()
        w.tick()
        with open(calls) as f:
            self.assertEqual(len(f.read().split()), 1)

    def test_status_shows_shared_pools(self):
        self.resources('license = 2\nnodes = { probe = "echo 7", every = "0s" }')
        self.add("--need", "license=1", "--need", "nodes=1", cmd="sleep 3")
        w = jobq.worker_for("default")
        w.tick()
        out = self.call("status")[1]
        self.assertIn("shared: ", out)
        self.assertIn("license 1/2", out)
        self.assertIn("nodes 1 in use (probe: 7 free", out)
        data = json.loads(self.call("status", "--json")[1])
        lic = next(p for p in data["shared"] if p["name"] == "license")
        self.assertEqual((lic["used"], lic["max"]), (1, 2))
        self.assertEqual(data["pools"]["default"][0]["name"], "slots")


class TestTheCardReading(QueueCase):
    """The machine's cards: [resources] gpu_gb's probe where it gives one, and nvidia-smi where it does not.
    This scope's own card reader sees no card, so a card here is one a reader of the test's prints."""

    def gpu_pool(self) -> dict:
        return next(p for p in jobq.pools("default") if p["name"] == "gpu_gb")

    def test_a_reading_older_than_every_is_taken_again_before_an_add_is_refused(self):
        """Whoever finds the cached reading older than `every` runs the probe, an add among them, so a job is
        not refused for a failure the probe no longer gives."""
        up = self.sb.path("cards-are-up")
        self.resources(f"gpu_gb = {{ probe = \"test -f {up} && echo '0, Fake GPU, 24576, 1024, 0' || exit 3\","
                       ' every = "0s" }')
        self.assertIsNone(jobq.card_reading([self.gpu_pool()])[0])      # the failure every reader shares now
        with open(up, "w"):
            pass
        rc, out, err = self.call("add", "--no-start", "--cwd", self.work, "--gpu-gb", "2", "--", "true")
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.state(int(out.split()[-1])), "queued")

    def test_a_job_over_the_pools_max_is_refused_while_the_first_reading_is_anothers(self):
        """A pool's own max is a budget no card widens, so a job no card can ever hold is refused at add time
        where nothing is read of the cards yet. A queue is strict head-of-line, and such a job queued would be
        a head no pass can start."""
        self.resources("gpu_gb = { probe = \"echo '0, Fake GPU, 8192, 1024, 0'\", max = 6 }")
        p = self.gpu_pool()
        with jobq.lock.Claim.take(jobq._probe_claim(p)):        # the first reading is another holder's
            self.assertEqual(jobq.card_reading([p])[2], jobpolicy.NO_READING)
            rc, _, err = self.call("add", "--no-start", "--cwd", self.work, "--gpu-gb", "80", "--", "true")
        self.assertEqual(rc, 2, err)
        self.assertEqual(jobstore.jobs("queued"), [])

    def test_the_view_of_a_gpu_pool_says_nothing_about_the_probe(self):
        """The cards are this pool's reading, so the card line is where a reader is told how they were read
        and what came of it. The pool's own line is its budget and what the scope holds of it."""
        self.resources('gpu_gb = { max = 40, probe = "echo nothing" }')
        rc, out, _ = self.call()
        self.assertEqual(rc, 0)
        r = json.loads(text(jobq._probe_path(self.gpu_pool())))
        self.assertEqual((r["value"], bool(r["error"])), (None, True))   # the probe ran, and it failed
        line = next(s for s in out.splitlines() if s.startswith("shared:"))
        self.assertNotIn("probe", next(f for f in line.split(" · ") if "gpu_gb" in f))

    def test_a_reading_carries_the_time_it_was_taken(self):
        """A pass holds a job's reservation for the pool's settle window from the reading's own time, so a
        reading carries the time it was taken: the time `reading` records for a probe, and the time
        nvidia-smi's reading records as well."""
        launched: list[float] = []

        def reader():
            launched.append(time.time())
            while time.time() == launched[0]:       # the clock moves on while a reading is being taken
                pass
            return [card()]

        with mock.patch.object(jobq, "_cards", reader):
            self.assertLessEqual(jobq.card_reading(jobq.card_pools())[1], launched[0])


class TestTimeoutRetry(QueueCase):
    def test_timeout_kills_the_group(self):
        j = self.add("--timeout", "0.5", cmd="sleep 30 & sleep 30; wait")
        w = jobq.worker_for("default")
        w.tick()
        pid = jobstore.find(j)["pid"]
        journey.until(lambda: len(proc.descendants(pid)) >= 2, timeout=5, what="the job's children")
        kids = proc.descendants(pid)
        self.run_until(w, lambda: self.state(j) == "failed", timeout=8)
        rec = jobstore.find(j)
        self.assertEqual((rec["reason"], rec["rc"]), ("timeout", None))
        journey.until(lambda: not any(proc.alive(k) for k in kids), timeout=5, what="its children to be gone")

    def test_retries_keep_the_id_and_back_off(self):
        marker = self.sb.path("m")
        j = self.add("--retries", "2", "--retry-delay", "0.2", cmd=f"test -f {marker}.2 && exit 0; "
                     f"test -f {marker}.1 && touch {marker}.2 && exit 1; touch {marker}.1; exit 1")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: (jobstore.find(j) or {}).get("attempt") == 2)
        rec = jobstore.find(j)
        # queued for its delay, or running again where a pass took longer than the delay on a loaded machine:
        # what the delay is, the arithmetic below says from the record's own stamps
        self.assertIn(rec["state"], ("queued", "running"))
        # the delay is counted from when the attempt ended, which the record says, not from any clock of this
        # test's: a pass that takes longer than the delay on a loaded machine still leaves the same stamp
        self.assertAlmostEqual(rec["not_before"] - rec["history"][-1]["ended"], 0.2, places=6)
        t2 = rec["not_before"]
        self.run_until(w, lambda: (jobstore.find(j) or {}).get("attempt") == 3)
        rec = jobstore.find(j)
        self.assertAlmostEqual(rec["not_before"] - rec["history"][-1]["ended"], 0.4, places=6)   # doubled
        self.assertLess(t2, rec["not_before"])
        self.run_until(w, lambda: self.state(j) == "done")
        rec = jobstore.find(j)
        self.assertEqual((rec["id"], rec["attempt"], len(rec["history"])), (j, 3, 2))
        with open(rec["log"]) as f:
            log = f.read()
        self.assertIn("attempt 1/3 after exit 1", log)
        self.assertIn("attempt 2/3 after exit 1", log)

    def test_retry_if(self):
        hit = self.add("--retries", "1", "--retry-delay", "0.05", "--retry-if", "transient", cmd="echo transient glitch; exit 1")
        miss = self.add("--retries", "1", "--retry-delay", "0.05", "--retry-if", "transient", cmd="echo real bug; exit 1")
        tmo = self.add("--retries", "1", "--retry-delay", "0.05", "--retry-if", "^timeout$", "--timeout", "0.3", cmd="sleep 30")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: all(self.state(x) == "failed" for x in (hit, miss, tmo)), timeout=10)
        self.assertEqual(jobstore.find(hit)["attempt"], 2)
        self.assertEqual(jobstore.find(miss)["attempt"], 1)       # not retryable: final at once
        self.assertEqual(jobstore.find(tmo)["attempt"], 2)        # the reason is matched too

    def test_a_delay_that_is_already_up_is_not_said_as_a_negative_one(self):
        """A worker notices an attempt ended a poll period after it did, so the delay of a job it is about to
        re-queue can be up already. The three places that say how long is left all say it the same way, and
        none of them counts backwards: `--retry-delay 1s` is the README's own line."""
        said: list = []
        j = self.add("--retries", "1", "--retry-delay", "1", cmd="echo trying; exit 3")
        w = jobq.worker_for("default", log=said.append)
        w.tick()
        rcf = jobstore.find(j)["rcfile"]
        journey.until(lambda: os.path.exists(rcf), timeout=10, what="the job's exit code")
        old = time.time() - 3           # the attempt ended a whole period before the worker noticed
        os.utime(rcf, (old, old))
        self.run_until(w, lambda: jobstore.find(j)["attempt"] == 2)
        self.assertEqual(jobq._in_delay(time.time() - 2), "0s")
        retry = [line for line in said if line.startswith(f"retry {j} ")]
        self.assertEqual(len(retry), 1, said)
        self.assertIn("in 0s", retry[0])            # the delay is behind it, and 0s is as low as it goes
        # and the other two readers of that delay: watch's line for the retry, and the queued row
        self.assertIn("attempt 1/2 failed (exit 3); attempt 2 in 0s",
                      jobq._event({"state": "running", "attempt": 1},
                                  {"state": "queued", "attempt": 2, "retries": 1, "tag": "t",
                                   "not_before": time.time() - 2, "history": [{"rc": 3}]}, j))
        self.assertNotIn("retry in -", self.call("status")[1])

    def test_retry_delay_does_not_block_the_line(self):
        self.config("slots = 1")
        a = self.add("--retries", "1", "--retry-delay", "30", cmd="exit 1")
        b = self.add(cmd="true")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(b) == "done")
        self.assertEqual((self.state(a), jobstore.find(a)["attempt"]), ("queued", 2))
        rc, out, _ = self.call()
        self.assertIn("retry in", out)
        self.assertIn("attempt 2/2", out)


class TestHooks(QueueCase):
    def hook_file(self, name="hook"):
        return self.sb.path(name)

    def test_on_fail_once_after_the_last_attempt_with_env(self):
        f = self.hook_file()
        j = self.add("--retries", "2", "--retry-delay", "0.05", "--tag", "flaky",
                     "--on-fail", f"env | grep '^PP_QUEUE' | sort >> {f}; echo --- >> {f}", cmd="exit 7")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "failed")
        journey.until(lambda: os.path.exists(f) and "---" in text(f), timeout=5, what="the hook's output")
        for _ in range(3):
            w.tick()        # the one actor that fires a hook, with nothing left to fire one for
        said = text(f)
        self.assertEqual(said.count("---"), 1)
        env = dict(line.split("=", 1) for line in said.splitlines() if "=" in line)
        rec = jobstore.find(j)
        self.assertEqual((env["PP_QUEUE"], env["PP_QUEUE_JOB"], env["PP_QUEUE_EVENT"], env["PP_QUEUE_RC"]),
                         ("default", str(j), "failed", "7"))
        self.assertEqual((env["PP_QUEUE_LOG"], env["PP_QUEUE_CWD"], env["PP_QUEUE_TAG"]), (rec["log"], self.work, "flaky"))
        self.assertEqual(env["PP_QUEUE_REASON"], "exit 7")        # what wait prints and --retry-if matches
        self.assertEqual([h["event"] for h in rec["hooks"]], ["failed"])
        self.assertTrue(os.path.exists(rec["hooks"][0]["log"]))

    def test_on_done(self):
        f = self.hook_file()
        j = self.add("--on-done", f"echo $PP_QUEUE_EVENT $PP_QUEUE_RC > {f}", "--on-fail", f"echo wrong > {f}", cmd="true")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "done")
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertEqual(text(f).strip(), "done 0")

    def test_kill_and_drop_fire_nothing(self):
        f = self.hook_file()
        k = self.add("--on-fail", f"echo $PP_QUEUE_JOB >> {f}", cmd="sleep 30")
        d = self.add("--on-fail", f"echo $PP_QUEUE_JOB >> {f}", "--hold", cmd="true")
        w = jobq.worker_for("default")
        w.tick()
        self.worker_beside(w)
        self.assertEqual(self.call("kill", str(k), "--grace", "1")[0], 0)
        self.assertEqual(self.call("drop", str(d))[0], 0)
        self.assertEqual(jobstore.find(k)["reason"], "killed")
        self.assertEqual(jobstore.find(k).get("hooks"), None)
        self.assertFalse(os.path.exists(f))

    def test_a_stop_that_was_signalled_is_never_retried_and_fires_nothing(self):
        """A job the worker reaps right after it signalled the stop is a kill, not a failure to retry or to
        report: the stop on its record says so, whatever its attempts left."""
        f = self.hook_file()
        k = self.add("--retries", "3", "--on-fail", f"echo x >> {f}", cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        with jobstore.txn() as s:
            cur = s.job(k, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 0, "sent": time.time()}
            s.save(cur)
        proc.signal_group(jobcore._group_handle(cur), signal.SIGKILL)
        self.run_until(w, lambda: self.state(k) == "failed")
        rec = jobstore.find(k)
        self.assertEqual((rec["reason"], rec["attempt"]), ("killed", 1))
        self.assertIsNone(rec.get("hooks"))
        self.assertFalse(os.path.exists(f))

    def test_a_stop_that_signalled_nothing_ends_the_job_as_it_ended_and_fires_its_hook(self):
        """A kill writes the stop, and the job ends of something outside pp before any signal of pp's goes
        out for it. Nothing of pp's reached it, so it is recorded as its own run ended, the hook that outcome
        owes fires, and the stop still means it is never retried.

        Without this a real failure is recorded as a kill, and the hook an agent waits on never runs."""
        f = self.hook_file()
        k = self.add("--retries", "3", "--on-fail", f"echo $PP_QUEUE_REASON >> {f}", cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        with jobstore.txn() as s:
            cur = s.job(k, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 10.0}   # written; nothing out for it
            s.save(cur)
        proc.signal_group(jobcore._group_handle(cur), signal.SIGKILL)       # something outside pp ended it
        journey.until(lambda: jobq.lock.Claim.holder(jobstore.run_claim("default", k)) is None, timeout=10,
                      what="the job's run to be over")
        w.carry_out()                   # its run is over before any pass of this worker, so nothing went out
        rec = jobstore.find(k)
        self.assertEqual((rec["state"], rec["reason"], rec["attempt"]), ("failed", "signal 9", 1))
        self.assertIsNone(rec.get("history"))           # a job a kill has touched runs no second time
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertEqual(text(f).strip(), "signal 9")
        self.assertEqual([h["event"] for h in jobstore.find(k)["hooks"]], ["failed"])

    def test_a_timeout_pp_was_refused_ends_the_attempt_at_its_timeout(self):
        """pp may not signal every process group. A timeout's stop pp was refused is recorded as refused and
        nothing more goes out for it, so the job runs on; once its run is over the attempt ends with reason
        "timeout", which is what the timeout asked for. So `--retry-if '^timeout$'` matches it, and `show`
        says pp was refused rather than leaving a person to read the job as lost."""
        j = self.add("--timeout", "60", "--retries", "1", "--retry-delay", "0.05", "--retry-if", "^timeout$",
                     cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        self.started_earlier(j, 120)                    # its own time has run out
        with mock.patch.object(jobq.proc, "signal_group", lambda *a, **k: "denied"):
            w.carry_out()                               # the timeout's stop goes on the record ...
            w.carry_out()                               # ... and pp is refused the job's group
            st = jobstore.find(j)["stop"]
            self.assertEqual((st["timeout"], st.get("sent")), (True, None))
            self.assertTrue(st["denied"])
            shown = self.call("show", str(j))[1]
        self.assertIn("nothing signalled", shown)
        self.assertIn("pp is not allowed to signal its group", shown)
        proc.signal_group(jobcore._group_handle(jobstore.find(j)), signal.SIGKILL)     # it ends some other way
        journey.until(lambda: jobq.lock.Claim.holder(jobstore.run_claim("default", j)) is None, timeout=10,
                      what="the job's run to be over")
        w.carry_out()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["attempt"]), ("queued", 2))         # the timeout may be retried
        self.assertEqual(rec["history"][-1]["reason"], "timeout")

    def test_a_final_timeout_fires_on_fail_as_the_help_says(self):
        """A timeout may be retried, and the last one is a failure like any other, so on_fail fires for it:
        a job that hangs is what a person wants to hear about. The sentence in the help about a signal that
        fires no hook is a kill's, since a kill is someone asking for this outcome."""
        f = self.hook_file()
        j = self.add("--timeout", "0.2", "--retries", "0", "--on-fail", f"echo $PP_QUEUE_REASON > {f}",
                     cmd="sleep 30")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "failed", timeout=15)
        self.assertEqual(jobstore.find(j)["reason"], "timeout")
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertEqual(text(f).strip(), "timeout")
        self.assertEqual([h["event"] for h in jobstore.find(j)["hooks"]], ["failed"])
        said = " ".join(queue_cmd.__doc__.split())       # the sentences the help owns, not how it wraps them
        self.assertIn("on a final timeout on_fail fires.", said)
        self.assertIn('is recorded "killed" and fires no hook', said)

    def test_a_hook_its_worker_did_not_get_to_fire_is_fired_by_the_next_one(self):
        """The outcome and the hook owed for it are written in one transaction, and the record says it is
        owed until the hook has run. A worker that dies in between leaves that record, and the next worker's
        pass fires it: a hook fires once, and not never."""
        f = self.hook_file()
        j = self.add("--on-fail", f"echo x >> {f}", cmd="exit 3")
        w = jobq.worker_for("default")
        with mock.patch.object(jobcore.Worker, "fire", lambda *a, **k: None):      # it died before firing it
            self.run_until(w, lambda: self.state(j) == "failed")
        self.assertEqual(jobstore.find(j)["hook_due"], "failed")
        self.assertFalse(os.path.exists(f))
        w2 = jobq.worker_for("default")
        w2.tick()                                   # the next worker finds the hook owed, and fires it
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertNotIn("hook_due", jobstore.find(j))
        self.assertEqual([h["event"] for h in jobstore.find(j)["hooks"]], ["failed"])
        for _ in range(3):
            w2.tick()                               # and no later tick fires it again
        self.assertEqual(text(f).count("x"), 1)

    def test_launch_failure_fires_on_fail(self):
        f = self.hook_file()
        j = self.add("--on-fail", f"echo $PP_QUEUE_REASON > {f}", cmd="true")
        shutil.rmtree(self.work)
        w = jobq.worker_for("default")
        w.tick()
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertIn("launch failed", text(f))
        self.assertEqual(self.state(j), "failed")

    def test_hook_argv(self):
        self.assertEqual(jobpolicy.hook_argv("echo hi"), ["/bin/sh", "-c", "echo hi"])
        self.assertIsNone(jobpolicy.hook_argv("none"))
        self.assertIsNone(jobpolicy.hook_argv(None))

    def test_owner_captured_and_no_default_hook(self):
        human = jobstore.find(self.add())
        self.assertIsNone(human["owner"])
        self.assertIsNone(human["on_fail"])
        self.sb.as_session("test-session-1")
        self.sb.setenv(TMUX_PANE="%999999")
        agent = jobstore.find(self.add())
        explicit = jobstore.find(self.add("--on-fail", "none"))
        self.assertIsNone(agent["on_fail"])
        self.assertEqual(agent["owner"], {"session": "test-session-1", "cwd": os.getcwd()})
        self.assertIsNone(explicit["on_fail"])

    def test_session_identity_never_reaches_the_job(self):
        self.sb.as_session("test-session-2")
        self.sb.setenv(TMUX_PANE="%999999", CLAUDECODE="1")
        j = self.add(cmd="env")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "done")
        with open(jobstore.find(j)["log"]) as f:
            log = f.read()
        self.assertNotIn("CLAUDE_CODE_SESSION_ID", log)
        self.assertNotIn("TMUX_PANE", log)
        self.assertNotIn("CLAUDECODE", log)
        self.assertIn(f"PP_QUEUE_JOB={j}", log)


class TestShowNote(QueueCase):
    def test_a_jobs_own_text_cannot_redraw_the_terminal(self):
        """A queued command, a note and the job's own output are all text from elsewhere, and a reader of
        `pp queue` may be an agent whose terminal is its own output. Every place pp prints a job's text puts
        it through ui.printable, so an escape sequence in it is shown rather than acted on. A job's name is
        letters, digits and . _ - : alone, so no escape ever reaches a view through it."""
        esc = "\x1b[2J"             # clear the screen
        osc = "\x1b]0;PWNED\x07"    # retitle the terminal, which the CSI pattern alone does not catch
        ris = "\x1bc"               # reset it
        self.assertEqual(self.call("add", "--no-start", "--cwd", self.work, "--tag", "tag" + esc,
                                   "--", "true")[0], 2)
        rc, out, err = self.call("add", "--no-start", "--cwd", self.work, "--",
                                 "sh", "-c", f"printf '{osc}oops\\n{ris}'; echo start{esc}; exit 1")
        j = int(out.split()[-1])
        self.assertEqual(rc, 0, err)
        self.assertNotIn("\x1b", err)                   # add's own summary of what it queued
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "failed")
        self.assertEqual(self.call("note", str(j), "note" + esc)[0], 0)
        for argv in (("status",), ("show", str(j)), ("wait", str(j), "--timeout", "10", "--poll", "0.05")):
            with self.subTest(argv=argv):
                _rc, out, err = self.call(*argv)
                self.assertNotIn("\x1b", out + err, f"{argv[0]} printed an escape sequence of the job's")
                self.assertIn("\\x1b", out + err, f"{argv[0]} printed none of the job's text")
        self.assertIsNone(jobstore.find(j)["tag"])      # it was added without --tag, so it has no name

    def test_note_and_show(self):
        j = self.add("--tag", "noted", "--timeout", "90s", cmd="echo out-line; exit 2")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "failed")
        rc, out, _ = self.call("note", str(j), "lr", "too", "high")
        self.assertEqual(rc, 0)
        rec = jobstore.find(j)
        self.assertEqual((rec["notes"][0]["text"], rec["notes"][0]["by"]), ("lr too high", "user"))
        rc, out, _ = self.call("show", str(j))
        self.assertEqual(rc, 0)
        for s in ("FAILED", "rc=2", "noted", "1m30s", "lr too high", "out-line", rec["log"]):
            self.assertIn(s, out)
        data = json.loads(self.call("show", "noted", "--json")[1])
        self.assertEqual((data["id"], data["state"], data["queue"], data["rc"]), (j, "failed", "default", 2))
        self.assertIn("note: lr too high", self.call()[1])

    def test_show_says_a_dash_for_an_attempt_that_wrote_no_exit_code(self):
        """An attempt that was killed, timed out or lost wrote no exit code. `show` says `-` for it, the way
        the header of the same output does, instead of leaking Python's None into what a person reads."""
        j = self.add("--timeout", "0.2", "--retries", "1", "--retry-delay", "0.05", "--retry-if", "timeout",
                     cmd="sleep 30")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: jobstore.find(j)["attempt"] == 2, timeout=15)
        out = self.call("show", str(j))[1]
        self.assertIn("attempt 1: rc=- (timeout)", out)
        self.assertNotIn("None", out)

    def test_the_json_gives_a_jobs_variables_by_name_and_not_by_value(self):
        """The help says to pass a token or a secret with --env, and `--json` is what an agent reads to see
        how its jobs are getting on, so the values of a job's environment would land in a transcript. The
        names are kept, since a reader wants to know which variables a job carries; the values are ***. The
        job's own record is where what it runs with lives."""
        j = self.add("--hold", "--env", "HF_TOKEN=hf-secret-123")
        for argv in (("show", str(j), "--json"), ("status", "--json")):
            out = self.call(*argv)[1]
            self.assertNotIn("hf-secret-123", out, argv)
            self.assertIn("HF_TOKEN", out, argv)
        shown = json.loads(self.call("show", str(j), "--json")[1])
        self.assertEqual(shown["env"], {"HF_TOKEN": "***"})
        self.assertEqual(set(shown["shell_env"]), set(jobstore.find(j)["shell_env"]))
        self.assertEqual(set(shown["shell_env"].values()) - {"***"}, set())
        self.assertNotIn("HF_TOKEN", self.call("show", str(j))[1])      # the text form says neither
        self.assertEqual(jobstore.find(j)["env"], {"HF_TOKEN": "hf-secret-123"})

    def test_every_promised_field_is_on_a_job_in_every_state(self):
        """The machine surface is every field of the promised list, off a record in each of the five states.
        A field a record does not hold yet comes back empty rather than missing, so a reader tells "nothing
        yet" from "no such field" and never has to guess which state a field appears in."""
        self.config("slots = 1")        # so the one that is to stay queued has no slot to take
        w = jobq.worker_for("default")
        states = {"done": self.add(cmd="true"), "failed": self.add(cmd="exit 3")}
        self.run_until(w, lambda: self.state(states["failed"]) == "failed")
        states["running"] = self.add(cmd="sleep 30")
        w.tick()
        states["queued"] = self.add()
        states["held"] = self.add("--hold")
        w.tick()
        for state, jid in states.items():
            with self.subTest(state=state):
                self.assertEqual(self.state(jid), state)
                j = json.loads(self.call("show", str(jid), "--json")[1])
                self.assertEqual([f for f in journey.PROMISED if f not in j], [])
                self.assertEqual(j["cards"] if state != "running" else [], [])
                for f in ("history", "notes", "reported", "hooks"):
                    self.assertIsInstance(j[f], list, f)
        self.assertNotIn("after", json.loads(self.call("show", str(states["queued"]), "--json")[1]))

    def test_note_by_session(self):
        j = self.add("--hold")
        self.sb.as_session("test-session-3")
        self.call("note", str(j), "from an agent")
        self.assertEqual(jobstore.find(j)["notes"][0]["by"], "test-session-3")


class TestStatusWait(QueueCase):
    def test_status_renders(self):
        self.config("slots = 1")
        self.add(cmd="sleep 2")
        self.add(cmd="true")
        self.add("--hold", cmd="true")
        w = jobq.worker_for("default")
        w.tick()
        rc, out, _ = self.call()
        self.assertEqual(rc, 0)
        for word in ("running", "queued", "held", "worker down"):
            self.assertIn(word, out)
        data = json.loads(self.call("status", "--json")[1])
        self.assertEqual((len(data["running"]), len(data["queued"]), len(data["held"])), (1, 1, 1))
        self.assertEqual(data["running"][0]["queue"], "default")

    def test_the_running_section_is_one_table(self):
        """Every row of a section comes from one table, so its columns line up. The running section is the one
        a person watches most, and a short tag beside a long one must not shift the columns after it. A row
        whose last cell is empty ends where its text ends, with no padding of a column nothing filled."""
        self.config("slots = 3")
        a = self.add("--tag", "a", cmd="sleep 30")
        b = self.add("--tag", "a-very-long-tag-name-for-alignment", cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual([self.state(x) for x in (a, b)], ["running", "running"])
        out = self.call("status")[1]
        rows = [ln for ln in out.splitlines() if ln.startswith("  ") and "[work]" in ln]
        self.assertEqual(len(rows), 2, out)
        self.assertEqual(len({ln.index("[work]") for ln in rows}), 1, f"the columns do not line up:\n{out}")
        for ln in rows:
            self.assertEqual(ln, ln.rstrip(), f"a row ends in the spaces of an empty column:\n{out}")

    def test_status_fits_an_eighty_column_terminal(self):
        """`pp queue` on a standard terminal keeps its shape: the tag is cut until the widest row of a section
        fits, and the queue's counts move to a line of their own where the header does not fit beside them.
        A row that wrapped lost its shape altogether, with its last two characters on a line of their own."""
        self.config("slots = 1")
        d = self.add("--tag", "d" * 64, cmd="echo done")
        w = self.as_the_worker()                    # a worker is up, so the header carries its pid
        self.run_until(w, lambda: self.state(d) == "done")
        a = self.add("--tag", "a" * 64, cmd="echo this-is-a-fairly-ordinary-long-output-line-of-a-job; sleep 30")
        w.tick()
        self.add("--tag", "q" * 64, cmd="sleep 30")
        self.add("--hold", "--tag", "h" * 64, cmd="sleep 30")
        journey.until(lambda: jobq._last_line(jobstore.find(a)["log"], 200), timeout=10, what="the job's output")
        self.sb.setenv(COLUMNS="80")
        rc, out, _ = self.call("status")
        self.assertEqual(rc, 0)
        self.assertEqual([ln for ln in out.splitlines() if len(ln) > 80], [], f"wider than 80 columns:\n{out}")
        self.assertEqual(out.count("…"), 4, f"a tag of each section is cut to fit:\n{out}")
        self.assertIn("worker up (pid", out)
        self.assertRegex(out, r"\n  slots 1/1 · queued 1")      # the counts on a line of their own

    def test_no_queues(self):
        """A machine with none of the three: no records, no worker, and no queue its config names."""
        self.tables.clear()
        self._write_config()
        rc, out, _ = self.call()
        self.assertEqual(rc, 0)
        self.assertIn("no queues yet", out)

    def test_exit_codes(self):
        ok = self.add(cmd="true")
        bad = self.add(cmd="exit 2")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(ok) == "done" and self.state(bad) == "failed")
        self.assertEqual(self.call("wait", str(ok))[0], 0)
        rc, out, _ = self.call("wait", f"{ok}-{bad}")
        self.assertEqual(rc, 1)
        self.assertIn("FAILED", out)

    def test_timeout(self):
        q = self.add(cmd="true")
        t = time.time()
        rc, _, err = self.call("wait", str(q), "--no-start", "--timeout", "0.3", "--poll", "0.05")
        self.assertEqual(rc, 3)
        self.assertLess(time.time() - t, 3)
        self.assertIn("timeout", err)

    def test_a_timeout_is_honoured_whatever_the_poll_is(self):
        """A poll longer than the timeout is still a wait that gives up at its timeout: the look after the
        deadline comes at the deadline and not a poll period later."""
        q = self.add(cmd="true")
        t = time.time()
        rc, _, err = self.call("wait", str(q), "--no-start", "--timeout", "0.3", "--poll", "60")
        self.assertEqual((rc, "timeout" in err), (3, True))
        self.assertLess(time.time() - t, 20)

    def test_a_bare_wait_covers_every_job_that_is_queued_or_running(self):
        """A bare wait waits for all of them, so a script that waits for what it queued is not told the first
        outcome with the rest still to come. It names no held job: a selection is what asks for a job, and a
        bare wait names none, so a parked stage does not hold it forever."""
        self.config("slots = 1")
        first = self.add(cmd="true")
        second = self.add(cmd="exit 5")
        held = self.add("--hold", cmd="true")
        self.sb.ticker(jobq.worker_for("default").tick)
        rc, out, err = self.call("wait", "--no-start", "--poll", "0.05", "--timeout", "20")
        self.assertEqual(rc, 1, out + err)                  # the second one failed, and it was waited for
        self.assertEqual((self.state(first), self.state(second)), ("done", "failed"))
        self.assertEqual(self.state(held), "held")          # never named, so never waited for
        self.assertNotIn(str(held), out + err)

    def test_a_bare_wait_stops_waiting_for_a_job_held_while_it_waits(self):
        """A job queued when a bare wait listed it and held before it ended is a job that wait named none of:
        a parked stage is normal, so the wait ends with the jobs that did end instead of sitting there until
        something releases it.

        One thread does every step: the wait's own pass is what ticks the worker, so the hold lands between
        two passes of it and nothing waits for a clock."""
        self.config("slots = 1")
        ran = self.add(cmd="true")
        parked = self.add(cmd="true")           # queued behind it: one slot
        gate = journey.Gate()

        def each_pass():
            gate.once(lambda: self.call("hold", str(parked)))    # the wait listed both; this parks one
        with self.one_thread(each_pass):
            rc, out, err = self.call("wait", "--poll", "0.01", "--timeout", "5")
        self.assertEqual((rc, gate.ran), (0, 1), out + err)
        self.assertEqual((self.state(ran), self.state(parked)), ("done", "held"))

    def test_nothing_to_wait_for(self):
        self.assertEqual(self.call("wait", "--no-start")[0], 0)
        self.assertEqual(self.call("wait", "--idle", "--no-start")[0], 0)


class TestPull(QueueCase):
    """wait / watch: how an agent hears about its jobs without anything being pushed into its session."""

    def test_wait_prints_the_failure_with_its_log_tail_and_stamps_the_job(self):
        j = self.add(cmd="echo first-line; echo boom-line; exit 3")
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "failed")
        rc, out, _ = self.call("wait", str(j), "--no-start")
        self.assertEqual(rc, 1)
        self.assertIn("  | boom-line", out)
        self.assertIn(f"pp queue retry {j}", out)
        self.assertTrue(jobstore.find(j)["reported"][0]["by"].startswith("wait pid"))

    def test_watch_streams_every_event_across_a_retry(self):
        marker = self.sb.path("flaky")
        j = self.add("--retries", "1", "--retry-delay", "0.4", "--on-fail", "none",
                     cmd=f"sleep 0.3; test -f {marker} || {{ touch {marker}; echo flaked; exit 1; }}; echo fine")
        self.sb.ticker(jobq.worker_for("default").tick)
        rc, out, _ = self.call("watch", str(j), "--no-start", "--poll", "0.05", "--timeout", "20")
        self.assertEqual(rc, 0, out)
        lines = out.splitlines()
        self.assertTrue(lines[0].startswith(f"[queue watch] {j} "), out)
        want = [f"{j} attempt 1/2 failed (exit 1); attempt 2 in", f"{j} started attempt 2/2", f"done {j}"]
        at = [next((i for i, ln in enumerate(lines) if ln.startswith(w)), None) for w in want]
        self.assertNotIn(None, at, out)
        self.assertEqual(at, sorted(at), out)

    def test_a_waiter_holds_a_lease_until_it_exits(self):
        j = self.add(cmd="true")                 # queued; nothing runs it
        p = journey.start([sys.executable, os.path.join(ROOT, "bin", "pp"), "queue", "wait", str(j), "--no-start",
                           "--poll", "0.1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            journey.until(lambda: jobcore.watchers(jobstore.find(j)), timeout=10, what="the wait's lease on the job")
            w = jobcore.watchers(jobstore.find(j))[0]
            self.assertEqual((w["pid"], w["ids"], w["verb"]), (p.pid, [j], "wait"))
            self.assertEqual(json.loads(self.call("show", str(j), "--json")[1])["watchers"][0]["pid"], p.pid)
        finally:
            p.close()         # no cleanup runs: the lease file stays behind ...
        self.assertEqual(jobcore.watchers(jobstore.find(j)), [])       # ... and is recognised as dead and removed
        self.assertEqual(os.listdir(os.path.join(self.sb.state, "queue", "watchers")), [])

    def test_a_lease_whose_wait_a_signal_ended_is_swept_by_a_worker(self):
        """A lease is a claim, which the kernel lets go of however its holder ends, and an agent ends a
        background `pp queue wait` with SIGTERM, which runs no way out of its own. The file that wait was
        holding is swept by every pass of a worker, where the queue is being read anyway, so an agent that
        only ever adds and waits leaves nothing behind."""
        d = os.path.join(self.sb.state, "queue", "watchers")
        j = self.add("--hold", cmd="true")           # held: nothing runs it, and the wait waits
        p = journey.start([sys.executable, os.path.join(ROOT, "bin", "pp"), "queue", "wait", str(j),
                           "--no-start", "--poll", "0.1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(p.close)
        journey.until(lambda: jobcore.watchers(jobstore.find(j)), timeout=10, what="the wait's lease on the job")
        leases = os.listdir(d)
        self.assertEqual(len(leases), 1, leases)
        p.signal(signal.SIGTERM)                     # as an agent ends a wait it ran in the background
        journey.until(lambda: p.poll() is not None, timeout=10, what="the wait to end")
        self.assertEqual(os.listdir(d), leases)      # nothing of its own took the file with it ...
        jobq.worker_for("default").tick()
        self.assertEqual(os.listdir(d), [])          # ... and the next pass of a worker did

    def test_a_lease_a_signal_ended_mid_take_is_swept_too(self):
        """A lease is locked under a name of this call's own and renamed into place. A holder killed between
        the two runs no way out of its own, and what it leaves is a claim file like any other: the next pass
        over the leases finds nobody holding it and forgets it. So a long-lived install collects none of
        them."""
        d = os.path.join(self.sb.state, "queue", "watchers")
        os.makedirs(d, exist_ok=True)
        lease = os.path.join(d, "999999-1.claim")
        p = journey.start([sys.executable, "-c", KILLED_TAKING_A_LEASE, lease], cwd=ROOT,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(p.close)
        journey.until(lambda: p.poll() is not None, timeout=10, what="the wait to be killed as it took a lease")
        self.assertEqual(p.poll(), -signal.SIGKILL)
        left = os.listdir(d)
        self.assertEqual(len(left), 1, left)            # the file it was holding, under the name it took
        self.assertFalse(os.path.exists(lease))         # and never under the name it was taking it for
        self.assertEqual(jobcore.watchers(), [])
        self.assertEqual(os.listdir(d), [])             # the sweep every read of a queue runs forgot it

    def test_a_lease_cannot_be_swept_away_while_a_wait_is_taking_it(self):
        """A worker sweeps the leases nobody holds on every pass. A lease is locked before its name is
        visible, so a sweep that runs at any step of taking one either does not see it yet or finds it held.
        A sweep that unlinked it would leave the wait holding a lease nothing names: `show` would report no
        watcher for that wait for its whole life, and its own way out would free nothing."""
        j = self.add("--hold", cmd="true")
        d = os.path.join(self.sb.state, "queue", "watchers")
        steps = {"created": (jobq.lock.os, "open"), "locked": (jobq.lock.fcntl, "fcntl"),
                 "named": (jobq.lock.os, "rename")}
        for step, (where, fn) in steps.items():
            with self.subTest(step):
                real, gate = getattr(where, fn), journey.Gate()

                def about_a_lease(args, step=step):
                    if step == "locked":
                        return args[1] == jobq.lock._OFD_SETLK
                    return ".claim" in str(args[0 if step == "created" else 1])

                def stepped(*args, real=real, gate=gate, about=about_a_lease, **kw):
                    out = real(*args, **kw)
                    if about(args):
                        gate.once(jobcore.watchers)        # a worker's pass over the leases, exactly here
                    return out
                with mock.patch.object(where, fn, stepped):
                    with jobcore.watching([j], None, "wait"):
                        held = [w["pid"] for w in jobcore.watchers(jobstore.find(j))]
                        files = os.listdir(d)
                self.assertEqual(gate.ran, 1, f"no sweep ran as the lease was {step}")
                self.assertEqual(held, [os.getpid()], f"the sweep took the lease away as it was {step}")
                self.assertEqual(len(files), 1, files)
                self.assertEqual(os.listdir(d), [])     # and the wait's own way out took its file with it

    def test_mine_is_this_sessions_jobs(self):
        self.sb.as_session("session-a")
        mine = self.add("--on-fail", "none", cmd="true")
        self.sb.as_session("session-b")
        theirs = self.add("--on-fail", "none", "--hold", cmd="true")
        self.sb.as_session("session-a")
        self.sb.ticker(jobq.worker_for("default").tick)
        rc, out, _ = self.call("wait", "--mine", "--no-start", "--poll", "0.05", "--timeout", "20")
        self.assertEqual(rc, 0, out)
        self.assertIn(f"done {mine}", out)
        self.assertNotIn(f" {theirs} ", out)
        self.sb.setenv(CLAUDE_CODE_SESSION_ID=None)         # a person, with no session of their own
        self.assertEqual(self.call("wait", "--mine", "--no-start")[0], 2)

    def test_the_two_halves_of_a_mine_wait_come_from_one_listing(self):
        """`wait --mine` takes the live jobs and the ended ones from one listing, so a job that ends between
        the two cannot be named twice, nor fall between them and be waited on forever."""
        self.sb.as_session("session-a")
        self.add("--hold", cmd="true")          # live, so the wait has something to wait for
        made: list = []
        real_init, real_jobs, seen = jobstore.Txn.__init__, jobstore.Txn.jobs, []

        def init(s, write):
            real_init(s, write)
            made.append(s)                      # kept, so two scopes are never the same object
            s.serial = len(made)

        def jobs(s, state=(), queue=None):
            seen.append((s.serial, (state,) if isinstance(state, str) else tuple(state)))
            return real_jobs(s, state, queue)
        with mock.patch.object(jobstore.Txn, "__init__", init), mock.patch.object(jobstore.Txn, "jobs", jobs):
            rc, _, err = self.call("wait", "--mine", "--no-start", "--poll", "0.05", "--timeout", "0.3")
        self.assertEqual(rc, 3, err)
        # one pass over the store, asked for every state and read in both halves: a job that ends between
        # them cannot be named twice, and none falls between two passes
        passes = [t for t in seen if t[1] in ((), tuple(jobstore.LIVE), tuple(jobstore.ENDED))]
        self.assertEqual([t[1] for t in passes], [()])

    def test_a_job_that_starts_while_wait_lists_is_waited_on_once(self):
        """wait names each target once: a job is one record in one state, and one listing reads both halves of
        `--mine`, so a job the worker starts meanwhile cannot be named twice and then waited on forever. The
        job here ends only once the wait has listed it, never before."""
        self.sb.as_session("session-a")
        real, go = jobstore.find, os.path.join(self.work, "go")

        def find(jid):          # the first look of the wait's own loop, which is after it listed its targets
            open(go, "a").close()
            return real(jid)
        for argv in (["--mine"], []):
            j = self.add("--on-fail", "none", cmd=f"while [ ! -f {go} ]; do sleep 0.02; done")
            ticker = self.sb.ticker(jobq.worker_for("default").tick)
            journey.until(lambda: self.state(j) == "running", timeout=10, what=f"job {j} to start")
            with mock.patch.object(jobcore, "find", find):
                rc, out, _ = self.call("wait", *argv, "--no-start", "--poll", "0.05", "--timeout", "10")
            self.assertEqual(rc, 0, out)
            self.assertEqual(out.count(f"done {j}"), 1, out)
            ticker.stop()           # what it raised, if anything, comes out here
            os.remove(go)

    def test_add_tells_an_agent_how_to_hear_back(self):
        rc, _, err = self.call("add", "--no-start", "--", "true")
        self.assertNotIn("in the background", err)
        self.sb.as_session("session-a")
        rc, out, err = self.call("add", "--no-start", "--on-fail", "none", "--", "true")
        self.assertIn(f"pp queue wait {out.split()[0]}` in the background", err)
        # one `add`'s ids are consecutive, so the command it hands an agent names them as one range word
        plan = self.sb.path("plan.txt")
        with open(plan, "w") as f:
            f.write("true\ntrue\ntrue\n")
        rc, out, err = self.call("add", "--no-start", "--from", plan)
        ids = out.split()
        self.assertEqual((rc, len(ids)), (0, 3), err)
        self.assertIn(f"pp queue wait {ids[0]}-{ids[2]}` in the background", err)


class TestEndToEnd(QueueCase):
    def test_cli_with_detached_worker_and_retries(self):
        self.config("slots = 2", "poll = 0.2\nidle_exit = 3")
        # an agent's identity in the caller: the worker it starts must not inherit it
        env = dict(os.environ, CLAUDE_CODE_SESSION_ID="test-session-e2e", CLAUDE_PID=str(os.getpid()), TMUX_PANE="%999999")
        pp = [sys.executable, os.path.join(ROOT, "bin", "pp")]
        marker = self.sb.path("flaky")

        def run(*argv, timeout=30):
            return subprocess.run(pp + ["queue", *argv], capture_output=True, text=True, env=env, cwd=self.work, timeout=timeout)

        try:
            r = run("add", "--tag", "e2e", "--retries", "1", "--retry-delay", "0.2", "--on-fail", "none", "--",
                    "sh", "-c", f"test -f {marker} || {{ touch {marker}; exit 1; }}; echo hello-e2e")
            self.assertEqual(r.returncode, 0, r.stderr)
            jid = r.stdout.split()[0]
            self.assertIn("started the default worker", r.stderr)
            journey.until(lambda: (worker_pid() or 0) > 0, timeout=10, what="the worker to be up")
            wpid = worker_pid()
            with open(f"/proc/{wpid}/environ", "rb") as f:
                wenv = f.read()
            self.assertNotIn(b"CLAUDE_CODE_SESSION_ID", wenv)
            self.assertNotIn(b"TMUX_PANE", wenv)
            r = run("wait", jid, "--timeout", "20", "--poll", "0.2")       # across the retry
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn(f"done {jid}", r.stdout)
            r = run("log", "e2e")
            self.assertIn("hello-e2e", r.stdout)
            data = json.loads(run("status", "--json").stdout)
            self.assertEqual((data["recent"][0]["id"], data["recent"][0]["attempt"]), (int(jid), 2))
            self.assertEqual(data["recent"][0]["owner"]["session"], "test-session-e2e")
        finally:
            run("worker", "stop", "-q", "default")
        self.assertIsNone(worker_pid())



class TestWorkerClaim(QueueCase):
    """A worker is up while it holds its queue's claim, and nothing else says so. Each test asks what the
    claim promises, not what a file holds."""

    def left_behind(self, **rec):
        """The record a worker that was killed left in the claim file: nobody holds the claim, and the bytes
        are still there. A release empties the file, so a worker that died is the only one that leaves a
        record behind, and another process is what leaves one here."""
        r = subprocess.run([sys.executable, "-c", LEFT_A_RECORD, "default", json.dumps(rec)], cwd=ROOT,
                           capture_output=True, text=True, timeout=60)
        self.assertEqual((r.returncode, r.stderr), (0, ""))

    def test_a_worker_record_with_no_holder_reads_as_down(self):
        """The record a worker that was killed left stays in its claim file. Whether a worker is up is the
        claim's answer; a record nobody holds is a worker that has ended."""
        self.add("--hold")
        me = proc.read(os.getpid())
        self.left_behind(pid=os.getpid(), pstart=me.start)
        self.assertIsNone(jobcore.worker("default"))
        self.assertIsNone(worker_pid())
        self.assertIsNone(jobcore._handle(jobcore.worker_now("default")))
        with jobq.lock.Claim.take(jobcore._worker_claim("default"), {"pid": os.getpid(), "pstart": me.start}):
            self.assertEqual(worker_pid(), os.getpid())   # held: the record is this holder's own

    def test_stopping_a_worker_that_just_exited(self):
        self.add("--hold")
        with open("/proc/sys/kernel/pid_max") as f:
            nobody = int(f.read())                  # one past the largest pid: no process can have it
        self.left_behind(pid=nobody, pstart=1)
        rc, out, _ = self.call("worker", "stop")
        self.assertEqual((rc, out.strip()), (0, "default: worker down"))

    def test_stopping_a_worker_that_exits_as_it_is_asked(self):
        """A worker that exits on its idle timer between the record this read and the signal it sends is a
        worker that is down: that is what the stop says, and it says it without a warning."""
        self.add("--hold")
        me = proc.read(os.getpid())
        with jobq.lock.Claim.take(jobcore._worker_claim("default"), {"pid": os.getpid(), "pstart": me.start}):
            with mock.patch.object(jobq.proc, "signal_pid", lambda *a, **k: "gone"):
                rc, out, err = self.call("worker", "stop")
            self.assertEqual((rc, out.strip(), err), (0, "default: worker down", ""))

    def test_stopping_a_worker_in_its_first_moment(self):
        """A worker takes the claim and writes its record in the call after it, so for that moment the claim
        is held by something that names no process. There is nothing to signal yet, so the stop says a worker
        is starting and asks the user to come back, rather than signalling whatever holds that pid."""
        self.add("--hold")
        with jobq.lock.Claim.take(jobcore._worker_claim("default")), \
                mock.patch.object(jobcore, "STARTING_PAUSE", 0):
            rc, _, err = self.call("worker", "stop")
        self.assertEqual(rc, 1)
        self.assertIn("the default worker is starting", err)

    def test_a_stop_waits_for_the_worker_it_signalled_not_for_a_free_claim(self):
        """An `add` or a background `wait` starts the next worker as soon as the claim is free, so a successor
        can hold it before the stop looks again. The stop is over when the worker it signalled has let the
        claim go, whoever holds it then."""
        self.add("--hold")
        me = proc.read(os.getpid())
        old = jobq.lock.Claim.take(jobcore._worker_claim("default"), {"pid": os.getpid(), "pstart": me.start})
        self.addCleanup(old.release)
        successor = []

        def signalled(h, sig=signal.SIGTERM):
            old.release()                   # the worker it signalled ends, and the next one takes the claim
            c = jobq.lock.Claim.take(jobcore._worker_claim("default"), {"pid": os.getpid() + 1, "pstart": 1})
            self.assertIsNotNone(c, "the successor could not take the claim")
            successor.append(c)
            self.addCleanup(c.release)
            return "exited"
        with mock.patch.object(jobq.proc, "signal_pid", signalled), mock.patch.object(jobcore, "STOP_WAIT", 2.0):
            rc, out, err = self.call("worker", "stop")
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"stopped worker {os.getpid()}", out)
        self.assertEqual((jobcore.worker("default") or {}).get("pid"), os.getpid() + 1)   # the successor is up

    def test_readiness_means_a_stop_is_handled(self):
        """A worker says it is ready only once it holds its claim, has recorded itself and has its stop
        handler in place. So a SIGTERM the instant it says so stops it between ticks, and leaves no worker."""
        self.add("--hold")
        p = jobcore._start_worker("default")           # returns when the worker says it is ready
        self.addCleanup(p.wait)
        self.addCleanup(p.end, signal.SIGKILL)
        self.assertEqual(p.end(signal.SIGTERM), "alive")      # it went out; the wait says what it did
        self.assertEqual(p.wait(10), 0)             # its own exit, not a signal's
        self.assertIsNone(jobcore.worker("default"))

    def test_a_worker_is_up_the_moment_worker_up_returns(self):
        """Nothing looks again to find out whether the worker it started came up: it was told."""
        self.config("slots = 3", table="idle_exit = 60\npoll = 0.2")
        self.add("--hold")
        self.addCleanup(self.call, "worker", "stop")
        w = jobq._worker_up("default", quiet=True)
        self.assertEqual((w.how, w.up), (jobcore.STARTED, True))
        self.assertEqual((jobcore.worker("default") or {}).get("pid"), w.pid)
        rc, out, _ = self.call("worker", "stop")
        self.assertEqual((rc, out.split()[:3]), (0, ["default:", "stopped", "worker"]))
        self.assertIsNone(jobcore.worker("default"))

    def test_the_answer_is_a_decision_taken_after_the_last_start(self):
        """A worker that lets the claim go between the start and the next look sends `worker_up` round
        again, three starts at most. The last of them is asked about like the others, so `worker start` can
        never say a worker did not come up while the worker it just started holds the claim."""
        self.add("--hold")
        me, starts = proc.read(os.getpid()), []

        def start(q):
            """The first workers take the claim and let it go, as one at its idle_exit does; the last keeps
            it, as a worker that is up does."""
            starts.append(q)
            claim = jobq.lock.Claim.take(jobcore._worker_claim(q), {"pid": os.getpid(), "pstart": me.start})
            self.assertIsNotNone(claim, f"the {q} worker claim was not free for the worker it started")
            if len(starts) < 3:
                claim.release()
                return mock.Mock(pid=-1)
            self.addCleanup(claim.release)
            return mock.Mock(pid=os.getpid())
        with mock.patch.object(jobcore, "_start_worker", start):
            rc, out, err = self.call("worker", "start")
        self.assertEqual(rc, 0, err)
        self.assertIn(f"started worker (pid {os.getpid()};", out)
        self.assertEqual(len(starts), jobcore.WORKER_STARTS)
        self.assertEqual(worker_pid(), os.getpid())

    def test_a_job_added_before_the_worker_lets_go_of_its_claim_keeps_it_up(self):
        """A worker lets go of its claim inside the scope that reads the store, so a job stored before that
        read is seen here and this worker stays for it. One stored after it finds the claim free, and the add
        that stored it starts the next worker."""
        self.config("slots = 3", table="idle_exit = 0\npoll = 0.01")
        self.add("--hold")                          # nothing to run: the worker is idle from the start
        real, added, started, ticks = jobcore.Worker.tick, [], [], []
        real_release, store = jobq.lock.Claim.release, []
        asked, adding = journey.Gate(), journey.Gate()

        def it_asks():
            """What another process is told about the store the moment this worker lets its claim go."""
            store.append(self.asking_for_the_store())

        def it_adds():
            rc, out, err = self.call("add", "--cwd", self.work, "--", "true")
            self.assertEqual(rc, 0, err)
            added.append(int(out.split()[-1]))

        def release(c):
            if os.path.basename(c.path) == "worker.lock":
                asked.once(it_asks)
            real_release(c)

        def tick(w):            # its first tick finds nothing to do; a job comes in right after it
            real(w)
            ticks.append(1)
            if len(ticks) > 500:
                raise SystemExit("the worker never came to the end of the job it was handed")
            adding.once(it_adds)

        def never(q):
            started.append(q)
            raise AssertionError("it started a worker while one held the claim")
        # work() installs its own stop handler; the scope hands SIGTERM back the way this test found it
        self.sb.on_signal(signal.SIGTERM, signal.getsignal(signal.SIGTERM))
        with mock.patch.object(jobcore.Worker, "tick", tick), redirect_stdout(io.StringIO()), \
                mock.patch.object(jobq.lock.Claim, "release", release), \
                mock.patch.object(jobcore, "_start_worker", never):
            jobq.cmd_work("default")
        # the claim went inside the scope that had read the store, so no other process could read it yet: a job
        # stored before that read is this worker's, and one stored after it finds the claim free
        self.assertEqual(store, ["busy"])
        self.assertEqual(started, [])               # the add found the worker up, as it was
        self.assertEqual(self.state(added[0]), "done")
        self.assertIsNone(jobcore.worker("default"))   # and it let the claim go on its way out


    def me(self) -> dict:
        """This process, as a worker's record names the process it is."""
        return {"pid": os.getpid(), "pstart": proc.read(os.getpid()).start}

    def test_a_worker_in_its_first_moment_is_looked_at_again(self):
        """A take locks the claim file and writes the record in the call after it, so for that moment the
        claim is held and names nothing. A verb that decided from that look would say the queue has no
        worker and start another, which would find the claim taken and bring nothing up. Nothing is decided
        from such a look: the answer is the holder's own record."""
        self.add("--hold")
        claim = jobq.lock.Claim.take(jobcore._worker_claim("default"))      # held, and naming nothing yet
        self.assertIsNotNone(claim, "something else holds the default worker claim")
        self.addCleanup(claim.release)
        looks, gate, real, started = [], journey.Gate(), jobcore.worker, []

        def records_itself(q):
            rec = real(q)
            looks.append(rec)
            # the worker writes its record exactly between the look that found none and the next one
            gate.once(lambda: claim.write({**self.me(), "waiting": None}))
            return rec
        with mock.patch.object(jobcore, "worker", records_itself), \
                mock.patch.object(jobcore, "_start_worker", lambda q: started.append(q) or mock.Mock(pid=-1)):
            w = jobq._worker_up("default", quiet=True)
        self.assertEqual((w.how, w.pid, gate.ran), (jobcore.UP, os.getpid(), 1))
        self.assertEqual(looks[0], {})      # the first look found a holder that named nothing
        self.assertEqual(started, [])       # and nothing was started while that worker held the claim

    def test_a_workers_first_moment_reads_as_starting_and_never_as_up(self):
        """The record a worker that was killed left is still in the claim file while the next worker takes
        it, since a take empties the file in the call after the lock. A verb that read it would name a pid
        that is gone as the queue's worker, and would say that nothing launches the queue's jobs while a
        worker is coming up to launch them. Nothing is decided from such a look: every verb says a worker is
        starting, a verb that starts workers starts none, and `--json` says the same."""
        self.add(cmd="sleep 30")            # queued, and a worker is coming up to launch it
        with open("/proc/sys/kernel/pid_max") as f:
            nobody = int(f.read())          # one past the largest pid: no process can have it
        self.left_behind(pid=nobody, pstart=1)
        said, data, gate, real = [], {}, journey.Gate(), jobq.lock.os.ftruncate
        started: list = []

        def asked_in_that_window():
            """What each verb says while the claim is held and the record is the killed worker's."""
            said.extend([self.call("status")[1], self.call("worker", "status")[1]])
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(jobq.cmd_work("default"), 0)   # a worker that finds the claim held says so too
            said.append(out.getvalue())
            said.append(self.call("worker", "stop")[2])
            # the two verbs that start workers: in this window they start none and say a worker is starting
            said.append(self.call("add", "--cwd", self.work, "--", "true")[2])
            said.append(self.call("worker", "start")[2])
            data["status"] = json.loads(self.call("status", "--json")[1])
            data["worker"] = json.loads(self.call("worker", "status", "--json")[1])

        def ftruncate(fd, length):
            # exactly between the lock and the write, with the killed worker's bytes still in the file
            gate.once(asked_in_that_window)
            return real(fd, length)
        with mock.patch.object(jobq.lock.os, "ftruncate", ftruncate), \
                mock.patch.object(jobcore, "STARTING_PAUSE", 0), \
                mock.patch.object(jobcore, "_start_worker", lambda q: started.append(q) or mock.Mock(pid=-1)):
            claim = jobq.lock.Claim.take(jobcore._worker_claim("default"), self.me())
            self.addCleanup(claim.release)
        self.assertEqual(gate.ran, 1)
        self.assertEqual(started, [])       # no verb started a worker beside the one holding the claim
        for out in said:
            self.assertNotIn("worker up", out)
            self.assertNotIn(str(nobody), out)      # never the pid of the worker that is gone
            self.assertIn("starting", out)
        self.assertIn("a worker is starting; it launches them", said[0])
        self.assertNotIn("nothing launches them", said[0])
        self.assertIn("so this worker is not the one", said[2])
        self.assertIn("ask again", said[3])
        self.assertIn("ask again", said[5])         # `worker start` in this window: exit 1, ask again
        # and what a JSON consumer reads of the same state
        self.assertEqual(data["status"]["starting"], ["default"])
        self.assertIsNone(data["status"]["workers"]["default"])
        self.assertEqual(data["worker"][0]["how"], jobcore.STARTING)
        self.assertIs(data["worker"][0]["up"], False)

    def test_a_start_that_brings_no_worker_up_is_told_to_the_verb_that_asked_out_loud(self):
        """A queue left with no worker is the one thing a verb must not swallow: an `add` that says it queued
        a job, and nothing to launch it, leaves a person waiting for an outcome that is not coming. So an ask
        that is not quiet says so, however the starts failed, and a quiet ask of a polling loop says nothing
        and gets the same answer back."""
        def never(q):
            raise TimeoutError("pp did not say it was ready within 60s")
        for failed, said in ((never, "did not say it was ready"),
                             (lambda q: mock.Mock(pid=-1), "the default worker did not come up")):
            with self.subTest(said=said):
                jobcore._none_came_up.clear()      # the hold-off the round before this one left
                out, err = io.StringIO(), io.StringIO()
                with mock.patch.object(jobcore, "_start_worker", failed), \
                        redirect_stdout(out), redirect_stderr(err):
                    loud = jobq._worker_up("default")
                self.assertEqual(loud.how, jobcore.DOWN)
                self.assertIn(said, err.getvalue())
                self.assertIn("worker.log", err.getvalue())
                jobcore._none_came_up.clear()
                qout, qerr = io.StringIO(), io.StringIO()
                with mock.patch.object(jobcore, "_start_worker", failed), \
                        redirect_stdout(qout), redirect_stderr(qerr):
                    quiet = jobq._worker_up("default", quiet=True)
                self.assertEqual(quiet.how, jobcore.DOWN)      # the same answer, and not a word about it
                self.assertEqual((qout.getvalue(), qerr.getvalue()), ("", ""))

    def test_worker_start_says_why_the_start_timed_out(self):
        """`worker start` is the one verb that asks quietly and then speaks for itself, so the reason comes
        back on the answer. It is the reason the worker's own log cannot carry: a worker that hung before it
        said it was ready wrote nothing, so a person sent to that log alone cannot tell a hang from a
        crash."""
        def never(q):
            raise TimeoutError("pp did not say it was ready within 60s")
        jobcore._none_came_up.clear()
        with mock.patch.object(jobcore, "_start_worker", never):
            rc, _out, err = self.call("worker", "start")
        self.assertEqual(rc, 1)
        self.assertIn("the default worker did not come up: pp did not say it was ready within 60s", err)
        self.assertIn("worker.log", err)

    def test_a_queue_whose_worker_came_up_is_not_held_off_the_next_start(self):
        """The hold-off is on a queue whose last round of starts brought none up, and it goes as soon as a
        worker is found up: a queue that recovers is started again at once."""
        jobcore._none_came_up["default"] = time.time()
        w = self.as_the_worker()                        # a worker is up now
        self.assertTrue(jobq._worker_up("default", quiet=True).up)
        self.assertNotIn("default", jobcore._none_came_up)
        self.assertEqual(w.queue, "default")

    def test_add_release_and_retry_wake_a_worker_that_is_up(self):
        """A verb that makes a job ready wakes the queue's worker, so the job it readied starts at the tick
        that wake brings forward and not at the end of the worker's period."""
        held = self.add("--hold", cmd="true")
        ended = self.add(cmd="true")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(ended) == "done")
        with jobcore.waking() as waits:        # this process is the worker: a wake lands in its own pipe
            claim = jobq.lock.Claim.take(jobcore._worker_claim("default"), w.record())
            self.assertIsNotNone(claim, "something else holds the default worker claim")
            self.addCleanup(claim.release)
            self.assertFalse(waits.wait(0), "a wake arrived before any verb sent one")
            for argv in (("add", "--cwd", self.work, "--", "true"), ("release", str(held)), ("retry", str(ended))):
                with self.subTest(argv[0]):
                    self.assertEqual(self.call(*argv)[0], 0)
                    self.assertTrue(waits.wait(0), f"{argv[0]} did not wake the worker that is up")
                    self.assertFalse(waits.wait(0), "one wake, taken once")


class TestAKillWithNoWorkerUp(QueueCase):
    """Only a worker ever holds a queue's worker claim, so a `pp queue kill` that finds no worker up makes
    sure one is up and that worker carries out the stop. Two kills, a kill on a timeout's stop and a kill with
    no worker up all end in the same place: the one worker carries out every stop.

    One thread does every step here. The worker the kill starts is this process, and the kill's own poll is
    what ticks it, so nothing waits for a clock and no second actor can carry a stop out."""

    def test_a_kill_with_no_worker_up_starts_one_and_the_job_ends_killed(self):
        """The kill writes its stop, finds nobody holding the claim, starts a worker and waits for the
        record. That worker signals the job, records how it ended, and launches the queue's queued jobs, as
        the worker an `add` starts does."""
        running = self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        self.assertEqual(self.state(running), "running")
        queued = self.add(cmd="true")
        self.assertIsNone(jobcore.worker("default"))
        with self.one_thread():
            rc, out, err = self.call("kill", str(running))
        self.assertEqual((rc, self.started), (0, ["default"]), err)
        self.assertIn("started the default worker", err)
        self.assertIn(f"killed {running} (stopped on SIGTERM)", out)
        self.assertEqual(jobstore.find(running)["reason"], "killed")
        self.assertEqual(worker_pid(), os.getpid())   # the worker it started is up ...
        self.run_until(self.worker, lambda: self.state(queued) == "done")    # ... and it launches the queue

    def test_two_kills_with_no_worker_up_both_end_through_one_worker(self):
        """The second kill runs inside the first one's wait. It finds the worker the first one started and
        wakes it, so one worker has both stops to carry out and both kills hear their own outcome."""
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "running"))
        self.assertIsNone(jobcore.worker("default"))
        second, other = journey.Gate(), []

        def the_second_kill():
            # once the first kill has the worker up, so that the second one finds it and wakes it
            second.once(lambda: other.append(self.call("kill", str(b), "--grace", "0")),
                        when=lambda: self.started)
        with self.one_thread(the_second_kill):
            rc, out, err = self.call("kill", str(a), "--grace", "0")
        self.assertEqual((rc, second.ran), (0, 1), err)
        self.assertEqual(self.started, ["default"])         # one worker, started by the first kill
        rc_b, out_b, err_b = other[0]
        self.assertEqual(rc_b, 0, err_b)
        self.assertNotIn("started the default worker", err_b)   # the second found that worker up
        for jid, said in ((a, out), (b, out_b)):
            self.assertIn(f"killed {jid}", said)
            self.assertEqual(jobstore.find(jid)["reason"], "killed")

    def test_a_kill_on_a_timeouts_stop_is_a_kill(self):
        """A timeout's stop sits on the record for its whole grace, and a `kill` of the job can land inside
        that window. The kill's ask becomes the stop (`merge_stop`), so the one worker ends the job as a
        kill: it is never retried, it fires no hook, and `show` says where the stop began."""
        f = self.sb.path("hook")
        j = self.add("--retries", "2", "--retry-delay", "0.05", "--timeout", "60", "--on-fail",
                     f"echo x >> {f}", cmd="trap '' TERM; while :; do sleep 0.05; done")
        jobq.worker_for("default").tick()
        with jobstore.txn() as s:       # its time ran out and the worker wrote the stop, as a timeout does
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "timeout", "timeout": True, "t": time.time(), "grace": 10.0}
            s.save(cur)
        self.addCleanup(proc.signal_group, jobcore._group_handle(cur), signal.SIGKILL)
        with self.one_thread():
            rc, out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual((rc, err.count("started the default worker")), (0, 1), err)
        self.assertIn(f"killed {j}", out)
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec.get("reason"), rec["attempt"]), ("failed", "killed", 1))
        self.assertIsNone(rec.get("history"))       # a job a kill has touched runs no second time
        self.assertIsNone(rec.get("hooks"))
        self.assertFalse(os.path.exists(f))
        self.assertEqual((rec["stop"]["by"], rec["stop"]["was"]), ("user", "timeout"))
        self.assertNotIn("timeout", rec["stop"])        # the merged stop is a kill's, not a timeout's
        # one stop carries one time, and it is the timeout's: the row says so instead of reading as the time
        # the user asked at, which was later
        self.assertIn("user, on the timeout's stop from", self.call("show", str(j))[1])

    def test_a_kill_whose_job_outlives_the_wait_says_no_worker_came_up(self):
        """What a person is told when the wait is over, the job is still running and no worker could be
        brought up: the queue has no worker now, nothing of pp's has gone out, and the log of the worker that
        never came up is where that is written. It never names a worker log as one that holds this job."""
        j = self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        self.assertEqual(self.state(j), "running")
        started: list = []
        with mock.patch.object(jobcore, "_start_worker", lambda q: started.append(q) or mock.Mock(pid=-1)), \
                mock.patch.object(jobq, "KILL_WAIT", 0.0):
            rc, _out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 1)
        self.assertIn(f"job {j} is still running", err)
        self.assertIn("no signal of pp's has gone out", err)
        self.assertIn("No worker of the default queue is up now", err)
        self.assertIn("worker.log", err)
        self.assertEqual(started, ["default"] * jobcore.WORKER_STARTS)     # it asked for one, each time

    def test_a_kill_whose_job_outlives_the_wait_says_what_went_out_for_it(self):
        """A queue with no worker now is not a stop nothing went out for: a worker can send the SIGTERM,
        record it and then go away. What went out is on the job's record, so the message says it from there
        instead of telling a person the opposite of what `show` would."""
        j = self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        with jobstore.txn() as s:               # a worker sent the SIGTERM and recorded it, then went away
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "ann", "t": time.time(), "grace": 30.0, "sent": time.time()}
            s.save(cur)
        with mock.patch.object(jobcore, "_start_worker", lambda q: mock.Mock(pid=-1)), \
                mock.patch.object(jobq, "KILL_WAIT", 0.0):
            rc, _out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 1)
        self.assertIn("the SIGTERM went out", err)
        self.assertNotIn("no signal of pp's has gone out", err)
        self.assertIn(f"pp queue show {j}", err)        # where the rest of what went out is
        self.assertIn("The default worker's log", err)  # the worker's, named as the worker's

    def test_a_kill_waiting_on_a_worker_that_cannot_come_up_does_not_start_one_every_pass(self):
        """The throttle is the construct's, so every loop that asks on every pass has it: a worker that takes
        the claim and dies in its first tick is started again only every START_EVERY, not ten times a second.
        The clock is the test's: each pass moves it on by a tenth of a second."""
        j = self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        started: list = []
        clock = [1000.0]

        def sleep(_s):
            clock[0] += 0.1
        with mock.patch.object(jobcore, "_start_worker", lambda q: started.append(clock[0]) or mock.Mock(pid=-1)), \
                mock.patch.object(jobq, "KILL_WAIT", 12.0), mock.patch.object(jobcore, "START_EVERY", 5.0), \
                mock.patch.object(jobq.time, "time", lambda: clock[0]), mock.patch.object(jobq.time, "sleep", sleep):
            rc, _out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 1)
        rounds = sorted({round(t - 1000.0) for t in started})
        self.assertEqual(rounds, [0, 5, 10], started)       # the verb's own ask, then one round every 5s
        self.assertLessEqual(len(started), 4 * jobcore.WORKER_STARTS)

    def test_a_round_of_starts_that_outlasts_the_interval_still_holds_the_next_one_off(self):
        """The interval is counted from the clock after the round, not before it: a start waits for the
        worker to say it is ready, so a round of them can take longer than START_EVERY. Counted from before,
        every pass after such a round would start another."""
        j = self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        started: list = []
        clock = [1000.0]

        def start(q):           # one start costs 4s of the test's clock, so a round of three costs 12s
            started.append(clock[0])
            clock[0] += 4.0
            return mock.Mock(pid=-1)

        def sleep(_s):
            clock[0] += 0.1
        with mock.patch.object(jobcore, "_start_worker", start), \
                mock.patch.object(jobq, "KILL_WAIT", 30.0), mock.patch.object(jobcore, "START_EVERY", 5.0), \
                mock.patch.object(jobq.time, "time", lambda: clock[0]), mock.patch.object(jobq.time, "sleep", sleep):
            rc, _out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 1)
        rounds = len(started) // jobcore.WORKER_STARTS
        self.assertEqual(len(started) % jobcore.WORKER_STARTS, 0, started)
        # 30s of waiting, 12s per round and 5s held off after each: three rounds, not one per pass
        self.assertEqual(rounds, 3, started)
        # the round that ran past the deadline is reported as soon as it is over, and the time said is the
        # time really waited: a round can take the wait past its 30s bound by the ready waits in it
        self.assertIn(f"job {j} is still running 34s after it was asked to stop", err)

    def test_a_wait_on_a_worker_that_cannot_come_up_does_not_start_one_every_pass(self):
        """`pp queue wait ID` run in the background is what an agent is told to do, and it has no timeout by
        default, so a pass that starts a worker it cannot bring up would churn for as long as the wait lasts.
        The throttle lives in `worker_up`, so this loop has it too."""
        j = self.add(cmd="true")            # queued: the wait asks for a worker on every pass
        started: list = []
        clock = [1000.0]

        def sleep(_s):
            clock[0] += 0.5                 # the wait's own --poll, as this test's clock sees it
        with mock.patch.object(jobcore, "_start_worker", lambda q: started.append(clock[0]) or mock.Mock(pid=-1)), \
                mock.patch.object(jobcore, "START_EVERY", 5.0), \
                mock.patch.object(jobq.time, "time", lambda: clock[0]), mock.patch.object(jobq.time, "sleep", sleep):
            rc, _out, err = self.call("wait", str(j), "--timeout", "20", "--poll", "0.5")
        self.assertEqual(rc, 3)             # it gave up at its timeout, with the job still queued
        rounds = sorted({round(t - 1000.0) for t in started})
        self.assertEqual(rounds, [0, 5, 10, 15, 20], started)
        self.assertIn("the default worker did not come up", err)     # and it said so, once

    def test_a_watch_whose_worker_cannot_come_up_says_so_once(self):
        """`watch` asks quietly on every pass, since the line about what holds a claim is not one of its
        events, so a queue no worker can come up for would leave it waiting forever without a word. The
        answer from a round of starts carries the reason, and the loop says it once, as an event line."""
        j = self.add(cmd="true")            # queued: the watch asks for a worker on every pass
        clock = [1000.0]

        def never(q):
            raise TimeoutError("pp did not say it was ready within 60s")

        def sleep(_s):
            clock[0] += 0.5                 # the watch's own --poll, as this test's clock sees it
        jobcore._none_came_up.clear()
        with mock.patch.object(jobcore, "_start_worker", never), \
                mock.patch.object(jobcore, "START_EVERY", 5.0), \
                mock.patch.object(jobq.time, "time", lambda: clock[0]), mock.patch.object(jobq.time, "sleep", sleep):
            rc, out, _err = self.call("watch", str(j), "--timeout", "20", "--poll", "0.5")
        self.assertEqual(rc, 3)             # it gave up at its timeout, with the job still queued
        said = [ln for ln in out.splitlines() if "did not come up" in ln]
        self.assertEqual(len(said), 1, out)
        self.assertIn("pp did not say it was ready within 60s", said[0])
        self.assertIn("worker.log", said[0])


class TestJobClaim(QueueCase):
    """One run of a job is held by that run's own wrapper, for exactly as long as it lives. So a held claim
    means a run is live, whatever the record says, and a free claim means the run is over."""

    def banners(self, jid) -> int:
        """How many times a job's log says an attempt was started."""
        with open(jobstore.find(jid)["log"]) as f:
            return len([ln for ln in f if jobq.LAUNCH_BANNER.match(ln)])

    def test_a_running_jobs_claim_is_held_whatever_its_record_says(self):
        """A record with nothing in it to signal is not a job that has ended: its claim says it runs, and the
        wrapper's own pid in that claim names it again."""
        j = self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        with jobstore.txn() as s:               # as if its worker had died before it recorded the process
            cur = s.job(j, must_be=("running",))
            for k in ("pid", "pgid", "pstart"):
                cur.pop(k, None)
            s.save(cur)
        self.assertIsNotNone(jobq.lock.Claim.holder(jobstore.run_claim("default", j)))
        jobq.worker_for("default").carry_out()
        self.assertEqual(self.state(j), "running")      # not lost, and not reported
        rec = jobstore.find(j)
        self.assertTrue(proc.alive(rec["pid"], rec["pstart"]))      # named again, from its claim
        self.assertEqual(rec["pgid"], rec["pid"])
        self.assertEqual(self.banners(j), 1)

    def test_a_job_whose_run_is_live_is_never_started_again(self):
        """The claim decides whether a run may start, so a job that is queued again while its own run is still
        live is not started a second time."""
        j = self.add(cmd="sleep 30")
        jobq.worker_for("default").tick()
        with jobstore.txn() as s:               # queued under its own run, as a lost race would leave it
            cur = s.job(j, must_be=("running",))
            cur["state"] = "queued"
            s.save(cur)
        jobq.worker_for("default").tick()
        self.assertEqual(self.state(j), "queued")       # its claim is held: nothing launched it
        self.assertEqual(self.banners(j), 1)
        with jobstore.txn() as s:               # as it was, so the test's own cleanup stops it
            cur = s.job(j, must_be=("queued",))
            cur["state"] = "running"
            s.save(cur)

    def test_a_running_job_with_no_claim_and_no_exit_code_is_lost(self):
        """Nothing holds its run claim, so its run is over, and it wrote no exit code, so nobody can say how
        it ended."""
        j = self.add()
        with jobstore.txn() as s:
            rec = s.job(j)
            rec.update(started=time.time(), state="running")
            s.save(rec)
        jobq.worker_for("default").carry_out()
        self.assertEqual((self.state(j), jobstore.find(j)["reason"]), ("failed", "lost"))

    def test_an_attempt_cannot_read_the_code_another_attempt_wrote(self):
        """Each attempt writes its exit code to a file of its own, named as the attempt starts and kept on the
        record, so one attempt's code is never read as another's and nothing has to be removed before a
        launch. `clean` takes them with the record."""
        m = os.path.join(self.work, "once")
        j = self.add("--retries", "1", "--retry-delay", "0.05", cmd=f"test -f {m} && exit 7; touch {m}; exit 1")
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "failed")
        rec = jobstore.find(j)
        first, second = rec["history"][0]["rcfile"], rec["rcfile"]
        self.assertNotEqual(first, second)
        self.assertEqual((rec["attempt"], rec["rc"]), (2, 7))
        self.assertEqual(jobcore.ended_on_its_own({"rcfile": first})[0], 1)     # attempt 1's code, still its own
        self.assertEqual(jobcore.ended_on_its_own(rec)[0], 7)
        # a record that has not run yet reads no code, whatever any attempt before it left on disk
        self.assertIsNone(jobcore.ended_on_its_own({k: v for k, v in rec.items() if k != "rcfile"}))
        self.assertEqual(self.call("clean", "-y")[0], 0)
        self.assertEqual([os.path.exists(p) for p in (first, second)], [False, False])

    def test_a_daemon_the_command_leaves_behind_does_not_keep_the_job_running(self):
        """The command reads /dev/null, not the claim, so neither it nor anything it leaves behind holds the
        run: the job ends when its own command ends, even with a process of its still in its group."""
        pidf = os.path.join(self.work, "daemon.pid")
        # the daemon writes its pid under another name and renames it, so the file is whole the moment it is
        # there: a `>` redirect creates and empties the file before the write, and the read can land in between
        j = self.add(cmd=f"sh -c 'echo $$ > {pidf}.part && mv {pidf}.part {pidf}; exec sleep 30' & exit 0")
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "done")
        journey.until(lambda: os.path.exists(pidf), timeout=10, what="the daemon's pid")
        with open(pidf) as f:
            dpid = int(f.read())
        self.addCleanup(journey.kill_pid, dpid)
        rec = jobstore.find(j)
        self.assertEqual(rec["rc"], 0)
        self.assertTrue(proc.alive(dpid), "the daemon is gone, so this proves nothing")
        self.assertIn(dpid, proc.members(rec["pgid"]))   # still in the job's group, and the job has ended


class TestTwoAtOnce(QueueCase):
    """Two actors on one job at once: a worker and a verb, two workers, a signal and the store. Each test runs the
    competing step at the exact point of the window (a patched step does it), so it fails every time without the fix
    it guards, on any machine."""

    @staticmethod
    def gated(name):
        """A job that runs until the test creates `name` in the work dir, then exits 0."""
        return f"while [ ! -f {name} ]; do sleep 0.02; done"

    def rc_file(self, jid):
        """The file this attempt of the job writes its exit code to, as its own record names it."""
        return jobstore.find(jid)["rcfile"]

    def test_a_ctrl_c_inside_a_change_lands_after_it(self):
        """A Ctrl-C that arrives while a transaction is open is delivered when it ends, so the change it was
        making is whole. It arrives before the record is written, so without the deferral the change would be
        left unmade."""
        j = self.add("--hold")
        real, gate = jobq.paths.write_json, journey.Gate()

        def write_json(path, data, **kw):
            if _is_job_file(path):
                # ... and the write below still has to happen
                gate.once(lambda: proc.raise_to_self(signal.SIGINT))
            real(path, data, **kw)
        with journey.interruptible(), mock.patch.object(jobq.paths, "write_json", write_json), \
                self.assertRaises(KeyboardInterrupt):
            self.call("release", str(j), "--no-start")
        self.assertEqual(gate.ran, 1)
        self.assertEqual(self.state(j), "queued")

    def test_a_ctrl_c_inside_a_change_of_several_records_lands_after_it(self):
        """`clean` removes a batch of records in one transaction. A Ctrl-C after the first of them is delivered
        when the scope ends, so the batch is whole: every record it named is gone, not some of them."""
        w = jobq.worker_for("default")
        ids = [self.add(cmd="true") for _ in range(3)]
        self.run_until(w, lambda: all(self.state(i) == "done" for i in ids))
        real, gate = os.remove, journey.Gate()

        def remove(path):
            out = real(path)
            if _is_job_file(path):
                # after the first record of the batch is gone
                gate.once(lambda: proc.raise_to_self(signal.SIGINT))
            return out
        with journey.interruptible(), mock.patch.object(jobq.os, "remove", remove), \
                self.assertRaises(KeyboardInterrupt):
            self.call("clean", "-y")
        self.assertEqual(gate.ran, 1)
        self.assertEqual([self.state(i) for i in ids], [None, None, None])

    def test_a_reaper_leaves_a_job_its_worker_is_starting(self):
        """A job whose run claim is held is being started or is running: a reaper that finds it running with
        no pid yet leaves it alone, since the claim says a run of it is live."""
        j = self.add(cmd="sleep 30")
        real = jobcore.Worker._spawn

        def spawn(w, job, gpu, claim):      # running, no pid yet: another reaper looks exactly now
            self.assertIsNotNone(jobq.lock.Claim.holder(jobstore.run_claim("default", j)))
            jobq.worker_for("default").carry_out()
            return real(w, job, gpu, claim)
        with mock.patch.object(jobcore.Worker, "_spawn", spawn):
            jobq.worker_for("default").tick()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec.get("attempt", 1)), ("running", 1))
        self.assertTrue(proc.alive(rec["pid"], rec["pstart"]))

    def test_a_note_added_while_a_job_starts_is_kept(self):
        j = self.add(cmd="sleep 30")
        real_init, real_read = jobq.Resources.__init__, jobq.proc.read
        gate = journey.Gate()

        def init(res, *a, **k):         # after the worker listed the queue, before it claims: a note comes in
            jobq.add_note(j, "before the claim", by="user")
            real_init(res, *a, **k)

        def read(pid):                  # its process started, its record not written yet: another note
            if pid != os.getpid():
                gate.once(lambda: jobq.add_note(j, "while it starts", by="user"))
            return real_read(pid)
        with mock.patch.object(jobq.Resources, "__init__", init), mock.patch.object(jobq.proc, "read", read):
            jobq.worker_for("default").tick()
        rec = jobstore.find(j)
        self.assertEqual(rec["state"], "running")
        self.assertEqual([n["text"] for n in rec.get("notes", [])], ["before the claim", "while it starts"])

    def test_a_kill_that_crosses_the_jobs_own_exit_is_recorded_once(self):
        """A job that ends on its own while it is being stopped keeps its own exit code, and that outcome is
        recorded by one actor: its hook fires once and one `wait` reports it once."""
        f = self.sb.path("hook")
        j = self.add("--on-done", f"echo x >> {f}", cmd=self.gated("go"))
        w = jobq.worker_for("default")
        w.tick()
        real, gate = jobcore.Worker.carry_out, journey.Gate()

        def it_exits():         # the job exits 0 of its own accord, and nothing is signalled until it has
            open(os.path.join(self.work, "go"), "w").close()
            journey.until(lambda: os.path.exists(self.rc_file(j)), timeout=10, what="the job's exit code")

        def serve(wk, *a, **kw):              # once the kill's stop is on the record, never before
            gate.once(it_exits, when=lambda: (jobstore.find(j) or {}).get("stop"))
            real(wk, *a, **kw)
        with mock.patch.object(jobcore.Worker, "carry_out", serve), self.one_thread():
            rc, out, _ = self.call("kill", str(j))      # the kill starts the worker that serves its stop
            self.assertEqual((rc, gate.ran), (0, 1))
            # whether the signal reaches the group before the job's last process goes is a race; either way
            # the kill reports the outcome the job itself wrote
            self.assertRegex(out, rf"job {j} (had already ended|ended on its own as it was being killed): "
                                  rf"done {j} rc=0")
            self.assertEqual(self.call("wait", str(j), "--timeout", "10", "--poll", "0.05")[0], 0)
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["rc"]), ("done", 0))        # its own exit code, not the kill's
        self.assertEqual([h["event"] for h in rec["hooks"]], ["done"])  # one actor fired it
        self.assertEqual(len(rec["reported"]), 1)                       # and one reported it
        journey.until(lambda: os.path.exists(f), timeout=10, what="the hook's output")
        with open(f) as fh:
            self.assertEqual(fh.read().count("x"), 1)

    def test_a_kill_whose_job_ends_before_the_stop_is_served_is_not_retried(self):
        """The window between the stop a kill writes and the signal that carries it out: a job that writes its
        own exit code inside it is ended there, with that code, and never runs again, whatever attempts it had
        left. One stop, one outcome, and the kill reports it instead of waiting out its deadline."""
        j = self.add("--retries", "1", "--retry-delay", "0.05", cmd=self.gated("go") + "; exit 7")
        w = jobq.worker_for("default")
        w.tick()
        open(os.path.join(self.work, "go"), "w").close()         # it exits 7 with no worker to reap it
        journey.until(lambda: jobq.lock.Claim.holder(jobstore.run_claim("default", j)) is None,
                      timeout=10, what="the job's run to be over")
        real, gate = jobcore.Worker.carry_out, journey.Gate()

        def pass_over(wk, *a, **kw):          # the first pass over it finds the stop written and its run over
            gate.once(lambda: journey.until(lambda: (jobstore.find(j) or {}).get("stop"), timeout=10,
                                            what="the kill's stop"))
            real(wk, *a, **kw)
        with mock.patch.object(jobcore.Worker, "carry_out", pass_over), mock.patch.object(jobq, "KILL_WAIT", 10.0):
            self.worker_beside(w)       # a worker from here on, and its first pass is the gated one
            rc, out, err = self.call("kill", str(j))
        self.assertEqual((rc, err, gate.ran), (0, "", 1))
        self.assertRegex(out, rf"job {j} had already ended: FAILED {j} rc=7")
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec["attempt"]), ("failed", 7, 1))
        self.assertIsNone(rec.get("reason"))        # the code it wrote itself, not a kill's
        self.assertIsNone(rec.get("history"))       # and no second attempt of a job a kill has touched

    def test_a_stop_whose_worker_died_before_it_recorded_the_signal_is_not_retried(self):
        """A worker killed between signalling a job's group and recording that it did leaves a stop with
        nothing marked on it and a run that is over. The next worker ends the job there: a stop is never
        retried, however little of it was recorded.

        What the record says went out is all the decision has, and it says nothing did, so the job is
        recorded as its own run ended rather than as a kill pp carried out."""
        j = self.add("--retries", "3", cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 10.0}   # written; nothing recorded yet
            s.save(cur)
        proc.signal_group(jobcore._group_handle(cur), signal.SIGKILL)          # the signal went out ...
        journey.until(lambda: jobq.lock.Claim.holder(jobstore.run_claim("default", j)) is None, timeout=10,
                      what="the job's run to be over")
        jobq.worker_for("default").carry_out()                                       # ... and the next worker reaps it
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec.get("reason"), rec["attempt"]), ("failed", "lost", 1))

    def test_a_stop_is_carried_out_while_the_run_is_live_whatever_code_is_written(self):
        """PP_QUEUE_RCFILE is in every job's environment, so the file an attempt's exit code goes in may hold
        a code the command itself wrote, with the command still running. An exit code counts only once the
        run is over, so the stop is carried out as for any other job: the SIGTERM goes out, the SIGKILL
        follows once the grace is up, and nothing waits out the job's own life. The run ends there, and the
        code that was written stands as the outcome."""
        j = self.add(cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        held = jobcore._group_handle(jobstore.find(j))
        self.addCleanup(proc.signal_group, held, signal.SIGKILL)
        with open(self.rc_file(j), "w") as f:       # the command's own doing: `echo 0 > "$PP_QUEUE_RCFILE"`
            f.write("0\n")
        self.assertIsNotNone(jobq.lock.Claim.holder(jobstore.run_claim("default", j)), "its run is already over")
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 0}
            s.save(cur)
        went: list = []
        with mock.patch.object(jobq.proc, "signal_group", lambda *a, **k: went.append(a[1]) or "alive"):
            w.carry_out()                                       # the SIGTERM, and the record says it went out
            self.assertEqual(went, [signal.SIGTERM])
            self.assertIsNotNone(jobstore.find(j)["stop"]["sent"])
            w.carry_out()                                       # its grace is up: the SIGKILL
        self.assertEqual(went, [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual((self.state(j), w.serving), ("running", 1))
        proc.signal_group(held, signal.SIGKILL)     # the signal reaches it, its run is over, and the code stands
        journey.until(lambda: jobq.lock.Claim.holder(jobstore.run_claim("default", j)) is None, timeout=10,
                      what="the job's run to be over")
        w.carry_out()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec.get("reason")), ("done", 0, None))

    def test_a_kill_whose_stop_leaves_the_record_does_not_wait_it_out(self):
        """A kill waits for the stop it asked for, not for a deadline. A stop can leave a record without the
        job having ended, since a timeout's retry takes it with it, and the kill then says where the job is.

        One thread does the steps in order. The retry happens where the kill makes sure the queue has a
        worker, once the stop is on the record and before the kill waits for it, which is the one window
        this is about. The worker holds the queue's claim and ticks not at all, so no second actor can carry
        the stop out or launch the retry while the kill reads the record."""
        j = self.add("--retries", "1", cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        group = jobcore._group_handle(jobstore.find(j))        # taken while it runs, so it names that run's group

        def nothing_is_left():
            """Its group goes, and the wrapper the worker holds is waited for, so this test leaves no child
            of its own to be collected unreaped."""
            proc.signal_group(group, signal.SIGKILL)
            p = w.children.pop(j, None)
            if p is not None:
                p.wait()
        self.addCleanup(nothing_is_left)
        self.as_the_worker(w)
        real_up, moved = jobcore.worker_up, journey.Gate()

        def back_to_its_queue():
            with jobstore.txn() as s:       # a retry, which is what takes the stop with it
                cur = s.job(j, must_be=("running",))
                self.assertIsNotNone(cur, "the kill wrote its stop on a job that was not running")
                cur.update(state="queued", attempt=2)
                cur.pop("stop", None)
                s.save(cur)

        def worker_up(q, **kw):
            moved.once(back_to_its_queue)
            return real_up(q, **kw)
        start = time.monotonic()
        with mock.patch.object(jobcore, "worker_up", worker_up), mock.patch.object(jobq, "KILL_WAIT", 20.0):
            rc, _, err = self.call("kill", str(j))
        self.assertEqual(moved.ran, 1, "the kill never made sure of the queue's worker")
        self.assertEqual(rc, 1)
        self.assertIn(f"job {j} no longer carries the stop it was asked for", err)
        self.assertIn("it is queued, attempt 2", err)
        self.assertLess(time.monotonic() - start, 10, "it waited for the deadline instead of the record")

    def test_a_kill_in_the_launch_window_is_carried_out_not_refused(self):
        """A job whose claim pp holds and whose process is not recorded yet has nothing in it to signal: the
        record names no group, and the claim is pp's own, with no pid written in it. A kill in that window is
        refused nothing. It writes its stop, and the worker that launched the job signals the group as soon as
        it knows it."""
        j = self.add(cmd="sleep 30")
        real, killing, window = jobcore.Worker._spawn, [], []

        def spawn(wk, job, gpu, claim):     # claimed, recorded as running, its process not recorded yet
            rec = jobstore.find(j)
            window.append((rec["state"], rec.get("pid"), jobq.lock.Claim.holder(jobstore.run_claim("default", j))))
            t = threading.Thread(target=lambda: killing.append(self.call("kill", str(j), "--grace", "0")),
                                 daemon=True)
            t.start()
            self.addCleanup(t.join, jobq.KILL_WAIT)
            journey.until(lambda: (jobstore.find(j) or {}).get("stop"), timeout=10, what="the kill's stop on the job")
            return real(wk, job, gpu, claim)

        w = self.as_the_worker()        # this worker holds the claim, so the kill wakes it and waits
        with mock.patch.object(jobcore.Worker, "_spawn", spawn):
            w.tick()
            self.run_until(w, lambda: self.state(j) == "failed")
            journey.until(lambda: killing, timeout=30, what="the kill to report")
        self.assertEqual(window, [("running", None, {})])
        rc, out, err = killing[0]
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"killed {j}", out)
        self.assertEqual(jobstore.find(j)["reason"], "killed")

    def test_a_recycled_group_id_is_not_signalled(self):
        other = journey.start(["sleep", "60"], group="own")     # someone else's group now
        self.addCleanup(other.close)
        j = self.add()
        with jobstore.txn() as s:
            rec = s.job(j)
            rec.update(started=time.time() - 60, pid=other.pid, pgid=other.pid, state="running",
                       pstart=proc.read(other.pid).start - 7,       # the pid was handed out again
                       boot=proc.boot_id(),                           # in this boot, so the pair is read
                       stop={"by": "timeout", "timeout": True, "t": time.time() - 30, "grace": 10,
                             "sent": time.time() - 30})
            s.save(rec)
        jobq.worker_for("default").carry_out()
        self.assertEqual(jobstore.find(j)["reason"], "timeout")
        self.assertIsNone(other.poll())

    def test_a_group_id_from_another_boot_names_nothing(self):
        """A pid and the start time read beside it name a process within one boot and no further, and a
        queue's state outlives a reboot: a machine that loses power with a stop on a job comes back up with
        that group id in use by a stranger. A record from another boot names nothing, so the job's run claim
        is the only thing that can name its group, and that claim is always this boot's."""
        other = journey.start(["sleep", "60"], group="own")     # whatever holds that group id now
        self.addCleanup(other.close)
        j = self.add()
        with jobstore.txn() as s:
            rec = s.job(j)
            rec.update(started=time.time() - 60, pid=other.pid, pgid=other.pid, state="running",
                       pstart=proc.read(other.pid).start,       # the pair names this very process ...
                       boot="00000000-0000-0000-0000-000000000000",     # ... but it was read before a reboot
                       stop={"by": "user", "t": time.time() - 30, "grace": 0})
            s.save(rec)
        self.assertIsNone(jobcore._group_handle(jobstore.find(j)))
        jobq.worker_for("default").carry_out()      # it ends the job, and what it sweeps is its group
        self.assertIsNone(other.poll(), "it signalled a process group that is not the job's")
        # no signal of pp's went out for the stop, so the job is recorded as its own run ended: with nothing
        self.assertEqual(jobstore.find(j).get("reason"), "lost")

    def test_a_log_removed_as_it_is_opened(self):
        j = self.add(cmd="echo hello")
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "done")
        log, real = jobstore.find(j)["log"], open

        def opener(path, *a, **k):      # `clean` removes it just before `log` opens it
            if path == log and os.path.exists(log):
                os.remove(log)
            return real(path, *a, **k)
        with mock.patch("builtins.open", opener):
            rc, _, err = self.call("log", str(j))
        self.assertEqual(rc, 1)
        self.assertIn("has no log yet", err)

    def test_wait_restarts_the_worker_of_a_running_job(self):
        j = self.add(cmd="true")
        w = jobq.worker_for("default")
        w.tick()
        journey.until(lambda: os.path.exists(self.rc_file(j)), timeout=10, what="the job's exit code")
        del w                                       # it died before recording the end
        started = []

        def brings_one_up(q, quiet=False, wake=False):      # stands in for the worker it starts
            started.append(q)
            jobq.worker_for(q).carry_out()
            return jobcore.Holder(jobcore.UP, rec={"pid": os.getpid()})
        with mock.patch.object(jobcore, "worker_up", brings_one_up):
            rc, out, _ = self.call("wait", str(j), "--timeout", "5", "--poll", "0.05")
        self.assertEqual((rc, started[:1]), (0, ["default"]))
        self.assertIn(f"done {j}", out)

    def test_a_probe_does_not_grant_a_unit_twice_before_it_shows_the_job(self):
        self.resources('nodes = { probe = "echo 1", every = "0s" }')      # settle left to its default
        self.config("slots = 3", q="a")
        self.config("slots = 3", q="b")
        a = self.add("--need", "nodes=1", cmd="sleep 30", q="a")
        b = self.add("--need", "nodes=1", cmd="sleep 30", q="b")
        wb = jobq.worker_for("b")
        jobq.worker_for("a").tick()
        wb.tick()                       # a fresh reading still says 1 free: a has not taken its node yet
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("1 of it launched since", self.why(wb))

    def test_an_empty_config_launches_nothing(self):
        self.config("slots = 1")
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        open(os.path.join(self.cfg, "config.toml"), "w").close()     # mid-save
        w = jobq.worker_for("default")
        with self.assertRaises(config.ConfigError):
            w.tick()                    # work() reports it as a bad config; nothing launches
        self.assertEqual((self.state(a), self.state(b)), ("queued", "queued"))
        self.assertIn("is empty", self.call("status")[1])
        self._write_config()
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))     # slots = 1 again

    def test_a_worker_told_to_stop_launches_nothing_new(self):
        j = self.add()
        w = jobq.worker_for("default")
        w.stopping = lambda: True
        w.tick()
        self.assertEqual(self.state(j), "queued")


class TestTheWorkersWait(QueueCase):
    """What jobcore.waking promises: the wait between a worker's ticks ends the moment a wake arrives, and a wake
    sent while a tick runs is taken by the next wait rather than lost."""

    def test_a_wake_ends_the_wait_at_once(self):
        """The wait is for the period, or until a wake, whichever comes first."""
        with jobcore.waking() as waits:
            self.sb.ticker(lambda: proc.raise_to_self(jobcore.WAKE), period=0.01, name="waker")
            start = time.monotonic()
            self.assertTrue(waits.wait(30))         # it says a signal ended it
            self.assertLess(time.monotonic() - start, 5, "the wake did not end the wait")

    def test_a_wake_sent_while_a_tick_runs_is_not_lost(self):
        """A wake that arrives before the wait begins is what a kill sends while the worker is mid-tick. It
        ends the next wait at once, so no stop waits a whole period for the tick after it."""
        with jobcore.waking() as waits:
            proc.raise_to_self(jobcore.WAKE)           # as a tick runs, before any wait
            start = time.monotonic()
            self.assertTrue(waits.wait(30))
            self.assertLess(time.monotonic() - start, 5, "the wake was lost")
            self.assertFalse(waits.wait(0.05))      # one wake, one wait: the next one waits its period out

    def test_a_kill_wakes_the_queues_worker_and_waits_for_the_job_alone(self):
        """A kill that finds a worker holding the claim wakes it, so the stop is served in the tick that wake
        ends the wait for, and not when the queue's poll comes round. The worker's period is short while that
        stop is in flight, so the outcome is recorded as soon as the job is gone: the whole kill costs the
        job's own exit time, whatever the poll is."""
        j = self.add(cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        woke: list = []
        with jobcore.waking() as waits:
            claim = jobq.lock.Claim.take(jobcore._worker_claim("default"), w.record())
            self.assertIsNotNone(claim, "something else holds the default worker claim")

            def loop():         # work()'s own loop: a wait of ten seconds that only a wake or a stop shortens
                woke.append(waits.wait(jobpolicy.STOP_PERIOD if w.serving else 10))
                w.tick()
            t = self.sb.ticker(loop, period=0, name="worker")
            start = time.monotonic()
            try:
                rc, out, _ = self.call("kill", str(j), "--grace", "1")
            finally:
                proc.raise_to_self(jobcore.WAKE)       # end the wait this loop is in, then stop it
                t.stop()
                claim.release()
        self.assertEqual(rc, 0)
        self.assertIn(f"killed {j} (stopped on SIGTERM)", out)
        self.assertIn(True, woke, "no wake reached the worker")
        self.assertLess(time.monotonic() - start, 5, "the kill waited for a loop period, not for the job")

    def test_a_worker_whose_queue_has_a_stop_in_flight_waits_a_short_period(self):
        """A stop in flight is the one thing that shortens a worker's own period: it ticks until the stop is
        carried out, so the outcome is recorded about as soon as the job ends."""
        j = self.add(cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual(w.serving, 0)
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 300.0}
            s.save(cur)
        w.carry_out()
        self.assertEqual(w.serving, 1)              # work() then waits STOP_PERIOD, not the queue's poll
        self.assertLess(jobpolicy.STOP_PERIOD, 2.0)
        self.run_until(w, lambda: self.state(j) == "failed")
        self.assertEqual(w.serving, 0)

    def test_a_kill_that_waits_wakes_the_worker_on_every_pass(self):
        """A kill waiting for a stop hands it to the queue's worker on every pass of its own wait, and a wake
        is how it reaches one. So the worker's next tick is that pass, whatever its period and whatever is
        left to carry out: once the SIGKILL has gone out the reap is still one pass away, and not one poll.

        The kill is the real one. This process is the queue's worker, so each wake lands in its own pipe, and
        the pause at the end of each pass is where this counts them. The worker carries the stop out from the
        third pass on, so the kill ends the way it does for a person."""
        j = self.add(cmd="sleep 30")
        w = self.as_the_worker()
        w.tick()
        self.assertEqual(self.state(j), "running")
        self.assertFalse(self.waits.wait(0), "a wake arrived before the kill sent one")
        woke: list = []

        def sleep(_s):
            # the pause at the end of one pass of the kill's wait, spent on the wakeup pipe: it ends the
            # moment that pass's wake arrives, and says whether one did
            woke.append(self.waits.wait(0.05))
            if len(woke) >= 3:
                w.carry_out()   # from the third pass: the SIGTERM, then the reap and the outcome
        with mock.patch.object(jobq.time, "sleep", sleep):
            rc, out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 0, err)
        self.assertIn(f"killed {j}", out)
        self.assertEqual(woke[:3], [True, True, True], woke)    # every pass of the wait sent a wake

    def test_a_pass_of_a_wait_wakes_the_worker_that_is_up(self):
        """The construct every such pass goes through, on its own: it asks whether a worker is up and wakes
        the one that is, so three passes are three wakes and not one."""
        self.add("--hold", cmd="true")
        w = self.as_the_worker()
        self.assertFalse(self.waits.wait(0), "a wake arrived before any pass sent one")
        for pass_ in range(3):
            self.assertTrue(jobq._worker_up("default", quiet=True, wake=True).up)
            self.assertTrue(self.waits.wait(0), f"pass {pass_ + 1} sent no wake")
        self.assertEqual(w.queue, "default")

    def test_a_timeout_nobody_waits_for_lets_the_worker_wait_at_its_poll(self):
        """The short period goes with the wake: a verb waiting for a stop asks for a tick on every pass of
        its own wait, so the worker ticks with it. A timeout is a stop the worker wrote itself and nobody
        waits for, so it waits its poll out rather than taking the one store lock ten times a second until
        the job is gone."""
        self.config("slots = 3", "poll = 5")        # the queue's own poll: the worker re-reads it every loop
        j = self.add("--timeout", "60", cmd="sleep 30")
        jobq.worker_for("default").tick()                   # running, under no claim of its own
        self.started_earlier(j, 120)                    # and its time is up, so the next tick writes the stop
        asked, real = [], jobcore.Wait.wait
        # work() installs its own stop handler; the scope hands SIGTERM back the way this test found it
        self.sb.on_signal(signal.SIGTERM, signal.getsignal(signal.SIGTERM))

        def wait(self_, seconds):
            asked.append(seconds)
            if len(asked) == 2:
                proc.raise_to_self(signal.SIGTERM)      # the worker stops after the second wait it asks for
            return real(self_, 0)
        with mock.patch.object(jobcore.Wait, "wait", wait), redirect_stdout(io.StringIO()):
            self.assertEqual(jobq.cmd_work("default"), 0)
        self.assertEqual(asked, [5.0, 5.0])             # the queue's poll, never STOP_PERIOD
        self.assertEqual((jobstore.find(j)["stop"] or {}).get("by"), "timeout")
        self.assertLess(jobpolicy.STOP_PERIOD, 5.0)

    def test_a_stop_whose_sigkill_is_out_is_not_in_flight(self):
        """Once the SIGKILL is out there is nothing left to carry out for that stop, only a reap. So the
        worker goes back to its poll instead of holding itself at ten passes a second: a job the kernel is
        slow to end -- an uninterruptible read, a hung driver call -- would otherwise take the one store
        lock ten times a second for as long as it took to die, and every other pp process would wait on it.
        The reap sees the run claim go at the queue's poll, or at the next pass of a verb that is waiting for
        the stop, which wakes this worker each time."""
        j = self.add(cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        now = time.time()
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": now, "grace": 300.0, "sent": now}
            s.save(cur)
        w.carry_out()
        self.assertEqual(w.serving, 1)      # inside its grace: the SIGKILL is still to go out
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"]["kill9"] = now
            s.save(cur)
        w.carry_out()
        self.assertEqual(w.serving, 0)      # out: nothing more goes out for it, so nothing is in flight
        self.assertEqual(self.state(j), "running")


# what has gone out for a stop, and whether pp was refused it: every state `_send` can leave one in
SENT = ("unsent", "sent", "sent-kill9", "denied", "sent-denied")
# and the stop states a record can hold: who asked crossed with those, and no stop at all
STOPS = ("none",) + tuple(f"{by}-{out}" for by in ("kill", "timeout") for out in SENT)


class TestTheDecision(unittest.TestCase):
    """`jobpolicy.decide` is the one place that says what is to be done about a running job, and `jobpolicy.merge_stop`
    the one place that says what a second stop on it comes to. Both are pure, so this table needs no process,
    no thread and no clock.

    Every combination of the facts is a row: the run's claim held or free, no exit code or one that is 0 or
    not (written while the run was live, or after it), the eleven states a stop on the record can be in (who
    asked for it crossed with what has gone out for it, and no stop), the grace up or not, the attempt number
    (its first, and one far enough along that the delay would pass RETRY_DELAY_CAP), attempts left or none,
    the job's time up or not, a signal its child died of or none, and a --retry-if that matches or one that
    cannot. Each row's expected action is written once below, against the rule `pp help queue` states it as,
    in the order that help states them; a row no rule holds for is a failure, so a new fact or a new action
    cannot go in without saying what it means everywhere.

    Each rule is written on the facts a stop carries, namely whether a kill asked for it, whether a signal
    of pp's went out and whether pp was refused, and never on the name of a state, so a state nobody thought
    of cannot slip past a rule that matched its name."""

    NOW = 2000.0
    GRACE = 10.0
    JOB_GRACE = 7.0         # the job's own, which a timeout's stop takes and no constant of pp's does
    RC_AT = 1990.0          # when the code was written, which is when the job really ended
    DELAY = 20.0

    def stop(self, kind: str, grace_up: bool) -> dict | None:
        """The stop on the record in the state `kind` names: a kill's is `by` whoever asked, a worker's own
        carries `timeout`, and what has gone out for it is what the rest of the name says."""
        if kind == "none":
            return None
        by, out = kind.split("-", 1)
        went = out.split("-")           # the field names, so "unsent" is not read as a sent one
        st = {"by": "timeout" if by == "timeout" else "user", "t": self.NOW - 60, "grace": self.GRACE}
        if by == "timeout":
            st["timeout"] = True
        if "sent" in went:
            st["sent"] = self.NOW - (self.GRACE + 5 if grace_up else 1)
        if "kill9" in went:
            st["kill9"] = self.NOW - 1
        if "denied" in went:
            st["denied"] = self.NOW - 30
        return st

    @staticmethod
    def facts_of(kind: str) -> dict:
        """The facts the stop in that state carries, which is all any rule below reads: whether there is one,
        whether a kill asked for it (and not a worker's own timeout), whether a signal of pp's went out, and
        whether pp was refused the job's group."""
        if kind == "none":
            return {"any": False, "asked": False, "sent": False, "kill9": False, "denied": False, "out": False}
        by, out = kind.split("-", 1)
        went = out.split("-")           # the field names, so "unsent" is not read as a sent one
        sent, kill9 = "sent" in went, "kill9" in went
        return {"any": True, "asked": by != "timeout", "sent": sent, "kill9": kill9,
                "denied": "denied" in went, "out": sent or kill9}

    # the first attempt, and one far enough along that doubling the delay per attempt would pass the cap
    ATTEMPTS = (1, 20)

    def rows(self):
        for held in (True, False):
            for rc in (None, 0, 7):
                for kind in STOPS:
                    for grace_up in (True, False):
                        for attempt in self.ATTEMPTS:
                            for left in (True, False):
                                for due in (True, False):
                                    for sig in (None, 9):
                                        for pat in (None, "nothing in this log"):
                                            for late in (True, False):
                                                yield {"held": held, "rc": rc, "stop": kind, "late": late,
                                                       "grace_up": grace_up, "attempt": attempt, "left": left,
                                                       "due": due, "sig": sig, "retry_if": pat,
                                                       **self.facts_of(kind)}

    def case(self, r: dict):
        """The job record and the facts one row stands for."""
        # attempts are left while the attempt number is no further than `retries`, so the row says both
        job = {"id": 1, "queue": "default", "state": "running", "tag": "t", "attempt": r["attempt"],
               "retries": r["attempt"] + 2 if r["left"] else 0, "retry_delay": self.DELAY,
               "retry_if": r["retry_if"], "timeout": 60.0, "started": self.started_at(r),
               "grace": self.JOB_GRACE, "on_fail": "echo failed", "on_done": "echo done"}
        st = self.stop(r["stop"], r["grace_up"])
        if st is not None:
            job["stop"] = st
        return job, jobpolicy.Facts(held=r["held"], rc=r["rc"], rc_at=self.rc_at(r), sig=r["sig"], now=self.NOW)

    # -- what each row's action is -------------------------------------------------------------------------
    def went(self, r: dict):
        """When the first thing went out for this row's stop, or None where nothing has."""
        st = self.stop(r["stop"], r["grace_up"]) or {}
        return st.get("sent") or st.get("kill9")

    def rc_at(self, r: dict):
        """When the attempt wrote its exit code: a second after the first thing went out for its stop, or a
        second before it, which is what decides whose outcome that code is (I10)."""
        if r["rc"] is None:
            return None
        went = self.went(r)
        return self.RC_AT if went is None else (went + 1 if r["late"] else went - 1)

    def late(self, r: dict) -> bool:
        """Whether the code this attempt wrote came after its stop went out, which an attempt that wrote none
        counts as, since there is no code of its own to stand instead."""
        return r["rc"] is None or (self.went(r) is not None and r["late"])

    @staticmethod
    def sweep(r: dict) -> bool:
        """Whether anything left in the job's group goes with it: it wrote no exit code of its own, or a stop
        was asked of it and not refused."""
        return r["rc"] is None or (r["any"] and not r["denied"])

    @staticmethod
    def own(r: dict) -> str | None:
        """How the job's own run ended where it wrote no exit code: the signal it died of, or lost."""
        return None if r["rc"] is not None else (f"signal {r['sig']}" if r["sig"] else "lost")

    def ends(self, rc, reason, ended, sweep, hook, not_retried=None) -> jobpolicy.Action:
        return jobpolicy.Action(jobpolicy.ENDED_AS, state="done" if rc == 0 and not reason else "failed", rc=rc,
                           reason=reason, hook=hook, ended=ended, sweep=sweep, not_retried=not_retried)

    def started_at(self, r: dict) -> float:
        """When the attempt this row is about started, which is what the row's own timeout rests on."""
        return self.NOW - (120 if r["due"] else 1)

    def failure(self, r, rc, reason, ended, sweep) -> jobpolicy.Action:
        """A failure of the job's own: another attempt where it has one left and --retry-if matches, else the
        end of it, which says why it was not retried. The delay doubles per attempt, up to RETRY_DELAY_CAP.

        A retry carries the entry the attempt that ended leaves in `history` and the keys that attempt's own
        fields are, so the store writes what the decision said and works nothing out again."""
        if r["left"] and r["retry_if"] is None:
            at = self.NOW if ended is None else ended
            delay = min(self.DELAY * 2 ** (r["attempt"] - 1), jobpolicy.RETRY_DELAY_CAP)
            was = {"attempt": r["attempt"], "rc": rc, "reason": reason, "started": self.started_at(r),
                   "ended": at, "seconds": at - self.started_at(r),
                   **dict.fromkeys(jobpolicy.KEPT)}     # the files and names this attempt had, and none here
            return jobpolicy.Action(jobpolicy.AGAIN, rc=rc, reason=reason, ended=at, sweep=sweep,
                               attempt=r["attempt"] + 1, not_before=at + delay, history=was,
                               clear=jobpolicy.ATTEMPT)
        why = reason or f"exit {rc}"
        nr = f"neither the reason ({why}) nor the log tail matches /{r['retry_if']}/" if r["left"] else None
        return self.ends(rc, reason, ended, sweep, "failed", nr)

    def expectations(self) -> list:
        """(the rule, the rows it holds for, the action it expects), in the order `pp help queue` states the
        rules. The first rule that holds for a row is that row's expectation, and each rule is a sentence of
        that help (test_every_rule_is_a_sentence_of_the_help)."""
        return [
            # while a run of it is live, its exit code counts for nothing: the command itself can write that
            # file, so what is done about the job is whatever its stop asks for
            ("A signal pp was refused is recorded as denied, and nothing more goes out for that stop.",
             lambda r: r["held"] and r["denied"],
             lambda r: jobpolicy.Action(jobpolicy.NOTHING)),
            ("A stop nobody has signalled yet goes out as SIGTERM to the job's process group.",
             lambda r: r["held"] and r["any"] and not r["sent"],
             lambda r: jobpolicy.Action(jobpolicy.TERM)),
            ("A stop whose grace is up is escalated to SIGKILL, once.",
             lambda r: r["held"] and r["sent"] and not r["kill9"] and r["grace_up"],
             lambda r: jobpolicy.Action(jobpolicy.KILL9)),
            ("A stop inside its grace, or SIGKILLed already, is waited for.",
             lambda r: r["held"] and r["any"],
             lambda r: jobpolicy.Action(jobpolicy.NOTHING)),
            ("A job with no stop whose own timeout has run out gets a timeout's stop.",
             lambda r: r["held"] and not r["any"] and r["due"],
             lambda r: jobpolicy.Action(jobpolicy.TIME_UP, t=self.NOW, grace=self.JOB_GRACE)),
            ("A job with no stop whose time is not up is left as it is.",
             lambda r: r["held"] and not r["any"],
             lambda r: jobpolicy.Action(jobpolicy.NOTHING)),
            # its run is over, so its exit code is the command's last word and this is where it ends
            ('A job a kill\'s signal stopped is recorded "killed" and fires no hook; an exit code the attempt '
             "wrote after that signal went out is kept as the attempt's code.",
             lambda r: not r["held"] and r["asked"] and r["out"] and self.late(r),
             lambda r: self.ends(r["rc"], "killed", self.rc_at(r), self.sweep(r), None)),
            ("A job that wrote its own exit code before a kill's signal went out, or that a kill wrote a stop "
             "for that no signal of pp's ever reached, ends with its own outcome, the exit code it wrote or "
             "the signal it died of, and fires the hook that outcome owes.",
             lambda r: not r["held"] and r["asked"],
             lambda r: self.ends(r["rc"], self.own(r), self.rc_at(r), self.sweep(r),
                                 "done" if r["rc"] == 0 else "failed")),
            ("An exit code of 0 is done, and on_done fires.",
             lambda r: not r["held"] and r["rc"] == 0 and not self.late(r),
             lambda r: self.ends(0, None, self.rc_at(r), self.sweep(r), "done")),
            ("A timeout's stop pp signalled or was refused is a timeout, which may be retried; an exit code "
             "the attempt wrote after that stop went out is kept as the attempt's code, and on a final "
             "timeout on_fail fires.",
             lambda r: not r["held"] and self.late(r) and (r["out"] or r["denied"]),
             lambda r: self.failure(r, r["rc"], "timeout", self.rc_at(r), self.sweep(r))),
            ("Anything else is the job's own failure, the exit code it wrote or the signal it died of, "
             "retried while an attempt is left and --retry-if matches it, and on_fail fires on the last.",
             lambda r: not r["held"],
             lambda r: self.failure(r, r["rc"], self.own(r), self.rc_at(r), self.sweep(r))),
        ]

    # -- the table ------------------------------------------------------------------------------------------
    def test_every_combination_of_the_facts_has_one_action(self):
        rules = self.expectations()
        seen, n = set(), 0
        for r in self.rows():
            n += 1
            said = [(what, then) for what, when, then in rules if when(r)]
            self.assertTrue(said, f"no rule says what to do with {r}")
            what, then = said[0]
            seen.add(what)
            job, facts = self.case(r)
            self.assertEqual(jobpolicy.decide(job, facts), then(r), f"{what}: {r}")
        self.assertEqual(n, 8448)                               # 2*3*11*2*2*2*2*2*2*2 rows
        self.assertEqual(seen, {what for what, _w, _t in rules})    # and every rule is a row of its own
        # both sides of the delay's cap are a row here, so neither arm can go quiet under a later constant
        self.assertGreater(self.DELAY * 2 ** (max(self.ATTEMPTS) - 1), jobpolicy.RETRY_DELAY_CAP)
        self.assertLess(self.DELAY * 2 ** (min(self.ATTEMPTS) - 1), jobpolicy.RETRY_DELAY_CAP)

    def test_every_rule_is_a_sentence_of_the_help(self):
        """`pp help queue` is the one statement of how a job ends, and this table is the same rules in the
        order they are tried. So each rule here is a sentence of that help, and their order there is the
        order here: neither copy can be changed on its own."""
        said = _sentences(queue_cmd.__doc__)
        rules = [what for what, _when, _then in self.expectations()]
        for what in rules:
            self.assertIn(what, said, "`pp help queue` does not state this rule")
        at = [said.index(what) for what in rules]
        self.assertEqual(at, sorted(at), "`pp help queue` states these rules in another order than the table")

    def test_the_decision_writes_nothing_and_reads_no_clock(self):
        """It is a function of the record and the facts alone: the same row twice gives the same action, and
        neither the record nor the facts come back changed."""
        for r in self.rows():
            job, facts = self.case(r)
            before = json.dumps(job, sort_keys=True)
            first = jobpolicy.decide(job, facts)
            self.assertEqual(json.dumps(job, sort_keys=True), before, r)
            self.assertEqual(jobpolicy.decide(job, facts), first, r)

    def test_a_job_with_no_hook_owes_none(self):
        """The hook a row names is the job's own: a job that carries no on_fail owes nothing when it fails,
        and nothing has to be cleared from its record."""
        bare = {"id": 1, "queue": "default", "state": "running", "attempt": 1, "retries": 0}
        over = jobpolicy.Facts(held=False, rc=7, rc_at=self.RC_AT, now=self.NOW)
        self.assertIsNone(jobpolicy.decide(bare, over).hook)
        self.assertEqual(jobpolicy.decide({**bare, "on_fail": "echo x"}, over).hook, "failed")
        self.assertEqual(jobpolicy.decide({**bare, "on_fail": "none"}, over).hook, None)     # `none` is no hook
        ok = jobpolicy.Facts(held=False, rc=0, rc_at=self.RC_AT, now=self.NOW)
        self.assertIsNone(jobpolicy.decide(bare, ok).hook)
        self.assertEqual(jobpolicy.decide({**bare, "on_done": "echo x"}, ok).hook, "done")

    def test_an_asker_called_timeout_is_still_a_kill(self):
        """A worker's own stop carries `timeout`, so what a stop is never rests on the name of whoever asked:
        a session or a person called `timeout` writes a kill's stop, and the job ends as a kill's."""
        job = {"id": 1, "queue": "default", "state": "running", "attempt": 1, "retries": 3,
               "retry_delay": self.DELAY, "on_fail": "echo failed",
               "stop": {"by": "timeout", "t": self.NOW - 60, "grace": self.GRACE, "sent": self.NOW - 60}}
        over = jobpolicy.Facts(held=False, rc=None, sig=9, now=self.NOW)
        a = jobpolicy.decide(job, over)
        self.assertEqual((a.do, a.reason, a.hook), (jobpolicy.ENDED_AS, "killed", None))      # never retried
        worker_wrote_it = {**job, "stop": {**job["stop"], "timeout": True}}
        self.assertEqual(jobpolicy.decide(worker_wrote_it, over).do, jobpolicy.AGAIN)      # a timeout may be retried

    def test_a_code_written_after_the_stop_went_out_is_that_stops_outcome(self):
        """A stop decides the reason (I10): an exit code the attempt wrote after its stop went out is that
        stop's outcome, killed or timeout, with the code kept as the attempt's own. A container runner writes
        the code of a container the stop command ended, so without this rule a kill or a timeout of a
        container job would read as the job's own failure.

        A code the attempt wrote before anything went out for that stop is its own outcome, as it was: it
        ended on its own as the stop arrived, and it fires the hook that outcome owes."""
        sent = self.NOW - 50
        timed = {"id": 1, "queue": "default", "state": "running", "attempt": 1, "retries": 3,
                 "retry_delay": self.DELAY, "retry_if": "^timeout$", "on_fail": "echo failed",
                 "on_done": "echo done",
                 "stop": {"by": "timeout", "timeout": True, "t": self.NOW - 60, "grace": self.GRACE,
                          "sent": sent}}
        killed = {**timed, "stop": {"by": "ann", "t": self.NOW - 60, "grace": self.GRACE, "sent": sent}}

        def decide(job, rc, rc_at):
            return jobpolicy.decide(job, jobpolicy.Facts(held=False, rc=rc, rc_at=rc_at, now=self.NOW))
        # after the stop went out: the stop's outcome, and the code is kept
        late = decide(timed, 137, sent + 1)
        self.assertEqual((late.do, late.state, late.rc, late.reason), (jobpolicy.AGAIN, None, 137, "timeout"))
        zero = decide(timed, 0, sent + 1)       # a runtime that exits 0 for a container it was told to end
        self.assertEqual((zero.do, zero.rc, zero.reason), (jobpolicy.AGAIN, 0, "timeout"))
        kill = decide(killed, 137, sent + 1)
        self.assertEqual((kill.do, kill.state, kill.rc, kill.reason, kill.hook),
                         (jobpolicy.ENDED_AS, "failed", 137, "killed", None))
        # before it: the attempt's own outcome, and the hook that outcome owes
        own = decide(timed, 0, sent - 1)
        self.assertEqual((own.do, own.state, own.reason, own.hook), (jobpolicy.ENDED_AS, "done", None, "done"))
        seven = decide(timed, 7, sent - 1)
        self.assertEqual((seven.do, seven.rc, seven.reason), (jobpolicy.ENDED_AS, 7, None))
        self.assertIn("exit 7", seven.not_retried)      # and /^timeout$/ is not what that failure is
        mine = decide(killed, 7, sent - 1)
        self.assertEqual((mine.do, mine.rc, mine.reason, mine.hook),
                         (jobpolicy.ENDED_AS, 7, None, "failed"))
        # and an attempt that wrote no code of its own is the stop's outcome, with no code to keep
        none = decide(timed, None, None)
        self.assertEqual((none.do, none.rc, none.reason), (jobpolicy.AGAIN, None, "timeout"))

    def test_a_second_stop_merges_into_the_one_on_the_record(self):
        """One stop per run of a job: what went out is all the decision and the waiting verb have to go on, so
        a second ask keeps `t`, `sent` and `kill9` and only shortens the grace. A kill's ask on a timeout's
        stop is a kill, so the job is ended as one and never retried."""
        tout = {"by": "timeout", "timeout": True, "t": 100.0, "grace": 300.0, "sent": 101.0, "kill9": 102.0}
        out = {"by": "ann", "t": 100.0, "grace": 300.0, "sent": 101.0, "kill9": 102.0}
        kill = {"by": "ann", "t": 500.0, "grace": 10.0}
        tmo = {"by": "timeout", "timeout": True, "t": 500.0, "grace": 10.0}
        rows = [
            ("a record that carries none takes the ask", None, kill, dict(kill)),
            ("a second kill keeps who asked, when, and what went out", out, kill, {**out, "grace": 10.0}),
            ("a longer grace never delays the SIGKILL", out, {**kill, "grace": 900.0}, dict(out)),
            ("a kill on a timeout's stop is the kill's, and says where it began", tout, kill,
             {**out, "was": "timeout", "grace": 10.0}),
            ("a timeout on a kill's stop changes nothing but the grace", out, tmo, {**out, "grace": 10.0}),
            ("a refused stop is asked for again", {**out, "denied": 103.0}, kill, {**out, "grace": 10.0}),
            ("a timeout on a timeout's stop is still the timeout's", tout, tmo, {**tout, "grace": 10.0}),
            ("an asker called timeout asks as a kill, and the stop stops being a timeout's", tout,
             {"by": "timeout", "t": 500.0, "grace": 10.0},
             {**out, "by": "timeout", "was": "timeout", "grace": 10.0}),
        ]
        for what, existing, ask, want in rows:
            with self.subTest(what):
                was, asked = None if existing is None else dict(existing), dict(ask)
                self.assertEqual(jobpolicy.merge_stop(existing, ask), want)
                self.assertEqual(existing, was, "it changed the stop it was given")
                self.assertEqual(ask, asked, "it changed the ask it was given")


class TestThePolicyTables(unittest.TestCase):
    """One table per rule of pp/lib/jobpolicy.py, exhaustive where the rule's input is finite.

    Every rule is handed records, readings, settings and `now`, and gives back a value, so a table is the
    whole of its behaviour: no process, no thread, no clock and no store. `TestTheDecision` above is the
    table for `decide` and `merge_stop`; these are the rest.
    """

    # -- merge_stop: every combination of the two stops, exhaustive -----------------------------------------
    def test_the_one_stop_a_run_carries_over_every_pair(self):
        """One run of a job carries one stop. Every pair of (what the record holds) and (what is asked) is a
        row here, with `denied` set or not and with either grace the shorter one, so a second ask can never
        write over what went out, never delay the SIGKILL, and never leave a refusal standing."""
        rows = 0
        for was in ("none", "timeout", "kill"):
            for ask_by in ("timeout", "kill"):
                for denied in (False, True):
                    for shorter in ("ask", "record"):
                        rows += 1
                        if was == "none" and denied:
                            continue        # a record that carries no stop carries no refusal either
                        existing = None if was == "none" else {
                            "by": "user" if was == "kill" else "timeout", "t": 100.0, "grace": 60.0,
                            "sent": 101.0, **({"timeout": True} if was == "timeout" else {}),
                            **({"denied": 102.0} if denied else {})}
                        ask = {"by": "ann" if ask_by == "kill" else "timeout", "t": 500.0,
                               "grace": 10.0 if shorter == "ask" else 900.0,
                               **({"timeout": True} if ask_by == "timeout" else {})}
                        got = jobpolicy.merge_stop(existing, ask)
                        with self.subTest(was=was, ask=ask_by, denied=denied, shorter=shorter):
                            if existing is None:
                                self.assertEqual(got, ask)      # the ask itself, the first time
                                continue
                            self.assertEqual((got["t"], got["sent"]), (existing["t"], existing["sent"]))
                            self.assertEqual(got["grace"], min(existing["grace"], ask["grace"]))
                            self.assertNotIn("denied", got)     # a refused stop is asked for again
                            kill = was == "kill" or ask_by == "kill"
                            self.assertEqual("timeout" in got, not kill)
                            self.assertEqual(got.get("was"), "timeout" if was == "timeout" and kill else None)
        self.assertEqual(rows, 24)

    # -- resolve: the four sources of one row, present or absent, exhaustive --------------------------------
    def test_the_one_order_a_settings_sources_are_read_in(self):
        """Section 2's one precedence, for every row of the declaration and over all sixteen ways its four
        sources can be present or absent: the flag, then the project's .pp.toml [queue], then the queue's
        [queue.NAME], then the row's own default.

        A source that does not give the setting is NOTSET, which is what lets `none` be a value: a row whose
        flag said `none` takes None and does not fall through to a key behind it. A scalar is taken whole
        from the first source that gives it; a table merges key by key, in the same order per key."""
        rows = 0
        for row in jobq.SETTINGS:
            for has_flag in (True, False):
                for has_project in (True, False):
                    for has_queue in (True, False):
                        for has_default in (True, False):
                            rows += 1
                            r = row if has_default else dataclasses.replace(row, default=None)
                            if row.merge:
                                src = {"flag": {"a": "f", "both": "f"}, "project": {"b": "p", "both": "p"},
                                       "queue": {"c": "q", "both": "q"}}
                                want = {}
                                for key, on in (("queue", has_queue), ("project", has_project),
                                                ("flag", has_flag)):
                                    want.update(src[key] if on else {})
                                want = {**(r.default or {}), **want}
                            else:
                                src = {"flag": "f", "project": "p", "queue": "q"}
                                want = ("f" if has_flag else "p" if has_project else "q" if has_queue
                                        else r.default)
                            got = jobpolicy.resolve(
                                r, src["flag"] if has_flag else jobpolicy.NOTSET,
                                src["project"] if has_project else jobpolicy.NOTSET,
                                src["queue"] if has_queue else jobpolicy.NOTSET)
                            with self.subTest(setting=row.name, scope=row.scope, flag=has_flag,
                                              project=has_project, queue=has_queue, default=has_default):
                                self.assertEqual(got, want)
        self.assertEqual(rows, 16 * len(jobq.SETTINGS))

    def test_a_value_of_none_wins_over_the_sources_behind_it(self):
        """`--timeout none` and `on_done = "none"` are values of their rows and not an absence of one, so a
        queue's default timeout can be lifted for one job and a project's hook turned off for one queue."""
        timeout, on_done = jobq.BY_SCOPE["job"]["timeout"], jobq.BY_SCOPE["job"]["on_done"]
        self.assertIsNone(jobpolicy.resolve(timeout, jobpolicy.value(timeout, "none"), config=7200.0))
        self.assertIsNone(jobpolicy.resolve(timeout, jobpolicy.value(timeout, "0"), config=7200.0))
        self.assertIsNone(jobpolicy.resolve(on_done, jobpolicy.value(on_done, "none"), config="true"))
        self.assertEqual(jobpolicy.resolve(timeout, jobpolicy.NOTSET, config=7200.0), 7200.0)

    # -- matches: --retry-if over the text it is handed, exhaustive ------------------------------------------
    def test_what_retry_if_matches_over_every_pattern(self):
        """`--retry-if` is read against the failure's own reason and the tail of the job's log. The tail is
        matched where the facts are read (`tail_hit`) and the reason inside the decision, so one pattern over
        the two reads the same as it did over one text. No pattern matches anything, since a job with no
        pattern is retried on any failure; a pattern that cannot be compiled matches nothing, so a typo
        retries nothing rather than everything."""
        rows = []
        for pat, holds in ((None, True), ("^lost$", True), ("CUDA out of memory", None),
                           ("nothing like it", False), ("(unclosed", False)):
            for tail in ("", "a line\nCUDA out of memory\nthe last line"):
                want = holds if holds is not None else bool(tail)
                rows.append((pat, tail, want))
                with self.subTest(pattern=pat, tail=bool(tail)):
                    job = {"id": 1} if pat is None else {"id": 1, "retry_if": pat}
                    self.assertEqual(jobpolicy.matches(job, "lost", jobpolicy.tail_hit(job, tail)), want)
        self.assertEqual(len(rows), 10)

    def test_the_log_tail_is_matched_where_the_facts_are_read(self):
        """I12. The decision is made inside the store's transaction, where every reader of every queue waits
        for the scope, and the tail of a job's log is matched against a pattern its caller wrote: a nested
        group over a line that does not match it backtracks exponentially, and a worker matching one in there
        holds the one store lock for as long as it takes.

        So the match is a fact, read with the others and before the scope, and the decision is made from that
        value: a failure whose tail matched is retried while attempts are left, and one whose tail did not is
        final, and neither reads the tail."""
        job = {"id": 1, "queue": "default", "attempt": 1, "retries": 1, "retry_if": "(a+)+$"}
        self.assertFalse(jobpolicy.tail_hit(job, "a" * 18 + "b"), "out here, where nothing waits for it")
        again = jobpolicy.decide(job, jobpolicy.facts(False, rc=1, rc_at=100.0, now=100.0, hit=True))
        self.assertEqual((again.do, again.attempt), (jobpolicy.AGAIN, 2))
        final = jobpolicy.decide(job, jobpolicy.facts(False, rc=1, rc_at=100.0, now=100.0, hit=False))
        self.assertEqual((final.do, final.state, final.rc), (jobpolicy.ENDED_AS, "failed", 1))

    # -- look: what one look at a worker's claim says, exhaustive --------------------------------------------
    def test_what_one_look_at_a_workers_claim_says(self):
        """Nobody holds the claim, a holder that has not recorded itself yet, and one whose record names a
        live process: the three answers, over every record a claim can hold. The answer is a value, and what
        it says about a worker being up and about the pid to signal comes from it alone."""
        live, dead = {"pid": 7, "pstart": 11}, {"pid": 8, "pstart": 12}
        rows = 0
        for rec in (None, {}, dead, live):
            for alive in (False, True):
                rows += 1
                with self.subTest(rec=rec, alive=alive):
                    if rec is None and alive:
                        continue        # nothing holds the claim, so no record of its can name a process
                    want = (jobpolicy.DOWN if rec is None else
                            jobpolicy.UP if alive else jobpolicy.STARTING)
                    w = jobpolicy.look(rec, alive)
                    self.assertEqual((w.how, w.rec, w.why, w.tried), (want, rec, None, False))
                    self.assertEqual(w.up, want == jobpolicy.UP)
                    self.assertEqual(w.pid, (rec or {}).get("pid") if w.up else None)
        self.assertEqual(rows, 8)
        self.assertEqual((jobpolicy.UP, jobpolicy.DOWN, jobpolicy.STARTING), (jobcore.UP, jobcore.DOWN,
                                                                             jobcore.STARTING))

    # -- next_tick: how long the worker waits next, exhaustive ------------------------------------------------
    def test_how_long_the_worker_waits_next(self):
        """A stop in flight and a wake together make the next wait the short one, so a verb waiting for a stop
        costs the job's own exit time and not the queue's poll. The idle time is spent only once the queue has
        held nothing for `idle_exit`, which a queue with something queued or running never has."""
        st = {"poll": 2.0, "idle_exit": 600.0}
        now, rows = 10_000.0, 0
        for serving in (0, 1):
            for woken in (False, True):
                for idle_since in (None, now - 599.0, now - 601.0):
                    rows += 1
                    with self.subTest(serving=serving, woken=woken, idle=idle_since):
                        period, spent = jobpolicy.next_tick(st, serving, woken, idle_since, now)
                        self.assertEqual(period, jobpolicy.STOP_PERIOD if serving and woken else 2.0)
                        self.assertEqual(spent, idle_since is not None and now - idle_since >= 600.0)
        self.assertEqual(rows, 12)
        # the settings a worker was handed, and the defaults where it was handed none
        self.assertEqual(jobpolicy.next_tick({}, 0, False, None, now), (jobpolicy.POLL, False))
        self.assertEqual(jobpolicy.next_tick({}, 0, False, now - jobpolicy.IDLE_EXIT, now), (jobpolicy.POLL, True))
        self.assertEqual(jobpolicy.next_tick({"poll": 0.01}, 1, True, None, now)[0], 0.01)   # the shorter of the two

    # -- job_env and hook_env: what a job and a hook run with ------------------------------------------------
    def test_what_one_attempt_of_a_job_runs_with(self):
        """The layers of one attempt's environment, in the order they merge: what the shell that queued the
        job was in, the project's env and the job's own, and then the names pp sets, which nothing overrides.
        Every layer present or absent, with a card reserved or none.

        The attempt's own names are read off its record, where the launch wrote them, so the environment of a
        job and the file its runner reads are one answer and not two."""
        rows = 0
        for shell in ({}, {"PATH": "/sh", "CONDA_PREFIX": "/c"}):
            for project in ({}, {"HF": "1"}):
                for own in ({}, {"SEED": "0", "PATH": "/mine"}):
                    for container in (None, "pp-dl-5-1"):
                        for envfile in (None, "/rc/5.1.ab.env"):
                            for cards in ([], [3]):
                                rows += 1
                                job = {"id": 5, "queue": "dl", "rcfile": "/rc/5.1.ab", "shell_env": shell,
                                       "env": {**project, **own}, "cards": cards,
                                       "container": container, "envfile": envfile}
                                got = jobpolicy.job_env(job)
                                with self.subTest(shell=bool(shell), project=bool(project), own=bool(own),
                                                  container=container, envfile=envfile, cards=cards):
                                    self.assertEqual(got["PP_QUEUE"], "dl")
                                    self.assertEqual(got["PP_QUEUE_JOB"], "5")
                                    self.assertEqual(got["PP_QUEUE_RCFILE"], "/rc/5.1.ab")
                                    self.assertEqual(got.get("PP_QUEUE_CONTAINER"), container)
                                    self.assertEqual(got.get("PP_QUEUE_ENVFILE"), envfile)
                                    self.assertEqual(got.get("CUDA_VISIBLE_DEVICES"),
                                                     "3" if cards else None)
                                    for k, v in {**project, **own}.items():
                                        self.assertEqual(got[k], v)       # the job's own, over the shell's
                                    for k, v in shell.items():
                                        if k not in {**project, **own}:
                                            self.assertEqual(got[k], v)
                                    # the file the runner reads: those layers, and nothing of this machine's
                                    text = jobpolicy.env_file(job)
                                    lines = [ln for ln in text.splitlines() if ln]
                                    want = [f"{k}={v}" for k, v in {**project, **own}.items()]
                                    want += [f"CUDA_VISIBLE_DEVICES={','.join(map(str, cards))}"] if cards else []
                                    want += ["PP_QUEUE=dl", "PP_QUEUE_JOB=5"]
                                    want += [f"PP_QUEUE_CONTAINER={container}"] if container else []
                                    self.assertEqual(lines, [ln for ln in want
                                                             if ln.split("=")[0] not in jobpolicy.NEVER_IN_ENVFILE])
                                    for never in jobpolicy.NEVER_IN_ENVFILE:
                                        self.assertNotIn(never + "=", text)
        self.assertEqual(rows, 64)
        # every card it reserved, as the comma-separated list a runtime and a library both read
        two = jobpolicy.job_env({"id": 5, "queue": "dl", "rcfile": "/rc/x", "cards": [1, 2]})
        self.assertEqual(two["CUDA_VISIBLE_DEVICES"], "1,2")

    def test_what_a_hook_of_a_job_runs_with(self):
        """A hook reads how the job ended from its environment, so every name is there even where the record
        holds none: a hook under `set -u` reads all of them. The reason a failure with no reason of its own
        carries is its exit code."""
        names = ("PP_QUEUE", "PP_QUEUE_JOB", "PP_QUEUE_EVENT", "PP_QUEUE_RC", "PP_QUEUE_REASON",
                 "PP_QUEUE_LOG", "PP_QUEUE_CWD", "PP_QUEUE_TAG")
        rows = 0
        for event in ("failed", "done"):
            for rc in (None, 0, 7):
                for reason in (None, "killed"):
                    rows += 1
                    job = {"id": 5, "queue": "dl", **({} if rc is None else {"rc": rc}),
                           **({} if reason is None else {"reason": reason})}
                    got = jobpolicy.hook_env(job, event)
                    with self.subTest(event=event, rc=rc, reason=reason):
                        self.assertEqual(sorted(got), sorted(names))
                        self.assertEqual(got["PP_QUEUE_EVENT"], event)
                        self.assertEqual(got["PP_QUEUE_RC"], "" if rc is None else str(rc))
                        want = reason or (f"exit {rc}" if event == "failed" and rc is not None else "")
                        self.assertEqual(got["PP_QUEUE_REASON"], want)
        self.assertEqual(rows, 12)

    # -- tag: what a job may be named --------------------------------------------------------------------
    def test_what_a_job_may_be_named(self):
        """A name is at most 64 characters of letters, digits and . _ - : , holds no `..`, is not `.` alone,
        and never reads as an id or a range. Every class of value, and the boundary of the length."""
        rows = [("a word", "c3_recall", True), ("dots, colons and dashes", "c3.recall:s0-1", True),
                ("64 characters", "a" * 64, True), ("65 characters", "a" * 65, False),
                ("one character", "a", True), ("nothing", "", False),
                ("a space", "c3 recall", False), ("a slash", "c3/recall", False),
                ("an escape sequence", "c3\x1b[2J", False), ("a newline", "c3\n", False),
                ("two dots", "c3..recall", False), ("two dots at the end", "c3..", False),
                ("a dot on its own", ".", False), ("a dot with a word", ".c3", True),
                ("an id", "12", False), ("a range", "12-15", False),
                ("a number in a word", "c3-15", True), ("not a string", 12, False)]
        for what, value, ok in rows:
            with self.subTest(what):
                if ok:
                    self.assertEqual(jobpolicy.tag(value), value)
                else:
                    self.assertRaises(ValueError, jobpolicy.tag, value)

    # -- log: where a job's output goes ----------------------------------------------------------------------
    def test_where_a_jobs_output_goes(self):
        """A closed set of two placeholders, `{tag}` and `{id}`, filled in when the job takes its id. `{tag}`
        is the job's name or its id where it has none, so nothing is refused for a missing name. Another
        placeholder and a path holding '..' are refused where the pattern is read, and a job with no pattern
        takes the queue's own path."""
        default = "/state/queue/lab/logs/12.log"
        rows = [("no pattern at all", None, "named", default),
                ("one job's own path", "/data/one.log", "named", "/data/one.log"),
                ("{tag}", "/p/logs/{tag}.log", "c3_recall.s0", "/p/logs/c3_recall.s0.log"),
                ("{tag} of a job with no name", "/p/logs/{tag}.log", None, "/p/logs/12.log"),
                ("{id}", "/p/logs/{id}.log", "c3_recall.s0", "/p/logs/12.log"),
                ("both, and each once", "/p/{tag}/{id}.log", "c3", "/p/c3/12.log"),
                ("{id} twice", "/p/{id}/{id}.log", None, "/p/12/12.log")]
        for what, spec, tag, want in rows:
            with self.subTest(what):
                self.assertEqual(jobpolicy.log_path(spec, 12, tag, default), want)
        for ok in ("logs/{tag}.log", "{id}.log", "/data/runs/{tag}/out.log", "plain.log", "~/logs/{id}.log"):
            self.assertEqual(jobpolicy.pattern(ok), ok)
        for bad in ("logs/{queue}.log", "logs/{tag:%Y}.log", "logs/{}.log", "../logs/{id}.log",
                    "logs/../{id}.log", "", "  ", None, 7, ["logs"]):
            self.assertRaises(ValueError, jobpolicy.pattern, bad)

    # -- select: the forms a selection takes -----------------------------------------------------------------
    # Records of two queues, of all five states, one of them with no name of its own: what every form of the
    # grammar is read against below.
    RECORDS = [{"id": 3, "tag": "c3_recall.s0", "queue": "a", "state": "queued"},
               {"id": 5, "tag": "c3_recall.s1", "queue": "a", "state": "held"},
               {"id": 8, "tag": "c3_shift.s0", "queue": "a", "state": "running"},
               {"id": 12, "tag": "s3_eval", "queue": "b", "state": "done"},
               {"id": 13, "tag": "s3_eval", "queue": "b", "state": "failed"},
               {"id": 20, "tag": None, "queue": "b", "state": "queued"}]
    SPANS = [{"id": 3, "tag": "same", "queue": "a", "state": "queued"},
             {"id": 5, "tag": "same", "queue": "b", "state": "running"}]

    def test_which_jobs_a_selection_names(self):
        """The five forms of the grammar and no others, over the records they are read against: an id, a range,
        a tag matched whole, a prefix of a name, and every job of the queue -q names.

        Every form against every class of answer it can give (one job, several, none), over records of two
        queues and of all five states, since no form reads a state or a queue: the narrowing is the caller's,
        which hands in the records of the queue -q named. The answer is in the order the specs were given,
        each id once, and the ids of one spec in id order; a form that names no record gives back nothing, for
        the verb to refuse."""
        P, A = jobpolicy.PREFIX, jobpolicy.ALL
        every = [3, 5, 8, 12, 13, 20]
        rows = [
            (jobpolicy.ID, "one", self.RECORDS, "8", [8]),
            (jobpolicy.ID, "one", self.RECORDS, "20", [20]),            # a job with no name, by its id
            (jobpolicy.ID, "none", self.RECORDS, "99", []),
            (jobpolicy.RANGE, "one", self.RECORDS, "12-12", [12]),
            (jobpolicy.RANGE, "several", self.RECORDS, "3-8", [3, 5, 8]),
            (jobpolicy.RANGE, "several", self.RECORDS, "13-20", [13, 20]),   # two queues, and an unnamed job
            (jobpolicy.RANGE, "none", self.RECORDS, "14-19", []),
            (jobpolicy.TAG, "one", self.RECORDS, "c3_shift.s0", [8]),
            (jobpolicy.TAG, "several", self.RECORDS, "s3_eval", [12, 13]),
            (jobpolicy.TAG, "several", self.SPANS, "same", [3, 5]),          # two queues: the caller refuses
            (jobpolicy.TAG, "none", self.RECORDS, "nothing_carries_this", []),
            (P, "one", self.RECORDS, (P, "c3_shift"), [8]),
            (P, "several", self.RECORDS, (P, "c3_"), [3, 5, 8]),
            (P, "several", self.SPANS, (P, "sa"), [3, 5]),
            (P, "none", self.RECORDS, (P, "zz"), []),
            (P, "several", self.RECORDS, (P, ""), every[:-1]),               # never a job with no name
            (A, "several", self.RECORDS, (A, None), every),
            (A, "one", self.RECORDS[:1], (A, None), [3]),
            (A, "none", [], (A, None), []),
        ]
        for form, size, records, selection, want in rows:
            with self.subTest(form=form, size=size, selection=selection):
                self.assertEqual(jobpolicy.kind(selection), form)
                self.assertEqual(jobpolicy.select(records, selection), want)    # one spec, as it is
                self.assertEqual(jobpolicy.select(records, [selection]), want)  # and in a list of one
        # every form against every class of answer, with the one that cannot happen asserted impossible: an id
        # names one job machine-wide, so no id ever names two
        classes = {(form, size) for form, size, *_rest in rows}
        self.assertEqual(classes | {(jobpolicy.ID, "several")},
                         {(f, s) for f in (jobpolicy.ID, jobpolicy.RANGE, jobpolicy.TAG, P, A)
                          for s in ("one", "several", "none")})
        for j in self.RECORDS:
            self.assertEqual(jobpolicy.select(self.RECORDS, str(j["id"])), [j["id"]])
            # no form reads a state: a job of any of the five is named by its name as readily as by its id
            self.assertIn(j["id"], jobpolicy.select(self.RECORDS, (A, None)))
            if j["tag"]:
                self.assertEqual(jobpolicy.select(self.RECORDS, j["tag"]),
                                 sorted(x["id"] for x in self.RECORDS if x["tag"] == j["tag"]))
        several = [("two specs, in the order given", ["12", "3-8"], [12, 3, 5, 8]),
                   ("each id once, however many specs name it", ["s3_eval", "12", "12-13"], [12, 13]),
                   ("the forms that are flags, beside the words", [(P, "c3_"), "12"], [3, 5, 8, 12]),
                   ("--all after a word names what is left of the queue", ["12", (A, None)],
                    [12, 3, 5, 8, 13, 20])]
        for what, selection, want in several:
            with self.subTest(what):
                self.assertEqual(jobpolicy.select(self.RECORDS, selection), want)
        for spec, form in (("7", jobpolicy.ID), ("12-15", jobpolicy.RANGE), ("sweep", jobpolicy.TAG),
                           ("12-", jobpolicy.TAG), ("", jobpolicy.TAG), ((P, "s"), P), ((A, None), A)):
            self.assertEqual(jobpolicy.kind(spec), form)

    # -- admit: what one launch pass starts, and what the head waits for -------------------------------------
    def pool(self, name="slots", scope=None, mx=3, probe=None, settle=30.0):
        return {"name": name, "scope": scope, "where": f"[resources] {name}", "max": mx, "probe": probe,
                "every": 30.0, "settle": settle}

    def seen(self, pools, cards=None, probes=None, cards_t=1000.0):
        by: dict = {}
        for p in pools:
            by.setdefault(p["name"], []).append(p)
        return {"q": "default", "pools": pools, "by_name": by, "cards": cards, "cards_t": cards_t,
                "probes": probes or {}, "cards_why": "", "where": "/cfg/config.toml"}

    def job(self, jid, needs=None, **kw):
        return {"id": jid, "queue": "default", "state": "queued", "prio": 10, "needs": needs or {"slots": 1},
                **kw}

    def pass_read(self, queued, running=(), held=()):
        """What one launch pass read inside its own transaction: this queue's queued jobs in run order, and
        every queue's live ones under one listing with no queue filter, which is where a shared pool's count
        and an unmet `after` both come from."""
        return {"queued": list(queued), "live": list(queued) + list(running) + list(held)}

    def test_what_one_launch_pass_admits(self):
        """One pass, over every class of pool and every way one can be held: a number, a reading, a settle
        window, and nothing providing it at all. Strict head-of-line unless the pass backfills, and a job
        waiting out a retry's delay is skipped rather than waited for, so it never becomes the head."""
        now = 2000.0
        a, b = self.job(1), self.job(2)
        rows = [
            ("both fit", [self.pool(mx=3)], {}, [a, b], [], False, [1, 2], None),
            ("the head fits and the next does not", [self.pool(mx=1)], {}, [a, b], [], False, [1], 2),
            ("a running job counts against the max", [self.pool(mx=1)], {}, [a], [self.job(9, state="running")],
             False, [], 1),
            ("one queue's own pool counts that queue's jobs alone",
             [self.pool(scope="default", mx=1)], {}, [a], [{**self.job(9, state="running"), "queue": "other"}],
             False, [1], None),
            ("a shared pool counts every queue's", [self.pool(mx=1)], {},
             [a], [{**self.job(9, state="running"), "queue": "other"}], False, [], 1),
            ("nothing provides what a job needs", [self.pool(mx=3)], {},
             [self.job(1, needs={"nodes": 1})], [], False, [], 1),
            ("a probe that says there is room", [self.pool(name="nodes", mx=None, probe="free")],
             {(None, "nodes"): {"t": 1000.0, "value": 4}}, [self.job(1, needs={"nodes": 2})], [], False,
             [1], None),
            ("a probe that says there is not", [self.pool(name="nodes", mx=None, probe="free")],
             {(None, "nodes"): {"t": 1000.0, "value": 1}}, [self.job(1, needs={"nodes": 2})], [], False,
             [], 1),
            ("a probe that failed", [self.pool(name="nodes", mx=None, probe="free")],
             {(None, "nodes"): {"t": 1000.0, "value": None, "error": "exit 1"}},
             [self.job(1, needs={"nodes": 2})], [], False, [], 1),
            ("a probe being read now", [self.pool(name="nodes", mx=None, probe="free")],
             {(None, "nodes"): {"t": 1000.0, "value": None, "running": True}},
             [self.job(1, needs={"nodes": 2})], [], False, [], 1),
            ("a reading older than what a running job took",
             [self.pool(name="nodes", mx=None, probe="free", settle=30.0)],
             {(None, "nodes"): {"t": 1000.0, "value": 2}},
             [self.job(1, needs={"nodes": 2})],
             [{**self.job(9, state="running", needs={"nodes": 2}), "started": 990.0}], False, [], 1),
            ("a reading taken after that job started",
             [self.pool(name="nodes", mx=None, probe="free", settle=30.0)],
             {(None, "nodes"): {"t": 1000.0, "value": 2}},
             [self.job(1, needs={"nodes": 2})],
             [{**self.job(9, state="running", needs={"nodes": 2}), "started": 900.0}], False, [1], None),
        ]
        for what, pools, probes, queued, running, backfill, starts, blocked in rows:
            with self.subTest(what):
                p = jobpolicy.admit(self.pass_read(queued, running), self.seen(pools, probes=probes),
                                    {"backfill": backfill, "now": now})
                self.assertEqual([j["id"] for j, _card in p.starts], starts)
                self.assertEqual(None if p.blocked[0] is None else p.blocked[0]["id"], blocked)
                if blocked is not None:
                    self.assertTrue(p.blocked[1], "a job that did not fit says why")

    def test_the_head_of_the_queue_and_the_jobs_behind_it(self):
        """Strict head-of-line by priority: the first job that does not fit ends the pass, so a big job is
        never starved by small ones. With backfill the pass goes on past it, and the job that did not fit is
        still the one the head waits for.

        A job that is not ready is skipped and not waited for, by its retry delay or by its `after`, so it
        never becomes the head and the jobs behind it still start. An `after` is met when no id in it names
        a job that is queued, held or running in any queue, and an id whose record is gone counts as
        ended."""
        now = 2000.0
        big, small = self.job(1, needs={"slots": 3}), self.job(2, needs={"slots": 1})
        delayed = self.job(3, needs={"slots": 1}, not_before=now + 60)
        for backfill in (False, True):
            with self.subTest(backfill=backfill, fits="the head, and what it took leaves no room"):
                p = jobpolicy.admit(self.pass_read([big, small]), self.seen([self.pool(mx=3)]),
                                    {"backfill": backfill, "now": now})
                self.assertEqual([j["id"] for j, _c in p.starts], [1])   # backfill invents no room
                self.assertEqual(p.blocked[0]["id"], 2)
            with self.subTest(backfill=backfill, fits="the head, with room behind it"):
                p = jobpolicy.admit(self.pass_read([big, small]), self.seen([self.pool(mx=4)]),
                                    {"backfill": backfill, "now": now})
                self.assertEqual([j["id"] for j, _c in p.starts], [1, 2])
                self.assertIsNone(p.blocked[0])
            with self.subTest(backfill=backfill, fits="not the head"):
                p = jobpolicy.admit(self.pass_read([big, small]), self.seen([self.pool(mx=2)]),
                                    {"backfill": backfill, "now": now})
                self.assertEqual([j["id"] for j, _c in p.starts], [2] if backfill else [])
                self.assertEqual(p.blocked[0]["id"], 1)     # the head is what the queue waits for, either way
        for backfill in (False, True):
            with self.subTest(backfill=backfill, what="a job in its delay is skipped and never the head"):
                p = jobpolicy.admit(self.pass_read([delayed, small]), self.seen([self.pool(mx=3)]),
                                    {"backfill": backfill, "now": now})
                self.assertEqual([j["id"] for j, _c in p.starts], [2])
                self.assertIsNone(p.blocked[0])
            # an unmet `after` is the same filter as a delay: the job is skipped, the jobs behind it start,
            # and it never becomes the head, which is why a stage that waits cannot hold a queue up
            for what, live, starts in (("queued elsewhere", [self.job(9, queue="other")], [2]),
                                       ("held elsewhere", [self.job(9, queue="other", state="held")], [2]),
                                       ("running elsewhere", [self.job(9, queue="other", state="running",
                                                                       needs={})], [2]),
                                       ("its record gone", [], [3, 2])):
                waits = self.job(3, after=[9])
                with self.subTest(backfill=backfill, after=what):
                    p = jobpolicy.admit(self.pass_read([waits, small], live), self.seen([self.pool(mx=3)]),
                                        {"backfill": backfill, "now": now})
                    self.assertEqual([j["id"] for j, _c in p.starts], starts)
                    self.assertIsNone(p.blocked[0])
        with self.subTest("one id of an after that has ended and one that has not"):
            p = jobpolicy.admit(self.pass_read([self.job(3, after=[8, 9])], [self.job(9, queue="other")]),
                                self.seen([self.pool(mx=3)]), {"backfill": False, "now": now})
            self.assertEqual(p.starts, ())
            self.assertIsNone(p.blocked[0])
        with self.subTest("what the pass has taken counts against the next job in it"):
            p = jobpolicy.admit(self.pass_read([small, self.job(4, needs={"slots": 1})]),
                                self.seen([self.pool(mx=1)]), {"backfill": False, "now": now})
            self.assertEqual([j["id"] for j, _c in p.starts], [2])
            self.assertEqual(p.blocked[0]["id"], 4)

    def test_which_card_a_job_takes(self):
        """A GPU pool is per card: the budget is the pool's max, or the card's own total less 2 GB, and what
        nvidia-smi cannot see yet is what a job launched inside the settle window took. The card with the most
        free memory that fits is the one the job takes."""
        now, cards = 2000.0, [card(0, total=24, used=20), card(1, total=24, used=4)]
        pools = [self.pool(name="gpu_gb", mx=None, settle=30.0), self.pool(name="slots", mx=3)]
        want = self.job(1, needs={"gpu_gb": 8, "slots": 1})
        # the budget of a pool with no max of its own is the card's total less 2 GB
        self.assertEqual(jobpolicy.gpu_budget(pools[0], cards[0]), 22.0)
        self.assertEqual(jobpolicy.gpu_budget({**pools[0], "max": 10}, cards[0]), 10)
        with self.subTest("the card that fits"):
            p = jobpolicy.admit(self.pass_read([want]), self.seen(pools, cards=cards),
                                {"backfill": False, "now": now})
            self.assertEqual([(j["id"], c) for j, c in p.starts], [(1, 1)])
        with self.subTest("a card another job has reserved"):
            on = [{**self.job(9, state="running", needs={"gpu_gb": 20}), "cards": [1], "started": 100.0}]
            p = jobpolicy.admit(self.pass_read([want], on), self.seen(pools, cards=cards),
                                {"backfill": False, "now": now})
            self.assertEqual(p.starts, ())
            self.assertIn("reserved", p.blocked[1])
        with self.subTest("a job launched inside the settle window, which the reading cannot see"):
            # the cards say 20 free on card 1, and 8 of it is a job launched since that reading: a job that
            # needs 16 does not fit, though the pool's own number leaves room for it
            wide = [{**self.pool(name="gpu_gb", mx=24, settle=30.0), "where": "[resources] gpu_gb"}]
            big = self.job(1, needs={"gpu_gb": 16})
            on = [{**self.job(9, state="running", needs={"gpu_gb": 8}), "cards": [1], "started": 1990.0}]
            p = jobpolicy.admit(self.pass_read([big], on),
                                self.seen(wide, cards=[card(0, total=24, used=24), card(1, total=24, used=4)],
                                          cards_t=1980.0), {"backfill": False, "now": now})
            self.assertEqual(p.starts, ())
            self.assertIn("not allocated yet", p.blocked[1])
        with self.subTest("no card at all"):
            p = jobpolicy.admit(self.pass_read([want]), self.seen(pools, cards=None),
                                {"backfill": False, "now": now})
            self.assertEqual(p.starts, ())
            self.assertIn("nvidia-smi", p.blocked[1])

    def test_what_can_never_fit_is_refused_before_it_is_queued(self):
        """`check` is the same arithmetic asked at add time: a need nothing provides, and one larger than
        every budget there is, are refused where the job is queued rather than waited for forever."""
        pools = [self.pool(name="slots", mx=3), self.pool(name="gpu_gb", mx=None)]
        seen = self.seen(pools, cards=[card(0, total=24)])
        self.assertIsNone(jobpolicy.check({"slots": 1, "gpu_gb": 20}, seen))
        self.assertIn("nothing provides 'nodes'", jobpolicy.check({"nodes": 1}, seen))
        self.assertIn("/cfg/config.toml", jobpolicy.check({"nodes": 1}, seen))      # and the file to set it in
        self.assertIn("no card's budget is that large", jobpolicy.check({"gpu_gb": 23}, seen))
        self.assertIn("[resources] slots = 3", jobpolicy.check({"slots": 4}, seen))
        self.assertIn("sees no GPU", jobpolicy.check({"gpu_gb": 1}, self.seen(pools, cards=None)))

    # -- pool_usage: what a scope's running jobs hold of one pool -------------------------------------------
    def test_what_a_scopes_running_jobs_hold_of_one_pool(self):
        """The entry a view publishes for one pool, over every class of pool there is: a shared one counts
        every queue's running jobs and a queue's own counts that queue's, and a gpu_gb pool counts each card
        of the reading on its own, against the budget that scope has on that card.

        A gpu_gb pool with no card reading carries an empty per_card, and a pool of any other kind carries
        none: the amount its jobs hold is the whole of what there is to say about it."""
        mine = {**self.job(1, needs={"gpu_gb": 8, "slots": 1}, state="running"), "cards": [0]}
        other = {**self.job(2, needs={"gpu_gb": 6, "slots": 1}, state="running", queue="other"), "cards": [1]}
        cards = [card(0, total=24), card(1, total=24)]
        rows = [
            ("a shared pool of the cards, over two of them", self.pool(name="gpu_gb", mx=None), cards, 14,
             [{"index": 0, "max": 22.0, "used": 8}, {"index": 1, "max": 22.0, "used": 6}]),
            ("a queue's own, which counts that queue's jobs alone",
             self.pool(name="gpu_gb", scope="default", mx=16), cards, 8,
             [{"index": 0, "max": 16, "used": 8}, {"index": 1, "max": 16, "used": 0}]),
            ("one card", self.pool(name="gpu_gb", mx=None), cards[:1], 14,
             [{"index": 0, "max": 22.0, "used": 8}]),
            ("no card reading", self.pool(name="gpu_gb", mx=None), None, 14, []),
            ("a pool that is not the cards'", self.pool(name="slots", mx=3), cards, 2, None),
        ]
        for what, p, reading, used, per_card in rows:
            with self.subTest(what):
                got = jobpolicy.pool_usage(p, [mine, other], reading)
                self.assertEqual((got["name"], got["max"], got["probe"], got["reading"]),
                                 (p["name"], p["max"], None, None))
                self.assertEqual(got["used"], used)
                self.assertEqual(got.get("per_card"), per_card)

    # -- campaign: one line of a file, split the way a shell splits words ----------------------------------
    LINES = (
        # (the line, its flags, its command) for a line that is a job
        ("python -m lab.run", [], ["python", "-m", "lab.run"]),
        ("--tag s3_L00 --gpu-gb 36 -- python -m lab.run --layer 0",
         ["--tag", "s3_L00", "--gpu-gb", "36"], ["python", "-m", "lab.run", "--layer", "0"]),
        ("-- python x.py", [], ["python", "x.py"]),                 # a line that is only a command, after --
        ("echo 'two words'", [], ["echo", "two words"]),
        ('echo "two words"', [], ["echo", "two words"]),
        ("echo one\\ word", [], ["echo", "one word"]),
        ("-- sh -c 'python x.py | grep -v noise'",
         [], ["sh", "-c", "python x.py | grep -v noise"]),          # a quoted operator is one word
        ('-- sh -c "a; b"', [], ["sh", "-c", "a; b"]),
        ("-- echo '--'", [], ["echo", "--"]),                       # a quoted -- is a word of the command
        ("-- echo a#b", [], ["echo", "a#b"]),                       # a # that does not begin the line
        ('-- echo "say \\"hi\\""', [], ["echo", 'say "hi"']),
        ("-- echo ''", [], ["echo", ""]),
        ("  --tag t  --  python  x.py  ", ["--tag", "t"], ["python", "x.py"]),
    )
    REFUSED = (
        ("./run.sh > out.txt", "shell operator"),
        ("./run.sh | tee out", "shell operator"),
        ("./run.sh && ./next.sh", "shell operator"),
        ("./run.sh; ./next.sh", "shell operator"),
        ("./run.sh < in.txt", "shell operator"),
        ("echo $(date)", "shell operator"),
        ("echo `date`", "shell operator"),
        ("echo (a)", "shell operator"),
        ('echo "unclosed', "never closed"),
        ("echo one\\", "backslash"),
        ("--tag s3_L00 --gpu-gb 36", "no command"),                 # flags with no --, so no command
    )

    def test_what_one_line_of_a_campaign_file_is(self):
        """A line is the rest of an `add` command line: its own flags, then the first bare `--`, then the
        command. It is split the way a shell splits words, with single quotes, double quotes and backslashes
        honoured and nothing else read, so a command is argv and pp expands no variable, no `~` and no glob.

        A shell operator outside quotes is refused, because a line holding one was written for a shell;
        inside quotes it is an ordinary character. A quote that is never closed, a line that ends in a
        backslash, and a line with no `--` whose first word begins with `-` are refused the same way."""
        for line, flags, cmd in self.LINES:
            with self.subTest(line=line):
                self.assertEqual(jobpolicy.campaign(line), (flags, cmd))
        for line, word in self.REFUSED:
            with self.subTest(line=line):
                with self.assertRaises(ValueError) as caught:
                    jobpolicy.campaign(line)
                self.assertIn(word, str(caught.exception))

    def test_a_lines_own_flag_wins_over_the_adds(self):
        """One source of a row read in two places: a line's own word over the `add`'s, per flag. A flag that
        may be repeated carries a list, and the two are read in order with the line's last, so a table a
        line adds a key to keeps the `add`'s other keys and wins on the key it names."""
        prio = jobq.BY_SCOPE["job"]["prio"]
        env = jobq.BY_SCOPE["job"]["env"]
        self.assertEqual(jobpolicy.given(prio, 0, 1), 0)
        self.assertEqual(jobpolicy.given(prio, 0, jobpolicy.NOTSET), 0)
        self.assertEqual(jobpolicy.given(env, ["A=line"], ["A=add", "B=add"]), ["A=add", "B=add", "A=line"])
        self.assertEqual(jobpolicy.pairs(jobpolicy.given(env, ["A=line"], ["A=add", "B=add"])),
                         {"A": "line", "B": "add"})
        self.assertEqual(jobpolicy.given(env, ["A=line"], None), ["A=line"])

    def test_every_rule_gives_the_same_answer_twice_and_changes_nothing(self):
        """Pure: the records and the readings a rule is handed come back as they were, and the same input
        twice gives the same value. So a table is the whole of the rule."""
        now = 2000.0
        state = self.pass_read([self.job(1), self.job(2)], [self.job(9, state="running")])
        seen = self.seen([self.pool(mx=2)])
        before = json.dumps([state, {k: v for k, v in seen.items() if k != "probes"}], sort_keys=True)
        first = jobpolicy.admit(state, seen, {"backfill": False, "now": now})
        again = jobpolicy.admit(state, seen, {"backfill": False, "now": now})
        self.assertEqual([j["id"] for j, _c in first.starts], [j["id"] for j, _c in again.starts])
        self.assertEqual(json.dumps([state, {k: v for k, v in seen.items() if k != "probes"}],
                                    sort_keys=True), before)
        job = {"id": 5, "queue": "dl", "rcfile": "/rc/x", "env": {"A": "1"}, "shell_env": {}}
        was = json.dumps(job, sort_keys=True)
        self.assertEqual(jobpolicy.job_env(job), jobpolicy.job_env(job))
        self.assertEqual(json.dumps(job, sort_keys=True), was)


class TestTheSettingsTable(unittest.TestCase):
    """The settings table generated from the one declaration in the code (pp/lib/jobq.py, SETTINGS), beside
    the table the interface holds, row for row.

    A row the code has and the table does not, a row the table has and the code does not, and a difference in
    a row's flag, its keys, its default or whether that default is printed, are each a failure here. So one
    declaration, one default and one precedence is a test and not a reading. The rows whose behaviour a later
    stage brings are named below and asserted absent, so neither list can go quiet.
    """

    # the table's own rows: (setting, scope, flag, its key or keys, default, its default is printed)
    TABLE = (
        ("name", "project", "", "name (top level)", None, False),
        ("runner", "project", "--runner", "[queue] runner", None, False),
        ("stop", "project", "--stop", "[queue] stop", None, False),
        ("prio", "job", "--prio", "[queue] prio, [queue.NAME] prio", 10, True),
        ("gpu_gb", "job", "--gpu-gb", "[queue] gpu_gb, [queue.NAME] gpu_gb", None, False),
        ("cpu", "job", "--cpu", "[queue] cpu, [queue.NAME] cpu", None, False),
        ("mem_gb", "job", "--mem-gb", "[queue] mem_gb, [queue.NAME] mem_gb", None, False),
        ("need", "job", "--need", "[queue] need, [queue.NAME] need", {}, False),
        ("env", "job", "--env", "[queue] env, [queue.NAME] env", {}, False),
        # the store names the file where no pattern does: <state>/queue/<name>/logs/<id>.log
        ("log", "job", "--log", "[queue] log, [queue.NAME] log", None, False),
        ("timeout", "job", "--timeout", "[queue] timeout, [queue.NAME] timeout", None, False),
        ("retries", "job", "--retries", "[queue] retries, [queue.NAME] retries", 0, True),
        ("retry_delay", "job", "--retry-delay", "[queue] retry_delay, [queue.NAME] retry_delay", 30.0, True),
        ("retry_if", "job", "--retry-if", "[queue] retry_if, [queue.NAME] retry_if", None, False),
        ("on_fail", "job", "--on-fail", "[queue] on_fail, [queue.NAME] on_fail", None, False),
        ("on_done", "job", "--on-done", "[queue] on_done, [queue.NAME] on_done", None, False),
        ("grace", "job", "--grace", "[queue] grace, [queue.NAME] grace", 10.0, True),
        ("poll", "queue", "", "[queue.NAME] poll", 2.0, True),
        ("idle_exit", "queue", "", "[queue.NAME] idle_exit", 600.0, True),
        ("backfill", "queue", "", "[queue.NAME] backfill", False, True),
        ("resources", "queue", "", "[queue.NAME.resources]", {"slots": 3}, True),
        ("resources", "machine", "", "[resources] NAME",
         {"cpu": "auto", "mem_gb": "auto", "gpu_gb": "auto"}, True),
        ("max", "machine", "", "[resources] max", "auto", False),
        ("probe", "machine", "", "[resources] probe", None, False),
        ("every", "machine", "", "[resources] every", 30.0, True),
        ("settle", "machine", "", "[resources] settle", 30.0, True),
    )

    # the rows of the same table whose flag and behaviour a later stage brings: (setting, scope, stage)
    LATER = ()

    @staticmethod
    def keys_of(row) -> str:
        """A row's key or keys as the table words them, from the tables its scope gives it and nothing else."""
        if row.kind == "pools":
            return "[queue.NAME.resources]" if row.scope == "queue" else "[resources] NAME"
        if row.top:
            return f"{row.name} (top level)"
        return ", ".join({"project": f"[queue] {row.name}", "queue": f"[queue.NAME] {row.name}",
                          "machine": f"[resources] {row.name}"}[t] for t in row.tables)

    def generated(self) -> tuple:
        """The table the declaration itself gives, in the order it declares its rows."""
        return tuple((r.name, r.scope, r.flag, self.keys_of(r), r.default, r.printed) for r in jobq.SETTINGS)

    def test_the_generated_table_is_the_one_the_interface_holds(self):
        """Printed beside it, and equal to it row for row: every setting a user can give, its scope, its
        flag where its scope has one, its keys, its default, and whether that default is printed."""
        got, want = self.generated(), self.TABLE
        print("\nsetting      scope    flag           keys                               default        printed")
        for name, scope, flag, keys, default, printed in got:
            print(f"{name:<12} {scope:<8} {flag:<14} {keys:<34} {str(default):<14} {printed}")
        self.assertEqual({(r[0], r[1]) for r in got} - {(r[0], r[1]) for r in want}, set(),
                         "the code has these rows and the table does not")
        self.assertEqual({(r[0], r[1]) for r in want} - {(r[0], r[1]) for r in got}, set(),
                         "the table has these rows and the code does not")
        self.assertEqual(sorted(got, key=lambda r: (r[0], r[1])),
                         sorted(want, key=lambda r: (r[0], r[1])))

    def test_the_rows_a_later_stage_brings_are_named_and_not_declared(self):
        """A row whose behaviour is not built yet is not declared, so nothing resolves a setting pp does not
        carry out. Each is named here with the stage that brings it, and the stage before that one fails
        here if it declares the row early or late."""
        declared = {(r.name, r.scope) for r in jobq.SETTINGS}
        for name, scope, stage in self.LATER:
            with self.subTest(setting=name, stage=stage):
                self.assertNotIn((name, scope), declared)
                self.assertGreater(stage, 4)
        self.assertEqual(self.LATER, (), "every row of the table is declared and carried out")

    def test_a_rows_sources_follow_from_its_scope(self):
        """Which sources a row has follows from what the setting belongs to, and nothing else decides it: a
        per-job row has a flag and both keys, a per-queue row the queue's key alone, the machine's rows the
        machine's table alone, and a per-project row its project's key and a flag for one job.

        A row that cannot have all of its scope's sources is not a setting but an argument of `add`, which is
        why `--tag`, `--hold`, `--after` and `--cwd` are no rows of this table."""
        want = {"job": ("project", "queue"), "queue": ("queue",), "machine": ("machine",),
                "project": ("project",)}
        for r in jobq.SETTINGS:
            with self.subTest(setting=r.name, scope=r.scope):
                self.assertEqual(r.tables, want[r.scope])
                self.assertEqual(bool(r.flag), r.scope in ("job", "project") and r.flagged)
        # the one row of the table that has no flag, because it names the project and not a job of it
        self.assertEqual([r.name for r in jobq.SETTINGS if not r.flag and r.scope in ("job", "project")],
                         ["name"])
        for name in ("tag", "hold", "after", "cwd", "no_runner"):
            self.assertNotIn(name, {r.name for r in jobq.SETTINGS})

    def test_every_row_says_its_type_and_what_it_refuses(self):
        """A row arrives with its type and its refusals written down, so a setting cannot be added without
        saying what a user may write and what is refused."""
        kinds = {"text", "whole", "amount", "counts", "strings", "duration", "period", "regex", "command",
                 "shell", "argv", "path", "number", "boolean", "pools"}
        for r in jobq.SETTINGS:
            with self.subTest(setting=r.name, scope=r.scope):
                self.assertIn(r.kind, kinds)
                self.assertTrue(r.refuses or r.kind == "pools", "this row says nothing about what it refuses")
                if r.flag:
                    self.assertTrue(r.says, "this row's flag has no help line")

    def test_adds_flags_are_the_rows_and_nothing_else_is_a_setting(self):
        """Every per-job row has its flag on `add`, built from the row itself, and every other flag of `add`
        is an argument of one run and no setting of anything. So a flag cannot be added beside the
        declaration, which is what makes a flag and a key unable to disagree."""
        add = jobq._parser("queue", "doc")._subparsers._group_actions[0].choices["add"]
        flags = {o for action in add._actions for o in action.option_strings if o.startswith("--")}
        rows = jobq.JOB_ROWS + jobq.PROJECT_ROWS
        self.assertEqual({r.flag for r in rows} - flags, set(), "these rows have no flag on add")
        self.assertEqual(flags - {r.flag for r in rows},
                         {"--help", "--queue", "--tag", "--cwd", "--after", "--hold", "--no-start", "--from"})


class TestTheSettingsSources(QueueCase):
    """Every per-job row read from each of the sources its scope gives it, end to end, with the resolved
    value on the job's record.

    A setting with a second reader would take one source and miss another, so these are the tests that there
    is one reading of each row: the same row is given in the queue's table, in the project's own [queue] and
    on `add`'s flag, and the one that wins is the one the order names."""

    # (setting, as a config table writes it, as add's flag writes it, what the record then holds)
    SOURCES = (
        ("prio", "prio = 3", ("--prio", "4"), ("prio", 3, 4)),
        ("timeout", 'timeout = "2h"', ("--timeout", "30m"), ("timeout", 7200.0, 1800.0)),
        ("retries", "retries = 2", ("--retries", "5"), ("retries", 2, 5)),
        ("retry_delay", 'retry_delay = "1m"', ("--retry-delay", "90s"), ("retry_delay", 60.0, 90.0)),
        ("retry_if", 'retry_if = "OOM"', ("--retry-if", "reset"), ("retry_if", "OOM", "reset")),
        ("on_fail", 'on_fail = "true"', ("--on-fail", "false"), ("on_fail", "true", "false")),
        ("on_done", 'on_done = "true"', ("--on-done", "false"), ("on_done", "true", "false")),
        ("grace", 'grace = "90s"', ("--grace", "5s"), ("grace", 90.0, 5.0)),
        ("cpu", "cpu = 2", ("--cpu", "3"), ("needs.cpu", 2, 3)),
        ("mem_gb", "mem_gb = 4", ("--mem-gb", "5"), ("needs.mem_gb", 4, 5)),
        ("need", "need = { tok = 1 }", ("--need", "tok=2"), ("needs.tok", 1, 2)),
        ("env", 'env = { K = "cfg" }', ("--env", "K=flag"), ("env.K", "cfg", "flag")),
    )

    def setUp(self):
        super().setUp()
        self.resources("tok = 4")

    def project(self, text: str, where: str | None = None) -> str:
        """A project directory with that [queue] table in its .pp.toml, and its path."""
        root = where or self.sb.dir("proj")
        with open(os.path.join(root, ".pp.toml"), "w") as f:
            f.write(f'name = "proj"\n[queue]\n{text}\n')
        return root

    def held(self, jid, field: str):
        """What the record holds for a row, by the field the row lands in (`needs.cpu` is one of its needs)."""
        j = jobstore.find(jid)
        for part in field.split("."):
            j = j.get(part) if isinstance(j, dict) else None
        return j

    def test_a_row_is_read_from_the_queues_table(self):
        """Every per-job key of [queue.NAME] is that queue's default for the jobs added to it."""
        for name, table, _flag, (field, want, _won) in self.SOURCES:
            with self.subTest(setting=name):
                self.config("slots = 3", table)
                self.assertEqual(self.held(self.add(), field), want)

    def test_a_row_is_read_from_the_projects_own_queue_table(self):
        """Every per-job key of a project's `.pp.toml [queue]` is that project's default for the jobs added
        from its directory, which is what makes one project's work describable in one file."""
        for name, table, _flag, (field, want, _won) in self.SOURCES:
            with self.subTest(setting=name):
                root = self.project(table)
                self.assertEqual(self.held(self.add(cwd=root), field), want)

    def test_the_flag_wins_over_both_keys_and_the_project_over_the_queue(self):
        """The one order, over every row that has more than one source: the flag, then the project's
        .pp.toml [queue], then the queue's table, then the default."""
        for name, table, flag, (field, want, won) in self.SOURCES:
            with self.subTest(setting=name):
                self.config("slots = 3", table)
                root = self.project(table)
                self.assertEqual(self.held(self.add(cwd=root, *flag), field), won)
        # the project's key wins over the queue's, with nothing on the command line
        self.config("slots = 3", "retries = 2")
        self.assertEqual(self.held(self.add(cwd=self.project("retries = 7")), "retries"), 7)

    def test_every_row_works_on_a_line_of_a_campaign_file(self):
        """A line of a campaign file is read by `add`'s own parser, so every per-job row of the declaration
        works on a line the day its flag lands, and a line's own flag wins over the `add`'s. This is the
        whole of what makes a campaign file keep up with the settings table."""
        path = self.sb.path("campaign.txt")
        for name, _table, flag, (field, _want, won) in self.SOURCES:
            with self.subTest(setting=name):
                with open(path, "w") as fh:
                    fh.write(f"{shlex.join(flag)} -- true\n")
                rc, out, err = self.call("add", "--no-start", "--cwd", self.work, "--from", path)
                self.assertEqual(rc, 0, err)
                self.assertEqual(self.held(int(out.split()[-1]), field), won)

    def test_a_table_merges_key_by_key_and_a_scalar_is_taken_whole(self):
        """env and need merge key by key, with the same order per key, so a project adds one variable to the
        queue's without naming the rest. Every other row is taken whole from one source."""
        self.config("slots = 3", 'env = { A = "q", B = "q" }\nneed = { tok = 1 }')
        root = self.project('env = { B = "p", C = "p" }')
        j = jobstore.find(self.add("--env", "C=f", "--env", "D=f", cwd=root))
        self.assertEqual(j["env"], {"A": "q", "B": "p", "C": "f", "D": "f"})
        self.assertEqual(j["needs"], {"slots": 1, "tok": 1})

    def test_a_default_stands_where_no_source_gives_the_row(self):
        """Nothing has to be configured to start: every row has one built-in default, and that is what an
        `add` with no flag, no project and no queue table comes to."""
        j = jobstore.find(self.add())
        for r in jobq.JOB_ROWS:
            with self.subTest(setting=r.name):
                if r.on_record == "needs":
                    self.assertNotIn(r.name, j["needs"])
                else:
                    self.assertEqual(j[r.on_record], r.default)
        self.assertEqual(j["needs"], {"slots": 1})       # every job takes one slot, and that has no row

    def test_a_retry_carries_every_row_the_record_holds(self):
        """A retry is a new job of the same work, so it carries what the old one was given rather than
        resolving the settings of the day it is re-queued."""
        self.config("slots = 3", 'grace = "90s"')
        j = self.add("--timeout", "2h", "--retries", "1")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) in jobstore.ENDED)
        self.config("slots = 3", 'grace = "1s"')        # changed under it, and the retry keeps the old one
        rc, out, err = self.call("retry", str(j), "--no-start")
        self.assertEqual(rc, 0, err)
        new = jobstore.find(int(re.search(r"\b(\d+)\b", out.split("\n")[0]).group(1)))
        self.assertEqual((new["grace"], new["timeout"], new["retries"]), (90.0, 7200.0, 1))


class TestWhatASettingRefuses(QueueCase):
    """What each row refuses, refused where it is read: on the flag as a usage error, and in a config table
    as an error naming the file's key. One row, so a flag and a key refuse the same values."""

    # (how a config table writes it, how add's flag writes it, a word of the refusal). A form a source
    # cannot express is None: a flag's NAME=VALUE splits on its first =, so only a table can hold a name
    # that itself holds one.
    REFUSED = (
        ("retries = 1001", ("--retries", "1001"), "1000"),
        ("retries = -1", ("--retries", "-1"), "whole number"),
        ("retries = 1.5", ("--retries", "1.5"), "whole number"),
        ('retry_delay = "2h"', ("--retry-delay", "2h"), "3600"),
        ('timeout = ""', ("--timeout", ""), "say how long"),
        ('timeout = "soon"', ("--timeout", "soon"), "not a duration"),
        ("timeout = nan", ("--timeout", "nan"), "duration"),
        ("timeout = 1e10", ("--timeout", "1e10"), "duration"),
        ('grace = ""', ("--grace", ""), "say how long"),
        ('retry_if = "(unclosed"', ("--retry-if", "(unclosed"), "unterminated"),
        ("cpu = 0", ("--cpu", "0"), "positive number"),
        ("cpu = -2", ("--cpu", "-2"), "positive number"),
        ("cpu = nan", ("--cpu", "nan"), "positive number"),
        ("need = { tok = 0 }", ("--need", "tok=0"), "positive number"),
        ("need = 3", None, "a table"),
        ('env = { "A=B" = "v" }', None, "NUL"),
        ('env = { "A\\nPATH" = "/evil" }', ("--env", "A\nPATH=/evil"), "newline"),
        ('env = { A = "one\\ntwo" }', ("--env", "A=one\ntwo"), "newline"),
        ("prio = 1.5", ("--prio", "1.5"), "whole number"),
    )

    def test_a_flag_a_row_refuses_is_a_usage_error_and_queues_nothing(self):
        """Refused at the earliest moment: before the job is stored, so nothing is queued under a value pp
        would have to guess at later."""
        for _table, flag, word in self.REFUSED:
            if flag is None:
                continue
            with self.subTest(flag=flag):
                rc, _out, err = self.call("add", "--no-start", "--cwd", self.work, *flag, "--", "true")
                self.assertEqual(rc, 2, err)
                self.assertIn(word, err)
                self.assertNotIn("Traceback", err)
        self.assertEqual(jobstore.jobs(), [])

    def test_a_key_a_row_refuses_names_the_table_and_the_key(self):
        """A config file pp cannot read is a loud error naming it, never a silent default: `pp queue` says
        so, and while it stands the queue's worker launches nothing."""
        for table, _flag, word in self.REFUSED:
            with self.subTest(key=table):
                self.config("slots = 3", table)
                with self.assertRaises(config.ConfigError) as cm:
                    jobq._qcfg("default")
                self.assertIn("[queue.default]", str(cm.exception))
                self.assertIn(table.split(" ")[0], str(cm.exception))
                self.assertIn(word, str(cm.exception))

    def test_a_key_of_a_project_that_a_row_refuses_names_that_file(self):
        """The same reading, in the project's own table, so one file's mistake is named where it was made.
        A project's keys are read where the job is added, so nothing of that project is queued."""
        root = self.sb.dir("bad")
        with open(os.path.join(root, ".pp.toml"), "w") as f:
            f.write("[queue]\nretries = 1001\n")
        with self.assertRaises(config.ConfigError) as cm:
            self.call("add", "--no-start", "--cwd", root, "--", "true")
        self.assertIn(".pp.toml", str(cm.exception))
        self.assertIn("retries", str(cm.exception))
        self.assertEqual(jobstore.jobs(), [])

    def test_an_unknown_key_is_a_typo_in_either_table(self):
        """A key pp does not read is a typo until proven otherwise, in the queue's table and in a project's,
        and the error names the keys that table does take."""
        self.config("slots = 3", "retires = 2")
        with self.assertRaises(config.ConfigError) as cm:
            jobq._qcfg("default")
        self.assertIn("unknown retires", str(cm.exception))
        root = self.sb.dir("typo")
        with open(os.path.join(root, ".pp.toml"), "w") as f:
            f.write("[queue]\nretires = 2\n")
        with self.assertRaises(config.ConfigError) as cm:
            self.call("add", "--no-start", "--cwd", root, "--", "true")
        self.assertIn("unknown retires", str(cm.exception))

    def test_a_queues_poll_is_longer_than_nothing(self):
        """The one per-queue bound: a poll of 0 would hold the store lock as fast as the loop can ask for
        it, so it is refused where it is read."""
        for bad in ("poll = 0", "poll = -1"):
            with self.subTest(poll=bad):
                self.config("slots = 3", bad)
                with self.assertRaises(config.ConfigError):
                    jobq._qcfg("default")
        self.config("slots = 3", 'poll = "0.5"')
        self.assertEqual(jobq._qcfg("default")["poll"], 0.5)

    def test_an_environment_name_holds_no_equals_or_newline_and_a_value_one_line(self):
        """The job's environment is handed to a runner as one line per variable. A flag's NAME=VALUE splits
        on its first `=`, so `--env A=B=v` is the variable A with the value B=v, and only a table can write
        a name that holds one.

        A name holding a newline is refused for the same reason as a value holding one: it would make two
        lines of the file the runner reads out of one variable, and the second of them any name at all. What
        that file never holds is read against the name declared, so a name that smuggled a newline past it
        would put this machine's PATH in the one file whose contract is that it holds none."""
        j = jobstore.find(self.add("--env", "A=B=v"))
        self.assertEqual(j["env"], {"A": "B=v"})
        before = sorted(x["id"] for x in jobstore.jobs())
        rc, _out, err = self.call("add", "--no-start", "--cwd", self.work, "--env", "A\nPATH=/evil",
                                  "--", "true")
        self.assertEqual((rc, sorted(x["id"] for x in jobstore.jobs())), (2, before), err)
        self.config("slots = 3", 'env = { "A=B" = "v" }')
        with self.assertRaises(config.ConfigError) as cm:
            jobq._qcfg("default")
        self.assertIn("A=B", str(cm.exception))


class TestAProjectBelowTheJobsDirectory(QueueCase):
    """A job added from a directory that holds projects instead of being one: its command runs bare, which is
    a whole stack of a lab's missing, so the `add` says what is below it and how to run the job as that
    project's. The runner and the stop are the command's own part of an add (pp/commands/queue.py), so these
    go through it."""

    def setUp(self):
        super().setUp()
        queue_cmd._LOOKED.clear()       # one process runs every test, and this says it once per directory

    def cmd(self, *argv) -> tuple[int, str, str]:
        """An `add` through the command that owns the runner and the stop, as the CLI runs it."""
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                rc = jobq.run_cli(list(argv), "queue", "default", queue_cmd.__doc__,
                                  prepare_job=queue_cmd._prepare, add_args=queue_cmd._add_args)
            except SystemExit as e:
                rc = e.code
        return rc, out.getvalue(), err.getvalue()

    def a_project(self, where: str, runner: str = "./run.sh") -> str:
        root = self.sb.dir("work", where)
        with open(os.path.join(root, "run.sh"), "w") as f:
            f.write('#!/bin/sh\nexec "$@"\n')
        os.chmod(os.path.join(root, "run.sh"), 0o755)
        with open(os.path.join(root, ".pp.toml"), "w") as f:
            f.write(f'[queue]\nrunner = "{runner}"\nstop = "group"\n')
        return root

    def test_an_add_above_a_project_queues_the_job_and_names_the_project(self):
        root = self.a_project("lab")
        rc, out, err = self.cmd("add", "--no-start", "--cwd", self.work, "--", "true")
        self.assertEqual(rc, 0, err)
        self.assertEqual(jobstore.find(int(out.split()[-1]))["runner"], [])      # queued as it asked
        self.assertIn(root, err)                    # the directory below, and the flag that runs it there
        self.assertIn("--cwd", err)

    def test_the_same_directory_is_named_once(self):
        self.a_project("lab")
        self.cmd("add", "--no-start", "--cwd", self.work, "--", "true")
        rc, _out, err = self.cmd("add", "--no-start", "--cwd", self.work, "--", "true")
        self.assertEqual((rc, "--cwd" in err), (0, False), err)

    def test_a_directory_with_no_project_under_it_says_nothing(self):
        self.sb.dir("work", "data")
        with open(self.sb.path("work", "data", ".pp.toml"), "w") as f:
            f.write('name = "data"\n')        # a project that puts no runner before a command
        self.sb.dir("work", "deeper", "lab")
        self.a_project(os.path.join("deeper", "lab"))        # two levels down, which this does not read
        rc, _out, err = self.cmd("add", "--no-start", "--cwd", self.work, "--", "true")
        self.assertEqual((rc, "--cwd" in err), (0, False), err)

    def test_a_project_file_that_does_not_parse_is_skipped(self):
        bad = self.sb.dir("work", "broken")
        with open(os.path.join(bad, ".pp.toml"), "w") as f:
            f.write("[queue\nrunner =\n")
        root = self.a_project("lab")
        rc, out, err = self.cmd("add", "--no-start", "--cwd", self.work, "--", "true")
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.state(int(out.split()[-1])), "queued")
        self.assertIn(root, err)

    @unittest.skipIf(os.geteuid() == 0, "root may read every file")
    def test_a_project_file_that_cannot_be_read_is_skipped(self):
        """A file this may not open is one of the tree, like one that does not parse: the scan goes on to the
        next directory, and the job is queued as it asks."""
        shut = self.sb.dir("work", "closed")        # before "lab", so the scan reads it first
        with open(os.path.join(shut, ".pp.toml"), "w") as f:
            f.write('[queue]\nrunner = "./run.sh"\n')
        os.chmod(os.path.join(shut, ".pp.toml"), 0)
        root = self.a_project("lab")
        rc, out, err = self.cmd("add", "--no-start", "--cwd", self.work, "--", "true")
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.state(int(out.split()[-1])), "queued")
        self.assertIn(root, err)

    def test_a_job_of_the_project_itself_says_nothing(self):
        root = self.a_project("lab")
        rc, out, err = self.cmd("add", "--no-start", "--cwd", root, "--", "true")
        self.assertEqual(rc, 0, err)
        self.assertEqual(jobstore.find(int(out.split()[-1]))["runner"], [f"{root}/run.sh"])
        self.assertNotIn("--cwd", err)

    def test_no_runner_asks_for_a_bare_job_and_is_answered(self):
        self.a_project("lab")
        rc, _out, err = self.cmd("add", "--no-start", "--no-runner", "--cwd", self.work, "--", "true")
        self.assertEqual((rc, "--cwd" in err), (0, False), err)


class TestTheJobsGrace(QueueCase):
    """`grace` is one setting of the job and one number: the gap between a stop of it and the SIGKILL that
    follows, whoever asked. A timeout's stop takes the job's own, `kill` takes it too, and `kill --grace`
    overrides it for the jobs that kill names."""

    def test_a_timeouts_stop_carries_the_jobs_own_grace(self):
        """Not a constant of pp's: a container project that wants a longer shutdown sets `grace` once and
        both a timeout and a kill of its jobs take it."""
        j = self.add("--timeout", "60", "--grace", "90s", cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        self.started_earlier(j, 120)        # its time is up, so the next tick writes a timeout's stop
        w.tick()
        stop = jobstore.find(j)["stop"]
        self.assertEqual((stop["by"], stop["timeout"], stop["grace"]), ("timeout", True, 90.0))

    def test_a_kill_takes_the_jobs_own_grace_and_grace_overrides_it(self):
        """A kill runs from any directory against jobs of any project, so the record is the only honest
        source for the grace; `--grace` is what overrides it, and nothing else does."""
        a = self.add("--grace", "90s", cmd="sleep 30")
        b = self.add("--grace", "90s", cmd="sleep 30")
        w = jobq.worker_for("default")
        w.tick()
        with self.one_thread():
            self.call("kill", str(a), "-y")
            self.assertEqual(jobstore.find(a)["stop"]["grace"], 90.0)
            self.call("kill", str(b), "--grace", "0.2", "-y")
            self.assertEqual(jobstore.find(b)["stop"]["grace"], 0.2)

    def test_the_grace_of_a_record_written_before_the_row_was_one(self):
        """A job queued by an older pp carries no grace, and the built-in default stands for it, so an
        untouched project sees the same ten seconds it saw before. A grace of 0 on the record is that job's
        own answer: the SIGKILL follows the stop at once."""
        job = {"id": 1, "queue": "default", "timeout": 60.0, "started": 0.0}
        self.assertEqual(jobpolicy.grace(job), 10.0)
        self.assertEqual(jobpolicy.grace({**job, "grace": None}), 10.0)
        self.assertEqual(jobpolicy.grace({**job, "grace": 0}), 0.0)
        self.assertEqual(jobpolicy.grace({**job, "grace": 2.5}), 2.5)


class TestWhatTheTextNames(QueueCase):
    """A sentence that names a file or a variable is read as the thing to go and look for, so each one is
    checked against the code that writes it."""

    def test_the_help_names_every_variable_that_is_captured(self):
        said = [ln for ln in queue_cmd.__doc__.splitlines()
                if "captured at add time" in ln or "from the shell that added the job" in ln]
        self.assertEqual(len(said), 2, said)
        for line in said:
            for name in jobq.CAPTURED_ENV:
                self.assertIn(name, line, f"{name} is captured and this sentence leaves it out: {line}")

    def test_the_json_shapes_name_every_field_they_publish(self):
        """`--json` is a contract an agent reads, so both shapes are stated once, in `cmd_status`'s and
        `cmd_worker`'s docstrings. A field published with no line there, or a line left behind by a field
        that went, fails here."""
        def named(doc: str) -> set:
            """The field names a shape's docstring lists: each line of the block, up to its description.

            `cleandoc` first: Python 3.13 and later strip a docstring's common indentation when they compile
            it, so the block is indented by four spaces inside the docstring either way."""
            doc = inspect.cleandoc(doc)
            return {n for names in re.findall(r"^ {4}(\w[\w, ]*?) {2,}\S", doc, re.M) for n in names.split(", ")}

        self.add("--hold", cmd="true")
        status = json.loads(self.call("status", "--json")[1])
        self.assertEqual(set(status), named(jobq.cmd_status.__doc__))
        rows = json.loads(self.call("worker", "status", "-q", "default", "--json")[1])
        self.assertEqual(set(rows[0]), named(jobq.cmd_worker.__doc__))

    def test_the_watchers_reference_names_the_file_a_lease_is_held_on(self):
        """A contributor looking for a stale lease goes by the name the reference gives, so that name is the
        one a `wait` really holds its lease on."""
        j = self.add("--hold", cmd="true")
        with jobcore.watching([j], None, "wait"):
            leases = os.listdir(os.path.join(self.sb.state, "queue", "watchers"))
        self.assertEqual(len(leases), 1, leases)
        self.assertRegex(leases[0], r"^\d+-\d+\.claim$")        # <pid>-<pstart>.claim, as it is written
        with open(os.path.join(ROOT, "pp", "lib", "jobcore.py")) as f:
            src = f.read()
        self.assertIn("watchers/<pid>-<pstart>.claim", src)
        self.assertNotIn("watchers/<pid>.json", src)


class TestWatchEvents(unittest.TestCase):
    """watch polls: when a change came and went between two looks, it still says every step."""

    def ev(self, was, now, **kw):
        return jobq._event({"state": was, "attempt": 1}, {"state": now, "attempt": 1, "tag": "t", **kw}, 7)

    def test_steps_a_poll_missed_are_said_in_order(self):
        self.assertEqual(self.ev("held", "queued"), "7 released  t")
        self.assertEqual(self.ev("held", "running"), "7 released  t\n7 started  t")
        self.assertEqual(self.ev("held", "done"), "7 released  t\n7 started  t")
        self.assertEqual(self.ev("queued", "failed"), "7 started  t")
        self.assertEqual(self.ev("queued", "held"), "7 held  t")
        self.assertIsNone(self.ev("running", "done"))           # the outcome line says it
        self.assertIsNone(self.ev("queued", "queued"))


# ---- one test per mechanism invariant, by fault injection -------------------------------------------
#
# What follows is one test per invariant the mechanism keeps, each by a fault at a named step: a worker that
# ends where a SIGKILL would end it, two processes that act at once, a record naming a process id another
# process holds, a reader of the store beside a writer of it. The processes the tests need come first, each a
# program of a few lines, then the scope they share (InjectedCase), then the tests.


# ---- one test per mechanism invariant, by fault injection -------------------------------------------
#
# What follows is one test per invariant the mechanism keeps, each by a fault at a named step: a worker that
# ends where a SIGKILL would end it, two processes that act at once, a record naming a process id another
# process holds, a reader of the store beside a writer of it. The processes the tests need come first, each a
# program of a few lines, then the scope they share (InjectedCase), then the tests.


# ---- one test per mechanism invariant, by fault injection -------------------------------------------
#
# What follows is one test per invariant the mechanism keeps, each by a fault at a named step: a worker that
# ends where a SIGKILL would end it, two processes that act at once, a record naming a process id another
# process holds, a reader of the store beside a writer of it. The processes the tests need come first, each a
# program of a few lines, then the scope they share (InjectedCase), then the tests.


# ---- one test per mechanism invariant, by fault injection -------------------------------------------
#
# What follows is one test per invariant the mechanism keeps, each by a fault at a named step: a worker that
# ends where a SIGKILL would end it, two processes that act at once, a record naming a process id another
# process holds, a reader of the store beside a writer of it. The processes the tests need come first, each a
# program of a few lines, then the scope they share (InjectedCase), then the tests.


# ---- one test per mechanism invariant, by fault injection -------------------------------------------
#
# What follows is one test per invariant the mechanism keeps, each by a fault at a named step: a worker that
# ends where a SIGKILL would end it, two processes that act at once, a record naming a process id another
# process holds, a reader of the store beside a writer of it. The processes the tests need come first, each a
# program of a few lines, then the scope they share (InjectedCase), then the tests.


# ---- one test per mechanism invariant, by fault injection -------------------------------------------
#
# What follows is one test per invariant the mechanism keeps, each by a fault at a named step: a worker that
# ends where a SIGKILL would end it, two processes that act at once, a record naming a process id another
# process holds, a reader of the store beside a writer of it. The processes the tests need come first, each a
# program of a few lines, then the scope they share (InjectedCase), then the tests.

AT_THE_STEP = 97        # the exit status of a worker of a test's own that ended at the step it was given

# A worker of one queue, in a process of its own, which ends with os._exit at a named step of pp's. That is
# how a SIGKILL of a worker is injected: no way out of its own runs, and the kernel lets go of every claim it
# held, exactly as it does for a worker the machine loses. The step is a call, never a moment of the clock, so
# the fault lands between the same two calls on every machine. What went out for a stop is noted in a file,
# for the test that asks what was signalled.
A_WORKER_KILLED_AT = '''
import os
import sys

sys.path.insert(0, sys.argv[1])
from pp.lib import jobpolicy, jobcore, jobq, jobstore, proc

STEP, Q, NOTES, PASSES = sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
AT_THE_STEP = 97            # the same number the test that started this worker reads


def note(what):
    with open(NOTES, "a") as f:
        f.write(what + "\\n")


def die(*a, **kw):
    os._exit(AT_THE_STEP)


def die_at_the_run_claim(path, record=None):
    if str(path).endswith(".run"):
        die()
    return take(path, record)


def start_then_die(*a, **kw):
    spawn(*a, **kw)
    die()


def note_then_die(h, sig):
    note("sent %d" % sig)
    die()


def send_then_die(h, sig):
    out = signal_group(h, sig)
    note("sent %d %s" % (sig, out))
    die()


def read_then_die(job):
    code = ended_on_its_own(job)
    note("code %s" % (None if code is None else code[0]))
    die()


take, spawn, signal_group, ended_on_its_own = (jobq.lock.Claim.take, proc.spawn, proc.signal_group,
                                               jobcore.ended_on_its_own)
if STEP == "the run claim":
    jobq.lock.Claim.take = die_at_the_run_claim
elif STEP == "the record":
    jobcore.Worker._claim = die
elif STEP == "the spawn":
    jobcore.Worker._spawn = die
elif STEP == "the start":
    proc.spawn = start_then_die
elif STEP == "the send":
    jobcore.Worker._send = die
elif STEP == "the signal":
    proc.signal_group = note_then_die
elif STEP == "the signal of the group":
    proc.signal_group = send_then_die
elif STEP == "the exit code":
    jobcore.ended_on_its_own = read_then_die
elif STEP == "the hook":
    jobcore.Worker.fire = die
elif STEP == "the log line":
    jobq._append_log = die
else:
    raise SystemExit("no step of a worker is called %r" % STEP)

jobq._cards = lambda: None      # the cards of the machine this runs on are nothing to a test
w = jobq.worker_for(Q)
for _ in range(PASSES):
    w.tick()
note("the step never came")
os._exit(1)
'''

# A reader of the store beside a test, which lists every queue over and over while another process writes
# them, and says what it saw: one line per pass, each the (id, state) of every job it found. It ends once the
# job it was given has ended, and at its deadline whatever happens, so it is bounded however the test goes.
A_READER_OF_THE_STORE = '''
import json
import sys
import time

sys.path.insert(0, sys.argv[1])
from pp.lib import jobpolicy, jobcore, jobq, jobstore

OUT, JID, DEADLINE = sys.argv[2], int(sys.argv[3]), time.monotonic() + 60
with jobstore.listing() as s:               # one look before it says it is up, so the test writes after it
    rows = [[(x["id"], x["state"]) for x in s.jobs()]]
print("reading", flush=True)
while time.monotonic() < DEADLINE:
    with jobstore.listing() as s:
        rows.append([(x["id"], x["state"]) for x in s.jobs()])
    if any(i == JID and st in jobstore.ENDED for i, st in rows[-1]):
        break
with open(OUT, "w") as f:
    json.dump(rows, f)
sys.exit(0)
'''

# An add that waits for a gate the test opens, so several of them ask the store for an id at once.
AN_ADD_THAT_WAITS_FOR_A_GATE = '''
import os
import sys
import time

sys.path.insert(0, sys.argv[1])
from pp.lib import jobpolicy, jobcore, jobq, jobstore

GATE, CWD, DEADLINE = sys.argv[2], sys.argv[3], time.monotonic() + 60
print("ready", flush=True)
while not os.path.exists(GATE):
    if time.monotonic() > DEADLINE:
        sys.exit(2)
    time.sleep(0.01)
sys.exit(jobq.run_cli(["add", "--no-start", "--cwd", CWD, "--", "true"], "queue", "default", "a test's doc"))
'''

# A process that wants a queue to have a worker, waiting for a gate so that several of them ask at once. Its
# exit status says whether a worker was up when its own ask came back.
A_CALLER_THAT_WANTS_A_WORKER = '''
import os
import sys
import time

sys.path.insert(0, sys.argv[1])
from pp.lib import jobpolicy, jobcore, jobq, jobstore

GATE, Q, DEADLINE = sys.argv[2], sys.argv[3], time.monotonic() + 60
print("ready", flush=True)
while not os.path.exists(GATE):
    if time.monotonic() > DEADLINE:
        sys.exit(2)
    time.sleep(0.01)
sys.exit(0 if jobq._worker_up(Q, quiet=True).up else 3)
'''


class InjectedCase(QueueCase):
    """What a test of one mechanism invariant by fault injection needs: a worker in a process of its own that
    ends at a named step, the ask a kill writes, a command that runs until a file is there, and the signals a
    worker sends recorded in place of going out.

    Every fault lands at a named call of pp's. So a test says which window it is about instead of hoping a
    sleep lands in it, and it fails every time without the construct it guards."""

    def setUp(self):
        super().setUp()
        self.noted: list = []       # the files the workers of this test noted what they did in

    def a_worker_killed_at(self, step: str, passes: int = 1, q: str = "default") -> list[str]:
        """A worker of queue q which ticks `passes` times and ends with os._exit at `step`. -> what it noted
        before it went, one line each.

        Its exit status is the one assertion that the fault landed where the test named it: a worker that
        reached the end of its passes says so and exits with another."""
        notes = self.sb.path(f"what-a-worker-did-{len(self.noted)}")
        self.noted.append(notes)
        r = subprocess.run([sys.executable, "-c", A_WORKER_KILLED_AT, ROOT, step, q, notes, str(passes)],
                           cwd=ROOT, capture_output=True, text=True, timeout=120)
        said = text(notes).splitlines() if os.path.exists(notes) else []
        self.assertEqual(r.returncode, AT_THE_STEP,
                         f"no worker of this test ended at {step}: {said}\n{r.stdout}{r.stderr}")
        return said

    def a_kill_asks(self, jid: int, grace: float = 10.0, by: str = "user") -> dict:
        """The ask a kill writes on a running job, merged into the one stop that run carries. -> that stop."""
        with jobstore.txn() as s:
            cur = s.job(jid, must_be=("running",))
            self.assertIsNotNone(cur, f"job {jid} is not running")
            cur["stop"] = jobpolicy.merge_stop(cur.get("stop"), {"by": by, "t": time.time(), "grace": grace})
            s.save(cur)
            return dict(cur["stop"])

    def hook_file(self) -> str:
        """Where a hook of this test's appends one line each time it runs."""
        return self.sb.path("what-a-hook-wrote")

    def ran(self, path: str) -> int:
        """How many times a hook that appends one line has run."""
        return text(path).count("x") if os.path.exists(path) else 0

    def lines(self, path: str) -> list[str]:
        """What each run of a command that appends one line wrote."""
        return text(path).split() if os.path.exists(path) else []

    def gate(self, name: str = "go") -> str:
        """The path a gated command of this test waits for."""
        return os.path.join(self.work, name)

    def gated(self, name: str = "go") -> str:
        """A command that runs until this test opens that gate, then exits 0."""
        return f"while [ ! -f {self.gate(name)} ]; do sleep 0.02; done"

    def open_the_gate(self, name: str = "go") -> None:
        open(self.gate(name), "w").close()

    def nothing_of_the_job_is_left(self, w: jobcore.Worker, jid: int) -> None:
        """This test's own way out for a job its own worker started: the group the job leads goes, and the
        wrapper that worker holds is waited for. So a test that drops its worker with a live child leaves no
        process of its own behind and none to be collected unreaped.

        Called while the job is running, since that is when its record names the group."""
        group = jobcore._group_handle(jobstore.find(jid))

        def done():
            if group is not None:
                proc.signal_group(group, signal.SIGKILL)
            p = w.children.pop(jid, None)
            if p is not None:
                p.wait(timeout=30)
        self.addCleanup(done)

    def the_run_is_live(self, jid: int, q: str = "default") -> dict:
        """The record in job jid's run claim, waited for: its wrapper writes its own pid in there, and a held
        claim is what a live run is."""
        return journey.until(lambda: jobq.lock.Claim.holder(jobstore.run_claim(q, jid)), timeout=20,
                             what=f"job {jid}'s run to hold its claim")

    def the_run_is_over(self, jid: int, q: str = "default") -> None:
        """Wait until nothing holds job jid's run claim, which is what a run being over means."""
        journey.until(lambda: jobq.lock.Claim.holder(jobstore.run_claim(q, jid)) is None, timeout=20,
                      what=f"job {jid}'s run to be over")

    @staticmethod
    def sender(went: list, outcome: str = "alive", beside=None):
        """A sender of a test's own: it records the signal in `went` and sends none, so the job's run stays
        live and the test reads what went out and in which order. What a test of a stop asks is exactly that.
        beside: read beside each signal, for a test that asks what the record said as the signal went out.
        -> what proc.signal_group is patched with."""
        def send(h, sig):
            went.append(sig if beside is None else (sig, beside()))
            return outcome
        return send

    def recording(self, outcome: str = "alive", beside=None) -> list:
        """That sender, in place for the whole test. -> the list the signals land in."""
        went: list = []
        patch = mock.patch.object(jobq.proc, "signal_group", self.sender(went, outcome, beside))
        patch.start()
        self.addCleanup(patch.stop)     # before the case's own way out, which signals what is left running
        return went


class TestAWorkerKilledWhileItLaunches(InjectedCase):
    """The four steps of a launch, with the worker ending between two of them each time: what the store holds
    then, and what the next worker makes of it."""

    def test_a_worker_killed_before_the_run_claim_leaves_the_job_queued(self):
        """A record changes only inside the store's transaction, so a worker that ends between the admission
        of a job and the claim on its run leaves that transaction unmade: the job is queued, it holds the one
        id it was given, and no run of it was ever claimed. The next worker starts it."""
        j = self.add(cmd="true")
        self.a_worker_killed_at("the run claim")
        self.assertEqual(self.state(j), "queued")
        self.assertIsNone(jobq.lock.Claim.holder(jobstore.run_claim("default", j)))
        self.assertEqual([x["id"] for x in jobstore.jobs()], [j])       # one record, under one id
        self.assertIsNone(jobstore.find(j).get("rcfile"))
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "done")

    def test_a_worker_killed_holding_the_claim_leaves_no_running_job_unclaimed(self):
        """A job's run claim is held before its record says running. A worker that ends between the two
        leaves the job queued and the claim free, so a reader finds no running job whose claim nothing holds
        and whose run is not over."""
        j = self.add(cmd="true")
        self.a_worker_killed_at("the record")
        self.assertEqual(self.state(j), "queued")
        self.assertIsNone(jobq.lock.Claim.holder(jobstore.run_claim("default", j)))
        self.assertEqual(jobstore.jobs("running"), [])
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "done")

    def test_a_worker_killed_before_the_spawn_leaves_a_job_the_next_worker_reaps(self):
        """A pass marks every job it admitted running before it starts any of them. A worker that ends
        between the record and the start leaves a running job whose claim nobody holds, which is a run that
        is over: the next worker records it, and the exit code it reads is none, since the attempt wrote
        none."""
        j = self.add(cmd="true")
        self.a_worker_killed_at("the spawn")
        self.assertEqual(self.state(j), "running")
        self.assertIsNone(jobq.lock.Claim.holder(jobstore.run_claim("default", j)))
        self.assertIsNone(jobcore.ended_on_its_own(jobstore.find(j)))
        jobq.worker_for("default").carry_out()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec["reason"]), ("failed", None, "lost"))

    def test_a_job_whose_worker_died_between_the_spawn_and_the_record_is_adopted(self):
        """The attempt's wrapper writes its own pid in the claim it is handed, so a worker that ends between
        the start and the record leaves a running job whose run is live and whose record names no process.
        The next worker adopts it from that claim: the pid, the group, the start time read beside it and the
        boot both were read in land on the record, and the job is reaped like any other."""
        j = self.add(cmd=self.gated())
        self.a_worker_killed_at("the start")
        held = self.the_run_is_live(j)
        self.assertEqual((self.state(j), jobstore.find(j).get("pid")), ("running", None))
        w = jobq.worker_for("default")
        w.carry_out()                   # the claim says which process the run is, and the record takes it
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["pid"], rec["boot"]), ("running", held["pid"], proc.boot_id()))
        self.assertEqual(rec["pgid"], proc.read(held["pid"]).pgid)
        self.assertIsNotNone(jobcore._group_handle(rec), "the record names no group a signal could go through")
        self.open_the_gate()
        self.the_run_is_over(j)
        self.run_until(w, lambda: self.state(j) == "done")
        self.assertEqual(jobstore.find(j)["rc"], 0)

    def test_an_adopted_attempt_knows_where_its_own_output_begins(self):
        """The launch hands the wrapper the offset its own output starts at, and the wrapper writes it in the
        claim beside its pid. So a worker that ends between the start and the record leaves both to be
        adopted, and the retry filter of an adopted attempt reads that attempt's own lines rather than
        starting at the file and matching the line pp wrote before it."""
        j = self.add(cmd=f"echo a-line-of-the-jobs-own; {self.gated()}")
        self.a_worker_killed_at("the start")
        held = self.the_run_is_live(j)
        self.assertIsNone(jobstore.find(j).get("log_at"))        # recorded with the pid, and neither is
        w = jobq.worker_for("default")
        w.carry_out()
        rec = jobstore.find(j)
        self.assertEqual(rec["log_at"], held["log_at"])
        self.assertGreater(rec["log_at"], 0)                     # past the banner the launch wrote
        journey.until(lambda: jobq.log_tail(rec, 40), timeout=10, what="the job's own first line")
        self.assertEqual(jobq.log_tail(rec, 40), ["a-line-of-the-jobs-own"])
        self.open_the_gate()
        self.the_run_is_over(j)
        self.run_until(w, lambda: self.state(j) == "done")


class TestAWorkerKilledWhileItStops(InjectedCase):
    """The four steps of a stop, with the worker ending between two of them each time: what the record says
    went out, what really went out, and what the next worker does about the rest of it."""

    def a_job_whose_run_is_live(self, *args, cmd=None, cwd=None) -> tuple:
        """A running job of this test's, with its run live and this test's worker holding its wrapper. -> the
        job's id and that worker."""
        j = self.add(*args, cmd=cmd or self.gated(), cwd=cwd)
        w = jobq.worker_for("default")
        w.tick()
        self.assertEqual(self.state(j), "running")
        self.the_run_is_live(j)
        self.nothing_of_the_job_is_left(w, j)
        return j, w

    def test_a_stop_asked_and_never_sent_goes_out_from_the_next_worker(self):
        """A stop is written in one transaction and carried out by the queue's worker. A worker that ends
        between the decision and the signal leaves a record that says nothing went out, so the next worker
        sends it: one stop, one signal, and no pass after that one sends it again."""
        j, w = self.a_job_whose_run_is_live()
        asked = self.a_kill_asks(j, grace=10.0)
        self.a_worker_killed_at("the send")
        stop = jobstore.find(j)["stop"]
        self.assertEqual((stop["t"], stop.get("sent"), stop.get("kill9"), stop.get("denied")),
                         (asked["t"], None, None, None))
        self.assertEqual(self.state(j), "running")
        went = self.recording()
        w.carry_out()
        self.assertEqual(went, [signal.SIGTERM])
        self.assertIsNotNone(jobstore.find(j)["stop"]["sent"])
        for _ in range(3):              # inside the grace it was given, so nothing more goes out for it
            w.carry_out()
        self.assertEqual(went, [signal.SIGTERM])

    def test_a_signal_that_went_out_is_recorded_before_anything_else_goes_out(self):
        """What went out for a stop is recorded after it went out, so a record that says a signal went out is
        a record of one that did. A worker that ends between the signal and the record leaves a stop with
        nothing marked on it, which is the one side of that order a decision can be wrong on."""
        j, w = self.a_job_whose_run_is_live()
        asked = self.a_kill_asks(j, grace=10.0)
        self.assertEqual(self.a_worker_killed_at("the signal"), [f"sent {int(signal.SIGTERM)}"])
        stop = jobstore.find(j)["stop"]
        self.assertEqual((stop["t"], stop.get("sent"), stop.get("kill9")), (asked["t"], None, None))
        went = self.recording(beside=lambda: jobstore.find(j)["stop"].get("sent"))
        w.carry_out()
        self.assertEqual(went, [(signal.SIGTERM, None)])        # the record was marked after this went out
        self.assertIsNotNone(jobstore.find(j)["stop"]["sent"])

    def test_a_stop_command_that_could_not_start_leaves_the_stop_to_pps_own_signal(self):
        """What went out for a stop is recorded after it went out, so a record that says a signal went out is
        a record of one that did. A project's stop command that cannot start sends nothing: pp's own signal
        goes out for that stop instead, and the code of a command that could not run is recorded for it, so
        the record says the project's stop failed rather than saying a stop went out that never did."""
        ran = self.sb.path("what-the-stop-command-did")
        sub = os.path.join(self.work, "sub")
        os.mkdir(sub)
        j, w = self.a_job_whose_run_is_live(cwd=sub)
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop_cmd"] = f"echo x >> {ran}"
            s.save(cur)
        os.rmdir(sub)           # the job's own directory, which that command starts in, is gone
        self.a_kill_asks(j, grace=10.0)
        went = self.recording()
        w.carry_out()
        stop = jobstore.find(j)["stop"]
        self.assertEqual(went, [signal.SIGTERM])        # what the record says went out is what did
        self.assertIsNotNone(stop["sent"])
        self.assertEqual(stop["rc"], 127)
        self.assertNotIn("cmd", stop, "the record names a command as what went out, and none went out")
        self.assertFalse(os.path.exists(ran), "a command that could not start ran")

    def test_the_stop_carries_the_command_that_went_out_for_it(self):
        """What went out for a stop is on the stop itself, as `cmd`: the project's command where that is what
        went out, and nothing of the kind where pp's own signal went. The job's configured command cannot
        answer this, since a command that could not start ran nothing and pp's signal went out for that stop
        instead, and the code 127 is the same either way: the field is what tells the two apart."""
        ran = self.sb.path("what-the-stop-command-did")
        cmd = f"echo x >> {ran}"
        j, w = self.a_job_whose_run_is_live()
        # and a second job of the same queue, whose work is in its own process group: both runs are live
        # before anything is asked of either, so this test's way out signals each of them itself
        k, _w2 = self.a_job_whose_run_is_live(cmd=self.gated("go2"))
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop_cmd"] = cmd
            s.save(cur)
        self.a_kill_asks(j, grace=10.0)
        went = self.recording()
        w.carry_out()
        stop = jobstore.find(j)["stop"]
        self.assertEqual(went, [])                      # the command went out in place of the SIGTERM
        self.assertEqual(stop["cmd"], cmd)
        self.assertIsNotNone(stop["sent"])
        journey.until(lambda: self.lines(ran) == ["x"], timeout=30, what="the stop command this stop ran")
        self.a_kill_asks(k, grace=10.0)
        w.carry_out()
        group = jobstore.find(k)["stop"]
        self.assertEqual(went, [signal.SIGTERM])        # pp's own signal went out for that one
        self.assertIsNotNone(group["sent"])
        self.assertNotIn("cmd", group)

    def test_a_code_written_while_the_stop_goes_out_is_that_stops_outcome(self):
        """A stop decides the reason, and what an exit code is late against is the time that stop went out.
        That time is read before anything goes out for it, so the window in which the project's stop command
        is starting belongs to the stop: a container runner writing the code of the container that command
        ended (137) inside it is that stop's outcome, with the code kept, and not the job's own failure.

        The window is driven by a named step, the start of the project's stop command, so the test says
        which window it is about instead of hoping a sleep lands in it."""
        j, w = self.a_job_whose_run_is_live()
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop_cmd"] = "true"        # the project's command, whose start is the step below
            s.save(cur)
        real, wrote = jobcore.Worker._stop, []

        def the_code_lands_as_the_command_starts(wk, job, cmd):
            at = time.time()        # inside the window: the stop's own time was read before this call

            def written():
                """The code, under a timestamp the file system reports as at or past `at`. A file's
                timestamp is coarser than the clock, so this is what makes the file say what the step
                already decided: the code was written after the stop's own time was read."""
                with open(job["rcfile"], "w") as f:
                    f.write("137\n")
                return os.stat(job["rcfile"]).st_mtime >= at
            journey.until(written, timeout=10, every=0.01, what="an exit code timed inside the window")
            wrote.append(os.stat(job["rcfile"]).st_mtime)
            return real(wk, job, cmd)
        self.a_kill_asks(j, grace=60.0)
        with mock.patch.object(jobcore.Worker, "_stop", the_code_lands_as_the_command_starts):
            w.carry_out()
        stop = jobstore.find(j)["stop"]
        self.assertEqual(len(wrote), 1)
        self.assertIsNotNone(stop["sent"])
        self.assertLessEqual(stop["sent"], wrote[0])    # read before the stop went out, recorded once it had
        # the run ends with no code of its own, so the code written inside the window is the one that is read
        proc.signal_group(jobcore._group_handle(jobstore.find(j)), signal.SIGKILL)
        self.the_run_is_over(j)
        w.carry_out()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["reason"], rec["rc"]), ("failed", "killed", 137))
        self.assertIsNone(rec.get("history"), "a job a kill has touched was retried")

    def test_a_grace_that_is_up_escalates_once_and_only_once(self):
        """The SIGKILL of a stop whose grace is up goes out once. A worker that ends between the grace being
        up and that signal leaves a stop with no SIGKILL marked, the next worker sends it, and no pass after
        that one sends another."""
        j, w = self.a_job_whose_run_is_live()
        self.a_kill_asks(j, grace=0.0)
        went = self.recording()
        w.carry_out()                                   # the SIGTERM, and its grace is up at once
        self.assertEqual(went, [signal.SIGTERM])
        self.assertIsNotNone(jobstore.find(j)["stop"]["sent"])
        self.a_worker_killed_at("the send")
        self.assertIsNone(jobstore.find(j)["stop"].get("kill9"))
        w.carry_out()
        self.assertEqual(went, [signal.SIGTERM, signal.SIGKILL])
        kill9 = jobstore.find(j)["stop"]["kill9"]
        self.assertIsNotNone(kill9)
        for _ in range(3):
            w.carry_out()
        self.assertEqual(went, [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(jobstore.find(j)["stop"]["kill9"], kill9)

    def test_a_kill9_that_went_out_is_never_sent_twice(self):
        """A worker that ends between the SIGKILL of a job's group and the record of it leaves a stop with no
        SIGKILL marked and a run that is over. Nothing goes out for that stop again: the next worker records
        how the job ended, and the one signal it sends is the sweep, which nobody asked for and which records
        nothing."""
        j, w = self.a_job_whose_run_is_live()
        self.a_kill_asks(j, grace=0.0)
        went = self.recording()
        w.carry_out()
        self.assertEqual(went, [signal.SIGTERM])
        self.assertIsNotNone(jobstore.find(j)["stop"]["sent"])
        self.assertEqual(self.a_worker_killed_at("the signal of the group"),
                         [f"sent {int(signal.SIGKILL)} alive"])
        self.assertIsNone(jobstore.find(j)["stop"].get("kill9"))
        self.the_run_is_over(j)
        went.clear()                                    # what goes out from here is the next worker's alone
        w.carry_out()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["reason"], rec.get("rc")), ("failed", "killed", None))
        self.assertIsNone(rec["stop"].get("kill9"), "the record says a SIGKILL of pp's went out twice")
        self.assertEqual(went, [signal.SIGKILL])        # the sweep of its group, inside the decision's scope
        for _ in range(3):
            w.carry_out()
        self.assertEqual(went, [signal.SIGKILL])

    def test_a_worker_that_died_holding_a_stop_leaves_the_stop_to_the_next_one(self):
        """A stop in flight is the queue's worker's to finish, whatever worker began it. A kill's ask lands on
        the stop a timeout wrote, the SIGTERM goes out, and the worker ends before the rest of it: the next
        worker finishes that stop, and the job it ends is never retried, however its stop began."""
        j, w = self.a_job_whose_run_is_live("--retries", "2", "--timeout", "1")
        self.started_earlier(j, 5)                  # its own time is up, so the worker writes its stop
        went: list = []
        with mock.patch.object(jobq.proc, "signal_group", self.sender(went)):
            w.carry_out()
            stop = jobstore.find(j)["stop"]
            self.assertEqual((stop["by"], stop["timeout"]), ("timeout", True))
            asked = self.a_kill_asks(j, grace=0.0)
            self.assertEqual((asked["by"], asked.get("was"), asked.get("timeout")), ("user", "timeout", None))
            w.carry_out()
            self.assertEqual(went, [signal.SIGTERM])
            self.a_worker_killed_at("the send")     # with the stop in flight and its grace up
            self.assertIsNone(jobstore.find(j)["stop"].get("kill9"))
            self.assertEqual(self.state(j), "running")
        w.carry_out()                               # the next worker finishes it, for real this time
        self.assertIsNotNone(jobstore.find(j)["stop"]["kill9"])
        self.the_run_is_over(j)
        w.carry_out()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["reason"], rec["attempt"]), ("failed", "killed", 1))
        self.assertIsNone(rec.get("history"), "a job a kill has touched was retried")
        self.assertEqual(rec["stop"].get("was"), "timeout")


class TestAWorkerKilledWhileItReaps(InjectedCase):
    """The four steps of a reap, with the worker ending between two of them each time: the exit code, the
    outcome, the hook that outcome owes, and the requeue of an attempt that has another left."""

    def test_an_exit_code_read_before_a_worker_died_is_read_again_and_once(self):
        """The exit code an attempt wrote is read once its run is over, and it is final: it lives in a file of
        that attempt's own, written under another name and renamed into place. A worker that reads it and ends
        before recording it leaves the record as it was, and the next worker reads the same code and records
        that outcome once."""
        f = self.hook_file()
        j = self.add("--on-fail", f"echo x >> {f}", cmd="exit 7")
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        rcfile = jobstore.find(j)["rcfile"]
        self.assertEqual(os.path.basename(rcfile).split(".")[:2], [str(j), "1"])    # this attempt's own name
        self.assertFalse(os.path.exists(rcfile + ".part"))
        self.assertEqual(self.a_worker_killed_at("the exit code"), ["code 7"])
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec.get("rc"), rec["rcfile"]), ("running", None, rcfile))
        self.assertEqual(jobcore.ended_on_its_own(rec)[0], 7)      # the same code, read again
        w.carry_out()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["rc"]), ("failed", 7))
        self.assertEqual(rec["ended"], os.stat(rcfile).st_mtime)
        self.run_until(w, lambda: not w.hooks)
        self.assertEqual([h["event"] for h in jobstore.find(j)["hooks"]], ["failed"])
        self.assertEqual(self.ran(f), 1)
        self.assertIsNone(jobstore.find(j).get("history"))

    def test_an_outcome_recorded_without_its_hook_fires_it_from_the_next_worker(self):
        """An outcome and the hook it owes are written in one transaction, and the owed field is cleared once
        that hook has run. A worker that ends between the two leaves a record that says a hook is owed, and
        the next worker fires it: a hook fires once, and not never."""
        f = self.hook_file()
        j = self.add("--on-fail", f"echo x >> {f}", cmd="exit 3")
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        self.a_worker_killed_at("the hook")
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec["hook_due"]), ("failed", 3, "failed"))
        self.assertEqual(self.ran(f), 0)
        self.assertIsNone(rec.get("hooks"))
        w2 = jobq.worker_for("default")
        self.run_until(w2, lambda: not w2.hooks and "hook_due" not in jobstore.find(j))
        self.assertEqual([h["event"] for h in jobstore.find(j)["hooks"]], ["failed"])
        self.assertEqual(self.ran(f), 1)

    # The record of a hook that has run is written after that hook starts, so a worker that ends between the
    # two leaves the hook owed, and the next worker runs it again: the hook of one outcome runs twice. The
    # other order loses the hook of an outcome whose worker ends before it, and a hook that never fires is
    # the worse of the two, so what this order costs is a second run and never none.

    def test_a_hook_whose_worker_died_while_starting_it_runs_again_and_no_more(self):
        """A hook runs at least once. A worker that ends between the hook's own start and the record of it leaves a
        record that says the hook is owed, so the next worker starts it again, and a worker after that does not."""
        f = self.hook_file()
        j = self.add("--on-fail", f"echo x >> {f}", cmd="exit 3")
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        self.a_worker_killed_at("the start")        # the hook ran; what the job owes is not cleared
        journey.until(lambda: self.ran(f) == 1, timeout=20, what="the hook of this outcome to run")
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["hook_due"]), ("failed", "failed"))
        self.assertIsNone(rec.get("hooks"))
        w2 = jobq.worker_for("default")
        self.run_until(w2, lambda: not w2.hooks and "hook_due" not in jobstore.find(j))
        self.assertEqual(self.ran(f), 2)
        w3 = jobq.worker_for("default")
        w3.tick()
        self.assertEqual((self.ran(f), len(jobstore.find(j)["hooks"])), (2, 1))

    def test_a_requeue_that_lost_its_worker_keeps_one_history_entry(self):
        """A requeue is one change of one record: the attempt that ended goes into the history, the fields of
        that attempt go, and the job is queued for the next one. A worker that ends between that change and
        the line it writes in the job's log leaves one history entry, and the job keeps its delay: it is
        skipped while it is not ready, and the job behind it runs."""
        self.config("slots = 1")
        j = self.add("--retries", "1", "--retry-delay", "60", cmd="exit 1")
        later = self.add(cmd="true")
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.assertEqual((self.state(j), self.state(later)), ("running", "queued"))
        self.the_run_is_over(j)
        self.a_worker_killed_at("the log line")
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["attempt"], len(rec["history"])), ("queued", 2, 1))
        self.assertEqual(rec["history"][0]["attempt"], 1)
        self.assertNotIn("rcfile", rec)                     # the fields of the attempt that ended are gone
        self.assertGreater(rec["not_before"], time.time())
        self.run_until(w, lambda: self.state(later) == "done")
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["attempt"], len(rec["history"])), ("queued", 2, 1))
        self.assertIsNone(w.waiting, "a job in its delay became the head of its queue")


class TestAStopAndAReapThatCross(InjectedCase):
    """One decision about one job, made inside the store's transaction on the record the store holds: a stop
    another actor wrote since the facts were read is part of it."""

    def a_kill_between_the_facts_and_the_decision(self, jid: int, grace: float = 10.0) -> journey.Gate:
        """Write a kill's stop in the window between the facts of a pass and its decision. The gate takes the
        step once, where the pass has the facts of this job and has opened no scope yet."""
        gate, real = journey.Gate(), jobcore.Worker._step

        def step(w, job, facts):
            gate.once(lambda: self.a_kill_asks(jid, grace=grace))
            return real(w, job, facts)
        patch = mock.patch.object(jobcore.Worker, "_step", step)
        patch.start()
        self.addCleanup(patch.stop)
        return gate

    def test_a_stop_racing_a_reap_ends_the_job_once(self):
        """A kill's stop that lands after the facts of a pass were read and before its decision is part of
        that decision: the job ends with the code it wrote itself, it is never retried, and the outcome is
        recorded once, so its hook fires once."""
        f = self.hook_file()
        j = self.add("--retries", "2", "--on-fail", f"echo x >> {f}", cmd="exit 5")
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        gate = self.a_kill_between_the_facts_and_the_decision(j)
        w.carry_out()
        self.assertEqual(gate.ran, 1)
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec["attempt"]), ("failed", 5, 1))
        self.assertEqual(rec["stop"]["by"], "user")
        self.assertIsNone(rec.get("history"), "a job a kill has touched was retried")
        self.run_until(w, lambda: not w.hooks and "hook_due" not in jobstore.find(j))
        self.assertEqual([h["event"] for h in jobstore.find(j)["hooks"]], ["failed"])
        self.assertEqual(self.ran(f), 1)

    def test_a_stop_commands_code_lands_on_the_stop_it_was_run_for(self):
        """The code of a stop command is recorded on the stop that command carried out, and on no other. A
        requeue takes the attempt's stop off the record, so a command still running then would otherwise land
        its code on the next attempt's stop, for which nothing has gone out: the record would say a stop
        failed that nothing had yet been sent for."""
        opens = self.gate("the-stop-command-may-end")
        j = self.add("--retries", "1", "--retry-delay", "0.01", "--retry-if", "^timeout$", cmd=self.gated())
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_live(j)
        with jobstore.txn() as s:       # a timeout's stop, whose command outlives the attempt it ended
            cur = s.job(j, must_be=("running",))
            cur["stop_cmd"] = f"while [ ! -f {opens} ]; do sleep 0.02; done; exit 3"
            cur["stop"] = {"by": "timeout", "timeout": True, "t": time.time(), "grace": 30.0}
            s.save(cur)
        w.carry_out()
        self.assertIsNotNone(jobstore.find(j)["stop"]["cmd"])
        h = jobcore._group_handle(jobstore.find(j))       # the attempt's run is over, having written no code
        self.assertIsNotNone(h, f"job {j}'s record names no group")
        proc.signal_group(h, signal.SIGKILL)
        self.the_run_is_over(j)
        w.tick()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["attempt"], rec.get("stop")), ("queued", 2, None))
        self.run_until(w, lambda: self.state(j) == "running")
        self.nothing_of_the_job_is_left(w, j)
        with jobstore.txn() as s:       # whose own stop is one pp's signal carries out
            cur = s.job(j, must_be=("running",))
            cur["stop_cmd"] = jobpolicy.GROUP
            s.save(cur)
        fresh = self.a_kill_asks(j, grace=30.0)
        self.open_the_gate("the-stop-command-may-end")
        journey.until(lambda: all(h.poll() is not None for h in w.stops.values()), timeout=30,
                      what="the stop command of the attempt that ended")
        w.tick()
        stop = jobstore.find(j)["stop"]
        self.assertEqual(stop["t"], fresh["t"])
        self.assertIsNotNone(stop["sent"])
        self.assertNotIn("rc", stop, "a code of a command run for the stop of an attempt that has ended")
        self.assertNotIn("cmd", stop)

    def test_the_decision_is_made_on_the_record_the_store_holds(self):
        """The facts of a pass are read before its transaction and the record is read inside it. A stop
        written in between is carried out by that same pass: the SIGTERM goes out for it, and the record says
        so, although the facts were read when the job carried no stop."""
        j = self.add(cmd=self.gated())
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_live(j)
        self.assertIsNone(jobstore.find(j).get("stop"))
        went = self.recording()
        gate = self.a_kill_between_the_facts_and_the_decision(j)
        w.carry_out()
        self.assertEqual((gate.ran, went), (1, [signal.SIGTERM]))
        self.assertIsNotNone(jobstore.find(j)["stop"]["sent"])


class TestTheStoreUnderTwoActors(InjectedCase):
    """What a reader of the store sees while another process writes it, and what the store takes back."""

    def test_a_record_is_never_read_in_two_states_or_in_none(self):
        """Every change of a record is one replace of one file, inside the store's transaction, so a reader
        that lists the store over and over while a job is held, released, started, requeued and recorded
        finds that job once in every pass, and in one state each time."""
        self.config("slots = 1")
        j = self.add("--hold", "--retries", "1", "--retry-delay", "0.05", cmd="exit 1")
        rows = self.sb.path("what-a-reader-saw")
        reader = journey.start([sys.executable, "-c", A_READER_OF_THE_STORE, ROOT, rows, str(j)],
                              ready="reading", stderr=subprocess.PIPE)
        self.addCleanup(reader.close)
        self.assertEqual(self.call("release", str(j), "--no-start")[0], 0)
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(j) == "failed", timeout=30)
        self.assertTrue(reader.dead(30), "the reader of the store did not come to the end of it")
        self.assertEqual(reader.returncode, 0, reader.stderr.read())
        seen = json.loads(text(rows))
        for row in seen:
            self.assertEqual([i for i, _st in row], [j], f"the job is named once in a pass: {row}")
            self.assertIn(row[0][1], jobstore.STATES, row)
        self.assertEqual((seen[0][0][1], seen[-1][0][1]), ("held", "failed"))   # it looked through the whole
        self.assertGreater(len({st for row in seen for _i, st in row}), 1, seen)

    def test_a_log_tail_is_matched_with_no_scope_of_the_store_held(self):
        """I12. Nothing that can block runs inside the store's transaction, and --retry-if is a pattern its
        caller wrote, matched against 200 lines of the job's own output: one nested group over a line that
        does not match backtracks exponentially, and a worker matching one inside the scope would hold the
        one store lock for as long as it took, leaving every queue on the machine unreadable.

        So the match is one of the facts, taken before the scope: another process asks for the store lock
        from inside the match itself and is given it."""
        asked, real = [], jobpolicy.tail_hit

        def hit(job, tail):
            asked.append(self.asking_for_the_store())
            return real(job, tail)
        j = self.add("--retries", "1", "--retry-if", "(a+)+$", cmd="echo aaaaaaaaaaaab; exit 1")
        w = jobq.worker_for("default")
        w.tick()
        self.the_run_is_over(j)
        with mock.patch.object(jobcore.policy, "tail_hit", hit):
            w.tick()
        self.assertEqual(asked, ["took it"])
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["attempt"]), ("failed", 1))     # the tail does not match it

    def test_a_write_of_a_copy_from_outside_the_scope_is_refused(self):
        """A record a scope writes is a record that scope read. A copy read before the scope, and a copy of
        the scope's own record, are both refused, so what another actor recorded meanwhile is never written
        over; a listing takes no write at all."""
        j = self.add(cmd="true")
        outside = jobstore.find(j)
        outside["prio"] = 1
        with jobstore.txn() as s:
            with self.assertRaises(RuntimeError):
                s.save(outside)
            with self.assertRaises(RuntimeError):
                s.save(dict(s.job(j)))
            s.job(j)["prio"] = 3
            s.save(s.job(j))
        self.assertEqual(jobstore.find(j)["prio"], 3)
        with jobstore.listing() as s:
            with self.assertRaises(RuntimeError):
                s.save(s.job(j))
        self.assertEqual(jobstore.find(j)["prio"], 3)

    def test_two_adds_at_once_never_choose_one_id(self):
        """An id names one job machine-wide: the whole record is written under a name of its writer's own and
        linked to the name the id gives, which fails where that name is there. Four adds let go at once take
        four ids, and four records hold them."""
        gate = self.gate("add")
        adds = [journey.start([sys.executable, "-c", AN_ADD_THAT_WAITS_FOR_A_GATE, ROOT, gate, self.work],
                              ready="ready", stderr=subprocess.DEVNULL) for _ in range(4)]
        for a in adds:
            self.addCleanup(a.close)
        self.open_the_gate("add")
        ids = []
        for a in adds:
            out, _err = a.communicate(timeout=60)
            self.assertEqual(a.returncode, 0, out)
            ids.append(int(out.split()[-1]))
        self.assertEqual(sorted(ids), sorted(set(ids)), ids)
        self.assertEqual(sorted(x["id"] for x in jobstore.jobs()), sorted(ids))

    def test_a_queue_another_layout_wrote_is_refused_where_it_is_opened(self):
        """A queue says what its store looks like in a file of its own. One holding another number holds
        records this pp cannot read: every path that opens that queue refuses, with the exit status of a
        refusal, and writes nothing. A queue is opened before anything of it is read, so the one read that
        takes no lock refuses there too. The queue is read again once the number is its own."""
        j = self.add(cmd="true")
        fmt = jobstore._format_file("default")
        self.assertEqual(jobstore._format_of("default"), jobstore.FORMAT)
        self.addCleanup(jobq.paths.write_json, fmt, jobstore.FORMAT)     # before this case's own way out reads it
        was = self.files()
        jobq.paths.write_json(fmt, jobstore.FORMAT + 1)
        for argv in (["status"], ["status", "-q", "default"], ["show", str(j)], ["kill", str(j)],
                     ["clean", "-y"], ["worker", "start", "-q", "default"], ["work", "-q", "default"],
                     ["add", "--no-start", "--cwd", self.work, "--", "true"]):
            self.assertEqual(self.call(*argv)[0], 2, argv)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as refused:
            jobstore.find(j)
        self.assertEqual(refused.exception.code, 2)
        self.assertEqual(self.files(), was)                 # and nothing of the queue was written
        jobq.paths.write_json(fmt, jobstore.FORMAT)
        self.assertEqual(self.state(j), "queued")
        self.run_until(jobq.worker_for("default"), lambda: self.state(j) == "done")


class TestOneWorkerPerQueue(InjectedCase):
    """Two processes that both want to be a queue's worker, and the wake that reaches the one that is."""

    def stop_the_worker(self, q: str = "default") -> None:
        """Stop the worker a test of this class started, and wait for its claim to be free, so nothing of the
        test is left running."""
        h = jobcore._handle(jobcore.worker_now(q))
        if h is not None:
            proc.signal_pid(h, signal.SIGTERM)
        journey.until(lambda: jobcore.worker(q) is None, timeout=30,
                      what=f"the {q} worker to let its claim go")

    def test_two_workers_of_one_queue_never_both_run(self):
        """Being a queue's worker is holding that queue's claim, and the worker's own loop is the one place
        that takes it. Two of them let go at once: one holds the claim, the other ends with the status of a
        command that did its work, and the queue's one slot is taken once."""
        self.config("slots = 1", table="poll = 0.05\nidle_exit = 600")
        a, b = self.add(cmd=self.gated()), self.add(cmd=self.gated())
        pp = os.path.join(ROOT, "bin", "pp")
        both = [journey.start([sys.executable, pp, "queue", "work", "-q", "default"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for _ in range(2)]
        for p in both:
            self.addCleanup(p.close)
        self.addCleanup(self.stop_the_worker)
        done = journey.until(lambda: [p for p in both if p.poll() is not None], timeout=30,
                             what="one of the two workers to find the claim taken")
        self.assertEqual(len(done), 1, "two workers of one queue both ran")
        self.assertEqual(done[0].returncode, 0)
        up = journey.until(lambda: jobcore.worker_now("default").rec, timeout=30, what="the worker that won")
        self.assertEqual(up["pid"], [p for p in both if p.poll() is None][0].pid)
        self.assertIsNone(jobq.lock.Claim.take(jobcore._worker_claim("default")), "the claim was free to take")
        journey.until(lambda: jobstore.jobs("running"), timeout=30, what="the worker to start the head of its queue")
        self.assertEqual([x["state"] for x in jobstore.jobs()], ["running", "queued"])
        self.assertEqual([x["attempt"] for x in jobstore.jobs("running")], [1])
        self.assertEqual(self.state(b), "queued")
        self.assertEqual(self.state(a), "running")

    def test_a_wake_sent_during_a_tick_ends_the_next_wait(self):
        """A wake reaches a worker through that worker's own pipe. One sent while a tick runs is in the pipe
        when the next wait begins, so it ends that wait instead of being lost, and it ends one wait only."""
        w = jobq.worker_for("default")
        gate, real = journey.Gate(), jobcore.Worker.carry_out

        def pass_over(wk, *a, **kw):
            gate.once(lambda: proc.raise_to_self(jobcore.WAKE))     # as a verb wakes a worker mid-tick
            return real(wk, *a, **kw)
        with jobcore.waking() as waits, mock.patch.object(jobcore.Worker, "carry_out", pass_over):
            w.tick()
            self.assertEqual(gate.ran, 1)
            self.assertTrue(waits.wait(30), "the wake sent during the tick was lost")
            self.assertFalse(waits.wait(0.05), "one wake ended two waits")

    def test_many_processes_that_all_want_a_worker_start_one_round(self):
        """Every verb that needs a worker asks one construct for one, and gets back the one answer it reports
        from. Four processes asking at once: each one ends saying a worker was up when its own ask came back,
        one holds the queue's claim, and the queue's one slot is taken once."""
        self.config("slots = 1", table="poll = 0.05\nidle_exit = 600")
        a, b = self.add(cmd=self.gated()), self.add(cmd=self.gated())
        gate = self.gate("ask")
        askers = [journey.start([sys.executable, "-c", A_CALLER_THAT_WANTS_A_WORKER, ROOT, gate, "default"],
                                ready="ready", stderr=subprocess.PIPE) for _ in range(4)]
        for p in askers:
            self.addCleanup(p.close)
        self.addCleanup(self.stop_the_worker)
        self.open_the_gate("ask")
        for p in askers:
            self.assertTrue(p.dead(60), "an ask for a worker never came back")
            self.assertEqual(p.returncode, 0, p.stderr.read())
        up = journey.until(lambda: jobcore.worker_now("default").rec, timeout=30, what="the queue's worker")
        self.assertIsNotNone(proc.Handle.running(up["pid"], up["pstart"]), "the claim's holder is gone")
        self.assertIsNone(jobq.lock.Claim.take(jobcore._worker_claim("default")), "the claim was free to take")
        with mock.patch.object(jobcore, "_start_worker",
                               lambda q: self.fail(f"a start began for {q}, whose worker is up")):
            self.assertTrue(jobq._worker_up("default", quiet=True).up)       # an ask that starts nothing
        journey.until(lambda: jobstore.jobs("running"), timeout=30, what="the worker to start the head of its queue")
        self.assertEqual([self.state(a), self.state(b)], ["running", "queued"])


class TestTheGroupTheLeaseAndTheSweep(InjectedCase):
    """What a signal is allowed to reach, what a lease outlives, and what a run leaves behind."""

    def test_a_group_id_that_belongs_to_another_process_is_not_signalled(self):
        """A process is named by its id and the start time read beside it, in the boot both were read in, and
        never by what its command line says. A record naming a group id that another process has taken since
        names nothing: no signal goes out to it, and the job is recorded from what the record says went out."""
        other = journey.start(["sleep", "60"], group="own")         # the job's own command line, on a stranger
        self.addCleanup(other.close)
        start = proc.read(other.pid).start
        j = self.add(cmd="sleep 60")
        with jobstore.txn() as s:
            rec = s.job(j)
            rec.update(state="running", started=time.time() - 60, pid=other.pid, pgid=other.pid,
                       pstart=start - 7, boot=proc.boot_id(),       # the id was handed out again since
                       stop={"by": "user", "t": time.time() - 30, "grace": 0, "sent": time.time() - 30})
            s.save(rec)
        self.assertIsNotNone(jobcore._group_handle(jobstore.find(j)), "a record of this boot names a group")
        jobq.worker_for("default").carry_out()
        self.assertIsNone(other.poll(), "a group that is not the job's was signalled")
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["reason"]), ("failed", "killed"))

    def test_a_lease_outlives_nothing_and_is_swept_where_the_queue_is_read(self):
        """A wait holds its lease as a claim, which the kernel lets go of however that process ends. A wait
        that is killed runs no way out of its own: the file it was holding is held by nobody, and the next
        pass of a worker, which reads the queue anyway, takes it away."""
        d = os.path.join(self.sb.state, "queue", "watchers")
        j = self.add("--hold", cmd="true")
        wait = journey.start([sys.executable, os.path.join(ROOT, "bin", "pp"), "queue", "wait", str(j),
                              "--no-start", "--poll", "0.1"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(wait.close)
        journey.until(lambda: jobcore.watchers(jobstore.find(j)), timeout=30, what="the wait's lease on the job")
        leases = os.listdir(d)
        self.assertEqual(len(leases), 1, leases)
        self.assertEqual(jobcore.watchers(jobstore.find(j))[0]["pid"], wait.pid)
        wait.signal(signal.SIGKILL)
        journey.until(lambda: wait.poll() is not None, timeout=30, what="the wait to end")
        self.assertEqual(wait.poll(), -signal.SIGKILL)
        self.assertEqual(os.listdir(d), leases)                 # nothing of its own ran
        self.assertEqual(jobcore.watchers(jobstore.find(j)), [])       # and nobody holds what it left
        jobq.worker_for("default").tick()
        self.assertEqual(os.listdir(d), [])

    def the_run_ends_with_no_code(self, jid: int) -> None:
        """End job jid's run the way a machine that went down does: its group is SIGKILLed, so its wrapper
        writes no exit code and the claim its run held is free."""
        h = jobcore._group_handle(jobstore.find(jid))
        self.assertIsNotNone(h, f"job {jid}'s record names no group")
        proc.signal_group(h, signal.SIGKILL)
        self.the_run_is_over(jid)

    def a_running_job_whose_stop_is_a_command(self, where: str, gate: str = "go") -> tuple:
        """A running job of this test's whose project stops it with a command that appends a line to `where`.
        -> the job's id and this test's worker."""
        j = self.add(cmd=self.gated(gate))
        w = jobq.worker_for("default")
        w.tick()
        self.the_run_is_live(j)
        self.nothing_of_the_job_is_left(w, j)
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop_cmd"] = f"echo ${{PP_QUEUE_CONTAINER}} >> {where}"
            s.save(cur)
        return j, w

    def test_a_sweep_runs_the_projects_stop_command_once_for_a_run_that_wrote_no_code(self):
        """I25. A run that is over having written no exit code of its own was lost or died of a signal, so
        whatever its runner started elsewhere is still out there: the sweep runs the project's stop command
        once, started after the decision's scope. Nobody asked for that stop, so nothing is recorded for it
        and the outcome is the one the attempt earned.

        A run a stop was asked of is swept the same way, whatever went out for that stop already: the work a
        stop command left behind is what a retry would start beside, and a run of that command the sweep
        makes records nothing, so it states no second stop."""
        ran = self.sb.path("what-the-stop-command-did")
        j, w = self.a_running_job_whose_stop_is_a_command(ran)
        self.the_run_ends_with_no_code(j)
        w.tick()
        rec = jobstore.find(j)
        # this worker started the attempt, so it holds the signal it died of; a worker that did not reads the
        # same case as `lost`, and either way the attempt wrote no exit code of its own
        self.assertEqual((rec["state"], rec["reason"], rec.get("rc")), ("failed", "signal 9", None))
        self.assertIsNone(rec.get("stop"), "the sweep's stop was recorded as one somebody asked for")
        journey.until(lambda: len(self.lines(ran)) == 1, timeout=30, what="the stop command the sweep ran")
        self.assertEqual(self.lines(ran), [rec["container"]])   # with this attempt's own name in it
        for _ in range(3):
            w.tick()
        self.assertEqual(len(self.lines(ran)), 1)
        # a kill whose stop command went out at the stop site: its code is recorded for the stop it carried
        # out, and the sweep of that attempt runs the command once more, for whatever it left behind
        k, w2 = self.a_running_job_whose_stop_is_a_command(ran, gate="go2")
        self.a_kill_asks(k, grace=30.0)
        w2.tick()
        journey.until(lambda: len(self.lines(ran)) == 2, timeout=30, what="the stop command the kill ran")
        journey.until(lambda: jobstore.find(k)["stop"].get("rc") == 0, timeout=30,
                      what="the exit code that command had, on the stop it carried out")
        self.the_run_ends_with_no_code(k)
        w2.tick()
        self.assertEqual(jobstore.find(k)["reason"], "killed")
        journey.until(lambda: len(self.lines(ran)) == 3, timeout=30, what="the stop command the sweep ran")
        self.assertEqual(self.lines(ran)[2], jobstore.find(k)["container"])
        for _ in range(3):
            w2.tick()
        self.assertEqual(len(self.lines(ran)), 3, "the sweep of one attempt ran that command twice")

    @contextmanager
    def the_workers_own_output(self):
        """Where this worker's own lines go, as a file to read: the worker log of a worker pp started, and
        this process's output while a test is the worker. -> that file's path."""
        path = self.sb.path("the-workers-own-output")
        with open(path, "w") as f:
            saved = os.dup(1)
            os.dup2(f.fileno(), 1)
            try:
                yield path
            finally:
                os.dup2(saved, 1)
                os.close(saved)

    def test_the_stop_a_kill_asked_for_writes_in_the_jobs_log_and_the_sweeps_run_does_not(self):
        """The job's log is the record of what that job and the stops asked of it did, so the stop command a
        kill asks for writes there. The sweep's run of the same command is nobody's ask: it writes where the
        worker's own lines go, and the job's log holds that command's output once."""
        ran = self.sb.path("what-the-stop-command-did")
        token = "the-stop-command-said-this"
        j, w = self.a_running_job_whose_stop_is_a_command(ran)
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop_cmd"] = f"echo {token}; echo ${{PP_QUEUE_CONTAINER}} >> {ran}"
            s.save(cur)
        log = jobstore.find(j)["log"]
        self.a_kill_asks(j, grace=30.0)
        w.tick()
        journey.until(lambda: len(self.lines(ran)) == 1, timeout=30, what="the stop command the kill ran")
        journey.until(lambda: token in text(log), timeout=30, what="its output in the job's log")
        self.the_run_ends_with_no_code(j)
        with self.the_workers_own_output() as out:
            w.tick()
            journey.until(lambda: len(self.lines(ran)) == 2, timeout=30, what="the stop command the sweep ran")
            journey.until(lambda: token in text(out), timeout=30,
                          what="the sweep's output where the worker's own lines go")
        self.assertEqual(jobstore.find(j)["reason"], "killed")
        self.assertEqual(text(log).count(token), 1)

    def test_the_sweep_of_a_retried_attempt_names_that_attempts_own_container(self):
        """A requeue takes the attempt's own names off the record, and the sweep's stop command is started
        after that scope: it is given the attempt that ended, so it names the container that attempt started
        and the next attempt begins beside nothing the last one left."""
        ran = self.sb.path("what-the-stop-command-did")
        j, w = self.a_running_job_whose_stop_is_a_command(ran)
        with jobstore.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur.update(retries=1, retry_delay=0.01)
            s.save(cur)
        was = jobstore.find(j)["container"]
        self.the_run_ends_with_no_code(j)
        w.tick()
        rec = jobstore.find(j)
        self.assertEqual((rec["state"], rec["attempt"]), ("queued", 2))      # back in its queue for another
        self.assertNotIn("container", rec)                                   # that attempt's name has gone
        self.assertEqual(rec["history"][0]["container"], was)
        journey.until(lambda: self.lines(ran) == [was], timeout=30,
                      what="the stop command the sweep ran, naming the attempt that ended")

    def test_a_sweep_runs_once_per_attempt_whose_run_is_over(self):
        """The sweep of a stopped job's group runs where its run is over: the SIGKILL goes out inside the
        decision's scope, once for that attempt, so a process the job left behind does not outlive it. It
        records nothing and changes no outcome: the job keeps the exit code it wrote."""
        kid = self.sb.path("the-job-left-this")
        j = self.add(cmd=f"sleep 300 & echo $! > {kid}; exit 0")
        w = jobq.worker_for("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        journey.until(lambda: os.path.exists(kid) and text(kid).strip(), timeout=30,
                      what="the process the job starts beside itself")
        left = int(text(kid))
        start = proc.read(left).start

        def nothing_is_left():
            h = proc.Handle.running(left, start)
            if h is not None:
                proc.signal_pid(h, signal.SIGKILL)
        self.addCleanup(nothing_is_left)
        self.the_run_is_over(j)
        self.a_kill_asks(j, grace=0.0)
        group = jobstore.find(j)["pgid"]
        went: list = []
        real = jobq.proc.signal_group

        def sender(h, sig):
            went.append((sig, h.id))
            return real(h, sig)
        with mock.patch.object(jobq.proc, "signal_group", sender):
            w.carry_out()
            rec = jobstore.find(j)
            self.assertEqual((rec["state"], rec["rc"]), ("done", 0))
            self.assertEqual(went, [(signal.SIGKILL, group)])
            journey.until(lambda: proc.Handle.running(left, start) is None, timeout=30,
                          what="what the job left behind to go with it")
            for _ in range(3):
                w.carry_out()
            self.assertEqual(went, [(signal.SIGKILL, group)])


class TestAnUnmetAfter(InjectedCase):
    """What a job that names the jobs it waits for is held by, and where that is read."""

    def test_an_unmet_after_is_read_across_every_queue(self):
        """An id names one job machine-wide, so a launch pass reads the jobs an `after` names with no queue
        filter, inside its own transaction. Stage one in another queue than stage two is the fault this
        guards: a pass reading its own queue alone would find no record for a stage-one job sitting
        elsewhere and would start stage two at once.

        A job that is queued, held or running has not ended, in any queue. A job whose record is gone has,
        so dropping one frees the stage behind it instead of deadlocking it."""
        self.config("slots = 3", q="lab")
        self.config("slots = 3", q="r1")
        one, gone = self.add(q="lab"), self.add(q="lab")
        two = self.add("--after", f"{one},{gone}", q="r1")
        self.assertEqual(jobstore.find(two)["after"], sorted([one, gone]))
        wl, wr = jobq.worker_for("lab"), jobq.worker_for("r1")
        wr.tick()
        self.assertEqual(self.state(two), "queued")             # both are queued, in another queue
        self.assertEqual(self.call("hold", str(one))[0], 0)
        wr.tick()
        self.assertEqual(self.state(two), "queued")             # a held job has not ended either
        self.assertEqual(self.call("drop", "-y", str(gone))[0], 0)
        wr.tick()
        self.assertEqual(self.state(two), "queued")             # the other one is still there
        self.assertEqual(self.call("release", "--no-start", str(one))[0], 0)
        self.run_until(wl, lambda: self.state(one) == "done")
        self.run_until(wr, lambda: self.state(two) == "done")

    def test_a_job_that_waits_never_becomes_the_head_of_its_queue(self):
        """A job whose `after` is unmet is skipped, not waited for, so a job behind it in run order still
        starts and the queue is never held up by a stage that is waiting (I17)."""
        self.config("slots = 1")
        one = self.add(q="lab")
        waits = self.add("--after", str(one), "--prio", "0")
        behind = self.add("--prio", "5")
        w = jobq.worker_for("default")
        self.run_until(w, lambda: self.state(behind) == "done")
        self.assertEqual(self.state(waits), "queued")           # better priority, and never the head
        self.run_until(jobq.worker_for("lab"), lambda: self.state(one) == "done")
        self.run_until(w, lambda: self.state(waits) == "done")

    def test_an_after_is_read_as_the_job_is_added_and_an_id_no_job_has_is_refused(self):
        """The selection is read once, as the job is queued, and the record keeps the ids: a tag added to
        later is not picked up, which is what makes a campaign file reproducible. An id no job has is a typo
        and a job waiting forever for it is the worst outcome, so it is a usage error and nothing is queued.
        A job may name a tag, and then it waits for the jobs that carried it at that moment. A value naming
        no selection at all is the same refusal, the empty one too: `--after "$(echo $IDS | tr ' ' ',')"`
        over a variable a shell never set is empty, and a stage queued with no `after` would run before the
        stage it was written to follow."""
        first = self.add("--tag", "s1_eval")
        named = self.add("--after", "s1_eval")
        self.assertEqual(jobstore.find(named)["after"], [first])
        later = self.add("--tag", "s1_eval")                    # a second job of that name, added after
        self.assertEqual(jobstore.find(named)["after"], [first])
        self.assertNotIn(later, jobstore.find(named)["after"])
        before = sorted(j["id"] for j in jobstore.jobs())
        for spec in ("999999", "9-99", "s1_nothing", ",", " ", ""):
            with self.subTest(after=spec):
                rc, _out, err = self.call("add", "--no-start", "--cwd", self.work, "--after", spec,
                                          "--", "true")
                self.assertEqual(rc, 2, err)
                self.assertEqual(sorted(j["id"] for j in jobstore.jobs()), before)
        plain = self.add()
        self.assertNotIn("after", jobstore.find(plain))         # absent on a job that names none
        parked = self.add("--hold", "--after", str(first))      # both together: held, and still waiting
        self.assertEqual((self.state(parked), jobstore.find(parked)["after"]), ("held", [first]))


if __name__ == "__main__":
    unittest.main()
