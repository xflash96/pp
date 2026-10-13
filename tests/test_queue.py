import inspect
import io
import json
import os
import re
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
from pp.lib import config, jobq, proc  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def card(index=0, total=24.0, used=0.0):
    return {"index": index, "name": "fake", "total_gb": total, "used_gb": used, "free_gb": total - used, "util": "0"}


def worker_pid(q: str = "default") -> int | None:
    """The pid of queue q's worker, as `worker_now` answers it: None where nobody holds the claim, and while a
    worker that holds it has not recorded itself. What every verb prints from, read here for a test."""
    return jobq.worker_now(q).pid


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
    j = jobq.find(int(os.path.basename(str(path)).split(".")[0]))
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
from pp.lib import jobq, lock
lock.Claim.take(jobq._worker_claim(sys.argv[1]), json.loads(sys.argv[2]))
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
        jobq._none_came_up.clear()
        self.tables: dict = {}
        self.shared = ""
        self.config("slots = 3")

    def _restore(self):
        """The queue's own part of the way out: no job of this test's is left running."""
        for j in jobq._jobs("running"):         # never leave a test job behind
            h = jobq._group_handle(j)
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

    def as_the_worker(self, w=None, q: str = "default") -> jobq.Worker:
        """This test's worker, holding queue q's worker claim: the worker a verb finds, so a kill wakes it
        and nothing starts a worker beside it.

        The test enters jobq.waking() with it, so a verb's wake lands in this process's own wakeup pipe
        instead of ending the test. `self.waits` is that pipe, which a test asks whether a wake arrived on."""
        w = w or jobq.Worker(q)
        wake = jobq.waking()
        self.waits = wake.__enter__()
        self.addCleanup(wake.__exit__, None, None, None)
        claim = jobq.lock.Claim.take(jobq._worker_claim(q), w.record())
        self.assertIsNotNone(claim, f"something else holds the {q} worker claim")
        self.addCleanup(claim.release)
        return w

    def worker_beside(self, w=None, q: str = "default") -> jobq.Worker:
        """A worker of queue q ticking beside the test until it ends, in place of the one a verb would start.
        What `kill` and `wait` need: only the queue's worker stops a job and records how it ended."""
        w = self.as_the_worker(w, q)
        self.sb.ticker(w.tick)
        return w

    def start_a_worker_here(self, q: str = "default"):
        """Stand in for the worker a verb starts, as the real one is by the time it says it is ready: it
        holds the queue's claim and has recorded itself. It is this process, so the test can tick it, and
        `self.worker` is it."""
        self.worker = w = jobq.Worker(q)
        claim = jobq.lock.Claim.take(jobq._worker_claim(q), w.record())
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
        real = jobq.worker_up

        def worker_up(q, **kw):
            if each_pass is not None:
                each_pass()
            out = real(q, **kw)
            self.tick_here()
            return out
        with jobq.waking(), mock.patch.object(jobq, "_start_worker", self.start_a_worker_here), \
                mock.patch.object(jobq, "worker_up", worker_up), \
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
        j = jobq.find(jid)
        return j["state"] if j else None

    def files(self, q: str = "default") -> set:
        """Every file under queue q's directory, by its path relative to that directory."""
        root = os.path.join(self.sb.state, "queue", q)
        return {os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs}

    def asking_for_the_store(self, how: str = "exclusive") -> str:
        """What another process is told when it asks for the store lock without waiting."""
        r = subprocess.run([sys.executable, "-c", ASK_FOR_THE_LOCK, jobq._store_lock(), how], cwd=ROOT,
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(r.stderr, "")
        return r.stdout.strip()

    def started_earlier(self, jid, seconds: float):
        """Move job jid's start time that far back, which is all the clock does to a running job. A pool's
        `settle` and a job's timeout are counted from that time, so a test says which side of the window it
        wants instead of waiting for the window to pass."""
        with jobq.txn() as s:
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
                      + json.dumps({st: [j["id"] for j in jobq._jobs(st)] for st in jobq.STATES}))


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
        self.assertEqual((jobq.find(a)["queue"], jobq.find(b)["queue"]), ("alpha", "beta"))
        self.assertEqual([j["id"] for j in jobq._resolve(["1-3"])], [1, 2, 3])
        self.assertEqual(jobq._resolve(["solo"])[0]["id"], c)
        with self.assertRaises(SystemExit) as cm, redirect_stderr(io.StringIO()) as err:
            jobq._resolve(["same"])
        self.assertEqual(cm.exception.code, 2)
        self.assertIn("alpha, beta", err.getvalue())
        self.assertEqual(jobq.queues(), ["alpha", "beta"])

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
        w = jobq.Worker("default")
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
        with jobq.listing() as s:
            self.assertEqual(s.jobs(), [])
        self.assertEqual(self.add(), 1)

    def test_a_queue_that_holds_no_record_is_opened_and_says_its_format(self):
        """A record is jobs/<id>.json. A run's claim is not one, so a queue whose records have all been
        cleaned holds nothing this pp cannot read: it is opened like any other, and the first writer to reach
        it says what format its store is in."""
        jobq.lock.Claim.take(jobq._run_claim("stray", 1)).release()     # what a run of a cleaned job left
        self.assertIsNone(jobq._format_of("stray"))
        self.assertEqual(self.call("status", "-q", "stray")[0], 0)
        self.assertEqual(self.state(self.add(q="stray")), "queued")
        self.assertEqual(jobq._format_of("stray"), jobq.FORMAT)
        jobq.writing_queue("fresh")                 # a writer that stores no job says the format too
        self.assertEqual(jobq._format_of("fresh"), jobq.FORMAT)

    def test_a_queue_whose_store_another_pp_wrote_is_refused(self):
        """A queue says what its store looks like in its FORMAT file, written by the first writer that
        reaches it. A queue whose jobs carry another number, or no number at all, holds records this pp cannot
        read: every path that opens a queue names it and refuses, rather than reporting it as empty and
        launching jobs beside ones that are still running."""
        self.config("slots = 3", table="idle_exit = 0\npoll = 0.01")
        jid = self.add(cmd="true")              # a queue with a job in it, and the format of its store
        fmt = os.path.join(self.sb.state, "queue", "default", "FORMAT")
        self.assertEqual(text(fmt).strip(), str(jobq.FORMAT))
        self.addCleanup(jobq.paths.write_json, fmt, jobq.FORMAT)     # before this case's own cleanup reads it
        opens = (["status"], ["status", "-q", "default"], ["add", "--no-start", "--cwd", self.work, "--", "true"],
                 ["wait", "--idle"], ["kill", str(jid)], ["show", str(jid)], ["clean", "-y"],
                 ["worker", "start", "-q", "default"], ["worker", "status", "-q", "default"],
                 ["work", "-q", "default"])
        for how in ("no FORMAT file", "another number"):
            if how == "no FORMAT file":
                os.remove(fmt)
            else:
                jobq.paths.write_json(fmt, jobq.FORMAT + 1)
            for argv in opens:
                rc, _, err = self.call(*argv)
                self.assertEqual(rc, 2, (how, argv, err))
                self.assertIn("queue 'default' holds jobs this pp cannot read", err)
                self.assertIn("Drain that queue with the pp that wrote it", err)
        jobq.paths.write_json(fmt, jobq.FORMAT)
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
        outside = jobq.find(jid)                        # read with no lock
        with jobq.txn() as s:
            with self.assertRaises(RuntimeError):
                s.save(outside)
            with self.assertRaises(RuntimeError):
                s.save(dict(s.job(jid)))                # a copy of this scope's own record is a copy too
            s.job(jid)["prio"] = 3
            s.save(s.job(jid))                          # the record itself
        self.assertEqual(jobq.find(jid)["prio"], 3)
        with jobq.listing() as s:
            with self.assertRaises(RuntimeError):
                s.save(s.job(jid))                      # a listing writes nothing

    def test_a_listing_sees_each_job_once_and_whole_and_keeps_a_writer_out(self):
        """POSIX does not say what a directory read sees while entries in it are replaced, and a file system
        may report one entry twice or skip it, so a listing holds the store lock shared."""
        self.config("slots = 1")
        w = jobq.Worker("default")
        done = self.add(cmd="true")
        self.run_until(w, lambda: self.state(done) == "done")
        failed = self.add(cmd="exit 1")
        self.run_until(w, lambda: self.state(failed) == "failed")
        running = self.add(cmd="sleep 30")
        w.tick()
        queued = self.add(cmd="true")
        held = self.add("--hold", cmd="true")
        want = {done: "done", failed: "failed", running: "running", queued: "queued", held: "held"}
        with jobq.listing() as s:
            seen = s.jobs()
            self.assertEqual(sorted(j["id"] for j in seen), sorted(want))        # once each
            self.assertEqual({j["id"]: j["state"] for j in seen}, want)
            for j in seen:                                                       # whole: the fields of its state
                self.assertEqual(j["id"] in (done, failed), "rc" in j and "ended" in j)
                self.assertEqual(j["id"] in (queued, held), "pid" not in j)
            # the two halves of one listing that `wait --mine` takes name a job once between them
            live, ended = s.jobs(jobq.LIVE), s.jobs(jobq.ENDED)
            self.assertEqual(len({j["id"] for j in live + ended}), len(live) + len(ended))
            self.assertEqual(self.asking_for_the_store(), "busy")                # a writer waits for it
            self.assertEqual(self.asking_for_the_store("shared"), "took it")     # another reader does not

    def test_status_counts_and_rows_come_from_one_listing(self):
        """status reads the store once, so it cannot show one job as both running and done, nor count more
        jobs than it lists."""
        self.config("slots = 1")
        w = jobq.Worker("default")
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
        j = jobq.find(jid)
        os.makedirs(os.path.dirname(j["log"]), exist_ok=True)
        with open(j["log"], "w") as f:
            f.write("first\n")
        with jobq.txn() as s:
            cur = s.job(jid)
            cur.update(rc=0, state="done")
            s.save(cur)
        start = jobq._tail_start

        def late(f, n):
            pos = start(f, n)
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
        self.assertEqual(jobq.find(2)["cmd"], ["echo", "two words"])
        self.assertEqual([j["id"] for j in jobq._resolve(["batch"])], [1, 2])

    def test_durations(self):
        self.assertEqual(jobq.parse_duration("90"), 90)
        self.assertEqual(jobq.parse_duration("30m"), 1800)
        self.assertEqual(jobq.parse_duration("2h"), 7200)
        self.assertEqual(jobq.parse_duration("1.5d"), 129600)
        self.assertEqual(jobq.parse_duration(7, "d"), 7 * 86400)
        self.assertIsNone(jobq.parse_duration(None))
        with self.assertRaises(ValueError):
            jobq.parse_duration("2 weeks")

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
        self.run_until(jobq.Worker("default"), lambda: self.state(ended) in jobq.ENDED)
        for argv in (["add", "--no-start", "--cwd", self.work, "--timeout", "", "--", "true"],
                     ["add", "--no-start", "--cwd", self.work, "--retry-delay", "", "--", "true"],
                     ["clean", "--older", "", "-n"]):
            rc, out, err = self.call(*argv)
            self.assertEqual(rc, 2, (argv, out, err))
            self.assertIn("say how long", err)
            self.assertNotIn("Traceback", err)
        self.assertEqual(self.state(ended), "done")         # nothing was queued, and no record was taken
        # the spellings that do mean "no bound", and the flag left out
        self.assertIsNone(jobq.find(self.add("--timeout", "none"))["timeout"])
        self.assertIsNone(jobq.find(self.add("--timeout", "0"))["timeout"])
        self.assertEqual(self.call("clean", "-n")[0], 0)

    def test_config_defaults_apply(self):
        self.config("slots = 3", 'timeout = "2h"\nretries = 2\nretry_delay = "1m"\nretry_if = "OOM"\non_done = "true"')
        j = jobq.find(self.add())
        self.assertEqual((j["timeout"], j["retries"], j["retry_delay"], j["retry_if"], j["on_done"]),
                         (7200, 2, 60, "OOM", "true"))
        j = jobq.find(self.add("--timeout", "5m", "--retries", "0", "--on-done", "none"))
        self.assertEqual((j["timeout"], j["retries"], j["on_done"]), (300, 0, None))


class TestWorker(QueueCase):
    def test_runs_and_records_rc(self):
        ok = self.add(cmd="echo hello")
        bad = self.add(cmd="exit 3")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(ok) == "done" and self.state(bad) == "failed")
        self.assertEqual(jobq.find(bad)["rc"], 3)
        with open(jobq.find(ok)["log"]) as f:
            log = f.read()
        self.assertIn("hello", log)
        self.assertIn("[pp queue]", log)

    def test_slots_serialize(self):
        self.config("slots = 1")
        a = self.add(cmd="sleep 0.3")
        b = self.add(cmd="sleep 0.1")
        w = jobq.Worker("default")
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.run_until(w, lambda: self.state(b) == "done")
        self.assertGreaterEqual(jobq.find(b)["started"], jobq.find(a)["ended"])

    def test_custom_counter_head_of_line_and_backfill(self):
        self.config("slots = 5\nquota = 1")
        a = self.add("--need", "quota=1", cmd="sleep 2")
        b = self.add("--need", "quota=1", cmd="sleep 2")
        c = self.add(cmd="sleep 2")
        w = jobq.Worker("default")
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
            jobq.Worker("default").launch()
        self.assertEqual([self.state(i) for i in ids], ["running"] * 4)
        self.assertEqual(len(passes), 2)        # the queued jobs, then the transaction that claims them

    def test_config_change_takes_effect_without_restart(self):
        self.config("slots = 1")
        a = self.add(cmd="sleep 2")
        b = self.add(cmd="sleep 2")
        w = jobq.Worker("default")
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
        w = jobq.Worker("default")
        w.tick()
        w.tick()
        self.assertEqual(self.state(h), "held")
        self.assertFalse(os.path.exists(jobq.find(h)["log"]))
        self.assertEqual(self.call("release", "--no-start", str(h))[0], 0)
        self.run_until(w, lambda: self.state(h) == "done")

    def test_reprio_order(self):
        self.config("slots = 1")
        ids = [self.add(cmd="sleep 0.1") for _ in range(3)]
        self.assertEqual(self.call("reprio", str(ids[2]), "0")[0], 0)
        w = jobq.Worker("default")
        w.tick()
        self.assertEqual(self.state(ids[2]), "running")
        self.assertEqual(self.state(ids[0]), "queued")

    def test_retry_verb_carries_the_job_over(self):
        j = self.add("--tag", "flaky", "--prio", "4", "--timeout", "1h", "--retries", "0", "--on-fail", "true", cmd="exit 1")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "failed")
        rc, out, _ = self.call("retry", "--no-start", str(j))
        self.assertEqual(rc, 0)
        self.assertEqual(len(out.split()), 1)       # the new id is the only stdout
        new = jobq.find(int(out.split()[0]))
        self.assertEqual((new["state"], new["cmd"], new["prio"], new["retry_of"], new["tag"], new["timeout"],
                          new["on_fail"], new["attempt"], new["queue"]),
                         ("queued", ["sh", "-c", "exit 1"], 4, j, "flaky", 3600, "true", 1, "default"))

    def test_drop_and_clean(self):
        q = self.add(cmd="true")
        self.assertEqual(self.call("drop", str(q))[0], 0)
        self.assertIsNone(jobq.find(q))
        d = self.add(cmd="true")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(d) == "done")
        rc, out, _ = self.call("clean", "-n")
        self.assertIn("1 ended", out)
        self.assertEqual(self.state(d), "done")
        log = jobq.find(d)["log"]
        self.assertEqual(self.call("clean", "-y")[0], 0)
        self.assertIsNone(jobq.find(d))
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
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "queued" and jobq.find(j)["attempt"] == 2
                       and self.state(done) == "done")
        journey.until(lambda: jobq.find(done).get("hooks"), timeout=10, what="the hook on the record")
        left = self.files() - before
        self.assertTrue([p for p in left if p.endswith(f"{j}.run")], left)              # its run claim
        self.assertTrue([p for p in left if p.startswith(f"rc{os.sep}")], left)         # attempt 1's exit code
        self.assertTrue([p for p in left if p.startswith(f"logs{os.sep}")], left)       # and its log
        hlog = jobq.find(done)["hooks"][0]["log"]
        self.assertTrue(os.path.exists(hlog))
        self.assertEqual(self.call("drop", str(j))[0], 0)
        self.assertEqual(self.call("clean", "-y")[0], 0)
        self.assertEqual((jobq.find(j), jobq.find(done)), (None, None))
        self.assertEqual({p for p in self.files() - before if p != "FORMAT"}, set())
        self.assertFalse(os.path.exists(hlog))      # the hook's log too: the record was the last to name it

    def test_adoption_reads_rc_from_file(self):
        j = self.add(cmd="sleep 0.3; exit 4")
        w1 = jobq.Worker("default")
        w1.tick()
        self.assertEqual(self.state(j), "running")
        del w1                                  # the worker dies; the job does not
        w2 = jobq.Worker("default")
        self.run_until(w2, lambda: self.state(j) == "failed")
        rec = jobq.find(j)
        self.assertEqual(rec["rc"], 4)
        self.assertNotIn("reason", rec)

    def test_adoption_without_rc_is_lost(self):
        j = self.add(cmd="sleep 30")
        w1 = jobq.Worker("default")
        w1.tick()
        proc.signal_group(jobq._group_handle(jobq.find(j)), signal.SIGKILL)   # it dies before writing an rc
        w2 = jobq.Worker("default")
        self.run_until(w2, lambda: self.state(j) == "failed")
        self.assertEqual(jobq.find(j)["reason"], "lost")

    def test_kill_takes_the_process_group(self):
        """A kill writes a stop on the job; its worker signals the whole group, so nothing the job started
        runs on, and records how it ended, which is what the kill prints."""
        j = self.add(cmd="sleep 30 & sleep 30; wait")
        w = jobq.Worker("default")
        w.tick()
        pid = jobq.find(j)["pid"]
        journey.until(lambda: len([p for p in proc.descendants(pid)
                                   if proc.read(p) and proc.read(p).comm == "sleep"]) >= 2,
                      timeout=5, what="the job's children")
        kids = proc.descendants(pid)
        self.worker_beside(w)
        rc, out, _ = self.call("kill", str(j), "--grace", "2")
        self.assertEqual(rc, 0)
        self.assertIn(f"killed {j} (stopped on SIGTERM)", out)
        rec = jobq.find(j)
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
        w = jobq.Worker("default")
        w.tick()
        pid = jobq.find(j)["pid"]
        journey.until(lambda: len(proc.descendants(pid)) >= 2, timeout=10, what="the job's children")
        kids = proc.descendants(pid)
        with jobq.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur.pop("pstart")
            s.save(cur)
        self.worker_beside(w)
        rc, out, err = self.call("kill", str(j), "--grace", "2")
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"killed {j}", out)
        self.assertEqual((self.state(j), jobq.find(j)["reason"]), ("failed", "killed"))
        self.assertFalse(any(proc.alive(k) for k in kids))

    def test_a_kill_clears_the_group_of_a_command_that_holds_the_signal_back(self):
        """The wrapper stops on the SIGTERM, so the run is over and the job is recorded as killed. Anything
        left in its group goes with it, as it does after a timeout, so a command that ignores the signal does
        not outlive the job."""
        j = self.add(cmd="trap '' TERM; while :; do sleep 0.05; done")
        w = jobq.Worker("default")
        w.tick()
        pid = jobq.find(j)["pid"]
        journey.until(lambda: proc.descendants(pid), timeout=5, what="the job's command")
        kids = proc.descendants(pid)
        self.worker_beside(w)
        rc, out, _ = self.call("kill", str(j), "--grace", "0.2")
        self.assertEqual(rc, 0)
        self.assertIn(f"killed {j}", out)
        self.assertEqual(jobq.find(j)["reason"], "killed")
        journey.until(lambda: not any(proc.alive(k) for k in kids), timeout=5, what="its group to be clear")

    def test_a_run_is_live_until_its_worker_reaps_it(self):
        """A dying process closes its files, the run claim among them, before its parent can read how it
        ended. So a tick can find the claim free and the wrapper not yet reapable: the run is still live then,
        and its end is read once the worker reaps it, never guessed as lost."""
        j = self.add(cmd="sleep 30")
        w = jobq.Worker("default")
        w.tick()
        rec = jobq.find(j)
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
        self.assertEqual(jobq.find(j)["reason"], "signal 9")

    def test_kill_over_a_range_asks_first_then_stops_each(self):
        """A range is a selection: it is shown first, -n only shows, -y goes ahead, and every job it named
        that is running is stopped and reported."""
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        held = self.add("--hold", cmd="true")
        w = jobq.Worker("default")
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
        self.assertEqual([jobq.find(x)["reason"] for x in (a, b)], ["killed", "killed"])
        self.assertEqual(self.state(held), "held")

    def test_a_second_kill_hastens_the_signal_and_keeps_what_went_out(self):
        """A job whose SIGTERM is out with five minutes of grace before the SIGKILL: a second kill with a
        short grace SIGKILLs it now. The record still says when the SIGTERM went out, so the job is not
        signalled from the start again and the grace does not begin a second time.

        One stop carries one grace, the shorter of every ask, and that is the one the SIGKILL goes out after.
        It is also the one `kill` prints, whichever ask it came from: a kill that asks for two minutes inside
        a grace of a fifth of a second does not report a two-minute grace."""
        w = jobq.Worker("default")
        self.worker_beside(w)       # one worker for the whole test, ticking beside it
        rows = [("a shorter ask hastens the SIGKILL", 300.0, 0.0, "0.2"),
                ("a longer ask leaves the grace the record kept", 0.2, 1.0, "120")]
        for what, grace, ago, asked in rows:
            with self.subTest(what):
                j = self.add(cmd="sleep 30")
                journey.until(lambda: self.state(j) == "running", timeout=10, what="the job to start")
                sent = time.time() - ago
                with jobq.txn() as s:       # a kill that asked for that grace, its SIGTERM out
                    cur = s.job(j, must_be=("running",))
                    cur["stop"] = {"by": "user", "t": sent, "grace": grace, "sent": sent}
                    s.save(cur)
                rc, out, err = self.call("kill", str(j), "--grace", asked)
                self.assertEqual((rc, err), (0, ""))
                kept = min(grace, float(asked))
                self.assertIn(f"killed {j} (SIGKILL after {kept:g}s)", out)
                st = jobq.find(j)["stop"]
                self.assertEqual((st["by"], st["t"], st["sent"], st["grace"]), ("user", sent, sent, kept))
                self.assertTrue(st["kill9"])
                self.assertEqual(jobq.find(j)["reason"], "killed")

    def test_a_stop_pp_may_not_signal_says_so_where_a_person_looks(self):
        """A stop pp was refused is not in flight: nothing more goes out for it. `show` says that, and the
        field is in the list JSON consumers read, so a second reader does not wait for a stop that is never
        carried out."""
        j = self.add(cmd="sleep 30")
        w = jobq.Worker("default")
        w.tick()
        with mock.patch.object(jobq.proc, "signal_group", lambda *a, **k: "denied"):
            w.carry_out()                       # nothing in flight yet
            with jobq.txn() as s:
                cur = s.job(j, must_be=("running",))
                cur["stop"] = {"by": "user", "t": time.time(), "grace": 10.0}
                s.save(cur)
            w.carry_out()
        st = jobq.find(j)["stop"]
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
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "queued" and jobq.find(j)["attempt"] == 2)
        as_running = [{**jobq.find(j), "state": "running", "_bulk": False}]      # as a kill read it before
        with mock.patch.object(jobq, "_resolve", lambda *a, **k: as_running):
            rc, out, err = self.call("kill", str(j))
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"killed {j} (it was queued, with no process to stop)", out)
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["reason"], rec["attempt"]), ("failed", "killed", 2))
        self.assertIsNone(jobq.worker("default"))       # nothing was asked of a worker, and none was started

    def test_a_run_claim_the_launch_took_is_never_left_held(self):
        """A launch takes each admitted job's run claim inside its transaction and hands it to the spawn after
        it. However that pass ends, pp's own copy of every claim it took is let go: a job whose spawn was
        never reached then reads as a run that is over, and the next pass records it, instead of a record that
        says running with a claim nothing will ever free."""
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        w = jobq.Worker("default")
        real, seen = jobq._new_rcfile, []

        def new_rcfile(j):          # the second job of the pass: the rest of its transaction cannot be done
            seen.append(j["id"])
            if len(seen) == 2:
                raise OSError(28, "No space left on device")
            return real(j)
        with mock.patch.object(jobq, "_new_rcfile", new_rcfile), self.assertRaises(OSError):
            w.tick()
        self.assertEqual(seen, [a, b])
        for x in (a, b):
            self.assertIsNone(jobq.lock.Claim.holder(jobq._run_claim("default", x)),
                              f"job {x} keeps a run claim, so nothing ever reaps it")
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        w.tick()                    # the next pass: a's run is over and is recorded, and b starts
        self.assertEqual((self.state(a), jobq.find(a).get("reason")), ("failed", "lost"))
        self.run_until(w, lambda: self.state(b) == "running")

    def test_launch_failure(self):
        j = self.add(cmd="true")
        shutil.rmtree(self.work)
        w = jobq.Worker("default")
        w.tick()
        rec = jobq.find(j)
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
        with jobq.txn() as s:
            cur = s.job(bad)
            cur["env"] = {"PP_IS=NO_NAME": "1"}         # what no execve takes, so this job can never start
            s.save(cur)
        w = jobq.Worker("default")
        w.tick()
        rec = jobq.find(bad)
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
        wa, wb = jobq.Worker("alpha"), jobq.Worker("beta")
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
        self.assertEqual(data["queues"], ["alpha", "beta"])

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
        self.assertEqual(jobq.queues(), [])
        self.assertEqual(json.loads(self.call("worker", "status", "-q", "typoqueue", "--json")[1])[0]["up"], False)
        self.assertFalse(os.path.exists(os.path.join(top, "typoqueue")))


class TestGpu(QueueCase):
    def test_reservation(self):
        jobq._cards = lambda: [card(total=24)]           # budget: 24 - 2 = 22
        a = self.add("--gpu-gb", "12", cmd="sleep 2")
        b = self.add("--gpu-gb", "12", cmd="sleep 2")
        w = jobq.Worker("default")
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("does not fit", self.why(w))
        self.assertEqual(jobq.find(a)["gpu"], 0)

    def test_reservation_counts_every_queue(self):
        jobq._cards = lambda: [card(total=24)]
        self.config("slots = 3", q="alpha")
        self.config("slots = 3", q="beta")
        a = self.add("--gpu-gb", "12", cmd="sleep 2", q="alpha")
        b = self.add("--gpu-gb", "12", cmd="sleep 2", q="beta")
        wa, wb = jobq.Worker("alpha"), jobq.Worker("beta")
        wa.tick()
        wb.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("reserved 12", self.why(wb))            # alpha's reservation, seen from beta

    def test_live_free_memory(self):
        cards = [card(total=24, used=19)]                # another tenant holds 19 GB
        jobq._cards = lambda: cards
        j = self.add("--gpu-gb", "10", cmd="true")
        w = jobq.Worker("default")
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
        wa, wb = jobq.Worker("alpha"), jobq.Worker("beta")
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

    def test_picks_the_card_that_fits(self):
        jobq._cards = lambda: [card(0, total=24, used=14), card(1, total=24, used=4)]
        j = self.add("--gpu-gb", "15", cmd='echo "dev=$CUDA_VISIBLE_DEVICES"')
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "done")
        rec = jobq.find(j)
        self.assertEqual(rec["gpu"], 1)
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
        wa, wb = jobq.Worker("alpha"), jobq.Worker("beta")
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
                          ('gpu_gb = { probe = "x" }', "nvidia-smi already"), ("nodes = { every = 3 }", "max and/or probe"),
                          ('nodes = "lots"', "not a number"), ('nodes = { probe = "x", every = "soon" }', "every")):
            self.resources(text)
            with self.assertRaises(config.ConfigError, msg=text) as cm:
                jobq.pools("default")
            self.assertIn(msg, str(cm.exception))

    def test_reserved_queue_names(self):
        rc, _, err = self.call("add", "-q", "probes", "--no-start", "--", "true")
        self.assertEqual(rc, 2)
        self.assertIn("not a queue name", err)


class TestProbe(QueueCase):
    def test_free_amount_minus_what_was_launched_since(self):
        free = self.sb.path("free")
        with open(free, "w") as f:
            f.write("3\n")
        self.resources(f'nodes = {{ probe = "cat {free}", every = "0s", settle = "0.5s" }}')
        a = self.add("--need", "nodes=2", cmd="sleep 5")
        b = self.add("--need", "nodes=2", cmd="sleep 5")
        w = jobq.Worker("default")
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
        w = jobq.Worker("default")
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
            w = jobq.Worker("default")
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
        w = jobq.Worker("default")
        w.tick()
        w.tick()
        with open(calls) as f:
            self.assertEqual(len(f.read().split()), 1)

    def test_status_shows_shared_pools(self):
        self.resources('license = 2\nnodes = { probe = "echo 7", every = "0s" }')
        self.add("--need", "license=1", "--need", "nodes=1", cmd="sleep 3")
        w = jobq.Worker("default")
        w.tick()
        out = self.call("status")[1]
        self.assertIn("shared: ", out)
        self.assertIn("license 1/2", out)
        self.assertIn("nodes 1 in use (probe: 7 free", out)
        data = json.loads(self.call("status", "--json")[1])
        lic = next(p for p in data["shared"] if p["name"] == "license")
        self.assertEqual((lic["used"], lic["max"]), (1, 2))
        self.assertEqual(data["pools"]["default"][0]["name"], "slots")


class TestTimeoutRetry(QueueCase):
    def test_timeout_kills_the_group(self):
        j = self.add("--timeout", "0.5", cmd="sleep 30 & sleep 30; wait")
        w = jobq.Worker("default")
        w.tick()
        pid = jobq.find(j)["pid"]
        journey.until(lambda: len(proc.descendants(pid)) >= 2, timeout=5, what="the job's children")
        kids = proc.descendants(pid)
        self.run_until(w, lambda: self.state(j) == "failed", timeout=8)
        rec = jobq.find(j)
        self.assertEqual((rec["reason"], rec["rc"]), ("timeout", None))
        journey.until(lambda: not any(proc.alive(k) for k in kids), timeout=5, what="its children to be gone")

    def test_retries_keep_the_id_and_back_off(self):
        marker = self.sb.path("m")
        j = self.add("--retries", "2", "--retry-delay", "0.2", cmd=f"test -f {marker}.2 && exit 0; "
                     f"test -f {marker}.1 && touch {marker}.2 && exit 1; touch {marker}.1; exit 1")
        w = jobq.Worker("default")
        self.run_until(w, lambda: (jobq.find(j) or {}).get("attempt") == 2)
        rec = jobq.find(j)
        # queued for its delay, or running again where a pass took longer than the delay on a loaded machine:
        # what the delay is, the arithmetic below says from the record's own stamps
        self.assertIn(rec["state"], ("queued", "running"))
        # the delay is counted from when the attempt ended, which the record says, not from any clock of this
        # test's: a pass that takes longer than the delay on a loaded machine still leaves the same stamp
        self.assertAlmostEqual(rec["not_before"] - rec["history"][-1]["ended"], 0.2, places=6)
        t2 = rec["not_before"]
        self.run_until(w, lambda: (jobq.find(j) or {}).get("attempt") == 3)
        rec = jobq.find(j)
        self.assertAlmostEqual(rec["not_before"] - rec["history"][-1]["ended"], 0.4, places=6)   # doubled
        self.assertLess(t2, rec["not_before"])
        self.run_until(w, lambda: self.state(j) == "done")
        rec = jobq.find(j)
        self.assertEqual((rec["id"], rec["attempt"], len(rec["history"])), (j, 3, 2))
        with open(rec["log"]) as f:
            log = f.read()
        self.assertIn("attempt 1/3 after exit 1", log)
        self.assertIn("attempt 2/3 after exit 1", log)

    def test_retry_if(self):
        hit = self.add("--retries", "1", "--retry-delay", "0.05", "--retry-if", "transient", cmd="echo transient glitch; exit 1")
        miss = self.add("--retries", "1", "--retry-delay", "0.05", "--retry-if", "transient", cmd="echo real bug; exit 1")
        tmo = self.add("--retries", "1", "--retry-delay", "0.05", "--retry-if", "^timeout$", "--timeout", "0.3", cmd="sleep 30")
        w = jobq.Worker("default")
        self.run_until(w, lambda: all(self.state(x) == "failed" for x in (hit, miss, tmo)), timeout=10)
        self.assertEqual(jobq.find(hit)["attempt"], 2)
        self.assertEqual(jobq.find(miss)["attempt"], 1)       # not retryable: final at once
        self.assertEqual(jobq.find(tmo)["attempt"], 2)        # the reason is matched too

    def test_a_delay_that_is_already_up_is_not_said_as_a_negative_one(self):
        """A worker notices an attempt ended a poll period after it did, so the delay of a job it is about to
        re-queue can be up already. The three places that say how long is left all say it the same way, and
        none of them counts backwards: `--retry-delay 1s` is the README's own line."""
        said: list = []
        j = self.add("--retries", "1", "--retry-delay", "1", cmd="echo trying; exit 3")
        w = jobq.Worker("default", log=said.append)
        w.tick()
        rcf = jobq.find(j)["rcfile"]
        journey.until(lambda: os.path.exists(rcf), timeout=10, what="the job's exit code")
        old = time.time() - 3           # the attempt ended a whole period before the worker noticed
        os.utime(rcf, (old, old))
        self.run_until(w, lambda: jobq.find(j)["attempt"] == 2)
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
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(b) == "done")
        self.assertEqual((self.state(a), jobq.find(a)["attempt"]), ("queued", 2))
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
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "failed")
        journey.until(lambda: os.path.exists(f) and "---" in text(f), timeout=5, what="the hook's output")
        for _ in range(3):
            w.tick()        # the one actor that fires a hook, with nothing left to fire one for
        said = text(f)
        self.assertEqual(said.count("---"), 1)
        env = dict(line.split("=", 1) for line in said.splitlines() if "=" in line)
        rec = jobq.find(j)
        self.assertEqual((env["PP_QUEUE"], env["PP_QUEUE_JOB"], env["PP_QUEUE_EVENT"], env["PP_QUEUE_RC"]),
                         ("default", str(j), "failed", "7"))
        self.assertEqual((env["PP_QUEUE_LOG"], env["PP_QUEUE_CWD"], env["PP_QUEUE_TAG"]), (rec["log"], self.work, "flaky"))
        self.assertEqual(env["PP_QUEUE_REASON"], "exit 7")        # what wait prints and --retry-if matches
        self.assertEqual([h["event"] for h in rec["hooks"]], ["failed"])
        self.assertTrue(os.path.exists(rec["hooks"][0]["log"]))

    def test_on_done(self):
        f = self.hook_file()
        j = self.add("--on-done", f"echo $PP_QUEUE_EVENT $PP_QUEUE_RC > {f}", "--on-fail", f"echo wrong > {f}", cmd="true")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "done")
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertEqual(text(f).strip(), "done 0")

    def test_kill_and_drop_fire_nothing(self):
        f = self.hook_file()
        k = self.add("--on-fail", f"echo $PP_QUEUE_JOB >> {f}", cmd="sleep 30")
        d = self.add("--on-fail", f"echo $PP_QUEUE_JOB >> {f}", "--hold", cmd="true")
        w = jobq.Worker("default")
        w.tick()
        self.worker_beside(w)
        self.assertEqual(self.call("kill", str(k), "--grace", "1")[0], 0)
        self.assertEqual(self.call("drop", str(d))[0], 0)
        self.assertEqual(jobq.find(k)["reason"], "killed")
        self.assertEqual(jobq.find(k).get("hooks"), None)
        self.assertFalse(os.path.exists(f))

    def test_a_stop_that_was_signalled_is_never_retried_and_fires_nothing(self):
        """A job the worker reaps right after it signalled the stop is a kill, not a failure to retry or to
        report: the stop on its record says so, whatever its attempts left."""
        f = self.hook_file()
        k = self.add("--retries", "3", "--on-fail", f"echo x >> {f}", cmd="sleep 30")
        w = jobq.Worker("default")
        w.tick()
        with jobq.txn() as s:
            cur = s.job(k, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 0, "sent": time.time()}
            s.save(cur)
        proc.signal_group(jobq._group_handle(cur), signal.SIGKILL)
        self.run_until(w, lambda: self.state(k) == "failed")
        rec = jobq.find(k)
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
        w = jobq.Worker("default")
        w.tick()
        with jobq.txn() as s:
            cur = s.job(k, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 10.0}   # written; nothing out for it
            s.save(cur)
        proc.signal_group(jobq._group_handle(cur), signal.SIGKILL)       # something outside pp ended it
        journey.until(lambda: jobq.lock.Claim.holder(jobq._run_claim("default", k)) is None, timeout=10,
                      what="the job's run to be over")
        w.carry_out()                   # its run is over before any pass of this worker, so nothing went out
        rec = jobq.find(k)
        self.assertEqual((rec["state"], rec["reason"], rec["attempt"]), ("failed", "signal 9", 1))
        self.assertIsNone(rec.get("history"))           # a job a kill has touched runs no second time
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertEqual(text(f).strip(), "signal 9")
        self.assertEqual([h["event"] for h in jobq.find(k)["hooks"]], ["failed"])

    def test_a_timeout_pp_was_refused_ends_the_attempt_at_its_timeout(self):
        """pp may not signal every process group. A timeout's stop pp was refused is recorded as refused and
        nothing more goes out for it, so the job runs on; once its run is over the attempt ends with reason
        "timeout", which is what the timeout asked for. So `--retry-if '^timeout$'` matches it, and `show`
        says pp was refused rather than leaving a person to read the job as lost."""
        j = self.add("--timeout", "60", "--retries", "1", "--retry-delay", "0.05", "--retry-if", "^timeout$",
                     cmd="sleep 30")
        w = jobq.Worker("default")
        w.tick()
        self.started_earlier(j, 120)                    # its own time has run out
        with mock.patch.object(jobq.proc, "signal_group", lambda *a, **k: "denied"):
            w.carry_out()                               # the timeout's stop goes on the record ...
            w.carry_out()                               # ... and pp is refused the job's group
            st = jobq.find(j)["stop"]
            self.assertEqual((st["timeout"], st.get("sent")), (True, None))
            self.assertTrue(st["denied"])
            shown = self.call("show", str(j))[1]
        self.assertIn("nothing signalled", shown)
        self.assertIn("pp is not allowed to signal its group", shown)
        proc.signal_group(jobq._group_handle(jobq.find(j)), signal.SIGKILL)     # it ends some other way
        journey.until(lambda: jobq.lock.Claim.holder(jobq._run_claim("default", j)) is None, timeout=10,
                      what="the job's run to be over")
        w.carry_out()
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["attempt"]), ("queued", 2))         # the timeout may be retried
        self.assertEqual(rec["history"][-1]["reason"], "timeout")

    def test_a_final_timeout_fires_on_fail_as_the_help_says(self):
        """A timeout may be retried, and the last one is a failure like any other, so on_fail fires for it:
        a job that hangs is what a person wants to hear about. The sentence in the help about a signal that
        fires no hook is a kill's, since a kill is someone asking for this outcome."""
        f = self.hook_file()
        j = self.add("--timeout", "0.2", "--retries", "0", "--on-fail", f"echo $PP_QUEUE_REASON > {f}",
                     cmd="sleep 30")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "failed", timeout=15)
        self.assertEqual(jobq.find(j)["reason"], "timeout")
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertEqual(text(f).strip(), "timeout")
        self.assertEqual([h["event"] for h in jobq.find(j)["hooks"]], ["failed"])
        said = " ".join(queue_cmd.__doc__.split())       # the sentences the help owns, not how it wraps them
        self.assertIn("on a final timeout, on_fail fires.", said)
        self.assertIn('is recorded "killed" and fires no hook', said)

    def test_a_hook_its_worker_did_not_get_to_fire_is_fired_by_the_next_one(self):
        """The outcome and the hook owed for it are written in one transaction, and the record says it is
        owed until the hook has run. A worker that dies in between leaves that record, and the next worker's
        pass fires it: a hook fires once, and not never."""
        f = self.hook_file()
        j = self.add("--on-fail", f"echo x >> {f}", cmd="exit 3")
        w = jobq.Worker("default")
        with mock.patch.object(jobq.Worker, "fire", lambda *a, **k: None):      # it died before firing it
            self.run_until(w, lambda: self.state(j) == "failed")
        self.assertEqual(jobq.find(j)["hook_due"], "failed")
        self.assertFalse(os.path.exists(f))
        w2 = jobq.Worker("default")
        w2.tick()                                   # the next worker finds the hook owed, and fires it
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertNotIn("hook_due", jobq.find(j))
        self.assertEqual([h["event"] for h in jobq.find(j)["hooks"]], ["failed"])
        for _ in range(3):
            w2.tick()                               # and no later tick fires it again
        self.assertEqual(text(f).count("x"), 1)

    def test_launch_failure_fires_on_fail(self):
        f = self.hook_file()
        j = self.add("--on-fail", f"echo $PP_QUEUE_REASON > {f}", cmd="true")
        shutil.rmtree(self.work)
        w = jobq.Worker("default")
        w.tick()
        journey.until(lambda: os.path.exists(f) and text(f).strip(), timeout=5, what="the hook's output")
        self.assertIn("launch failed", text(f))
        self.assertEqual(self.state(j), "failed")

    def test_hook_argv(self):
        self.assertEqual(jobq.hook_argv("echo hi"), ["/bin/sh", "-c", "echo hi"])
        self.assertIsNone(jobq.hook_argv("none"))
        self.assertIsNone(jobq.hook_argv(None))

    def test_owner_captured_and_no_default_hook(self):
        human = jobq.find(self.add())
        self.assertIsNone(human["owner"])
        self.assertIsNone(human["on_fail"])
        self.sb.as_session("test-session-1")
        self.sb.setenv(TMUX_PANE="%999999")
        agent = jobq.find(self.add())
        explicit = jobq.find(self.add("--on-fail", "none"))
        self.assertIsNone(agent["on_fail"])
        self.assertEqual(agent["owner"], {"session": "test-session-1", "cwd": os.getcwd()})
        self.assertIsNone(explicit["on_fail"])

    def test_session_identity_never_reaches_the_job(self):
        self.sb.as_session("test-session-2")
        self.sb.setenv(TMUX_PANE="%999999", CLAUDECODE="1")
        j = self.add(cmd="env")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "done")
        with open(jobq.find(j)["log"]) as f:
            log = f.read()
        self.assertNotIn("CLAUDE_CODE_SESSION_ID", log)
        self.assertNotIn("TMUX_PANE", log)
        self.assertNotIn("CLAUDECODE", log)
        self.assertIn(f"PP_QUEUE_JOB={j}", log)


class TestShowNote(QueueCase):
    def test_a_jobs_own_text_cannot_redraw_the_terminal(self):
        """A queued command, its tag, a note and the job's own output are all text from elsewhere, and a
        reader of `pp queue` may be an agent whose terminal is its own output. Every place pp prints a job's
        text puts it through ui.printable, so an escape sequence in it is shown rather than acted on."""
        esc = "\x1b[2J"             # clear the screen
        osc = "\x1b]0;PWNED\x07"    # retitle the terminal, which the CSI pattern alone does not catch
        ris = "\x1bc"               # reset it
        rc, out, err = self.call("add", "--no-start", "--cwd", self.work, "--tag", "tag" + esc, "--",
                                 "sh", "-c", f"printf '{osc}oops\\n{ris}'; echo start{esc}; exit 1")
        j = int(out.split()[-1])
        self.assertEqual(rc, 0, err)
        self.assertNotIn("\x1b", err)                   # add's own summary of what it queued
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "failed")
        self.assertEqual(self.call("note", str(j), "note" + esc)[0], 0)
        for argv in (("status",), ("show", str(j)), ("wait", str(j), "--timeout", "10", "--poll", "0.05")):
            with self.subTest(argv=argv):
                _rc, out, err = self.call(*argv)
                self.assertNotIn("\x1b", out + err, f"{argv[0]} printed an escape sequence of the job's")
                self.assertIn("\\x1b", out + err, f"{argv[0]} printed none of the job's text")
        self.assertEqual(jobq.find(j)["tag"], "tag" + esc)      # the record keeps what was asked for

    def test_note_and_show(self):
        j = self.add("--tag", "noted", "--timeout", "90s", cmd="echo out-line; exit 2")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "failed")
        rc, out, _ = self.call("note", str(j), "lr", "too", "high")
        self.assertEqual(rc, 0)
        rec = jobq.find(j)
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
        w = jobq.Worker("default")
        self.run_until(w, lambda: jobq.find(j)["attempt"] == 2, timeout=15)
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
        self.assertEqual(set(shown["shell_env"]), set(jobq.find(j)["shell_env"]))
        self.assertEqual(set(shown["shell_env"].values()) - {"***"}, set())
        self.assertNotIn("HF_TOKEN", self.call("show", str(j))[1])      # the text form says neither
        self.assertEqual(jobq.find(j)["env"], {"HF_TOKEN": "hf-secret-123"})

    def test_note_by_session(self):
        j = self.add("--hold")
        self.sb.as_session("test-session-3")
        self.call("note", str(j), "from an agent")
        self.assertEqual(jobq.find(j)["notes"][0]["by"], "test-session-3")


class TestStatusWait(QueueCase):
    def test_status_renders(self):
        self.config("slots = 1")
        self.add(cmd="sleep 2")
        self.add(cmd="true")
        self.add("--hold", cmd="true")
        w = jobq.Worker("default")
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
        w = jobq.Worker("default")
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
        d = self.add("--tag", "d" * 70, cmd="echo done")
        w = self.as_the_worker()                    # a worker is up, so the header carries its pid
        self.run_until(w, lambda: self.state(d) == "done")
        a = self.add("--tag", "a" * 70, cmd="echo this-is-a-fairly-ordinary-long-output-line-of-a-job; sleep 30")
        w.tick()
        self.add("--tag", "q" * 70, cmd="sleep 30")
        self.add("--hold", "--tag", "h" * 70, cmd="sleep 30")
        journey.until(lambda: jobq._last_line(jobq.find(a)["log"], 200), timeout=10, what="the job's output")
        self.sb.setenv(COLUMNS="80")
        rc, out, _ = self.call("status")
        self.assertEqual(rc, 0)
        self.assertEqual([ln for ln in out.splitlines() if len(ln) > 80], [], f"wider than 80 columns:\n{out}")
        self.assertEqual(out.count("…"), 4, f"a tag of each section is cut to fit:\n{out}")
        self.assertIn("worker up (pid", out)
        self.assertRegex(out, r"\n  slots 1/1 · queued 1")      # the counts on a line of their own

    def test_no_queues(self):
        rc, out, _ = self.call()
        self.assertEqual(rc, 0)
        self.assertIn("no queues yet", out)

    def test_exit_codes(self):
        ok = self.add(cmd="true")
        bad = self.add(cmd="exit 2")
        w = jobq.Worker("default")
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

    def test_nothing_to_wait_for(self):
        self.assertEqual(self.call("wait", "--no-start")[0], 0)
        self.assertEqual(self.call("wait", "--idle", "--no-start")[0], 0)


class TestPull(QueueCase):
    """wait / watch: how an agent hears about its jobs without anything being pushed into its session."""

    def test_wait_prints_the_failure_with_its_log_tail_and_stamps_the_job(self):
        j = self.add(cmd="echo first-line; echo boom-line; exit 3")
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "failed")
        rc, out, _ = self.call("wait", str(j), "--no-start")
        self.assertEqual(rc, 1)
        self.assertIn("  | boom-line", out)
        self.assertIn(f"pp queue retry {j}", out)
        self.assertTrue(jobq.find(j)["reported"][0]["by"].startswith("wait pid"))

    def test_watch_streams_every_event_across_a_retry(self):
        marker = self.sb.path("flaky")
        j = self.add("--retries", "1", "--retry-delay", "0.4", "--on-fail", "none",
                     cmd=f"sleep 0.3; test -f {marker} || {{ touch {marker}; echo flaked; exit 1; }}; echo fine")
        self.sb.ticker(jobq.Worker("default").tick)
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
            journey.until(lambda: jobq.watchers(jobq.find(j)), timeout=10, what="the wait's lease on the job")
            w = jobq.watchers(jobq.find(j))[0]
            self.assertEqual((w["pid"], w["ids"], w["verb"]), (p.pid, [j], "wait"))
            self.assertEqual(json.loads(self.call("show", str(j), "--json")[1])["watchers"][0]["pid"], p.pid)
        finally:
            p.close()         # no cleanup runs: the lease file stays behind ...
        self.assertEqual(jobq.watchers(jobq.find(j)), [])       # ... and is recognised as dead and removed
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
        journey.until(lambda: jobq.watchers(jobq.find(j)), timeout=10, what="the wait's lease on the job")
        leases = os.listdir(d)
        self.assertEqual(len(leases), 1, leases)
        p.signal(signal.SIGTERM)                     # as an agent ends a wait it ran in the background
        journey.until(lambda: p.poll() is not None, timeout=10, what="the wait to end")
        self.assertEqual(os.listdir(d), leases)      # nothing of its own took the file with it ...
        jobq.Worker("default").tick()
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
        self.assertEqual(jobq.watchers(), [])
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
                        gate.once(jobq.watchers)        # a worker's pass over the leases, exactly here
                    return out
                with mock.patch.object(where, fn, stepped):
                    with jobq.watching([j], None, "wait"):
                        held = [w["pid"] for w in jobq.watchers(jobq.find(j))]
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
        self.sb.ticker(jobq.Worker("default").tick)
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
        real_init, real_jobs, seen = jobq.Txn.__init__, jobq.Txn.jobs, []

        def init(s, write):
            real_init(s, write)
            made.append(s)                      # kept, so two scopes are never the same object
            s.serial = len(made)

        def jobs(s, state=(), queue=None):
            seen.append((s.serial, (state,) if isinstance(state, str) else tuple(state)))
            return real_jobs(s, state, queue)
        with mock.patch.object(jobq.Txn, "__init__", init), mock.patch.object(jobq.Txn, "jobs", jobs):
            rc, _, err = self.call("wait", "--mine", "--no-start", "--poll", "0.05", "--timeout", "0.3")
        self.assertEqual(rc, 3, err)
        halves = [t for t in seen if t[1] in (tuple(jobq.LIVE), tuple(jobq.ENDED))]
        self.assertEqual([t[1] for t in halves], [tuple(jobq.LIVE), tuple(jobq.ENDED)])
        self.assertEqual(halves[0][0], halves[1][0], "the two halves came from two listings")

    def test_a_job_that_starts_while_wait_lists_is_waited_on_once(self):
        """wait names each target once: a job is one record in one state, and one listing reads both halves of
        `--mine`, so a job the worker starts meanwhile cannot be named twice and then waited on forever. The
        job here ends only once the wait has listed it, never before."""
        self.sb.as_session("session-a")
        real, go = jobq.find, os.path.join(self.work, "go")

        def find(jid):          # the first look of the wait's own loop, which is after it listed its targets
            open(go, "a").close()
            return real(jid)
        for argv in (["--mine"], []):
            j = self.add("--on-fail", "none", cmd=f"while [ ! -f {go} ]; do sleep 0.02; done")
            ticker = self.sb.ticker(jobq.Worker("default").tick)
            journey.until(lambda: self.state(j) == "running", timeout=10, what=f"job {j} to start")
            with mock.patch.object(jobq, "find", find):
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
        self.assertIsNone(jobq.worker("default"))
        self.assertIsNone(worker_pid())
        self.assertIsNone(jobq.worker_now("default").handle)
        with jobq.lock.Claim.take(jobq._worker_claim("default"), {"pid": os.getpid(), "pstart": me.start}):
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
        with jobq.lock.Claim.take(jobq._worker_claim("default"), {"pid": os.getpid(), "pstart": me.start}):
            with mock.patch.object(jobq.proc, "signal_pid", lambda *a, **k: "gone"):
                rc, out, err = self.call("worker", "stop")
            self.assertEqual((rc, out.strip(), err), (0, "default: worker down", ""))

    def test_stopping_a_worker_in_its_first_moment(self):
        """A worker takes the claim and writes its record in the call after it, so for that moment the claim
        is held by something that names no process. There is nothing to signal yet, so the stop says a worker
        is starting and asks the user to come back, rather than signalling whatever holds that pid."""
        self.add("--hold")
        with jobq.lock.Claim.take(jobq._worker_claim("default")), \
                mock.patch.object(jobq, "STARTING_PAUSE", 0):
            rc, _, err = self.call("worker", "stop")
        self.assertEqual(rc, 1)
        self.assertIn("the default worker is starting", err)

    def test_a_stop_waits_for_the_worker_it_signalled_not_for_a_free_claim(self):
        """An `add` or a background `wait` starts the next worker as soon as the claim is free, so a successor
        can hold it before the stop looks again. The stop is over when the worker it signalled has let the
        claim go, whoever holds it then."""
        self.add("--hold")
        me = proc.read(os.getpid())
        old = jobq.lock.Claim.take(jobq._worker_claim("default"), {"pid": os.getpid(), "pstart": me.start})
        self.addCleanup(old.release)
        successor = []

        def signalled(h, sig=signal.SIGTERM):
            old.release()                   # the worker it signalled ends, and the next one takes the claim
            c = jobq.lock.Claim.take(jobq._worker_claim("default"), {"pid": os.getpid() + 1, "pstart": 1})
            self.assertIsNotNone(c, "the successor could not take the claim")
            successor.append(c)
            self.addCleanup(c.release)
            return "exited"
        with mock.patch.object(jobq.proc, "signal_pid", signalled), mock.patch.object(jobq, "STOP_WAIT", 2.0):
            rc, out, err = self.call("worker", "stop")
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"stopped worker {os.getpid()}", out)
        self.assertEqual((jobq.worker("default") or {}).get("pid"), os.getpid() + 1)   # the successor is up

    def test_readiness_means_a_stop_is_handled(self):
        """A worker says it is ready only once it holds its claim, has recorded itself and has its stop
        handler in place. So a SIGTERM the instant it says so stops it between ticks, and leaves no worker."""
        self.add("--hold")
        p = jobq._start_worker("default")           # returns when the worker says it is ready
        self.addCleanup(p.wait)
        self.addCleanup(p.end, signal.SIGKILL)
        self.assertEqual(p.end(signal.SIGTERM), "alive")      # it went out; the wait says what it did
        self.assertEqual(p.wait(10), 0)             # its own exit, not a signal's
        self.assertIsNone(jobq.worker("default"))

    def test_a_worker_is_up_the_moment_worker_up_returns(self):
        """Nothing looks again to find out whether the worker it started came up: it was told."""
        self.config("slots = 3", table="idle_exit = 60\npoll = 0.2")
        self.add("--hold")
        self.addCleanup(self.call, "worker", "stop")
        w = jobq.worker_up("default", quiet=True)
        self.assertEqual((w.how, w.up), (jobq.STARTED, True))
        self.assertEqual((jobq.worker("default") or {}).get("pid"), w.pid)
        rc, out, _ = self.call("worker", "stop")
        self.assertEqual((rc, out.split()[:3]), (0, ["default:", "stopped", "worker"]))
        self.assertIsNone(jobq.worker("default"))

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
            claim = jobq.lock.Claim.take(jobq._worker_claim(q), {"pid": os.getpid(), "pstart": me.start})
            self.assertIsNotNone(claim, f"the {q} worker claim was not free for the worker it started")
            if len(starts) < 3:
                claim.release()
                return mock.Mock(pid=-1)
            self.addCleanup(claim.release)
            return mock.Mock(pid=os.getpid())
        with mock.patch.object(jobq, "_start_worker", start):
            rc, out, err = self.call("worker", "start")
        self.assertEqual(rc, 0, err)
        self.assertIn(f"started worker (pid {os.getpid()};", out)
        self.assertEqual(len(starts), jobq.WORKER_STARTS)
        self.assertEqual(worker_pid(), os.getpid())

    def test_a_job_added_before_the_worker_lets_go_of_its_claim_keeps_it_up(self):
        """A worker lets go of its claim inside the scope that reads the store, so a job stored before that
        read is seen here and this worker stays for it. One stored after it finds the claim free, and the add
        that stored it starts the next worker."""
        self.config("slots = 3", table="idle_exit = 0\npoll = 0.01")
        self.add("--hold")                          # nothing to run: the worker is idle from the start
        real, added, started, ticks = jobq.Worker.tick, [], [], []
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
        with mock.patch.object(jobq.Worker, "tick", tick), redirect_stdout(io.StringIO()), \
                mock.patch.object(jobq.lock.Claim, "release", release), \
                mock.patch.object(jobq, "_start_worker", never):
            jobq.work("default")
        # the claim went inside the scope that had read the store, so no other process could read it yet: a job
        # stored before that read is this worker's, and one stored after it finds the claim free
        self.assertEqual(store, ["busy"])
        self.assertEqual(started, [])               # the add found the worker up, as it was
        self.assertEqual(self.state(added[0]), "done")
        self.assertIsNone(jobq.worker("default"))   # and it let the claim go on its way out


    def me(self) -> dict:
        """This process, as a worker's record names the process it is."""
        return {"pid": os.getpid(), "pstart": proc.read(os.getpid()).start}

    def test_a_worker_in_its_first_moment_is_looked_at_again(self):
        """A take locks the claim file and writes the record in the call after it, so for that moment the
        claim is held and names nothing. A verb that decided from that look would say the queue has no
        worker and start another, which would find the claim taken and bring nothing up. Nothing is decided
        from such a look: the answer is the holder's own record."""
        self.add("--hold")
        claim = jobq.lock.Claim.take(jobq._worker_claim("default"))      # held, and naming nothing yet
        self.assertIsNotNone(claim, "something else holds the default worker claim")
        self.addCleanup(claim.release)
        looks, gate, real, started = [], journey.Gate(), jobq.worker, []

        def records_itself(q):
            rec = real(q)
            looks.append(rec)
            # the worker writes its record exactly between the look that found none and the next one
            gate.once(lambda: claim.write({**self.me(), "started": time.time(), "backfill": False,
                                           "waiting": None}))
            return rec
        with mock.patch.object(jobq, "worker", records_itself), \
                mock.patch.object(jobq, "_start_worker", lambda q: started.append(q) or mock.Mock(pid=-1)):
            w = jobq.worker_up("default", quiet=True)
        self.assertEqual((w.how, w.pid, gate.ran), (jobq.UP, os.getpid(), 1))
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
                self.assertEqual(jobq.work("default"), 0)   # a worker that finds the claim held says so too
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
                mock.patch.object(jobq, "STARTING_PAUSE", 0), \
                mock.patch.object(jobq, "_start_worker", lambda q: started.append(q) or mock.Mock(pid=-1)):
            claim = jobq.lock.Claim.take(jobq._worker_claim("default"), self.me())
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
        self.assertEqual(data["worker"][0]["how"], jobq.STARTING)
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
                jobq._none_came_up.clear()      # the hold-off the round before this one left
                out, err = io.StringIO(), io.StringIO()
                with mock.patch.object(jobq, "_start_worker", failed), \
                        redirect_stdout(out), redirect_stderr(err):
                    loud = jobq.worker_up("default")
                self.assertEqual(loud.how, jobq.DOWN)
                self.assertIn(said, err.getvalue())
                self.assertIn("worker.log", err.getvalue())
                jobq._none_came_up.clear()
                qout, qerr = io.StringIO(), io.StringIO()
                with mock.patch.object(jobq, "_start_worker", failed), \
                        redirect_stdout(qout), redirect_stderr(qerr):
                    quiet = jobq.worker_up("default", quiet=True)
                self.assertEqual(quiet.how, jobq.DOWN)      # the same answer, and not a word about it
                self.assertEqual((qout.getvalue(), qerr.getvalue()), ("", ""))

    def test_worker_start_says_why_the_start_timed_out(self):
        """`worker start` is the one verb that asks quietly and then speaks for itself, so the reason comes
        back on the answer. It is the reason the worker's own log cannot carry: a worker that hung before it
        said it was ready wrote nothing, so a person sent to that log alone cannot tell a hang from a
        crash."""
        def never(q):
            raise TimeoutError("pp did not say it was ready within 60s")
        jobq._none_came_up.clear()
        with mock.patch.object(jobq, "_start_worker", never):
            rc, _out, err = self.call("worker", "start")
        self.assertEqual(rc, 1)
        self.assertIn("the default worker did not come up: pp did not say it was ready within 60s", err)
        self.assertIn("worker.log", err)

    def test_a_queue_whose_worker_came_up_is_not_held_off_the_next_start(self):
        """The hold-off is on a queue whose last round of starts brought none up, and it goes as soon as a
        worker is found up: a queue that recovers is started again at once."""
        jobq._none_came_up["default"] = time.time()
        w = self.as_the_worker()                        # a worker is up now
        self.assertTrue(jobq.worker_up("default", quiet=True).up)
        self.assertNotIn("default", jobq._none_came_up)
        self.assertEqual(w.queue, "default")

    def test_add_release_and_retry_wake_a_worker_that_is_up(self):
        """A verb that makes a job ready wakes the queue's worker, so the job it readied starts at the tick
        that wake brings forward and not at the end of the worker's period."""
        held = self.add("--hold", cmd="true")
        ended = self.add(cmd="true")
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(ended) == "done")
        with jobq.waking() as waits:        # this process is the worker: a wake lands in its own pipe
            claim = jobq.lock.Claim.take(jobq._worker_claim("default"), w.record())
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
        jobq.Worker("default").tick()
        self.assertEqual(self.state(running), "running")
        queued = self.add(cmd="true")
        self.assertIsNone(jobq.worker("default"))
        with self.one_thread():
            rc, out, err = self.call("kill", str(running))
        self.assertEqual((rc, self.started), (0, ["default"]), err)
        self.assertIn("started the default worker", err)
        self.assertIn(f"killed {running} (stopped on SIGTERM)", out)
        self.assertEqual(jobq.find(running)["reason"], "killed")
        self.assertEqual(worker_pid(), os.getpid())   # the worker it started is up ...
        self.run_until(self.worker, lambda: self.state(queued) == "done")    # ... and it launches the queue

    def test_two_kills_with_no_worker_up_both_end_through_one_worker(self):
        """The second kill runs inside the first one's wait. It finds the worker the first one started and
        wakes it, so one worker has both stops to carry out and both kills hear their own outcome."""
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        jobq.Worker("default").tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "running"))
        self.assertIsNone(jobq.worker("default"))
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
            self.assertEqual(jobq.find(jid)["reason"], "killed")

    def test_a_kill_on_a_timeouts_stop_is_a_kill(self):
        """A timeout's stop sits on the record for its whole grace, and a `kill` of the job can land inside
        that window. The kill's ask becomes the stop (`merge_stop`), so the one worker ends the job as a
        kill: it is never retried, it fires no hook, and `show` says where the stop began."""
        f = self.sb.path("hook")
        j = self.add("--retries", "2", "--retry-delay", "0.05", "--timeout", "60", "--on-fail",
                     f"echo x >> {f}", cmd="trap '' TERM; while :; do sleep 0.05; done")
        jobq.Worker("default").tick()
        with jobq.txn() as s:       # its time ran out and the worker wrote the stop, as a timeout does
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "timeout", "timeout": True, "t": time.time(), "grace": 10.0}
            s.save(cur)
        self.addCleanup(proc.signal_group, jobq._group_handle(cur), signal.SIGKILL)
        with self.one_thread():
            rc, out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual((rc, err.count("started the default worker")), (0, 1), err)
        self.assertIn(f"killed {j}", out)
        rec = jobq.find(j)
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
        jobq.Worker("default").tick()
        self.assertEqual(self.state(j), "running")
        started: list = []
        with mock.patch.object(jobq, "_start_worker", lambda q: started.append(q) or mock.Mock(pid=-1)), \
                mock.patch.object(jobq, "KILL_WAIT", 0.0):
            rc, _out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 1)
        self.assertIn(f"job {j} is still running", err)
        self.assertIn("no signal of pp's has gone out", err)
        self.assertIn("No worker of the default queue is up now", err)
        self.assertIn("worker.log", err)
        self.assertEqual(started, ["default"] * jobq.WORKER_STARTS)     # it asked for one, each time

    def test_a_kill_whose_job_outlives_the_wait_says_what_went_out_for_it(self):
        """A queue with no worker now is not a stop nothing went out for: a worker can send the SIGTERM,
        record it and then go away. What went out is on the job's record, so the message says it from there
        instead of telling a person the opposite of what `show` would."""
        j = self.add(cmd="sleep 30")
        jobq.Worker("default").tick()
        with jobq.txn() as s:               # a worker sent the SIGTERM and recorded it, then went away
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "ann", "t": time.time(), "grace": 30.0, "sent": time.time()}
            s.save(cur)
        with mock.patch.object(jobq, "_start_worker", lambda q: mock.Mock(pid=-1)), \
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
        jobq.Worker("default").tick()
        started: list = []
        clock = [1000.0]

        def sleep(_s):
            clock[0] += 0.1
        with mock.patch.object(jobq, "_start_worker", lambda q: started.append(clock[0]) or mock.Mock(pid=-1)), \
                mock.patch.object(jobq, "KILL_WAIT", 12.0), mock.patch.object(jobq, "START_EVERY", 5.0), \
                mock.patch.object(jobq.time, "time", lambda: clock[0]), mock.patch.object(jobq.time, "sleep", sleep):
            rc, _out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 1)
        rounds = sorted({round(t - 1000.0) for t in started})
        self.assertEqual(rounds, [0, 5, 10], started)       # the verb's own ask, then one round every 5s
        self.assertLessEqual(len(started), 4 * jobq.WORKER_STARTS)

    def test_a_round_of_starts_that_outlasts_the_interval_still_holds_the_next_one_off(self):
        """The interval is counted from the clock after the round, not before it: a start waits for the
        worker to say it is ready, so a round of them can take longer than START_EVERY. Counted from before,
        every pass after such a round would start another."""
        j = self.add(cmd="sleep 30")
        jobq.Worker("default").tick()
        started: list = []
        clock = [1000.0]

        def start(q):           # one start costs 4s of the test's clock, so a round of three costs 12s
            started.append(clock[0])
            clock[0] += 4.0
            return mock.Mock(pid=-1)

        def sleep(_s):
            clock[0] += 0.1
        with mock.patch.object(jobq, "_start_worker", start), \
                mock.patch.object(jobq, "KILL_WAIT", 30.0), mock.patch.object(jobq, "START_EVERY", 5.0), \
                mock.patch.object(jobq.time, "time", lambda: clock[0]), mock.patch.object(jobq.time, "sleep", sleep):
            rc, _out, err = self.call("kill", str(j), "--grace", "0")
        self.assertEqual(rc, 1)
        rounds = len(started) // jobq.WORKER_STARTS
        self.assertEqual(len(started) % jobq.WORKER_STARTS, 0, started)
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
        with mock.patch.object(jobq, "_start_worker", lambda q: started.append(clock[0]) or mock.Mock(pid=-1)), \
                mock.patch.object(jobq, "START_EVERY", 5.0), \
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
        jobq._none_came_up.clear()
        with mock.patch.object(jobq, "_start_worker", never), \
                mock.patch.object(jobq, "START_EVERY", 5.0), \
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
        with open(jobq.find(jid)["log"]) as f:
            return len([ln for ln in f if jobq.LAUNCH_BANNER.match(ln)])

    def test_a_running_jobs_claim_is_held_whatever_its_record_says(self):
        """A record with nothing in it to signal is not a job that has ended: its claim says it runs, and the
        wrapper's own pid in that claim names it again."""
        j = self.add(cmd="sleep 30")
        jobq.Worker("default").tick()
        with jobq.txn() as s:               # as if its worker had died before it recorded the process
            cur = s.job(j, must_be=("running",))
            for k in ("pid", "pgid", "pstart"):
                cur.pop(k, None)
            s.save(cur)
        self.assertIsNotNone(jobq.lock.Claim.holder(jobq._run_claim("default", j)))
        jobq.Worker("default").carry_out()
        self.assertEqual(self.state(j), "running")      # not lost, and not reported
        rec = jobq.find(j)
        self.assertTrue(proc.alive(rec["pid"], rec["pstart"]))      # named again, from its claim
        self.assertEqual(rec["pgid"], rec["pid"])
        self.assertEqual(self.banners(j), 1)

    def test_a_job_whose_run_is_live_is_never_started_again(self):
        """The claim decides whether a run may start, so a job that is queued again while its own run is still
        live is not started a second time."""
        j = self.add(cmd="sleep 30")
        jobq.Worker("default").tick()
        with jobq.txn() as s:               # queued under its own run, as a lost race would leave it
            cur = s.job(j, must_be=("running",))
            cur["state"] = "queued"
            s.save(cur)
        jobq.Worker("default").tick()
        self.assertEqual(self.state(j), "queued")       # its claim is held: nothing launched it
        self.assertEqual(self.banners(j), 1)
        with jobq.txn() as s:               # as it was, so the test's own cleanup stops it
            cur = s.job(j, must_be=("queued",))
            cur["state"] = "running"
            s.save(cur)

    def test_a_running_job_with_no_claim_and_no_exit_code_is_lost(self):
        """Nothing holds its run claim, so its run is over, and it wrote no exit code, so nobody can say how
        it ended."""
        j = self.add()
        with jobq.txn() as s:
            rec = s.job(j)
            rec.update(started=time.time(), state="running")
            s.save(rec)
        jobq.Worker("default").carry_out()
        self.assertEqual((self.state(j), jobq.find(j)["reason"]), ("failed", "lost"))

    def test_an_attempt_cannot_read_the_code_another_attempt_wrote(self):
        """Each attempt writes its exit code to a file of its own, named as the attempt starts and kept on the
        record, so one attempt's code is never read as another's and nothing has to be removed before a
        launch. `clean` takes them with the record."""
        m = os.path.join(self.work, "once")
        j = self.add("--retries", "1", "--retry-delay", "0.05", cmd=f"test -f {m} && exit 7; touch {m}; exit 1")
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "failed")
        rec = jobq.find(j)
        first, second = rec["history"][0]["rcfile"], rec["rcfile"]
        self.assertNotEqual(first, second)
        self.assertEqual((rec["attempt"], rec["rc"]), (2, 7))
        self.assertEqual(jobq.ended_on_its_own({"rcfile": first})[0], 1)     # attempt 1's code, still its own
        self.assertEqual(jobq.ended_on_its_own(rec)[0], 7)
        # a record that has not run yet reads no code, whatever any attempt before it left on disk
        self.assertIsNone(jobq.ended_on_its_own({k: v for k, v in rec.items() if k != "rcfile"}))
        self.assertEqual(self.call("clean", "-y")[0], 0)
        self.assertEqual([os.path.exists(p) for p in (first, second)], [False, False])

    def test_a_daemon_the_command_leaves_behind_does_not_keep_the_job_running(self):
        """The command reads /dev/null, not the claim, so neither it nor anything it leaves behind holds the
        run: the job ends when its own command ends, even with a process of its still in its group."""
        pidf = os.path.join(self.work, "daemon.pid")
        # the daemon writes its pid under another name and renames it, so the file is whole the moment it is
        # there: a `>` redirect creates and empties the file before the write, and the read can land in between
        j = self.add(cmd=f"sh -c 'echo $$ > {pidf}.part && mv {pidf}.part {pidf}; exec sleep 30' & exit 0")
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "done")
        journey.until(lambda: os.path.exists(pidf), timeout=10, what="the daemon's pid")
        with open(pidf) as f:
            dpid = int(f.read())
        self.addCleanup(journey.kill_pid, dpid)
        rec = jobq.find(j)
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
        return jobq.find(jid)["rcfile"]

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
        w = jobq.Worker("default")
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
        real = jobq.Worker._spawn

        def spawn(w, job, gpu, claim):      # running, no pid yet: another reaper looks exactly now
            self.assertIsNotNone(jobq.lock.Claim.holder(jobq._run_claim("default", j)))
            jobq.Worker("default").carry_out()
            return real(w, job, gpu, claim)
        with mock.patch.object(jobq.Worker, "_spawn", spawn):
            jobq.Worker("default").tick()
        rec = jobq.find(j)
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
            jobq.Worker("default").tick()
        rec = jobq.find(j)
        self.assertEqual(rec["state"], "running")
        self.assertEqual([n["text"] for n in rec.get("notes", [])], ["before the claim", "while it starts"])

    def test_a_kill_that_crosses_the_jobs_own_exit_is_recorded_once(self):
        """A job that ends on its own while it is being stopped keeps its own exit code, and that outcome is
        recorded by one actor: its hook fires once and one `wait` reports it once."""
        f = self.sb.path("hook")
        j = self.add("--on-done", f"echo x >> {f}", cmd=self.gated("go"))
        w = jobq.Worker("default")
        w.tick()
        real, gate = jobq.Worker.carry_out, journey.Gate()

        def it_exits():         # the job exits 0 of its own accord, and nothing is signalled until it has
            open(os.path.join(self.work, "go"), "w").close()
            journey.until(lambda: os.path.exists(self.rc_file(j)), timeout=10, what="the job's exit code")

        def serve(wk, *a, **kw):              # once the kill's stop is on the record, never before
            gate.once(it_exits, when=lambda: (jobq.find(j) or {}).get("stop"))
            real(wk, *a, **kw)
        with mock.patch.object(jobq.Worker, "carry_out", serve), self.one_thread():
            rc, out, _ = self.call("kill", str(j))      # the kill starts the worker that serves its stop
            self.assertEqual((rc, gate.ran), (0, 1))
            # whether the signal reaches the group before the job's last process goes is a race; either way
            # the kill reports the outcome the job itself wrote
            self.assertRegex(out, rf"job {j} (had already ended|ended on its own as it was being killed): "
                                  rf"done {j} rc=0")
            self.assertEqual(self.call("wait", str(j), "--timeout", "10", "--poll", "0.05")[0], 0)
        rec = jobq.find(j)
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
        w = jobq.Worker("default")
        w.tick()
        open(os.path.join(self.work, "go"), "w").close()         # it exits 7 with no worker to reap it
        journey.until(lambda: jobq.lock.Claim.holder(jobq._run_claim("default", j)) is None,
                      timeout=10, what="the job's run to be over")
        real, gate = jobq.Worker.carry_out, journey.Gate()

        def pass_over(wk, *a, **kw):          # the first pass over it finds the stop written and its run over
            gate.once(lambda: journey.until(lambda: (jobq.find(j) or {}).get("stop"), timeout=10,
                                            what="the kill's stop"))
            real(wk, *a, **kw)
        with mock.patch.object(jobq.Worker, "carry_out", pass_over), mock.patch.object(jobq, "KILL_WAIT", 10.0):
            self.worker_beside(w)       # a worker from here on, and its first pass is the gated one
            rc, out, err = self.call("kill", str(j))
        self.assertEqual((rc, err, gate.ran), (0, "", 1))
        self.assertRegex(out, rf"job {j} had already ended: FAILED {j} rc=7")
        rec = jobq.find(j)
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
        w = jobq.Worker("default")
        w.tick()
        with jobq.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 10.0}   # written; nothing recorded yet
            s.save(cur)
        proc.signal_group(jobq._group_handle(cur), signal.SIGKILL)          # the signal went out ...
        journey.until(lambda: jobq.lock.Claim.holder(jobq._run_claim("default", j)) is None, timeout=10,
                      what="the job's run to be over")
        jobq.Worker("default").carry_out()                                       # ... and the next worker reaps it
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec.get("reason"), rec["attempt"]), ("failed", "lost", 1))

    def test_a_stop_is_carried_out_while_the_run_is_live_whatever_code_is_written(self):
        """PP_QUEUE_RCFILE is in every job's environment, so the file an attempt's exit code goes in may hold
        a code the command itself wrote, with the command still running. An exit code counts only once the
        run is over, so the stop is carried out as for any other job: the SIGTERM goes out, the SIGKILL
        follows once the grace is up, and nothing waits out the job's own life. The run ends there, and the
        code that was written stands as the outcome."""
        j = self.add(cmd="sleep 30")
        w = jobq.Worker("default")
        w.tick()
        held = jobq._group_handle(jobq.find(j))
        self.addCleanup(proc.signal_group, held, signal.SIGKILL)
        with open(self.rc_file(j), "w") as f:       # the command's own doing: `echo 0 > "$PP_QUEUE_RCFILE"`
            f.write("0\n")
        self.assertIsNotNone(jobq.lock.Claim.holder(jobq._run_claim("default", j)), "its run is already over")
        with jobq.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 0}
            s.save(cur)
        went: list = []
        with mock.patch.object(jobq.proc, "signal_group", lambda *a, **k: went.append(a[1]) or "alive"):
            w.carry_out()                                       # the SIGTERM, and the record says it went out
            self.assertEqual(went, [signal.SIGTERM])
            self.assertIsNotNone(jobq.find(j)["stop"]["sent"])
            w.carry_out()                                       # its grace is up: the SIGKILL
        self.assertEqual(went, [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual((self.state(j), w.serving), ("running", 1))
        proc.signal_group(held, signal.SIGKILL)     # the signal reaches it, its run is over, and the code stands
        journey.until(lambda: jobq.lock.Claim.holder(jobq._run_claim("default", j)) is None, timeout=10,
                      what="the job's run to be over")
        w.carry_out()
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec.get("reason")), ("done", 0, None))

    def test_a_kill_whose_stop_leaves_the_record_does_not_wait_it_out(self):
        """A kill waits for the stop it asked for, not for a deadline. A stop can leave a record without the
        job having ended, since a timeout's retry takes it with it, and the kill then says where the job is.

        One thread does the steps in order. The retry happens where the kill makes sure the queue has a
        worker, once the stop is on the record and before the kill waits for it, which is the one window
        this is about. The worker holds the queue's claim and ticks not at all, so no second actor can carry
        the stop out or launch the retry while the kill reads the record."""
        j = self.add("--retries", "1", cmd="sleep 30")
        w = jobq.Worker("default")
        w.tick()
        group = jobq._group_handle(jobq.find(j))        # taken while it runs, so it names that run's group

        def nothing_is_left():
            """Its group goes, and the wrapper the worker holds is waited for, so this test leaves no child
            of its own to be collected unreaped."""
            proc.signal_group(group, signal.SIGKILL)
            p = w.children.pop(j, None)
            if p is not None:
                p.wait()
        self.addCleanup(nothing_is_left)
        self.as_the_worker(w)
        real_up, moved = jobq.worker_up, journey.Gate()

        def back_to_its_queue():
            with jobq.txn() as s:       # a retry, which is what takes the stop with it
                cur = s.job(j, must_be=("running",))
                self.assertIsNotNone(cur, "the kill wrote its stop on a job that was not running")
                cur.update(state="queued", attempt=2)
                cur.pop("stop", None)
                s.save(cur)

        def worker_up(q, **kw):
            moved.once(back_to_its_queue)
            return real_up(q, **kw)
        start = time.monotonic()
        with mock.patch.object(jobq, "worker_up", worker_up), mock.patch.object(jobq, "KILL_WAIT", 20.0):
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
        real, killing, window = jobq.Worker._spawn, [], []

        def spawn(wk, job, gpu, claim):     # claimed, recorded as running, its process not recorded yet
            rec = jobq.find(j)
            window.append((rec["state"], rec.get("pid"), jobq.lock.Claim.holder(jobq._run_claim("default", j))))
            t = threading.Thread(target=lambda: killing.append(self.call("kill", str(j), "--grace", "0")),
                                 daemon=True)
            t.start()
            self.addCleanup(t.join, jobq.KILL_WAIT)
            journey.until(lambda: (jobq.find(j) or {}).get("stop"), timeout=10, what="the kill's stop on the job")
            return real(wk, job, gpu, claim)

        w = self.as_the_worker()        # this worker holds the claim, so the kill wakes it and waits
        with mock.patch.object(jobq.Worker, "_spawn", spawn):
            w.tick()
            self.run_until(w, lambda: self.state(j) == "failed")
            journey.until(lambda: killing, timeout=30, what="the kill to report")
        self.assertEqual(window, [("running", None, {})])
        rc, out, err = killing[0]
        self.assertEqual((rc, err), (0, ""))
        self.assertIn(f"killed {j}", out)
        self.assertEqual(jobq.find(j)["reason"], "killed")

    def test_a_recycled_group_id_is_not_signalled(self):
        other = journey.start(["sleep", "60"], group="own")     # someone else's group now
        self.addCleanup(other.close)
        j = self.add()
        with jobq.txn() as s:
            rec = s.job(j)
            rec.update(started=time.time() - 60, pid=other.pid, pgid=other.pid, state="running",
                       pstart=proc.read(other.pid).start - 7,       # the pid was handed out again
                       boot=proc.boot_id(),                           # in this boot, so the pair is read
                       stop={"by": "timeout", "timeout": True, "t": time.time() - 30, "grace": 10,
                             "sent": time.time() - 30})
            s.save(rec)
        jobq.Worker("default").carry_out()
        self.assertEqual(jobq.find(j)["reason"], "timeout")
        self.assertIsNone(other.poll())

    def test_a_group_id_from_another_boot_names_nothing(self):
        """A pid and the start time read beside it name a process within one boot and no further, and a
        queue's state outlives a reboot: a machine that loses power with a stop on a job comes back up with
        that group id in use by a stranger. A record from another boot names nothing, so the job's run claim
        is the only thing that can name its group, and that claim is always this boot's."""
        other = journey.start(["sleep", "60"], group="own")     # whatever holds that group id now
        self.addCleanup(other.close)
        j = self.add()
        with jobq.txn() as s:
            rec = s.job(j)
            rec.update(started=time.time() - 60, pid=other.pid, pgid=other.pid, state="running",
                       pstart=proc.read(other.pid).start,       # the pair names this very process ...
                       boot="00000000-0000-0000-0000-000000000000",     # ... but it was read before a reboot
                       stop={"by": "user", "t": time.time() - 30, "grace": 0})
            s.save(rec)
        self.assertIsNone(jobq._group_handle(jobq.find(j)))
        jobq.Worker("default").carry_out()      # it ends the job, and what it sweeps is its group
        self.assertIsNone(other.poll(), "it signalled a process group that is not the job's")
        # no signal of pp's went out for the stop, so the job is recorded as its own run ended: with nothing
        self.assertEqual(jobq.find(j).get("reason"), "lost")

    def test_a_log_removed_as_it_is_opened(self):
        j = self.add(cmd="echo hello")
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "done")
        log, real = jobq.find(j)["log"], open

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
        w = jobq.Worker("default")
        w.tick()
        journey.until(lambda: os.path.exists(self.rc_file(j)), timeout=10, what="the job's exit code")
        del w                                       # it died before recording the end
        started = []

        def brings_one_up(q, quiet=False, wake=False):      # stands in for the worker it starts
            started.append(q)
            jobq.Worker(q).carry_out()
            return jobq.Holder(jobq.UP, rec={"pid": os.getpid()})
        with mock.patch.object(jobq, "worker_up", brings_one_up):
            rc, out, _ = self.call("wait", str(j), "--timeout", "5", "--poll", "0.05")
        self.assertEqual((rc, started[:1]), (0, ["default"]))
        self.assertIn(f"done {j}", out)

    def test_a_probe_does_not_grant_a_unit_twice_before_it_shows_the_job(self):
        self.resources('nodes = { probe = "echo 1", every = "0s" }')      # settle left to its default
        self.config("slots = 3", q="a")
        self.config("slots = 3", q="b")
        a = self.add("--need", "nodes=1", cmd="sleep 30", q="a")
        b = self.add("--need", "nodes=1", cmd="sleep 30", q="b")
        wb = jobq.Worker("b")
        jobq.Worker("a").tick()
        wb.tick()                       # a fresh reading still says 1 free: a has not taken its node yet
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))
        self.assertIn("1 of it launched since", self.why(wb))

    def test_an_empty_config_launches_nothing(self):
        self.config("slots = 1")
        a, b = self.add(cmd="sleep 30"), self.add(cmd="sleep 30")
        open(os.path.join(self.cfg, "config.toml"), "w").close()     # mid-save
        w = jobq.Worker("default")
        with self.assertRaises(config.ConfigError):
            w.tick()                    # work() reports it as a bad config; nothing launches
        self.assertEqual((self.state(a), self.state(b)), ("queued", "queued"))
        self.assertIn("is empty", self.call("status")[1])
        self._write_config()
        w.tick()
        self.assertEqual((self.state(a), self.state(b)), ("running", "queued"))     # slots = 1 again

    def test_a_worker_told_to_stop_launches_nothing_new(self):
        j = self.add()
        w = jobq.Worker("default")
        w.stopping = lambda: True
        w.tick()
        self.assertEqual(self.state(j), "queued")


class TestTheWorkersWait(QueueCase):
    """What jobq.waking promises: the wait between a worker's ticks ends the moment a wake arrives, and a wake
    sent while a tick runs is taken by the next wait rather than lost."""

    def test_a_wake_ends_the_wait_at_once(self):
        """The wait is for the period, or until a wake, whichever comes first."""
        with jobq.waking() as waits:
            self.sb.ticker(lambda: proc.raise_to_self(jobq.WAKE), period=0.01, name="waker")
            start = time.monotonic()
            self.assertTrue(waits.wait(30))         # it says a signal ended it
            self.assertLess(time.monotonic() - start, 5, "the wake did not end the wait")

    def test_a_wake_sent_while_a_tick_runs_is_not_lost(self):
        """A wake that arrives before the wait begins is what a kill sends while the worker is mid-tick. It
        ends the next wait at once, so no stop waits a whole period for the tick after it."""
        with jobq.waking() as waits:
            proc.raise_to_self(jobq.WAKE)           # as a tick runs, before any wait
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
        w = jobq.Worker("default")
        w.tick()
        woke: list = []
        with jobq.waking() as waits:
            claim = jobq.lock.Claim.take(jobq._worker_claim("default"), w.record())
            self.assertIsNotNone(claim, "something else holds the default worker claim")

            def loop():         # work()'s own loop: a wait of ten seconds that only a wake or a stop shortens
                woke.append(waits.wait(jobq.STOP_PERIOD if w.serving else 10))
                w.tick()
            t = self.sb.ticker(loop, period=0, name="worker")
            start = time.monotonic()
            try:
                rc, out, _ = self.call("kill", str(j), "--grace", "1")
            finally:
                proc.raise_to_self(jobq.WAKE)       # end the wait this loop is in, then stop it
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
        w = jobq.Worker("default")
        w.tick()
        self.assertEqual(w.serving, 0)
        with jobq.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": time.time(), "grace": 300.0}
            s.save(cur)
        w.carry_out()
        self.assertEqual(w.serving, 1)              # work() then waits STOP_PERIOD, not the queue's poll
        self.assertLess(jobq.STOP_PERIOD, 2.0)
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
            self.assertTrue(jobq.worker_up("default", quiet=True, wake=True).up)
            self.assertTrue(self.waits.wait(0), f"pass {pass_ + 1} sent no wake")
        self.assertEqual(w.queue, "default")

    def test_a_timeout_nobody_waits_for_lets_the_worker_wait_at_its_poll(self):
        """The short period goes with the wake: a verb waiting for a stop asks for a tick on every pass of
        its own wait, so the worker ticks with it. A timeout is a stop the worker wrote itself and nobody
        waits for, so it waits its poll out rather than taking the one store lock ten times a second until
        the job is gone."""
        j = self.add("--timeout", "60", cmd="sleep 30")
        jobq.Worker("default").tick()                   # running, under no claim of its own
        self.started_earlier(j, 120)                    # and its time is up, so the next tick writes the stop
        asked, real = [], jobq.Wait.wait
        # work() installs its own stop handler; the scope hands SIGTERM back the way this test found it
        self.sb.on_signal(signal.SIGTERM, signal.getsignal(signal.SIGTERM))

        def wait(self_, seconds):
            asked.append(seconds)
            if len(asked) == 2:
                proc.raise_to_self(signal.SIGTERM)      # the worker stops after the second wait it asks for
            return real(self_, 0)
        with mock.patch.object(jobq.Wait, "wait", wait), redirect_stdout(io.StringIO()):
            self.assertEqual(jobq.work("default", poll=5.0), 0)
        self.assertEqual(asked, [5.0, 5.0])             # the queue's poll, never STOP_PERIOD
        self.assertEqual((jobq.find(j)["stop"] or {}).get("by"), "timeout")
        self.assertLess(jobq.STOP_PERIOD, 5.0)

    def test_a_stop_whose_sigkill_is_out_is_not_in_flight(self):
        """Once the SIGKILL is out there is nothing left to carry out for that stop, only a reap. So the
        worker goes back to its poll instead of holding itself at ten passes a second: a job the kernel is
        slow to end -- an uninterruptible read, a hung driver call -- would otherwise take the one store
        lock ten times a second for as long as it took to die, and every other pp process would wait on it.
        The reap sees the run claim go at the queue's poll, or at the next pass of a verb that is waiting for
        the stop, which wakes this worker each time."""
        j = self.add(cmd="sleep 30")
        w = jobq.Worker("default")
        w.tick()
        now = time.time()
        with jobq.txn() as s:
            cur = s.job(j, must_be=("running",))
            cur["stop"] = {"by": "user", "t": now, "grace": 300.0, "sent": now}
            s.save(cur)
        w.carry_out()
        self.assertEqual(w.serving, 1)      # inside its grace: the SIGKILL is still to go out
        with jobq.txn() as s:
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
    """`jobq.decide` is the one place that says what is to be done about a running job, and `jobq.merge_stop`
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
                                            yield {"held": held, "rc": rc, "stop": kind, "grace_up": grace_up,
                                                   "attempt": attempt, "left": left, "due": due, "sig": sig,
                                                   "retry_if": pat, **self.facts_of(kind)}

    def case(self, r: dict):
        """The job record and the facts one row stands for."""
        # attempts are left while the attempt number is no further than `retries`, so the row says both
        job = {"id": 1, "queue": "default", "state": "running", "tag": "t", "attempt": r["attempt"],
               "retries": r["attempt"] + 2 if r["left"] else 0, "retry_delay": self.DELAY,
               "retry_if": r["retry_if"], "timeout": 60.0, "started": self.NOW - (120 if r["due"] else 1),
               "on_fail": "echo failed", "on_done": "echo done"}
        st = self.stop(r["stop"], r["grace_up"])
        if st is not None:
            job["stop"] = st
        return job, jobq.Facts(held=r["held"], rc=r["rc"], rc_at=self.rc_at(r), sig=r["sig"], now=self.NOW)

    # -- what each row's action is -------------------------------------------------------------------------
    @staticmethod
    def rc_at(r: dict):
        return None if r["rc"] is None else TestTheDecision.RC_AT

    @staticmethod
    def sweep(r: dict) -> bool:
        """Whether anything left in the job's group goes with it: a stop was asked of it, and not refused."""
        return r["any"] and not r["denied"]

    @staticmethod
    def own(r: dict) -> str | None:
        """How the job's own run ended where it wrote no exit code: the signal it died of, or lost."""
        return None if r["rc"] is not None else (f"signal {r['sig']}" if r["sig"] else "lost")

    def ends(self, rc, reason, ended, sweep, hook, not_retried=None) -> jobq.Action:
        return jobq.Action(jobq.ENDED_AS, state="done" if rc == 0 and not reason else "failed", rc=rc,
                           reason=reason, hook=hook, ended=ended, sweep=sweep, not_retried=not_retried)

    def failure(self, r, rc, reason, ended, sweep) -> jobq.Action:
        """A failure of the job's own: another attempt where it has one left and --retry-if matches, else the
        end of it, which says why it was not retried. The delay doubles per attempt, up to RETRY_DELAY_CAP."""
        if r["left"] and r["retry_if"] is None:
            at = self.NOW if ended is None else ended
            delay = min(self.DELAY * 2 ** (r["attempt"] - 1), jobq.RETRY_DELAY_CAP)
            return jobq.Action(jobq.AGAIN, rc=rc, reason=reason, ended=at, sweep=sweep,
                               attempt=r["attempt"] + 1, not_before=at + delay)
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
             lambda r: jobq.Action(jobq.NOTHING)),
            ("A stop nobody has signalled yet goes out as SIGTERM to the job's process group.",
             lambda r: r["held"] and r["any"] and not r["sent"],
             lambda r: jobq.Action(jobq.TERM)),
            ("A stop whose grace is up is escalated to SIGKILL, once.",
             lambda r: r["held"] and r["sent"] and not r["kill9"] and r["grace_up"],
             lambda r: jobq.Action(jobq.KILL9)),
            ("A stop inside its grace, or SIGKILLed already, is waited for.",
             lambda r: r["held"] and r["any"],
             lambda r: jobq.Action(jobq.NOTHING)),
            ("A job with no stop whose own timeout has run out gets a timeout's stop.",
             lambda r: r["held"] and not r["any"] and r["due"],
             lambda r: jobq.Action(jobq.TIME_UP, t=self.NOW, grace=jobq.TIMEOUT_GRACE)),
            ("A job with no stop whose time is not up is left as it is.",
             lambda r: r["held"] and not r["any"],
             lambda r: jobq.Action(jobq.NOTHING)),
            # its run is over, so its exit code is the command's last word and this is where it ends
            ('A job a kill\'s signal stopped is recorded "killed" and fires no hook, unless it wrote its own '
             "exit code as the kill arrived, and then it keeps that code and fires the hook for it.",
             lambda r: not r["held"] and r["asked"] and r["out"],
             lambda r: self.ends(r["rc"], None if r["rc"] is not None else "killed", self.rc_at(r),
                                 self.sweep(r),
                                 None if r["rc"] is None else ("done" if r["rc"] == 0 else "failed"))),
            ("A job a kill wrote a stop for that no signal of pp's ever reached ends with its own outcome, "
             "the exit code it wrote or the signal it died of, and fires the hook that outcome owes.",
             lambda r: not r["held"] and r["asked"],
             lambda r: self.ends(r["rc"], self.own(r), self.rc_at(r), self.sweep(r),
                                 "done" if r["rc"] == 0 else "failed")),
            ("An exit code of 0 is done, and on_done fires.",
             lambda r: not r["held"] and r["rc"] == 0,
             lambda r: self.ends(0, None, self.rc_at(r), self.sweep(r), "done")),
            ("A timeout's stop pp signalled or was refused, with no exit code of the attempt's own, is a "
             "timeout, which may be retried; on a final timeout, on_fail fires.",
             lambda r: not r["held"] and r["rc"] is None and (r["out"] or r["denied"]),
             lambda r: self.failure(r, None, "timeout", None, self.sweep(r))),
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
            self.assertEqual(jobq.decide(job, facts), then(r), f"{what}: {r}")
        self.assertEqual(n, 4224)                                   # 2*3*11*2*2*2*2*2*2 rows
        self.assertEqual(seen, {what for what, _w, _t in rules})    # and every rule is a row of its own
        # both sides of the delay's cap are a row here, so neither arm can go quiet under a later constant
        self.assertGreater(self.DELAY * 2 ** (max(self.ATTEMPTS) - 1), jobq.RETRY_DELAY_CAP)
        self.assertLess(self.DELAY * 2 ** (min(self.ATTEMPTS) - 1), jobq.RETRY_DELAY_CAP)

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
            first = jobq.decide(job, facts)
            self.assertEqual(json.dumps(job, sort_keys=True), before, r)
            self.assertEqual(jobq.decide(job, facts), first, r)

    def test_a_job_with_no_hook_owes_none(self):
        """The hook a row names is the job's own: a job that carries no on_fail owes nothing when it fails,
        and nothing has to be cleared from its record."""
        bare = {"id": 1, "queue": "default", "state": "running", "attempt": 1, "retries": 0}
        over = jobq.Facts(held=False, rc=7, rc_at=self.RC_AT, now=self.NOW)
        self.assertIsNone(jobq.decide(bare, over).hook)
        self.assertEqual(jobq.decide({**bare, "on_fail": "echo x"}, over).hook, "failed")
        self.assertEqual(jobq.decide({**bare, "on_fail": "none"}, over).hook, None)     # `none` is no hook
        ok = jobq.Facts(held=False, rc=0, rc_at=self.RC_AT, now=self.NOW)
        self.assertIsNone(jobq.decide(bare, ok).hook)
        self.assertEqual(jobq.decide({**bare, "on_done": "echo x"}, ok).hook, "done")

    def test_an_asker_called_timeout_is_still_a_kill(self):
        """A worker's own stop carries `timeout`, so what a stop is never rests on the name of whoever asked:
        a session or a person called `timeout` writes a kill's stop, and the job ends as a kill's."""
        job = {"id": 1, "queue": "default", "state": "running", "attempt": 1, "retries": 3,
               "retry_delay": self.DELAY, "on_fail": "echo failed",
               "stop": {"by": "timeout", "t": self.NOW - 60, "grace": self.GRACE, "sent": self.NOW - 60}}
        over = jobq.Facts(held=False, rc=None, sig=9, now=self.NOW)
        a = jobq.decide(job, over)
        self.assertEqual((a.do, a.reason, a.hook), (jobq.ENDED_AS, "killed", None))      # never retried
        worker_wrote_it = {**job, "stop": {**job["stop"], "timeout": True}}
        self.assertEqual(jobq.decide(worker_wrote_it, over).do, jobq.AGAIN)      # a timeout may be retried

    def test_a_code_the_attempt_wrote_at_its_timeout_is_its_own_outcome(self):
        """The rule the help, CONTRIBUTING and `decide` all state: an attempt that wrote its own exit code as
        the timeout's signal arrived keeps that code and ends as it would have without the timeout. So a 0 is
        done and fires on_done, 7 is that failure, and `--retry-if '^timeout$'` matches an attempt that wrote
        no code of its own."""
        job = {"id": 1, "queue": "default", "state": "running", "attempt": 1, "retries": 3,
               "retry_delay": self.DELAY, "retry_if": "^timeout$", "on_fail": "echo failed",
               "on_done": "echo done",
               "stop": {"by": "timeout", "timeout": True, "t": self.NOW - 60, "grace": self.GRACE,
                        "sent": self.NOW - 50}}
        wrote_0 = jobq.decide(job, jobq.Facts(held=False, rc=0, rc_at=self.RC_AT, now=self.NOW))
        self.assertEqual((wrote_0.do, wrote_0.state, wrote_0.reason, wrote_0.hook),
                         (jobq.ENDED_AS, "done", None, "done"))
        wrote_7 = jobq.decide(job, jobq.Facts(held=False, rc=7, rc_at=self.RC_AT, now=self.NOW))
        self.assertEqual((wrote_7.do, wrote_7.rc, wrote_7.reason), (jobq.ENDED_AS, 7, None))
        self.assertIn("exit 7", wrote_7.not_retried)        # and /^timeout$/ is not what that failure is
        wrote_none = jobq.decide(job, jobq.Facts(held=False, rc=None, now=self.NOW))
        self.assertEqual((wrote_none.do, wrote_none.reason), (jobq.AGAIN, "timeout"))

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
                self.assertEqual(jobq.merge_stop(existing, ask), want)
                self.assertEqual(existing, was, "it changed the stop it was given")
                self.assertEqual(ask, asked, "it changed the ask it was given")


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
        with jobq.watching([j], None, "wait"):
            leases = os.listdir(os.path.join(self.sb.state, "queue", "watchers"))
        self.assertEqual(len(leases), 1, leases)
        self.assertRegex(leases[0], r"^\d+-\d+\.claim$")        # <pid>-<pstart>.claim, as it is written
        with open(os.path.join(ROOT, "pp", "lib", "jobq.py")) as f:
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
from pp.lib import jobq, proc

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
                                               jobq.ended_on_its_own)
if STEP == "the run claim":
    jobq.lock.Claim.take = die_at_the_run_claim
elif STEP == "the record":
    jobq.Worker._claim = die
elif STEP == "the spawn":
    jobq.Worker._spawn = die
elif STEP == "the start":
    proc.spawn = start_then_die
elif STEP == "the send":
    jobq.Worker._send = die
elif STEP == "the signal":
    proc.signal_group = note_then_die
elif STEP == "the signal of the group":
    proc.signal_group = send_then_die
elif STEP == "the exit code":
    jobq.ended_on_its_own = read_then_die
elif STEP == "the hook":
    jobq.Worker.fire = die
elif STEP == "the log line":
    jobq._append_log = die
else:
    raise SystemExit("no step of a worker is called %r" % STEP)

jobq._cards = lambda: None      # the cards of the machine this runs on are nothing to a test
w = jobq.Worker(Q)
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
from pp.lib import jobq

OUT, JID, DEADLINE = sys.argv[2], int(sys.argv[3]), time.monotonic() + 60
with jobq.listing() as s:               # one look before it says it is up, so the test writes after it
    rows = [[(x["id"], x["state"]) for x in s.jobs()]]
print("reading", flush=True)
while time.monotonic() < DEADLINE:
    with jobq.listing() as s:
        rows.append([(x["id"], x["state"]) for x in s.jobs()])
    if any(i == JID and st in jobq.ENDED for i, st in rows[-1]):
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
from pp.lib import jobq

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
from pp.lib import jobq

GATE, Q, DEADLINE = sys.argv[2], sys.argv[3], time.monotonic() + 60
print("ready", flush=True)
while not os.path.exists(GATE):
    if time.monotonic() > DEADLINE:
        sys.exit(2)
    time.sleep(0.01)
sys.exit(0 if jobq.worker_up(Q, quiet=True).up else 3)
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
        with jobq.txn() as s:
            cur = s.job(jid, must_be=("running",))
            self.assertIsNotNone(cur, f"job {jid} is not running")
            cur["stop"] = jobq.merge_stop(cur.get("stop"), {"by": by, "t": time.time(), "grace": grace})
            s.save(cur)
            return dict(cur["stop"])

    def hook_file(self) -> str:
        """Where a hook of this test's appends one line each time it runs."""
        return self.sb.path("what-a-hook-wrote")

    def ran(self, path: str) -> int:
        """How many times a hook that appends one line has run."""
        return text(path).count("x") if os.path.exists(path) else 0

    def gate(self, name: str = "go") -> str:
        """The path a gated command of this test waits for."""
        return os.path.join(self.work, name)

    def gated(self, name: str = "go") -> str:
        """A command that runs until this test opens that gate, then exits 0."""
        return f"while [ ! -f {self.gate(name)} ]; do sleep 0.02; done"

    def open_the_gate(self, name: str = "go") -> None:
        open(self.gate(name), "w").close()

    def nothing_of_the_job_is_left(self, w: jobq.Worker, jid: int) -> None:
        """This test's own way out for a job its own worker started: the group the job leads goes, and the
        wrapper that worker holds is waited for. So a test that drops its worker with a live child leaves no
        process of its own behind and none to be collected unreaped.

        Called while the job is running, since that is when its record names the group."""
        group = jobq._group_handle(jobq.find(jid))

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
        return journey.until(lambda: jobq.lock.Claim.holder(jobq._run_claim(q, jid)), timeout=20,
                             what=f"job {jid}'s run to hold its claim")

    def the_run_is_over(self, jid: int, q: str = "default") -> None:
        """Wait until nothing holds job jid's run claim, which is what a run being over means."""
        journey.until(lambda: jobq.lock.Claim.holder(jobq._run_claim(q, jid)) is None, timeout=20,
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
        self.assertIsNone(jobq.lock.Claim.holder(jobq._run_claim("default", j)))
        self.assertEqual([x["id"] for x in jobq._jobs()], [j])       # one record, under one id
        self.assertIsNone(jobq.find(j).get("rcfile"))
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "done")

    def test_a_worker_killed_holding_the_claim_leaves_no_running_job_unclaimed(self):
        """A job's run claim is held before its record says running. A worker that ends between the two
        leaves the job queued and the claim free, so a reader finds no running job whose claim nothing holds
        and whose run is not over."""
        j = self.add(cmd="true")
        self.a_worker_killed_at("the record")
        self.assertEqual(self.state(j), "queued")
        self.assertIsNone(jobq.lock.Claim.holder(jobq._run_claim("default", j)))
        self.assertEqual(jobq._jobs("running"), [])
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "done")

    def test_a_worker_killed_before_the_spawn_leaves_a_job_the_next_worker_reaps(self):
        """A pass marks every job it admitted running before it starts any of them. A worker that ends
        between the record and the start leaves a running job whose claim nobody holds, which is a run that
        is over: the next worker records it, and the exit code it reads is none, since the attempt wrote
        none."""
        j = self.add(cmd="true")
        self.a_worker_killed_at("the spawn")
        self.assertEqual(self.state(j), "running")
        self.assertIsNone(jobq.lock.Claim.holder(jobq._run_claim("default", j)))
        self.assertIsNone(jobq.ended_on_its_own(jobq.find(j)))
        jobq.Worker("default").carry_out()
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec["reason"]), ("failed", None, "lost"))

    def test_a_job_whose_worker_died_between_the_spawn_and_the_record_is_adopted(self):
        """The attempt's wrapper writes its own pid in the claim it is handed, so a worker that ends between
        the start and the record leaves a running job whose run is live and whose record names no process.
        The next worker adopts it from that claim: the pid, the group, the start time read beside it and the
        boot both were read in land on the record, and the job is reaped like any other."""
        j = self.add(cmd=self.gated())
        self.a_worker_killed_at("the start")
        held = self.the_run_is_live(j)
        self.assertEqual((self.state(j), jobq.find(j).get("pid")), ("running", None))
        w = jobq.Worker("default")
        w.carry_out()                   # the claim says which process the run is, and the record takes it
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["pid"], rec["boot"]), ("running", held["pid"], proc.boot_id()))
        self.assertEqual(rec["pgid"], proc.read(held["pid"]).pgid)
        self.assertIsNotNone(jobq._group_handle(rec), "the record names no group a signal could go through")
        self.open_the_gate()
        self.the_run_is_over(j)
        self.run_until(w, lambda: self.state(j) == "done")
        self.assertEqual(jobq.find(j)["rc"], 0)


class TestAWorkerKilledWhileItStops(InjectedCase):
    """The four steps of a stop, with the worker ending between two of them each time: what the record says
    went out, what really went out, and what the next worker does about the rest of it."""

    def a_job_whose_run_is_live(self, *args, cmd=None) -> tuple:
        """A running job of this test's, with its run live and this test's worker holding its wrapper. -> the
        job's id and that worker."""
        j = self.add(*args, cmd=cmd or self.gated())
        w = jobq.Worker("default")
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
        stop = jobq.find(j)["stop"]
        self.assertEqual((stop["t"], stop.get("sent"), stop.get("kill9"), stop.get("denied")),
                         (asked["t"], None, None, None))
        self.assertEqual(self.state(j), "running")
        went = self.recording()
        w.carry_out()
        self.assertEqual(went, [signal.SIGTERM])
        self.assertIsNotNone(jobq.find(j)["stop"]["sent"])
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
        stop = jobq.find(j)["stop"]
        self.assertEqual((stop["t"], stop.get("sent"), stop.get("kill9")), (asked["t"], None, None))
        went = self.recording(beside=lambda: jobq.find(j)["stop"].get("sent"))
        w.carry_out()
        self.assertEqual(went, [(signal.SIGTERM, None)])        # the record was marked after this went out
        self.assertIsNotNone(jobq.find(j)["stop"]["sent"])

    def test_a_grace_that_is_up_escalates_once_and_only_once(self):
        """The SIGKILL of a stop whose grace is up goes out once. A worker that ends between the grace being
        up and that signal leaves a stop with no SIGKILL marked, the next worker sends it, and no pass after
        that one sends another."""
        j, w = self.a_job_whose_run_is_live()
        self.a_kill_asks(j, grace=0.0)
        went = self.recording()
        w.carry_out()                                   # the SIGTERM, and its grace is up at once
        self.assertEqual(went, [signal.SIGTERM])
        self.assertIsNotNone(jobq.find(j)["stop"]["sent"])
        self.a_worker_killed_at("the send")
        self.assertIsNone(jobq.find(j)["stop"].get("kill9"))
        w.carry_out()
        self.assertEqual(went, [signal.SIGTERM, signal.SIGKILL])
        kill9 = jobq.find(j)["stop"]["kill9"]
        self.assertIsNotNone(kill9)
        for _ in range(3):
            w.carry_out()
        self.assertEqual(went, [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(jobq.find(j)["stop"]["kill9"], kill9)

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
        self.assertIsNotNone(jobq.find(j)["stop"]["sent"])
        self.assertEqual(self.a_worker_killed_at("the signal of the group"),
                         [f"sent {int(signal.SIGKILL)} alive"])
        self.assertIsNone(jobq.find(j)["stop"].get("kill9"))
        self.the_run_is_over(j)
        went.clear()                                    # what goes out from here is the next worker's alone
        w.carry_out()
        rec = jobq.find(j)
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
            stop = jobq.find(j)["stop"]
            self.assertEqual((stop["by"], stop["timeout"]), ("timeout", True))
            asked = self.a_kill_asks(j, grace=0.0)
            self.assertEqual((asked["by"], asked.get("was"), asked.get("timeout")), ("user", "timeout", None))
            w.carry_out()
            self.assertEqual(went, [signal.SIGTERM])
            self.a_worker_killed_at("the send")     # with the stop in flight and its grace up
            self.assertIsNone(jobq.find(j)["stop"].get("kill9"))
            self.assertEqual(self.state(j), "running")
        w.carry_out()                               # the next worker finishes it, for real this time
        self.assertIsNotNone(jobq.find(j)["stop"]["kill9"])
        self.the_run_is_over(j)
        w.carry_out()
        rec = jobq.find(j)
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
        w = jobq.Worker("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        rcfile = jobq.find(j)["rcfile"]
        self.assertEqual(os.path.basename(rcfile).split(".")[:2], [str(j), "1"])    # this attempt's own name
        self.assertFalse(os.path.exists(rcfile + ".part"))
        self.assertEqual(self.a_worker_killed_at("the exit code"), ["code 7"])
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec.get("rc"), rec["rcfile"]), ("running", None, rcfile))
        self.assertEqual(jobq.ended_on_its_own(rec)[0], 7)      # the same code, read again
        w.carry_out()
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["rc"]), ("failed", 7))
        self.assertEqual(rec["ended"], os.stat(rcfile).st_mtime)
        self.run_until(w, lambda: not w.hooks)
        self.assertEqual([h["event"] for h in jobq.find(j)["hooks"]], ["failed"])
        self.assertEqual(self.ran(f), 1)
        self.assertIsNone(jobq.find(j).get("history"))

    def test_an_outcome_recorded_without_its_hook_fires_it_from_the_next_worker(self):
        """An outcome and the hook it owes are written in one transaction, and the owed field is cleared once
        that hook has run. A worker that ends between the two leaves a record that says a hook is owed, and
        the next worker fires it: a hook fires once, and not never."""
        f = self.hook_file()
        j = self.add("--on-fail", f"echo x >> {f}", cmd="exit 3")
        w = jobq.Worker("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        self.a_worker_killed_at("the hook")
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec["hook_due"]), ("failed", 3, "failed"))
        self.assertEqual(self.ran(f), 0)
        self.assertIsNone(rec.get("hooks"))
        w2 = jobq.Worker("default")
        self.run_until(w2, lambda: not w2.hooks and "hook_due" not in jobq.find(j))
        self.assertEqual([h["event"] for h in jobq.find(j)["hooks"]], ["failed"])
        self.assertEqual(self.ran(f), 1)

    # The record of a hook that has run is written after that hook starts, so a worker that ends between the
    # two leaves the hook owed, and the next worker runs it again: the hook of one outcome runs twice. The
    # other order loses the hook of an outcome whose worker ends before it, and a hook that never fires is
    # the worse of the two, so what this order costs is a second run and never none.
    @unittest.expectedFailure
    def test_a_hook_that_ran_is_never_run_twice(self):
        """The hook of an outcome runs once. A worker that ends between the hook's own start and the record of
        it leaves a record that says the hook is owed, and the next worker reads that record."""
        f = self.hook_file()
        j = self.add("--on-fail", f"echo x >> {f}", cmd="exit 3")
        w = jobq.Worker("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        self.a_worker_killed_at("the start")        # the hook ran; what the job owes is not cleared
        journey.until(lambda: self.ran(f) == 1, timeout=20, what="the hook of this outcome to run")
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["hook_due"]), ("failed", "failed"))
        self.assertIsNone(rec.get("hooks"))
        w2 = jobq.Worker("default")
        self.run_until(w2, lambda: not w2.hooks and "hook_due" not in jobq.find(j))
        self.assertEqual(self.ran(f), 1, "the hook ran again for an outcome it had already run for")

    def test_a_requeue_that_lost_its_worker_keeps_one_history_entry(self):
        """A requeue is one change of one record: the attempt that ended goes into the history, the fields of
        that attempt go, and the job is queued for the next one. A worker that ends between that change and
        the line it writes in the job's log leaves one history entry, and the job keeps its delay: it is
        skipped while it is not ready, and the job behind it runs."""
        self.config("slots = 1")
        j = self.add("--retries", "1", "--retry-delay", "60", cmd="exit 1")
        later = self.add(cmd="true")
        w = jobq.Worker("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.assertEqual((self.state(j), self.state(later)), ("running", "queued"))
        self.the_run_is_over(j)
        self.a_worker_killed_at("the log line")
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["attempt"], len(rec["history"])), ("queued", 2, 1))
        self.assertEqual(rec["history"][0]["attempt"], 1)
        self.assertNotIn("rcfile", rec)                     # the fields of the attempt that ended are gone
        self.assertGreater(rec["not_before"], time.time())
        self.run_until(w, lambda: self.state(later) == "done")
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["attempt"], len(rec["history"])), ("queued", 2, 1))
        self.assertIsNone(w.waiting, "a job in its delay became the head of its queue")


class TestAStopAndAReapThatCross(InjectedCase):
    """One decision about one job, made inside the store's transaction on the record the store holds: a stop
    another actor wrote since the facts were read is part of it."""

    def a_kill_between_the_facts_and_the_decision(self, jid: int, grace: float = 10.0) -> journey.Gate:
        """Write a kill's stop in the window between the facts of a pass and its decision. The gate takes the
        step once, where the pass has the facts of this job and has opened no scope yet."""
        gate, real = journey.Gate(), jobq.Worker._step

        def step(w, job, facts):
            gate.once(lambda: self.a_kill_asks(jid, grace=grace))
            return real(w, job, facts)
        patch = mock.patch.object(jobq.Worker, "_step", step)
        patch.start()
        self.addCleanup(patch.stop)
        return gate

    def test_a_stop_racing_a_reap_ends_the_job_once(self):
        """A kill's stop that lands after the facts of a pass were read and before its decision is part of
        that decision: the job ends with the code it wrote itself, it is never retried, and the outcome is
        recorded once, so its hook fires once."""
        f = self.hook_file()
        j = self.add("--retries", "2", "--on-fail", f"echo x >> {f}", cmd="exit 5")
        w = jobq.Worker("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_over(j)
        gate = self.a_kill_between_the_facts_and_the_decision(j)
        w.carry_out()
        self.assertEqual(gate.ran, 1)
        rec = jobq.find(j)
        self.assertEqual((rec["state"], rec["rc"], rec["attempt"]), ("failed", 5, 1))
        self.assertEqual(rec["stop"]["by"], "user")
        self.assertIsNone(rec.get("history"), "a job a kill has touched was retried")
        self.run_until(w, lambda: not w.hooks and "hook_due" not in jobq.find(j))
        self.assertEqual([h["event"] for h in jobq.find(j)["hooks"]], ["failed"])
        self.assertEqual(self.ran(f), 1)

    def test_the_decision_is_made_on_the_record_the_store_holds(self):
        """The facts of a pass are read before its transaction and the record is read inside it. A stop
        written in between is carried out by that same pass: the SIGTERM goes out for it, and the record says
        so, although the facts were read when the job carried no stop."""
        j = self.add(cmd=self.gated())
        w = jobq.Worker("default")
        w.tick()
        self.nothing_of_the_job_is_left(w, j)
        self.the_run_is_live(j)
        self.assertIsNone(jobq.find(j).get("stop"))
        went = self.recording()
        gate = self.a_kill_between_the_facts_and_the_decision(j)
        w.carry_out()
        self.assertEqual((gate.ran, went), (1, [signal.SIGTERM]))
        self.assertIsNotNone(jobq.find(j)["stop"]["sent"])


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
        w = jobq.Worker("default")
        self.run_until(w, lambda: self.state(j) == "failed", timeout=30)
        self.assertTrue(reader.dead(30), "the reader of the store did not come to the end of it")
        self.assertEqual(reader.returncode, 0, reader.stderr.read())
        seen = json.loads(text(rows))
        for row in seen:
            self.assertEqual([i for i, _st in row], [j], f"the job is named once in a pass: {row}")
            self.assertIn(row[0][1], jobq.STATES, row)
        self.assertEqual((seen[0][0][1], seen[-1][0][1]), ("held", "failed"))   # it looked through the whole
        self.assertGreater(len({st for row in seen for _i, st in row}), 1, seen)

    def test_a_write_of_a_copy_from_outside_the_scope_is_refused(self):
        """A record a scope writes is a record that scope read. A copy read before the scope, and a copy of
        the scope's own record, are both refused, so what another actor recorded meanwhile is never written
        over; a listing takes no write at all."""
        j = self.add(cmd="true")
        outside = jobq.find(j)
        outside["prio"] = 1
        with jobq.txn() as s:
            with self.assertRaises(RuntimeError):
                s.save(outside)
            with self.assertRaises(RuntimeError):
                s.save(dict(s.job(j)))
            s.job(j)["prio"] = 3
            s.save(s.job(j))
        self.assertEqual(jobq.find(j)["prio"], 3)
        with jobq.listing() as s:
            with self.assertRaises(RuntimeError):
                s.save(s.job(j))
        self.assertEqual(jobq.find(j)["prio"], 3)

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
        self.assertEqual(sorted(x["id"] for x in jobq._jobs()), sorted(ids))

    def test_a_queue_another_layout_wrote_is_refused_where_it_is_opened(self):
        """A queue says what its store looks like in a file of its own. One holding another number holds
        records this pp cannot read: every path that opens that queue refuses, with the exit status of a
        refusal, and writes nothing. A queue is opened before anything of it is read, so the one read that
        takes no lock refuses there too. The queue is read again once the number is its own."""
        j = self.add(cmd="true")
        fmt = jobq._format_file("default")
        self.assertEqual(jobq._format_of("default"), jobq.FORMAT)
        self.addCleanup(jobq.paths.write_json, fmt, jobq.FORMAT)     # before this case's own way out reads it
        was = self.files()
        jobq.paths.write_json(fmt, jobq.FORMAT + 1)
        for argv in (["status"], ["status", "-q", "default"], ["show", str(j)], ["kill", str(j)],
                     ["clean", "-y"], ["worker", "start", "-q", "default"], ["work", "-q", "default"],
                     ["add", "--no-start", "--cwd", self.work, "--", "true"]):
            self.assertEqual(self.call(*argv)[0], 2, argv)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as refused:
            jobq.find(j)
        self.assertEqual(refused.exception.code, 2)
        self.assertEqual(self.files(), was)                 # and nothing of the queue was written
        jobq.paths.write_json(fmt, jobq.FORMAT)
        self.assertEqual(self.state(j), "queued")
        self.run_until(jobq.Worker("default"), lambda: self.state(j) == "done")


class TestOneWorkerPerQueue(InjectedCase):
    """Two processes that both want to be a queue's worker, and the wake that reaches the one that is."""

    def stop_the_worker(self, q: str = "default") -> None:
        """Stop the worker a test of this class started, and wait for its claim to be free, so nothing of the
        test is left running."""
        h = jobq.worker_now(q).handle
        if h is not None:
            proc.signal_pid(h, signal.SIGTERM)
        journey.until(lambda: jobq.worker(q) is None, timeout=30,
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
        up = journey.until(lambda: jobq.worker_now("default").rec, timeout=30, what="the worker that won")
        self.assertEqual(up["pid"], [p for p in both if p.poll() is None][0].pid)
        self.assertIsNone(jobq.lock.Claim.take(jobq._worker_claim("default")), "the claim was free to take")
        journey.until(lambda: jobq._jobs("running"), timeout=30, what="the worker to start the head of its queue")
        self.assertEqual([x["state"] for x in jobq._jobs()], ["running", "queued"])
        self.assertEqual([x["attempt"] for x in jobq._jobs("running")], [1])
        self.assertEqual(self.state(b), "queued")
        self.assertEqual(self.state(a), "running")

    def test_a_wake_sent_during_a_tick_ends_the_next_wait(self):
        """A wake reaches a worker through that worker's own pipe. One sent while a tick runs is in the pipe
        when the next wait begins, so it ends that wait instead of being lost, and it ends one wait only."""
        w = jobq.Worker("default")
        gate, real = journey.Gate(), jobq.Worker.carry_out

        def pass_over(wk, *a, **kw):
            gate.once(lambda: proc.raise_to_self(jobq.WAKE))     # as a verb wakes a worker mid-tick
            return real(wk, *a, **kw)
        with jobq.waking() as waits, mock.patch.object(jobq.Worker, "carry_out", pass_over):
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
        up = journey.until(lambda: jobq.worker_now("default").rec, timeout=30, what="the queue's worker")
        self.assertIsNotNone(proc.Handle.running(up["pid"], up["pstart"]), "the claim's holder is gone")
        self.assertIsNone(jobq.lock.Claim.take(jobq._worker_claim("default")), "the claim was free to take")
        with mock.patch.object(jobq, "_start_worker",
                               lambda q: self.fail(f"a start began for {q}, whose worker is up")):
            self.assertTrue(jobq.worker_up("default", quiet=True).up)       # an ask that starts nothing
        journey.until(lambda: jobq._jobs("running"), timeout=30, what="the worker to start the head of its queue")
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
        with jobq.txn() as s:
            rec = s.job(j)
            rec.update(state="running", started=time.time() - 60, pid=other.pid, pgid=other.pid,
                       pstart=start - 7, boot=proc.boot_id(),       # the id was handed out again since
                       stop={"by": "user", "t": time.time() - 30, "grace": 0, "sent": time.time() - 30})
            s.save(rec)
        self.assertIsNotNone(jobq._group_handle(jobq.find(j)), "a record of this boot names a group")
        jobq.Worker("default").carry_out()
        self.assertIsNone(other.poll(), "a group that is not the job's was signalled")
        rec = jobq.find(j)
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
        journey.until(lambda: jobq.watchers(jobq.find(j)), timeout=30, what="the wait's lease on the job")
        leases = os.listdir(d)
        self.assertEqual(len(leases), 1, leases)
        self.assertEqual(jobq.watchers(jobq.find(j))[0]["pid"], wait.pid)
        wait.signal(signal.SIGKILL)
        journey.until(lambda: wait.poll() is not None, timeout=30, what="the wait to end")
        self.assertEqual(wait.poll(), -signal.SIGKILL)
        self.assertEqual(os.listdir(d), leases)                 # nothing of its own ran
        self.assertEqual(jobq.watchers(jobq.find(j)), [])       # and nobody holds what it left
        jobq.Worker("default").tick()
        self.assertEqual(os.listdir(d), [])

    def test_a_sweep_runs_once_per_attempt_whose_run_is_over(self):
        """The sweep of a stopped job's group runs where its run is over: the SIGKILL goes out inside the
        decision's scope, once for that attempt, so a process the job left behind does not outlive it. It
        records nothing and changes no outcome: the job keeps the exit code it wrote."""
        kid = self.sb.path("the-job-left-this")
        j = self.add(cmd=f"sleep 300 & echo $! > {kid}; exit 0")
        w = jobq.Worker("default")
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
        group = jobq.find(j)["pgid"]
        went: list = []
        real = jobq.proc.signal_group

        def sender(h, sig):
            went.append((sig, h.id))
            return real(h, sig)
        with mock.patch.object(jobq.proc, "signal_group", sender):
            w.carry_out()
            rec = jobq.find(j)
            self.assertEqual((rec["state"], rec["rc"]), ("done", 0))
            self.assertEqual(went, [(signal.SIGKILL, group)])
            journey.until(lambda: proc.Handle.running(left, start) is None, timeout=30,
                          what="what the job left behind to go with it")
            for _ in range(3):
                w.carry_out()
            self.assertEqual(went, [(signal.SIGKILL, group)])

if __name__ == "__main__":
    unittest.main()
