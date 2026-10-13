"""User journeys through `pp queue`: what a person (or an agent) types, step by step, and what they see.

Each test is one story through the real CLI and a real detached worker (poll 0.2s), in the sandbox of
tests/journey.py. A fake nvidia-smi that sees no GPU keeps the machine's real cards out of it unless a story
installs fake ones."""
import json
import os
import shlex
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from journey import ROOT, Journey, replace_file  # noqa: E402

BASE = "[queue.default]\npoll = 0.2\nidle_exit = 30\n"


class QueueJourney(Journey):
    def setUp(self):
        super().setUp()
        self.setcfg(BASE)

    def setcfg(self, text: str):
        """Replace the whole config file (TOML forbids defining a table twice, so stories rewrite it), in one step."""
        replace_file(f"{self.env['PP_CONFIG_DIR']}/config.toml", text)

    def add(self, *args, env=None, cwd=None) -> str:
        r = self.pp("queue", "add", *args, env=env, cwd=cwd)
        return r.stdout.split()[-1]

    def show(self, jid) -> dict:
        return json.loads(self.pp("queue", "show", str(jid), "--json").stdout)

    def wait(self, *ids, code=0, env=None, extra=()):
        return self.pp("queue", "wait", *map(str, ids), "--timeout", "20", "--poll", "0.2", *extra, code=code, env=env)

    def status(self, *args, code=0) -> str:
        return self.pp("queue", *args, code=code).stdout

    def state(self, jid) -> str:
        return self.show(jid)["state"]

    def as_agent(self, sid="sess-a"):
        return dict(self.env, CLAUDE_CODE_SESSION_ID=sid)


class TestFirstSteps(QueueJourney):
    def test_first_job_from_nothing_to_its_log(self):
        self.assertIn("no queues yet", self.status())
        r = self.pp("queue", "add", "--", "sh", "-c", "echo hello-from-the-job")
        jid = r.stdout.strip()
        self.assertTrue(jid.isdigit(), r.stdout)
        self.assertIn("started the default worker", r.stderr)          # nobody has to start it
        self.assertIn(f"queued {jid} (default)", r.stderr)
        out = self.wait(jid).stdout
        self.assertIn(f"done {jid} rc=0", out)
        st = self.status()
        self.assertIn("== default   worker up", st)
        self.assertIn(f"{jid}  done  rc=0", st)
        show = self.pp("queue", "show", jid).stdout
        for word in ("(default)  done", "command", "sh -c 'echo hello-from-the-job'", "\n  hello-from-the-job"):
            self.assertIn(word, show)
        self.assertIn("hello-from-the-job", self.pp("queue", "log", jid).stdout)
        j = self.show(jid)
        self.assertEqual((j["state"], j["rc"], j["watchers"]), ("done", 0, []))
        d = json.loads(self.status("--json"))
        self.assertEqual([x["id"] for x in d["recent"]], [int(jid)])
        self.assertEqual(d["config_errors"], [])
        self.assertEqual(json.loads(self.status("ls", "--json"))["recent"], d["recent"])

    def test_a_quick_job_reports_how_long_it_ran_not_how_long_until_it_was_noticed(self):
        jid = self.add("--", "sh", "-c", "while [ ! -f go ]; do sleep 0.05; done")
        self.until(lambda: self.state(jid) == "running")
        self.assertIn("stopped worker", self.pp("queue", "worker", "stop").stdout)
        rcf = self.show(jid)["rcfile"]                      # where this attempt writes its exit code
        self.write("work/go", "")
        self.until(lambda: os.path.exists(rcf))             # it ended, unwatched
        wrote_its_code = os.path.getmtime(rcf)              # its own last act, with nothing watching for it
        noticed = time.time()
        self.pp("queue", "worker", "start")                 # and a worker sees it only now
        self.wait(jid)
        j = self.show(jid)
        self.assertAlmostEqual(j["ended"], wrote_its_code, places=3)   # when it ended ...
        self.assertLess(j["ended"], noticed)                           # ... not when it was noticed
        self.assertAlmostEqual(j["seconds"], j["ended"] - j["started"], places=3)

    def test_mistakes_while_adding_are_refused_before_anything_is_queued(self):
        bad = [(["add"], "no command"),
               (["add", "--from", "nope.txt"], "No such file"),
               (["add", "--need", "slots=-1", "--", "true"], "positive number"),
               (["add", "--need", "slots=nan", "--", "true"], "positive number"),
               (["add", "--env", "FOO", "--", "true"], "NAME=VALUE"),
               (["add", "--cwd", "/no/such/dir", "--", "true"], "no directory"),
               (["add", "--retries", "-1", "--", "true"], "--retries"),
               (["add", "--retry-if", "(", "--", "true"], "--retry-if"),
               (["add", "--timeout", "soon", "--", "true"], "not a duration"),
               (["add", "--need", "net=1", "--", "./fetch.sh"], "nothing provides 'net'")]
        self.write("work/quotes.txt", "echo fine\necho \"unclosed\n")
        bad.append((["add", "--from", "quotes.txt"], "line 2"))
        for argv, want in bad:
            r = self.pp("queue", *argv, code=2)
            self.assertIn(want, r.stderr, argv)
            self.assertNotIn("Traceback", r.stderr, argv)
        self.assertIn("no queues yet", self.status())               # nothing slipped through
        for argv in (["show", "999"], ["kill", "abc"], ["log", "7"], ["drop", "3-1"]):
            self.assertIn("no job matches", self.pp("queue", *argv, code=2).stderr)
        self.assertIn("invalid choice", self.pp("queue", "bogus", code=2).stderr)
        self.setcfg(BASE + "[queue.dl]\npoll = 0.2\n[queue.dl.resources]\nnet = 2\n")      # declared, as in the help
        self.write("work/fetch.sh", "#!/bin/sh\necho fetched\n", 0o755)
        self.assertIn("done", self.wait(self.add("-q", "dl", "--need", "net=1", "--", "./fetch.sh")).stdout)

    def test_one_job_per_line_from_a_file_or_stdin(self):
        self.write("work/cmds.txt", "# nightly\necho one\n\nsh -c 'echo two; exit 1'\n")
        ids = self.pp("queue", "add", "--from", "cmds.txt", "--prio", "5").stdout.split()
        ids += self.pp("queue", "add", "--from", "-", input="echo three\n").stdout.split()
        self.assertEqual(len(ids), 3)
        r = self.wait(f"{ids[0]}-{ids[-1]}", code=1)              # a range
        self.assertIn(f"done {ids[0]}", r.stdout)
        self.assertIn(f"FAILED {ids[1]} rc=1", r.stdout)
        self.assertIn(f"done {ids[2]}", r.stdout)
        self.assertEqual(self.show(ids[0])["prio"], 5)
        self.assertIn("running 0, queued 0", self.pp("queue", "wait", "--idle", "--timeout", "20", "--poll", "0.2").stdout)


class TestOrdering(QueueJourney):
    def test_slots_run_order_reprio_hold_release_and_drop(self):
        self.setcfg(BASE + "[queue.default.resources]\nslots = 1\n")
        blocker = self.add("--tag", "blocker", "--", "sleep", "30")
        a, b, c = (self.add("--tag", t, "--", "true") for t in ("a", "b", "c"))
        self.until(lambda: self.state(blocker) == "running")
        st = self.status()
        self.assertIn("slots 1/1", st)
        self.assertIn("waiting: slots", st)
        self.assertNotIn("[pp queue]", st)          # a running job with no output yet shows no pp banner as its line
        self.assertIn(f"reprio {c} -> p0", self.pp("queue", "reprio", "c", "0").stdout)     # by tag
        queued = self.status()
        self.assertLess(queued.index(f"  {c}  p0"), queued.index(f"  {a}  p10"))
        self.pp("queue", "hold", b)
        self.assertIn("is held; hold works on queued jobs", self.pp("queue", "hold", b, code=2).stderr)
        r = self.pp("queue", "drop", blocker, code=2)
        self.assertIn(f"drop works on queued/held jobs. `pp queue kill {blocker}` stops it", r.stderr)
        self.pp("queue", "drop", a)
        self.assertIn(f"killed {blocker} (stopped on SIGTERM)", self.pp("queue", "kill", blocker).stdout)
        self.wait(c)
        self.assertEqual(self.state(b), "held")
        r = self.pp("queue", "wait", b, "--timeout", "1", "--poll", "0.2", code=3)      # held: it never ends
        self.assertIn(f"{b} held", r.stderr)
        self.pp("queue", "release", b)
        self.wait(b)
        self.assertLess(self.show(c)["started"], self.show(b)["started"])
        self.assertEqual(self.show(blocker)["reason"], "killed")
        self.assertIn("no job matches", self.pp("queue", "show", a, code=2).stderr)      # dropped: gone
        self.assertIn("not running", self.pp("queue", "kill", c, code=2).stderr)

    def test_a_job_added_on_hold_runs_once_released(self):
        self.setcfg(BASE + "[queue.later]\npoll = 0.2\n")
        h = self.add("-q", "later", "--hold", "--", "echo", "released-ok")
        self.assertIn("worker down", self.pp("queue", "-q", "later", "worker", "status").stdout)
        r = self.pp("queue", "release", h)
        self.assertIn("started the later worker", r.stderr)            # nothing else would launch it
        self.wait(h)


class TestFailures(QueueJourney):
    def test_failure_then_log_note_and_retry(self):
        jid = self.add("--tag", "boom", "--", "sh", "-c", "echo working; echo 'Traceback: kaboom' >&2; exit 4")
        r = self.wait(jid, code=1)
        self.assertIn(f"FAILED {jid} rc=4", r.stdout)
        self.assertIn("  | Traceback: kaboom", r.stdout)
        self.assertNotIn(f"job {jid} (default) in", r.stdout)       # the launch banner only repeats the command
        self.assertIn(f"next: pp queue log {jid} | pp queue retry {jid}", r.stdout)
        self.assertIn("Traceback: kaboom", self.pp("queue", "log", jid).stdout)
        self.assertIn(f"noted on {jid}", self.pp("queue", "note", jid, "kaboom", "because", "X").stdout)
        self.pp("queue", "note", jid, "--by", "nightly", "told the team")
        self.assertIn("note: told the team", self.status())        # the latest note, in the recent list
        notes = self.show(jid)["notes"]
        self.assertEqual([(n["text"], n["by"]) for n in notes], [("kaboom because X", "user"), ("told the team", "nightly")])
        self.pp("queue", "note", jid, code=2)
        r = self.pp("queue", "retry", jid)
        new = r.stdout.strip()
        self.assertIn(f"retry of {jid} queued as {new}", r.stderr)
        self.wait(new, code=1)
        self.assertRegex(self.pp("queue", "show", new).stdout, rf"retry of +job {jid}\n")
        live = self.add("--", "sleep", "30")
        self.assertIn("retry re-queues ended jobs", self.pp("queue", "retry", live, code=2).stderr)

    def test_timeout_then_retries_that_match_and_that_do_not(self):
        t = self.add("--timeout", "1s", "--tag", "slow", "--", "sh", "-c", 'trap "echo got-TERM; exit 7" TERM; '
                     'while :; do sleep 0.1; done')
        flaky = self.add("--retries", "2", "--retry-delay", "0s", "--retry-if", "Connection reset", "--tag", "flaky",
                         "--", "sh", "-c", "echo Connection reset by peer; exit 1")
        other = self.add("--retries", "2", "--retry-delay", "0s", "--retry-if", "Connection reset", "--tag", "disk",
                         "--", "sh", "-c", "echo disk full; exit 1")
        r = self.wait(t, flaky, other, code=1)
        self.assertIn(f"FAILED {t} rc=- (timeout)", r.stdout)
        self.assertIn("got-TERM", r.stdout)
        self.assertEqual(self.show(flaky)["attempt"], 3)                  # 0s delay: retried at once, twice
        self.assertEqual(len(self.show(flaky)["history"]), 2)
        self.assertEqual(self.show(other)["attempt"], 1)
        self.assertIn("not retried: neither the reason (exit 1) nor the log tail matches /Connection reset/", r.stdout)
        self.assertIn("not retried", self.pp("queue", "show", other).stdout)

    def test_a_queue_default_timeout_can_be_lifted_for_one_job(self):
        self.setcfg(BASE + 'timeout = "1s"\n')
        a = self.add("--", "sleep", "2")
        b = self.add("--timeout", "none", "--", "sleep", "2")
        self.assertEqual((self.show(a)["timeout"], self.show(b)["timeout"]), (1.0, None))
        self.wait(a, code=1)
        self.wait(b)

    def test_hooks_get_the_outcome_in_their_environment(self):
        jid = self.add("--tag", "hooky", "--on-fail", 'echo "$PP_QUEUE_JOB $PP_QUEUE_EVENT rc=$PP_QUEUE_RC $PP_QUEUE_TAG" > failed.out',
                       "--on-done", "touch done.out", "--", "sh", "-c", "exit 3")
        ok = self.add("--on-done", "echo ok > done2.out", "--", "true")
        self.wait(jid, code=1)
        self.wait(ok)
        out = self.until(lambda: os.path.exists(f"{self.work}/failed.out") and self.read(f"{self.work}/failed.out"))
        self.assertEqual(out, f"{jid} failed rc=3 hooky\n")
        self.assertTrue(self.until(lambda: os.path.exists(f"{self.work}/done2.out")))
        self.assertFalse(os.path.exists(f"{self.work}/done.out"))
        self.assertIn("failed: echo", self.pp("queue", "show", jid).stdout)

    def test_log_follow_rides_through_a_retry_and_exits_like_wait(self):
        jid = self.add("--retries", "1", "--retry-delay", "0s", "--", "sh", "-c", "echo line-$PP_QUEUE_JOB; sleep 0.5; exit 1")
        r = self.pp("queue", "log", jid, "-f", code=1, timeout=30)
        self.assertEqual(r.stdout.count(f"line-{jid}"), 2)
        self.assertIn("attempt 1/2 after exit 1", r.stdout)
        self.assertIn(f"FAILED {jid}", r.stderr)


class TestWorkers(QueueJourney):
    def test_a_stopped_worker_leaves_jobs_running_and_the_next_one_adopts_them(self):
        jid = self.add("--", "sh", "-c", "sleep 1.5; echo adopted; exit 3")
        self.until(lambda: self.state(jid) == "running")
        self.assertIn("stopped worker", self.pp("queue", "worker", "stop").stdout)
        self.assertIn("worker down", self.pp("queue", "worker", "status").stdout)
        st = self.status()
        self.assertIn("nothing records how they end: `pp queue worker start -q default`", st)
        self.assertIn("started worker", self.pp("queue", "worker", "start").stdout)
        self.assertIn("already up", self.pp("queue", "worker", "start").stdout)
        r = self.wait(jid, code=1)
        self.assertIn(f"FAILED {jid} rc=3", r.stdout)
        self.assertIn("adopting 1 running job", self.read(f"{self.env['PP_STATE_DIR']}/queue/default/worker.log"))

    def test_killing_a_job_that_already_ended_records_how_it_really_ended(self):
        jid = self.add("--", "sh", "-c", "while [ ! -f go ]; do sleep 0.05; done; exit 5")
        self.until(lambda: self.state(jid) == "running")
        self.assertIn("stopped worker", self.pp("queue", "worker", "stop").stdout)    # nobody records the end now
        rcf = self.show(jid)["rcfile"]
        self.write("work/go", "")
        self.until(lambda: os.path.exists(rcf))                                       # it ended
        r = self.pp("queue", "kill", jid)
        self.assertIn(f"job {jid} had already ended: FAILED {jid} rc=5", r.stdout)
        self.assertEqual((self.show(jid)["rc"], self.show(jid).get("reason")), (5, None))

    def test_a_kill_with_no_worker_up_starts_one_and_it_stops_the_job(self):
        """Only a queue's worker stops its jobs. With no worker up the kill starts one, that worker carries
        out the stop, and the queue's queued jobs are launched too, as they are after any other `add`."""
        jid = self.add("--", "sh", "-c", "while :; do sleep 0.1; done")
        self.until(lambda: self.state(jid) == "running")
        self.assertIn("stopped worker", self.pp("queue", "worker", "stop").stdout)
        self.assertIn(f"kill 1 job(s): {jid}", self.pp("queue", "kill", jid, "-n").stdout)
        self.assertEqual(self.state(jid), "running")        # -n only shows, so nothing asked it to stop
        queued = self.add("--no-start", "--", "true")
        r = self.pp("queue", "kill", jid)
        self.assertIn("started the default worker", r.stderr)
        self.assertIn(f"killed {jid} (stopped on SIGTERM)", r.stdout)
        self.assertEqual(self.show(jid)["reason"], "killed")
        self.assertIn("worker up", self.pp("queue", "worker", "status").stdout)
        self.wait(queued)                                   # and the rest of the queue ran

    def test_a_stop_is_not_a_pause_and_the_next_add_starts_a_worker(self):
        """What the help says about `pp queue worker stop`: it stops the worker that is up, and the next
        command that needs a worker starts one."""
        self.wait(self.add("--", "true"))
        self.assertIn("stopped worker", self.pp("queue", "worker", "stop").stdout)
        self.assertIn("worker down", self.pp("queue", "worker", "status").stdout)
        jid = self.add("--", "true")
        self.assertIn("worker up", self.pp("queue", "worker", "status").stdout)
        self.wait(jid)


class TestResources(QueueJourney):
    def test_a_shared_counter_and_a_queues_own_share_of_it(self):
        self.setcfg(BASE + "[queue.a]\npoll = 0.2\n[queue.b]\npoll = 0.2\n[queue.b.resources]\nlicense = 1\n"
                    "[resources]\nlicense = 2\n")
        self.assertIn("[resources] license = 2", self.pp("queue", "add", "--need", "license=3", "--", "true", code=2).stderr)
        self.assertIn("[queue.b.resources] license = 1",
                      self.pp("queue", "add", "-q", "b", "--need", "license=2", "--", "true", code=2).stderr)
        a1 = self.add("-q", "a", "--need", "license=1", "--", "sleep", "30")
        b1 = self.add("-q", "b", "--need", "license=1", "--", "sleep", "30")
        self.until(lambda: self.state(a1) == self.state(b1) == "running")    # both licences held before any waiter
        b2 = self.add("-q", "b", "--need", "license=1", "--", "true")
        a2 = self.add("-q", "a", "--need", "license=1", "--", "true")
        st = self.until(lambda: (s := self.status()).count("waiting: license") == 2 and s)
        self.assertIn("shared: license 2/2", st)
        self.assertIn("waiting: license: [resources] license; 2+1 > 2", st)              # a2: the shared pool
        self.assertIn("waiting: license: [queue.b.resources] license; 1+1 > 1", st)      # b2: b's own share
        self.pp("queue", "kill", a1)
        self.wait(a2)
        self.assertEqual(self.state(b2), "queued")

    def test_a_probed_pool_and_a_failing_probe(self):
        self.write("home/free", "1\n")
        self.setcfg(BASE + '[resources]\nnodes = { probe = "cat $HOME/free", every = "0.2s", settle = "60s" }\n')
        first = self.add("--need", "nodes=1", "--", "sleep", "30")
        second = self.add("--need", "nodes=1", "--", "true")
        self.until(lambda: self.state(first) == "running")
        st = self.until(lambda: (s := self.status()) and "probe says" in s and s)
        self.assertIn("not free ([resources] nodes); probe says 1, 1 of it launched since, need 1", st)
        self.assertIn("nodes 1 in use (probe: 1 free", st)
        self.setcfg(BASE + '[resources]\nnodes = { probe = "echo cannot reach slurmctld >&2; exit 1", every = "0.2s" }\n')
        st = self.until(lambda: (s := self.status()) and "probe failed" in s and s)
        self.assertIn("exit 1: cannot reach slurmctld", st)
        self.assertEqual(self.state(second), "queued")

    def test_gpu_memory_is_reserved_per_card(self):
        self.gpus((24, 2), (12, 1))
        self.assertIn("no card's budget is that large (22)", self.pp("queue", "add", "--gpu-gb", "30", "--", "true", code=2).stderr)
        cmd = ["--", "sh", "-c", "echo card=$CUDA_VISIBLE_DEVICES; sleep 30"]
        g1, g2, g3 = self.add("--gpu-gb", "20", *cmd), self.add("--gpu-gb", "10", *cmd), self.add("--gpu-gb", "10", *cmd)
        self.until(lambda: self.state(g2) == "running")
        self.assertEqual((self.show(g1)["cards"], self.show(g2)["cards"]), ([0], [1]))
        self.assertIn("card=1", self.until(lambda: "card=1" in (s := self.pp("queue", "log", g2).stdout) and s))
        st = self.status()
        self.assertIn("GPU0 0% 2.0/24G  GPU1 0% 1.0/12G", st)
        self.assertIn("gpu_gb 10 does not fit on any card; card 0 reserved 20+10 > 22", st)
        self.assertIn("card 1 reserved 10+10 > 10", st)
        self.assertEqual(self.state(g3), "queued")
        self.tool("nvidia-smi", "exit 9\n")
        self.assertIn("nvidia-smi sees no GPU (nvidia-smi exited 9)",     # its own word on why
                      self.pp("queue", "add", "--gpu-gb", "1", "--", "true", code=2).stderr)


class TestConfigMistakes(QueueJourney):
    def test_a_broken_config_is_named_and_stops_launches_but_not_bookkeeping(self):
        runner = self.add("--tag", "gated", "--", "sh", "-c", "while [ ! -f go ]; do sleep 0.05; done")
        self.until(lambda: self.state(runner) == "running")       # and it runs until the story says so
        self.setcfg(BASE + "retry = 2\n")                         # a typo: retries
        later = self.pp("queue", "add", "--", "true", code=1)
        self.assertIn("unknown retry", later.stderr)
        st = self.status(code=1)                                   # still shows what runs, and why nothing starts
        self.assertIn("bad config: [queue.default]: unknown retry", st)
        self.assertIn(f"{runner}  gated", st)
        self.write("work/go", "")
        self.wait(runner, extra=("--no-start",))                   # the worker still records the end
        self.setcfg(BASE.replace("poll = 0.2", 'poll = "fast"'))
        self.assertIn("poll: 'fast' is not a duration", self.status(code=1))
        self.setcfg("[queue.default]\nretries = \"two\"\n")
        r = self.pp("queue", "add", "--", "true", code=1)
        self.assertIn("retries", r.stderr)              # the key it is about, and what it takes
        self.assertIn("a whole number", r.stderr)
        self.assertNotIn("Traceback", r.stderr)
        self.setcfg("[resources\nx = 1\n")
        self.assertIn("bad config", self.pp("queue", code=1).stdout)
        self.setcfg(BASE)
        self.assertNotIn("bad config", self.status())
        log = self.read(f"{self.env['PP_STATE_DIR']}/queue/default/worker.log")
        self.assertIn("bad config, nothing launches until it is fixed", log)
        self.assertLess(log.count("unknown retry"), 3)              # said once per change, not every loop

    def test_queue_names(self):
        for name in ("DL", "probes", "watchers", "a/b"):
            self.assertIn("is not a queue name", self.pp("queue", "add", "-q", name, "--", "true", code=2).stderr)
        self.add("-q", "dl", "--", "true")
        self.assertIn("no queue 'dlx' yet (there are: dl)", self.status("-q", "dlx"))


class TestAgent(QueueJourney):
    def test_an_agent_queues_waits_on_its_own_jobs_and_is_told_how(self):
        me, other = self.as_agent("sess-a"), self.as_agent("sess-b")
        r = self.pp("queue", "add", "--", "sh", "-c", "sleep 0.5; echo agent-tail; exit 2", env=me)
        mine = r.stdout.strip()
        self.assertIn(f"run `pp queue wait {mine}` in the background", r.stderr)
        theirs = self.add("--hold", "--", "true", env=other)
        ok = self.pp("queue", "add", "--", "sleep", "1", env=me).stdout.strip()
        self.assertIsNone(self.show(ok)["on_fail"])                         # it hears back by waiting, not by a hook
        self.assertEqual(self.show(mine)["owner"]["session"], "sess-a")
        self.wait(mine, code=1)                   # it ended before the agent waited (a person saw it): not missed
        r = self.wait(code=1, env=me, extra=("--mine",))
        self.assertIn(f"FAILED {mine} rc=2", r.stdout)
        self.assertIn("| agent-tail", r.stdout)
        self.assertIn(f"done {ok}", r.stdout)
        self.assertNotIn(f" {theirs} ", r.stdout)
        self.assertIn("wait pid", self.pp("queue", "show", mine).stdout)       # reported to
        self.assertIn("nothing of this session's", self.wait(env=me, extra=("--mine",)).stdout)
        self.assertIn("not inside an agent session", self.wait(code=2, extra=("--mine",)).stderr)
        self.assertNotIn("in the background", self.pp("queue", "add", "--", "true").stderr)     # a person: no hint

    def test_watch_streams_each_event_and_a_drop_is_not_success(self):
        x = self.add("--hold", "--tag", "hx", "--", "true")
        y = self.add("--hold", "--tag", "hy", "--", "true")
        w = self.spawn("queue", "watch", x, y, "--poll", "0.2", "--timeout", "20")
        self.until(lambda: "watch pid" in self.pp("queue", "show", x).stdout)
        self.pp("queue", "release", x)
        self.until(lambda: self.state(x) == "done")
        self.pp("queue", "drop", y)
        out, err = w.communicate(timeout=20)
        self.assertEqual(w.returncode, 1)
        lines = out.splitlines()
        self.assertEqual(lines[0], f"[queue watch] {x} held, {y} held")
        self.assertIn(f"{x} released  hx", lines)
        self.assertIn(f"{x} started  hx", lines)
        self.assertTrue(any(ln.startswith(f"done {x} rc=0") for ln in lines), out)
        self.assertIn(f"dropped {y}", lines)
        self.assertIn("held, so this waits forever. `pp queue release` them", err)


class TestCleaning(QueueJourney):
    def test_clean_previews_refuses_without_a_terminal_then_removes(self):
        ids = [self.add("--", "true") for _ in range(3)]
        self.wait(*ids)
        live = self.add("--hold", "--", "true")
        r = self.pp("queue", "clean", "-n")
        self.assertIn("3 ended job record(s)", r.stdout)
        self.assertIn(f"default: {ids[0]}-{ids[2]}", r.stdout)
        self.assertIn("pass -y", self.pp("queue", "clean", code=2).stderr)
        self.assertEqual(self.state(ids[0]), "done")
        self.assertIn("nothing to clean", self.pp("queue", "clean", "--older", "1d", "-y").stdout)
        self.assertIn("removed 3", self.pp("queue", "clean", "-y").stdout)
        self.assertEqual(os.listdir(f"{self.env['PP_STATE_DIR']}/queue/default/logs"), [])
        self.assertEqual(self.state(live), "held")



class TestSelections(QueueJourney):
    """A range, or a tag several jobs share, is a selection: jobs it does not suit are left as they are, and kill and
    drop show it and ask first. A job named by its id must suit the verb."""

    def running(self, jid):
        self.assertTrue(self.until(lambda: self.state(jid) == "running"), self.state(jid))

    def test_drop_a_range_shows_asks_and_leaves_a_running_job(self):
        run = self.add("--tag", "r", "--", "sleep", "30")
        self.running(run)
        a = self.add("--hold", "--tag", "a", "--", "true")
        b = self.add("--hold", "--tag", "b", "--", "true")
        r = self.pp("queue", "drop", f"{run}-{b}", "-n")
        self.assertIn(f"drop 2 job(s): {a} a, {b} b", r.stdout)
        self.assertIn(f"left as they are: {run} running", r.stdout)
        self.assertEqual(self.state(a), "held")
        self.assertIn("pass -y", self.pp("queue", "drop", f"{run}-{b}", code=2).stderr)    # no terminal to ask in
        r = self.pp("queue", "drop", f"{run}-{b}", "-y")              # did what it could: exit 0
        self.assertIn(f"dropped {a}", r.stdout)
        self.assertEqual(self.state(run), "running")
        self.assertIn(f"job {run} is running", self.pp("queue", "drop", run, code=2).stderr)    # named: an error
        self.assertIn(f"killed {run}", self.pp("queue", "kill", run).stdout)                     # an id: at once

    def test_hold_over_a_range_lists_what_it_left(self):
        self.setcfg(BASE + "[queue.default.resources]\nslots = 1\n")         # q waits for run's slot
        run = self.add("--need", "slots=1", "--", "sleep", "30")
        self.running(run)
        q = self.add("--need", "slots=1", "--", "true")
        r = self.pp("queue", "hold", f"{run}-{q}")
        self.assertIn(f"held {q}", r.stdout)
        self.assertIn(f"left as they are: {run} running", r.stdout)
        self.pp("queue", "kill", run)

    def test_a_tag_several_jobs_share(self):
        a = self.add("--tag", "exp1", "--", "true")
        self.wait(a)
        b = self.pp("queue", "retry", a).stdout.split()[0]           # a retry keeps the tag
        self.wait(b)
        r = self.pp("queue", "show", "exp1")
        self.assertIn(f"job {b} ", r.stdout)
        self.assertIn(f"'exp1' names jobs {a}, {b}; showing the newest, {b}", r.stderr)
        self.assertIn("note one by its id", self.pp("queue", "note", "exp1", "why", code=2).stderr)
        self.assertIn(f"done {a}", self.pp("queue", "wait", "exp1").stdout)           # wait takes the whole group
        self.assertIn("reads as a job id or a range", self.pp("queue", "add", "--tag", "12-15", "--", "true", code=2).stderr)

    def test_q_narrows_a_tag_that_spans_queues(self):
        a = self.add("--hold", "--tag", "same", "--", "true")
        b = self.add("-q", "dl", "--hold", "--tag", "same", "--", "true")
        self.assertIn("use ids, or -q", self.pp("queue", "drop", "same", "-n", code=2).stderr)
        r = self.pp("queue", "-q", "dl", "drop", "same", "-y")
        self.assertIn(f"dropped {b}", r.stdout)
        self.assertEqual(self.state(a), "held")

    def test_clean_takes_a_selection_as_well_as_an_age(self):
        """`clean` takes the same five forms as every other verb that takes jobs, beside `--older AGE`: it
        removes the ended jobs the selection named, leaves the ones it does not suit as they are, and refuses
        a job named by its own id that has not ended."""
        a, b = self.add("--tag", "c3_a", "--", "true"), self.add("--tag", "c3_b", "--", "true")
        live = self.add("--tag", "c3_live", "--", "sleep", "30")
        self.wait(a, b)
        self.until(lambda: self.state(live) == "running", what=f"job {live} running")
        r = self.pp("queue", "clean", "--prefix", "c3_", "-n")
        self.assertIn("2 ended job record(s)", r.stdout)
        self.assertIn(f"job {live} is running", self.pp("queue", "clean", live, code=2).stderr)
        self.pp("queue", "clean", "--prefix", "c3_", "-y")
        for jid in (a, b):
            self.pp("queue", "show", jid, code=2)
        self.assertEqual(self.state(live), "running")
        self.pp("queue", "kill", live)

    def test_release_over_a_selection_starts_one_worker(self):
        ids = [self.add("--hold", "--tag", "batch3", "--", "true") for _ in range(3)]
        r = self.pp("queue", "release", "batch3")
        self.assertEqual(r.stderr.count("started the default worker"), 1, r.stderr)
        self.wait(*ids)

    def test_wait_takes_a_duration_and_ends_with_its_own_jobs(self):
        j = self.add("--", "true")
        r = self.pp("queue", "wait", j, "--timeout", "1m", "--poll", "0.2")
        self.assertIn(f"done {j}", r.stdout)
        self.assertNotIn("[queue wait]", r.stdout)          # no whole-queue line under a wait for ids
        self.assertIn("duration", self.pp("queue", "wait", j, "--timeout", "soon", code=2).stderr)

    def test_worker_status_is_a_report(self):
        self.add("--no-start", "--", "true")
        js = json.loads(self.pp("queue", "worker", "status", "--json").stdout)
        self.assertEqual([(w["queue"], w["up"]) for w in js], [("default", False)])

    def test_backfill_follows_the_config_while_the_worker_runs(self):
        self.setcfg(BASE + "[resources]\ntok = 1\n")
        a = self.add("--need", "tok=1", "--", "sleep", "30")
        self.running(a)
        b = self.add("--need", "tok=1", "--", "true")              # waits at the head for tok
        c = self.add("--", "true")                                  # fits, but strict order keeps it behind b
        self.until(lambda: "waiting: tok" in self.status())       # a tick found b blocked, and so launched no c
        self.assertEqual(self.state(c), "queued")
        self.setcfg(BASE + "backfill = true\n[resources]\ntok = 1\n")
        self.assertTrue(self.until(lambda: self.state(c) == "done"), self.state(c))
        self.assertEqual(self.state(b), "queued")
        self.pp("queue", "kill", a)


class TestReadme(QueueJourney):
    def test_try_it(self):
        """The README's "Try it", line by line, and what its comments promise."""
        with open(os.path.join(ROOT, "README.md")) as f:
            block = f.read().split("## Try it", 1)[1].split("```", 2)[1]
        lines = [ln.split("  #")[0].strip() for ln in block.splitlines() if ln.startswith("pp ")]
        self.assertEqual(len(lines), 6, lines)
        outs = []
        for line in lines:
            args = shlex.split(line)[1:]
            if args[:2] == ["queue", "wait"]:
                args += ["--poll", "0.2"]
            if args == ["queue"]:       # its comment promises the job's last line of output, so wait for one
                self.until(lambda: "working" in self.pp("queue", code=None).stdout,
                           what="the job's first line of output")
            outs.append(self.pp(*args, code=None))
        self.assertIn("working", outs[1].stdout)        # `pp queue`: running, with that last line
        self.assertIn("done 1 rc=0", outs[2].stdout)
        self.assertEqual(outs[2].returncode, 0)
        self.assertIn("hello", outs[3].stdout)
        self.assertIn("FAILED 2 rc=3", outs[5].stdout)
        # the tail of a failure is the failed attempt's own output, from where that attempt began
        self.assertEqual(outs[5].stdout.count("| trying"), 1, outs[5].stdout)
        self.assertEqual(outs[5].returncode, 1)

# a project's runner, as a container wrapper would be: it reports what it was given, then runs the command
RUNNER = """#!/bin/sh
echo "RUNNER cwd=$(pwd) cuda=${CUDA_VISIBLE_DEVICES-unset} hf=${HF_HUB_OFFLINE-unset} seed=${SEED-unset} job=$PP_QUEUE_JOB"
exec "$@"
"""
PROJECT = ('name = "toyproj"\n[queue]\nrunner = "docker/run.sh"\nstop = "group"\n'
           'env = { HF_HUB_OFFLINE = "1", SEED = "project" }\n')


class TestProjectRunner(QueueJourney):
    """A project's .pp.toml [queue] wraps the jobs added from it, as experiments in a container need."""

    def setUp(self):
        super().setUp()
        self.setcfg(BASE + "[queue.exp]\npoll = 0.2\nidle_exit = 30\n")
        self.proj = self.git_repo("work/toyproj", {".pp.toml": PROJECT, "docker/run.sh": RUNNER, "lab/run.py": ""})

    def exp(self, *args, cwd=None) -> str:
        return self.add("-q", "exp", *args, cwd=cwd or self.proj)

    def project(self, name: str, toml: str, files: dict | None = None) -> str:
        root = os.path.join(self.work, name)
        self.write(os.path.join(root, ".pp.toml"), toml)
        for rel, (text, mode) in (files or {}).items():
            self.write(os.path.join(root, rel), text, mode)
        return root

    def refused(self, root: str, *words: str, code: int = 2):
        r = self.pp("queue", "add", "--", "echo", "x", cwd=root, code=code)
        self.assertNotIn("Traceback", r.stderr)
        for w in words:
            self.assertIn(w, r.stderr)

    def test_queue_from_the_root_and_a_subdirectory_then_read_the_results(self):
        sub = os.path.join(self.proj, "lab")
        a = self.exp("--tag", "root-run", "--", "echo", "from-root")
        r = self.pp("queue", "add", "-q", "exp", "--env", "SEED=job", "--", "sh", "-c", "echo from-sub", cwd=sub)
        b = r.stdout.strip()
        self.assertIn(f"queued {b} (exp) [toyproj]", r.stderr)
        self.assertIn("(via run.sh)", r.stderr)
        self.wait(a, b)
        log = self.pp("queue", "log", b).stdout
        self.assertIn(f"RUNNER cwd={sub} cuda=unset hf=1 seed=job job={b}", log)    # --env beats the project's env
        self.assertIn("from-sub", log)
        self.assertIn(f"cwd={self.proj} ", self.pp("queue", "log", a).stdout)
        job = self.show(b)
        self.assertEqual((job["project"], job["queue"]), ("toyproj", "exp"))
        self.assertEqual(job["runner"], [os.path.join(self.proj, "docker/run.sh")])
        self.assertIn("root-run", self.status("-q", "exp"))

    def test_add_echoes_the_command_not_just_the_runner(self):
        deep = os.path.join(self.work, "a-rather-long-directory-name-for-a-project", "with-a-nested-checkout")
        self.git_repo(deep, {".pp.toml": PROJECT, "docker/run.sh": RUNNER})
        r = self.pp("queue", "add", "--no-start", "--", "python", "-m", "lab.run", "--seed", "7", cwd=deep)
        self.assertIn("python -m lab.run --seed 7", r.stderr)

    def test_a_directory_without_pp_toml_runs_the_bare_command(self):
        plain = os.path.join(self.work, "scratch")
        os.makedirs(plain)
        j = self.add("--", "sh", "-c", "echo bare; pwd", cwd=plain)
        self.wait(j)
        log = self.pp("queue", "log", j).stdout
        self.assertNotIn("RUNNER", log)
        self.assertIn(f"bare\n{plain}", log)

    def test_no_runner_skips_the_wrapper_for_one_job(self):
        j = self.exp("--no-runner", "--", "sh", "-c", "echo unwrapped hf=$HF_HUB_OFFLINE")
        self.wait(j)
        log = self.pp("queue", "log", j).stdout
        self.assertNotIn("RUNNER", log)
        self.assertIn("unwrapped hf=1", log)            # the project's env still applies
        # --json names the variables a job carries and gives no value: a secret goes in with --env
        self.assertEqual(self.show(j)["env"], {"HF_HUB_OFFLINE": "***", "SEED": "***"})

    def test_each_broken_project_says_what_to_fix(self):
        missing = self.project("missing", '[queue]\nrunner = "docker/run.sh"\nstop = "group"\n')
        self.refused(missing, "does not exist", "--no-runner")
        noexec = self.project("noexec", '[queue]\nrunner = "docker/run.sh"\nstop = "group"\n',
                              {"docker/run.sh": (RUNNER, 0o644)})
        self.refused(noexec, "not executable", "chmod +x")
        self.refused(self.project("nopath", '[queue]\nrunner = "no-such-wrapper --gpus all"\nstop = "group"\n'),
                     "not on PATH")
        # a project whose work runs through a runner says how a stop reaches it, or no job of it is queued
        self.refused(self.project("nostop", '[queue]\nrunner = "docker/run.sh"\n',
                                  {"docker/run.sh": (RUNNER, 0o755)}), "it needs a stop")
        self.refused(self.project("badtype", "[queue]\nrunner = 3\n"), "runner", "a command line", code=1)
        self.refused(self.project("badenv", '[queue]\nenv = "HF=1"\n'), "env", "a table", code=1)
        self.refused(self.project("notable", 'queue = "docker/run.sh"\n'), "queue must be a table", code=1)
        self.refused(self.project("typo", '[queue]\nrunnr = "docker/run.sh"\n'), "unknown runnr", code=1)
        self.refused(self.project("badtoml", "[queue\n"), ".pp.toml", code=1)
        self.assertEqual(json.loads(self.status("--json"))["queued"], [])    # nothing got queued
        j = self.pp("queue", "add", "--no-runner", "--", "echo", "anyway", cwd=missing).stdout.strip()
        self.wait(j)

    def test_one_job_says_its_own_runner_stop_and_log(self):
        """`--runner`, `--stop` and `--log` are the flags of three rows whose key is a project's own, so one
        job of its own needs no .pp.toml: what each comes to is on the job's record. A flag wins over the
        project's key for that one job, a job that skips the runner carries the group whatever the project
        says, and a log path with another placeholder in it, or with `..`, is refused."""
        plain = os.path.join(self.work, "scratch")
        self.write("work/scratch/run.sh", RUNNER, 0o755)
        jid = self.add("-q", "exp", "--no-start", "--tag", "solo", "--runner", "./run.sh",
                       "--stop", "echo stopped", "--log", "logs/{tag}.log", "--", "true", cwd=plain)
        j = self.show(jid)
        self.assertEqual(j["runner"], [os.path.join(plain, "run.sh")])
        self.assertEqual(j["stop_cmd"], "echo stopped")
        self.assertEqual(j["log"], os.path.join(plain, "logs/solo.log"))
        cont = self.project("container",
                            '[queue]\nrunner = "./run.sh"\nstop = "echo project"\nlog = "{id}.log"\n',
                            {"run.sh": (RUNNER, 0o755)})
        kept = self.show(self.add("-q", "exp", "--no-start", "--", "true", cwd=cont))
        self.assertEqual((kept["stop_cmd"], kept["log"]), ("echo project", f"{cont}/{kept['id']}.log"))
        mine = self.add("-q", "exp", "--no-start", "--stop", "echo mine", "--", "true", cwd=cont)
        self.assertEqual(self.show(mine)["stop_cmd"], "echo mine")       # the flag, over the project's key
        skipped = self.show(self.add("-q", "exp", "--no-start", "--no-runner", "--", "true", cwd=cont))
        self.assertEqual((skipped["runner"], skipped["stop_cmd"]), ([], "group"))
        for bad in ("logs/{queue}.log", "../out.log"):
            r = self.pp("queue", "add", "-q", "exp", "--no-start", "--log", bad, "--", "true", code=2)
            self.assertIn("--log", r.stderr)

    def test_a_runner_list_and_a_runner_on_path_work(self):
        root = self.project("listy", '[queue]\nrunner = ["env", "LISTY=1"]\nstop = "group"\n')
        j = self.add("--", "sh", "-c", "echo listy=$LISTY", cwd=root)
        self.wait(j)
        self.assertIn("listy=1", self.pp("queue", "log", j).stdout)

    def test_the_reserved_card_is_handed_to_the_runner(self):
        self.gpus((24, 2), (24, 20))                          # card 1 is nearly full: someone else uses it
        a = self.exp("--gpu-gb", "20", "--", "echo", "a-ran")
        self.wait(a)
        self.assertIn("cuda=0", self.pp("queue", "log", a).stdout)



# ---------------------------------------------------------------------------------------------------------------
# The journey of a lab: one machine, one card shared with another tenant, two projects, work that runs in a
# container, campaigns of tens of jobs in families, lanes, and an agent session operating it between runs. One
# test per step, each typing what `pp help queue` prints, in the order the help prints it.
# ---------------------------------------------------------------------------------------------------------------
import shutil  # noqa: E402
import signal  # noqa: E402

from journey import kill_pid, needs  # noqa: E402

from pp.lib import proc  # noqa: E402

# The machine, described once: the card pool every queue draws from, a queue of three slots, and a lane of one.
MACHINE = ('[resources]\ngpu_gb = "auto"\n'
           '[queue.default]\npoll = 0.2\nidle_exit = 30\n'
           '[queue.lab]\npoll = 0.2\nidle_exit = 30\n'
           '[queue.lab.resources]\nslots = 3\n'
           '[queue.r1]\npoll = 0.2\nidle_exit = 30\n'
           '[queue.r1.resources]\nslots = 1\n')

# A machine that may run a journey under a real runtime says so by naming the image to run it in: a real
# container is handed a real card and a mount of a real directory.
REAL_IMAGE = os.environ.get("PP_JOURNEY_IMAGE")

# The headings the help contract names, each a line of its own.
HELP_SECTIONS = ("A first project whose work runs in a container, in four steps", "Naming jobs",
                 "Selecting jobs", "Order, and a stage after another", "Campaign files",
                 "Containers, and anything else a runner starts elsewhere",
                 "Settings: one default, one key, one flag, one order", "Where a job's output goes",
                 "Timeouts and retries", "Hooks, on the FINAL outcome of a job", "Workers, and after a reboot",
                 "Agents", "What pp queue leaves to you, on purpose")

# Every field `show --json` promises, off one record. A reader ignores a field it does not know, so a field
# beside these is no failure.
PROMISED = ("id", "state", "queue", "tag", "project", "cwd", "cmd", "runner", "needs", "prio", "owner",
            "timeout", "grace", "retries", "attempt", "rc", "reason", "not_retried", "added", "started",
            "ended", "seconds", "log", "log_at", "container", "envfile", "rcfile", "cards", "history", "notes",
            "reported", "hooks", "env", "shell_env", "watchers")

# A runner of the shape a container runner has, and the fault a stop command closes: the work it starts is in a
# session of its own, outside the job's process group, so no signal pp sends reaches it, and the runner itself
# ignores SIGTERM the way a container's first process with no handler does. The work is recorded under the name
# in PP_QUEUE_CONTAINER, which is the only way to reach it. Every expansion is guarded, so a caller that sets no
# PP_QUEUE_ variable runs the same script under `set -u`.
FAKE_RUNNER = """#!/bin/bash
set -uo pipefail
trap '' TERM
ROOT=$(cd "$(dirname "$0")/.." && pwd)
GPUS=${CUDA_VISIBLE_DEVICES:+device=$CUDA_VISIBLE_DEVICES}
NAME=${PP_QUEUE_CONTAINER:-plain}
CELLS=$ROOT/cells
mkdir -p "$CELLS"
{ echo "root=$ROOT"; echo "cwd=$PWD"; echo "gpus=${GPUS:-all}"; echo "job=${PP_QUEUE_JOB:-unset}"
  echo "envfile=${PP_QUEUE_ENVFILE:-unset}"; } > "$CELLS/$NAME.run.part"
mv "$CELLS/$NAME.run.part" "$CELLS/$NAME.run"
if [ -n "${PP_QUEUE_ENVFILE:-}" ]; then
  cp "$PP_QUEUE_ENVFILE" "$CELLS/$NAME.env.part" && mv "$CELLS/$NAME.env.part" "$CELLS/$NAME.env"
fi
cd "$ROOT"      # the work runs at the project root, as -v "$ROOT:/work" -w /work makes it in a container
setsid "$ROOT/docker/cell.sh" "$CELLS/$NAME.pid" "$@" &
wait $!
"""

# The same runner with a real runtime, which is the same journey: the two guarded arguments, the card request
# narrowed rather than added to, the project root worked out from the script's own place, and no -e line for a
# variable the environment file carries.
REAL_RUNNER = """#!/bin/bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
GPUS=${CUDA_VISIBLE_DEVICES:+device=$CUDA_VISIBLE_DEVICES}
NAME=${PP_QUEUE_CONTAINER:-plain}
CELLS=$ROOT/cells
mkdir -p "$CELLS"
{ echo "root=$ROOT"; echo "cwd=$PWD"; echo "gpus=${GPUS:-all}"; echo "job=${PP_QUEUE_JOB:-unset}"
  echo "envfile=${PP_QUEUE_ENVFILE:-unset}"; } > "$CELLS/$NAME.run.part"
mv "$CELLS/$NAME.run.part" "$CELLS/$NAME.run"
if [ -n "${PP_QUEUE_ENVFILE:-}" ]; then
  cp "$PP_QUEUE_ENVFILE" "$CELLS/$NAME.env.part" && mv "$CELLS/$NAME.env.part" "$CELLS/$NAME.env"
fi
exec docker run --rm \\
  ${PP_QUEUE_CONTAINER:+--name "$PP_QUEUE_CONTAINER"} \\
  ${PP_QUEUE_ENVFILE:+--env-file "$PP_QUEUE_ENVFILE"} \\
  --gpus "${GPUS:-all}" ${CUDA_VISIBLE_DEVICES:+-e CUDA_VISIBLE_DEVICES=0} \\
  -v "$ROOT:/work" -w /work "${PP_IMAGE:-busybox}" "$@"
"""

# One cell of work, in a session of its own, as a container's work runs in a namespace of its own. It records
# the session's pid under the container's name in one step, and then becomes the work.
CELL = """#!/bin/sh
set -u
pidfile=$1
shift
printf '%s\\n' "$$" > "$pidfile.part"
mv "$pidfile.part" "$pidfile"
exec "$@"
"""

# The project's stop command: it ends the work recorded under one name and exits with the code it is given. A
# name is the key, and the pid it recorded is what is signalled, so nothing is ever matched by a substring of a
# command line.
STOP = """#!/bin/sh
set -u
rc=0
real=0
while :; do
  case ${1:-} in
    --rc) rc=$2; shift 2 ;;
    --real) real=1; shift ;;
    *) break ;;
  esac
done
name=${1:-}
cells=$(cd "$(dirname "$0")/.." && pwd)/cells
mkdir -p "$cells"
printf '%s %s\\n' "$name" "${PP_QUEUE_CONTAINER:-unset}" >> "$cells/stopped"
if [ "$rc" = 0 ]; then
  if [ "$real" = 1 ]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
  else
    pid=$(cat "$cells/$name.pid" 2>/dev/null) || pid=
    if [ -n "$pid" ]; then kill -9 "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true; fi
  fi
fi
exit "$rc"
"""

# The lab's own work: one cell; a cell that runs until a file appears; a cell that fails saying the words a
# retry filter looks for, on its first attempt alone; and a cell that holds until a file named after this
# attempt's container appears and then fails, so a test reaches one attempt at a time.
WORK = {
    "lab/run.sh": '#!/bin/sh\necho "cell $*"\n',
    "lab/hold.sh": '#!/bin/sh\nwhile [ ! -f "$1" ]; do sleep 0.05; done\necho "cell held $1"\n',
    "lab/fail.sh": ('#!/bin/sh\nif [ -f "$1" ]; then echo quiet; else echo "CUDA out of memory"; : > "$1"; fi\n'
                    'exit 1\n'),
    "lab/once.sh": ('#!/bin/sh\nset -u\nname=${PP_QUEUE_CONTAINER:-plain}\n'
                    'while [ ! -f "$1/$name.go" ]; do sleep 0.05; done\necho "CUDA out of memory"\nexit 1\n'),
}
HOLD = WORK["lab/hold.sh"]
RUN = WORK["lab/run.sh"]


class Lab(QueueJourney):
    """The lab every step below runs in: the machine of step 2, the project of step 3 and the container of
    step 4.

    The container is a fake one, so a journey needs no container runtime and still meets the fault a stop
    command closes: pp's SIGTERM reaches the wrapper, the work is somewhere else, and only the project's own
    stop command reaches it. `TestTheProjectUnderARealRunner` is the same journey with a real runtime, skipped
    where there is none."""

    RUNNER = FAKE_RUNNER
    STOP_ARGS = ""              # what the stop script needs to reach a fake cell, and a real container

    def setUp(self):
        super().setUp()
        self.setcfg(MACHINE)

    # -- the project, its runner and its stop ----------------------------------------------------------------
    def stop_cmd(self, root: str) -> str:
        return f"{root}/docker/stop.sh {self.STOP_ARGS}$PP_QUEUE_CONTAINER"

    def failing_stop_cmd(self, root: str) -> str:
        return f"{root}/docker/stop.sh --rc 1 {self.STOP_ARGS}$PP_QUEUE_CONTAINER"

    def lab(self, where: str = "lab", *, name: str = "lab", runner: str | None = "docker/run.sh",
            stop: str | None = "ok", log: str | None = "logs/{tag}.log",
            env: str | None = '{ OFFLINE = "1", PKGS = "/work/pkgs" }') -> str:
        """A project root: its .pp.toml, its runner, its stop command and its work. -> the root."""
        root = os.path.join(self.work, where)
        self.write(os.path.join(root, "docker/run.sh"), self.RUNNER, 0o755)
        self.write(os.path.join(root, "docker/cell.sh"), CELL, 0o755)
        self.write(os.path.join(root, "docker/stop.sh"), STOP, 0o755)
        for rel, text in WORK.items():
            self.write(os.path.join(root, rel), text, 0o755)
        cmd = {"ok": self.stop_cmd(root), "fail": self.failing_stop_cmd(root)}.get(stop, stop)
        lines = [f'name = "{name}"', "[queue]"]
        lines += [f'{k} = "{v}"' for k, v in (("runner", runner), ("stop", cmd), ("log", log)) if v]
        if env:
            lines.append(f"env = {env}")
        self.write(os.path.join(root, ".pp.toml"), "\n".join(lines) + "\n")
        return root

    # -- reading what the runner and the stop command were given ---------------------------------------------
    def cell_pid(self, root: str, name: str) -> int | None:
        """The pid of the session the runner started under that name, or None where it has not started yet."""
        try:
            return int(self.read(os.path.join(root, "cells", f"{name}.pid")).split()[0])
        except (OSError, IndexError, ValueError):
            return None

    def cell_up(self, root: str, name: str) -> bool:
        """Whether the work started under that name is still running."""
        pid = self.cell_pid(root, name)
        return pid is not None and proc.alive(pid)

    def started_cell(self, root: str, jid: str) -> str:
        """Wait for the job to run and its work to be up, bounded. -> the attempt's container name."""
        self.until(lambda: self.state(jid) == "running", what=f"job {jid} running")
        name = self.field(self.show(jid), "container")
        self.until(lambda: self.cell_up(root, name), what=f"the work under {name}")
        return name

    def asked_to_stop(self, root: str) -> list:
        """One entry per run of the project's stop command: the name it was given, and the name in its own
        environment."""
        path = os.path.join(root, "cells", "stopped")
        return [ln.split() for ln in self.read(path).splitlines()] if os.path.exists(path) else []

    def run_file(self, root: str, name: str) -> dict:
        """What the runner was given, as the runner recorded it."""
        return self.kv(os.path.join(root, "cells", f"{name}.run"))

    def end_cell(self, root: str, name: str) -> None:
        """End by hand the work left under that name, which is the one command `show` prints for the case where
        everything else failed."""
        self.sh(f"{root}/docker/stop.sh {self.STOP_ARGS}{shlex.quote(name)}")

    @staticmethod
    def kv(path: str) -> dict:
        """A file of KEY=VALUE lines, as a map."""
        with open(path) as f:
            return dict(ln.split("=", 1) for ln in f.read().splitlines() if "=" in ln)

    # -- reading a record, a queue and a worker --------------------------------------------------------------
    def field(self, job: dict, name: str):
        """One field of a record, with a failure naming the field where the record does not carry it."""
        self.assertIn(name, job, sorted(job))
        return job[name]

    def stop_of(self, job: dict) -> str | None:
        """The stop command the job carries, from the field of its own the record keeps it in, beside the
        runner it was resolved with. `stop` is the stop the job was asked for, and carries that command's own
        exit code where it ran."""
        return self.field(job, "stop_cmd")

    def ended(self, jid) -> bool:
        return self.state(jid) in ("done", "failed")

    def all_ids(self) -> list:
        """Every job the machine holds, in every state."""
        d = json.loads(self.status("--json", "-n", "500"))
        return sorted(j["id"] for k in ("running", "queued", "held", "recent") for j in d[k])

    def worker(self, q: str = "default") -> dict:
        for w in json.loads(self.pp("queue", "worker", "status", "--json").stdout):
            if w["queue"] == q:
                return w
        return {"queue": q, "up": False, "pid": None}


class TestTheFirstLook(Lab):
    """Steps 0 to 2: read one page, install and look, and describe the machine once."""

    @unittest.expectedFailure
    def test_the_help_is_one_page_and_names_every_section(self):
        """`pp help queue` is the page this journey follows. Its three spellings each exit 0. The page holds
        every section heading the contract names, each a line of its own. Every line fits in 118 columns."""
        page = self.pp("help", "queue").stdout
        self.pp("-h", "queue")
        self.pp("--help", "queue", "add")
        heads = {ln.rstrip() for ln in page.splitlines() if ln[:1] not in (" ", "\t", "")}
        self.assertEqual([], [h for h in HELP_SECTIONS if h not in heads])
        self.assertEqual([], [n for n, ln in enumerate(page.splitlines(), 1) if len(ln) > 118])

    def test_a_fresh_machine_has_no_queues_and_still_says_what_a_card_holds(self):
        """A fresh machine has no queue and no configuration of its own. `pp queue` exits 0 there and names no
        queue. The card it prints counts the memory another tenant holds."""
        os.remove(f"{self.env['PP_CONFIG_DIR']}/config.toml")       # nothing is configured yet
        self.gpus((24, 4))                              # another tenant holds 4 GB of this card's 24
        self.pp("--version")
        d = json.loads(self.pp("queue", "--json").stdout)
        self.assertEqual(d["queues"], [])
        self.assertEqual([(c["index"], c["total_gb"], c["used_gb"], c["free_gb"]) for c in d["cards"]],
                         [(0, 24.0, 4.0, 20.0)])
        self.pp("queue")

    def test_the_machine_and_its_lanes_are_described_in_one_file(self):
        """One config file gives each queue its own pools. A lane is a queue of one slot. A job declaring more
        GPU memory than a pool provides is refused when it is added, and nothing is queued for it."""
        self.gpus((24, 4))
        self.add("-q", "lab", "--hold", "--no-start", "--", "true")
        self.add("-q", "r1", "--hold", "--no-start", "--", "true")
        pools = {q: {p["name"]: p["max"] for p in ps}
                 for q, ps in json.loads(self.status("--json"))["pools"].items()}
        self.assertEqual(pools["lab"]["slots"], 3)
        self.assertEqual(pools["r1"]["slots"], 1)
        before = self.all_ids()
        self.pp("queue", "add", "-q", "lab", "--gpu-gb", "200", "--no-start", "--", "true", code=2)
        self.assertEqual(self.all_ids(), before)


class TestTheProject(Lab):
    """Steps 3 to 6: a project declares how its work is launched and stopped, its runner keeps working for its
    other callers, a kill and a timeout both end what the runner started, and one job by hand prints its id."""

    def test_a_project_declares_its_runner_stop_log_and_env_once(self):
        """A job added from the project carries its runner, its stop command, its log path and its environment
        on the record. A project with a runner and no stop refuses the add. A job with no tag is queued under a
        {tag} pattern filled in with its id."""
        root = self.lab()
        jid = self.add("-q", "lab", "--tag", "c3_recall.s0", "--no-start", "--", "./lab/run.sh", cwd=root)
        j = self.show(jid)
        self.assertEqual(self.field(j, "runner"), [f"{root}/docker/run.sh"])
        self.assertEqual(self.stop_of(j), self.stop_cmd(root))
        self.assertEqual(self.field(j, "log"), f"{root}/logs/c3_recall.s0.log")
        self.assertEqual(self.field(j, "env"), {"OFFLINE": "***", "PKGS": "***"})
        nostop = self.lab("nostop", stop=None, log=None, env=None)
        self.pp("queue", "add", "-q", "lab", "--no-start", "--", "./lab/run.sh", cwd=nostop, code=2)
        plain = self.add("-q", "lab", "--no-start", "--", "./lab/run.sh", cwd=root)
        p = self.show(plain)
        self.assertEqual((p["state"], self.field(p, "log")), ("queued", f"{root}/logs/{plain}.log"))

    def test_the_runner_runs_the_same_for_a_caller_that_is_not_pp(self):
        """The runner's other callers set no PP_QUEUE variable, and the script runs for them under `set -u`.
        Under pp the environment file the runner is handed holds the project's env, the job's own env and the
        card it reserved, and none of the four host variables that would name a python outside the container.
        It is readable by its owner alone. A job queued from a subdirectory gets the project root. One card
        reserved is one card asked for, so the runner narrowed its request instead of adding a second one."""
        root = self.lab()
        self.sh(f"{root}/docker/run.sh ./lab/run.sh --cell c3", cwd=root)
        self.assertEqual(self.run_file(root, "plain")["gpus"], "all")        # no card, so no card is asked for
        self.gpus((24, 4))
        sub = os.path.join(root, "lab")
        jid = self.add("-q", "lab", "--tag", "c3_recall.s0", "--gpu-gb", "10", "--env", "SEED=0",
                       "--", "./lab/hold.sh", f"{self.work}/go", cwd=sub)
        name = self.started_cell(root, jid)
        j = self.show(jid)
        envfile = self.field(j, "envfile")
        self.assertEqual(os.stat(envfile).st_mode & 0o777, 0o600)
        env = self.kv(envfile)
        self.assertEqual((env.get("OFFLINE"), env.get("PKGS"), env.get("SEED")), ("1", "/work/pkgs", "0"))
        self.assertEqual(env.get("CUDA_VISIBLE_DEVICES"), "0")
        self.assertEqual((env.get("PP_QUEUE"), env.get("PP_QUEUE_JOB"), env.get("PP_QUEUE_CONTAINER")),
                         ("lab", jid, name))
        self.assertEqual([k for k in ("PATH", "VIRTUAL_ENV", "CONDA_PREFIX", "CONDA_DEFAULT_ENV",
                                      "PP_QUEUE_RCFILE", "PP_QUEUE_ENVFILE") if k in env], [])
        run = self.run_file(root, name)
        self.assertEqual((run["root"], run["cwd"], run["envfile"]), (root, sub, envfile))
        self.assertEqual(run["gpus"], "device=0")
        self.assertEqual(self.field(j, "cards"), [0])
        self.write("work/go", "")
        self.wait(jid)

    def test_a_kill_and_a_timeout_both_end_what_the_runner_started(self):
        """A kill runs the project's stop command once, with this attempt's container name in its environment,
        and what the runner started is gone. A timeout does the same. The stop command's exit code is on the
        record. A stop command that exits 1 leaves the kill at exit 0, and the record still names the
        container. A job that skips the runner has its work in its own group, so its stop is the group: SIGTERM
        first, and SIGKILL once the grace is up."""
        root = self.lab()
        jid = self.add("-q", "lab", "--tag", "stop_probe", "--", "sleep", "600", cwd=root)
        name = self.started_cell(root, jid)
        self.pp("queue", "kill", jid)
        self.until(lambda: not self.cell_up(root, name), what="the work the runner started to be gone")
        self.until(lambda: self.ended(jid), what=f"job {jid} ended")
        j = self.show(jid)
        self.assertEqual(self.asked_to_stop(root), [[name, name]])
        self.assertEqual((j["state"], self.field(j, "stop").get("rc")), ("failed", 0))
        self.assertIsNone(self.field(j, "stop").get("kill9"))       # it ended inside its grace
        self.assertEqual(self.field(j, "container"), name)

        timed = self.add("-q", "lab", "--tag", "stop_probe", "--timeout", "2s", "--", "sleep", "600", cwd=root)
        tname = self.started_cell(root, timed)
        self.until(lambda: not self.cell_up(root, tname), timeout=25,
                   what="the work a timeout's stop reached to be gone")
        self.until(lambda: self.ended(timed), what=f"job {timed} ended")
        t = self.show(timed)
        self.assertEqual([e[0] for e in self.asked_to_stop(root)], [name, tname])
        self.assertEqual((t["state"], self.field(t, "stop").get("rc")), ("failed", 0))
        self.assertIn("timeout", self.field(t, "stop"))             # a timeout's stop, not a kill's

        unhappy = self.lab("unhappy", stop="fail")
        bad = self.add("-q", "lab", "--tag", "stop_probe", "--", "sleep", "600", cwd=unhappy)
        badname = self.started_cell(unhappy, bad)
        self.pp("queue", "kill", bad, "--grace", "1")
        self.until(lambda: self.ended(bad), timeout=25, what=f"job {bad} ended")
        b = self.show(bad)
        self.assertEqual(self.field(b, "stop").get("rc"), 1)
        self.assertEqual(self.field(b, "container"), badname)       # so one command by hand is always printed
        self.end_cell(unhappy, badname)
        self.until(lambda: not self.cell_up(unhappy, badname), what="the work a failed stop left")

        group = self.add("-q", "lab", "--no-runner", "--tag", "stop_probe", "--",
                         "sh", "-c", 'trap "" TERM; while :; do sleep 0.2; done', cwd=root)
        self.until(lambda: self.state(group) == "running", what=f"job {group} running")
        held = self.show(group)["pgid"]                             # what the job's own group holds
        asks = len(self.asked_to_stop(root))
        self.pp("queue", "kill", group, "--grace", "1")
        self.until(lambda: self.ended(group), timeout=25, what=f"job {group} ended")
        g = self.show(group)
        self.assertEqual(self.stop_of(g), "group")
        self.assertIsNotNone(self.field(g, "stop").get("sent"))     # the SIGTERM went to that group
        self.assertEqual(g["reason"], "killed")
        self.assertEqual(len(self.asked_to_stop(root)), asks)       # and no command ran for a job of the group
        # the command held the signal back, so what took it was the SIGKILL of its group once its run was over
        self.until(lambda: not proc.alive(held), what="the group of a command that ignored the SIGTERM")

    def test_one_job_by_hand_prints_its_id_on_stdout(self):
        """`add` prints one id on stdout and nothing else there, so a driver reads the id from it. The job's
        whole output goes to the path the project's pattern names, and the record names that path."""
        root = self.lab()
        self.gpus((80, 4))
        r = self.pp("queue", "add", "-q", "lab", "--tag", "c3_recall.s0", "--gpu-gb", "36",
                    "--", "./lab/run.sh", "--cell", "c3", "--seed", "0", cwd=root, env=self.as_agent())
        jid = r.stdout.strip()
        self.assertEqual(r.stdout.split(), [jid])
        self.assertTrue(jid.isdigit(), r.stdout)
        self.wait(jid)
        j = self.show(jid)
        self.assertEqual(self.field(j, "log"), f"{root}/logs/c3_recall.s0.log")
        self.assertTrue(os.path.exists(j["log"]), j["log"])


@needs("queue")
@unittest.skipUnless(REAL_IMAGE and shutil.which("docker"), "no container runtime set aside for a journey")
class TestTheProjectUnderARealRunner(TestTheProject):
    """Steps 3 to 6 again, with a runner that starts a real container.

    It is the same journey: the same tests, over a runner whose work is in a container instead of a session of
    its own. A real container is handed a real card and a mount of the project root, so this needs a runtime on
    PATH and a machine that set an image aside for it (PP_JOURNEY_IMAGE); the stories keep the machine's own
    cards and directories out of a journey otherwise."""

    RUNNER = REAL_RUNNER
    STOP_ARGS = "--real "

    def setUp(self):
        super().setUp()
        self.env["PP_IMAGE"] = REAL_IMAGE               # what the runner's `docker run` line runs

    def cell_up(self, root: str, name: str) -> bool:
        """Whether the container of that name is running, which the runtime answers by name."""
        said = self.sh("docker inspect -f '{{.State.Running}}' " + shlex.quote(name), check=False)
        return said.strip() == "true"


class TestTheCampaign(Lab):
    """Steps 7 and 8: a grid through a campaign file, and a stage that waits for the stage before it."""

    @unittest.expectedFailure
    def test_a_campaign_file_queues_every_line_or_none(self):
        """A campaign file queues one job per line, every line carrying its own flags. The ids come out on
        stdout in order, one per line. A flag on the `add` is the default for every line and a line's own flag
        wins over it. A line holding an unquoted shell operator is refused and nothing is queued. A quoted pipe
        is one word of the command. Blank lines and lines beginning with # are skipped."""
        self.gpus((80, 4))
        self.write("work/run.sh", RUN, 0o755)
        plan = ["# the cells this campaign covers", ""]
        plan += [f"--tag s3_L{n:02d} --gpu-gb 2 -- ./run.sh --layer {n}" for n in range(24)]
        plan.append("--tag s3_eval --gpu-gb 12 --prio 0 -- ./run.sh --eval")
        plan.append("-- sh -c 'echo a | grep a'")
        ids = self.pp("queue", "add", "-q", "lab", "--from", "-", "--prio", "1", "--no-start",
                      input="\n".join(plan) + "\n").stdout.split()
        self.assertEqual(len(ids), 26)
        self.assertEqual([int(i) for i in ids], sorted(int(i) for i in ids))
        first, graded, piped = self.show(ids[0]), self.show(ids[24]), self.show(ids[25])
        self.assertEqual((first["tag"], first["prio"], first["needs"].get("gpu_gb")), ("s3_L00", 1, 2))
        self.assertEqual((graded["tag"], graded["prio"]), ("s3_eval", 0))
        self.assertEqual(piped["cmd"], ["sh", "-c", "echo a | grep a"])
        self.assertIsNone(piped["tag"])                          # a job added without --tag has no name
        before = self.all_ids()
        bad = list(plan)
        bad[13] = "-- ./run.sh --layer 11 > out.txt"
        self.pp("queue", "add", "-q", "lab", "--from", "-", "--no-start", input="\n".join(bad) + "\n", code=2)
        self.assertEqual(self.all_ids(), before)

    @unittest.expectedFailure
    def test_a_stage_waits_for_the_stage_before_it(self):
        """A job added with --after is queued until every job that selection named has ended. It is skipped
        rather than waited for, so it never becomes the head of its queue and a job behind it still starts. A
        dropped job counts as ended. The ids are read across every queue. An id no job has is refused, and
        nothing is queued for it."""
        self.write("work/hold.sh", HOLD, 0o755)
        one = self.pp("queue", "add", "-q", "lab", "--from", "-",
                      input="".join(f"./hold.sh go{n}\n" for n in range(1, 5))).stdout.split()
        self.assertEqual(len(one), 4)
        two = self.pp("queue", "add", "-q", "r1", "--from", "-", "--prio", "1", "--after", ",".join(one),
                      input="./hold.sh go5\n./hold.sh go6\n").stdout.split()
        for t in two:
            j = self.show(t)
            self.assertEqual(j["state"], "queued")
            self.assertEqual(self.field(j, "after"), [int(i) for i in one])
        self.until(lambda: self.state(one[0]) == "running", what=f"job {one[0]} running")
        self.assertEqual([self.state(t) for t in two], ["queued", "queued"])
        late = self.add("-q", "r1", "--prio", "5", "--", "true")          # one slot, and a worse priority
        self.wait(late)
        self.assertEqual([self.state(t) for t in two], ["queued", "queued"])
        parked = self.until(lambda: next((j for j in one if self.state(j) == "queued"), None),
                            what="the fourth job of stage one waiting for a slot")
        self.pp("queue", "drop", parked, "-y")                            # a dropped job counts as ended
        for n in range(1, 7):
            self.write(f"work/go{n}", "")
        self.wait(*[j for j in one if j != parked])
        self.wait(*two)
        before = self.all_ids()
        self.pp("queue", "add", "-q", "lab", "--after", "999999", "--", "true", code=2)
        self.assertEqual(self.all_ids(), before)


class TestTheLanesAndTheFamilies(Lab):
    """Steps 9 and 10: a lane is a single-slot queue that --all names on its own, and a family is steered by
    the start of its name."""

    def test_a_single_slot_queue_is_a_lane_and_all_names_only_it(self):
        """--all with -q names every job of that queue whatever its tag, and no job of another queue. Without
        -q it is a usage error. A hold before a kill keeps the lane from starting anything between the kill and
        the drop. A tag that spans two queues is a usage error."""
        self.write("work/hold.sh", HOLD, 0o755)
        running = self.add("-q", "r1", "--tag", "recall_A10", "--", "./hold.sh", "go")
        self.until(lambda: self.state(running) == "running", what=f"job {running} running")
        parked = self.add("-q", "r1", "--tag", "recall_A11", "--", "true")
        other = self.add("-q", "lab", "--tag", "recall_A10", "--hold", "--", "true")
        self.pp("queue", "hold", "-q", "r1", "--all")
        self.assertEqual(self.state(parked), "held")
        self.assertEqual(self.state(running), "running")
        self.pp("queue", "kill", "-y", "-q", "r1", "--all")
        self.until(lambda: self.ended(running), what=f"job {running} ended")
        self.assertEqual(self.state(parked), "held")             # the hold is what keeps it from starting
        self.pp("queue", "drop", "-y", "-q", "r1", "--all")
        self.pp("queue", "show", parked, code=2)                 # it is gone, with its own files
        self.assertEqual(self.state(other), "held")              # another queue's job is untouched
        self.pp("queue", "hold", "--all", code=2)
        self.pp("queue", "drop", "recall_A10", "-n", code=2)     # a tag over two queues names the ids

    def test_a_family_is_steered_by_prefix(self):
        """--prefix matches from the first character of a tag and is repeatable, and the union of two that
        overlap is taken once. A release over a family that is already queued exits 0 and leaves those jobs as
        they are, so a driver that runs twice is no error the second time. A note takes one job, and the record
        carries it."""
        shift = [self.add("-q", "lab", "--no-start", "--tag", f"c3_shift.s{n}", "--", "true") for n in range(2)]
        family = [self.add("-q", "lab", "--no-start", "--tag", t, "--", "true")
                  for t in ("s3_a", "s3_b", "s3_eval")]
        self.pp("queue", "reprio", "--prefix", "c3_shift", "0")
        self.assertEqual([self.show(j)["prio"] for j in shift], [0, 0])
        self.pp("queue", "hold", "--prefix", "s3_")
        self.assertEqual([self.state(j) for j in family], ["held"] * 3)
        self.pp("queue", "release", "--prefix", "s3_", "--no-start")
        self.pp("queue", "release", "--prefix", "s3_", "--no-start")          # twice: exit 0 both times
        self.assertEqual([self.state(j) for j in family], ["queued"] * 3)
        self.pp("queue", "reprio", "--prefix", "s3_", "--prefix", "s3_e", "7")
        self.assertEqual([self.show(j)["prio"] for j in family], [7, 7, 7])
        self.pp("queue", "note", "s3_a", "formed at 3500; seen 0.954")
        self.assertEqual([n["text"] for n in self.field(self.show(family[0]), "notes")],
                         ["formed at 3500; seen 0.954"])
        self.add("-q", "lab", "--no-start", "--tag", "s3_a", "--", "true")   # a second job of that one name
        self.pp("queue", "note", "s3_a", "which one", code=2)                # a note takes one job
        self.pp("queue", "drop", "--prefix", "c3_", "--prefix", "s3_", "-n")  # only the plan
        self.assertEqual([self.state(j) for j in family], ["queued"] * 3)
        self.pp("queue", "drop", "--prefix", "s3_", "--prefix", "s3_e", "-y")
        for j in family:
            self.pp("queue", "show", j, code=2)


class TestTheRetriesAndTheRecovery(Lab):
    """Steps 11 to 13: the retries are pp's, a worker that goes away is replaced, and a machine that went down
    leaves work the next worker reaps."""

    @unittest.expectedFailure
    def test_a_retry_keeps_the_id_and_reads_only_its_own_attempt(self):
        """An automatic retry keeps the job's id. A retry filter matches the failed attempt's own lines, from
        the offset where that attempt began, so a pattern an earlier attempt matched matches no more. A job
        waiting out its delay never becomes the head of its queue. An attempt's container is gone before the
        next attempt starts."""
        self.write("work/fail.sh", WORK["lab/fail.sh"], 0o755)
        jid = self.add("--retries", "2", "--retry-if", "CUDA out of memory", "--retry-delay", "0.2",
                       "--", "./fail.sh", f"{self.work}/tried")
        self.wait(jid, code=1)
        j = self.show(jid)
        self.assertEqual((j["id"], j["state"]), (int(jid), "failed"))
        self.assertEqual(j["attempt"], 2)                        # the second attempt said nothing to match
        self.assertIsNotNone(self.field(j, "not_retried"))
        self.assertGreater(self.field(j, "log_at"), 0)
        self.assertEqual(self.field(self.field(j, "history")[0], "log_at"), 0)

        slow = self.add("-q", "r1", "--retries", "1", "--retry-delay", "60", "--", "sh", "-c", "exit 1")
        self.until(lambda: self.show(slow)["attempt"] == 2 and self.state(slow) == "queued",
                   what=f"job {slow} waiting out its delay")
        behind = self.add("-q", "r1", "--", "true")              # one slot, and the delay is not its head
        self.wait(behind)
        self.assertEqual(self.state(slow), "queued")
        self.pp("queue", "drop", slow, "-y")

        root = self.lab()
        held = self.add("-q", "lab", "--retries", "1", "--retry-delay", "0.2", "--", "./lab/once.sh",
                        self.work, cwd=root)
        first = self.started_cell(root, held)
        self.write(f"work/{first}.go", "")
        second = self.until(lambda: (self.state(held) == "running"
                                     and (self.show(held).get("container") or first) != first
                                     and self.show(held)["container"]),
                            what="the attempt after the one that failed")
        self.assertFalse(self.cell_up(root, first))              # gone before the next attempt starts
        self.write(f"work/{second}.go", "")
        self.wait(held, code=1)
        self.assertEqual(self.field(self.show(held), "container"), second)

    def test_the_next_worker_adopts_a_running_job(self):
        """A job keeps running when its worker stops, and nothing records how it ends until a worker is back.
        The next worker adopts it under the pid the job's claim names. The exit code the job wrote is its own,
        and so is its duration. One worker of a queue runs, so its limits are never doubled. A worker that was
        killed leaves its claim free at once. A kill after the restart runs the same stop command on the same
        container."""
        jid = self.add("--", "sh", "-c", f"while [ ! -f {self.work}/go ]; do sleep 0.05; done; exit 7")
        self.until(lambda: self.state(jid) == "running", what=f"job {jid} running")
        pid = self.field(self.show(jid), "pid")
        self.pp("queue", "worker", "stop")
        self.until(lambda: not self.worker()["up"], what="the worker to be down")
        self.assertEqual(self.state(jid), "running")
        self.assertIsNone(self.show(jid).get("ended"))           # nothing is recording how it ends
        self.pp("queue", "worker", "start")
        self.until(lambda: self.worker()["up"], what="a worker to be up")
        self.assertEqual(self.field(self.show(jid), "pid"), pid)      # adopted, and not started again
        up = self.worker()["pid"]
        self.pp("queue", "work", "-q", "default")                # a second worker of one queue: it holds none
        self.assertEqual(self.worker()["pid"], up)
        kill_pid(up)                                             # killed outright, so its claim is free at once
        self.until(lambda: not self.worker()["up"], what="the killed worker's claim to be free")
        self.write("work/go", "")
        self.until(lambda: os.path.exists(self.field(self.show(jid), "rcfile")),
                   what="the exit code the job wrote itself")
        self.assertEqual(self.state(jid), "running")             # its own last act, with nothing watching
        self.wait(jid, code=1)                                   # a wait starts a worker, which reaps it
        j = self.show(jid)
        self.assertEqual((j["state"], j["rc"]), ("failed", 7))
        self.assertAlmostEqual(j["seconds"], j["ended"] - j["started"], places=3)

        root = self.lab()
        c = self.add("-q", "lab", "--", "sleep", "600", cwd=root)
        name = self.started_cell(root, c)
        kill_pid(self.until(lambda: self.worker("lab")["pid"], what="the lab worker's pid"))
        self.until(lambda: not self.worker("lab")["up"], what="the lab worker's claim to be free")
        self.pp("queue", "kill", c)                              # the worker this starts carries out the stop
        self.until(lambda: not self.cell_up(root, name), what="the work the runner started to be gone")
        self.assertEqual([e[0] for e in self.asked_to_stop(root)], [name])

    def test_after_a_reboot_a_running_job_is_reaped_as_lost(self):
        """A machine that went down leaves a job that was running with no live run and no exit code of its own.
        The first worker that comes up records that attempt as lost and retries it where the retry filter
        matches lost. No worker is up until something needs one. Every queue that exists is named. A campaign
        file run again makes new jobs with new ids, because a tag is a name and never a key."""
        self.write("work/hold.sh", HOLD, 0o755)
        jid = self.add("--retries", "1", "--retry-if", "^lost$", "--retry-delay", "0.2",
                       "--", "./hold.sh", f"{self.work}/go")
        self.until(lambda: self.state(jid) == "running", what=f"job {jid} running")
        j = self.show(jid)
        kill_pid(self.until(lambda: self.worker()["pid"], what="the worker's pid"))   # the machine goes down ...
        group = proc.Handle.recorded(j["pgid"], j["pstart"], group=True)
        self.assertIsNotNone(group)
        proc.signal_group(group, signal.SIGKILL)                 # ... and the run, which writes no exit code
        self.until(lambda: not proc.alive(j["pid"]), what="the run to be gone")
        self.assertFalse(self.worker()["up"])                    # nothing needs a worker, so none is up
        self.write("work/go", "")                                # what the attempt after it waits for
        self.wait(jid)
        after = self.show(jid)
        self.assertEqual((after["state"], after["attempt"]), ("done", 2))
        self.assertEqual(self.field(self.field(after, "history")[0], "reason"), "lost")
        self.add("-q", "r1", "--no-start", "--hold", "--", "true")
        self.assertEqual(sorted(json.loads(self.status("--json"))["queues"]), ["default", "r1"])
        self.write("work/plan.txt", "./hold.sh go\n./hold.sh go\n")
        once = self.pp("queue", "add", "-q", "lab", "--from", "plan.txt", "--no-start").stdout.split()
        again = self.pp("queue", "add", "-q", "lab", "--from", "plan.txt", "--no-start").stdout.split()
        self.assertEqual(len(set(once) | set(again)), 4)


class TestTheSecondProjectAndTheSession(Lab):
    """Steps 14 and 15: a second project joins the queue, and an agent session hears how its own jobs ended."""

    def test_two_projects_share_one_queue(self):
        """One queue holds jobs of both projects, and each job carries its own project's name. A job added with
        --cwd into the second project takes that project's runner, stop, log and env, not the first project's.
        Neither project's tree holds the other's records."""
        lab = self.lab("lab")
        bench = self.lab("bench", name="bench", env='{ OFFLINE = "0", CELLS = "/work/cells" }')
        a = self.add("-q", "lab", "--tag", "cellA", "--", "./lab/run.sh", cwd=lab)
        b = self.add("-q", "lab", "--tag", "cellB", "--cwd", bench, "--", "./lab/run.sh", cwd=lab)
        ja, jb = self.show(a), self.show(b)
        self.assertEqual((ja["project"], jb["project"]), ("lab", "bench"))
        self.assertEqual((ja["queue"], jb["queue"]), ("lab", "lab"))
        self.assertEqual(self.field(jb, "runner"), [f"{bench}/docker/run.sh"])
        self.assertEqual(self.stop_of(jb), self.stop_cmd(bench))
        self.assertEqual(self.field(jb, "log"), f"{bench}/logs/cellB.log")
        self.assertEqual(self.field(jb, "env"), {"OFFLINE": "***", "CELLS": "***"})
        self.wait(a, b)
        self.assertTrue(os.path.exists(f"{lab}/logs/cellA.log"))
        self.assertTrue(os.path.exists(f"{bench}/logs/cellB.log"))
        self.assertFalse(os.path.exists(f"{lab}/logs/cellB.log"))
        self.assertFalse(os.path.exists(f"{bench}/logs/cellA.log"))
        for j in (ja, jb):                                       # the records are the store's, not a tree's
            self.assertTrue(self.field(j, "rcfile").startswith(self.env["PP_STATE_DIR"]), j["rcfile"])

    @unittest.expectedFailure
    def test_a_session_hears_how_its_own_jobs_ended(self):
        """`wait --mine` covers every job this session queued that is live, and every one that ended without
        the session hearing it. It exits 0 when all ended well, 1 when any failed, and 3 when it gives up at
        its timeout. `show --json` holds every field the machine surface promises. A wait over a held job does
        not return while the job is held."""
        agent = self.as_agent("sess-a")
        bad = self.add("--", "sh", "-c", "exit 3", env=agent)
        self.until(lambda: self.ended(bad), what=f"job {bad} ended")
        self.pp("queue", "wait", "--mine", "--poll", "0.2", code=1, env=agent)      # an outcome it never heard
        self.pp("queue", "wait", "--mine", "--poll", "0.2", code=0, env=agent)      # and every one reported
        live = self.add("--", "sh", "-c", f"while [ ! -f {self.work}/go ]; do sleep 0.05; done", env=agent)
        self.pp("queue", "wait", "--mine", "--timeout", "1", "--poll", "0.2", code=3, env=agent)
        self.write("work/go", "")
        self.pp("queue", "wait", "--mine", "--poll", "0.2", code=0, env=agent)
        j = self.show(live)
        self.assertEqual([f for f in PROMISED if f not in j], [])
        parked = self.add("--hold", "--tag", "s1_eval", "--", "true", env=agent)
        waiter = self.spawn("queue", "wait", "s1_eval", "--poll", "0.2")
        self.until(lambda: self.show(parked)["watchers"], what="the wait to be watching the held job")
        self.assertIsNone(waiter.poll())                         # held, so it is not ended and does not return
        self.pp("queue", "release", "s1_eval")
        self.assertEqual(waiter.wait(timeout=30), 0)


if __name__ == "__main__":
    unittest.main()
