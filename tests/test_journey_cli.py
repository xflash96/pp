"""Journeys through pp itself: the first run, finding a command, plugins of every kind, a broken config or cwd,
Ctrl-C and closed pipes, publishing a subset, and shell completion driven through real bash and zsh."""
import ast
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from journey import BUILTINS, PP, ROOT, Journey, copy_tree, has, has_env, needs  # noqa: E402
from pp.lib import session  # noqa: E402

ANY = BUILTINS[0]        # some built-in, for steps about pp itself


def summary(cmd: str) -> str:
    """A built-in's summary line: the first line of its docstring."""
    with open(os.path.join(ROOT, "pp", "commands", f"{cmd}.py")) as f:
        return ast.get_docstring(ast.parse(f.read())).splitlines()[0]


def files_under(d: str) -> list[str]:
    return sorted(os.path.relpath(os.path.join(r, f), d) for r, _, fs in os.walk(d) for f in fs)


class TestFirstRun(Journey):
    def test_listing_and_every_manual_work_on_a_bare_machine_and_write_nothing(self):
        r = self.pp()
        for cmd in BUILTINS:
            self.assertIn(cmd, r.stdout)
            self.assertIn(summary(cmd), r.stdout)
        self.assertIn("Unique prefixes work", r.stdout)
        for cmd in BUILTINS:
            for argv in (("help", cmd), (cmd, "-h"), (cmd, "--help"), ("-h", cmd), ("--help", cmd)):
                r = self.pp(*argv)
                self.assertIn(summary(cmd), r.stdout, argv)
                self.assertIn(f"pp {cmd}", r.stdout, argv)
        self.assertIn(os.path.join(ROOT), self.pp("-V").stdout)
        self.assertEqual(files_under(self.home), [], "reading help must not create config or state")

    def test_help_of_a_verb_and_help_about_help(self):
        if has("queue"):
            self.assertIn("usage: pp queue add", self.pp("help", "queue", "add").stdout)
            # the three spellings of one command's help are one reading of the words: `pp help NAME`, and the
            # flag before the name, with a verb after it. A reader who typed the flag first is answered.
            self.assertEqual(self.pp("-h", "queue").stdout, self.pp("help", "queue").stdout)
            self.assertEqual(self.pp("--help", "queue", "add").stdout, self.pp("help", "queue", "add").stdout)
        for argv in (("help",), ("help", "help"), ("help", "-h"), ("-h", "--all"), ("--all", "-h")):
            self.assertIn("usage: pp <command>", self.pp(*argv).stdout, argv)
        self.assertIn(os.path.join(ROOT, "pp", "commands", f"{ANY}.py"), self.pp("-h", "--all").stdout)


class TestTheHarness(Journey):
    def test_the_sandbox_isolates_what_journey_py_says(self):
        self.assertEqual(self.sh("command -v nvidia-smi").split(), [f"{self.bin}/nvidia-smi"])
        env = json.loads(self.sh(f"{sys.executable} -c 'import json, os; print(json.dumps(dict(os.environ)))'"))
        self.assertEqual(env["HOME"], self.home)
        for k in ("PP_CONFIG_DIR", "PP_STATE_DIR", "PP_DATA_DIR"):
            self.assertTrue(env[k].startswith(self.home + "/"), k)
        self.assertEqual((env["GIT_AUTHOR_EMAIL"], env["GIT_CONFIG_NOSYSTEM"], env["NO_COLOR"]),
                         (sandbox.GIT_EMAIL, "1", "1"))
        self.assertTrue(env["GIT_CONFIG_GLOBAL"].startswith(self.tmp + "/"), env["GIT_CONFIG_GLOBAL"])
        self.assertEqual(self.sh("git config --get core.hooksPath", check=False), "")
        self.assertEqual([k for k in env if k.startswith("XDG_")], [])
        self.assertEqual([k for k in env if k in session.SESSION_VARS], [])

    def test_cleanup_stops_what_holds_no_lock_yet(self):
        """A worker still starting (no lock, no pid file) and a job's own child are found by the sandbox's
        PP_STATE_DIR in their environment, and gone after cleanup, like everything else the story started."""
        starting = self.child(["sh", "-c", "sleep 60 & wait"], group="own")
        self._stop_workers()
        self.assertIsNotNone(starting.poll())
        mark = f"PP_STATE_DIR={self.env['PP_STATE_DIR']}".encode()
        self.assertEqual([p for p in os.listdir("/proc") if p.isdigit() and has_env(int(p), mark)], [])

    def test_a_gates_step_can_wait_for_another_actor_to_pass_the_gate(self):
        """journey.Gate's two halves, in the shape its own docstring gives and CONTRIBUTING names: a step
        that waits inside the gate for another actor to pass a point. The step holds the gate's lock for as
        long as it waits, so neither half of the rendezvous may want that lock, and a call the step makes
        into its own window runs nothing instead of waiting for itself."""
        gate, ran = journey.Gate(), []

        def step():
            gate.passed("inside")                   # said from inside the step, which holds the gate's lock
            self.assertFalse(gate.once(lambda: ran.append("again")), "the step ran inside itself")
            gate.wait("signalled", timeout=15)
            ran.append("stepped")

        def actor():
            ran.append(gate.once(step))
            ran.append(gate.once(lambda: ran.append("a second step")))      # the gate is done: nothing runs
        th = threading.Thread(target=actor, daemon=True)        # daemon: a gate that deadlocked ends the run
        th.start()
        try:
            gate.wait("inside", timeout=15)         # the step is in the gate now, holding its lock ...
            gate.passed("signalled")                # ... and this does not wait for it
        finally:
            th.join(20)
        self.assertFalse(th.is_alive(), "the step never came back out of the gate")
        self.assertEqual((gate.ran, ran), (1, ["stepped", True, False]))

    def test_every_verb_in_a_help_example_is_walked_by_a_journey(self):
        """CONTRIBUTING.md: a command's help examples run in its journey file, checked here as: every `pp NAME VERB` in
        the docstring appears in it. `completion` is walked in this file."""
        from pp import cli
        for cmd in BUILTINS:
            with open(os.path.join(ROOT, "pp", "commands", f"{cmd}.py")) as f:
                doc = ast.get_docstring(ast.parse(f.read())) or ""
            name = "cli" if cmd == "completion" else cmd
            path = os.path.join(ROOT, "tests", f"test_journey_{name}.py")
            self.assertTrue(os.path.exists(path), f"pp {cmd} has no journey file ({path})")
            text = self.read(path)
            for verb in cli._verbs(cmd, doc):
                self.assertIn(f'"{verb}"', text, f"`pp {cmd} {verb}` is in its help, but no journey runs it")


class TestFindingACommand(Journey):
    @needs("promote", "queue")
    def test_prefixes_aliases_typos(self):
        self.assertIn("usage: pp promote", self.pp("prom", "-h").stdout)
        self.assertIn("usage: pp queue", self.pp("help", "qu").stdout)
        r = self.pp("qeue", code=2)
        self.assertIn("did you mean queue", r.stderr)
        r = self.pp("help", "nosuch", code=2)
        self.assertIn("no command 'nosuch'", r.stderr)
        self.assertIn("no command ''", self.pp("", code=2).stderr)      # not "'' is ambiguous: <everything>"
        self.assertIn("unknown option -x", self.pp("-x", code=2).stderr)
        # a plugin starting with q makes `pp q` ambiguous, while full names and longer prefixes keep working
        self.write("home/.config/pp/commands/quote.py", '"""Quote"""\nALIASES = "qt"\ndef main(argv):\n    print("quoted")\n')
        r = self.pp("q", code=2)
        self.assertIn("'q' is ambiguous: queue, quote", r.stderr)
        self.assertIn("usage: pp queue", self.pp("queue", "-h").stdout)
        self.assertEqual(self.pp("quo").stdout, "quoted\n")
        self.assertEqual(self.pp("qt").stdout, "quoted\n")            # a string ALIASES is one alias...
        self.assertNotIn("(q, t)", self.pp().stdout)                   # ...not one per letter
        self.assertIn("quote (qt)", self.pp().stdout)


class TestAliases(Journey):
    """`[alias]` in config.toml, and the one order a word is resolved in: an exact command name, then the
    table, then a module's own ALIASES, then a unique prefix."""

    def setUp(self):
        super().setUp()
        self.write("home/.config/pp/commands/zecho.py",
                   '"""Show argv"""\nALIASES = ["ze"]\ndef main(argv):\n    print(" ".join(argv))\n    return 0\n')
        self.write("home/.config/pp/commands/zother.py",
                   '"""Show argv, too"""\ndef main(argv):\n    print("other", *argv)\n    return 0\n')

    def test_a_word_is_resolved_in_one_order(self):
        self.config('[alias]\nzecho = "zother"\nze = "zother"\nzec = "zother"\n'
                    'two = "zecho a b"\nchain = "two"\n')
        self.assertEqual(self.pp("zecho", "hi").stdout, "hi\n")         # an exact name beats an alias of it
        self.assertEqual(self.pp("ze", "hi").stdout, "other hi\n")      # an alias beats a module's ALIASES
        self.assertEqual(self.pp("zec", "hi").stdout, "other hi\n")     # ... and a unique prefix
        self.assertEqual(self.pp("two", "c").stdout, "a b c\n")         # its own words come before the typed
        self.pp("chain", code=2)                                        # one expansion: `two` names no command
        self.pp("-h", code=0)                                           # the entries it skipped break nothing

    def test_a_name_the_table_takes_is_listed_as_the_tables_own_and_no_module_s(self):
        """`ze` is zecho's own ALIASES and a name the table gives: the table is read first, so every listing
        offers that word once, as the table's, and none of them offers it as a name of zecho."""
        self.config('[alias]\nze = "zother"\n')
        self.assertEqual(self.pp("ze", "hi").stdout, "other hi\n")
        rows = [r.split("\t") for r in self.pp("--commands").stdout.splitlines()]
        self.assertEqual([r for r in rows if r[0] == "ze"], [["ze", "zother"]])
        self.assertNotIn("zecho (ze)", self.pp().stdout)
        if has("cmd"):
            ls = {r["name"]: r for r in json.loads(self.pp("cmd", "ls", "--json").stdout)}
            self.assertEqual((ls["zecho"]["aliases"], ls["zother"]["config_aliases"]), ([], ["ze"]))

    def test_a_config_file_that_does_not_parse_stops_the_word_that_needs_it(self):
        self.config('[alias\nze = "zother"\n')
        self.pp(code=0)                     # the listing names the file under what it skipped
        self.pp("-h", code=0)
        self.pp("zecho", "x", code=0)       # an exact name is dispatched without reading the table
        self.pp("zq", code=1)               # a word that needs the table: the usual bad-config error
        if has("queue"):
            self.pp("queue", "-h", code=0)

    @needs("promote")
    def test_the_help_and_the_completion_follow_the_expansion(self):
        self.config('[alias]\nst = "promote status"\n')
        self.assertEqual(self.pp("help", "st").stdout, self.pp("promote", "status", "--help").stdout)
        for cur in ("", "-"):
            want = self.pp("--complete", "promote", "status", cur).stdout
            self.assertEqual(self.pp("--complete", "st", cur).stdout, want)
        self.assertTrue(self.pp("--complete", "st", "-").stdout.strip())
        self.assertIn("st", self.pp("--complete", "s").stdout.split())
        self.assertIn("st", self.pp("--commands").stdout.split())


class TestPlugins(Journey):
    def setUp(self):
        super().setUp()
        cmds = "home/.config/pp/commands"
        self.marker = os.path.join(self.tmp, "imported")
        self.write(f"{cmds}/hello.py", f'"""Say hello\n\n    pp hello greet NAME\n    pp hello --loud NAME\n"""\n'
                   f'open({self.marker!r}, "w").close()\ndef main(argv):\n    print("hello", *argv)\n')
        self.write(f"{cmds}/syntax.py", '"""Broken"""\ndef main(argv:\n')
        self.write(f"{cmds}/needsdep.py", '"""Needs a dep"""\nimport no_such_module_xyz\ndef main(argv):\n    pass\n')
        self.write(f"{cmds}/nomain.py", '"""No main"""\n')
        self.write(f"{cmds}/wrapper.py", '"""Wraps a package"""\nfrom json.tool import main\n')
        self.write(f"{cmds}/kill.py", '"""Impostor"""\ndef main(argv):\n    print("impostor")\n')
        self.write(f"{cmds}/says.py", '"""Returns a message"""\ndef main(argv):\n    return "it went wrong"\n')
        self.write(f"{cmds}/says2.py", '"""Returns several lines"""\ndef main(argv):\n'
                   '    return "it went wrong\\nand here is why"\n')
        self.tool("pp-shout", 'echo "SHOUT $*"; exit 3\n')
        self.write("bin/pp-notexec", "#!/bin/sh\necho no\n", 0o644)
        self.write("bin/pp-noshebang", "echo no\n", 0o755)               # the kernel cannot run this file
        self.write("bin/pp-badinterp", "#!/no/such/interpreter\n", 0o755)

    def test_listing_sorts_them_and_runs_none(self):
        out = self.pp().stdout
        user, external = out.split("user commands")[1], out.split("external (pp-* on PATH):")[1]
        self.assertIn("hello", user)
        self.assertIn("Say hello", user)
        self.assertIn("wrapper", user)               # `from x import main` defines main too
        self.assertIn("shout", external)
        self.assertNotIn("notexec", out)
        broken = out.split("broken (skipped):")[1]
        for name, why in (("syntax", "SyntaxError"), ("nomain", "defines no main")):
            self.assertRegex(broken, rf"{name}\s+{why}")
        self.assertIn("needsdep", user)                # parses fine: listed; it fails only when run
        hidden = out.split("hidden by a same-named command")[1]
        self.assertIn(os.path.join("commands", "kill.py"), hidden)
        self.assertIn("shadows", self.pp("-h", "--all").stdout)
        self.assertFalse(os.path.exists(self.marker), "listing imported a plugin")

    @needs("kill", "queue")
    def test_running_them(self):
        self.assertEqual(self.pp("hello", "a", "b").stdout, "hello a b\n")
        self.assertTrue(os.path.exists(self.marker))
        self.assertEqual(self.pp("shout", "x", code=3).stdout, "SHOUT x\n")      # an external's exit code passes through
        self.assertIn("is broken: SyntaxError", self.pp("syntax", code=1).stderr)
        r = self.pp("needsdep", code=1)
        self.assertIn("failed to load: ModuleNotFoundError", r.stderr)
        self.assertIn("PP_DEBUG=1", r.stderr)
        self.assertNotIn("Traceback", r.stderr)
        r = self.pp("needsdep", code=1, env=dict(self.env, PP_DEBUG="1"))
        self.assertIn("Traceback", r.stderr)
        self.assertNotIn("PP_DEBUG=1 for", r.stderr)
        r = self.pp("says", code=1)                                              # like sys.exit("message")
        self.assertEqual(r.stderr, "pp: it went wrong\n")
        r = self.pp("says2", code=1)                 # a message of several lines reads as the lines it is
        self.assertEqual(r.stderr, "pp: it went wrong\n  and here is why\n")
        self.assertNotIn("impostor", self.pp("kill", "-h").stdout)              # a plugin never replaces a built-in
        self.assertIn("usage: pp queue", self.pp("queue", "-h").stdout)         # and broken plugins break nothing

    def test_an_external_the_kernel_will_not_run_says_so(self):
        """An external command is listed because it is executable, which is not the same as runnable: a file
        with no shebang and one naming an interpreter that is gone are both listed and neither can exec. Each
        says what is wrong, like every other broken command, rather than printing a traceback."""
        out = self.pp().stdout.split("external (pp-* on PATH):")[1]
        for name in ("noshebang", "badinterp"):
            self.assertIn(name, out)
            r = self.pp(name, code=1)
            self.assertIn(f"command '{name}' ({self.bin}/pp-{name}) could not run:", r.stderr)
            self.assertNotIn("Traceback", r.stderr)


class TestLauncher(Journey):
    def test_through_a_chain_of_symlinks_from_anywhere(self):
        rel = os.path.join(self.home, ".local/share/pp/releases/pp")
        os.makedirs(rel)
        os.symlink(ROOT, os.path.join(rel, "current"))
        os.makedirs(os.path.join(self.home, ".local/bin"))
        link = os.path.join(self.home, ".local/bin/pp")
        os.symlink(os.path.join(rel, "current/bin/pp"), link)
        for cwd in ("/", self.home):
            r = subprocess.run([link, "-V"], capture_output=True, text=True, cwd=cwd, env=self.env, timeout=30)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn(os.path.realpath(ROOT), r.stdout)
            r = subprocess.run([link, "help", ANY], capture_output=True, text=True, cwd=cwd, env=self.env, timeout=30)
            self.assertIn(f"usage: pp {ANY}", r.stdout)


class TestWhenThingsGoWrong(Journey):
    def test_a_bad_config_is_named_not_a_traceback(self):
        path = self.config("[queue.default\nslots = 1\n")
        for argv in [a for a in (("queue",), ("queue", "add", "--", "true")) if has(a[0])]:
            r = self.pp(*argv, code=1)
            self.assertIn(f"bad config: {path}", r.stdout + r.stderr, argv)     # status still shows, saying why
            self.assertIn("line 1", r.stdout + r.stderr)
            self.assertNotIn("Traceback", r.stderr)
        self.pp()                       # what needs no config keeps working
        for cmd in BUILTINS:
            self.pp(cmd, "-h")

    def test_a_misspelt_key_is_named_in_each_table(self):
        """CONTRIBUTING: an unknown key or a bad value is a config error naming it, never a silent default."""
        if has("promote"):
            self.write("work/proj/.pp.toml", '[promote]\nchek = "false"\n')
            r = self.pp("promote", "-n", cwd=f"{self.work}/proj", code=1)
            self.assertIn("[promote]: unknown chek", r.stdout + r.stderr)
        if has("batch"):
            path = self.config('[batch]\npresetz = 1\n')
            self.assertIn("[batch]: unknown presetz", self.pp("batch", "presets", code=1).stderr)
            with open(path, "w") as f:
                f.write('[batch.presets]\nwebp = { cmd = "cwebp {} -o {out}", outt = "{base}.webp" }\n')
            self.assertIn("[batch.presets] webp", self.pp("batch", "presets", code=1).stderr)
        if has("queue"):
            self.write("work/q/.pp.toml", '[queue]\nrunnr = "x"\n')
            self.assertIn("[queue]: unknown runnr", self.pp("queue", "add", "--", "true", cwd=f"{self.work}/q",
                                                            code=1).stderr)

    def test_a_command_table_written_as_a_value(self):
        """`http = 3` where [http] belongs: every command that reads its table says so, none crashes."""
        path = self.config('batch = 1\nhttp = 3\nqueue = []\nresources = "lots"\n')
        for argv, key in ((("batch", "presets"), "batch"), (("http", "--no-discover", "-p", "0"), "http"),
                          (("queue", "add", "--", "true"), "queue")):
            if not has(argv[0]):
                continue
            r = self.pp(*argv, code=None, timeout=30)
            self.assertNotIn("Traceback", r.stderr, argv)
            self.assertNotEqual(r.returncode, 0, argv)
            self.assertIn(path, r.stdout + r.stderr, argv)
            self.assertIn(f"[{key}] must be a table", r.stdout + r.stderr, argv)

    @needs("queue", "promote")
    def test_a_deleted_cwd(self):
        gone = os.path.join(self.work, "gone")
        os.makedirs(gone)
        p = self.child(["sh", "-c", f"cd {gone} && rmdir {gone} && "
                        f"{sys.executable} {PP} queue add -- true; echo rc=$?; "
                        f"{sys.executable} {PP} promote -n; echo rc=$?; "
                        f"{sys.executable} {PP} -h >/dev/null; echo rc=$?"], stderr=subprocess.PIPE)
        out, err = p.communicate(timeout=60)
        self.assertEqual(out.split(), ["rc=1", "rc=1", "rc=0"], err)
        self.assertIn("the current directory no longer exists", err)
        self.assertNotIn("Traceback", err)

    def test_ctrl_c_is_exit_130_without_a_traceback(self):
        self.write("home/.config/pp/commands/slow.py", '"""Slow"""\nimport time\ndef main(argv):\n'
                   '    print("working", flush=True)\n    time.sleep(30)\n')
        p = self.child([sys.executable, PP, "slow"], group="own", stderr=subprocess.PIPE, ready="working")
        p.signal_group(signal.SIGINT)                         # what the terminal sends the foreground group
        _, err = p.communicate(timeout=15)
        self.assertEqual(p.returncode, 130)
        self.assertNotIn("Traceback", err)

    def test_a_reader_that_went_away(self):
        """`pp ... | head -1` and friends: the pipe is closed before pp writes, or while it writes."""
        self.write("home/.config/pp/commands/lots.py", '"""Lots"""\ndef main(argv):\n'
                   '    for i in range(100000):\n        print("line", i)\n')
        for argv in ((), ("help", ANY), ("--commands",), ("lots",)):
            r, w = os.pipe()
            os.close(r)
            p = subprocess.run([sys.executable, PP, *argv], stdout=w, stderr=subprocess.PIPE, text=True, env=self.env,
                               timeout=30)
            os.close(w)
            self.assertEqual((p.returncode, p.stderr), (0, ""), argv)
        r = subprocess.run(f"{sys.executable} {PP} lots | head -1", shell=True, capture_output=True, text=True,
                           env=self.env, timeout=30)
        self.assertEqual((r.stdout, r.stderr), ("line 0\n", ""))

    @needs("batch")
    def test_a_plan_comes_before_the_refusal_in_one_stream(self):
        """ui.confirm flushes: what a command printed before asking is not stuck behind the refusal in a pipe."""
        self.write("work/x1.txt", "")
        r = subprocess.run([sys.executable, PP, "batch", "rename", "x", "y", "x*"], stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, text=True, cwd=self.work, env=self.env,
                           timeout=30)
        self.assertEqual(r.returncode, 2, r.stdout)
        self.assertLess(r.stdout.index("x1.txt"), r.stdout.index("no terminal to ask at"))


class TestPublishingASubset(Journey):
    def test_deleting_command_files_leaves_the_rest_working(self):
        tree = copy_tree(os.path.join(self.tmp, "subset"))
        gone = BUILTINS[1::2]
        for cmd in gone:
            os.remove(os.path.join(tree, "pp/commands", f"{cmd}.py"))
        pp = [sys.executable, os.path.join(tree, "bin", "pp")]
        out = subprocess.run(pp, capture_output=True, text=True, env=self.env).stdout
        for cmd in BUILTINS:
            self.assertEqual(f"\n  {cmd} " in out, cmd not in gone, cmd)
        for cmd in set(BUILTINS) - set(gone):
            r = subprocess.run(pp + [cmd, "-h"], capture_output=True, text=True, env=self.env)
            self.assertEqual(r.returncode, 0, (cmd, r.stderr))
        r = subprocess.run(pp + [gone[0]], capture_output=True, text=True, env=self.env)
        self.assertEqual(r.returncode, 2)
        self.assertIn(f"no command '{gone[0]}'", r.stderr)


BASH_DRIVER = r'''source <(pp completion bash)
for line in "$@"; do
  read -ra COMP_WORDS <<< "$line"
  [[ $line == *" " ]] && COMP_WORDS+=("")
  COMP_CWORD=$(( ${#COMP_WORDS[@]} - 1 ))
  _pp
  echo "$line|${COMPREPLY[*]}"
done
'''

BASH_TAB_DRIVER = r'''source <(pp completion bash)
# the same, and then what pressing Enter would do: readline leaves the candidate on the command line as the
# shell's own source, so each one is read back here the way the shell reads a command line, word by word
for line in "$@"; do
  read -ra COMP_WORDS <<< "$line"
  [[ $line == *" " ]] && COMP_WORDS+=("")
  COMP_CWORD=$(( ${#COMP_WORDS[@]} - 1 ))
  _pp
  echo "REPLY|$line|${COMPREPLY[*]}"
  for rep in "${COMPREPLY[@]}"; do eval "printf 'WORD|%s\n' $rep"; done
done
'''


ZSH_TAB_DRIVER = r'''# types $2 + TAB into an interactive zsh with pp's completion, and waits for $1 on the screen.
# The line is never run: what the candidates are is the whole point, and running the line would be the driver
# doing the thing the test says nothing does.
zmodload zsh/zpty
zpty z "zsh -f -i"
buf=""
upto() {      # read until the screen holds $1; a driver that waited forever would fail no test
  local chunk i=0
  while [[ $buf != *"$1"* ]]; do
    if zpty -r -t z chunk; then buf+=$chunk; i=0; else sleep 0.05; (( ++i > 300 )) && { print "TIMEOUT: $1"; exit 1 } fi
  done
}
zpty -w z 'bindkey -e; PS1="RE""ADY> "; autoload -U compinit; compinit -u -D; source <(pp completion zsh)'
upto "READY> "
zpty -w -n z "$2"$'\t'
upto "$1"
zpty -d z
print -r -- "SAW: $1"
'''


ZSH_DRIVER = r'''# types each argument + TAB into an interactive zsh with pp's completion, then prints "LINE: <the line>"
zmodload zsh/zpty
zpty z "zsh -f -i"
prompts() { local rest=$buf c=0; while [[ $rest == *"READY> "* ]]; do rest=${rest#*"READY> "}; (( c++ )); done; print $c }
upto() {
  local chunk i=0
  while (( $(prompts) < $1 )); do
    if zpty -r -t z chunk; then buf+=$chunk; else sleep 0.05; (( ++i > 300 )) && { print "TIMEOUT"; exit 1 }; fi
  done
}
buf=""
zpty -w z 'bindkey -e; PS1="RE""ADY> "; autoload -U compinit; compinit -u -D; source <(pp completion zsh)'
n=1; upto $n
for line in "$@"; do
  zpty -w -n z "$line"$'\t\C-aprint -r -- LINE: \n'
  (( n++ )); upto $n
done
zpty -d z
print -r -- "${buf//$'\r'/}" | grep -a '^LINE:'
'''


@needs("completion")
class TestCompletion(Journey):
    def setUp(self):
        super().setUp()
        os.symlink(PP, os.path.join(self.bin, "pp"))         # the shells run `pp` from PATH, as a person's do
        self.write("home/.config/pp/commands/greet.py", '"""Greet\n\n    pp greet loud NAME\n    pp greet -s NAME\n"""\n'
                   f'open({os.path.join(self.tmp, "imported")!r}, "w").close()\ndef main(argv):\n    pass\n')
        self.write("work/notes file.txt", "")

    @needs("queue", "promote", "http")
    def test_bash_completes_commands_verbs_flags_then_files(self):
        lines = ["pp ", "pp q", "pp queue ", "pp queue a", "pp queue add --ti", "pp queue add -- ", "pp help q",
                 "pp queue add --retry-", "pp promote ", "pp completion ", "pp greet ", "pp http "]
        r = subprocess.run(["bash", "--norc", "-c", BASH_DRIVER, "driver", *lines], capture_output=True, text=True,
                           cwd=self.work, env=self.env, timeout=60)
        got = dict(line.split("|", 1) for line in r.stdout.splitlines())
        self.assertEqual(set(got), set(lines), r.stderr)
        for cmd in BUILTINS + ("greet", "help"):
            self.assertIn(cmd, got["pp "].split())
        self.assertEqual(got["pp q"], "queue")
        self.assertIn("add", got["pp queue "].split())
        self.assertIn("watch", got["pp queue "].split())
        self.assertEqual(got["pp queue a"], "add")
        self.assertEqual(got["pp queue add --ti"], "--timeout")
        self.assertEqual(got["pp queue add -- "], "notes file.txt")        # after --: the job's own command line
        self.assertEqual(got["pp help q"], "queue")
        self.assertEqual(sorted(got["pp queue add --retry-"].split()), ["--retry-delay", "--retry-if"])
        self.assertIn("rollback", got["pp promote "].split())
        self.assertEqual(sorted(got["pp completion "].split()), ["bash", "zsh"])
        self.assertEqual(got["pp greet "], "loud")                         # a plugin's verbs, from its docstring
        self.assertEqual(got["pp http "], "notes file.txt")
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "imported")), "TAB ran a plugin")
        self.assertEqual(files_under(os.path.join(self.home, ".local")) if os.path.isdir(
            os.path.join(self.home, ".local")) else [], [], "completion created state")

    # Names a shell reads as code if anything ever expands one, or as another name if anything reads one of
    # their characters as its own: a command's name is any file name ending in .py, and a plugin brings the
    # file. The last is not valid UTF-8, which is a file name too.
    HOSTILE = ("zz$(touch pwned1)a", "zz`touch pwned2`b", "zz spaced", "zz*glob", "zzq\"uote", "zzbs\\lash")
    SHOWN = list(HOSTILE) + ["zz\\udcffbyte"]       # each as pp offers it, escaped where it has to be

    def plant_hostile_names(self) -> None:
        for name in list(self.HOSTILE) + [os.fsdecode(b"zz\xffbyte")]:
            self.write(f"home/.config/pp/commands/{name}.py",
                       '"""Innocent looking"""\ndef main(argv):\n    return 0\n')

    def test_what_bash_leaves_on_the_command_line_is_the_name_and_nothing_to_run(self):
        """TAB must never run anyone's code, which is why a plugin's docstring is read without importing it.
        A candidate is as much someone else's text, and the line TAB leaves behind is the shell's own source,
        which Enter is what reads: each candidate goes onto it quoted, so a `$(...)` in a command file's name
        is the name you completed rather than a command run as you on the next Enter, and a name holding a
        space, a quote or a backslash still names the command it is."""
        self.plant_hostile_names()
        # errors="replace": what pp hands the shell is escaped, and a byte that got through unescaped is this
        # test's answer to read, not an exception in the middle of reading it
        r = subprocess.run(["bash", "--norc", "-c", BASH_TAB_DRIVER, "driver", "pp zz", "pp "],
                           capture_output=True, text=True, errors="replace", cwd=self.work, env=self.env,
                           timeout=60)
        self.assertEqual(files_under(self.work), ["notes file.txt"], f"TAB ran a name: {r.stdout}{r.stderr}")
        said = r.stdout.splitlines()
        got = dict(ln.removeprefix("REPLY|").split("|", 1) for ln in said if ln.startswith("REPLY|"))
        words = [ln.removeprefix("WORD|") for ln in said if ln.startswith("WORD|")]
        for name in self.SHOWN:
            self.assertIn(name, words, (name, r.stdout, r.stderr))      # one word, and the name it was
        self.assertNotIn("$(", got["pp zz"], r.stdout)                  # nothing is left for Enter to expand
        for cmd in BUILTINS:
            self.assertIn(cmd, got["pp "].split())

    @unittest.skipUnless(shutil.which("zsh"), "no zsh")
    def test_zsh_expands_no_candidate_on_tab(self):
        """The same TAB in the other shell's line editor, where the candidates go to compadd. The screen
        showing the last of them is how the test knows they all reached it."""
        self.plant_hostile_names()
        env = {k: v for k, v in self.env.items() if k not in ("EDITOR", "VISUAL")}
        r = subprocess.run(["zsh", "-f", "-c", ZSH_TAB_DRIVER, "driver", "zz*glob", "pp zz"], capture_output=True,
                           text=True, cwd=self.work, env=env, timeout=60)
        self.assertEqual(r.stdout.strip(), "SAW: zz*glob", (r.stdout, r.stderr))
        self.assertEqual(files_under(self.work), ["notes file.txt"], f"TAB ran a name: {r.stdout}{r.stderr}")

    @unittest.skipUnless(shutil.which("zsh"), "no zsh")
    def test_what_zsh_leaves_on_the_command_line_is_the_name_itself(self):
        """zsh quotes what it inserts, so nothing in a name is run there. What the first word's candidates go
        through is _describe, whose spec reads a backslash in a name as the escape of the character after it,
        so a name holding one is escaped for that spec. The driver reads the line back as the shell reads it,
        which is the name the user would have run."""
        self.plant_hostile_names()
        env = {k: v for k, v in self.env.items() if k not in ("EDITOR", "VISUAL")}
        r = subprocess.run(["zsh", "-f", "-c", ZSH_DRIVER, "driver", "pp zzbs"], capture_output=True, text=True,
                           cwd=self.work, env=env, timeout=60)
        self.assertEqual([ln.removeprefix("LINE: ").strip() for ln in r.stdout.splitlines()],
                         ["pp zzbs\\lash"], (r.stdout, r.stderr))
        self.assertEqual(files_under(self.work), ["notes file.txt"], f"TAB ran a name: {r.stdout}{r.stderr}")

    @unittest.skipUnless(shutil.which("zsh"), "no zsh")
    @needs("queue", "promote", "kill")
    def test_zsh_completes_in_a_real_line_editor(self):
        lines = {"pp queue a": "pp queue add", "pp promote ro": "pp promote rollback", "pp queue add --ti":
                 "pp queue add --timeout", "pp help ki": "pp help kill", "pp gre": "pp greet",
                 "pp completion z": "pp completion zsh"}
        env = {k: v for k, v in self.env.items() if k not in ("EDITOR", "VISUAL")}
        t = time.monotonic()
        r = subprocess.run(["zsh", "-f", "-c", ZSH_DRIVER, "driver", *lines], capture_output=True, text=True,
                           cwd=self.work, env=env, timeout=60)
        got = [line.removeprefix("LINE: ").strip() for line in r.stdout.splitlines()]
        self.assertEqual(got, list(lines.values()), (r.stdout, r.stderr, time.monotonic() - t))


if __name__ == "__main__":
    unittest.main()
