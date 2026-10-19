import io
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import tomllib
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp import cli  # noqa: E402
from pp.commands import cmd  # noqa: E402
from pp.lib import paths, proc  # noqa: E402

# who commits comes from the sandbox, which owns HOME and GIT_*; these two are what a global config of the
# machine's would have said and nothing in the scope says
GIT = ["git", "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main"]


class TestUrl(unittest.TestCase):
    def test_github_shorthand_paths_and_urls(self):
        self.assertEqual(cmd._url("github.com/alice/tools"), "https://github.com/alice/tools")
        self.assertEqual(cmd._url("https://example.org/x.git"), "https://example.org/x.git")
        self.assertEqual(cmd._url("git@github.com:alice/tools.git"), "git@github.com:alice/tools.git")
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(cmd._url(os.path.relpath(d)), os.path.realpath(d) if os.path.islink(d) else os.path.abspath(d))

    def test_plugin_names(self):
        for ok in ("tools", "pp-tools", "a.b_c"):
            self.assertTrue(cmd.NAME_RE.fullmatch(ok), ok)
        for bad in ("../x", ".hidden", "a/b", ""):
            self.assertFalse(cmd.NAME_RE.fullmatch(bad), bad)



class TestInstalledPackage(unittest.TestCase):
    """A wheel installs the pp package alone: no bin/pp beside it, no tests/ to add to."""

    def test_workers_start_as_a_module_and_new_refuses_without_user(self):
        with tempfile.TemporaryDirectory() as site, mock.patch.object(paths, "pp_root", return_value=site):
            self.assertEqual(paths.self_cmd(), [sys.executable, "-P", "-m", "pp"])
            err = io.StringIO()
            with redirect_stderr(err), self.assertRaises(SystemExit) as e:
                cmd._source_tree()
            self.assertEqual(e.exception.code, 2)
            self.assertIn("--user", err.getvalue())

    def test_a_tree_runs_its_own_launcher(self):
        self.assertEqual(paths.self_cmd(), [sys.executable, os.path.join(paths.pp_root(), "bin", "pp")])


class CmdCase(unittest.TestCase):
    """A `pp cmd` in process, with its own plugins directory, and repositories to add from."""

    def setUp(self):
        self.sb = sandbox.enter(self)
        self.tmp = self.sb.root
        self.plugins = os.path.join(self.sb.data, "plugins")

    def repo(self, name: str, *commands: str) -> str:
        """A git repository of command files, ready to add as a plugin."""
        path = os.path.join(self.tmp, "src", name, "commands")
        os.makedirs(path)
        for c in commands:
            with open(os.path.join(path, c + ".py"), "w") as f:
                f.write(f'"""{c}, from {name}"""\n\n\ndef main(argv):\n    return 0\n')
        root = os.path.dirname(path)
        for args in (["init", "-q"], ["add", "-A"], ["commit", "-qm", "init"]):
            subprocess.run(GIT + ["-C", root, *args], check=True, capture_output=True)
        return root

    def pp(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err), mock.patch("sys.stdin", io.StringIO()):
            try:
                rc = cmd.main(list(argv))
            except SystemExit as e:
                rc = e.code
        return rc, out.getvalue(), err.getvalue()

    def other_pp(self, *argv) -> subprocess.CompletedProcess:
        """`pp cmd ...` as a second process, which has the PP_* directories from the environment and a staging
        directory and a pid of its own."""
        r = subprocess.run([sys.executable, os.path.join(paths.pp_root(), "bin", "pp"), "cmd", *argv],
                           capture_output=True, text=True, timeout=120)
        return r

    def index(self) -> dict:
        with open(os.path.join(self.plugins, ".index.json")) as f:
            return json.load(f)

    def interrupted(self, *argv):
        """`pp cmd ...` that a Ctrl-C ends -> (stdout, stderr). A verb that swallowed the signal would come
        back with an exit code instead. journey.interruptible is what makes the raised SIGINT arrive."""
        out, err = io.StringIO(), io.StringIO()
        with journey.interruptible(), redirect_stdout(out), redirect_stderr(err), \
                mock.patch("sys.stdin", io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                cmd.main(list(argv))
        return out.getvalue(), err.getvalue()


class TestAliasFlags(CmdCase):
    """`pp cmd alias` reads its own flags wherever they stand before a `--`, and every word after a `--` is
    the aliased command line's. So a `-n` that follows the command is a dry run, and a command of its own
    `-n` or `--json` is written by putting it after a `--`."""

    def setUp(self):
        super().setUp()
        self.cfg = os.path.join(self.sb.config, "config.toml")

    def bytes_of(self) -> bytes:
        with open(self.cfg, "rb") as f:
            return f.read()

    def table(self) -> dict:
        return tomllib.loads(self.bytes_of().decode())["alias"]

    def test_a_dry_run_flag_after_the_command_writes_nothing(self):
        rc, _out, err = self.pp("alias", "st", "cmd", "ls", "-n")
        self.assertEqual(rc, 0, err)
        self.assertFalse(os.path.exists(self.cfg))      # a dry run makes no file where there was none

    def test_a_dry_run_flag_wherever_it_stands_leaves_the_file_as_it_was(self):
        self.assertEqual(self.pp("alias", "zc", "cmd", "ls")[0], 0)
        was = self.bytes_of()
        for args in (("alias", "-n", "st", "cmd"), ("alias", "st", "-n", "cmd"), ("alias", "st", "cmd", "-n")):
            rc, _out, err = self.pp(*args)
            self.assertEqual(rc, 0, err)
            self.assertEqual(self.bytes_of(), was)
        self.assertEqual(sorted(self.table()), ["zc"])

    def test_a_command_line_after_a_double_dash_keeps_the_flags_in_it(self):
        rc, _out, err = self.pp("alias", "dq", "--", "cmd", "ls", "-n")
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.table()["dq"], "cmd ls -n")

    def test_a_double_dash_the_command_line_holds_is_written_as_it_was_given(self):
        for name, argv, value in (("br", ("cmd", "ls", "--", "-x.txt"), "cmd ls -- -x.txt"),
                                  ("st", ("cmd", "--"), "cmd --"), ("bn", ("cmd", "ls", "--", "-n"), "cmd ls -- -n")):
            rc, _out, err = self.pp("alias", name, *argv)
            self.assertEqual(rc, 0, err)
            self.assertEqual(self.table()[name], value)

    def test_json_with_a_name_and_a_command_is_a_usage_error(self):
        rc, out, _err = self.pp("alias", "qj", "cmd", "ls", "--json")
        self.assertEqual(rc, 2)
        self.assertEqual(out, "")
        self.assertFalse(os.path.exists(self.cfg))

    def test_a_flag_that_is_not_this_verbs_own_belongs_to_the_command(self):
        rc, _out, err = self.pp("alias", "q", "cmd", "-q", "big")
        self.assertEqual(rc, 0, err)
        self.assertEqual(self.table()["q"], "cmd -q big")


class TestACtrlCWhileItWorks(CmdCase):
    """A Ctrl-C while a verb does the part a person waits for. The signal is not held back, and what it leaves
    is a state the next `pp cmd` of that plugin acts on."""

    def test_the_checkout_of_an_update_ends_and_the_pin_stays_where_it_was(self):
        src = self.repo("alpha", "alfa")
        self.assertEqual(self.pp("add", src, "-y")[0], 0)
        pinned = self.index()["alpha"]["commit"]
        with open(os.path.join(src, "commands", "alfa.py"), "a") as f:
            f.write("# two\n")
        for args in (["add", "-A"], ["commit", "-qm", "two"]):
            subprocess.run(GIT + ["-C", src, *args], check=True, capture_output=True)
        real = cmd._git

        def at_the_checkout(*args, **kw):
            if args[0] == "checkout":
                proc.raise_to_self(signal.SIGINT)       # what a terminal sends pp on Ctrl-C
            return real(*args, **kw)

        with mock.patch.object(cmd, "_git", at_the_checkout):
            self.interrupted("update", "alpha", "-y")
        self.assertEqual(self.index()["alpha"]["commit"], pinned, "the pin moved although the checkout did not")
        rc, out, err = self.pp("update", "alpha", "-y")         # the next update offers the move again
        self.assertEqual(rc, 0, err)
        self.assertIn("alpha: now at", out)
        self.assertNotEqual(self.index()["alpha"]["commit"], pinned)

    def test_the_removal_of_an_rm_ends_and_the_record_of_it_stays(self):
        src = self.repo("alpha", "alfa")
        self.assertEqual(self.pp("add", src, "-y")[0], 0)
        clone = os.path.join(self.plugins, "alpha")
        real = shutil.rmtree

        def part_way(path, **kw):
            real(os.path.join(path, "commands"))        # part of the clone is gone
            proc.raise_to_self(signal.SIGINT)
            return real(path, **kw)

        with mock.patch.object(cmd.shutil, "rmtree", part_way):
            self.interrupted("rm", "alpha", "-y")
        self.assertEqual(sorted(self.index()), ["alpha"], "the record went although the clone is still there")
        self.assertTrue(os.path.isdir(clone))
        rc, out, err = self.pp("rm", "alpha", "-y")            # the next rm removes what is left
        self.assertEqual(rc, 0, err)
        self.assertIn("removed alpha", out)
        self.assertEqual(self.index(), {})
        self.assertFalse(os.path.exists(clone))


class TestTwoAtOnce(CmdCase):
    """Two `pp cmd` verbs at once: this one in process, the competing one a second pp of its own, run at the
    exact point where this one had already been told the name was free, or had already read the index it is
    about to write."""

    def test_a_plugin_added_while_it_asked_is_refused_by_name(self):
        mine, theirs = self.repo("alpha", "alfa"), self.repo("other", "bravo")
        asked = journey.Gate()

        def add_theirs():
            r = self.other_pp("add", theirs, "--name", "alpha", "-y")
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

        def proceed(a, prompt):
            """The question is the window: the name was free when this add was told to go ahead."""
            asked.once(add_theirs)
            return None

        with mock.patch.object(cmd, "_proceed", proceed):
            rc, _, err = self.pp("add", mine, "-y")
        self.assertEqual((rc, asked.ran), (2, 1), err)
        self.assertIn("a plugin named alpha is there already", err)
        self.assertEqual(sorted(self.index()), ["alpha"])
        self.assertEqual(self.index()["alpha"]["url"], theirs)      # the one that actually landed
        self.assertEqual(sorted(os.listdir(self.plugins)), [".index.json", ".lock", "alpha"])

    def test_a_command_name_taken_while_it_asked_is_refused(self):
        mine, theirs = self.repo("alpha", "shared"), self.repo("beta", "shared")
        asked = journey.Gate()

        def add_theirs():
            r = self.other_pp("add", theirs, "-y")
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

        def proceed(a, prompt):
            asked.once(add_theirs)
            return None

        with mock.patch.object(cmd, "_proceed", proceed):
            rc, _, err = self.pp("add", mine, "-y")
        self.assertEqual((rc, asked.ran), (2, 1), err)
        self.assertIn("shared: plugin command", err)
        self.assertEqual(sorted(self.index()), ["beta"])
        self.assertFalse(os.path.exists(os.path.join(self.plugins, "alpha")))
        self.assertEqual(cli.discover()["shared"].shadows, [])      # one plugin provides it, not two

    def test_a_plugin_added_while_it_asked_is_still_in_the_index_after(self):
        """Another plugin lands while this add is on the question. Both are in the index afterwards, and `rm`
        of each finds a record and a tree. That no read-modify-write of the index forgets the other's record
        is what tests/test_paths.py pins; this is the same promise through the command."""
        mine, theirs = self.repo("alpha", "alfa"), self.repo("beta", "bravo")
        asked = journey.Gate()

        def add_theirs():
            r = self.other_pp("add", theirs, "-y")
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

        def proceed(a, prompt):
            asked.once(add_theirs)
            return None

        with mock.patch.object(cmd, "_proceed", proceed):
            rc, _, err = self.pp("add", mine, "-y")
        self.assertEqual((rc, asked.ran), (0, 1), err)
        self.assertEqual(sorted(self.index()), ["alpha", "beta"])
        for name in ("alpha", "beta"):
            self.assertEqual(self.pp("rm", name, "-y")[0], 0, f"{name} is on disk but not in the index")


if __name__ == "__main__":
    unittest.main()
