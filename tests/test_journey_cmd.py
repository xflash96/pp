"""Journeys through `pp cmd`: writing a command of your own, naming a command whatever you like, and adding,
updating and removing a git repository of someone else's. A local repository stands in for GitHub."""
import json
import os
import shutil
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from journey import BUILTINS, Journey, copy_tree  # noqa: E402

ANY = BUILTINS[0]        # some built-in, for a name that is taken
HELLO = '"""Say hello"""\ndef main(argv):\n    print("hello {v}")\n    return 0\n'


class TestNew(Journey):
    def test_a_machine_local_command_from_scaffold_to_use(self):
        path = os.path.join(self.env["PP_CONFIG_DIR"], "commands", "hello.py")
        self.pp("cmd", "new", "hello", "--user", "-n")        # the plan names the file and writes nothing
        self.assertFalse(os.path.lexists(path))
        r = self.pp("cmd", "new", "hello", "--user", "-s", "Say hello")
        self.assertIn(f"wrote {path}", r.stdout)
        self.assertIn("try: pp hello -h", r.stdout)
        self.assertIn("Say hello", self.pp().stdout.split("user commands")[1])
        h = self.pp("hello", "-h").stdout
        self.assertIn("Say hello", h)
        for flag in ("-n, --dry-run", "-y, --yes"):          # the contract's flags are in the scaffold
            self.assertIn(flag, h)
        self.assertEqual(self.pp("hello", "world").stdout, "world\n")
        self.assertIn("is taken: user command", self.pp("cmd", "new", "hello", "--user", code=2).stderr)
        # a plan of what the run would refuse is that refusal's status, and the file it named stays as it is
        was = self.read(path)
        self.pp("cmd", "new", "hello2", "--user", "-n", code=0)
        self.pp("cmd", "new", "hello", "--user", "-n", code=2)
        self.assertEqual(self.read(path), was)

    def test_names_that_are_taken_or_invalid(self):
        self.tool("pp-ext", "echo ext\n")
        self.write("home/.config/pp/commands/al.py", '"""A"""\nALIASES = ["alias1"]\ndef main(argv):\n    pass\n')
        for name, why in ((ANY, "builtin command"), ("help", "pp's own"), ("ext", "external command"),
                          ("alias1", "an alias of 'al'")):
            r = self.pp("cmd", "new", name, "--user", code=2)
            self.assertIn(why, r.stderr, name)
        for name in ("Foo-Bar", "1x", "_x", "x.y", "a b", ""):
            self.assertIn("lowercase letters", self.pp("cmd", "new", name, "--user", code=2).stderr, name)
        self.assertEqual(os.listdir(os.path.join(self.env["PP_CONFIG_DIR"], "commands")), ["al.py"])

    def test_a_builtin_in_a_source_tree_passes_its_own_test(self):
        src = copy_tree(os.path.join(self.tmp, "src"))
        pp = [sys.executable, os.path.join(src, "bin", "pp")]
        r = subprocess.run(pp + ["cmd", "new", "foo", "-s", "Foo things"], capture_output=True, text=True, env=self.env)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertTrue(os.path.isfile(os.path.join(src, "pp/commands/foo.py")))
        self.assertTrue(os.path.isfile(os.path.join(src, "tests/test_foo.py")))
        self.assertTrue(os.path.isfile(os.path.join(src, "tests/test_journey_foo.py")))
        # the unit test it writes enters the sandbox's scope, so a command that reads paths.state_dir() or
        # config.load() cannot write the contributor's own ~/.config/pp from it
        self.assertIn("sandbox.enter(self)", self.read(os.path.join(src, "tests/test_foo.py")))
        r = subprocess.run([sys.executable, "-m", "unittest", "tests.test_foo", "tests.test_journey_foo", "-q"],
                           capture_output=True, text=True, cwd=src, env=self.env, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("Ran 2 tests", r.stderr)
        r = subprocess.run(pp, capture_output=True, text=True, env=self.env)
        self.assertIn("Foo things", r.stdout.split("commands:")[1].split("\n\n")[0])     # listed as a built-in

    def test_from_a_release_it_writes_into_the_source_tree(self):
        """The promoted `pp` runs a snapshot that the next promote replaces: a command written there would vanish."""
        src = copy_tree(os.path.join(self.tmp, "src"))
        rel = copy_tree(os.path.join(self.tmp, "release"))
        meta = os.path.join(rel, ".pp-release.json")
        with open(meta, "w") as f:
            json.dump({"name": "pp", "source": src}, f)
        pp = [sys.executable, os.path.join(rel, "bin", "pp")]
        r = subprocess.run(pp + ["cmd", "new", "bar"], capture_output=True, text=True, env=self.env)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn(f"wrote {src}/pp/commands/bar.py", r.stdout)
        self.assertIn("try: ppd bar -h", r.stdout)
        self.assertFalse(os.path.exists(os.path.join(rel, "pp/commands/bar.py")))
        with open(meta, "w") as f:
            json.dump({"name": "pp", "source": os.path.join(self.tmp, "gone")}, f)
        r = subprocess.run(pp + ["cmd", "new", "baz"], capture_output=True, text=True, env=self.env)
        self.assertEqual(r.returncode, 2)
        self.assertIn("--user", r.stderr)
        self.assertFalse(os.path.exists(os.path.join(rel, "pp/commands/baz.py")))


class TestAlias(Journey):
    """`pp cmd alias`, the one place pp writes config.toml: one line of its `[alias]` table changes, and every
    other byte of the file is the person's and stays as it was."""

    KEPT = '# mine\n[http]\nport = 8080\n\n[alias]\nc = "cmd ls"   # short\n\n[batch]\n'

    def setUp(self):
        super().setUp()
        self.cfg = os.path.join(self.env["PP_CONFIG_DIR"], "config.toml")
        self.write("home/.config/pp/commands/zecho.py",
                   '"""Show argv"""\ndef main(argv):\n    print(" ".join(argv))\n    return 0\n')

    def bytes_of(self) -> bytes:
        with open(self.cfg, "rb") as f:
            return f.read()

    def test_a_line_lands_inside_the_table_and_comes_back_out_of_it(self):
        self.config(self.KEPT)
        was = self.bytes_of()
        self.pp("cmd", "alias", "ze", "zecho", "a b")
        self.assertEqual(self.bytes_of().replace(b'ze = "zecho \'a b\'"\n', b"", 1), was)
        self.assertEqual(self.pp("ze", "c").stdout, "a b c\n")
        self.pp("cmd", "alias", "ze", "--rm")
        self.assertEqual(self.bytes_of(), was)

    def test_a_file_with_no_table_gets_one_at_its_end_and_a_new_file_is_private(self):
        self.config("[http]\nport = 1\n")
        was = self.bytes_of()
        self.pp("cmd", "alias", "ze", "zecho")
        self.assertEqual(self.bytes_of(), was + b'\n[alias]\nze = "zecho"\n')
        os.chmod(self.cfg, 0o640)
        self.pp("cmd", "alias", "zf", "zecho")
        self.assertEqual(os.stat(self.cfg).st_mode & 0o777, 0o640)
        os.remove(self.cfg)
        self.pp("cmd", "alias", "ze", "zecho")
        self.assertEqual(self.bytes_of(), b'[alias]\nze = "zecho"\n')
        self.assertEqual(os.stat(self.cfg).st_mode & 0o777, 0o600)

    def test_a_line_lands_at_the_end_of_a_table_in_a_file_that_ends_without_a_newline(self):
        self.config('[http]\nport = 1\n\n[alias]\nc = "cmd ls"')
        self.pp("cmd", "alias", "ze", "zecho")
        self.assertEqual(self.bytes_of(), b'[http]\nport = 1\n\n[alias]\nc = "cmd ls"\nze = "zecho"\n')
        self.assertEqual(self.pp("ze", "hi").stdout, "hi\n")

    def test_a_file_of_no_bytes_is_left_for_the_writer_that_is_saving_it(self):
        self.config(self.KEPT)
        with open(self.cfg, "wb"):      # what a rewrite in place leaves while its writer still holds the file
            pass
        self.pp("cmd", "alias", "ze", "zecho", code=1)
        self.assertEqual(self.bytes_of(), b"")

    def test_a_dry_run_prints_the_change_and_writes_nothing(self):
        self.config(self.KEPT)
        was, stamp = self.bytes_of(), os.stat(self.cfg).st_mtime_ns
        self.pp("cmd", "alias", "qz", "-n", "zecho")
        self.pp("cmd", "alias", "c", "-n", "--rm")
        self.assertEqual((self.bytes_of(), os.stat(self.cfg).st_mtime_ns), (was, stamp))
        self.pp("qz", code=2)       # the name the plan printed names nothing

    def test_a_name_or_a_command_it_refuses_leaves_the_file_alone(self):
        self.config(self.KEPT)
        was = self.bytes_of()
        for args in ((ANY, "zecho"), ("help", "zecho"), ("Ze", "zecho"), ("z e", "zecho"),
                     ("ze", "nosuchthing"), ("c", "zecho"), ("nope", "--rm")):
            self.pp("cmd", "alias", *args, code=2)
        self.assertEqual(self.bytes_of(), was)

    def test_a_table_written_inline_is_named_rather_than_rewritten(self):
        self.config('# mine\nalias = { c = "cmd ls" }\n')
        was = self.bytes_of()
        self.pp("cmd", "alias", "ze", "-n", "zecho", code=2)        # the plan is the answer the run gives
        self.pp("cmd", "alias", "ze", "zecho", code=2)
        self.pp("cmd", "alias", "c", "--rm", code=2)
        self.assertEqual(self.bytes_of(), was)

    def test_every_alias_is_listed_with_what_it_runs_and_where_it_is_declared(self):
        self.write("home/.config/pp/commands/zal.py",
                   '"""Aliased"""\nALIASES = ["zq"]\ndef main(argv):\n    return 0\n')
        self.config('[alias]\nc = "cmd ls"\nbad = "nosuchthing"\nzecho = "cmd ls"\n')
        by = {(r["source"], r["name"]): r for r in json.loads(self.pp("cmd", "alias", "--json").stdout)}
        self.assertEqual(by[("config", "c")]["argv"], ["cmd", "ls"])
        self.assertEqual(by[("config", "c")]["path"], self.cfg)
        self.assertIsNone(by[("config", "c")]["error"])
        self.assertIsNotNone(by[("config", "bad")]["error"])
        self.assertIsNotNone(by[("config", "zecho")]["error"])      # a command's own name, so it is skipped
        self.assertEqual(self.pp("zecho", "hi").stdout, "hi\n")
        self.assertEqual(by[("command", "zq")]["argv"], ["zal"])
        self.assertEqual(by[("command", "zq")]["path"],
                         os.path.join(self.env["PP_CONFIG_DIR"], "commands", "zal.py"))
        ls = {r["name"]: r for r in json.loads(self.pp("cmd", "ls", "--json").stdout)}
        self.assertEqual((ls["zal"]["aliases"], ls["zal"]["config_aliases"]), (["zq"], []))
        self.assertEqual(ls["cmd"]["config_aliases"], ["c"])


class PluginJourney(Journey):
    def setUp(self):
        super().setUp()
        self.src = self.git_repo("work/tools", {"commands/hello.py": HELLO.format(v="one"), "README.md": "tools\n"})
        self.plugins = os.path.join(self.env["PP_DATA_DIR"], "plugins")

    def commit(self, rel: str, text: str, msg: str):
        self.write(os.path.join(self.src, rel), text)
        self.sh(f"git add -A && git commit -qm {msg}", cwd=self.src)

    def head(self, repo: str) -> str:
        return self.sh("git rev-parse HEAD", cwd=repo).strip()


class TestPlugins(PluginJourney):
    def test_add_shows_asks_then_pins(self):
        r = self.pp("cmd", "add", self.src, "-n")
        self.assertIn("hello        Say hello  (commands/hello.py, 4 lines)", r.stdout)
        self.assertIn("read it first", r.stdout)
        self.assertIn("(dry run: nothing changed)", r.stdout)
        self.assertIn('    |     print("hello one")', self.pp("cmd", "add", self.src, "-n", "--show").stdout)
        self.assertIn("pass -y", self.pp("cmd", "add", self.src, code=2).stderr)      # no terminal to ask in
        self.pp("hello", code=2)
        self.assertEqual(os.listdir(self.plugins), [])                               # no clone left behind
        r = self.pp("cmd", "add", self.src, "-y")
        commit = self.head(self.src)
        self.assertIn(f"added tools at {commit[:7]}: hello", r.stdout)
        self.assertEqual(self.pp("hello").stdout, "hello one\n")
        self.assertIn("Say hello", self.pp().stdout.split("plugins (")[1])
        row = next(x for x in json.loads(self.pp("cmd", "ls", "--json").stdout) if x["name"] == "hello")
        self.assertEqual((row["origin"], row["plugin"], row["commit"], row["url"]), ("plugin", "tools", commit, self.src))
        self.assertIn(f"tools at {commit[:7]} from {self.src}", self.pp("cmd", "ls").stdout)
        self.assertIn("is there already", self.pp("cmd", "add", self.src, "-y", code=2).stderr)

    def test_update_shows_the_change_and_moves_only_on_yes(self):
        self.pp("cmd", "add", self.src, "-y")
        self.assertIn("tools: up to date", self.pp("cmd", "update").stdout)
        self.commit("commands/hello.py", HELLO.format(v="two"), "two")
        r = self.pp("cmd", "update", "tools", "-n")
        self.assertIn("commands/hello.py | 2 +-", r.stdout)
        self.assertEqual(self.pp("hello").stdout, "hello one\n")
        r = self.pp("cmd", "update", "--show", code=2)                              # no terminal: the diff, then no
        self.assertIn('+    print("hello two")', r.stdout)
        self.assertEqual(self.pp("hello").stdout, "hello one\n")                     # the new code never ran
        self.assertIn(f"tools: now at {self.head(self.src)[:7]}", self.pp("cmd", "update", "-y").stdout)
        self.assertEqual(self.pp("hello").stdout, "hello two\n")

    def test_a_name_that_is_taken_is_refused(self):
        self.commit(f"commands/{ANY}.py", '"""Mine"""\ndef main(argv):\n    return 0\n', "clash")
        r = self.pp("cmd", "add", self.src, "-y", code=2)
        self.assertIn(f"{ANY}: builtin command", r.stderr)
        self.assertEqual(os.listdir(self.plugins), [])
        self.sh(f"git rm -q commands/{ANY}.py && git commit -qm unclash", cwd=self.src)
        self.pp("cmd", "add", self.src, "-y")
        self.write("home/.config/pp/commands/greet.py", '"""Mine"""\ndef main(argv):\n    print("mine")\n')
        self.commit("commands/greet.py", '"""Theirs"""\ndef main(argv):\n    print("theirs")\n', "greet")
        r = self.pp("cmd", "update", "-y", code=2)
        self.assertIn("greet: user command", r.stderr)
        self.assertIn("stays at", r.stderr)
        self.assertEqual(self.pp("greet").stdout, "mine\n")

    def test_a_tag_a_name_and_commands_at_the_top(self):
        flat = self.git_repo("work/flat", {"yo.py": '"""Yo"""\ndef main(argv):\n    print("yo")\n'})
        self.sh("git tag v1", cwd=flat)
        self.write(os.path.join(flat, "yo.py"), '"""Yo"""\ndef main(argv):\n    print("yo two")\n')
        self.sh("git commit -qam two", cwd=flat)
        self.pp("cmd", "add", flat, "--ref", "v1", "--name", "flatty", "-y")
        self.assertEqual(self.pp("yo").stdout, "yo\n")                    # the tag, not the newer commit
        self.assertIn("flatty: up to date", self.pp("cmd", "update", "flatty").stdout)
        empty = self.git_repo("work/empty", {"README.md": "nothing\n"})
        self.assertIn("has no command files", self.pp("cmd", "add", empty, "-y", code=2).stderr)
        self.assertIn("cannot name a plugin", self.pp("cmd", "add", flat, "--name", "../x", "-y", code=2).stderr)
        self.assertIn("no plugin named nope", self.pp("cmd", "update", "nope", code=2).stderr)

    def test_rm_shows_then_removes(self):
        self.assertIn("no plugins", self.pp("cmd", "update").stdout)
        self.pp("cmd", "add", self.src, "-y")
        self.assertIn("removes hello", self.pp("cmd", "rm", "tools", "-n").stdout)
        self.pp("cmd", "rm", "tools", code=2)                               # no terminal
        self.assertEqual(self.pp("hello").stdout, "hello one\n")
        self.assertIn("removed tools", self.pp("cmd", "rm", "tools", "-y").stdout)
        self.pp("hello", code=2)
        self.assertIn("no plugin named tools", self.pp("cmd", "rm", "tools", "-y", code=2).stderr)



class TestUpdatingSeveral(PluginJourney):
    def setUp(self):
        super().setUp()
        self.flat = self.git_repo("work/flat", {"yo.py": '"""Yo"""\ndef main(argv):\n    print("yo")\n'})
        self.pp("cmd", "add", self.src, "-y")
        self.pp("cmd", "add", self.flat, "-y")
        self.write(os.path.join(self.flat, "yo.py"), '"""Yo"""\ndef main(argv):\n    print("yo two")\n')
        self.sh("git commit -qam two", cwd=self.flat)

    def test_one_unreachable_plugin_does_not_stop_the_others(self):
        os.rename(self.src, self.src + "-gone")
        r = self.pp("cmd", "update", "-y", code=1)
        self.assertIn(f"tools: could not fetch from {self.src}; it stays at", r.stderr)
        self.assertIn("flat: now at", r.stdout)
        self.assertEqual(self.pp("yo").stdout, "yo two\n")

    def test_local_changes_in_a_clone_refuse_in_the_preview_too(self):
        self.assertEqual(self.pp("hello").stdout, "hello one\n")         # leaves __pycache__: not a change
        self.commit("commands/hello.py", HELLO.format(v="two"), "two")
        self.write(os.path.join(self.plugins, "tools", "commands", "hello.py"), HELLO.format(v="mine"))
        for flags in (("-n",), ("-y",)):
            r = self.pp("cmd", "update", "tools", *flags, code=2)
            self.assertIn("has local changes, so it stays at", r.stderr)
        self.assertEqual(self.pp("hello").stdout, "hello mine\n")

    def test_the_dry_run_footer_comes_once_at_the_end(self):
        self.commit("commands/hello.py", HELLO.format(v="two"), "two")
        out = self.pp("cmd", "update", "-n").stdout
        self.assertEqual(out.count("(dry run: nothing changed)"), 1)
        self.assertTrue(out.rstrip().endswith("(dry run: nothing changed)"), out)


class TestNamesAndSources(PluginJourney):
    def test_any_plugin_name_and_a_folder_that_is_not_a_repository(self):
        self.pp("cmd", "add", self.src, "--name", "index.json", "-y")      # pp's own registry is .index.json
        self.assertEqual(self.pp("hello").stdout, "hello one\n")
        self.assertIn("index.json at", self.pp("cmd", "ls").stdout)
        plain = os.path.join(self.work, "plain")
        os.makedirs(os.path.join(plain, "commands"))
        r = self.pp("cmd", "add", plain, "-y", code=2)
        self.assertIn("is not a git repository", r.stderr)

    def test_ls_shows_the_ref_a_plugin_follows(self):
        self.sh("git tag v1", cwd=self.src)
        self.pp("cmd", "add", self.src, "--ref", "v1", "-y")
        self.assertIn("(v1) from", self.pp("cmd", "ls").stdout)
        row = next(x for x in json.loads(self.pp("cmd", "ls", "--json").stdout) if x["name"] == "hello")
        self.assertEqual(row["ref"], "v1")
        self.assertEqual(row["aliases"], [])
        # a command's short names are its own row's, so a reader needs no second command to find them
        self.write("home/.config/pp/commands/quote.py", '"""Quote"""\nALIASES = ["qt"]\ndef main(argv):\n    pass\n')
        rows = {x["name"]: x["aliases"] for x in json.loads(self.pp("cmd", "ls", "--json").stdout)}
        self.assertEqual(rows["quote"], ["qt"])


class TestWhatGitCanAsk(PluginJourney):
    """A remote over ssh asks for a key passphrase or a host key on /dev/tty, and a process in a session of its
    own has no terminal to be asked on. The clone and the fetch therefore stay in pp's session; the reads and
    the checkout, which ask nothing, run in a session of their own, so pp can end them itself."""

    def shim(self) -> str:
        """A git first on PATH that records the verb and the session it ran in, then runs the real git. Field 6
        of /proc/<pid>/stat is the session id."""
        log = os.path.join(self.tmp, "sessions.log")
        real = shutil.which("git")
        self.tool("git", f"""echo "$1 $(cut -d' ' -f6 /proc/self/stat)" >> {log}\nexec {real} "$@"\n""")
        return log

    def test_the_clone_and_the_fetch_run_where_a_question_can_be_answered(self):
        log = self.shim()
        self.pp("cmd", "add", self.src, "-y")
        self.commit("commands/hello.py", HELLO.format(v="two"), "two")
        self.pp("cmd", "update", "-y")
        ours = str(os.getsid(0))        # pp runs in this test's session, so this is pp's own
        where: dict[str, set[bool]] = {}
        for line in self.read(log).splitlines():
            verb, sid = line.split()
            where.setdefault(verb, set()).add(sid == ours)
        for verb in ("clone", "fetch"):
            self.assertEqual(where[verb], {True}, f"{verb} ran where nothing can be asked")
        for verb in ("checkout", "ls-tree"):
            self.assertEqual(where[verb], {False}, f"{verb} ran in pp's own session")


class TestReadingAPlugin(PluginJourney):
    def test_a_commit_subject_cannot_write_lines_onto_the_screen_that_asks(self):
        """The commit line is git's output, so its subject is the repository author's text. A carriage return
        in it reaches pp as a newline, and a line of its own on that screen could name a command the plugin
        does not add, or hide the ones it does."""
        forged = ("tidy up\radds\r  harmless     A tiny helper  (harmless.py, 4 lines)\r"
                  "\x1b[33mits code runs as you: read it first\x1b[0m\x1b[8m")
        msg = self.write("work/subject.txt", forged)
        self.commit("commands/sly.py", '"""Steals your keys"""\ndef main(argv):\n    return 0\n', "sly")
        self.sh(f"git commit -q --amend -F {msg}", cwd=self.src)
        r = self.pp("cmd", "add", self.src, "-n")
        self.assertNotIn("\x1b", r.stdout)                        # no mode the rest of the screen is drawn in
        self.assertEqual(r.stdout.count("\nadds\n"), 1)           # one adds block, and pp wrote it
        self.assertNotIn("harmless", r.stdout.splitlines()[-1])   # the forged line is inside the commit line
        self.assertIn("sly          Steals your keys", r.stdout)  # what it really adds is still on the screen

    def test_a_file_name_of_a_plugins_is_escaped_in_every_listing(self):
        """A command file's name is the plugin author's text too, and it reaches `pp -h` and `pp cmd ls` long
        after the screen that asked about it."""
        self.commit("commands/zb\x1b[31mad.py", '"""Fine one"""\ndef main(argv):\n    return 0\n', "named")
        self.pp("cmd", "add", self.src, "-y")
        for argv in ((), ("-h", "--all"), ("cmd", "ls")):
            out = self.pp(*argv).stdout
            self.assertNotIn("\x1b", out, argv)
            self.assertIn("zb\\x1b[31mad", out, argv)

    def test_a_plugin_whose_file_name_is_not_valid_utf8_is_listed_and_removable(self):
        """os.listdir hands such a name back with a lone surrogate per byte it could not read, and a UTF-8
        locale's stdout refuses to encode one. A plugin could bring one, and then `pp -h`, `pp cmd ls` and
        `pp cmd rm` all raised, so it could be neither read about nor taken off."""
        self.commit(os.fsdecode(b"commands/ztool\xff.py"),
                    '"""Odd name"""\ndef main(argv):\n    return 0\n', "odd")
        env = dict(self.env, PYTHONIOENCODING="utf-8")      # what a UTF-8 locale gives a child: errors strict
        self.pp("cmd", "add", self.src, "-y", env=env)
        for argv in ((), ("-h", "--all"), ("cmd", "ls")):
            out = self.pp(*argv, env=env).stdout
            self.assertIn("ztool\\udcff", out, argv)
        self.assertIn("ztool\\udcff", self.pp("cmd", "rm", "tools", "-y", env=env).stdout)
        self.assertEqual(json.loads(self.read(os.path.join(self.plugins, ".index.json"))), {})

    def test_what_a_remote_says_cannot_write_lines_on_the_screen_that_asks(self):
        """git relays the stderr of the helper it talks to a remote through, so an ssh banner is the remote's
        own text, and it arrives before there is anything to say yes to. pp names it in the refusal."""
        self.tool("fakessh", 'printf "\\033[31m\\033[2Jpp: added plugin trusted at 1a2b3c4\\033[0m\\n" >&2\nexit 255\n')
        r = self.pp("cmd", "add", "git@localhost:some/repo.git", "--name", "sshx", code=1,
                    env=dict(self.env, GIT_SSH_COMMAND=os.path.join(self.bin, "fakessh")))
        self.assertNotIn("\x1b", r.stderr)
        self.assertIn("\\x1b[31m\\x1b[2Jpp: added plugin trusted at 1a2b3c4", r.stderr)
        self.assertEqual(os.listdir(self.plugins), [])

    def test_what_a_remote_says_of_several_lines_stays_several_lines(self):
        """git says four lines whenever a clone finds no key, which is the commonest way `pp cmd add` fails.
        They are the lines pp names them in, each one a line pp wrote, so a `pp: ` among them is indented
        under pp's own and still forges nothing."""
        self.tool("fakessh", 'printf "Permission denied (publickey).\\nfatal: Could not read from remote '
                  'repository.\\n\\npp: added plugin trusted\\n" >&2\nexit 255\n')
        r = self.pp("cmd", "add", "git@localhost:some/repo.git", "--name", "sshy", code=1,
                    env=dict(self.env, GIT_SSH_COMMAND=os.path.join(self.bin, "fakessh")))
        self.assertNotIn("\\n", r.stderr)                 # not one line with its newlines written out
        lines = r.stderr.splitlines()
        self.assertEqual(lines[0], "pp: git clone --quiet: Permission denied (publickey).")
        self.assertIn("  fatal: Could not read from remote repository.", lines)
        self.assertIn("  pp: added plugin trusted", lines)
        self.assertEqual(os.listdir(self.plugins), [])

    def test_an_escape_sequence_in_a_plugin_cannot_redraw_the_review(self):
        self.commit("commands/sly.py", '"""Looks fine\x1b[2K"""\n# \x1b[1A\x1b[2K hidden\ndef main(argv):\n    pass\n', "sly")
        r = self.pp("cmd", "add", self.src, "-n", "--show")
        self.assertNotIn("\x1b", r.stdout)
        self.assertIn("    | # \\x1b[1A\\x1b[2K hidden", r.stdout)
        self.pp("cmd", "add", self.src, "-y")
        out = self.pp().stdout
        self.assertNotIn("\x1b", out)
        self.assertIn("Looks fine\\x1b[2K", out)

if __name__ == "__main__":
    unittest.main()
