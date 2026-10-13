"""Discovery and dispatch: help never imports commands, one broken command never breaks another."""
import ast
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PP = os.path.join(ROOT, "bin", "pp")


@unittest.skipUnless(all(os.path.exists(os.path.join(ROOT, "pp", "commands", f"{c}.py")) for c in ("kill", "cmd")),
                     "uses pp kill and pp cmd, which this tree does not have")
class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.env = {**os.environ, "PP_CONFIG_DIR": f"{t}/config", "PP_STATE_DIR": f"{t}/state",
                    "PP_DATA_DIR": f"{t}/data", "PP_PATH": f"{t}/plug", "NO_COLOR": "1"}
        os.makedirs(f"{t}/config/commands")
        os.makedirs(f"{t}/plug")
        os.makedirs(f"{t}/bin")
        self.env["PATH"] = f"{t}/bin:" + os.environ["PATH"]

    def write(self, rel, text, mode=0o644):
        path = os.path.join(self.tmp.name, rel)
        with open(path, "w") as f:
            f.write(text)
        os.chmod(path, mode)

    def pp(self, *args):
        return subprocess.run([sys.executable, PP, *args], capture_output=True, text=True, env=self.env, timeout=30)

    def test_help_lists_builtins_and_plugins(self):
        self.write("config/commands/hello.py", '"""Say hello"""\ndef main(argv):\n    print("hi", *argv)\n')
        self.write("bin/pp-ext", "#!/bin/sh\necho ext \"$@\"\n", 0o755)
        r = self.pp()
        self.assertEqual(r.returncode, 0)
        for word in ("kill", "cmd", "hello", "Say hello", "ext"):
            self.assertIn(word, r.stdout)
        self.assertEqual(self.pp("hello", "a").stdout.strip(), "hi a")
        self.assertEqual(self.pp("ext", "b").stdout.strip(), "ext b")

    def test_broken_commands_are_isolated(self):
        self.write("plug/syntax.py", '"""Broken"""\ndef main(argv:\n')
        self.write("plug/badimport.py", '"""Needs a missing dep"""\nimport no_such_module_xyz\ndef main(argv):\n    pass\n')
        self.write("plug/nomain.py", '"""No main"""\n')
        r = self.pp()
        self.assertEqual(r.returncode, 0)
        self.assertIn("broken", r.stdout)
        self.assertIn("syntax", r.stdout)
        self.assertIn("badimport", r.stdout)     # parse fine, so listed normally; fails only when run
        r = self.pp("badimport")
        self.assertEqual(r.returncode, 1)
        self.assertIn("failed to load", r.stderr)
        self.assertEqual(self.pp("kill", "-h").returncode, 0)

    def test_builtin_wins_over_plugin(self):
        self.write("plug/kill.py", '"""Impostor"""\ndef main(argv):\n    print("impostor")\n')
        r = self.pp("kill", "-h")
        self.assertNotIn("impostor", r.stdout)
        self.assertIn("shadows", self.pp("-h", "--all").stdout)

    def test_prefix_alias_and_typo(self):
        self.write("plug/zeta.py", '"""Z"""\nALIASES = ["zz"]\ndef main(argv):\n    print("zeta")\n')
        self.assertEqual(self.pp("zet").stdout.strip(), "zeta")
        self.assertEqual(self.pp("zz").stdout.strip(), "zeta")
        r = self.pp("zeat")
        self.assertEqual(r.returncode, 2)
        self.assertIn("did you mean zeta", r.stderr)
        self.write("plug/zebra.py", '"""Z2"""\ndef main(argv):\n    pass\n')
        r = self.pp("ze")
        self.assertEqual(r.returncode, 2)
        self.assertIn("ambiguous", r.stderr)

    def test_a_file_name_cannot_redraw_a_listing(self):
        """A command file's name and its path are whoever wrote the file's text, and git carries any byte but
        NUL and /. CONTRIBUTING.md: such text goes through ui.printable, in every listing that prints it."""
        esc = "zbad\x1b[31mRED"
        self.write(f"plug/{esc}.py", '"""Fine one"""\ndef main(argv):\n    pass\n')
        self.write(f"plug/{esc}2.py", '"""No main here"""\n')          # the broken table prints its own rows
        for argv in ((), ("-h", "--all"), ("cmd", "ls")):
            out = self.pp(*argv).stdout
            self.assertNotIn("\x1b", out, argv)
            self.assertIn("zbad\\x1b[31mRED", out, argv)

    def test_a_file_name_cannot_redraw_a_refusal_either(self):
        """A command file's name and its path are whoever wrote the file's text, and the listings escape them.
        The same names reach pp's own messages: an ambiguous prefix lists them, a typo is answered with the
        closest one, a file pp cannot parse is named by its name, its path and python's words, and argparse
        names a word pp was handed, which a glob fills with the names of the files in a directory."""
        esc = "zbad\x1b[31mRED"
        self.write(f"plug/{esc}.py", '"""Fine one"""\ndef main(argv):\n    pass\n')
        self.write(f"plug/{esc}2.py", '"""Also fine"""\ndef main(argv):\n    pass\n')
        self.write("plug/zalias.py", '"""Aliased"""\nALIASES = ["zq\x1b[35mAL"]\ndef main(argv):\n    pass\n')
        self.write("plug/zbroke\x1b[32mX.py", "def main(argv:\n")
        for argv, code, says in ((("zbad",), 2, "is ambiguous"), (("zq\x1b[35mA",), 2, "did you mean"),
                                 (("zbroke\x1b[32mX",), 1, "is broken"),
                                 # argparse writes this one, and a glob is where such a word comes from
                                 (("cmd", "ls", f"{esc}.txt"), 2, "unrecognized arguments")):
            r = self.pp(*argv)
            self.assertEqual(r.returncode, code, r.stderr)
            self.assertIn(says, r.stderr, argv)
            self.assertNotIn("\x1b", r.stderr, argv)
            self.assertIn("\\x1b[3", r.stderr, argv)

    def test_a_file_name_that_is_not_valid_utf8_breaks_no_listing(self):
        """Such a name reaches pp as lone surrogates, which a UTF-8 locale's stdout refuses to encode: every
        listing that printed one raised, `pp cmd ls` and `pp cmd rm` among them, so a plugin that brought one
        could not be listed or removed. It is listed escaped instead, like any other name from elsewhere."""
        self.env["PYTHONIOENCODING"] = "utf-8"          # what a UTF-8 locale gives a child: errors strict
        name = os.fsdecode(b"ztool\xff")
        self.write(f"plug/{name}.py", '"""Odd name"""\ndef main(argv):\n    return 0\n')
        for argv in ((), ("-h", "--all"), ("cmd", "ls")):
            r = self.pp(*argv)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn("ztool\\udcff", r.stdout, argv)

    def test_help_subcommand(self):
        r = self.pp("help", "kill")
        self.assertEqual(r.returncode, 0)
        self.assertIn("pkill", r.stdout)

    def test_bad_config_is_loud(self):
        self.write("config/config.toml", "[http\nport = 1\n")
        self.write("plug/cfg.py", '"""C"""\nfrom pp.lib import config\ndef main(argv):\n    config.load("http")\n')
        r = self.pp("cfg")
        self.assertEqual(r.returncode, 1)
        self.assertIn("config.toml", r.stderr)

    def test_new_user_command(self):
        r = self.pp("cmd", "new", "mytool", "--user", "-s", "My tool")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("My tool", self.pp().stdout)
        self.assertEqual(self.pp("mytool", "x").stdout.strip(), "x")

    def test_a_changed_command_runs_its_new_code(self):
        """Even at the same size and mtime, which is all a cached .pyc is checked by (a plugin updated in the
        second it last ran)."""
        path = os.path.join(self.tmp.name, "config/commands/hello.py")
        self.write("config/commands/hello.py", '"""Say hello"""\ndef main(argv):\n    print("one")\n')
        self.assertEqual(self.pp("hello").stdout, "one\n")
        then = os.stat(path).st_mtime_ns
        self.write("config/commands/hello.py", '"""Say hello"""\ndef main(argv):\n    print("two")\n')
        os.utime(path, ns=(then, then))
        self.assertEqual(self.pp("hello").stdout, "two\n")
        self.assertFalse(os.path.exists(os.path.join(self.tmp.name, "config/commands/__pycache__")))


def module_level_imports(node):
    """The imports that run when the module is imported: everything outside a function body."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.Import, ast.ImportFrom)):
            yield child
        elif not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            yield from module_level_imports(child)


class TestStdlibOnly(unittest.TestCase):
    def test_module_level_imports_are_stdlib_or_pp(self):
        """pp has no dependencies: a third-party import goes inside the function that needs it (CONTRIBUTING.md)."""
        files = [os.path.join(ROOT, "bin", "pp")]
        files += [os.path.join(d, f) for d, _, fs in os.walk(os.path.join(ROOT, "pp")) for f in fs if f.endswith(".py")]
        bad = []
        for path in files:
            with open(path) as f:
                tree = ast.parse(f.read(), path)
            for node in module_level_imports(tree):
                names = [a.name for a in node.names] if isinstance(node, ast.Import) else \
                    [node.module] if node.level == 0 else []
                bad += [f"{os.path.relpath(path, ROOT)}:{node.lineno} {n}" for n in names
                        if n.split(".")[0] not in sys.stdlib_module_names and n.split(".")[0] != "pp"]
        self.assertGreater(len(files), 10)
        self.assertEqual(bad, [])



class TestPackage(unittest.TestCase):
    def test_pyproject_says_what_pp_says(self):
        """PyPI's ppkit: its version is pp's, and its `pp` command is pp.cli.main."""
        from pp import __version__, cli
        with open(os.path.join(ROOT, "pyproject.toml"), "rb") as f:
            project = tomllib.load(f)["project"]
        self.assertEqual((project["name"], project["version"]), ("ppkit", __version__))
        self.assertEqual(project["scripts"], {"pp": "pp.cli:main"})
        self.assertTrue(callable(cli.main))
        self.assertEqual(project["dependencies"], [])

    def test_the_changelog_has_a_section_for_the_next_release(self):
        from pp import __version__
        with open(os.path.join(ROOT, "CHANGELOG.md")) as f:
            heads = [line.strip() for line in f if line.startswith("## ")]
        self.assertTrue({"## Unreleased", f"## v{__version__}"} & set(heads), heads)

    def test_one_tagline(self):
        """The tagline is pp.TAGLINE; the package docstring, pyproject and the README say the same words."""
        import pp
        said = pp.TAGLINE[0].upper() + pp.TAGLINE[1:]
        with open(os.path.join(ROOT, "pyproject.toml"), "rb") as f:
            self.assertEqual(tomllib.load(f)["project"]["description"], said)
        with open(os.path.join(ROOT, "README.md")) as f:
            self.assertIn(f"\n{said}.\n", f.read())
        self.assertIn(pp.TAGLINE, pp.__doc__)

if __name__ == "__main__":
    unittest.main()
