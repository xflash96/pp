"""User journeys through `pp promote`: a tool's first release, the gate, uncommitted work, a failed activation and
the rollbacks after it, links in the way, Ctrl-C mid-check, and pp releasing itself. Each journey runs the real
CLI in a sandboxed HOME, asserting what the user reads and what lands on disk."""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from journey import PP, ROOT, Journey  # noqa: E402

TOOL = "#!/bin/sh\necho tool {v}\n"


class PromoteJourney(Journey):
    def setUp(self):
        super().setUp()
        self.releases = f"{self.home}/.local/share/pp/releases"
        self.binl = f"{self.home}/.local/bin"

    def tool_repo(self, promote: str, files: dict | None = None) -> str:
        """A git repo `tool` with bin/tool printing v1 and the given [promote] body."""
        self.src = self.git_repo("work/tool", {"bin/tool": TOOL.format(v="v1"), ".pp.toml": "[promote]\n" + promote,
                                               **(files or {})})
        return self.src

    def commit(self, rel: str, text: str, msg: str = "change"):
        self.write(os.path.join(self.src, rel), text, 0o755 if text.startswith("#!") else 0o644)
        self.sh(f"git add -A && git commit -qm {msg}", cwd=self.src)

    def current(self, name: str = "tool") -> str | None:
        try:
            return os.readlink(f"{self.releases}/{name}/current")
        except OSError:
            return None

    def run_link(self, name: str) -> str:
        return self.sh(f"{self.binl}/{name}").strip()

    def tree(self) -> list[str]:
        return sorted(os.path.relpath(os.path.join(d, n), self.home)
                      for d, ds, fs in os.walk(self.home) for n in ds + fs)

    def promote(self, *args, code=0, cwd=None, **kw):
        return self.pp("promote", *args, code=code, cwd=cwd or self.src, **kw)

    def logs_say(self, text: str) -> bool:
        logs = f"{self.home}/.local/state/pp/promote/logs"
        for f in os.listdir(logs) if os.path.isdir(logs) else []:
            if text in self.read(os.path.join(logs, f)):
                return True
        return False


class TestFirstRelease(PromoteJourney):
    def test_init_preflight_release_and_already_current(self):
        src = self.tool_repo("")
        os.remove(f"{src}/.pp.toml")
        self.sh("git commit -qam 'no config'", cwd=src)
        r = self.promote(code=2)
        self.assertIn("`pp promote init` writes one", r.stderr)

        r = self.promote("init")
        self.assertIn("edit it, then `pp promote -n`", r.stdout)
        text = self.read(f"{src}/.pp.toml")
        self.assertIn('source = "git"', text)
        self.assertIn('# links = { "~/.local/bin/tool" = "bin/tool" }', text)
        self.assertEqual(self.promote("init", code=2).stderr.strip(), f"pp: {src}/.pp.toml already has a [promote] table")
        with open(f"{src}/.pp.toml", "w") as f:            # what the user does next: uncomment the links
            f.write(text.replace("# links = ", "links = ").replace("# dev_links = ", "dev_links = "))
        self.sh("git add -A && git commit -qm config", cwd=src)

        before = self.tree()
        r = self.promote("-n")
        self.assertIn("preflight: clear", r.stdout)
        def same(out):      # the `new` line names the release by the second it is previewed in
            return re.sub(r"\b\d{8}-\d{6}-", "STAMP-", out)
        self.assertEqual(same(self.promote("-n", src, cwd=self.work).stdout), same(r.stdout))   # -n PATH, elsewhere
        self.assertIn(f"{self.binl}/tool -> {self.releases}/tool/current/bin/tool  [create]", r.stdout)
        self.assertIn("check     - (nothing gates this release)", r.stdout)
        self.assertEqual(self.tree(), before, "-n must change nothing")

        r = self.promote()
        stamp = self.current()
        self.assertRegex(stamp, r"^\d{8}-\d{6}-[0-9a-f]{7}$")
        self.assertIn(f"promoted tool -> {stamp}", r.stdout)
        self.assertEqual(self.run_link("tool"), "tool v1")             # prod: the release
        self.assertEqual(os.readlink(f"{self.binl}/toold"), f"{src}/bin/tool")   # dev: the working tree
        self.write(f"{src}/bin/tool", TOOL.format(v="dev"), 0o755)
        self.assertEqual(self.run_link("tool"), "tool v1")             # an edit never reaches prod
        self.assertEqual(self.run_link("toold"), "tool dev")
        self.sh("git checkout -q bin/tool", cwd=src)

        r = self.promote()
        self.assertIn("already current", r.stdout)
        self.assertEqual(self.current(), stamp)
        [row] = json.loads(self.pp("promote", "status", "--json").stdout)
        self.assertEqual((row["name"], row["current"], row["ahead"], row["dirty"], row["links_ok"]),
                         ("tool", stamp, 0, False, True))


class TestGate(PromoteJourney):
    def test_a_failing_check_keeps_prod_and_says_where_the_output_is(self):
        self.tool_repo('check = "./test.sh"\nlinks = { "~/.local/bin/tool" = "bin/tool" }\n',
                             {"test.sh": "#!/bin/sh\n./bin/tool | grep -q v1 || { echo 'FAIL: wanted v1'; exit 1; }\n"})
        self.promote()
        good = self.current()
        self.commit("bin/tool", TOOL.format(v="v2"), "v2")

        r = self.promote(code=1)
        self.assertIn("FAIL: wanted v1", r.stdout)
        self.assertIn(f"NOT PROMOTED: check failed; current stays {good}", r.stdout)
        log = r.stdout.strip().splitlines()[-1].removeprefix("full output: ")
        self.assertIn("FAIL: wanted v1", self.read(log))
        self.assertEqual(self.current(), good)
        self.assertEqual(self.run_link("tool"), "tool v1")
        self.assertFalse([n for n in os.listdir(f"{self.releases}/tool") if n.startswith(".staging")])
        self.assertIn("+1", self.pp("promote", "status").stdout)

        r = self.promote("--keep-failed", code=1)
        kept = [n for n in os.listdir(f"{self.releases}/tool") if n.startswith(".staging")]
        self.assertEqual(len(kept), 1)
        self.assertIn(f"kept the failed staging dir: {self.releases}/tool/{kept[0]}", r.stdout)

    def test_ctrl_c_during_the_check_cleans_up(self):
        src = self.tool_repo('check = "echo checking; sleep 30"\n')
        p = self.child([sys.executable, PP, "promote"], cwd=src, group="own", stderr=subprocess.STDOUT)
        staging = lambda: [n for n in os.listdir(f"{self.releases}/tool") if n.startswith(".staging")] \
            if os.path.isdir(f"{self.releases}/tool") else []
        self.assertTrue(self.until(staging, 15))
        self.assertTrue(self.until(lambda: self.logs_say("checking"), 15))
        p.signal_group(signal.SIGINT)                    # what Ctrl-C does: the whole foreground group
        out, _ = p.communicate(timeout=20)
        self.assertEqual(p.returncode, 130, out)
        self.assertIn("NOT PROMOTED: interrupted; current stays (none)", out)
        self.assertEqual(staging(), [])
        self.assertIsNone(self.current())


class TestUncommittedWork(PromoteJourney):
    def test_tracked_edits_refuse_untracked_files_are_named_dirty_takes_the_tree(self):
        src = self.tool_repo('links = { "~/.local/bin/tool" = "bin/tool" }\n')
        self.promote()
        first = self.current()

        self.write(f"{src}/bin/tool", TOOL.format(v="edited"), 0o755)
        before = self.tree()
        r = self.promote("-n", code=2)                  # something checked first is wrong: a refusal
        self.assertIn("1 tracked file(s) modified; commit, or --dirty", r.stdout)
        r = self.promote(code=2)
        self.assertIn("REFUSED", r.stderr)
        self.assertEqual(self.tree(), before)
        self.assertIn("dirty", self.pp("promote", "status").stdout)
        self.sh("git checkout -q bin/tool", cwd=src)

        self.write(f"{src}/bin/helper", "#!/bin/sh\necho helper\n", 0o755)  # new, never `git add`ed
        r = self.promote()
        self.assertIn("already current", r.stdout)
        self.assertIn("note: 1 untracked file(s) are not in HEAD, so not in the release: bin/helper", r.stdout)
        self.assertIn("bin/helper", self.promote("-n").stdout)

        r = self.promote("--dirty")
        dirty = self.current()
        self.assertTrue(dirty.endswith("-dirty"), dirty)
        self.assertTrue(os.path.exists(f"{self.releases}/tool/current/bin/helper"))
        self.assertNotIn("untracked", r.stdout)
        self.assertIn("*", self.pp("promote", "status").stdout.splitlines()[1])   # REV marks a dirty release

        os.remove(f"{src}/bin/helper")
        self.promote()                                   # same HEAD, but current is dirty: a clean release
        self.assertNotIn(self.current(), (first, dirty))
        self.assertFalse(os.path.exists(f"{self.releases}/tool/current/bin/helper"))


    def test_a_commit_subject_and_a_file_name_from_git_cannot_redraw_the_terminal(self):
        """CONTRIBUTING: text from elsewhere, git's output among it, is printable before it is printed. A
        commit subject and an untracked file name both come from git and both reach the screen, so an escape
        sequence in either would clear the terminal around what the user is reading."""
        src = self.tool_repo("")
        self.promote()
        evil = f"{src}/evil\x1b[2Jname.txt"
        self.write(evil, "x")                           # untracked, so it is named in the note
        for r in (self.promote(), self.promote("-n")):
            self.assertIn("evil\\x1b[2Jname.txt", r.stdout)
            self.assertNotIn("\x1b", r.stdout)
        os.remove(evil)

        msg = self.write("work/msg", "fix \x1b[31mRED\x1b[0m and \x1b[2J wipe\n")
        self.write(f"{src}/bin/tool", TOOL.format(v="v2"), 0o755)
        self.sh(f"git add -A && git commit -q -F {msg}", cwd=src)
        r = self.pp("promote", "diff", "tool")
        self.assertIn("fix \\x1b[31mRED\\x1b[0m and \\x1b[2J wipe", r.stdout)
        self.assertNotIn("\x1b", r.stdout)


class TestActivation(PromoteJourney):
    def test_failed_after_rolls_back_and_rollback_never_lands_on_it(self):
        self.tool_repo('after = "test ! -e $HOME/break-after"\nlinks = { "~/.local/bin/tool" = "bin/tool" }\n')
        self.promote()
        v1 = self.current()
        self.commit("bin/tool", TOOL.format(v="v2"), "v2")
        open(f"{self.home}/break-after", "w").close()
        r = self.promote(code=1)
        self.assertIn(f"after failed: rolled back current -> {v1}", r.stdout)
        self.assertIn(f"`after` failed for {v1} too", r.stdout)     # the rollback's own activation failed
        self.assertIn("(rollback skips it)", r.stdout)
        self.assertEqual(self.current(), v1)
        self.assertEqual(self.run_link("tool"), "tool v1")
        failed = [s for s in os.listdir(f"{self.releases}/tool") if s not in (v1, "current", "revisions.log", ".lock")]
        self.assertEqual(len(failed), 1)
        self.assertIn("failed (after)", self.pp("promote", "ls", "tool").stdout)

        os.remove(f"{self.home}/break-after")
        self.promote()                                   # HEAD is not current's rev: no --again needed
        v2 = self.current()
        self.assertEqual(self.run_link("tool"), "tool v2")

        self.assertEqual(self.pp("promote", "rollback", "tool", code=2, input="").returncode, 2)   # no tty: asks -y
        self.assertEqual(self.current(), v2)
        r = self.pp("promote", "rollback", "tool", "-y")
        self.assertIn(f"tool: current {v2} -> {v1}", r.stdout)       # past the failed release, to the last good one
        self.assertIn(f"pp promote rollback tool --to {v2}", r.stdout)
        self.assertEqual(self.run_link("tool"), "tool v1")
        self.assertIn(f"no release older than {v1}", self.pp("promote", "rollback", "tool", "-y", code=1).stderr)

        self.pp("promote", "rollback", "tool", "--to", v2[:-2], code=2)          # a prefix matching v2 and failed
        self.pp("promote", "rollback", "tool", "--to", v2, "-y")
        self.assertEqual(self.run_link("tool"), "tool v2")
        log = self.read(f"{self.releases}/tool/revisions.log")
        self.assertIn("(auto: after failed)", log)
        self.assertEqual(log.count("(manual)"), 2)


class TestLinks(PromoteJourney):
    def test_what_stands_in_a_links_way(self):
        src = self.tool_repo('links = { "~/.local/bin/tool" = "bin/tool" }\ndev_links = { "~/.local/bin/toold" = "bin/tool" }\n')
        self.write(f"{self.binl}/tool", "#!/bin/sh\necho hand-installed\n", 0o755)
        os.symlink("/bin/true", f"{self.binl}/toold")
        r = self.promote("-n", code=2)
        self.assertIn(f"{self.binl}/tool is a regular file; pass --force-links", r.stdout)
        self.assertIn(f"{self.binl}/toold is a symlink to /bin/true, not pp's; pass --force-links", r.stdout)
        self.promote(code=2)
        self.assertIsNone(self.current())

        r = self.promote("--force-links", code=2, input="")             # no tty: names what it would replace
        self.assertIn(f"will replace {self.binl}/tool (a regular file)", r.stdout)
        self.assertEqual(self.run_link("tool"), "hand-installed")
        self.promote("--force-links", "-y")
        self.assertEqual(self.run_link("tool"), "tool v1")
        self.assertEqual(os.readlink(f"{self.binl}/toold"), f"{src}/bin/tool")

        os.remove(f"{self.binl}/tool")
        self.assertIn(f"missing {self.binl}/tool", self.pp("promote", "status").stdout)
        self.assertIn("2 link(s) in place", self.pp("promote", "link", "tool").stdout)
        self.assertEqual(self.run_link("tool"), "tool v1")

        os.remove(f"{self.binl}/toold")
        os.mkdir(f"{self.binl}/toold")
        r = self.pp("promote", "link", "tool", "--force-links", "-y", code=2)
        self.assertIn("a directory (never replaced, even with --force-links)", r.stderr)


class TestHousekeeping(PromoteJourney):
    def test_releases_in_one_second_gc_subdirs_and_names(self):
        src = self.tool_repo("keep = 5\n")
        for _ in range(3):
            self.promote("--again")
        stamps = [s for s in os.listdir(f"{self.releases}/tool") if s[0].isdigit()]
        self.assertEqual(len(set(stamps)), 3)
        cur = self.current()
        r = self.pp("promote", "gc", "tool", "--keep", "1", "-n")
        self.assertIn("gc: 1 old release(s) of tool to remove", r.stdout)
        self.assertEqual(len([s for s in os.listdir(f"{self.releases}/tool") if s[0].isdigit()]), 3)   # -n: none gone
        self.assertIn("pass -y", self.pp("promote", "gc", "tool", "--keep", "1", code=2).stderr)    # no terminal
        r = self.pp("promote", "gc", "tool", "--keep", "1", "-y")
        self.assertIn("gc: removed 1 old release(s)", r.stdout)
        self.assertIn("gc: nothing to remove", self.pp("promote", "gc", "tool", "--keep", "0").stdout)
        self.assertEqual(self.current(), cur)

        os.makedirs(f"{src}/sub/deeper")
        self.assertIn("already current", self.promote(cwd=f"{src}/sub/deeper").stdout)
        self.assertIn(cur, self.pp("promote", "ls", cwd=f"{src}/sub/deeper").stdout)

        for verb in ("ls", "diff", "rollback"):
            r = self.pp("promote", verb, code=2)        # cwd: not a project
            self.assertIn("not in a promoted project: name one (tool)", r.stderr)
        self.assertIn("known: tool", self.pp("promote", "ls", "nosuch", code=2).stderr)

        self.commit("bin/tool", TOOL.format(v="v2"), "v2")
        r = self.pp("promote", "diff", "tool")
        self.assertIn("v2", r.stdout)
        self.assertIn("bin/tool | 2 +-", r.stdout)

        other = self.git_repo("work/elsewhere/tool", {".pp.toml": "[promote]\n"})
        r = self.promote(code=2, cwd=other)
        self.assertIn(f"name 'tool' already belongs to {src}", r.stderr)

    def test_tree_mode_and_a_moved_source(self):
        src = os.path.join(self.tmp, "work/notes")
        self.write(f"{src}/.pp.toml", '[promote]\nsource = "tree"\nexclude = ["*.log"]\n')
        self.write(f"{src}/serve.sh", "#!/bin/sh\necho serve\n", 0o755)
        self.write(f"{src}/run.log", "noise\n")
        self.pp("promote", cwd=src)
        cur = self.current("notes")
        self.assertRegex(cur, r"-[0-9a-f]{10}$")                      # tree mode: content hash
        self.assertFalse(os.path.exists(f"{self.releases}/notes/current/run.log"))
        self.assertIn("already current", self.pp("promote", cwd=src).stdout)
        self.write(f"{src}/run.log", "more noise\n")                  # excluded: still the same tree
        self.assertIn("already current", self.pp("promote", cwd=src).stdout)
        self.write(f"{src}/serve.sh", "#!/bin/sh\necho serve v2\n", 0o755)
        self.assertIn("differs", self.pp("promote", "status").stdout)
        self.assertIn("serve.sh", self.pp("promote", "diff", "notes").stdout)

        shutil.move(src, src + "-moved")
        r = self.pp("promote", "status")
        self.assertIn(f"notes: its source {src} is gone", r.stderr)
        self.assertIn("(gone)", r.stdout)


class TestSelfPromotion(PromoteJourney):
    def test_pp_releases_itself_then_rolls_back(self):
        """CONTRIBUTING's "Running pp from a clone", then "Running your changes", in a sandbox: a copy of this tree,
        its own .pp.toml, except a quick gate (the full suite inside the snapshot would run every journey again)."""
        src = os.path.join(self.tmp, "work/pp")
        shutil.copytree(ROOT, src, ignore=shutil.ignore_patterns(".git", "__pycache__", "*.pyc", ".pp-release.json"))
        cfg = self.read(f"{src}/.pp.toml")
        self.assertIn('links = { "~/.local/bin/pp" = "bin/pp" }', cfg)
        with open(f"{src}/.pp.toml", "w") as f:
            f.write("\n".join('check = "python3 -m unittest tests.test_cli -q"' if l.startswith("check =") else l
                              for l in cfg.splitlines()) + "\n")
        self.sh("git init -q -b main && git add -A && git commit -qm pp", cwd=src)
        self.sh(f"mkdir -p {self.binl} && ln -s {src}/bin/pp {self.binl}/pp")      # the link to the clone

        r = subprocess.run([sys.executable, "bin/pp", "promote"], cwd=src, env=self.env, capture_output=True,
                           text=True, timeout=120)                   # it takes over the link to the clone
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        first = self.current("pp")
        self.assertEqual(os.path.realpath(f"{self.binl}/pp"), f"{self.releases}/pp/{first}/bin/pp")
        self.assertEqual(os.path.realpath(f"{self.binl}/ppd"), f"{src}/bin/pp")
        for exe in ("pp", "ppd"):
            self.assertIn("promote", self.sh(f"{self.binl}/{exe} -h"))

        self.write(f"{src}/NOTE", "a change\n")
        self.sh("git add -A && git commit -qm note", cwd=src)
        self.assertIn("+1", self.sh(f"{self.binl}/pp promote status", cwd=src))
        out = self.sh(f"{self.binl}/pp promote", cwd=src)               # afterwards: pp promote
        second = self.current("pp")
        self.assertNotEqual(second, first)
        self.assertIn(f"promoted pp -> {second}", out)
        self.assertTrue(os.path.exists(f"{self.binl}/pp") and os.path.exists(f"{self.releases}/pp/current/NOTE"))

        self.sh(f"{self.binl}/pp promote rollback pp -y", cwd=self.work)
        self.assertEqual(self.current("pp"), first)
        self.assertEqual(os.path.realpath(f"{self.binl}/pp"), f"{self.releases}/pp/{first}/bin/pp")


if __name__ == "__main__":
    unittest.main()
