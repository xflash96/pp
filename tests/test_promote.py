import gc
import io
import json
import os
import subprocess
import sys
import threading
import tomllib
import unittest
import warnings
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp.commands import promote  # noqa: E402

# who commits comes from the sandbox, which owns HOME and GIT_*; these two are what a global config of the
# machine's would have said and nothing in the scope says
GIT = ["git", "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main"]


class Base(unittest.TestCase):
    def setUp(self):
        self.sb = sandbox.enter(self)
        self.tmp = self.sb.root
        self.repo = os.path.join(self.tmp, "proj")
        self.link = os.path.join(self.tmp, "bin", "demo")
        self.devlink = os.path.join(self.tmp, "bin", "demod")
        self.rel = os.path.join(self.sb.data, "releases", "demo")

    # ---- helpers
    def write(self, rel, text, exe=False, root=None):
        p = os.path.join(root or self.repo, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            f.write(text)
        if exe:
            os.chmod(p, 0o755)

    def git(self, *args, root=None):
        return subprocess.run(GIT + ["-C", root or self.repo, *args], check=True, capture_output=True, text=True).stdout

    def commit(self, msg="c"):
        self.git("add", "-A")
        self.git("commit", "-qm", msg)
        return self.git("rev-parse", "HEAD").strip()

    def make_repo(self, extra=""):
        os.makedirs(self.repo)
        self.git("init", "-q")
        self.write(".pp.toml", f'name = "demo"\n[promote]\nsource = "git"\n'
                               f'links = {{ "{self.link}" = "bin/tool" }}\n'
                               f'dev_links = {{ "{self.devlink}" = "bin/tool" }}\n' + extra)
        self.write("bin/tool", "#!/bin/sh\necho v1\n", exe=True)
        self.write(".gitignore", "*.log\n")
        return self.commit("init")

    def pp(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err), mock.patch("sys.stdin", io.StringIO()):
            try:
                rc = promote.main(list(argv))
            except SystemExit as e:
                rc = e.code
        return rc, out.getvalue(), err.getvalue()

    def current(self):
        try:
            return os.readlink(os.path.join(self.rel, "current"))
        except OSError:
            return None

    def stamps(self):
        if not os.path.isdir(self.rel):
            return []
        return sorted(n for n in os.listdir(self.rel) if not n.startswith(".") and n not in ("current", "revisions.log"))

    def revlog(self):
        with open(os.path.join(self.rel, "revisions.log")) as f:
            return f.read()

    def registry(self) -> dict:
        """The one registry every promoted project is recorded in."""
        with open(os.path.join(self.sb.state, "promote", "registry.json")) as f:
            return json.load(f)


class TestPromote(Base):
    def test_creates_release_current_and_links(self):
        head = self.make_repo()
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 0, out)
        cur = self.current()
        self.assertFalse(os.path.isabs(cur))
        self.assertTrue(cur.endswith(head[:7]))
        self.assertEqual(os.path.realpath(self.link), os.path.realpath(os.path.join(self.rel, cur, "bin", "tool")))
        self.assertEqual(os.readlink(self.link), os.path.join(self.rel, "current", "bin", "tool"))   # through current
        self.assertTrue(os.access(self.link, os.X_OK))
        self.assertEqual(subprocess.run([self.link], capture_output=True, text=True).stdout, "v1\n")
        self.assertEqual(os.path.realpath(self.devlink), os.path.join(self.repo, "bin", "tool"))
        with open(os.path.join(self.rel, cur, ".pp-release.json")) as f:
            self.assertEqual(json.load(f)["rev"], head)
        self.assertIn(f"promote - -> {cur} rev={head}", self.revlog())
        self.assertEqual(self.registry()["demo"]["source"], self.repo)
        logs = os.listdir(os.path.join(self.sb.state, "promote", "logs"))
        self.assertEqual(logs, [f"demo-{cur}.log"])

    def test_two_projects_are_both_in_the_registry(self):
        """One registry holds every promoted project, so a promotion records its own name beside the names
        already there, and each record names that project's own source."""
        self.make_repo()
        self.assertEqual(self.pp(self.repo)[0], 0)
        other = os.path.join(self.tmp, "other")
        self.write(".pp.toml", 'name = "other"\n[promote]\nsource = "tree"\n', root=other)
        self.write("a.txt", "a", root=other)
        rc, out, err = self.pp(other)
        self.assertEqual(rc, 0, out + err)
        reg = self.registry()
        self.assertEqual(sorted(reg), ["demo", "other"])
        self.assertEqual((reg["demo"]["source"], reg["other"]["source"]), (self.repo, other))

    def test_link_moves_with_the_flip(self):
        self.make_repo()
        self.pp(self.repo)
        self.write("bin/tool", "#!/bin/sh\necho v2\n", exe=True)
        self.commit()
        self.pp(self.repo)
        self.assertEqual(subprocess.run([self.link], capture_output=True, text=True).stdout, "v2\n")

    def test_already_current_and_again(self):
        self.make_repo()
        self.pp(self.repo)
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 0)
        self.assertIn("already current", out)
        self.assertEqual(len(self.stamps()), 1)
        self.assertEqual(self.pp(self.repo, "--again")[0], 0)
        self.assertEqual(len(self.stamps()), 2)

    def test_dirty_refused_then_dirty_snapshot(self):
        self.make_repo()
        self.pp(self.repo)
        self.write("bin/tool", "#!/bin/sh\necho wip\n", exe=True)
        self.write("new.txt", "untracked\n")
        self.write("noise.log", "ignored\n")
        rc, _, err = self.pp(self.repo)
        self.assertEqual(rc, 2)
        self.assertIn("modified", err)
        self.assertEqual(len(self.stamps()), 1)
        rc, out, _ = self.pp(self.repo, "--dirty")
        self.assertEqual(rc, 0, out)
        cur = self.current()
        self.assertTrue(cur.endswith("-dirty"))
        d = os.path.join(self.rel, cur)
        with open(os.path.join(d, "bin", "tool")) as f:
            self.assertIn("wip", f.read())
        self.assertTrue(os.access(os.path.join(d, "bin", "tool"), os.X_OK))
        self.assertTrue(os.path.exists(os.path.join(d, "new.txt")))
        self.assertFalse(os.path.exists(os.path.join(d, "noise.log")))
        self.assertIn("dirty", self.revlog().splitlines()[-1])

    def test_failing_check_leaves_current(self):
        self.make_repo('check = "test ! -e FAIL"\n')
        self.pp(self.repo)
        first = self.current()
        self.write("FAIL", "x")
        self.commit()
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 1)
        self.assertIn("check failed", out)
        self.assertEqual(self.current(), first)
        self.assertEqual(self.stamps(), [first])
        self.assertFalse([n for n in os.listdir(self.rel) if n.startswith(".staging")])

    def test_keep_failed(self):
        self.make_repo('check = "exit 3"\n')
        rc, _, _ = self.pp(self.repo, "--keep-failed")
        self.assertEqual(rc, 1)
        self.assertIsNone(self.current())
        self.assertTrue([n for n in os.listdir(self.rel) if n.startswith(".staging")])

    def test_build_runs_in_release_before_check(self):
        head = self.make_repo('build = "echo $PP_NAME $PP_REV > built.txt"\ncheck = "test -f built.txt"\n')
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 0, out)
        with open(os.path.join(self.rel, self.current(), "built.txt")) as f:
            self.assertEqual(f.read().split(), ["demo", head])
        self.assertFalse(os.path.exists(os.path.join(self.repo, "built.txt")))

    def test_link_target_produced_by_build(self):
        self.make_repo('build = "mkdir -p dist && cp bin/tool dist/tool"\n')
        self.write(".pp.toml", f'name = "demo"\n[promote]\nbuild = "mkdir -p dist && cp bin/tool dist/tool"\n'
                               f'links = {{ "{self.link}" = "dist/tool" }}\n')
        self.commit()
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 0, out)
        self.assertTrue(os.path.exists(self.link))

    def test_verify_failure_rolls_back_and_reruns_after(self):
        afterlog = os.path.join(self.tmp, "after.log")
        self.make_repo(f'after = "basename $PP_RELEASE >> {afterlog}"\nverify = "test ! -e BAD"\n')
        self.pp(self.repo)
        first = self.current()
        self.write("BAD", "x")
        self.commit()
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 1)
        self.assertEqual(self.current(), first)
        with open(afterlog) as f:
            ran = f.read().split()
        self.assertEqual(len(ran), 3)
        self.assertEqual((ran[0], ran[2]), (first, first))
        self.assertNotEqual(ran[1], first)
        self.assertIn("auto: verify failed", self.revlog())

    def test_verify_failure_on_first_release_removes_current(self):
        self.make_repo('verify = "false"\n')
        rc, _, _ = self.pp(self.repo)
        self.assertEqual(rc, 1)
        self.assertIsNone(self.current())

    def test_rollback_and_to(self):
        self.make_repo()
        made = []
        for i in range(3):
            self.write("v", str(i))
            self.commit()
            self.pp(self.repo)
            made.append(self.current())
        self.assertEqual(self.pp("rollback", "demo")[0], 2)            # no terminal, no -y: refuse
        self.assertEqual(self.current(), made[2])
        self.assertEqual(self.pp("rollback", "demo", "-y")[0], 0)
        self.assertEqual(self.current(), made[1])
        self.assertEqual(self.pp("rollback", "demo", "--to", made[0], "-y")[0], 0)
        self.assertEqual(self.current(), made[0])
        self.assertEqual(self.pp("rollback", "demo", "--to", made[0], "-y")[0], 2)
        self.assertIn(f"rollback {made[2]} -> {made[1]} (manual)", self.revlog())
        # the source is still ahead: promoting again moves forward
        self.assertEqual(self.pp(self.repo, "--again")[0], 0)

    def test_preflight_mutates_nothing_and_lists_every_blocker(self):
        self.make_repo()
        os.makedirs(os.path.dirname(self.link))
        self.write(self.link, "precious", root="/")
        self.write("bin/tool", "changed", exe=True)
        rc, out, _ = self.pp(self.repo, "-n")
        self.assertEqual(rc, 2)
        self.assertIn("modified", out)
        self.assertIn("regular file", out)
        self.assertFalse(os.path.exists(self.sb.data))
        self.assertFalse(os.path.exists(self.sb.state))
        self.assertFalse(os.path.islink(self.link))

    def test_preflight_clear(self):
        self.make_repo()
        rc, out, _ = self.pp("-n", self.repo)
        self.assertEqual(rc, 0)
        self.assertIn("preflight: clear", out)
        self.assertFalse(os.path.exists(self.sb.data))

    def test_link_conflicts_and_force(self):
        self.make_repo()
        os.makedirs(os.path.dirname(self.link))
        self.write(self.link, "precious", root="/")
        os.symlink("/etc/hostname", self.devlink)
        rc, _, err = self.pp(self.repo)
        self.assertEqual(rc, 2)
        self.assertIn("--force-links", err)
        self.assertEqual(self.pp(self.repo, "--force-links")[0], 2)    # foreign replacement needs -y off a terminal
        self.assertTrue(os.path.isfile(self.link) and not os.path.islink(self.link))
        self.assertEqual(self.pp(self.repo, "--force-links", "-y")[0], 0)
        self.assertTrue(os.path.islink(self.link))
        self.assertEqual(os.path.realpath(self.devlink), os.path.join(self.repo, "bin", "tool"))

    def test_an_already_current_release_asks_before_replacing_a_foreign_link(self):
        """A run that finds the release already current applies the same link plan, so it asks the same
        question first. Otherwise the user is asked about replacing their own file the first time and never
        again, although every run replaces it."""
        self.make_repo()
        os.makedirs(os.path.dirname(self.link))
        self.assertEqual(self.pp(self.repo)[0], 0)              # the first release, with the paths free
        os.remove(self.link)
        self.write(self.link, "precious", root="/")              # the user's own file, back in the way

        rc, out, _ = self.pp(self.repo, "--force-links")         # already current: it asks all the same
        self.assertEqual(rc, 2)
        self.assertIn(f"will replace {self.link}", out)
        with open(self.link) as f:
            self.assertEqual(f.read(), "precious")

        rc, out, _ = self.pp(self.repo, "--force-links", "-y")
        self.assertEqual(rc, 0)
        self.assertIn("already current", out)
        self.assertTrue(os.path.islink(self.link))

    def test_link_into_source_is_replaced_without_force(self):
        self.make_repo()
        os.makedirs(os.path.dirname(self.link))
        os.symlink(os.path.join(self.repo, "bin", "tool"), self.link)   # e.g. the old by-hand dev link
        self.assertEqual(self.pp(self.repo)[0], 0)
        self.assertIn(os.path.join(self.rel, "current"), os.readlink(self.link))

    def test_tree_mode_hash_stamp(self):
        src = os.path.join(self.tmp, "treeproj")
        self.write(".pp.toml", 'name = "demo"\n[promote]\nsource = "tree"\nexclude = ["*.tmp"]\n', root=src)
        self.write("a.txt", "a", root=src)
        self.write("sub/b.txt", "b", root=src)
        self.write("node_modules/x.js", "x", root=src)
        self.write("junk.tmp", "x", root=src)
        os.symlink("a.txt", os.path.join(src, "alias"))
        rc, out, _ = self.pp(src)
        self.assertEqual(rc, 0, out)
        t = promote._project(src)
        h = promote._tree_hash(src, promote._tree_files(src, t.excludes()))
        cur = self.current()
        self.assertTrue(cur.endswith("-" + h[:10]))
        d = os.path.join(self.rel, cur)
        self.assertTrue(os.path.exists(os.path.join(d, "sub", "b.txt")))
        self.assertTrue(os.path.islink(os.path.join(d, "alias")))
        self.assertFalse(os.path.exists(os.path.join(d, "node_modules")))
        self.assertFalse(os.path.exists(os.path.join(d, "junk.tmp")))
        self.assertIn("already current", self.pp(src)[1])
        self.write("a.txt", "changed", root=src)
        self.assertEqual(self.pp(src)[0], 0)
        self.assertEqual(len(self.stamps()), 2)

    def test_gc_keeps_current_and_previous(self):
        self.make_repo("keep = 1\n")
        for _ in range(4):
            self.assertEqual(self.pp(self.repo, "--again")[0], 0)
        self.assertEqual(len(self.stamps()), 2)
        self.assertIn(self.current(), self.stamps())
        self.assertEqual(self.pp("gc", "demo", "--keep", "0")[0], 0)
        self.assertEqual(len(self.stamps()), 2)

    def test_status_json(self):
        self.make_repo()
        self.pp(self.repo)
        rc, out, _ = self.pp("status", "--json")
        row = json.loads(out)[0]
        self.assertEqual((row["name"], row["current"], row["ahead"], row["dirty"], row["links_ok"]),
                         ("demo", self.current(), 0, False, True))
        self.write("more", "x")
        self.commit()
        os.remove(self.devlink)
        row = json.loads(self.pp("status", "--json")[1])[0]
        self.assertEqual(row["ahead"], 1)
        self.assertFalse(row["links_ok"])
        self.assertEqual(row["link_problems"], [{"path": self.devlink, "kind": "dev_link", "problem": "missing"}])
        self.assertEqual(self.pp("link", "demo")[0], 0)
        self.assertTrue(json.loads(self.pp("status", "--json")[1])[0]["links_ok"])

    def test_ls_and_diff(self):
        self.make_repo()
        self.pp(self.repo)
        self.write("more", "x")
        self.commit("second change")
        rows = json.loads(self.pp("ls", "demo", "--json")[1])
        self.assertTrue(rows[0]["current"])
        rc, out, _ = self.pp("diff", self.repo)
        self.assertEqual(rc, 0)
        self.assertIn("second change", out)

    def test_name_collision_blocks(self):
        self.make_repo()
        self.pp(self.repo)
        other = os.path.join(self.tmp, "other")
        self.write(".pp.toml", 'name = "demo"\n[promote]\nsource = "tree"\n', root=other)
        rc, _, err = self.pp(other)
        self.assertEqual(rc, 2)
        self.assertIn("already belongs to", err)

    def test_project_in_a_git_subdirectory(self):
        os.makedirs(self.repo)
        self.git("init", "-q")
        sub = os.path.join(self.repo, "tools", "demo")
        self.write(".pp.toml", 'name = "demo"\n[promote]\n', root=sub)
        self.write("run.sh", "x", root=sub)
        self.write("elsewhere.txt", "x")
        self.commit()
        self.assertEqual(self.pp(sub)[0], 0)
        self.assertEqual(sorted(os.listdir(os.path.join(self.rel, self.current()))), [".pp-release.json", ".pp.toml", "run.sh"])
        self.write("elsewhere.txt", "changed")       # a change outside the project does not make it dirty
        self.assertIn("already current", self.pp(sub)[1])

    def test_no_promote_table(self):
        os.makedirs(self.repo)
        self.assertEqual(self.pp(self.repo)[0], 2)

    def test_not_git_and_no_commits(self):
        os.makedirs(self.repo)
        self.write(".pp.toml", "[promote]\n")
        rc, out, _ = self.pp(self.repo, "-n")
        self.assertEqual(rc, 2)
        self.assertIn("not a git repository", out)
        self.git("init", "-q")
        rc, out, _ = self.pp(self.repo, "-n")
        self.assertIn("no commits", out)

    def test_init(self):
        d = os.path.join(self.tmp, "fresh")
        os.makedirs(d)
        self.assertEqual(self.pp("init", d)[0], 0)
        with open(os.path.join(d, ".pp.toml"), "rb") as f:
            self.assertEqual(tomllib.load(f)["promote"]["source"], "tree")
        self.assertEqual(self.pp("init", d)[0], 2)
        e = os.path.join(self.tmp, "existing")
        self.write(".pp.toml", 'name = "keepme"', root=e)
        self.assertEqual(self.pp("init", e)[0], 0)
        with open(os.path.join(e, ".pp.toml"), "rb") as f:
            data = tomllib.load(f)
        self.assertEqual((data["name"], "promote" in data), ("keepme", True))



class TestNothingLeftOpen(Base):
    def test_a_promote_and_a_rollback_close_their_log_files(self):
        """Each run appends to a log file of its own. The file is closed when the run ends, however it ends, so an
        in-process caller is left no open file."""
        self.make_repo()
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", ResourceWarning)
            for i in range(2):
                self.write("v", str(i))
                self.commit()
                rc, out, _ = self.pp(self.repo)
                self.assertEqual(rc, 0, out)
            rc, out, _ = self.pp("rollback", "demo", "-y")
            self.assertEqual(rc, 0, out)
            gc.collect()
        self.assertEqual([str(w.message) for w in seen if issubclass(w.category, ResourceWarning)], [])


class TestWhatItSays(Base):
    def test_preflight_blocks_a_link_target_the_release_would_not_have(self):
        self.make_repo()
        self.write(".pp.toml", f'name = "demo"\n[promote]\nsource = "git"\nlinks = {{ "{self.link}" = "bin/later" }}\n')
        self.commit("link to a file not committed")
        self.write("bin/later", "#!/bin/sh\n", exe=True)          # on disk, not in HEAD: the snapshot lacks it
        rc, out, _ = self.pp(self.repo, "-n")
        self.assertEqual(rc, 2)
        self.assertIn("bin/later is a link target but the release would not have it", out)
        self.assertEqual(self.pp(self.repo)[0], 2)                  # the real run says the same, before snapshotting
        self.write(".pp.toml", f'name = "demo"\n[promote]\nsource = "git"\nbuild = "true"\n'
                               f'links = {{ "{self.link}" = "bin/later" }}\n')
        self.commit("a build may make it")
        self.assertEqual(self.pp(self.repo, "-n")[0], 0)

    def test_status_names_what_is_wrong_with_each_link(self):
        self.make_repo()
        self.assertEqual(self.pp(self.repo)[0], 0)
        os.remove(self.link)
        self.write(self.link, "mine", root="/")                     # the user's own file where the link was
        os.remove(self.devlink)
        rc, out, _ = self.pp("status", "--json")
        links = {x["path"]: x["problem"] for x in json.loads(out)[0]["link_problems"]}
        self.assertEqual(links, {self.link: "conflict", self.devlink: "missing"})
        out = self.pp("status")[1]
        self.assertIn(f"conflict {self.link}", out)
        self.assertNotIn("BROKEN", out)

    def test_diff_says_when_there_is_nothing_and_has_json(self):
        self.make_repo()
        self.pp(self.repo)
        out = self.pp("diff", "demo")[1]
        self.assertIn("nothing: the source is level with the release", out)
        self.assertNotIn("(nothing)", out)
        self.write("bin/tool", "#!/bin/sh\necho v2\n", exe=True)
        self.commit("v2")
        out = self.pp("diff", "demo")[1]
        self.assertIn("commits:\n", out)
        self.assertIn("files:\n", out)
        d = json.loads(self.pp("diff", "demo", "--json")[1])
        self.assertEqual((d["name"], len(d["commits"])), ("demo", 1))

    def test_a_release_nothing_checked_says_so(self):
        self.make_repo('check = "true"\n')
        self.pp(self.repo)
        self.pp(self.repo, "--again", "--no-check")
        rows = json.loads(self.pp("ls", "demo", "--json")[1])
        self.assertEqual([r["checked"] for r in rows], [False, True])       # newest first
        self.assertIn("unchecked", self.pp("ls", "demo")[1].splitlines()[1])


class TestTwoAtOnce(Base):
    """Two promotes, or a promote and a rollback, at the same time. Each test parks one of them at the exact
    statement where the other used to cost it work, so the bad order happens on every run."""

    def test_rollback_refuses_when_current_moved_while_it_asked(self):
        self.make_repo()
        for i in range(2):
            self.write("v", str(i))
            self.commit()
            self.pp(self.repo)
        asked_about = self.current()
        landed, asked = [], journey.Gate()

        def promote_again():
            self.assertEqual(promote.main([self.repo, "--again"]), 0)
            landed.append(self.current())

        def confirm(prompt, yes=False):
            """The question is the window: a promote runs to completion while it is on screen."""
            asked.once(promote_again)
            return True

        with mock.patch.object(promote.ui, "confirm", confirm):
            rc, _, err = self.pp("rollback", "demo")
        self.assertEqual((rc, asked.ran), (2, 1), err)
        self.assertIn(f"current is {landed[0]} now, not the {asked_about} this asked about", err)
        self.assertEqual(self.current(), landed[0])
        self.assertNotIn("rollback", self.revlog())

    def test_the_release_a_promote_flips_away_from_is_read_under_the_lock(self):
        afterlog = os.path.join(self.tmp, "after.log")
        self.make_repo(f'after = "echo ${{PP_PREV:-none}} >> {afterlog}"\n')
        self.pp(self.repo)
        self.write("v", "2")
        self.commit()
        landed, asked = [], journey.Gate()

        def promote_again():
            self.assertEqual(promote.main([self.repo, "--again"]), 0)
            landed.append(self.current())

        def confirm_foreign(plan, yes):
            """cmd_run asks here, after reading `current` and before taking the lock."""
            asked.once(promote_again)
            return True

        with mock.patch.object(promote, "_confirm_foreign", confirm_foreign):
            rc, out, _ = self.pp(self.repo)
        self.assertEqual((rc, asked.ran), (0, 1), out)
        self.assertIn(f"promote {landed[0]} -> {self.current()}", self.revlog())
        with open(afterlog) as f:
            self.assertEqual(f.read().split()[-1], os.path.join(self.rel, landed[0]))   # PP_PREV

    def test_a_second_applier_does_not_remove_this_ones_new_link(self):
        self.make_repo()
        self.pp(self.repo)
        t = promote._project(self.repo)
        os.remove(self.link)                        # so both appliers have this link to create
        plan = promote._link_plan(t, False)
        real = os.replace
        ran = journey.Gate()

        def hooked(src, dst, **kw):
            """Another applier runs between this one's symlink and its rename."""
            ran.once(lambda: promote._apply_links(promote._link_plan(t, False), say=lambda msg: None),
                     when=lambda: str(dst) == self.link)
            return real(src, dst, **kw)

        with mock.patch.object(promote.os, "replace", hooked):
            try:
                promote._apply_links(plan, say=lambda msg: None)
            except OSError as e:
                self.fail(f"the second applier removed this one's new link: {e}")
        self.assertEqual(ran.ran, 1)
        self.assertEqual(os.readlink(self.link), os.path.join(self.rel, "current", "bin", "tool"))

    def test_link_refuses_while_a_promote_holds_the_lock(self):
        self.make_repo()
        self.pp(self.repo)
        t = promote._project(self.repo)
        got, done = threading.Event(), threading.Event()

        def holder():
            """The target's lock held by another actor, as a promote in flight holds it. A thread of its own:
            the lock lets its holder take it again, so holding it here would let this one straight back in."""
            with promote._lock(t):
                got.set()
                done.wait(30)

        th = threading.Thread(target=holder)
        th.start()
        self.addCleanup(th.join, 30)
        self.addCleanup(done.set)
        self.assertTrue(got.wait(30), "the holder never took the lock")
        os.remove(self.link)
        rc, _, err = self.pp("link", "demo")
        self.assertEqual(rc, 1, err)
        self.assertIn("another promote/rollback of demo is running", err)
        self.assertFalse(os.path.lexists(self.link))

    def test_a_link_failure_after_the_flip_says_what_to_run(self):
        self.make_repo()
        self.pp(self.repo)
        first = self.current()
        self.write("v", "2")
        self.commit()
        with mock.patch.object(promote, "_apply_links", side_effect=OSError("its temporary link is gone")):
            rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 1)
        self.assertNotEqual(self.current(), first)
        self.assertIn(f"current is {self.current()} but its links or hooks did not finish", out)
        self.assertIn("its temporary link is gone", out)
        self.assertIn("pp promote link demo", out)
        self.assertIn(f"pp promote rollback demo` goes back to {first}", out)


class TestAGitThatDidNotAnswer(Base):
    def test_a_run_whose_time_ran_out_refuses_instead_of_reading_as_a_clean_empty_tree(self):
        """git's time runs out and it is ended, so it leaves no output. Read as an answer, that output says the
        tree is clean and holds no file, and the release is a snapshot of nothing."""
        src = os.path.join(self.tmp, "nolinks")
        self.write(".pp.toml", 'name = "nolinks"\n[promote]\nsource = "git"\n', root=src)
        self.write("a.txt", "a\n", root=src)
        for args in (("init", "-q"), ("add", "-A"), ("commit", "-qm", "init")):
            self.git(*args, root=src)
        self.write("b.txt", "untracked\n", root=src)         # so the tree is dirty
        real = promote.proc.git

        def timed_out(*args, **kw):
            if "status" in args or "ls-files" in args:
                return promote.proc.Result(None, "", "", timed_out=True)
            return real(*args, **kw)

        with mock.patch.object(promote.proc, "git", timed_out):
            rc, out, err = self.pp(src, "--dirty")
        self.assertEqual(rc, 1, out + err)
        self.assertIn("took more than 5 minutes", err)
        self.assertFalse(os.path.isdir(os.path.join(self.sb.data, "releases", "nolinks")),
                         "a release was made from a git that did not answer")


class TestTheArchive(Base):
    def test_an_extraction_that_stops_part_way_does_not_leave_the_snapshot_blocked(self):
        """The snapshot comes out of a pipe. Reading what the writer said on its stderr before closing that pipe
        waits for a writer that is itself blocked writing a tree bigger than the pipe holds."""
        self.make_repo()
        self.write("big.txt", "x" * (1 << 20))
        self.commit("big")
        raised, dest = [], os.path.join(self.tmp, "dest")

        def archive():
            try:
                with mock.patch.object(promote.tarfile, "open", side_effect=OSError("extraction stopped")):
                    promote._archive(self.repo, dest)
            except BaseException as e:      # noqa: BLE001 - the point is that it comes back at all
                raised.append(e)
        t = threading.Thread(target=archive, daemon=True)
        t.start()
        t.join(30)
        self.assertFalse(t.is_alive(), "the snapshot was left waiting for a blocked writer")
        self.assertEqual(str(raised[0]), "extraction stopped")

    def test_a_git_that_never_answers_is_ended_and_the_snapshot_says_so(self):
        """A git that says nothing holds the pipe open, and with it this promote and the target's lock. The
        read and the wait get proc.GIT_WAIT, so the snapshot fails with a word and ends what it started."""
        self.make_repo()
        started, raised, dest = [], [], os.path.join(self.tmp, "dest")

        def never_answers(*args, **kw):
            c = promote.proc.spawn(["sleep", "600"], group="own", stdin="none", env="inherit",
                                   stdout=promote.proc.PIPE, stderr=promote.proc.PIPE)
            started.append(c)
            return c

        def archive():
            try:
                with mock.patch.object(promote.proc, "git_child", never_answers), \
                     mock.patch.object(promote.proc, "GIT_WAIT", 1.0):
                    promote._archive(self.repo, dest)
            except BaseException as e:      # noqa: BLE001 - what it raises is the point
                raised.append(e)

        t = threading.Thread(target=archive, daemon=True)
        t.start()
        t.join(30)
        self.assertFalse(t.is_alive(), "a git that never answers held the snapshot")
        self.assertIn("git archive took more than 1s", str(raised[0]))
        self.assertIsNotNone(started[0].poll(), "the git it started is still running")


if __name__ == "__main__":
    unittest.main()
