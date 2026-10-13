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
        """The command, as pp/cli.py runs it: a ConfigError is what the user reads, and exit 1."""
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err), mock.patch("sys.stdin", io.StringIO()):
            try:
                rc = promote.main(list(argv))
            except promote.config.ConfigError as e:
                print(f"pp: bad config: {e}", file=sys.stderr)
                rc = 1
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



class TestARemoveThatCannotFinish(Base):
    """Removing a release either happens or leaves that release whole. rmtree deletes what it can and leaves the
    rest, so a release pp could not remove was emptied where it stood: `ls` listed the husk with no rev, a
    `rollback --to` flipped `current` onto it and every registered link then pointed at nothing, and the mark
    that keeps a rollback off a bad release went with the metadata rmtree deleted. The keep sweep of an
    ordinary promote is the way in, not only the `gc` verb."""

    def unwritable(self, p: str):
        """Take the write bit off p for as long as the test lasts. Nothing in it can be unlinked then, which is
        what a build that write-protects its output leaves in a release: a module cache, a `pip install
        --target`, a packer's output directory."""
        os.chmod(p, 0o555)
        self.sb.later(self.writable_again)

    def writable_again(self):
        """Put the write bit back on every directory under the releases dir, whatever name it has by then: the
        remove renames the directory the test took the bit off, and the scope's tree goes with the scope."""
        for d, ds, _ in os.walk(self.rel):
            for n in ds:
                os.chmod(os.path.join(d, n), 0o755)
        os.chmod(self.rel, 0o755)

    def three(self):
        for _ in range(3):
            self.assertEqual(self.pp(self.repo, "--again")[0], 0)

    def listed(self) -> list[dict]:
        return json.loads(self.pp("ls", "demo", "--json")[1])

    @unittest.skipIf(os.geteuid() == 0, "root writes in a directory with no write bit")
    def test_a_release_gc_cannot_take_out_of_the_way_is_left_whole(self):
        """A releases directory pp may not write in, with the target's `.lock` already in it, so the lock is
        taken and the remove is reached."""
        self.make_repo("keep = 5\n")
        self.three()
        old = [s for s in self.stamps() if s != self.current()]
        self.unwritable(self.rel)
        rc, out, err = self.pp("gc", "demo", "--keep", "1", "-y")
        self.assertEqual(rc, 1, out)
        self.assertIn("could not remove 1 old release(s)", err)
        self.assertNotIn("gc: removed", out)
        for s in old:
            self.assertTrue(os.path.isfile(os.path.join(self.rel, s, promote.META)), f"{s} was emptied")
        self.assertEqual(len(self.listed()), 3)
        self.assertTrue(all(r.get("rev") for r in self.listed()))

    @unittest.skipIf(os.geteuid() == 0, "root unlinks what the permissions refuse")
    def test_a_release_whose_delete_stops_part_way_is_no_release_any_more(self):
        self.make_repo("keep = 5\n")
        self.three()
        old = [s for s in self.stamps() if s != self.current()][0]
        self.unwritable(os.path.join(self.rel, old, "bin"))
        rc, out, err = self.pp("gc", "demo", "--keep", "1", "-y")
        self.assertEqual(rc, 1, out)
        self.assertIn("could not remove 1 old release(s)", err)
        self.assertFalse(os.path.lexists(os.path.join(self.rel, old)))
        self.assertNotIn(old, [r["stamp"] for r in self.listed()])
        rc, out, _ = self.pp("gc", "demo", "-n")        # what is left is a staging dir, which gc takes again
        self.assertEqual(rc, 0, out)
        self.assertIn("1 staging dir(s)", out)

    @unittest.skipIf(os.geteuid() == 0, "root unlinks what the permissions refuse")
    def test_a_promote_that_could_not_remove_its_staging_dir_says_so(self):
        """A promote that is given up removes its staging dir, and a user who passed no `--keep-failed` reads
        the run as keeping nothing. The same unwritable subdirectory left it emptied under its own name, with
        no line in the output or in the log, and every later gc offered it, could not remove it and exited 0."""
        self.make_repo('build = "mkdir -p cache/pkg && echo c > cache/pkg/x && chmod 555 cache/pkg"\n'
                       'check = "false"\n')
        self.sb.later(self.writable_again)
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 1, out)
        self.assertIn("the failed staging dir is not gone", out)
        left = [n for n in os.listdir(self.rel) if n.startswith(".staging-")]
        self.assertEqual(len(left), 1)
        self.assertTrue(left[0].startswith(".staging-gone-"), left[0])   # no release's name, so no listing
        self.assertEqual(self.listed(), [])

    @unittest.skipIf(os.geteuid() == 0, "root unlinks what the permissions refuse")
    def test_a_keep_sweep_that_left_a_release_says_so_and_the_promotion_stands(self):
        """The sweep at the end of an ordinary promote is a gc with no verb typed, and the release is in
        service whatever it could remove: the old one still on disk is a line on stderr, and the status is
        the promotion's. Exit 1 of a promote is NOT PROMOTED, which a release script rolls back on, so a
        directory that could not be deleted undid a release that had gone out."""
        self.make_repo("keep = 1\n")
        self.assertEqual(self.pp(self.repo, "--again")[0], 0)
        first = self.current()
        self.assertEqual(self.pp(self.repo, "--again")[0], 0)       # the one it flips away from always stays
        self.unwritable(os.path.join(self.rel, first, "bin"))
        rc, out, err = self.pp(self.repo, "--again")
        self.assertEqual(rc, 0, out + err)
        self.assertIn("could not remove 1 old release(s)", err)
        self.assertIn("promoted demo", out)
        # the release the promote made is in service, read through the link a person uses
        self.assertEqual(subprocess.run([self.link], capture_output=True, text=True).stdout, "v1\n")
        self.assertNotIn(first, [r["stamp"] for r in self.listed()])


class TestAWordThatIsNoVerb(Base):
    def test_a_mistyped_verb_does_not_release_the_project_in_the_cwd(self):
        """Every other word is a PATH, and a path that is not there resolves to the project of the cwd: a typo
        for a read-only verb used to snapshot, gate, flip `current` and run `after`."""
        self.make_repo()
        self.assertEqual(self.pp(self.repo)[0], 0)
        first = self.current()
        self.write("v", "2")
        self.commit("v2")
        self.sb.chdir(self.repo)
        rc, out, err = self.pp("stat")
        self.assertEqual(rc, 2, out)
        self.assertIn("no such verb or path 'stat'", err)
        self.assertEqual(self.current(), first)
        self.assertEqual(self.stamps(), [first])

        rc, _, err = self.pp("demo")                  # the name every other verb takes
        self.assertEqual(rc, 2)
        self.assertIn("'demo' is a promoted target", err)
        self.assertEqual(self.current(), first)

        self.assertEqual(self.pp("-n", self.repo)[0], 0)      # a path, and a flag before it, still work
        self.assertEqual(self.pp("-n")[0], 0)
        self.assertEqual(self.pp("-n", "bin")[0], 0)          # a relative path in the project

    def test_the_verb_is_the_verb_wherever_it_sits_among_the_options(self):
        """The refusal covered the first argument only, so an option before the word sent it on as a PATH:
        `pp promote -y rollback`, the transposition of a line all three verbs take `-y` on, promoted the
        project in the cwd instead of rolling it back, without asking."""
        self.make_repo("keep = 5\n")
        self.assertEqual(self.pp(self.repo)[0], 0)
        first = self.current()
        self.write("v", "2")
        self.commit("v2")
        self.assertEqual(self.pp(self.repo)[0], 0)
        second = self.current()
        self.sb.chdir(self.repo)

        rc, out, err = self.pp("--again", "stat")                  # a typo for a read-only verb
        self.assertEqual(rc, 2, out)
        self.assertIn("no such verb or path 'stat'", err)
        self.assertEqual(self.current(), second)
        self.assertEqual(len(self.stamps()), 2)

        rc, out, err = self.pp("-y", "rollback")                   # the verb, with its option first
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(self.current(), first)
        self.assertEqual(len(self.stamps()), 2, "it released the source instead of rolling back")
        self.assertEqual(self.pp("--json", "status")[0], 0)        # a verb after an option it takes itself

    def test_a_word_argparse_reads_as_the_path_is_judged_as_the_path(self):
        """The scan for the word passed over every word starting with `-`, and argparse reads such a word as
        the positional where it holds a space or looks like a negative number: `OPTS="--dirty --again"` in a
        zsh, which does not split it, and `pp promote -1` both reached PATH, and a path that is not there
        resolves to the project of the cwd, so each released it without asking."""
        self.make_repo()
        self.assertEqual(self.pp(self.repo)[0], 0)
        first = self.current()
        self.write("v", "2")
        self.commit("v2")
        self.sb.chdir(self.repo)
        for word in ("--dirty --again", "-1"):
            with self.subTest(word=word):
                rc, out, err = self.pp(word)
                self.assertEqual(rc, 2, out)
                self.assertIn(f"no such verb or path '{word}'", err)
                self.assertEqual(self.current(), first)
                self.assertEqual(self.stamps(), [first])
        rc, out, _ = self.pp("rollback", "-h")      # the verb's own help: main() reads with no -h of its own
        self.assertEqual(rc, 0)
        self.assertIn("flip `current` back", out)

    def test_an_option_of_a_verb_is_read_by_that_verbs_parser(self):
        """main() looks the verb up before any option is read, because the options on that line are the verb's
        own. Judged against `pp promote`'s options first, gc's documented `--keep` was a unique prefix of
        `--keep-failed` there: `--keep=1` was refused with a usage line about a flag the user never typed,
        while `--keep 1` ran, so the two spellings of one option disagreed."""
        self.make_repo("keep = 5\n")
        made = []
        for i in range(3):
            self.write("v", str(i))
            self.commit(f"v{i}")
            self.assertEqual(self.pp(self.repo)[0], 0)
            made.append(self.current())
        for spelling in (["--keep=1"], ["--keep", "1"]):
            with self.subTest(spelling=spelling):
                rc, out, err = self.pp("gc", "demo", *spelling, "-n")
                self.assertEqual(rc, 0, out + err)
                self.assertIn(f"to remove: {made[0]}", out)
        self.assertEqual(self.stamps(), sorted(made), "a dry run removed a release")

    def test_a_mistaken_option_is_answered_with_the_usage_that_names_h(self):
        """main() reads argv with a parser of no `-h` and no abbreviations of its own, so `pp promote rollback
        -h` is the verb's help and an abbreviation is resolved by the parser that reports it. That parser
        reports nothing: one command line has one usage line, and it names the flag that would explain the
        rest."""
        self.make_repo()
        self.sb.chdir(self.repo)
        for word in ("--dirty=x", "--qq", "--d"):
            with self.subTest(word=word):
                rc, out, err = self.pp(word)
                self.assertEqual(rc, 2, out + err)
                self.assertEqual(err.count("usage: pp promote"), 1)
                self.assertIn("[-h]", err)
        self.assertEqual(self.stamps(), [], "it released the project in the cwd")


class TestATreeProjectPpCannotRead(Base):
    """A tree project is what `source = "tree"` is written for, and a service project holds more than regular
    files. The hash opened whatever the walk listed, with no bound and no handler."""

    def tree(self) -> str:
        src = os.path.join(self.tmp, "svc")
        self.write(".pp.toml", 'name = "svc"\n[promote]\nsource = "tree"\n', root=src)
        self.write("f.txt", "x", root=src)
        return src

    def test_a_fifo_is_no_part_of_a_release_and_does_not_hold_the_command(self):
        """A dev server's fifo in the source blocked the open that hashes the tree, with no bound: promote, its
        preflight and status never came back. `_copy` never copied one into a release either, so the hash and
        the snapshot now agree on what a file is."""
        src = self.tree()
        os.mkfifo(os.path.join(src, "dev.sock"))
        rc, out, err = self.pp(src, "-n")
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(self.pp(src)[0], 0)
        self.assertFalse(os.path.exists(os.path.join(self.sb.data, "releases", "svc", "current", "dev.sock")))
        self.assertEqual(self.pp("status")[0], 0)

    @unittest.skipIf(os.geteuid() == 0, "root may read every file")
    def test_an_unreadable_file_is_a_blocker_and_leaves_the_other_rows_standing(self):
        """A file pp may not read raised a traceback out of the hash, in the preflight that promises every
        blocker and in the status of every other target with it."""
        src = self.tree()
        self.write("secret.key", "s3cret", root=src)
        self.assertEqual(self.pp(src)[0], 0)
        os.chmod(os.path.join(src, "secret.key"), 0)
        rc, out, err = self.pp(src, "-n")
        self.assertEqual(rc, 2, out + err)
        self.assertIn("pp cannot read it", out)
        self.assertIn("exclude", out)
        self.assertEqual(self.pp(src)[0], 2)
        rc, out, err = self.pp("status")
        self.assertEqual(rc, 0, err)
        self.assertIn("svc", out)
        self.assertIn("pp cannot read it", err)

    def test_an_exclude_pattern_takes_the_file_pp_cannot_read(self):
        """What the blocker offers works: the file named in `exclude` is no part of the release, and the
        promotion goes through."""
        src = self.tree()
        os.mkfifo(os.path.join(src, "dev.sock"))
        self.write(".pp.toml", 'name = "svc"\n[promote]\nsource = "tree"\nexclude = ["dev.sock"]\n', root=src)
        rc, out, err = self.pp(src)
        self.assertEqual(rc, 0, out + err)
        rel = os.path.join(self.sb.data, "releases", "svc", "current")
        self.assertEqual(sorted(os.listdir(rel)), [".pp-release.json", ".pp.toml", "f.txt"])


class TestRollingBackAndForward(Base):
    """The journey the whole command exists for: release, find it bad, go back, fix forward. What the user is
    running is then not the newest release."""

    def releases(self, n: int) -> list[str]:
        made = []
        for i in range(n):
            self.write("v", str(i))
            self.commit(f"v{i}")
            self.assertEqual(self.pp(self.repo)[0], 0)
            made.append(self.current())
        return made

    def test_the_release_a_promote_flips_away_from_stays(self):
        """gc spared the newest releases, `current` and its creation-order predecessor, so the promote after a
        rollback removed the release it had just flipped away from: the known-good one, in the same run."""
        self.make_repo("keep = 2\n")
        a, b = self.releases(2)
        self.assertEqual(self.pp("rollback", "demo", "-y")[0], 0)
        self.assertEqual(self.current(), a)
        self.write("v", "c")
        self.commit("c")
        rc, out, _ = self.pp(self.repo)
        self.assertEqual(rc, 0, out)
        self.assertIn(a, self.stamps())
        self.assertNotIn("gc: removed", out)

    def test_the_sweep_still_removes_what_no_longer_serves(self):
        """The release a promote leaves is one more release that stays, and nothing else changes: at keep = 1
        the fourth promote still removes the oldest."""
        self.make_repo("keep = 1\n")
        made = self.releases(4)
        self.assertNotIn(made[0], self.stamps())
        self.assertEqual(self.stamps(), sorted(made[2:]))


class TestABadValue(Base):
    """config.py promises a loud error naming the file, and `_checked` promises that a misspelt key does not
    release with nothing gating it. A value of the wrong type is the same mistake: it used to pass preflight
    and reach the code that uses it, where it dropped files from the release or raised a traceback."""

    def bad(self, line: str, says: str):
        src = os.path.join(self.tmp, "bad")
        self.write(".pp.toml", f'name = "bad"\n[promote]\nsource = "tree"\n{line}\n', root=src)
        self.write("f.txt", "x", root=src)
        for argv in ((src, "-n"), (src,)):
            rc, out, err = self.pp(*argv)
            self.assertEqual(rc, 1, f"{line}: {out + err}")
            self.assertIn(f"{src}/.pp.toml", err)
            self.assertIn(says, err, line)
        self.assertFalse(os.path.exists(os.path.join(self.sb.data, "releases", "bad")), line)

    def test_a_string_where_a_list_belongs_is_not_a_list_of_characters(self):
        """`exclude = "*.log"` used to become the patterns * . l o g, and `*` drops every file: the release was
        empty and status called it level with the source."""
        self.bad('exclude = "*.log"', "exclude")

    def test_every_value_is_checked_by_its_key(self):
        for line, says in (('exclude = [7]', "exclude"),
                           ('keep = "five"', "keep"),
                           ('check = ["python3", "-c", "print(1)"]', "check"),
                           ('build = 7', "build"),
                           ('links = ["~/.local/bin/x"]', "links"),
                           ('links = { "~/.local/bin/x" = 7 }', "links"),
                           ('dev_links = { "bin/x" = "bin/x" }', "relative")):
            with self.subTest(line=line):
                self.bad(line, says)

    def test_a_name_that_is_not_one_path_component(self):
        """`name` names the releases directory, and `gc` removes directories under it: `name = ".."` made that
        directory the data directory itself, and two promotes removed every release of every project."""
        for name in ("..", ".", "a/b", "sub/", ""):
            with self.subTest(name=name):
                src = os.path.join(self.tmp, "named")
                self.write(".pp.toml", f'name = "{name}"\n[promote]\nsource = "tree"\n', root=src)
                self.write("f.txt", "x", root=src)
                rc, out, err = self.pp(src)
                self.assertEqual(rc, 1, out + err)
                self.assertIn("one path component", err)
        self.assertEqual(sorted(os.listdir(self.sb.data)) if os.path.isdir(self.sb.data) else [], [])

    def test_a_registry_name_that_is_not_one_path_component_is_acted_on_by_nothing(self):
        """0.1.0 checked no name, so a registry it wrote holds keys like `..`, whose releases directory is the
        data directory itself: `gc` of that target removed every other target's releases."""
        for n in ("alpha", "beta"):
            src = os.path.join(self.tmp, n)
            self.write(".pp.toml", f'name = "{n}"\n[promote]\nsource = "tree"\n', root=src)
            self.write("f.txt", n, root=src)
            self.assertEqual(self.pp(src)[0], 0)
        dots = os.path.join(self.tmp, "dots")
        self.write(".pp.toml", 'name = ".."\n[promote]\nsource = "tree"\n', root=dots)
        self.write("f.txt", "x", root=dots)
        reg = dict(self.registry(), **{"..": {"source": dots}})
        with open(os.path.join(self.sb.state, "promote", "registry.json"), "w") as f:
            json.dump(reg, f)

        self.sb.chdir()                 # `..` as a path is then this directory's parent, which is no project
        self.assertNotEqual(self.pp("gc", "..", "--keep", "0", "-y")[0], 0)
        self.assertEqual(sorted(os.listdir(os.path.join(self.sb.data, "releases"))), ["alpha", "beta"])
        self.assertEqual(sorted(os.listdir(self.sb.data)), ["releases"])
        rows = json.loads(self.pp("status", "--json")[1])
        self.assertEqual(sorted(r["name"] for r in rows), ["alpha", "beta"])
        for n in ("alpha", "beta"):
            self.assertEqual(len(json.loads(self.pp("ls", n, "--json")[1])), 1)
        rc, out, err = self.pp(dots)                    # the project itself is where the name is fixed
        self.assertEqual(rc, 1, out)
        self.assertIn("one path component", err)

        rc, out, err = self.pp("status")                # the one verb that surveys the registry says it once
        self.assertEqual(rc, 0, err)
        self.assertIn("not one path component: ..", err)
        self.assertIn(os.path.join(self.sb.state, "promote", "registry.json"), err)
        # the leftovers of this name are where the path led, which for `..` is the directory every target's
        # releases are under: the line names that directory and what in it is this target's, and never the
        # directory itself, whose removal would take every release the user has
        self.assertIn(self.sb.data, err)
        self.assertNotIn(os.path.join(self.sb.data, "releases"), err)
        self.assertIn("revisions.log", err)

    def test_a_name_with_a_space_is_a_name(self):
        """Only a name that would not be one directory is refused: a project called `My Project` is not."""
        src = os.path.join(self.tmp, "spaced")
        self.write(".pp.toml", 'name = "My Project"\n[promote]\nsource = "tree"\n', root=src)
        self.write("f.txt", "x", root=src)
        rc, out, err = self.pp(src)
        self.assertEqual(rc, 0, out + err)
        self.assertTrue(os.path.isdir(os.path.join(self.sb.data, "releases", "My Project")))

    def test_a_name_with_a_dot_in_front_is_a_name(self):
        """With no `name` key the project directory's own name is the target's, so most names are names pp
        derived. A dotted project directory (~/.dotfiles, ~/.emacs.d) is an ordinary one."""
        src = os.path.join(self.tmp, ".dotfiles")
        self.write(".pp.toml", '[promote]\nsource = "tree"\n', root=src)
        self.write("f.txt", "x", root=src)
        rc, out, err = self.pp(src)
        self.assertEqual(rc, 0, out + err)
        self.assertTrue(os.path.isdir(os.path.join(self.sb.data, "releases", ".dotfiles")))
        self.assertIn(".dotfiles", self.registry())
        self.assertIn(".dotfiles", self.pp("status")[1])
        self.assertEqual(len(json.loads(self.pp("ls", ".dotfiles", "--json")[1])), 1)
        self.assertIn(".dotfiles: no release older than", self.pp("rollback", ".dotfiles", "-y")[2])
        self.assertEqual(self.pp("gc", ".dotfiles", "-y")[0], 0)

    def test_one_bad_value_leaves_the_other_targets_listed(self):
        """`status` surveys every target, so a value pp refuses in one project's config is a line about that
        target and the rows of the others still print: the refusal belongs to `pp promote` in that project."""
        for n in ("alpha", "beta"):
            src = os.path.join(self.tmp, n)
            self.write(".pp.toml", f'name = "{n}"\n[promote]\nsource = "tree"\n', root=src)
            self.write("f.txt", n, root=src)
            self.assertEqual(self.pp(src)[0], 0)
        self.write(".pp.toml", 'name = "beta"\n[promote]\nsource = "tree"\nexclude = "*.log"\n',
                   root=os.path.join(self.tmp, "beta"))
        rc, out, err = self.pp("status")
        self.assertEqual(rc, 0, err)
        self.assertIn("exclude", err)
        for n in ("alpha", "beta"):
            self.assertIn(n, out)


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
