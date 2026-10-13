import io
import json
import os
import signal
import sys
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp.commands import batch  # noqa: E402
from pp.lib import proc, ui  # noqa: E402


def touch(path, text=""):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        f.write(text or path)


def read(path):
    with open(path) as f:
        return f.read()


class BatchCase(unittest.TestCase):
    def setUp(self):
        """These tests name files relative to the cwd, as a person does, so the sandbox's work directory is the
        cwd for the test and the one it started in comes back afterwards."""
        self.sb = sandbox.enter(self)
        self.root = self.sb.chdir()

    def run_pp(self, *argv, stdin=""):
        """-> (rc, stdout, stderr); stdin is a non-tty StringIO unless a test patches it."""
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err), mock.patch.object(sys, "stdin", io.StringIO(stdin)):
            try:
                rc = batch.main(list(argv))
            except SystemExit as e:
                rc = e.code
        return rc, out.getvalue(), err.getvalue()

    def interrupted(self, *argv):
        """`pp batch ...` that a Ctrl-C ends -> (stdout, stderr). A command that held the signal back would
        come back with an exit code instead. journey.interruptible is what makes the raised SIGINT arrive."""
        out, err = io.StringIO(), io.StringIO()
        with journey.interruptible(), redirect_stdout(out), redirect_stderr(err), \
                mock.patch.object(sys, "stdin", io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                batch.main(list(argv))
        return out.getvalue(), err.getvalue()

    def journal(self) -> dict:
        """The newest rename journal, as it is on disk."""
        d = os.path.join(self.sb.state, "batch", "journal")
        with open(os.path.join(d, sorted(os.listdir(d))[-1])) as f:
            return json.load(f)

    def tree(self):
        out = []
        for d, dirs, files in os.walk("."):
            dirs.sort()
            out += [os.path.relpath(os.path.join(d, x)) + "/" for x in dirs]
            out += [os.path.relpath(os.path.join(d, x)) for x in sorted(files)]
        return sorted(out)


class TestCollect(BatchCase):
    def test_glob_natural_sort_and_recursive_glob(self):
        for n in ("x1", "x10", "x2"):
            touch(f"a/{n}.txt")
        touch("a/sub/y.txt")
        touch("a/node_modules/z.txt")
        self.assertEqual(batch.collect(["a/*.txt"]), ["a/x1.txt", "a/x2.txt", "a/x10.txt"])
        got = batch.collect(["a/**/*.txt"])
        self.assertIn("a/sub/y.txt", got)
        self.assertNotIn("a/node_modules/z.txt", got)       # pruned unless the pattern names it
        self.assertIn("a/node_modules/z.txt", batch.collect(["a/node_modules/*.txt"]))

    def test_dir_is_item_unless_recursive(self):
        touch("d/f.txt")
        touch("d/.hidden")
        touch("d/s/g.md")
        self.assertEqual(batch.collect(["d"]), ["d"])
        self.assertEqual(set(batch.collect(["d"], recursive=True)), {"d/f.txt", "d/s", "d/s/g.md"})
        self.assertEqual(set(batch.collect(["d"], recursive=True, type_="f")), {"d/f.txt", "d/s/g.md"})
        self.assertEqual(batch.collect(["d"], recursive=True, type_="d"), ["d/s"])
        self.assertEqual(batch.collect(["d"], recursive=True, exts="md"), ["d/s/g.md"])
        self.assertIn("d/.hidden", batch.collect(["d"], recursive=True, hidden=True))

    def test_nothing_matched_exits_1(self):
        rc, _, err = self.run_pp("rename", "a", "b", "nope*.txt", "-y")
        self.assertEqual(rc, 1)
        self.assertIn("nothing matches nope*.txt", err)


class TestTemplate(BatchCase):
    def render(self, t, path="photos/trip/IMG 7.JPG", n=7, N=9, extra=()):
        touch(path)
        return batch.render(batch.compile_template(t, batch.FIELDS | set(extra)), batch._fields(path, n, N, 1))

    def test_fields(self):
        self.assertEqual(self.render("{name}|{stem}|{ext}|{dir}|{parent}|{base}|{n}/{N}"),
                         "IMG 7.JPG|IMG 7|.JPG|photos/trip|trip|photos/trip/IMG 7|7/9")
        self.assertEqual(self.render("{}"), "photos/trip/IMG 7.JPG")
        self.assertRegex(self.render("{mtime}"), r"^\d{8}$")

    def test_formats_filters_and_braces(self):
        self.assertEqual(self.render("{n:03}"), "007")
        self.assertEqual(self.render("{stem:.3}"), "IMG")
        self.assertEqual(self.render("{stem|lower|slug}{ext|lower}"), "img-7.jpg")
        self.assertEqual(self.render("{stem|snake|upper}"), "IMG_7")
        self.assertEqual(self.render("{{{n}}}"), "{7}")
        self.assertEqual(self.render("{stem|slug}", path="My Notes (v2).md"), "my-notes-v2")
        self.assertEqual(self.render("{stem|slug}", path="Café Ünïcode.md"), "cafe-unicode")

    def test_errors_name_the_problem(self):
        with self.assertRaisesRegex(batch.TemplateError, r"unknown field \{bogus\}"):
            batch.compile_template("{bogus}", batch.FIELDS)
        with self.assertRaisesRegex(batch.TemplateError, r"unknown filter \|shout"):
            batch.compile_template("{stem|shout}", batch.FIELDS)
        with self.assertRaisesRegex(batch.TemplateError, "unmatched"):
            batch.compile_template("{stem", batch.FIELDS)
        with self.assertRaisesRegex(batch.TemplateError, "regex groups"):
            batch.compile_template("{1}", batch.FIELDS)
        rc, _, err = self.run_pp("rename", "-t", "{nope}", "whatever")
        self.assertEqual(rc, 2)
        self.assertIn("{nope}", err)


class TestRename(BatchCase):
    def test_regex_backrefs_and_fields(self):
        touch("trip/IMG_12.jpg")
        touch("trip/IMG_3.jpg")
        touch("trip/other.jpg")
        rc, out, _ = self.run_pp("rename", r"^IMG_(?P<num>\d+)", r"{parent}-\1-\g<num>-{num:0>3}", "trip/*.jpg", "-y")
        self.assertEqual(rc, 0, out)
        self.assertEqual(self.tree(), ["trip/", "trip/other.jpg", "trip/trip-12-12-012.jpg", "trip/trip-3-3-003.jpg"])
        self.assertIn("1 unchanged", out)

    def test_the_preview_lines_up_a_double_width_name_with_an_ascii_one(self):
        """The preview is what the user approves before anything is renamed, so its arrows line up. A CJK,
        Japanese or Korean name takes two columns per character, which is what the padding has to count."""
        touch("aaaaaaaa.txt")
        touch("报告报告.txt")
        rc, out, _ = self.run_pp("rename", "-n", "txt", "md", "*.txt")
        self.assertEqual(rc, 0, out)
        arrows = {ui.width(line[:line.index("->")]) for line in out.splitlines() if "->" in line}
        self.assertEqual(len(arrows), 1, out)

    def test_a_name_cannot_redraw_the_preview_it_is_approved_in(self):
        """A file name can hold any byte but a slash and a NUL, so it can hold the escape sequence that clears
        a terminal. The preview is what the user says yes to, so a name is shown rather than sent on: a name
        holding ESC [ 2 J would otherwise wipe the plan off the screen it is being read on. `pp batch undo`
        previews the same names on the way back."""
        touch("plain.txt")
        touch("wipe\x1b[2Jme.txt")
        rc, out, _ = self.run_pp("rename", "-n", "txt", "md", "*.txt")
        self.assertEqual(rc, 0, out)
        self.assertNotIn("\x1b", out)                           # nothing of it reaches the terminal
        self.assertIn("wipe\\x1b[2Jme.txt", out)                # it is there to read, as five characters
        arrows = {ui.width(line[:line.index("->")]) for line in out.splitlines() if "->" in line}
        self.assertEqual(len(arrows), 1, out)                   # and what is shown is what is counted
        rc, out, _ = self.run_pp("rename", "txt", "md", "*.txt", "-y")
        self.assertEqual(rc, 0, out)
        rc, out, _ = self.run_pp("undo", "-n")
        self.assertEqual(rc, 0, out)
        self.assertNotIn("\x1b", out)
        self.assertIn("wipe\\x1b[2Jme.md", out)

    def test_stem_keeps_extension_and_ignore_case(self):
        touch("Draft.draft")
        rc, _, _ = self.run_pp("rename", "-i", "--stem", "draft", "final", "Draft.draft", "-y")
        self.assertEqual(rc, 0)
        self.assertEqual(self.tree(), ["final.draft"])

    def test_template_mode(self):
        for n in ("b", "a", "c"):
            touch(f"pics/{n}.JPG")
        rc, _, _ = self.run_pp("rename", "-t", "{parent}_{n:03}{ext|lower}", "pics/*.JPG", "-y")
        self.assertEqual(rc, 0)
        self.assertEqual(self.tree(), ["pics/", "pics/pics_001.jpg", "pics/pics_002.jpg", "pics/pics_003.jpg"])
        self.assertEqual(read("pics/pics_001.jpg"), "pics/a.JPG")

    def test_collisions_refused_with_every_problem(self):
        touch("a1.txt")
        touch("a2.txt")
        touch("b9.txt")
        touch("taken.md")
        touch("taken.txt")
        rc, _, err = self.run_pp("rename", r"^\w\d", "x", "a1.txt", "a2.txt", "-y")   # two -> one
        self.assertEqual(rc, 2)
        self.assertIn("would all become x.txt", err)
        rc, _, err = self.run_pp("rename", r"(a1|b9)\.txt$", "taken.txt", "a1.txt", "b9.txt", "-y")
        self.assertEqual(rc, 2)
        self.assertIn("would all become taken.txt", err)
        rc, _, err = self.run_pp("rename", r"\.md$", ".txt", "taken.md", "-y")   # target exists
        self.assertEqual(rc, 2)
        self.assertIn("taken.txt already exists", err)
        rc, _, err = self.run_pp("rename", "-t", "{stem}x.txt", "a1.txt", "a2.txt", "taken.md", "-y")
        self.assertEqual(rc, 0)   # control: no collision -> applies
        touch("p.txt")
        touch("q.txt")
        touch("r.txt")
        touch("pq.txt")
        rc, _, err = self.run_pp("rename", "-t", "pq.txt", "p.txt", "q.txt", "r.txt", "-y")
        self.assertEqual(rc, 2)
        self.assertEqual(err.count("would all become"), 1)
        rc, _, err = self.run_pp("rename", r"^(p|q)\.", r"\1q.", "p.txt", "q.txt", "-y")
        self.assertEqual(rc, 2)
        self.assertIn("pq.txt already exists", err)
        self.assertIn("qq.txt", self.run_pp("rename", r"^(p|q)\.", r"\1q.", "p.txt", "q.txt", "-n")[1])
        rc, _, err = self.run_pp("rename", "-t", "", "r.txt", "-y")
        self.assertEqual(rc, 2)
        self.assertIn("not a valid name", err)

    def test_all_problems_listed_at_once(self):
        for f in ("a.txt", "b.txt", "c.txt", "d.txt", "x.md", "y.md"):
            touch(f)
        rc, _, err = self.run_pp("rename", r"^[ab]\.", "c.", "a.txt", "b.txt", "x.md", "-y")
        self.assertEqual(rc, 2)
        self.assertIn("would all become c.txt", err)
        rc, _, err = self.run_pp("rename", r"^(.)\.md$", r"\1.txt", "x.md", "y.md", "-y")
        self.assertEqual(rc, 0)
        touch("x.md")
        touch("y.md")
        rc, _, err = self.run_pp("rename", r"^(.)\.md$", r"\1.txt", "x.md", "y.md", "-y")
        self.assertEqual(rc, 2)
        self.assertIn("2 problem(s)", err)
        self.assertIn("x.txt already exists", err)
        self.assertIn("y.txt already exists", err)

    def test_swap(self):
        touch("1.txt", "one")
        touch("2.txt", "two")
        rc, _, _ = self.run_pp("rename", "-t", "{n}{ext}", "2.txt", "1.txt", "-y")   # arg order: 2 -> 1, 1 -> 2
        self.assertEqual(rc, 0)
        self.assertEqual((read("1.txt"), read("2.txt")), ("two", "one"))
        self.assertEqual(self.tree(), ["1.txt", "2.txt"])

    def test_chain(self):
        touch("1.txt", "one")
        touch("2.txt", "two")
        rc, _, _ = self.run_pp("rename", "-t", "{n}{ext}", "--start", "2", "1.txt", "2.txt", "-y")
        self.assertEqual(rc, 0)
        self.assertEqual(self.tree(), ["2.txt", "3.txt"])
        self.assertEqual((read("2.txt"), read("3.txt")), ("one", "two"))

    def test_dir_and_children_then_undo(self):
        touch("root/Old Dir/My File.txt", "x")
        touch("root/Old Dir/Deeper Dir/A B.md", "y")
        touch("root/keep.txt", "k")
        before = self.tree()
        rc, out, _ = self.run_pp("rename", " ", "_", "-r", "root", "-y")
        self.assertEqual(rc, 0, out)
        self.assertEqual(self.tree(), ["root/", "root/Old_Dir/", "root/Old_Dir/Deeper_Dir/",
                                       "root/Old_Dir/Deeper_Dir/A_B.md", "root/Old_Dir/My_File.txt", "root/keep.txt"])
        rc, out, _ = self.run_pp("undo", "-y")
        self.assertEqual(rc, 0, out)
        self.assertEqual(self.tree(), before)
        self.assertEqual(read("root/Old Dir/Deeper Dir/A B.md"), "y")
        rc, _, err = self.run_pp("undo", "-y")    # nothing left to undo
        self.assertEqual(rc, 1)
        rc, out, _ = self.run_pp("undo", "--list")
        self.assertIn("undone", out)

    def test_undo_walks_back_through_history(self):
        touch("a.txt")
        self.run_pp("rename", "a", "b", "a.txt", "-y")
        self.run_pp("rename", "b", "c", "b.txt", "-y")
        self.run_pp("undo", "-y")
        self.assertEqual(self.tree(), ["b.txt"])
        self.run_pp("undo", "-y")
        self.assertEqual(self.tree(), ["a.txt"])

    def half_renamed(self, status: str):
        """A rename of b0..b9 to c0..c9 that ended after five of them, as a kill or a reboot leaves it: the
        journal says `status` and says nothing about which items landed. -> its path.

        A SIGKILL cannot be held back, so no journal repair runs and the directory is left half renamed.
        Written out here rather than killed, so the half is the same half on every machine."""
        jpath = os.path.join(batch._journal_dir(), "29991231-235959-000000.json")
        entries = [{"parent": os.getcwd(), "old": f"b{i}.txt", "new": f"c{i}.txt",
                    "depth": os.getcwd().count(os.sep)} for i in range(10)]
        for i in range(5):
            os.rename(f"b{i}.txt", f"c{i}.txt")
        batch._write_journal(jpath, {"created": time.time(), "cwd": os.getcwd(), "argv": [],
                                     "status": status, "entries": entries})
        return jpath

    def test_undo_takes_the_journal_of_the_rename_that_was_killed(self):
        """A rename ended by a kill, a closed session or a reboot leaves its journal saying nothing about
        which items landed, and the directory half renamed. That half-done rename is the one to reverse, so
        undo takes its journal and tests every item on disk: the items that landed go back, and the ones the
        rename never reached are named. Reversing the rename before it instead would put a third generation
        of names into the directory, and the next step undo prints would be about that other rename."""
        for i in range(10):
            touch(f"a{i}.txt")
        self.run_pp("rename", "^a", "b", "*.txt", "-y")                  # the rename before: applied
        jpath = self.half_renamed("pending")
        rc, out, err = self.run_pp("undo", "-n")
        self.assertEqual(rc, 2, out + err)
        self.assertIn("29991231-235959-000000 ended before it said what it renamed", err)
        self.assertIn(jpath, err)
        self.assertIn("undo 29991231-235959-000000", out)                # the killed rename, not the one before
        self.assertIn("b9.txt is at its old name already, so there is nothing to put back", err)
        self.assertIn("pp batch undo --partial", err)
        rc, out, err = self.run_pp("undo", "--partial", "-y")            # the step the refusal names
        self.assertEqual(rc, 0, out + err)
        self.assertIn("restored 5 item(s); 5 left as they are", out)
        self.assertEqual(self.tree(), [f"b{i}.txt" for i in range(10)])  # one generation of names, not three
        rc, out, err = self.run_pp("undo", "-y")                         # and the rename before is next
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(self.tree(), [f"a{i}.txt" for i in range(10)])

    def test_undo_takes_a_journal_whose_rollback_stopped_part_way(self):
        """A rename whose own rollback stopped part way leaves items at half-renamed names, and its journal
        says "failed". Which of its items are on disk is as unknown as a killed rename's, so undo takes it
        the same way.

        What each item it cannot put back says comes from disk: an item the rollback did put back is at its
        old name with nothing to do, and only one that is at neither name is one to go looking for."""
        for i in range(10):
            touch(f"b{i}.txt")
        jpath = self.half_renamed("failed")
        os.remove("c4.txt")                              # at neither name, as a killed swap leaves one
        rc, out, err = self.run_pp("undo", "-n")
        self.assertEqual(rc, 2, out + err)
        self.assertIn("b9.txt is at its old name already, so there is nothing to put back", err)
        self.assertIn("c4.txt is gone", err)
        self.assertIn(".staging-batch- name", err)       # where such an item is, if it is anywhere
        self.assertNotIn("b9.txt is gone", err)
        touch("c4.txt")                                  # found again, so the rest of the undo can go ahead
        rc, out, err = self.run_pp("undo", "--partial", "-y")
        self.assertEqual(rc, 0, out + err)
        self.assertIn("stopped part way through its own rollback", err)
        self.assertIn(jpath, err)
        self.assertEqual(self.tree(), [f"b{i}.txt" for i in range(10)])
        self.assertEqual(batch._journal(jpath)["status"], "undone")
        # a rolled-back rename put every item back itself, so there is nothing for undo to say about it
        touch("a.txt")
        self.run_pp("rename", "a", "b", "a.txt", "-y")
        batch._write_journal(jpath, {"created": time.time(), "cwd": os.getcwd(), "argv": [],
                                     "status": "rolled-back", "entries": []})
        rc, out, err = self.run_pp("undo", "-y")
        self.assertEqual((rc, err), (0, ""), out)
        self.assertIn("a.txt", self.tree())

    def test_undo_refuses_when_state_changed(self):
        touch("one.txt")
        touch("two.txt")
        self.run_pp("rename", r"\.txt$", ".md", "one.txt", "two.txt", "-y")
        os.remove("one.md")
        touch("two.txt", "squatter")
        rc, _, err = self.run_pp("undo", "-y")
        self.assertEqual(rc, 2)
        self.assertIn("one.md is gone", err)
        self.assertIn("two.txt exists now", err)
        self.assertEqual(self.tree(), ["two.md", "two.txt"])

    def test_a_ctrl_c_part_way_through_a_rename_leaves_what_landed_to_undo(self):
        """The renames are what a person waits for, so a Ctrl-C ends them where it lands. The journal then
        holds the renames that landed and nothing else, so `pp batch undo` is about what is on disk, and the
        rename before this one is not what it offers."""
        touch("prev.txt")
        self.run_pp("rename", "prev", "PREV", "prev.txt", "-y")
        for i in range(1, 5):
            touch(f"a{i}.txt")
        real, moved = batch._mv, []

        def after_two(src, dst, done):
            real(src, dst, done)
            moved.append(dst)
            if len(moved) == 2:
                proc.raise_to_self(signal.SIGINT)       # what a terminal sends pp on Ctrl-C

        with mock.patch.object(batch, "_mv", after_two):
            _, err = self.interrupted("rename", "^a", "b", "a*.txt", "-y")
        self.assertEqual(self.tree(), ["PREV.txt", "a3.txt", "a4.txt", "b1.txt", "b2.txt"])
        self.assertIn("stopped with 2 of 4 item(s) renamed; undo with: pp batch undo", err)
        j = self.journal()
        self.assertEqual(j["status"], "applied")
        self.assertEqual([e["new"] for e in j["entries"]], ["b1.txt", "b2.txt"])
        rc, out, err = self.run_pp("undo", "-y")
        self.assertEqual(rc, 0, err)
        self.assertIn("restored 2 item(s)", out)
        self.assertEqual(self.tree(), ["PREV.txt", "a1.txt", "a2.txt", "a3.txt", "a4.txt"])

    def test_a_rename_that_landed_nothing_is_not_an_undo_of_its_own(self):
        """Nothing is renamed, so there is nothing to put back: the journal says so, and undo walks past it to
        the rename before."""
        touch("prev.txt")
        self.run_pp("rename", "prev", "PREV", "prev.txt", "-y")
        touch("a.txt")

        def at_once(src, dst, done):
            proc.raise_to_self(signal.SIGINT)

        with mock.patch.object(batch, "_mv", at_once):
            _, err = self.interrupted("rename", "a", "b", "a.txt", "-y")
        self.assertIn("stopped with 0 of 1 item(s) renamed", err)
        self.assertNotIn("undo with", err)
        self.assertEqual(self.journal()["status"], "rolled-back")
        rc, out, _ = self.run_pp("undo", "-y")
        self.assertEqual(rc, 0, out)
        self.assertEqual(self.tree(), ["a.txt", "prev.txt"])

    def test_a_ctrl_c_part_way_through_an_undo_leaves_the_rest_to_undo(self):
        """The renames are what a person waits for, so a Ctrl-C ends them where it lands. The journal then
        holds the ones still renamed, and a second `pp batch undo` puts those back and nothing else."""
        for i in range(1, 5):
            touch(f"a{i}.txt")
        self.run_pp("rename", "^a", "b", "*.txt", "-y")
        self.assertEqual(self.tree(), ["b1.txt", "b2.txt", "b3.txt", "b4.txt"])
        real, moved = batch._mv, []

        def after_two(src, dst, done):
            real(src, dst, done)
            moved.append(dst)
            if len(moved) == 2:
                proc.raise_to_self(signal.SIGINT)       # what a terminal sends pp on Ctrl-C

        with mock.patch.object(batch, "_mv", after_two):
            _, err = self.interrupted("undo", "-y")
        self.assertEqual(self.tree(), ["a1.txt", "a2.txt", "b3.txt", "b4.txt"])
        self.assertIn("stopped with 2 of 4 item(s) back", err)
        j = self.journal()
        self.assertEqual(j["status"], "applied")
        self.assertEqual([e["new"] for e in j["entries"]], ["b3.txt", "b4.txt"])
        self.assertEqual([e["new"] for e in j["restored"]], ["b1.txt", "b2.txt"])
        rc, out, err = self.run_pp("undo", "-y")
        self.assertEqual(rc, 0, err)
        self.assertIn("restored 2 item(s)", out)
        self.assertEqual(self.tree(), ["a1.txt", "a2.txt", "a3.txt", "a4.txt"])
        self.assertEqual(self.journal()["status"], "undone")

    def test_a_ctrl_c_inside_a_cycle_leaves_no_file_at_a_temporary_name(self):
        """A group whose new names are its own old ones is renamed in two passes, through `.staging-` names
        that no journal and no listing holds. A Ctrl-C in the middle of it waits for the group, so every file
        is back at a name the journal knows, and the next `pp batch undo` takes the group that is left."""
        for d in ("one", "two"):
            for i in (1, 2, 3):
                touch(f"{d}/f{i}.txt")
        self.run_pp("rename", "-t", "f{i}.txt", "--start", "2", "one/*.txt", "two/*.txt", "-y")
        renamed = ["one/", "one/f2.txt", "one/f3.txt", "one/f4.txt", "two/", "two/f2.txt", "two/f3.txt",
                   "two/f4.txt"]
        self.assertEqual(self.tree(), renamed, "each folder chains f1->f2->f3->f4")
        real, moved = batch._mv, []

        def after_one(src, dst, done):
            real(src, dst, done)
            moved.append(dst)
            if len(moved) == 1:
                proc.raise_to_self(signal.SIGINT)       # what a terminal sends pp on Ctrl-C, mid-cycle

        with mock.patch.object(batch, "_mv", after_one):
            _, err = self.interrupted("undo", "-y")
        self.assertEqual(self.tree(), ["one/", "one/f1.txt", "one/f2.txt", "one/f3.txt", "two/", "two/f2.txt",
                                       "two/f3.txt", "two/f4.txt"])
        self.assertIn("stopped with 3 of 6 item(s) back; `pp batch undo` restores the rest", err)
        j = self.journal()
        self.assertEqual(j["status"], "applied")
        self.assertEqual({e["parent"].rsplit(os.sep, 1)[-1] for e in j["entries"]}, {"two"})
        self.assertEqual({e["parent"].rsplit(os.sep, 1)[-1] for e in j["restored"]}, {"one"})
        rc, out, err = self.run_pp("undo", "-y")
        self.assertEqual(rc, 0, err)
        self.assertIn("restored 3 item(s)", out)
        self.assertEqual(self.tree(), ["one/", "one/f1.txt", "one/f2.txt", "one/f3.txt", "two/", "two/f1.txt",
                                       "two/f2.txt", "two/f3.txt"])
        self.assertEqual(self.journal()["status"], "undone")

    def test_an_interrupted_partial_undo_names_the_command_that_takes_the_rest(self):
        """Something is gone, so only `pp batch undo --partial` gets past the refusal. That is the command the
        interrupted undo names, and running it restores what is left."""
        for d in ("one", "two", "three"):
            for i in (1, 2, 3) if d == "one" else (1,):
                touch(f"{d}/f{i}.txt")
        self.run_pp("rename", "-t", "f{i}.txt", "--start", "2", "one/*.txt", "two/*.txt", "three/*.txt", "-y")
        os.remove("three/f2.txt")
        self.assertEqual(self.run_pp("undo", "-y")[0], 2, "a gone file refuses a whole undo")
        real, moved = batch._mv, []

        def after_one(src, dst, done):
            real(src, dst, done)
            moved.append(dst)
            if len(moved) == 1:
                proc.raise_to_self(signal.SIGINT)

        with mock.patch.object(batch, "_mv", after_one):
            _, err = self.interrupted("undo", "--partial", "-y")
        self.assertIn("stopped with 3 of 4 item(s) back; `pp batch undo --partial` restores the rest", err)
        self.assertEqual(self.tree(), ["one/", "one/f1.txt", "one/f2.txt", "one/f3.txt", "three/", "two/",
                                       "two/f2.txt"])
        rc, out, err = self.run_pp("undo", "--partial", "-y")
        self.assertEqual(rc, 0, err)
        self.assertIn("restored 1 item(s)", out)
        self.assertEqual(self.tree(), ["one/", "one/f1.txt", "one/f2.txt", "one/f3.txt", "three/", "two/",
                                       "two/f1.txt"])
        j = self.journal()
        self.assertEqual(j["status"], "undone")
        self.assertEqual([e["new"] for e in j["left"]], ["f2.txt"])

    def test_a_journal_undone_while_this_one_asked_is_refused(self):
        """Two `pp batch undo` at once must not take the same journal and reverse it twice. The second one
        reads the journal under the journals' lock, so it sees what the first recorded."""
        touch("a.txt")
        self.run_pp("rename", "a", "b", "a.txt", "-y")
        asked = journey.Gate()

        def proceed(a, prompt):
            """The question is the window: another undo runs to the end while this one is on it."""
            asked.once(lambda: self.assertEqual(self.run_pp("undo", "-y")[0], 0))
            return None

        with mock.patch.object(batch, "_proceed", proceed):
            rc, _, err = self.run_pp("undo", "-y")
        self.assertEqual((rc, asked.ran), (1, 1), err)
        self.assertIn("was undone while this asked", err)
        self.assertEqual(self.tree(), ["a.txt"])            # restored once, not twice

    def test_dry_run_and_non_tty_change_nothing(self):
        touch("a b.txt")
        rc, out, _ = self.run_pp("rename", " ", "_", "a b.txt", "-n")
        self.assertEqual(rc, 0)
        self.assertIn("a_b.txt", out)
        self.assertEqual(self.tree(), ["a b.txt"])
        rc, _, err = self.run_pp("rename", " ", "_", "a b.txt")        # stdin is not a terminal
        self.assertEqual(rc, 2)
        self.assertIn("pass -y", err)
        self.assertEqual(self.tree(), ["a b.txt"])

    def test_interactive_yes_and_no(self):
        touch("a b.txt")
        tty = io.StringIO("n\n")
        tty.isatty = lambda: True
        out = io.StringIO()
        with redirect_stdout(out), mock.patch.object(sys, "stdin", tty), mock.patch("builtins.input", return_value="n"):
            self.assertEqual(batch.main(["rename", " ", "_", "a b.txt"]), 2)
        self.assertEqual(self.tree(), ["a b.txt"])
        with redirect_stdout(out), mock.patch.object(sys, "stdin", tty), mock.patch("builtins.input", return_value="y"):
            self.assertEqual(batch.main(["rename", " ", "_", "a b.txt"]), 0)
        self.assertEqual(self.tree(), ["a_b.txt"])

    def test_failure_midway_rolls_back(self):
        touch("a.txt")
        touch("b.txt")
        real = os.rename
        calls = []

        def flaky(src, dst):
            calls.append(src)
            if len(calls) == 2:
                raise PermissionError("simulated")
            return real(src, dst)

        with mock.patch.object(batch.os, "rename", flaky):
            rc, _, err = self.run_pp("rename", r"\.txt$", ".md", "a.txt", "b.txt", "-y")
        self.assertEqual(rc, 1)
        self.assertIn("rolled back", err)
        self.assertEqual(self.tree(), ["a.txt", "b.txt"])
        rc, out, _ = self.run_pp("undo", "--list")
        self.assertIn("rolled-back", out)


HOSTILE = ["a;touch PWNED-semi;.txt", "b|touch PWNED-pipe|.txt", "c&touch PWNED-amp&.txt",
           "d$(touch PWNED-sub).txt", "e`touch PWNED-tick`.txt", "f'g\"h.txt", "i\nj.txt", "-k.txt"]


class TestRun(BatchCase):
    def test_a_name_that_holds_a_command_is_one_word_wherever_a_field_may_sit(self):
        """A file name can hold any byte but a slash and a NUL, so it can hold every character a shell reads
        as code, and these names do. In every position a field may sit in, the name reaches the command as
        one word: each command reads the file it is about, and nothing the name holds runs."""
        for i, n in enumerate(HOSTILE):
            touch(n, f"[{i}]")                                  # what only this file holds
        for tmpl in ("cat {}", "cat '{}'", 'cat "{}"', "cat -- {}", "case {stem} in *) cat {};; esac",
                     "if true; then cat {}; fi", """sh -c 'cat "$1"' sh {}"""):
            rc, out, err = self.run_pp("run", tmpl, "*.txt", "-y", "--keep-going")
            self.assertEqual(rc, 0, f"{tmpl}\n{out}{err}")
            for i in range(len(HOSTILE)):
                self.assertIn(f"[{i}]", out, tmpl)              # every command read the file it is about
            self.assertEqual(self.tree(), sorted(HOSTILE), tmpl)    # and nothing else happened
        # a name is one word as a redirection's target too, so each file is copied to its own name
        rc, out, err = self.run_pp("run", "cat {} > {out}", "*.txt", "--out", "copy/{stem}.c", "-y")
        self.assertEqual(rc, 0, out + err)
        self.assertEqual([read(f"copy/{os.path.splitext(n)[0]}.c") for n in HOSTILE],
                         [f"[{i}]" for i in range(len(HOSTILE))])
        copies = ["copy/"] + [f"copy/{os.path.splitext(n)[0]}.c" for n in HOSTILE]
        self.assertEqual(self.tree(), sorted(HOSTILE + copies))     # the copies, and nothing a name made

    def test_a_name_field_a_shell_would_read_again_is_refused(self):
        """A value a shell reads a second time is a value it can read as code, and quoting it for the word it
        sits in says nothing about that second reading. Every such position is refused before anything runs,
        and the refusal names the position and the spelling that hands the name over as an argument."""
        touch("a;touch PWNED;.txt", "[0]")
        rc, out, err = self.run_pp("run", 'echo "$(head -c 3 {})"', "*.txt", "-y", "--keep-going")
        self.assertEqual(rc, 2, out + err)
        self.assertEqual(self.tree(), ["a;touch PWNED;.txt"])   # what the name holds is no command of its own
        touch("a.txt")
        refused = {"echo $(basename {})": "inside $( )",
                   "echo `basename {}`": "inside backticks",
                   "echo ${{a{}b}}": "inside ${ }",
                   "cat \"$'{}'\"": "inside $'...'",
                   "echo $(( {stem} + 1 ))": "inside $(( ))",
                   "(( {stem} ))": "inside (( ))",
                   "echo $[ {stem} ]": "inside $[ ]",
                   "[[ {} = x ]]": "inside a [[ ]] test",
                   "case x in {stem}) echo hi;; esac": "in a case pattern",
                   "echo hi >& {}": "in the target of a >& or a <&",
                   "echo \\{}": "right after a \\",
                   "echo ${}": "right after a $",
                   "echo hi # {}": "in a comment",
                   "eval cat {}": "inside eval",
                   'trap "cat {}" INT': "inside trap",
                   "let x={stem}": "inside let",
                   "sh -c 'cat {}'": "in the script of a shell's -c",
                   'timeout 5 env A=1 bash -ec "cat {}"': "in the script of a shell's -c",
                   "cat {} <<EOF\nx\nEOF\n": "in a template holding a <<",
                   "f() {{ cat {}; }}": "in a template that defines a function",
                   "function f {{ cat {}; }}": "in a template that defines a function"}
        for tmpl, why in refused.items():
            rc, out, err = self.run_pp("run", tmpl, "a.txt", "-y")
            self.assertEqual(rc, 2, f"{tmpl}\n{out}{err}")
            self.assertIn(why, err, tmpl)
            self.assertIn("""sh -c 'cmd "$1"' sh {}""", err, tmpl)
        self.assertEqual(self.tree(), ["a.txt", "a;touch PWNED;.txt"])      # not one of them ran
        # pp's own numbers are digits pp writes, so they sit in those positions, and the word after a
        # script is an argument of that shell again, which is the spelling the refusals name
        rc, out, err = self.run_pp("run", "echo $(( {n} + 1 ))", "a.txt", "-y")
        self.assertEqual((rc, out.count("2")), (0, 1), out + err)
        rc, out, err = self.run_pp("run", """sh -c 'cat "$1"' sh {}""", "a.txt", "-y")
        self.assertEqual(rc, 0, out + err)
        self.assertIn("a.txt", out)

    def test_the_plan_shows_every_command_it_will_run(self):
        """The plan is what the user says yes to. A name decides how its command reads, so ten of them stand
        for nothing: the one that differs is the one a cap hides."""
        for i in range(12):
            touch(f"f{i}.txt")
        rc, out, _ = self.run_pp("run", "cat {}", "*.txt", "-n")
        self.assertEqual(rc, 0, out)
        self.assertEqual(out.count("$ cat "), 12)
        self.assertNotIn("more", out)

    def test_quoting_names_with_spaces_and_quotes(self):
        name = "it's a \"file\" $HOME.txt"
        touch(name, "payload")
        rc, out, _ = self.run_pp("run", "cp {} {out} && echo done", name, "--out", "out/{stem}.bak", "-y")
        self.assertEqual(rc, 0, out)
        self.assertEqual(read("out/it's a \"file\" $HOME.bak"), "payload")
        self.assertIn("done", out)

    def test_out_skips_existing_unless_overwrite(self):
        touch("a.txt")
        touch("b.txt")
        touch("a.out", "old")
        rc, out, _ = self.run_pp("run", "echo new > {out}", "*.txt", "--out", "{base}.out", "-y")
        self.assertEqual(rc, 0)
        self.assertIn("1 skipped", out)
        self.assertEqual((read("a.out"), read("b.out")), ("old", "new\n"))
        rc, out, _ = self.run_pp("run", "echo new > {out}", "*.txt", "--out", "{base}.out", "-y")
        self.assertIn("0 command(s) to run", out)
        rc, out, _ = self.run_pp("run", "echo new > {out}", "*.txt", "--out", "{base}.out", "--overwrite", "-y")
        self.assertEqual(read("a.out"), "new\n")

    def test_parallel(self):
        for i in range(8):
            touch(f"f{i}.txt")
        rc, out, _ = self.run_pp("run", "echo {n} > {base}.n", "f*.txt", "-j", "4", "-y", "-q")
        self.assertEqual(rc, 0)
        self.assertEqual(sorted(read(f"f{i}.n").strip() for i in range(8)), [str(i) for i in range(1, 9)])
        self.assertIn("8 ok, 0 failed", out)

    def test_two_jobs_over_one_out_are_refused(self):
        touch("a.txt")
        touch("b.txt")
        # gated so the failing job always finishes after the one that wrote the output: b waits for a's token
        cmd = ("case {stem} in a) echo A > {out}; touch tok;; "
               "b) n=0; until [ -f tok ]; do n=$((n+1)); [ $n -gt 400 ] && break; sleep 0.05; done; exit 1;; esac")
        rc, out, err = self.run_pp("run", cmd, "*.txt", "--out", "one.out", "-j", "2", "-y")
        self.assertEqual(rc, 2, out + err)
        self.assertIn("a.txt, b.txt would all write one.out", err)
        self.assertIn("{stem}", err)
        self.assertEqual(self.tree(), ["a.txt", "b.txt"])       # neither of them ran
        # an {out} that differs per item is what it asks for: then a's output is a's own, and only b's goes
        rc, out, _ = self.run_pp("run", cmd, "*.txt", "--out", "{stem}.out", "-j", "2", "-y")
        self.assertEqual(rc, 1, out)
        self.assertEqual(read("a.out"), "A\n")

    def test_failure_stops_unless_keep_going(self):
        for n in ("a", "b", "c"):
            touch(f"{n}.txt")
        cmd = "case {stem} in b) echo boom >&2; exit 3;; esac; touch {base}.done"
        rc, out, _ = self.run_pp("run", cmd, "*.txt", "-y")
        self.assertEqual(rc, 1)
        self.assertIn("FAILED rc=3", out)
        self.assertIn("boom", out)
        self.assertIn("1 not run", out)
        self.assertEqual([os.path.exists(f"{n}.done") for n in "abc"], [True, False, False])
        rc, out, _ = self.run_pp("run", cmd, "*.txt", "-y", "--keep-going")
        self.assertEqual(rc, 1)
        self.assertTrue(os.path.exists("c.done"))

    def test_a_partial_output_that_cannot_be_removed_still_leaves_a_summary(self):
        """A failed command's half-written output is removed so the next run does not take it for a finished one.
        When the removal itself fails, the item says so and the run still reports what became of every item."""
        touch("a.txt")
        touch("b.txt")
        with mock.patch.object(batch.os, "remove", side_effect=PermissionError("simulated")):
            rc, out, _ = self.run_pp("run", "echo half > {out}; exit 4", "*.txt", "--out", "{base}.out",
                                     "-y", "--keep-going")
        self.assertEqual(rc, 1)
        self.assertIn("a.out is still there: simulated", out)
        self.assertIn("0 ok, 2 failed", out)
        self.assertEqual((read("a.out"), read("b.out")), ("half\n", "half\n"))

    def test_a_name_in_a_command_and_a_commands_own_output_are_shown_not_sent(self):
        """Quoting a name for the shell does nothing about the terminal, so the plan of commands shows an
        escape sequence in a name the way the rename preview does. What each command prints is another
        program's output, and it is shown too."""
        touch("wipe\x1b[2Jme.txt")
        rc, out, _ = self.run_pp("run", "echo {}", "*.txt", "-n")
        self.assertEqual(rc, 0, out)
        self.assertNotIn("\x1b", out)
        self.assertIn("wipe\\x1b[2Jme.txt", out)
        rc, out, _ = self.run_pp("run", "printf 'from\\033[2Jthe command'", "*.txt", "-y")
        self.assertEqual(rc, 0, out)
        self.assertNotIn("\x1b", out)
        self.assertIn("from\\x1b[2Jthe command", out)
        self.assertIn("1 ok, 0 failed", out)

    def test_dry_run_and_refusal(self):
        touch("a.txt")
        rc, out, _ = self.run_pp("run", "touch {base}.x", "a.txt", "-n")
        self.assertEqual(rc, 0)
        self.assertIn("$ touch a.x", out)
        rc, _, _ = self.run_pp("run", "touch {base}.x", "a.txt")
        self.assertEqual(rc, 2)
        self.assertFalse(os.path.exists("a.x"))

    def test_presets_from_config(self):
        cfg = self.sb.dir("config")
        with open(os.path.join(cfg, "config.toml"), "w") as f:
            f.write('[batch.presets]\nbak = { cmd = "cp {} {out}", out = "{base}.bak" }\n'
                    'up = "echo {stem|upper} > {base}.up"\n')
        touch("a.txt", "A")
        rc, out, _ = self.run_pp("presets")
        self.assertIn("@bak", out)
        self.assertIn("@up", out)
        self.assertEqual(self.run_pp("run", "@bak", "a.txt", "-y")[0], 0)
        self.assertEqual(read("a.bak"), "A")
        self.assertEqual(self.run_pp("run", "@up", "a.txt", "-y")[0], 0)
        self.assertEqual(read("a.up"), "A\n")
        rc, _, err = self.run_pp("run", "@nope", "a.txt", "-y")
        self.assertEqual(rc, 2)
        self.assertIn("@bak", err)


if __name__ == "__main__":
    unittest.main()
