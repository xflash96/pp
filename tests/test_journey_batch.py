"""User journeys through `pp batch`: a person tidies folders of photos and notes, changes their mind, converts
files in bulk and reruns after a failure. Each step is a fresh `pp` in a sandbox, checked by what is on screen
and what is on disk."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from journey import Journey  # noqa: E402


class TestRenameJourneys(Journey):
    def files(self, root=None):
        root = root or self.work
        return sorted(os.path.relpath(os.path.join(d, f), root)
                      for d, dirs, fs in os.walk(root) for f in fs + dirs)

    def touch(self, *names):
        for n in names:
            self.write(f"work/{n}", "")

    def test_a_replacement_that_starts_with_a_dash(self):
        self.touch("a.md")
        self.assertIn("FIND REPLACE PATHS...", self.pp("batch", "rename", "-h").stdout.splitlines()[0])
        self.pp("batch", "rename", "-y", "--", "^", "-draft-", "*.md")
        self.assertEqual(self.files(), ["-draft-a.md"])
        self.pp("batch", "rename", "-y", "--template=x{stem}{ext}", "*.md")
        self.assertEqual(self.files(), ["x-draft-a.md"])

    def test_photo_folders_numbered_per_folder_previewed_applied_and_undone(self):
        self.touch("photos/trip/IMG_2.jpg", "photos/trip/IMG_10.jpg", "photos/home/a.jpg")
        before = self.files()
        # no terminal: the plan is shown, nothing happens, exit 2 says "refused"
        r = self.pp("batch", "rename", "-t", "{parent}_{i:03}{ext}", "photos/**/*.jpg", code=2)
        self.assertIn("photos/trip/IMG_2.jpg", r.stdout)
        self.assertIn("pass -y", r.stderr)
        self.assertEqual(self.files(), before)
        self.pp("batch", "rename", "-t", "{parent}_{i:03}{ext}", "photos/**/*.jpg", "-n")
        self.assertEqual(self.files(), before)
        r = self.pp("batch", "rename", "-t", "{parent}_{i:03}{ext}", "photos/**/*.jpg", "-y")
        self.assertIn("undo with: pp batch undo", r.stdout)
        # natural order inside each folder: IMG_2 before IMG_10, and numbering restarts per folder
        self.assertEqual([f for f in self.files() if f.endswith(".jpg")],
                         ["photos/home/home_001.jpg", "photos/trip/trip_001.jpg", "photos/trip/trip_002.jpg"])
        self.assertIn("applied", self.pp("batch", "undo", "--list").stdout)
        self.pp("batch", "undo", code=2)                            # undo previews and asks too
        self.pp("batch", "undo", "-y")
        self.assertEqual(self.files(), before)
        r = self.pp("batch", "undo", "-y", code=1)
        self.assertIn("nothing to undo", r.stderr)

    def test_help_examples_do_what_they_say(self):
        self.touch("a.jpeg", "IMG_42.jpg", "Draft plan.md", "notes/My Notes (v2).md", "my file.txt")
        self.pp("batch", "rename", r"\.jpeg$", ".jpg", "*.jpeg", "-y")
        self.pp("batch", "rename", r"^IMG_(\d+)", r"photo-\1", "*.jpg", "-y")
        self.pp("batch", "rename", "-i", "draft", "final", "--stem", "*.md", "-y")
        self.pp("batch", "rename", "-t", "{stem|slug}{ext}", "notes/*.md", "-y")
        self.pp("batch", "rename", " ", "_", "*", "-y")
        self.assertEqual(self.files(), ["a.jpg", "final_plan.md", "my_file.txt", "notes", "notes/my-notes-v2.md",
                                        "photo-42.jpg"])
        for _ in range(5):                                          # each undo goes one rename further back
            self.pp("batch", "undo", "-y")
        self.assertEqual(self.files(), ["Draft plan.md", "IMG_42.jpg", "a.jpeg", "my file.txt", "notes",
                                        "notes/My Notes (v2).md"])

    def test_unicode_and_names_that_look_like_options(self):
        self.touch("héllo wörld.txt", "-dash file.txt")
        self.pp("batch", "rename", " ", "_", "*.txt", "-y")
        self.assertEqual(self.files(), ["-dash_file.txt", "héllo_wörld.txt"])
        # a FIND that starts with '-' goes after --, like any other command
        self.pp("batch", "rename", "-y", "--", "^-", "", "-dash_file.txt")
        self.assertEqual(self.files(), ["dash_file.txt", "héllo_wörld.txt"])

    def test_collisions_are_all_listed_and_nothing_moves(self):
        self.touch("a1.txt", "a2.txt", "b.txt", "c.txt")
        r = self.pp("batch", "rename", r"\d|c", "", "*.txt", "-y", code=2)
        self.assertIn("a1.txt, a2.txt would all become a.txt", r.stderr)
        r = self.pp("batch", "rename", "c", "b", "*.txt", "-y", code=2)
        self.assertIn("b.txt already exists (wanted by c.txt)", r.stderr)
        r = self.pp("batch", "rename", "-t", "{stem}/x", "c.txt", "-y", code=2)
        self.assertIn("is not a valid name", r.stderr)
        self.assertEqual(self.files(), ["a1.txt", "a2.txt", "b.txt", "c.txt"])
        # a swap is not a collision: a-b and b-a trade names
        self.write("work/a-b", "first")
        self.write("work/b-a", "second")
        self.pp("batch", "rename", r"^(\w)-(\w)$", r"\2-\1", "a-b", "b-a", "-y")
        self.assertEqual(self.read(f"{self.work}/b-a"), "first")

    def test_folder_with_contents_then_undo_after_something_moved(self):
        self.touch("My Dir/a b.txt", "My Dir/sub dir/c d.txt", "top a")
        self.pp("batch", "rename", " ", "_", "My Dir", "My Dir/**", "top a", "-y")
        self.assertEqual(self.files(), ["My_Dir", "My_Dir/a_b.txt", "My_Dir/sub_dir", "My_Dir/sub_dir/c_d.txt",
                                        "top_a"])
        os.rename(f"{self.work}/My_Dir", f"{self.work}/Elsewhere")   # the person moved the folder meanwhile
        r = self.pp("batch", "undo", "-y", code=2)
        self.assertIn("My_Dir is gone", r.stderr)
        self.assertIn("--partial", r.stderr)
        r = self.pp("batch", "undo", "--partial", "-y")
        self.assertIn("restored 1 item(s); 4 left as they are", r.stdout)
        self.assertEqual(self.files(), ["Elsewhere", "Elsewhere/a_b.txt", "Elsewhere/sub_dir",
                                        "Elsewhere/sub_dir/c_d.txt", "top a"])
        self.assertIn("undone, 4 left", self.pp("batch", "undo", "--list").stdout)
        self.pp("batch", "undo", "-y", code=1)                      # and the journal no longer blocks undo

    def test_nothing_matched_and_bad_input(self):
        r = self.pp("batch", "rename", "a", "b", "zzz*.txt", "missing.txt", code=1)
        self.assertIn("nothing matches zzz*.txt", r.stderr)
        self.assertIn("no such file or folder: missing.txt", r.stderr)
        self.touch("x.txt")
        self.assertIn("bad regex", self.pp("batch", "rename", "(", "x", "x.txt", code=2).stderr)
        self.assertIn("unknown field {nope}", self.pp("batch", "rename", "-t", "{nope}", "x.txt", code=2).stderr)
        self.assertIn("ambiguous: rename, run", self.pp("batch", "r", "x", "y", code=2).stderr)
        self.assertIn("unknown verb", self.pp("batch", "zap", code=2).stderr)
        self.assertIn("rename", self.pp("batch").stdout)
        self.assertIn("rename", self.pp("batch", "ren", "-h").stdout)    # a unique prefix is the verb


class TestRunJourneys(Journey):
    def test_convert_skip_done_and_retry_after_a_failure(self):
        for n in ("one", "two", "bad"):
            self.write(f"work/in/{n}.txt", f"{n}\n")
        # the converter writes its output, then fails on "bad", leaving a half-written file behind
        self.write("work/conv.sh", '#!/bin/sh\ncat "$1" > "$2"\ngrep -q bad "$1" && exit 7\nexit 0\n', 0o755)
        self.config('[batch.presets]\nconv = { cmd = "./conv.sh {} {out}", out = "out/{stem}.o" }\n'
                    'lines = "wc -l {}"\n')
        r = self.pp("batch", "presets")
        self.assertIn("@conv", r.stdout)
        self.assertIn("@lines", r.stdout)
        r = self.pp("batch", "run", "@conv", "in/*.txt", "-y", "--keep-going", code=1)
        self.assertIn("FAILED rc=7  in/bad.txt", r.stdout)
        self.assertIn("removed its partial output out/bad.o", r.stdout)
        self.assertIn("2 ok, 1 failed", r.stdout)
        self.assertEqual(sorted(os.listdir(f"{self.work}/out")), ["one.o", "two.o"])
        # fix the input; the rerun only does what is missing
        self.write("work/in/bad.txt", "fixed\n")
        r = self.pp("batch", "run", "@conv", "in/*.txt", "-y")
        self.assertIn("1 command(s) to run, 2 skipped", r.stdout)
        self.assertEqual(sorted(os.listdir(f"{self.work}/out")), ["bad.o", "one.o", "two.o"])
        r = self.pp("batch", "run", "@conv", "in/*.txt", "-y")
        self.assertIn("0 command(s) to run, 3 skipped", r.stdout)
        r = self.pp("batch", "run", "@conv", "in/*.txt", "--overwrite", "-n")
        self.assertIn("3 command(s) to run", r.stdout)
        self.assertIn("no preset '@nope'; have: @conv, @lines", self.pp("batch", "run", "@nope", "x", code=2).stderr)

    def test_awkward_names_reach_the_command_whole(self):
        names = ["-n.txt", "it's.txt", "a b.txt", 'we$ird "q".txt']
        for n in names:
            self.write(f"work/{n}", f"<{n}>")
        # bare {}, and the find -exec habits '{}' and "{}": every form gets the file, never an option or a split
        for tmpl in ("cat {}", "cat '{}'", 'cat "{}"'):
            r = self.pp("batch", "run", tmpl, "*.txt", "-y")
            for n in names:
                self.assertIn(f"<{n}>", r.stdout, tmpl)
            self.assertIn("4 ok, 0 failed", r.stdout)
        r = self.pp("batch", "run", "cp {} {out}", "*.txt", "--out", "{base}.bak", "-y")
        self.assertIn("4 ok", r.stdout)
        self.assertTrue(os.path.exists(f"{self.work}/-n.bak"))
        self.assertIn("{out} exists only with --out", self.pp("batch", "run", "echo {out}", "*.txt", code=2).stderr)

    def meet(self, where: str, tries: int) -> str:
        """A command for every job: say it started, then wait for all N of them to have started. Run at once
        they all meet; run one at a time the first waits for jobs that cannot start and gives up. There is no
        clock in it: one at a time, no number of tries is enough, so `tries` only says how long the test waits."""
        os.makedirs(f"{self.work}/{where}")
        return ('touch %(d)s/{n}; n=0; until [ "$(ls %(d)s | wc -l)" -ge {N} ]; do '
                'n=$((n+1)); [ $n -gt %(t)d ] && exit 1; sleep 0.1; done') % {"d": where, "t": tries}

    def test_parallel_and_stop_at_first_failure(self):
        for n in range(4):
            self.write(f"work/f{n}", "")
        r = self.pp("batch", "run", self.meet("together", 200), "f*", "-j", "4", "-y", "-q")
        self.assertIn("4 ok, 0 failed", r.stdout)
        self.assertEqual(len(os.listdir(f"{self.work}/together")), 4)
        # the same jobs one at a time: the first waits for three that cannot start, so the run fails
        r = self.pp("batch", "run", self.meet("alone", 15), "f*", "-y", "-q", code=1)
        self.assertIn("0 ok, 1 failed, 3 not run", r.stdout)
        self.assertEqual(os.listdir(f"{self.work}/alone"), ["1"])
        r = self.pp("batch", "run", "test {n} -lt 2", "f*", "-y", code=1)
        self.assertIn("1 ok, 1 failed, 2 not run (stopped at the first failure; --keep-going to continue)", r.stdout)
        r = self.pp("batch", "run", "echo hi", "f*", "-n")
        self.assertIn("uses no {field}", r.stderr)


if __name__ == "__main__":
    import unittest
    unittest.main()
