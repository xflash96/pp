import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
from pp.lib import paths, ui  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BUMP = """
import sys
from pp.lib import paths
for _ in range(50):
    with paths.json_txn(sys.argv[1]) as rec:
        rec["n"] = rec.get("n", 0) + 1
"""


class TestWriteJson(unittest.TestCase):
    def test_two_writers_at_once_each_land_whole(self):
        """The second write runs inside the first one's rename, the order two processes can take. Each has its own
        temporary file, so both renames succeed and the file is one writer's document, never a mix."""
        with tempfile.TemporaryDirectory() as t:
            path = os.path.join(t, "registry.json")
            real, second = os.replace, journey.Gate()

            def replace(src, dst):
                second.once(lambda: paths.write_json(path, {"by": "second"}))
                real(src, dst)

            with mock.patch.object(paths.os, "replace", replace):
                paths.write_json(path, {"by": "first"})
            self.assertEqual(second.ran, 1)
            with open(path) as f:
                self.assertEqual(json.load(f), {"by": "first"})          # the later rename wins
            self.assertEqual(os.listdir(t), ["registry.json"])          # no temporary file left behind

    def test_a_failed_write_leaves_the_old_file_and_no_temporary(self):
        with tempfile.TemporaryDirectory() as t:
            path = os.path.join(t, "x.json")
            paths.write_json(path, [1])
            with self.assertRaises(TypeError):
                paths.write_json(path, {"bad": object()})
            with open(path) as f:
                self.assertEqual(json.load(f), [1])
            self.assertEqual(os.listdir(t), ["x.json"])


class TestWriteText(unittest.TestCase):
    def test_a_rewritten_file_keeps_its_mode_and_a_new_one_is_0644(self):
        with tempfile.TemporaryDirectory() as t:
            new = os.path.join(t, "new.toml")
            paths.write_text(new, "a = 1\n")
            self.assertEqual(os.stat(new).st_mode & 0o777, 0o644)
            os.chmod(new, 0o600)
            paths.write_text(new, "a = 2\n")
            with open(new) as f:
                self.assertEqual(f.read(), "a = 2\n")
            self.assertEqual(os.stat(new).st_mode & 0o777, 0o600)
            self.assertEqual(os.listdir(t), ["new.toml"])


class TestStaging(unittest.TestCase):
    def test_two_names_in_one_process_differ_by_construction(self):
        """Each name carries this pid and a number this process hands out once, so no draw can repeat one: the
        numbers of 200 names in a row are 200 numbers in a row."""
        names = [paths.staging("/tmp/x", "demo") for _ in range(200)]
        for n in names:
            self.assertRegex(os.path.basename(n), rf"^\.staging-demo-{os.getpid()}-\d+-[0-9a-f]{{8}}$")
        numbers = [int(os.path.basename(n).rsplit("-", 2)[1]) for n in names]
        self.assertEqual(numbers, list(range(numbers[0], numbers[0] + len(names))))
        self.assertEqual(len(set(names)), len(names))

    def test_another_process_picks_another_name(self):
        code = "from pp.lib import paths; print(paths.staging('/tmp/x', 'demo'))"
        r = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(r.stderr, "")
        self.assertNotEqual(r.stdout.strip(), paths.staging("/tmp/x", "demo"))


class TestJsonTxn(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        self.path = os.path.join(self._td.name, "record.json")

    def read(self) -> bytes:
        with open(self.path, "rb") as f:
            return f.read()

    def test_a_record_read_changed_and_written_lands(self):
        with paths.json_txn(self.path, indent=2) as rec:
            rec["targets"] = ["demo"]
        with open(self.path) as f:
            self.assertEqual(json.load(f), {"targets": ["demo"]})

    def test_two_processes_at_once_lose_no_increment(self):
        """Two of them, each adding 1 to the same number 50 times. Without the lock a writer would read a
        number another has already moved on from, and the total would fall short of 100."""
        script = '"$1" -c "$2" "$3" & one=$!; "$1" -c "$2" "$3" & two=$!; wait $one && wait $two'
        r = subprocess.run(["sh", "-c", script, "bump", sys.executable, BUMP, self.path], cwd=ROOT,
                           capture_output=True, text=True, timeout=300)
        self.assertEqual((r.returncode, r.stderr), (0, ""))
        with open(self.path) as f:
            self.assertEqual(json.load(f), {"n": 100})

    def test_a_refusal_inside_leaves_the_file_as_it_was(self):
        paths.write_json(self.path, {"targets": ["demo"]}, indent=2)
        before = self.read()
        err = io.StringIO()
        with redirect_stderr(err), self.assertRaises(SystemExit) as e:
            with paths.json_txn(self.path, indent=2) as rec:
                rec["targets"].append("other")
                ui.die("that name belongs to another project", 2)
        self.assertEqual(e.exception.code, 2)
        self.assertEqual(self.read(), before)
        self.assertEqual(sorted(os.listdir(self._td.name)), ["record.json", "record.json.lock"])

    def test_a_record_nothing_changed_is_not_written(self):
        paths.write_json(self.path, {"targets": ["demo"]}, indent=2)
        stamp = os.stat(self.path)
        with paths.json_txn(self.path, indent=2) as rec:
            self.assertEqual(rec, {"targets": ["demo"]})
        self.assertEqual(os.stat(self.path).st_ino, stamp.st_ino)

    def test_the_lock_can_be_another_file(self):
        shared = os.path.join(self._td.name, ".lock")
        with paths.json_txn(self.path, lock=shared) as rec:
            rec["a"] = 1
        self.assertEqual(sorted(os.listdir(self._td.name)), [".lock", "record.json"])


if __name__ == "__main__":
    unittest.main()
