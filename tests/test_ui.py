"""ui.parse_intermixed: one parse on every Python pp supports (argparse differs between 3.11, 3.12.3 and 3.12.12)."""
import os
import sys
import unicodedata
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pp.lib import ui  # noqa: E402


def rename_parser():
    p = ui.parser("t", None)
    p.add_argument("target")
    p.add_argument("message", nargs="*")
    p.add_argument("--wait")
    p.add_argument("-n", action="store_true")
    return p


class TestParseIntermixed(unittest.TestCase):
    def test_options_between_positionals(self):
        a = ui.parse_intermixed(rename_parser(), ["beta", "--wait", "1s", "hello", "-n", "there"])
        self.assertEqual((a.target, a.message, a.wait, a.n), ("beta", ["hello", "there"], "1s", True))

    def test_after_double_dash_everything_is_positional(self):
        a = ui.parse_intermixed(rename_parser(), ["-n", "beta", "--", "-x", "--", "-x", "--wait"])
        self.assertEqual((a.target, a.message, a.wait, a.n), ("beta", ["-x", "--", "-x", "--wait"], None, True))

    def test_a_positional_after_double_dash_alone(self):
        a = ui.parse_intermixed(rename_parser(), ["--", "-dash"])
        self.assertEqual((a.target, a.message), ("-dash", []))

    def test_an_unknown_option_is_still_a_usage_error(self):
        with self.assertRaises(SystemExit) as e, open(os.devnull, "w") as null:
            sys.stderr, old = null, sys.stderr
            try:
                ui.parse_intermixed(rename_parser(), ["beta", "--nope"])
            finally:
                sys.stderr = old
        self.assertEqual(e.exception.code, 2)



def column_of(line: str, text: str) -> int:
    """The printed column `text` starts at in this line, which is what a reader sees as its alignment."""
    return ui.width(line[:line.index(text)])


def ui_red(text: str) -> str:
    """What c() returns on a terminal, which is what a table is given there."""
    return f"\x1b[31m{text}\x1b[0m"


class TestWidth(unittest.TestCase):
    def test_what_a_terminal_prints_is_what_is_counted(self):
        self.assertEqual(ui.width("done"), 4)
        self.assertEqual(ui.width("\x1b[31mFAILED\x1b[0m"), 6)       # the colour prints nothing
        self.assertEqual(ui.width("报告报告.txt"), 12)                # 8 characters, 12 columns
        self.assertEqual(ui.width("ｗｉｄｅ"), 8)                     # fullwidth forms count two as well
        self.assertEqual(ui.pad("报告", 8), "报告" + "    ")

    def test_a_character_the_terminal_draws_no_column_for_counts_for_nothing(self):
        """A name in NFD is ordinary input: a copy from a Mac, an unzipped archive, some input methods write
        an accent as a character of its own, drawn on the letter before it. Counting it as a column pads every
        such name one place too far, which leaves the arrow of a rename preview and the column after a table
        cell out of line with the rest. The expectations are counted by hand, since measuring the output with
        width() would pass for any width function that disagrees with the terminal the same way twice."""
        nfd = unicodedata.normalize("NFD", "naïve.txt")
        self.assertEqual(len(nfd), 10)                              # ten characters
        self.assertEqual(ui.width(nfd), 9)                          # nine columns
        self.assertEqual(ui.width("naïve.txt"), 9)                  # the same name precomposed
        self.assertEqual(ui.pad(nfd, 11), nfd + "  ")
        self.assertEqual(ui.width("e\u0301\ufe0f\u200d"), 1)        # a mark, a variation selector, a joiner
        self.assertEqual(ui.cut(nfd, 9), nfd)                       # a cut counts what width() counts
        self.assertEqual(ui.cut(nfd, 4), "nai\u0308…")              # and keeps a mark with its own letter
        for to in range(1, 12):
            self.assertLessEqual(ui.width(ui.cut(nfd, to)), to, to)

    def test_a_cut_is_counted_in_columns_and_never_splits_a_wide_character(self):
        self.assertEqual(ui.cut("short", 8), "short")
        self.assertEqual(ui.cut("a-long-tag", 6), "a-lon…")
        for to in range(1, 12):
            out = ui.cut("报告报告.txt", to)
            self.assertLessEqual(ui.width(out), to, (to, out))
        self.assertEqual(ui.cut("报告报告", 6), "报告…")            # a third 报 would make it 7 columns


class TestTable(unittest.TestCase):
    def test_a_newline_in_a_cell_cannot_start_a_row(self):
        out = ui.table([[1, "sleep 9\n0  root  rm -rf /"], [2, "ok"]], ["pid", "command"])
        self.assertEqual(len(out.splitlines()), 3)
        self.assertIn("sleep 9\\n0  root  rm -rf /", out)

    def test_a_coloured_cell_does_not_move_the_columns_after_it(self):
        """A table is the one construct every listing goes through, and its docstring says a cell may carry
        c()'s colours: `pp queue` colours FAILED in a middle column, so measuring the escape bytes moved every
        column after it nine places left on exactly the rows a person is looking for."""
        rows = [[23, "done", "rc=0", "12s"], [22, ui_red("FAILED"), "rc=-", "4s"]]
        lines = ui.table(rows, ["id", "state", "code", "took"]).splitlines()
        self.assertEqual({column_of(ln, "rc") for ln in lines[1:]}, {column_of(lines[0], "code")})

    def test_a_double_width_cell_does_not_move_the_columns_after_it(self):
        lines = ui.table([["报告报告.txt", "one"], ["aaaaaaaa.txt", "two"]]).splitlines()
        self.assertEqual(column_of(lines[0], "one"), column_of(lines[1], "two"))

if __name__ == "__main__":
    unittest.main()
