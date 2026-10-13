import http.client
import json
import os
import re
import signal
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journey  # noqa: E402
import sandbox  # noqa: E402
from pp.commands import http as pphttp  # noqa: E402
from pp.lib import proc  # noqa: E402


def touch(path, text="", mtime=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    if mtime is not None:
        os.utime(path, (mtime, mtime))


class Served(unittest.TestCase):
    """A server on an OS-chosen port over two temp roots A and B (+ an outside dir)."""

    @classmethod
    def setUpClass(cls):
        cls.sb = sandbox.enter(cls)         # one scope for the class: its tree, and the server it serves from
        cls.A, cls.B, cls.out = (cls.sb.path(n) for n in ("A", "B", "outside"))
        now = time.time()
        touch(os.path.join(cls.A, "a.md"), "# Title A\n\nmath $x_1$ and </script> here\n", now - 300)
        touch(os.path.join(cls.A, "sub", "deep.md"), "deep", now - 200)
        touch(os.path.join(cls.A, "node_modules", "pkg", "x.md"), "excluded", now)
        touch(os.path.join(cls.A, ".hidden", "y.md"), "dot dir", now)
        touch(os.path.join(cls.A, ".notes.md"), "dot file", now - 50)
        touch(os.path.join(cls.A, ".env"), "SECRET=1")
        touch(os.path.join(cls.A, "code.py"), "print('hi')\n")
        touch(os.path.join(cls.A, "page.html"), "<h1>x</h1>")
        touch(os.path.join(cls.A, "筆記 one.md"), "unicode body", now - 100)
        with open(os.path.join(cls.A, "img.png"), "wb") as f:
            f.write(b"\x89PNG\r\n\x1a\n" + b"\0" * 16)
        touch(os.path.join(cls.B, "b.md"), "B body", now - 10)
        touch(os.path.join(cls.out, "secret.md"), "outside root")
        os.symlink(os.path.join(cls.out, "secret.md"), os.path.join(cls.A, "link.md"))
        os.symlink(os.path.join(cls.A, ".env"), os.path.join(cls.A, "envlink.txt"))
        s = pphttp.Settings()
        cls.app = pphttp.App(s, [("A", cls.A), ("B", cls.B)], [], quiet=True)
        cls.server = cls.sb.serving(pphttp.make_server(cls.app, "127.0.0.1", 0))
        cls.port = cls.server.server_address[1]

    def get(self, path, method="GET"):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        c.request(method, path)      # http.client sends the path as given: no dot-segment normalisation
        r = c.getresponse()
        body = r.read()
        c.close()
        return r.status, dict((k.lower(), v) for k, v in r.getheaders()), body.decode("utf-8", "replace")


class TestPages(Served):
    def test_csp_allows_only_our_script(self):
        """Rendered markdown is untrusted: the CSP must pin our inline script by hash and allow no other inline JS."""
        import base64
        import hashlib
        status, headers, body = self.get("/t/A/a.md")
        self.assertEqual(status, 200)
        csp = headers["content-security-policy"]
        self.assertNotIn("unsafe-inline", csp.split("script-src")[1].split(";")[0])
        self.assertNotIn("unsafe-eval", csp)
        # the one executable inline script (JSON data blocks are not executed)
        scripts = re.findall(r"<script>(.*?)</script>", body, re.S)
        self.assertEqual(len(scripts), 1)
        h = base64.b64encode(hashlib.sha256(scripts[0].encode()).digest()).decode()
        self.assertIn(f"'sha256-{h}'", csp)

    def test_a_crafted_sort_comes_back_inert(self):
        """A folder page writes ?sort= back into its own links, so the value is one of the three names it
        offers and nothing else reaches the markup. Only this origin may frame a page, so reading one still
        takes the person's own click."""
        crafted = '"><img src=x onerror=alert(1)><b>PWNED</b>'
        st, headers, body = self.get("/t/A/?hidden=1&sort=" + urllib.parse.quote(crafted, safe=""))
        self.assertEqual(st, 200)
        self.assertNotIn("PWNED", body)
        self.assertNotIn("<img src=x", body)
        self.assertIn('<a href="/t/A/?sort=name&hidden=1" class="on">name</a>', body)   # the name sort, kept
        self.assertIn("frame-ancestors 'self'", headers["content-security-policy"])

    def test_recent_newest_first_across_roots(self):
        st, _, body = self.get("/_pp/recent")
        self.assertEqual(st, 200)
        items = json.loads(body)["items"]
        got = [(x["root"], x["path"]) for x in items]
        self.assertEqual(got, [("B", "b.md"), ("A", "筆記 one.md"), ("A", "sub/deep.md"), ("A", "a.md")])
        # node_modules, dot dirs/files and symlinks are not collected
        self.assertFalse(any("node_modules" in p or p.startswith(".") or p == "link.md" for _, p in got))
        st, _, html = self.get("/")
        self.assertEqual(st, 200)
        self.assertLess(html.index("b.md"), html.index("a.md"))
        self.assertIn("/t/A/sub/deep.md", html)

    def test_recent_glob_and_query(self):
        items = json.loads(self.get("/_pp/recent?glob=*.py")[2])["items"]
        self.assertEqual([x["path"] for x in items], ["code.py"])
        items = json.loads(self.get("/_pp/recent?q=deep")[2])["items"]
        self.assertEqual([x["path"] for x in items], ["sub/deep.md"])

    def test_dir_listing(self):
        st, _, html = self.get("/t/A/")
        self.assertEqual(st, 200)
        self.assertIn(">a.md<", html)
        self.assertIn("sub/", html)
        self.assertLess(html.index("sub/"), html.index(">a.md<"))   # dirs first
        self.assertEqual(self.get("/t/A")[0], 301)
        st, h, _ = self.get("/t/A/sub")
        self.assertEqual((st, h["location"]), (301, "/t/A/sub/"))
        self.assertEqual(self.get("/t/A/?sort=mtime")[0], 200)

    def test_markdown_page(self):
        st, _, html = self.get("/t/A/a.md")
        self.assertEqual(st, 200)
        self.assertIn('id="pp-src"', html)
        self.assertIn("math $x_1$ and \\u003c/script\\u003e here", html)   # embedded, </script> neutralised
        self.assertNotIn("and </script> here", html)
        for url in ("cdnjs.cloudflare.com/ajax/libs/marked/12.0.2/marked.min.js",
                    "cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js",
                    "cdnjs.cloudflare.com/ajax/libs/KaTeX/0.16.9/katex.min.js",
                    "cdnjs.cloudflare.com/ajax/libs/KaTeX/0.16.9/katex.min.css"):
            self.assertIn(url, html)
        self.assertIn('"kind": "md"', html)

    def test_code_page_language(self):
        st, _, html = self.get("/t/A/code.py")
        self.assertEqual(st, 200)
        self.assertIn('"lang": "python"', html)

    def test_raw_content_types(self):
        st, h, body = self.get("/raw/A/code.py")
        self.assertEqual((st, h["content-type"], body), (200, "text/plain; charset=utf-8", "print('hi')\n"))
        st, h, _ = self.get("/raw/A/img.png")
        self.assertEqual(h["content-type"], "image/png")
        st, h, _ = self.get("/raw/A/page.html")
        self.assertEqual(h["content-type"], "text/html; charset=utf-8")
        self.assertEqual(h["content-security-policy"], "sandbox")
        st, h, _ = self.get("/raw/A/a.md?dl=1")
        self.assertTrue(h["content-disposition"].startswith("attachment"))
        st, h, body = self.get("/raw/A/a.md", method="HEAD")
        self.assertEqual((st, body), (200, ""))
        self.assertEqual(int(h["content-length"]), os.path.getsize(os.path.join(self.A, "a.md")))

    def test_traversal_is_404(self):
        for p in ("/t/A/../B/b.md", "/t/A/%2e%2e/B/b.md", "/raw/A/..%2F..%2Fetc%2Fpasswd", "/raw/A/%2Fetc%2Fpasswd",
                  "/t/A/sub/../../outside/secret.md", "/t/A/link.md", "/raw/A/link.md", "/t/nope/", "/raw/A/",
                  "//etc/passwd", "/t/A/x%00y"):
            self.assertEqual(self.get(p)[0], 404, p)

    def test_deny_list(self):
        st, _, html = self.get("/t/A/?hidden=1")
        self.assertEqual(st, 200)
        self.assertNotIn(".env", html)
        for p in ("/t/A/.env", "/t/A/.env?hidden=1", "/raw/A/.env", "/raw/A/envlink.txt", "/_pp/stat?root=A&path=.env"):
            self.assertEqual(self.get(p)[0], 404, p)

    def test_hidden_toggle(self):
        self.assertNotIn(".notes.md", self.get("/t/A/")[2])
        self.assertIn(".notes.md", self.get("/t/A/?hidden=1")[2])
        self.assertEqual(self.get("/t/A/.notes.md")[0], 200)   # direct access is fine; only listings hide

    def test_unicode_and_spaces(self):
        name = "筆記 one.md"
        self.assertIn(urllib.parse.quote(name, safe=""), self.get("/t/A/")[2])
        st, _, body = self.get("/raw/A/" + urllib.parse.quote(name))
        self.assertEqual((st, body), (200, "unicode body"))
        self.assertEqual(self.get("/t/A/" + urllib.parse.quote(name))[0], 200)

    def test_stat_json(self):
        st, h, body = self.get("/_pp/stat?root=A&path=a.md")
        self.assertEqual(st, 200)
        self.assertTrue(h["content-type"].startswith("application/json"))
        d = json.loads(body)
        self.assertEqual(d["size"], os.path.getsize(os.path.join(self.A, "a.md")))
        self.assertAlmostEqual(d["mtime"], os.path.getmtime(os.path.join(self.A, "a.md")))
        self.assertTrue(json.loads(self.get("/_pp/stat?root=A&path=")[2])["dir"])
        self.assertEqual(self.get("/_pp/stat?root=A&path=missing.md")[0], 404)

    def test_tabs_on_every_page(self):
        html = self.get("/t/B/b.md")[2]
        self.assertIn('href="/t/A/"', html)
        self.assertIn('class="on" title', html)

    def test_only_get_head(self):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        c.request("POST", "/", body=b"x")
        self.assertGreaterEqual(c.getresponse().status, 400)
        c.close()


class TestDiscovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.under = os.path.join(self.tmp.name, "services")
        os.makedirs(os.path.join(self.under, "p1", ".git"))
        os.makedirs(os.path.join(self.under, "p2"))
        touch(os.path.join(self.under, "p3", "CLAUDE.md"))
        os.makedirs(os.path.join(self.under, "node_modules", ".git"))
        touch(os.path.join(self.under, "grp", "q1", "CLAUDE.md"))

    def tearDown(self):
        self.tmp.cleanup()

    def app(self, ttl, depth=1):
        s = pphttp.Settings()
        s.cache_ttl = ttl
        rule = {"under": self.under, "depth": depth, "marker": [".git", "CLAUDE.md"]}
        return pphttp.App(s, [], [rule], quiet=True)

    def test_marker_and_refresh(self):
        app = self.app(ttl=0)
        self.assertEqual(list(app.roots()), ["p1", "p3"])
        os.makedirs(os.path.join(self.under, "p4", ".git"))
        self.assertEqual(list(app.roots()), ["p1", "p3", "p4"])

    def test_cache_holds_within_ttl(self):
        app = self.app(ttl=60)
        self.assertEqual(list(app.roots()), ["p1", "p3"])
        os.makedirs(os.path.join(self.under, "p4", ".git"))
        self.assertEqual(list(app.roots()), ["p1", "p3"])

    def test_depth_two_names_route(self):
        app = self.app(ttl=0, depth=2)
        self.assertEqual(list(app.roots()), ["grp/q1"])
        with sandbox.Sandbox() as sb:
            server = sb.serving(pphttp.make_server(app, "127.0.0.1", 0))
            c = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=10)
            c.request("GET", "/raw/grp%2Fq1/CLAUDE.md")
            self.assertEqual(c.getresponse().status, 200)
            c.close()

    def test_settings_extend_defaults(self):
        s = pphttp.Settings.from_config({"deny": ["*.secret"], "exclude_dirs": "target2", "recent": "*.txt"})
        self.assertIn(".env", s.deny)
        self.assertIn("*.secret", s.deny)
        self.assertIn("node_modules", s.exclude_dirs)
        self.assertIn("target2", s.exclude_dirs)
        self.assertEqual(s.recent, ["*.txt"])


class TestTheTreeChangesUnderARequest(unittest.TestCase):
    """open_in_tab approves a name and then checks the fd it opened is that path. The gate turns tab/sub into a
    symlink out of the tab the moment the name is approved, which is what a writer in a served tab can do while
    the request is in flight, and the fd check is what refuses it."""

    def setUp(self):
        self.sb = sandbox.enter(self)
        self.tab, self.out = self.sb.path("tab"), self.sb.path("outside")
        touch(os.path.join(self.tab, "sub", "f.txt"), "inside the tab")
        touch(os.path.join(self.tab, "sub", "readme.md"), "inside the tab")
        touch(os.path.join(self.out, "f.txt"), "OUTSIDE EVERY TAB")
        touch(os.path.join(self.out, "readme.md"), "OUTSIDE EVERY TAB")
        touch(os.path.join(self.out, "only-outside.txt"), "OUTSIDE EVERY TAB")
        # one walk for the whole test, whatever else the machine is doing: a test here needs the tab cache to
        # still name a tab whose directory has gone, and a ttl in seconds would make that a race with the clock
        self.app = pphttp.App(pphttp.Settings(cache_ttl=float("inf")), [("tab", self.tab)], [], quiet=True)
        self.server = self.sb.serving(pphttp.make_server(self.app, "127.0.0.1", 0))
        self.port = self.server.server_address[1]

    def swapped_after(self, segs):
        """Patch App.resolve: once it has approved `segs`, tab/sub is a symlink to the outside directory."""
        real = pphttp.App.resolve
        swap = journey.Gate()

        def to_the_outside():
            sub = os.path.join(self.tab, "sub")
            for name in os.listdir(sub):
                os.remove(os.path.join(sub, name))
            os.rmdir(sub)
            os.symlink(self.out, sub)

        def resolve_then_swap(app, root_name, asked):
            r = real(app, root_name, asked)
            swap.once(to_the_outside, when=lambda: asked == segs)
            return r
        return swap, mock.patch.object(pphttp.App, "resolve", resolve_then_swap)

    def get(self, path):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        c.request("GET", path)
        r = c.getresponse()
        body = r.read().decode("utf-8", "replace")
        c.close()
        return r.status, body

    def test_raw_bytes_come_from_the_file_that_opened(self):
        swap, patch = self.swapped_after(["sub", "f.txt"])
        with patch:
            status, body = self.get("/raw/tab/sub/f.txt")
        self.assertEqual(swap.ran, 1)
        self.assertEqual(status, 404)
        self.assertNotIn("OUTSIDE EVERY TAB", body)

    def test_a_file_view_shows_the_file_that_opened(self):
        swap, patch = self.swapped_after(["sub", "f.txt"])
        with patch:
            status, body = self.get("/t/tab/sub/f.txt")
        self.assertEqual(swap.ran, 1)
        self.assertEqual(status, 404)
        self.assertNotIn("OUTSIDE EVERY TAB", body)

    def test_a_folder_page_lists_the_directory_that_opened(self):
        swap, patch = self.swapped_after(["sub"])
        with patch:
            status, body = self.get("/t/tab/sub/")
        self.assertEqual(swap.ran, 1)
        self.assertEqual(status, 404)
        self.assertNotIn("only-outside.txt", body)
        self.assertNotIn("OUTSIDE EVERY TAB", body)

    def test_a_tab_whose_directory_became_a_file_is_answered(self):
        """The tabs are cached for cache_ttl, so for that window a tab still names a path that may no longer
        be a directory: a project directory removed and a file written in its place. A tab's own root is only
        ever a folder, so the request is refused like any other stale name, and the browser gets an answer
        rather than a dropped connection. The rule sits in open_in_tab, so every endpoint answers the same:
        the view, the bytes and the live-reload probe."""
        self.assertEqual(self.get("/t/tab/")[0], 200)           # the tab is in the cache, and it is a folder
        for name in os.listdir(os.path.join(self.tab, "sub")):
            os.remove(os.path.join(self.tab, "sub", name))
        os.rmdir(os.path.join(self.tab, "sub"))
        os.rmdir(self.tab)
        touch(self.tab, "the tab's name is a file now")
        self.assertEqual(self.app.roots().get("tab"), self.tab, "the cache no longer names the tab")
        for path in ("/t/tab/", "/t/tab", "/raw/tab/", "/raw/tab"):
            status, body = self.get(path)
            self.assertEqual(status, 404, path)                 # what _refuse answers for a stale name
            self.assertNotIn("is a file now", body)
        # the probe the open page polls answers the same, so the page says the folder is gone instead of
        # reloading itself against a file no other endpoint will serve
        status, body = self.get("/_pp/stat?root=tab&path=")
        self.assertEqual((status, json.loads(body)), (404, {"error": "not found"}))

    def test_the_live_reload_probe_reports_nothing_about_what_was_swapped_in(self):
        swap, patch = self.swapped_after(["sub"])
        with patch:
            status, body = self.get("/_pp/stat?root=tab&path=sub")
        self.assertEqual(swap.ran, 1)
        self.assertEqual((status, json.loads(body)), (404, {"error": "not found"}))


class TestEveryAnswerComesFromTheFileThatOpened(unittest.TestCase):
    """open_in_tab hands a request one open fd and the fstat of that fd, and that is where every answer about
    the file comes from: the bytes a page shows, the live-reload baseline it carries, and what /_pp/stat
    reports. The spy records the fstat and gives the name away the moment the fd is handed out, so an answer
    taken from the name afterwards is a different one."""

    def setUp(self):
        self.sb = sandbox.enter(self)
        self.tab = self.sb.path("tab")
        self.spare = os.path.join(self.tab, "spare.md")
        touch(os.path.join(self.tab, "a.md"), "the file that opened\n")
        touch(self.spare, "the name was given to this longer file instead\n")
        touch(os.path.join(self.tab, "sub", "one.md"), "1")
        old = time.time() - 1000
        os.utime(os.path.join(self.tab, "sub"), (old, old))      # so "written since" needs no clock resolution
        app = pphttp.App(pphttp.Settings(), [("tab", self.tab)], [], quiet=True)
        self.server = self.sb.serving(pphttp.make_server(app, "127.0.0.1", 0))
        self.port = self.server.server_address[1]

    def get(self, path):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        c.request("GET", path)
        r = c.getresponse()
        body = r.read().decode("utf-8", "replace")
        c.close()
        return r.status, body

    def handed_out(self, act):
        """Patch open_in_tab to record (fstat, first bytes) of every fd it hands out and then run `act(path)`
        on the path it was asked for -> (records, patch)."""
        real, seen = pphttp.open_in_tab, []

        def open_then_act(app, root, segs, **kw):
            o = real(app, root, segs, **kw)
            if o:
                seen.append((os.fstat(o.fd), b"" if o.is_dir else os.pread(o.fd, 4096, 0)))
                act(os.path.join(self.tab, *segs))
            return o
        return seen, mock.patch.object(pphttp, "open_in_tab", open_then_act)

    def page_data(self, body):
        return json.loads(re.search(r'id="pp-page">(.*?)</script>', body, re.S).group(1))

    def test_a_file_page_shows_the_bytes_and_the_fstat_of_the_fd_it_read(self):
        seen, patch = self.handed_out(lambda path: os.replace(self.spare, path))
        with patch:
            status, body = self.get("/t/tab/a.md")
        self.assertEqual(status, 200)
        st, head = seen[0]
        self.assertNotEqual(os.stat(os.path.join(self.tab, "a.md")).st_size, st.st_size)   # the name moved on
        data = self.page_data(body)
        self.assertEqual((data["size"], data["mtime"]), (st.st_size, st.st_mtime))
        self.assertEqual(json.loads(re.search(r'id="pp-src">(.*?)</script>', body, re.S).group(1)), head.decode())

    def test_a_folder_page_carries_the_fstat_the_folder_opened_with(self):
        seen, patch = self.handed_out(lambda path: touch(os.path.join(path, "later.md"), "later"))
        with patch:
            status, body = self.get("/t/tab/sub/")
        self.assertEqual(status, 200)
        st, _ = seen[0]
        base, now = self.page_data(body), json.loads(self.get("/_pp/stat?root=tab&path=sub")[1])
        self.assertEqual((base["mtime"], base["size"]), (st.st_mtime, st.st_size))
        self.assertGreater(now["mtime"], base["mtime"])     # written since: the next poll differs, as it must

    def test_the_live_reload_probe_answers_from_an_fd(self):
        seen, patch = self.handed_out(lambda path: os.replace(self.spare, path))
        with patch:
            status, body = self.get("/_pp/stat?root=tab&path=a.md")
        self.assertEqual(status, 200)
        st, _ = seen[0]
        self.assertNotEqual(os.stat(os.path.join(self.tab, "a.md")).st_size, st.st_size)
        self.assertEqual(json.loads(body), {"mtime": st.st_mtime, "size": st.st_size, "dir": False})


class AskedFor:
    """A lock that says how many callers have asked for it. A caller is counted before it waits, so a test can
    wait for a state (every caller is inside the scope that takes this lock) instead of for a while."""

    def __init__(self):
        self._lock = threading.Lock()
        self._asked = threading.Semaphore(0)

    def acquire(self, *a, **kw):
        self._asked.release()
        return self._lock.acquire(*a, **kw)

    def release(self):
        self._lock.release()

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *exc):
        self.release()
        return False

    def taken(self, timeout: float) -> bool:
        """Whether one more caller has asked for it within timeout."""
        return self._asked.acquire(timeout=timeout)


class TestOneOwnerForTheCaches(unittest.TestCase):
    """The tabs and the Recent listing are each one Cached: one computation at a time, one place the answer is
    stored, and every caller that asks while a value is stale reads that computation's answer."""

    CALLERS = 5

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.under = os.path.join(self.tmp.name, "services")
        os.makedirs(os.path.join(self.under, "alpha"))
        touch(os.path.join(self.under, "alpha", "one.md"), "1")
        s = pphttp.Settings()
        s.cache_ttl = 600                     # nothing in this test waits for a ttl to run out
        self.app = pphttp.App(s, [], [{"under": self.under, "depth": 1}], quiet=True)

    def test_callers_that_ask_during_a_computation_read_its_answer(self):
        runs, inside, go, got = [], threading.Event(), threading.Event(), []

        def compute(key):
            runs.append(key)
            inside.set()
            self.assertTrue(go.wait(15))
            return len(runs)

        c = pphttp.Cached(600, compute)
        threads = [threading.Thread(target=lambda: got.append(c.get())) for _ in range(5)]
        threads[0].start()
        self.assertTrue(inside.wait(15))      # the first computation is in flight and holds the lock
        for t in threads[1:]:
            t.start()                         # the only way in is that lock
        go.set()
        for t in threads:
            t.join(15)
        self.assertEqual((runs, got), ([()], [1] * 5))

    def counted(self, name, cache):
        """Patch App.<name> to count its calls, and hold the first call until every caller is inside the
        cache's get() -> (calls, patch). The cache's lock is replaced by one that counts the callers that ask
        for it, which they do before they wait for it, so the hold ends on a state and not on a wait: however
        the CPUs are shared out, each caller has reached the cache before the first answer is stored."""
        calls, real, asked = [], getattr(pphttp.App, name), AskedFor()
        cache._lock = asked

        def count(app, *args):
            calls.append(args)
            if len(calls) == 1:
                for _ in range(self.CALLERS):
                    self.assertTrue(asked.taken(15), "a caller never reached the cache")
            return real(app, *args)
        return calls, mock.patch.object(pphttp.App, name, count)

    def asked_at_once(self, ask):
        """Every caller calls ask() as near together as threads can, and each answer comes back."""
        at = threading.Barrier(self.CALLERS)
        answers = []

        def one():
            at.wait(15)
            answers.append(ask())
        threads = [threading.Thread(target=one) for _ in range(self.CALLERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        return answers

    def test_requests_past_the_ttl_make_one_walk_of_the_tabs(self):
        calls, patch = self.counted("_discover", self.app._tabs)
        with patch:
            answers = self.asked_at_once(lambda: list(self.app.roots()))
        self.assertEqual(answers, [["alpha"]] * self.CALLERS)
        self.assertEqual(len(calls), 1)

    def test_requests_past_the_ttl_make_one_recent_scan(self):
        calls, patch = self.counted("_scan", self.app._found)
        with patch:
            answers = self.asked_at_once(lambda: self.app.recent()["total"])
        self.assertEqual(answers, [1] * self.CALLERS)
        self.assertEqual(len(calls), 1)


class TestServerBits(unittest.TestCase):
    def test_port_fallback(self):
        blocker = socket.socket()
        blocker.bind(("127.0.0.1", 0))
        blocker.listen()
        taken = blocker.getsockname()[1]
        app = pphttp.App(pphttp.Settings(), [], [], quiet=True)
        try:
            server = pphttp.make_server(app, "127.0.0.1", taken, tries=20)
        except OSError:
            self.skipTest("no free port above the blocker")
        try:
            self.assertNotEqual(server.server_address[1], taken)
            self.assertTrue(taken < server.server_address[1] < taken + 20)
        finally:
            server.server_close()
            blocker.close()

    @staticmethod
    def blocks(thread) -> set:
        """Which stop signals a thread blocks, from SigBlk in /proc/self/task/<id>/status. A mask belongs to
        one thread, and bit n-1 of SigBlk is signal n."""
        with open(f"/proc/self/task/{thread.native_id}/status") as f:
            mask = int(next(ln for ln in f if ln.startswith("SigBlk:")).split()[1], 16)
        return {s for s in (signal.SIGINT, signal.SIGTERM) if mask >> (int(s) - 1) & 1}

    def test_either_stop_signal_ends_the_serving_loop(self):
        """SIGINT and SIGTERM both stop the server: the loop exits and the port closes. The signal goes to the
        process, as a terminal's Ctrl-C and `pp kill` do, so the thread that takes it is whichever one does not
        block it; the thread that serves blocks both, so the wait here is what takes it."""
        app = pphttp.App(pphttp.Settings(), [], [], quiet=True)
        server = pphttp.make_server(app, "127.0.0.1", 0)
        blocked = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                with pphttp.Serving(server) as serving:
                    self.assertEqual(self.blocks(serving._thread), {signal.SIGINT, signal.SIGTERM})
                    proc.raise_to_self(sig)     # pending, and nothing has asked for it yet
                    self.assertEqual(serving.wait(), sig)
                self.assertFalse(serving._thread.is_alive())        # nothing serves after the block
        finally:
            server.server_close()
        self.assertEqual(server.socket.fileno(), -1)
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, set()), blocked)

    def test_ctrl_c_as_the_banner_is_printed_is_a_clean_stop(self):
        """A Ctrl-C right after "Ctrl-C to stop" is read (a person, or a test, acting on the line): exit 0, port closed."""
        made, real = [], pphttp.make_server

        def make(*a, **k):
            made.append(real(*a, **k))
            return made[-1]

        def say(*a, **k):
            if "Ctrl-C to stop" in " ".join(map(str, a)):
                raise KeyboardInterrupt          # the signal lands before the line's print returns

        with sandbox.Sandbox() as sb, \
                mock.patch.object(pphttp, "make_server", make), \
                mock.patch.object(pphttp, "print", say, create=True):
            self.assertEqual(pphttp.main([sb.work, "-p", "0", "-q"]), 0)
        self.assertEqual(made[0].socket.fileno(), -1)

    def test_stdlib_http_not_shadowed(self):
        self.assertEqual(pphttp.ThreadingHTTPServer.__module__, "http.server")

    def test_guess_lang(self):
        self.assertEqual(pphttp.guess_lang("Makefile", ""), "makefile")
        self.assertEqual(pphttp.guess_lang("x.cljs", ""), "clojure")
        self.assertEqual(pphttp.guess_lang("run", "#!/usr/bin/env python3\n"), "python")
        self.assertEqual(pphttp.guess_lang("go", "#!/bin/sh\n"), "bash")
        self.assertEqual(pphttp.guess_lang("TODO", "plain"), "")


if __name__ == "__main__":
    unittest.main()
