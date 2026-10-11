"""User journeys for `pp http`: start the real server the way a person does, read what it prints, browse it the way
a browser does (http.client: no URL normalisation, so traversal attempts reach the server as typed), stop it."""
import http.client
import json
import os
import re
import select
import signal
import socket
import subprocess
import sys
import time
import unittest
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from journey import PP, Journey  # noqa: E402


class HttpJourney(Journey):
    def serve(self, *args, cwd=None, port=None):
        """`pp http` in the foreground of a terminal: SIGINT is Ctrl-C. Returns (proc, base url, banner).

        Without a port it serves on -p 0, so the OS picks one and the banner says which; a number picked here
        would have to be bound a moment later, and anything on the machine could take it in between."""
        p = self.child([sys.executable, PP, "http", "-p", str(port or 0), *args], cwd=cwd,
                       stderr=subprocess.PIPE, text=False, bufsize=0)
        banner, end = "", time.monotonic() + 15
        while "Ctrl-C to stop" not in banner:
            if p.poll() is not None or time.monotonic() > end:
                err = p.stderr.read().decode() if p.poll() is not None else ""
                self.fail(f"pp http did not start: rc={p.poll()}\n{banner}{err}")
            if select.select([p.stdout], [], [], 0.2)[0]:
                banner += os.read(p.stdout.fileno(), 65536).decode()
        m = re.search(r"serving (http://\S+?)/?\s", banner)
        return p, m.group(1), banner

    def stop(self, p):
        """Ctrl-C: a clean exit, and the port is free again."""
        p.signal(signal.SIGINT)
        out, err = (b.decode() for b in p.communicate(timeout=10))
        self.assertEqual(p.returncode, 0, err)
        return out, err

    def get(self, base, path, method="GET", headers=None):
        u = urllib.parse.urlsplit(base)
        c = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
        try:
            c.request(method, path, headers=headers or {})
            r = c.getresponse()
            return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read().decode("utf-8", "replace")
        finally:
            c.close()

    def follow(self, base, path):
        for _ in range(5):
            st, h, body = self.get(base, path)
            if st != 301:
                return st, body
            path = h["location"]
        self.fail(f"redirect loop at {path}")

    def project(self):
        """A project an agent has been writing in: notes, code, an image, nested docs, odd names."""
        now = time.time()
        files = {"README.md": "# Proj\n\nSee [the guide](docs/guide.md), [docs](docs/), [root link](/docs/guide.md).\n\n"
                              "![plot](docs/plot.png)\n\nMath $E=mc^2$.\n\n```python\nprint('hi')\n```\n",
                 "docs/guide.md": "# Guide\n\nBack to [the readme](../README.md).\n",
                 "train.py": "#!/usr/bin/env python3\nprint('training')\n",
                 "notes/筆記 one.md": "unicode and a space",
                 "notes/old.md": "old"}
        for i, (rel, text) in enumerate(files.items()):
            path = self.write(f"work/proj/{rel}", text)
            os.utime(path, (now - 100 * i, now - 100 * i))      # README newest, notes/old.md oldest
        with open(f"{self.work}/proj/docs/plot.png", "wb") as f:
            f.write(b"\x89PNG\r\n\x1a\n" + b"\0" * 64)
        os.makedirs(f"{self.work}/proj/empty")
        return f"{self.work}/proj"


class TestFirstRun(HttpJourney):
    def test_serve_the_current_folder_browse_it_and_stop(self):
        proj = self.project()
        p, base, banner = self.serve(cwd=proj)
        self.assertRegex(banner, r"pp http: serving http://127\.0\.0\.1:\d+/")
        self.assertIn(f"proj  {proj}", banner)

        # Recent: every markdown file across tabs, newest first, linked to its view
        st, _, body = self.get(base, "/")
        self.assertEqual(st, 200)
        order = [m for m in re.findall(r'href="/t/proj/([^"]+)"', body) if m.endswith(".md")]
        self.assertEqual(order, ["README.md", "docs/guide.md", urllib.parse.quote("notes/筆記 one.md", safe="/"),
                                 "notes/old.md"])
        self.assertRegex(body, r'id="pp-count">4</span> of 4 files matching \*\.md across 1 tab')

        # the folder: dirs first, README embedded for rendering, every link in it leads somewhere
        st, body = self.follow(base, "/t/proj")
        self.assertEqual(st, 200)
        self.assertLess(body.index(">docs/<"), body.index(">train.py<"))
        # size and modified sit right-aligned over their values: the rule for .r must outrank `table.list th`
        self.assertIn('<th class="r hide-sm">size</th><th class="r">modified</th>', body)
        self.assertIn("table.list .r{text-align:right", body)
        self.assertIn("# Proj", json.loads(re.search(r'id="pp-src">(.*?)</script>', body).group(1)))
        for href in set(re.findall(r'href="(/t/[^"#?]+)', body)):
            self.assertEqual(self.follow(base, href)[0], 200, href)

        # a file view and its raw bytes; an image; a unicode name with a space; an empty folder
        st, _, body = self.get(base, "/t/proj/train.py")
        self.assertEqual((st, '"lang": "python"' in body), (200, True))
        st, h, raw = self.get(base, "/raw/proj/train.py")
        self.assertEqual((st, h["content-type"], raw), (200, "text/plain; charset=utf-8", "#!/usr/bin/env python3\nprint('training')\n"))
        self.assertEqual(self.get(base, "/raw/proj/docs/plot.png")[1]["content-type"], "image/png")
        st, _, raw = self.get(base, "/raw/proj/notes/" + urllib.parse.quote("筆記 one.md"))
        self.assertEqual((st, raw), (200, "unicode and a space"))
        self.assertIn("Empty folder.", self.get(base, "/t/proj/empty/")[2])

        # a file deleted while it is open: the page and its live-reload probe say so
        os.remove(f"{proj}/notes/old.md")
        st, _, body = self.get(base, "/t/proj/notes/old.md")
        self.assertEqual(st, 404)
        self.assertIn("Not found: proj/notes/old.md", body)
        self.assertIn('href="/"', body)
        self.assertEqual(self.get(base, "/_pp/stat?root=proj&path=notes/old.md")[0], 404)

        # HEAD answers like GET, without the body
        st, h, body = self.get(base, "/raw/proj/README.md", method="HEAD")
        self.assertEqual((st, body, int(h["content-length"])), (200, "", os.path.getsize(f"{proj}/README.md")))

        _, err = self.stop(p)
        self.assertIn("200 GET /t/proj/train.py", err)          # the request log

        # a restart asks for the port pp has just released. Anything on the machine may have taken it in
        # between, and then pp moves on and says so, which is all pp promises: it serves on the port its
        # banner names, and it names another one only when the one asked for is busy.
        want = int(base.rsplit(":", 1)[1])
        p, again, banner = self.serve("-q", cwd=proj, port=want)
        self.assertEqual(self.get(again, "/")[0], 200)
        self.assertEqual(again == base, f"(port {want} is busy)" not in banner)
        self.stop(p)

    def test_busy_port_moves_on_and_says_so(self):
        with socket.socket() as blocker:
            blocker.bind(("127.0.0.1", 0))
            blocker.listen()
            busy = blocker.getsockname()[1]
            p, base, banner = self.serve("-q", port=busy)
            self.assertNotEqual(base, f"http://127.0.0.1:{busy}")
            self.assertIn(f"(port {busy} is busy)", banner)
            self.assertEqual(self.get(base, "/")[0], 200)
            self.stop(p)

    def test_file_arguments_open_their_folders_and_print_their_urls(self):
        proj = self.project()
        self.write("work/other/proj/notes.md", "# other")
        p, base, banner = self.serve("-q", "README.md", "../other/proj/notes.md", "docs", cwd=proj)
        self.assertIn(f"file: {base}/t/proj/README.md", banner)
        self.assertIn(f"file: {base}/t/proj-2/notes.md", banner)     # same folder name: a suffix
        self.assertIn(f"docs    {proj}/docs", banner)
        self.assertEqual(self.get(base, "/t/proj-2/notes.md")[0], 200)
        self.stop(p)

    def test_bad_flags_and_paths_are_usage_errors(self):
        for port in ("99999", "abc", "-1"):
            r = self.pp("http", "-p", port, code=2)
            self.assertIn("is not a port (0..65535)", r.stderr)
        r = self.pp("http", "/no/such/dir", code=2)
        self.assertIn("not a directory: /no/such/dir", r.stderr)
        r = self.pp("http", "--bind", "no.such.host.invalid", "-p", "0", code=1)
        self.assertIn("cannot listen on no.such.host.invalid", r.stderr)
        self.assertNotIn("Traceback", r.stderr)


class TestConfigured(HttpJourney):
    def test_tabs_discovery_and_recent_globs_from_config(self):
        svc = f"{self.tmp}/svc"
        for rel in ("alpha/.git/HEAD", "beta/CLAUDE.md", "gamma/x.txt", "node_modules/.git/HEAD"):
            self.write(f"{svc}/{rel}", "x")
        self.write("notes/today.txt", "t")
        self.config(f'[http]\nrecent = ["*.md", "*.txt"]\ncache_ttl = 0\n\n'
                    f'[[http.roots]]\nname = "notes"\npath = "{self.tmp}/notes"\n\n'
                    f'[[http.discover]]\nunder = "{svc}"\nmarker = [".git", "CLAUDE.md"]\n')
        p, base, banner = self.serve("-q")
        self.assertIn(f"notes  {self.tmp}/notes", banner)
        self.assertIn("alpha", banner)
        self.assertIn("beta", banner)
        self.assertNotIn("gamma", banner)                          # no marker
        self.assertIn(f"discover: {svc} depth 1 marker .git|CLAUDE.md", banner)
        items = json.loads(self.get(base, "/_pp/recent")[2])["items"]
        self.assertEqual({(x["root"], x["path"]) for x in items}, {("notes", "today.txt"), ("beta", "CLAUDE.md")})
        self.write(f"{svc}/delta/.git/HEAD", "x")                 # a new project shows up without a restart
        self.assertIn('href="/t/delta/"', self.get(base, "/")[2])
        self.stop(p)

        p, base, banner = self.serve("-q", "--no-discover")
        self.assertNotIn("alpha", banner)
        self.stop(p)

    def test_config_only_refuses_to_fall_back(self):
        r = self.pp("http", "--config-only", code=2)
        self.assertIn("no [[http.roots]] or [[http.discover]]", r.stderr)

    def test_a_typo_in_config_names_the_key(self):
        for text, msg in (('[http]\nport = "8000"', "[http] port = '8000': an integer"),
                          ("[http]\nrecent = 5", "[http] recent = 5: a string or a list of strings"),
                          ('[http]\nroots = "~/notes"', "[http] roots must be written [[http.roots]]"),
                          ('[[http.roots]]\nname = "x"', 'needs path = "/some/dir"'),
                          ('[[http.discover]]\nunder = "/tmp"\ndepth = "2"', "depth = '2' is not an integer"),
                          ('[http]\nfollow_symlinks = "yes"', "follow_symlinks = 'yes': true or false")):
            with open(self.config(""), "w") as f:
                f.write(text + "\n")
            r = self.pp("http", "-p", "0", code=1)
            self.assertTrue(r.stderr.startswith("pp: bad config: "), r.stderr)
            self.assertIn(msg, r.stderr)
            self.assertNotIn("Traceback", r.stderr)


class TestConfinement(HttpJourney):
    def setUp(self):
        super().setUp()
        self.proj = self.project()
        self.write("outside/secret.md", "outside the tab")
        os.symlink(f"{self.tmp}/outside/secret.md", f"{self.proj}/link.md")
        os.symlink(f"{self.tmp}/outside", f"{self.proj}/outdir")
        os.symlink("docs/guide.md", f"{self.proj}/inner.md")       # stays inside: fine
        self.write("work/proj/.env", "TOKEN=1")
        self.write("work/proj/.git-credentials", "https://u:tok@host")

    def test_nothing_outside_a_tab_is_reachable(self):
        p, base, _ = self.serve("-q", cwd=self.proj)
        for path in ("/t/proj/../outside/secret.md", "/t/proj/%2e%2e/outside/secret.md",
                     "/raw/proj/..%2F..%2Foutside%2Fsecret.md", "/raw/proj/%2Fetc%2Fpasswd", "//etc/passwd",
                     "/t/proj/docs/../../outside/secret.md", "/t/proj/link.md", "/raw/proj/link.md",
                     "/t/proj/outdir/", "/raw/proj/outdir/secret.md", "/raw/proj/.env", "/t/proj/.env?hidden=1",
                     "/raw/proj/.git-credentials", "/_pp/stat?root=proj&path=../outside/secret.md",
                     "/_pp/stat?root=proj&path=link.md"):
            st, _, body = self.get(base, path)
            self.assertEqual(st, 404, path)
            self.assertNotIn("outside the tab", body)
        self.assertEqual(self.get(base, "/raw/proj/inner.md")[2], "# Guide\n\nBack to [the readme](../README.md).\n")

        # the listing names the links that leave, but neither links nor sizes them; secrets are not listed at all
        body = self.get(base, "/t/proj/?hidden=1")[2]
        self.assertIn("link.md ↪ not served", body)
        self.assertIn("outdir ↪ not served", body)
        self.assertNotIn('href="/t/proj/link.md"', body)
        self.assertNotIn("15B", body)                                # the outside file's size
        self.assertNotIn(".env", body)
        self.assertNotIn(".git-credentials", body)
        self.assertIn('href="/t/proj/inner.md"', body)
        self.stop(p)

    def test_a_readme_that_leaves_the_tab_is_named_but_not_embedded(self):
        self.write("work/tab/a.txt", "inside")
        os.symlink(f"{self.tmp}/outside/secret.md", f"{self.tmp}/work/tab/README.md")
        p, base, _ = self.serve("-q", f"{self.tmp}/work/tab")
        st, _, body = self.get(base, "/t/tab/")
        self.assertEqual(st, 200)
        self.assertIn("README.md ↪ not served", body)
        self.assertNotIn("outside the tab", body)                  # the folder page renders no README at all
        self.stop(p)

    def test_follow_symlinks_is_the_explicit_way_out(self):
        self.config("[http]\nfollow_symlinks = true\n")
        p, base, _ = self.serve("-q", cwd=self.proj)
        self.assertEqual(self.get(base, "/raw/proj/link.md")[2], "outside the tab")
        self.assertEqual(self.get(base, "/t/proj/../outside/secret.md")[0], 404)   # .. never
        self.stop(p)

    def test_a_web_page_cannot_rebind_its_domain_onto_the_server(self):
        p, base, _ = self.serve("-q", cwd=self.proj)
        port = base.rsplit(":", 1)[1]
        st, _, body = self.get(base, "/raw/proj/README.md", headers={"Host": f"evil.example:{port}"})
        self.assertEqual(st, 403)
        self.assertIn("[http] hosts", body)
        for host in (f"localhost:{port}", f"127.0.0.1:{port}", f"[::1]:{port}", f"app.localhost:{port}"):
            self.assertEqual(self.get(base, "/raw/proj/README.md", headers={"Host": host})[0], 200, host)
        self.stop(p)
        self.config('[http]\nhosts = ["box.tailnet.example"]\n')        # a proxy in front, by name
        p, base, _ = self.serve("-q", cwd=self.proj)
        self.assertEqual(self.get(base, "/", headers={"Host": "box.tailnet.example"})[0], 200)
        self.stop(p)

    def test_a_pipe_is_not_opened(self):
        os.mkfifo(f"{self.proj}/stream.md")
        p, base, _ = self.serve("-q", cwd=self.proj)
        t0 = time.monotonic()
        st, _, body = self.get(base, "/t/proj/stream.md")
        self.assertEqual(st, 403)
        self.assertIn("Not a regular file", body)
        self.assertEqual(self.get(base, "/raw/proj/stream.md")[0], 403)         # raw says the same as the view
        self.assertNotIn("stream.md", self.get(base, "/_pp/recent")[2])
        self.assertLess(time.monotonic() - t0, 5)
        self.stop(p)


class TestContent(HttpJourney):
    def test_big_binary_and_media_files(self):
        proj = self.project()
        self.config("[http]\nembed_cap = 1000\n")
        self.write("work/proj/big.log", "line\n" * 1000)
        with open(f"{proj}/blob.bin", "wb") as f:
            f.write(b"bin\0ary" * 10)
        with open(f"{proj}/clip.mp4", "wb") as f:
            f.write(bytes(range(256)) * 4)
        p, base, _ = self.serve("-q", cwd=proj)
        body = self.get(base, "/t/proj/big.log")[2]
        self.assertIn("Showing the first 1000B of 4.9K", body)
        self.assertIn('href="/raw/proj/big.log">raw</a> has it all', body)
        self.assertIn("Binary file, 70B", self.get(base, "/t/proj/blob.bin")[2])
        self.assertIn('<video class="media" src="/raw/proj/clip.mp4"', self.get(base, "/t/proj/clip.mp4")[2])
        # a video player seeks with byte ranges
        st, h, _ = self.get(base, "/raw/proj/clip.mp4", headers={"Range": "bytes=256-511"})
        self.assertEqual((st, h["content-range"], h["content-length"]), (206, "bytes 256-511/1024", "256"))
        st, h, _ = self.get(base, "/raw/proj/clip.mp4", headers={"Range": "bytes=-24"})
        self.assertEqual((st, h["content-range"]), (206, "bytes 1000-1023/1024"))
        self.assertEqual(self.get(base, "/raw/proj/clip.mp4", headers={"Range": "bytes=2000-"})[0], 416)
        st, h, _ = self.get(base, "/raw/proj/clip.mp4")
        self.assertEqual((st, h["accept-ranges"], h["content-length"]), (200, "bytes", "1024"))
        st, h, _ = self.get(base, "/raw/proj/blob.bin?dl=1")
        self.assertEqual(h["content-disposition"], "attachment; filename*=UTF-8''blob.bin")
        self.stop(p)

    def test_the_request_log_cannot_drive_the_terminal(self):
        proj = self.project()
        p, base, _ = self.serve(cwd=proj)
        self.get(base, "/%1b%5d0;pwned%07")
        _, err = self.stop(p)
        self.assertIn("404 GET /\\x1b]0;pwned\\x07", err)
        self.assertNotIn("\x1b", err)

    def test_quiet_means_no_request_log(self):
        proj = self.project()
        p, base, _ = self.serve("-q", cwd=proj)
        self.get(base, "/")
        _, err = self.stop(p)
        self.assertEqual(err, "")


if __name__ == "__main__":
    unittest.main()
