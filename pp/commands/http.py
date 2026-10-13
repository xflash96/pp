"""Browse project folders: rendered md, highlighted code

Like `python -m http.server`, but for reading what agents write across projects: markdown rendered with math
(KaTeX) and diagrams (mermaid), code highlighted with line anchors, one tab per project, a cross-project
"Recent" view (newest files first), and live reload while a file or folder changes.

    pp http                      roots from config (tabs + discovery rules); no config -> the current directory
    pp http ~/projects/webapp .  just these folders, one tab each (a file argument serves its folder)
    pp http -p 9000              port (busy -> the next free one of ~20, printed)
    pp http --no-discover        configured [[http.roots]] only, skip [[http.discover]] rules
    pp http --config-only        never fall back to the current directory
    pp http -q                   no request log

Pages: /  Recent (filter box is instant; Enter re-queries all files, `glob` takes e.g. "*.md *.py")
       /t/<tab>/<path>   folder listing (README rendered below) or file view   /raw/<tab>/<path>  raw bytes

Config: ~/.config/pp/config.toml (every key optional)

    [http]
    port = 8000
    bind = "127.0.0.1"
    recent = ["*.md"]               # what the Recent view collects
    exclude_dirs = ["target"]       # folders Recent and discovery skip (still listed and served; deny hides),
                                    #   ADDED to the defaults (node_modules .git .venv __pycache__ dist build ...)
    deny = ["*.secret"]             # ADDED to the defaults (.env .env.* *.pem *.key *.p12 *.pfx id_rsa* id_ed25519*
                                    #   id_ecdsa* id_dsa* *.sqlite credentials* .netrc .git .git-credentials
                                    #   .pgpass .pypirc .npmrc); never listed, never served, hidden or not
    follow_symlinks = false         # true: serve symlinks that leave their root
    hosts = ["box.tailnet.ts.net"]  # Host names a proxy in front may pass through (loopback names always work)
    cache_ttl = 10.0                # seconds a tab list and a Recent result are reused for before pp looks again
    scan_budget = 2.0               # seconds one Recent scan may take; the view says when a scan was cut
    scan_depth = 8                  # folders deep a Recent scan goes under a tab
    scan_entries = 100000           # entries one Recent scan reads
    recent_limit = 300              # files the Recent view lists
    list_cap = 5000                 # entries a folder listing shows
    embed_cap = 2000000             # bytes of a text file shown in its page (the raw view serves all of it)

    [[http.roots]]                  # a fixed tab
    name = "notes"
    path = "~/notes"

    [[http.discover]]               # a rule: every dir `depth` levels under `under` holding any marker is a tab
    under = "~/projects"
    depth = 1
    marker = [".git", "CLAUDE.md", "README.md"]     # optional; without it every subdirectory qualifies

Rules are re-read every ~10 s, so a new project shows up without a restart.

Security: binds 127.0.0.1 by default, so only this machine can connect. Exposing it is a deliberate act:
`--bind 0.0.0.0` (anyone on the network can read every file under every tab) or, better, keep loopback and put an
authenticating proxy / tunnel in front. On loopback, a request naming any Host but a loopback one (or one of
`hosts`) is refused, so a web page cannot rebind its own domain to 127.0.0.1 and read the tabs. Paths are confined
to their tab's real directory (symlinks out are listed but not served, unless follow_symlinks), deny globs apply to
every path component, dot-entries are hidden from listings unless ?hidden=1, pipes and devices are never opened, and
raw HTML/SVG/XML is served with a CSP sandbox so it cannot script this origin. Every answer comes from the file or
folder that opened, so a path given to something else while a request is in flight is not served. Rendering
libraries (marked, highlight.js, KaTeX, mermaid) load from cdnjs; without network the raw text is still shown.
"""
from __future__ import annotations

import argparse
import base64
import email.utils
import errno
import fnmatch
import functools
import hashlib
import html
import ipaddress
import json
import mimetypes
import os
import re
import signal
import socket
import socketserver
import stat
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pp.lib import config, paths, ui

EXCLUDE_DIRS = ["node_modules", ".git", ".venv", "venv", "__pycache__", ".cache", ".pkgs", ".triton_cache", "dist",
                "build", ".shadow-cljs", ".cpcache", ".mypy_cache", ".pytest_cache", ".tox", "target", ".next",
                "site-packages", "bazel-out", "bazel-bin", "bazel-testlogs"]
DENY = [".env", ".env.*", "*.pem", "*.key", "id_rsa*", "id_ed25519*", "id_ecdsa*", "id_dsa*", "*.p12", "*.pfx",
        "*.sqlite", "credentials*", ".netrc", ".git", ".git-credentials", ".pgpass", ".pypirc", ".npmrc"]

CDNJS = "https://cdnjs.cloudflare.com/ajax/libs"
CDN = {
    "marked": f"{CDNJS}/marked/12.0.2/marked.min.js",
    "hljs": f"{CDNJS}/highlight.js/11.9.0/highlight.min.js",
    "hljsLang": f"{CDNJS}/highlight.js/11.9.0/languages/{{}}.min.js",
    "hljsLight": f"{CDNJS}/highlight.js/11.9.0/styles/github.min.css",
    "hljsDark": f"{CDNJS}/highlight.js/11.9.0/styles/github-dark.min.css",
    "katex": f"{CDNJS}/KaTeX/0.16.9/katex.min.js",
    "katexCss": f"{CDNJS}/KaTeX/0.16.9/katex.min.css",
    "mermaid": f"{CDNJS}/mermaid/10.9.1/mermaid.min.js",
}

MD_EXT = {".md", ".markdown", ".mdown", ".mkd", ".mdx"}
IMG_EXT = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico", ".avif"}
VIDEO_EXT = {".mp4", ".webm", ".mov", ".m4v", ".ogv"}
AUDIO_EXT = {".mp3", ".wav", ".ogg", ".oga", ".flac", ".m4a", ".opus"}
README_NAMES = ("readme.md", "readme.markdown", "index.md")
SORTS = ("name", "mtime", "size")       # how a folder page sorts, and so the only values ?sort= can take

# extension -> highlight.js language (core bundle, or a languages/<x>.min.js the page loads on demand)
LANG = {
    "py": "python", "pyi": "python", "pyx": "python", "bzl": "python", "star": "python", "ipynb": "json",
    "js": "javascript", "mjs": "javascript", "cjs": "javascript", "jsx": "javascript",
    "ts": "typescript", "mts": "typescript", "cts": "typescript", "tsx": "typescript",
    "clj": "clojure", "cljs": "clojure", "cljc": "clojure", "edn": "clojure", "bb": "clojure",
    "sh": "bash", "bash": "bash", "zsh": "bash", "fish": "bash", "ksh": "bash",
    "toml": "ini", "ini": "ini", "cfg": "ini", "conf": "ini", "properties": "properties", "env": "bash",
    "yaml": "yaml", "yml": "yaml", "json": "json", "jsonl": "json", "json5": "json", "geojson": "json",
    "rs": "rust", "go": "go", "c": "c", "h": "c", "cpp": "cpp", "cc": "cpp", "cxx": "cpp", "hpp": "cpp",
    "hh": "cpp", "hxx": "cpp", "cu": "cpp", "cuh": "cpp", "ino": "cpp", "m": "objectivec", "mm": "objectivec",
    "java": "java", "kt": "kotlin", "kts": "kotlin", "scala": "scala", "groovy": "groovy", "gradle": "gradle",
    "rb": "ruby", "lua": "lua", "sql": "sql", "pl": "perl", "pm": "perl", "php": "php", "r": "r", "swift": "swift",
    "cs": "csharp", "fs": "fsharp", "hs": "haskell", "ex": "elixir", "exs": "elixir", "erl": "erlang",
    "ml": "ocaml", "jl": "julia", "dart": "dart", "el": "lisp", "lisp": "lisp", "scm": "scheme", "rkt": "scheme",
    "vim": "vim", "nix": "nix", "proto": "protobuf", "cmake": "cmake", "mk": "makefile", "make": "makefile",
    "css": "css", "scss": "scss", "less": "less", "html": "xml", "htm": "xml", "xml": "xml", "svg": "xml",
    "xsl": "xml", "plist": "xml", "vue": "xml", "tex": "latex", "sty": "latex", "cls": "latex", "bib": "latex",
    "diff": "diff", "patch": "diff", "graphql": "graphql", "gql": "graphql", "wasm": "wasm", "wat": "wasm",
    "ps1": "powershell", "bat": "dos", "cmd": "dos", "awk": "awk", "nginx": "nginx", "dockerfile": "dockerfile",
    "glsl": "glsl", "frag": "glsl", "vert": "glsl", "asm": "x86asm", "s": "x86asm", "ll": "llvm",
    "txt": "plaintext", "log": "plaintext", "csv": "plaintext", "tsv": "plaintext",
}
LANG_BY_NAME = {
    "makefile": "makefile", "gnumakefile": "makefile", "dockerfile": "dockerfile", "containerfile": "dockerfile",
    "build": "python", "workspace": "python", "build.bazel": "python", "module.bazel": "python",
    "workspace.bazel": "python", "cmakelists.txt": "cmake", "jenkinsfile": "groovy", "vagrantfile": "ruby",
    "gemfile": "ruby", "rakefile": "ruby", ".bashrc": "bash", ".zshrc": "bash", ".profile": "bash",
    ".bash_profile": "bash", ".zprofile": "bash", ".gitignore": "bash", ".dockerignore": "bash",
    ".editorconfig": "ini", ".gitconfig": "ini", "pipfile": "ini", "go.mod": "go",
}
SHEBANG = {"python": "python", "bash": "bash", "sh": "bash", "zsh": "bash", "node": "javascript", "bb": "clojure",
           "perl": "perl", "ruby": "ruby", "lua": "lua"}


@dataclass
class Settings:
    port: int = 8000
    bind: str = "127.0.0.1"
    recent: list = field(default_factory=lambda: ["*.md"])
    exclude_dirs: set = field(default_factory=lambda: set(EXCLUDE_DIRS))
    deny: list = field(default_factory=lambda: list(DENY))
    follow_symlinks: bool = False
    hosts: list = field(default_factory=list)     # Host names a browser may use besides loopback ones
    cache_ttl: float = 10.0
    scan_budget: float = 2.0       # seconds for one Recent scan over all tabs
    scan_depth: int = 8
    scan_entries: int = 100_000
    recent_limit: int = 300
    list_cap: int = 5000
    embed_cap: int = 2_000_000     # bytes of a text file embedded in its view page

    @classmethod
    def from_config(cls, cfg: dict) -> "Settings":
        """Settings from the [http] table; a value of the wrong type is a ConfigError naming the key."""
        s = cls()
        config.check_keys(cfg, "[http]", KEYS)

        def strs(k):
            v = cfg.get(k)
            if isinstance(v, str):
                return [v]
            if v is None or (isinstance(v, list) and all(isinstance(x, str) for x in v)):
                return list(v or [])
            raise config.ConfigError(f"[http] {k} = {v!r}: a string or a list of strings")

        def num(k, typ, lo):
            v = cfg[k]
            if isinstance(v, bool) or not isinstance(v, (int, float) if typ is float else int) or v < lo:
                raise config.ConfigError(f"[http] {k} = {v!r}: {'a number' if typ is float else 'an integer'} >= {lo}")
            return typ(v)
        if "port" in cfg:
            s.port = num("port", int, 0)
            if s.port > 65535:
                raise config.ConfigError(f"[http] port = {s.port}: a port is 0..65535")
        if "bind" in cfg:
            if not isinstance(cfg["bind"], str):
                raise config.ConfigError(f"[http] bind = {cfg['bind']!r}: an address like \"127.0.0.1\"")
            s.bind = cfg["bind"]
        if "recent" in cfg:
            s.recent = strs("recent") or s.recent
        s.exclude_dirs |= set(strs("exclude_dirs"))
        s.deny += [d for d in strs("deny") if d not in s.deny]
        if not isinstance(cfg.get("follow_symlinks", False), bool):
            raise config.ConfigError(f"[http] follow_symlinks = {cfg['follow_symlinks']!r}: true or false")
        s.follow_symlinks = cfg.get("follow_symlinks", False)
        s.hosts = strs("hosts")
        for k in ("cache_ttl", "scan_budget"):
            if k in cfg:
                setattr(s, k, num(k, float, 0))
        for k in ("scan_depth", "scan_entries", "recent_limit", "list_cap", "embed_cap"):
            if k in cfg:
                setattr(s, k, num(k, int, 1))
        return s


KEYS = ("port", "bind", "recent", "exclude_dirs", "deny", "follow_symlinks", "hosts", "roots", "discover", "cache_ttl",
        "scan_budget", "scan_depth", "scan_entries", "recent_limit", "list_cap", "embed_cap")
TABLE_KEYS = {"roots": ("name", "path"), "discover": ("under", "depth", "marker")}


def tables(cfg: dict, key: str, need: str) -> list[dict]:
    """[[http.<key>]]: an array of tables, each with `need` and no other key than its own; anything else is a
    ConfigError naming the file and the key. A misspelt `marker` served every folder under `under`, so a key
    pp does not read stops the server here, where the file is read, rather than widening what it serves."""
    v = cfg.get(key, [])
    if not isinstance(v, list) or not all(isinstance(t, dict) for t in v):
        raise config.ConfigError(f"[http] {key} must be written [[http.{key}]] (an array of tables)")
    for t in v:
        # the keys first, so a misspelt `path` is named as the key it is and not reported as a missing one
        config.check_keys(t, f"{config.global_path()} [[http.{key}]]", TABLE_KEYS[key])
        if not isinstance(t.get(need), str) or not t[need]:
            raise config.ConfigError(f"[[http.{key}]] {t}: needs {need} = \"/some/dir\"")
    for t in v if key == "discover" else []:
        d, m = t.get("depth", 1), t.get("marker", [])
        if isinstance(d, bool) or not isinstance(d, int) or d < 1:
            raise config.ConfigError(f"[[http.discover]] under = {t['under']!r}: depth = {d!r} is not an integer >= 1")
        if not isinstance(m, str) and not (isinstance(m, list) and all(isinstance(x, str) for x in m)):
            raise config.ConfigError(f"[[http.discover]] under = {t['under']!r}: marker is a string or a list of them")
    return v


def _globs(spec) -> list[str]:
    if isinstance(spec, str):
        spec = spec.replace(",", " ").split()
    return [g for g in (spec or []) if g]


def _match(globs: list[str], name: str, rel: str) -> bool:
    name, rel = name.lower(), rel.lower()
    for g in globs:
        g = g.lower()
        if "/" in g:
            if fnmatch.fnmatchcase(rel, g) or (g.startswith("**/") and fnmatch.fnmatchcase(rel, g[3:])):
                return True
        elif fnmatch.fnmatchcase(name, g):
            return True
    return False


class Cached:
    """A value per key, computed by one caller at a time and no oftener than once every `ttl` seconds.

    `get(key)` returns the value for that key, computing it with `compute(key)` when there is none or it is
    older than the ttl. The computation runs under the lock, so callers that arrive while a value is stale
    make one computation between them and all read its answer, and this is the one place a value is stored:
    no two computations overlap, so no answer can replace a newer one. A value is stamped with the time its
    computation started, so a slow scan is not passed off as fresh, and keys older than the ttl are dropped,
    so the keys a request names cannot pile up.
    """

    def __init__(self, ttl: float, compute):
        self.ttl = ttl
        self._compute = compute
        self._lock = threading.Lock()
        self._values: dict = {}

    def get(self, key=()):
        with self._lock:
            now = time.monotonic()
            hit = self._values.get(key)
            if hit is not None and now - hit[0] < self.ttl:
                return hit[1]
            value = self._compute(key)
            self._values = {k: v for k, v in self._values.items() if now - v[0] < self.ttl}
            self._values[key] = (now, value)
            return value


class App:
    """Roots (fixed + discovered, cached), path confinement, the Recent scan."""

    def __init__(self, settings: Settings, fixed: list[tuple[str, str]], rules: list[dict], quiet: bool = False):
        self.s = settings
        self.fixed = fixed
        self.rules = rules
        self.quiet = quiet
        self._tabs = Cached(settings.cache_ttl, self._walk)
        self._found = Cached(settings.cache_ttl, self._scan_once)

    # ---- roots ----

    def denied(self, name: str) -> bool:
        n = name.lower()
        return any(fnmatch.fnmatchcase(n, p.lower()) for p in self.s.deny)

    def _discover(self, rule: dict) -> list[tuple[str, str]]:
        if not rule.get("under"):
            return []
        under = paths.expand(str(rule["under"]))
        depth = max(1, int(rule.get("depth", 1)))
        markers = rule.get("marker") or []
        markers = [markers] if isinstance(markers, str) else list(markers)
        level = [(under, "")]
        for _ in range(depth):
            nxt = []
            for d, rel in level:
                try:
                    entries = sorted(os.scandir(d), key=lambda e: e.name)
                except OSError:
                    continue
                for e in entries:
                    if e.name.startswith(".") or e.name in self.s.exclude_dirs or self.denied(e.name):
                        continue
                    try:
                        if not e.is_dir():
                            continue
                    except OSError:
                        continue
                    nxt.append((e.path, f"{rel}/{e.name}" if rel else e.name))
            level = nxt
        return [(rel, d) for d, rel in level
                if not markers or any(os.path.exists(os.path.join(d, m)) for m in markers)]

    def roots(self) -> dict[str, str]:
        """tab name -> real directory, fixed tabs first, then discovered ones by name. One walk every
        cache_ttl seconds, whoever asks."""
        return self._tabs.get()

    def _walk(self, _key) -> dict[str, str]:
        found = list(self.fixed)
        disc = []
        for rule in self.rules:
            disc += self._discover(rule)
        found += sorted(disc)
        out: dict[str, str] = {}
        seen = set()
        for name, p in found:
            real = os.path.realpath(paths.expand(p))
            if real in seen or not os.path.isdir(real):
                continue
            seen.add(real)
            base, k = name or os.path.basename(real) or "root", 2
            name = base
            while name in out:
                name, k = f"{base}-{k}", k + 1
            out[name] = real
        return out

    def resolve(self, root_name: str, segs: list[str]) -> tuple[str, str] | None:
        """(root real dir, real path) for a tab-relative path, or None when it must 404."""
        root = self.roots().get(root_name)
        if root is None:
            return None
        for s in segs:
            if s in (".", "..") or "/" in s or "\\" in s or "\0" in s or self.denied(s):
                return None
        real = os.path.realpath(os.path.join(root, *segs))
        inside = real == root or real.startswith(root.rstrip(os.sep) + os.sep)
        if not inside and not self.s.follow_symlinks:
            return None
        comps = os.path.relpath(real, root).split(os.sep) if inside and real != root else [os.path.basename(real)]
        if real != root and any(self.denied(c) for c in comps):
            return None
        if not os.path.exists(real):
            return None
        return root, real

    # ---- Recent ----

    def _scan(self, roots: dict[str, str], globs: list[str]) -> tuple[list[dict], bool]:
        items: list[dict] = []
        truncated = False
        t0 = time.monotonic()
        n = 0
        names = list(roots)
        for i, name in enumerate(names):
            # each tab gets its share of the budget; time a tab leaves unused passes to the next
            deadline = t0 + self.s.scan_budget * (i + 1) / len(names)
            cap = self.s.scan_entries * (i + 1) // len(names)
            stack = [(roots[name], "", 0)]
            while stack:
                d, rel, depth = stack.pop()
                try:
                    it = os.scandir(d)
                except OSError:
                    continue
                with it:
                    for e in it:
                        n += 1
                        if n > cap or time.monotonic() > deadline:
                            truncated = True
                            stack = []
                            break
                        nm = e.name
                        if nm.startswith(".") or self.denied(nm):
                            continue
                        try:
                            if e.is_symlink() and not self.s.follow_symlinks:
                                continue
                            if e.is_dir():
                                if nm not in self.s.exclude_dirs and depth < self.s.scan_depth:
                                    stack.append((e.path, f"{rel}{nm}/", depth + 1))
                                continue
                            if not _match(globs, nm, rel + nm) or not e.is_file():     # no pipes, sockets
                                continue
                            st = e.stat()
                        except OSError:
                            continue
                        items.append({"root": name, "path": rel + nm, "size": st.st_size, "mtime": st.st_mtime})
        items.sort(key=lambda x: -x["mtime"])
        return items, truncated

    def _scan_once(self, key) -> tuple[list[dict], bool, float]:
        """One scan for a (roots, globs) key, with how long it took."""
        t0 = time.monotonic()
        items, truncated = self._scan(dict(key[0]), list(key[1]))
        return items, truncated, time.monotonic() - t0

    def recent(self, globs: list[str] | None = None, q: str = "", limit: int | None = None) -> dict:
        globs = globs or self.s.recent
        roots = self.roots()
        items, truncated, took = self._found.get((tuple(roots.items()), tuple(globs)))
        if q:
            ql = q.lower()
            items = [x for x in items if ql in f"{x['root']}/{x['path']}".lower()]
        return {"items": items[: limit or self.s.recent_limit], "total": len(items), "truncated": truncated,
                "took": round(took, 3), "globs": globs, "roots": len(roots)}


# ---- HTML -----------------------------------------------------------------------------------------

def esc(s) -> str:
    return html.escape(str(s), quote=True)


def q(s: str) -> str:
    """The one encoder for every name that reaches a URL. A POSIX name is bytes, and one that is not valid
    UTF-8 comes back from the filesystem with surrogate escapes, so what is quoted is the bytes."""
    return urllib.parse.quote(os.fsencode(s), safe="")


def unq(s: str) -> str:
    """The name a URL segment stands for: `q` the other way round, so a name pp was given round-trips."""
    return os.fsdecode(urllib.parse.unquote_to_bytes(s))


def turl(root: str, segs: list[str], is_dir: bool = False, kind: str = "t") -> str:
    tail = "/".join(q(s) for s in segs)
    return f"/{kind}/{q(root)}/" + tail + ("/" if is_dir and segs else "")


def _json_script(id_: str, obj) -> str:
    data = json.dumps(obj, ensure_ascii=False).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    return f'<script type="application/json" id="{id_}">{data}</script>'


def _age(ts: float) -> str:
    return ui.human_age(ts).replace(" ago", "")


def _when(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


FAVICON = ("data:image/svg+xml," + urllib.parse.quote(
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'><rect width='16' height='16' rx='3' fill='#0969da'/>"
    "<text x='8' y='12' font-size='10' text-anchor='middle' fill='white' font-family='sans-serif'>pp</text></svg>"))


def page(app: App, title: str, active: str | None, body: str, data: dict, src: str | None = None,
         libs_css: bool = False) -> bytes:
    roots = app.roots()
    tabs = [f'<a href="/" class="{"on" if active is None else ""}">Recent</a>']
    tabs += [f'<a href="{turl(n, [], True)}" class="{"on" if n == active else ""}" title="{esc(p)}">{esc(n)}</a>'
             for n, p in roots.items()]
    head_css = ""
    if libs_css:
        head_css = (f'<link rel="stylesheet" href="{CDN["hljsLight"]}" media="(prefers-color-scheme: light)">'
                    f'<link rel="stylesheet" href="{CDN["hljsDark"]}" media="(prefers-color-scheme: dark)">'
                    f'<link rel="stylesheet" href="{CDN["katexCss"]}">')
    data = dict(data, cdn=CDN)
    doc = (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        '<meta name=viewport content="width=device-width,initial-scale=1">'
        f"<title>{esc(title)}</title><link rel=icon href=\"{FAVICON}\">{head_css}<style>{CSS}</style></head><body>"
        f'<header><nav class="tabs"><span class="brand">pp</span>{"".join(tabs)}</nav></header>'
        f"<main>{body}</main>"
        + _json_script("pp-page", data)
        + (_json_script("pp-src", src) if src is not None else "")
        + f"<script>{JS}</script></body></html>"
    )
    return doc.encode("utf-8", "replace")        # a name that is no text is shown as ?, and its links still work


def crumbs(root: str, segs: list[str], is_dir: bool) -> str:
    parts = [f'<a href="{turl(root, [], True)}">{esc(root)}</a>']
    for i, s in enumerate(segs):
        if i == len(segs) - 1:
            parts.append(f"<b>{esc(s)}</b>")
        else:
            parts.append(f'<a href="{turl(root, segs[: i + 1], True)}">{esc(s)}</a>')
    return '<nav class="crumbs">' + ' <span class="sep">/</span> '.join(parts) + "</nav>"


def guess_lang(name: str, head: str) -> str:
    low = name.lower()
    if low in LANG_BY_NAME:
        return LANG_BY_NAME[low]
    if low.endswith(".dockerfile") or low.startswith("dockerfile."):
        return "dockerfile"
    ext = low.rsplit(".", 1)[1] if "." in low.strip(".") else ""
    if ext in LANG:
        return LANG[ext]
    if head.startswith("#!"):
        first = head.split("\n", 1)[0]
        words = first[2:].replace("/", " ").split()
        for w in reversed(words):
            w = w.rstrip("0123456789.")
            if w in SHEBANG:
                return SHEBANG[w]
    return ""


def raw_type(name: str, head: bytes) -> tuple[str, bool]:
    """(Content-Type, needs a CSP sandbox) for raw serving. A type can only come from the name's extension;
    everything else about the answer comes from the open file. The leading slash keeps a name like `data:x`
    an extension rather than a URL scheme."""
    mime, _ = mimetypes.guess_type("/" + name)
    if mime == "image/svg+xml":
        return mime, True
    if mime and (mime.split("/")[0] in ("image", "audio", "video", "font") or mime == "application/pdf"):
        return mime, False
    if b"\0" in head:
        return mime or "application/octet-stream", False
    if mime in ("text/html", "application/xhtml+xml"):
        return "text/html; charset=utf-8", True
    if mime in ("application/xml", "text/xml"):
        return mime + "; charset=utf-8", True
    if mime == "text/css":
        return "text/css; charset=utf-8", False
    if mime in ("text/javascript", "application/javascript"):
        return "text/javascript; charset=utf-8", False
    if mime == "application/json":
        return "application/json; charset=utf-8", False
    return "text/plain; charset=utf-8", False


# ---- request handling -----------------------------------------------------------------------------

def _loopback(host: str) -> bool:
    """A name or address that only ever means this machine."""
    h = host.strip("[]").lower().rstrip(".")
    if h == "localhost" or h.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


class Opened:
    """What a request may act on: a folder or file inside a tab, open, with the fstat of that open fd.

    True when something opened. `why` says what to answer when nothing did: "gone" (nothing there, not this
    tab's, or not what the approved name means any more), "kind" (a folder where a file was asked for, or the
    other way round), "irregular" (a pipe, socket or device, whose contents pp does not read) or "unreadable"
    (`error` is the error the open gave). Used as `with`, so the fd is closed on every way out.
    """

    __slots__ = ("fd", "st", "why", "error")

    def __init__(self, fd: int | None = None, st: os.stat_result | None = None, why: str = "",
                 error: OSError | None = None):
        self.fd, self.st, self.why, self.error = fd, st, why, error

    def __bool__(self) -> bool:
        return self.fd is not None

    def __enter__(self) -> "Opened":
        return self

    def __exit__(self, *exc) -> bool:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        return False

    @property
    def is_dir(self) -> bool:
        return stat.S_ISDIR(self.st.st_mode)

    def entries(self):
        """What this folder holds, read through the fd it opened on: the listing is of the folder the request
        acted on, whatever its name means by now. Used as `with`, as os.scandir is."""
        return os.scandir(self.fd)


def _not_opened(e: OSError, real: str) -> Opened:
    """Why an open failed: a name that has gone in the meantime is not found, anything else is pp not reading
    what is there. The error names the path that was asked for, not the fd it was opened through."""
    gone = e.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP)
    return Opened(why="gone" if gone else "unreadable", error=OSError(e.errno, e.strerror, real))


def open_in_tab(app: App, root: str, segs: list[str], *, dir_ok: bool, file_ok: bool) -> Opened:
    """Open what a tab-relative path names, so every answer about it comes from the file that opened.

    App.resolve approves the name. Nothing freezes the tree between a check and an open, so a directory
    component swapped for a symlink in between would be followed and the answer would be a file outside every
    tab. The first open therefore takes an O_PATH fd, which reads nothing and starts no device, and
    /proc/self/fd says whether that fd is the path that was approved. The fstat of that fd says what this is,
    and only a folder or a regular file is then opened for its contents, through the fd already held, so the
    bytes and the fstat returned here belong to one file. Every decision the request makes comes from that
    fstat, the live-reload baseline included, so a page's baseline is the file its body was read from.

    A tab's own root is a folder and nothing else, so an empty path is never a file here, whatever the caller
    asks for: App._walk admits a directory and keeps the name only while it is one, and a tab whose directory
    has become a regular file is stale. That holds for every endpoint, so none of them can be left serving it.
    """
    file_ok = file_ok and bool(segs)
    r = app.resolve(root, segs)
    if r is None:
        return Opened(why="gone")
    real = r[1]
    try:
        pin = os.open(real, os.O_PATH)
    except OSError as e:
        return _not_opened(e, real)
    try:
        if os.path.realpath(f"/proc/self/fd/{pin}") != real:
            return Opened(why="gone")
        st = os.fstat(pin)
        if stat.S_ISDIR(st.st_mode):
            if not dir_ok:
                return Opened(why="kind")
            flags = os.O_RDONLY | os.O_DIRECTORY
        elif stat.S_ISREG(st.st_mode):
            if not file_ok:
                return Opened(why="kind")
            flags = os.O_RDONLY
        else:
            return Opened(why="irregular")
        try:
            fd = os.open(f"/proc/self/fd/{pin}", flags)
        except OSError as e:
            return _not_opened(e, real)
        return Opened(fd, os.fstat(fd))
    finally:
        os.close(pin)


class Handler(BaseHTTPRequestHandler):
    app: App = None   # set on the subclass made by make_server
    host_guard = True   # bound to loopback: refuse Host names that are not (DNS rebinding)
    server_version = "pp-http"
    sys_version = ""

    def log_request(self, code="-", size="-"):
        code = getattr(code, "value", code)
        if self.app.quiet or (self.path.startswith("/_pp/stat") and code == 200):
            return
        sys.stderr.write(f"{time.strftime('%H:%M:%S')} {code} {self.command} "
                         f"{ui.printable(urllib.parse.unquote(self.path)[:160], tabs=False)}\n")

    def log_message(self, fmt, *args):
        if not self.app.quiet:
            sys.stderr.write(f"{time.strftime('%H:%M:%S')} {ui.printable(fmt % args, tabs=False)}\n")

    def do_GET(self):
        self._dispatch()

    def do_HEAD(self):
        self._dispatch()

    # ---- plumbing ----

    def _send(self, code: int, body: bytes, ctype: str, headers: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        if ctype.startswith("text/html"):
            self.send_header("Content-Security-Policy", _csp())
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, obj, code: int = 200):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8", "replace"),
                   "application/json; charset=utf-8")

    def _redirect(self, loc: str):
        self.send_response(301)
        self.send_header("Location", loc)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _not_found(self, what: str = ""):
        body = page(self.app, "not found", "", f'<p class="note">Not found{": " + esc(what) if what else ""}.</p>'
                    '<p><a href="/">Recent</a></p>', {"kind": "none"})
        self._send(404, body, "text/html; charset=utf-8")

    def _unreadable(self, root, ex):
        """403: what opened cannot be read, and the error says why."""
        self._send(403, page(self.app, "unreadable", root, f'<p class="note">{esc(ex)}</p>', {"kind": "none"}),
                   "text/html; charset=utf-8")

    def _refuse(self, o: Opened, root, segs):
        """The answer when nothing opened: 403 for what pp will not read, 404 for everything else."""
        if o.why == "irregular":
            return self._not_a_file(root, segs)
        if o.why == "unreadable":
            return self._unreadable(root, o.error)
        return self._not_found("/".join([root] + segs))

    def _host_ok(self) -> bool:
        h = self.headers.get("Host")
        if not self.host_guard or h is None:      # no Host: an HTTP/1.0 tool, never a browser
            return True
        try:
            name = urllib.parse.urlsplit("//" + h).hostname or ""
        except ValueError:
            return False
        return _loopback(name) or name in (x.lower() for x in self.app.s.hosts)

    def _dispatch(self):
        try:
            if not self._host_ok():
                msg = (f"pp http: Host {self.headers['Host']!r} is not a loopback name. If a proxy in front of pp "
                       "uses it, add it to [http] hosts in ~/.config/pp/config.toml.\n")
                return self._send(403, msg.encode(), "text/plain; charset=utf-8")
            u = urllib.parse.urlsplit(self.path)
            qs = {k: v[-1] for k, v in urllib.parse.parse_qs(u.query).items()}
            if not u.path.startswith("/"):
                return self._not_found()
            raw = u.path.split("/")[1:]
            if u.path == "/":
                return self._recent_page(qs)
            if u.path == "/_pp/stat":
                return self._stat(qs)
            if u.path == "/_pp/recent":
                return self._recent_json(qs)
            if raw[0] in ("t", "raw") and len(raw) >= 2 and raw[1]:
                root = unq(raw[1])
                segs = [unq(s) for s in raw[2:] if s != ""]
                if raw[0] == "t":
                    return self._view(root, segs, u.path.endswith("/") and len(raw) > 2, qs)
                return self._raw(root, segs, qs)
            return self._not_found()
        except (BrokenPipeError, ConnectionResetError):
            pass

    # ---- endpoints ----

    def _stat(self, qs):
        """What the live reload polls: the mtime and size of the file or folder that opened.

        It answers from an fd like every other endpoint, so the page it feeds compares its baseline against
        the same file it was built from, and a name given to something else since is not answered for."""
        segs = [s for s in qs.get("path", "").split("/") if s]
        with open_in_tab(self.app, qs.get("root", ""), segs, dir_ok=True, file_ok=True) as o:
            if not o:
                return self._json({"error": "not found"}, 404)
            self._json({"mtime": o.st.st_mtime, "size": o.st.st_size, "dir": o.is_dir})

    def _recent_json(self, qs):
        try:
            limit = int(qs.get("limit") or 0) or None
        except ValueError:
            limit = None
        self._json(self.app.recent(_globs(qs.get("glob")), qs.get("q", ""), limit))

    def _recent_page(self, qs):
        glob_s, qtext = qs.get("glob", ""), qs.get("q", "")
        res = self.app.recent(_globs(glob_s), qtext)
        rows = []
        for x in res["items"]:
            segs = x["path"].split("/")
            rows.append(
                f'<tr data-k="{esc((x["root"] + "/" + x["path"]).lower())}">'
                f'<td><a class="tag" href="{turl(x["root"], [], True)}">{esc(x["root"])}</a></td>'
                f'<td class="p"><a href="{turl(x["root"], segs)}">{esc(x["path"])}</a></td>'
                f'<td class="r hide-sm">{ui.human_size(x["size"])}</td>'
                f'<td class="r" title="{_when(x["mtime"])}">{_age(x["mtime"])}</td></tr>')
        note = (f'<span id="pp-count">{len(res["items"])}</span> of {res["total"]} files matching '
                f'{esc(" ".join(res["globs"]))} across {res["roots"]} tab(s), scanned in {res["took"]:.2f}s')
        if res["truncated"]:
            note += "; <b>scan truncated</b> (time/entry cap): narrow the glob or the tabs"
        body = (
            '<form class="bar" method="get" action="/">'
            f'<input id="pp-filter" name="q" value="{esc(qtext)}" placeholder="filter… (instant; Enter searches all)" autocomplete="off">'
            f'<input name="glob" value="{esc(glob_s)}" placeholder="{esc(" ".join(self.app.s.recent))}" title="globs, space-separated" class="glob">'
            '<button type="submit">scan</button><span id="pp-upd" class="badge" hidden></span></form>'
            '<div id="pp-main">'
            + ('<div class="scroll"><table class="list"><thead><tr><th>tab</th><th>path</th><th class="r hide-sm">size</th>'
               '<th class="r">modified</th></tr></thead><tbody>' + "".join(rows) + "</tbody></table></div>"
               if rows else '<p class="note">Nothing matches.</p>')
            + f'<p class="note">{note}</p></div>')
        self._send(200, page(self.app, "Recent (pp http)", None, body, {"kind": "recent"}), "text/html; charset=utf-8")

    def _view(self, root: str, segs: list[str], slash: bool, qs):
        with open_in_tab(self.app, root, segs, dir_ok=True, file_ok=True) as o:
            if not o:
                return self._refuse(o, root, segs)
            if o.is_dir:
                if segs and not slash:
                    return self._redirect(turl(root, segs, True) + (f"?{urllib.parse.urlencode(qs)}" if qs else ""))
                if not segs and not self.path.split("?")[0].endswith("/"):
                    return self._redirect(turl(root, [], True))
                return self._dir(root, segs, o, qs)
            return self._file(root, segs, o)

    def _dir(self, root, segs, o: Opened, qs):
        hidden = qs.get("hidden") == "1"
        sort = qs.get("sort", "name")
        if sort not in SORTS:       # the page writes this value back into its own links, so it is one of ours
            sort = "name"
        entries, capped, dots = [], False, 0
        try:
            with o.entries() as it:             # the folder that opened, listing itself through its own fd
                for e in it:
                    if self.app.denied(e.name):
                        continue
                    if e.name.startswith(".") and not hidden:
                        dots += 1
                        continue
                    link = e.is_symlink()
                    if link and not self.app.resolve(root, segs + [e.name]):
                        # leaves the tab, is denied or dangles: named, but neither linked nor stat'ed
                        entries.append((e.name, False, 0, 0, "out"))
                        continue
                    try:
                        is_dir = e.is_dir()
                        est = e.stat()
                        size, mtime = est.st_size, est.st_mtime
                    except OSError:
                        is_dir, size, mtime = False, 0, 0
                    entries.append((e.name, is_dir, size, mtime, "in" if link else ""))
                    if len(entries) >= self.app.s.list_cap:
                        capped = True
                        break
        except OSError as ex:
            return self._unreadable(root, ex)
        keyf = {"mtime": lambda x: -x[3], "size": lambda x: -x[2]}.get(sort, lambda x: x[0].lower())
        entries.sort(key=lambda x: (not x[1], keyf(x)))
        extra = "?hidden=1" if hidden else ""
        rows = []
        for name, is_dir, size, mtime, link in entries:
            if link == "out":
                why = ("a symlink that leaves this tab, points at a denied name or points nowhere"
                       + ("" if self.app.s.follow_symlinks else "; [http] follow_symlinks = true serves links out"))
                rows.append(f'<tr><td class="p"><span class="note" title="{esc(why)}">{esc(name)} ↪ not served</span>'
                            '</td><td class="hide-sm"></td><td></td></tr>')
                continue
            href = turl(root, segs + [name], is_dir) + (extra if is_dir else "")
            label = esc(name) + ("/" if is_dir else "") + (' <span class="note">↪</span>' if link else "")
            rows.append(f'<tr><td class="p"><a href="{href}" class="{"d" if is_dir else ""}">{label}</a></td>'
                        f'<td class="r hide-sm">{"" if is_dir else ui.human_size(size)}</td>'
                        f'<td class="r" title="{_when(mtime) if mtime else ""}">{_age(mtime) if mtime else ""}</td></tr>')
        here = turl(root, segs, True)
        hq = "&hidden=1" if hidden else ""
        sorts = " · ".join(f'<a href="{here}?sort={k}{hq}" class="{"on" if sort == k else ""}">{k}</a>'
                           for k in SORTS)
        toggle = (f'<a href="{here}{"?sort=" + sort if sort != "name" else ""}">hide dotfiles</a>' if hidden
                  else f'<a href="{here}?hidden=1{"&sort=" + sort if sort != "name" else ""}">show dotfiles</a>')
        files = {n.lower(): n for n, d, *_ in entries if not d}
        readme_name = next((files[k] for k in README_NAMES if k in files), None)
        readme_src = self._embed(root, segs + [readme_name]) if readme_name else None
        if readme_src is None:
            readme_name = None
        body = (
            crumbs(root, segs, True)
            + f'<div class="bar tools"><span class="meta">sort: {sorts}</span><span class="meta">{toggle}</span>'
              '<span id="pp-upd" class="badge" hidden></span></div>'
            + '<div id="pp-main"><div class="scroll"><table class="list"><thead><tr><th>name</th>'
              '<th class="r hide-sm">size</th><th class="r">modified</th></tr></thead><tbody>'
            + (f'<tr><td class="p"><a href="{turl(root, segs[:-1], True)}{extra}" class="d">../</a></td><td class="hide-sm"></td><td></td></tr>'
               if segs else "")
            + "".join(rows) + "</tbody></table></div>"
            + ("" if rows else f'<p class="note">Empty folder{f" but for {dots} hidden dotfile(s)" if dots else ""}.</p>')
            + (f'<p class="note">Showing the first {self.app.s.list_cap} entries.</p>' if capped else "")
            + (f'<div class="readme"><div class="readme-h"><a href="{turl(root, segs + [readme_name])}">{esc(readme_name)}</a></div>'
               '<div id="pp-body"></div></div>' if readme_name else "")
            + "</div>")
        data = {"kind": "dir", "root": root, "path": "/".join(segs), "mtime": o.st.st_mtime, "size": o.st.st_size,
                "readme": bool(readme_name)}
        title = ("/".join(segs) or root) + " (pp http)"
        self._send(200, page(self.app, title, root, body, data, readme_src, libs_css=bool(readme_name)),
                   "text/html; charset=utf-8")

    def _embed(self, root, segs) -> str | None:
        """A README's text for the folder page it sits in, or None when this tab has no business reading it.
        It opens like any other path, so a symlink out of the tab is named in the listing and not embedded,
        and what is read is the file that opened."""
        with open_in_tab(self.app, root, segs, dir_ok=False, file_ok=True) as o:
            if not o:
                return None
            try:
                with os.fdopen(o.fd, "rb", closefd=False) as f:
                    return f.read(self.app.s.embed_cap).decode("utf-8", "replace")
            except OSError:
                return None

    def _not_a_file(self, root, segs):
        """A pipe, socket or device under a tab: never opened, the same answer from the view and from /raw/."""
        return self._send(403, page(self.app, "not a file", root, crumbs(root, segs, False)
                                    + '<p class="note">Not a regular file (a pipe, socket or device): not opened.</p>',
                                    {"kind": "none"}), "text/html; charset=utf-8")

    def _file(self, root, segs, o: Opened):
        st = o.st                               # the file that opened, not whatever the name means now
        try:
            with os.fdopen(o.fd, "rb", closefd=False) as f:
                head = f.read(self.app.s.embed_cap + 1)
        except OSError as ex:
            return self._unreadable(root, ex)
        name = segs[-1]
        ext = os.path.splitext(name)[1].lower()
        raw = turl(root, segs, kind="raw")
        binary = b"\0" in head[:8192]
        data = {"kind": "code", "root": root, "path": "/".join(segs), "mtime": st.st_mtime, "size": st.st_size,
                "raw": raw}
        src = None
        tools = [f'<span class="meta">{ui.human_size(st.st_size)} · modified <span title="{_when(st.st_mtime)}">'
                 f'{_age(st.st_mtime)} ago</span></span>',
                 f'<a href="{raw}">raw</a>', f'<a href="{raw}?dl=1">download</a>']
        if ext in IMG_EXT:
            data["kind"] = "image"
            main = f'<img class="media" src="{raw}?v={st.st_mtime}" alt="{esc(name)}">'
        elif ext in VIDEO_EXT:
            data["kind"] = "media"
            main = f'<video class="media" src="{raw}" controls preload="metadata"></video>'
        elif ext in AUDIO_EXT:
            data["kind"] = "media"
            main = f'<audio src="{raw}" controls preload="metadata"></audio>'
        elif ext == ".pdf":
            data["kind"] = "media"
            main = f'<embed class="pdf" src="{raw}" type="application/pdf">'
        elif binary:
            data["kind"] = "binary"
            main = (f'<p class="note">Binary file, {ui.human_size(st.st_size)}. '
                    f'<a href="{raw}?dl=1">Download</a></p>')
        else:
            truncated = len(head) > self.app.s.embed_cap
            src = head[: self.app.s.embed_cap].decode("utf-8", "replace")
            data["truncated"] = truncated
            if ext in MD_EXT:
                data["kind"] = "md"
                tools.append('<button id="pp-source" type="button">source</button>')
            else:
                data["lang"] = guess_lang(name, src[:200])
                tools.append('<button id="pp-wrap" type="button">wrap</button>')
            if ext in (".html", ".htm", ".svg"):
                tools.append(f'<a href="{raw}" target="_blank" rel="noopener">open rendered (sandboxed)</a>')
            main = '<div id="pp-body"></div><noscript><p class="note">Needs JavaScript; see the raw link.</p></noscript>'
            if truncated:
                main = (f'<p class="note">Showing the first {ui.human_size(self.app.s.embed_cap)} of '
                        f'{ui.human_size(st.st_size)}. Live reload is off; <a href="{raw}">raw</a> has it all.</p>' + main)
        tools.append('<span id="pp-upd" class="badge" hidden></span>')
        body = (crumbs(root, segs, False) + f'<div class="bar tools">{" ".join(tools)}</div>'
                f'<div id="pp-main" class="{"wide" if data["kind"] == "code" else ""}">{main}</div>')
        self._send(200, page(self.app, f"{name} (pp http)", root, body, data, src, libs_css=data["kind"] in ("md", "code")),
                   "text/html; charset=utf-8")

    def _raw(self, root, segs, qs):
        with open_in_tab(self.app, root, segs, dir_ok=False, file_ok=True) as o:
            if not o:
                return self._refuse(o, root, segs)
            self._send_bytes(segs, qs, o)

    def _send_bytes(self, segs, qs, o: Opened):
        """The bytes of the file that opened, with the one byte range a media player asks for."""
        st = o.st
        with os.fdopen(o.fd, "rb", closefd=False) as f:
            head = f.read(8192)
            ctype, sandbox = raw_type(segs[-1], head)
            start, end = 0, st.st_size - 1
            # one byte range, so a video can seek (Safari will not play without it); anything else gets it all
            m = re.fullmatch(r"bytes=(\d*)-(\d*)", (self.headers.get("Range") or "").strip())
            if m and (m[1] or m[2]):
                start, end = ((int(m[1]), min(int(m[2] or end), end)) if m[1]
                              else (max(0, st.st_size - int(m[2])), end))
                if start > end:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{st.st_size}")
                    self.send_header("Content-Length", "0")
                    return self.end_headers()
            self.send_response(206 if m and (m[1] or m[2]) else 200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(end - start + 1))
            if m and (m[1] or m[2]):
                self.send_header("Content-Range", f"bytes {start}-{end}/{st.st_size}")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Last-Modified", email.utils.formatdate(st.st_mtime, usegmt=True))
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Content-Type-Options", "nosniff")
            if sandbox:
                self.send_header("Content-Security-Policy", "sandbox")
            if qs.get("dl") == "1":
                self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{q(segs[-1])}")
            self.end_headers()
            if self.command == "HEAD":
                return
            f.seek(start)
            left = end - start + 1
            while left > 0:
                chunk = f.read(min(65536, left))
                if not chunk:
                    break
                self.wfile.write(chunk)
                left -= len(chunk)


@functools.cache
def _csp() -> str:
    """Rendered markdown is untrusted HTML (any repo's README). Only our one inline script (pinned by hash) and
    cdnjs may run, so an injected <img onerror=...> or javascript: link is inert. Styles stay inline-able for
    KaTeX and mermaid; images may come from anywhere, as markdown images do. Only this origin may frame a page,
    so a page elsewhere cannot open one in a hidden frame and read it."""
    h = base64.b64encode(hashlib.sha256(JS.encode()).digest()).decode()
    return (f"default-src 'self'; script-src 'sha256-{h}' https://cdnjs.cloudflare.com; "
            "style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; font-src 'self' data: https://cdnjs.cloudflare.com; "
            "img-src 'self' data: blob: https: http:; media-src 'self'; object-src 'self'; frame-src 'self'; "
            "connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'self'")


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self):
        # HTTPServer.server_bind does a reverse-DNS getfqdn(); skip it (slow on some hosts, useless here)
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

    def handle_error(self, request, client_address):
        if isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)


def make_server(app: App, bind: str, port: int, tries: int = 20) -> ThreadingHTTPServer:
    """Bind port, or the next free one of `tries` (port 0: the OS picks)."""
    handler = type("PPHandler", (Handler,), {"app": app, "host_guard": _loopback(bind)})
    family = socket.AF_INET6 if ":" in bind else socket.AF_INET
    cls = type("PPServer", (_Server,), {"address_family": family})
    last = None
    ports = [0] if port == 0 else range(port, min(port + tries, 65536))
    for p in ports:
        try:
            return cls((bind, p), handler)
        except OSError as e:
            if e.errno not in (errno.EADDRINUSE, errno.EACCES):
                raise OSError(f"cannot listen on {bind} port {p}: {e.strerror or e}") from None
            last = e
    raise OSError(f"no free port in {ports[0]}..{ports[-1]} on {bind}: {last.strerror}")


class Serving:
    """The serving loop, and the clean stop SIGINT or SIGTERM asks for.

    `with Serving(server) as s:` serves in a thread, with both signals blocked in every thread, so a stop that
    arrives at any moment stays pending until `s.wait()` takes it and none can be missed between two
    statements. `wait()` returns the signal it took. Leaving the block stops taking connections: `shutdown()`
    returns only once the serving loop has exited, so the thread that serves can never be the one to call it,
    and the thread that waits here serves nothing. The threads that answer requests are daemons, so a response
    still being written is cut short when pp exits.
    """

    POLL = 0.1      # how long the serving loop may take to notice the stop, so how long Ctrl-C takes

    SIGS = (signal.SIGINT, signal.SIGTERM)      # the stop a person asks for, however they ask

    def __init__(self, server):
        self.server = server
        self.sigs = set(self.SIGS)
        self._thread = None
        self._mask = None

    def __enter__(self) -> "Serving":
        self._mask = signal.pthread_sigmask(signal.SIG_BLOCK, self.sigs)
        self._thread = threading.Thread(target=self.server.serve_forever, args=(self.POLL,), name="pp-http",
                                        daemon=True)
        self._thread.start()        # started under the mask, which it inherits, so no stop reaches it
        return self

    def wait(self) -> int:
        while True:
            try:
                return signal.sigwait(self.sigs)
            except InterruptedError:    # some other signal; the ones waited for are still pending
                pass

    def __exit__(self, *exc) -> bool:
        self.server.shutdown()
        self._thread.join()
        signal.pthread_sigmask(signal.SIG_SETMASK, self._mask)
        return False


def _port(v: str) -> int:
    if not v.isdigit() or int(v) > 65535:
        raise argparse.ArgumentTypeError(f"{v!r} is not a port (0..65535)")
    return int(v)


def main(argv: list[str]) -> int:
    ap = ui.parser("http", __doc__)
    ap.add_argument("paths", nargs="*", metavar="PATH", help="folders to serve, one tab each (default: from config)")
    ap.add_argument("-p", "--port", type=_port, help="port (default 8000, or [http] port; 0: any free one)")
    ap.add_argument("--bind", help="address to bind (default 127.0.0.1, or [http] bind)")
    ap.add_argument("--config-only", action="store_true", help="never fall back to the current directory")
    ap.add_argument("--no-discover", action="store_true", help="skip [[http.discover]] rules")
    ap.add_argument("-q", "--quiet", action="store_true", help="no request log")
    a = ap.parse_args(argv)
    cfg = config.load("http")
    s = Settings.from_config(cfg)
    roots_cfg, rules_cfg = tables(cfg, "roots", "path"), tables(cfg, "discover", "under")
    files = []
    if a.paths:
        fixed, rules = [], []
        for p in a.paths:
            full = paths.expand(p)
            if os.path.isfile(full):
                files.append(full)
                full = os.path.dirname(full)
            if not os.path.isdir(full):
                ui.die(f"not a directory: {p}", 2)
            fixed.append((os.path.basename(os.path.normpath(full)) or full, full))
    else:
        fixed = [(str(r.get("name") or ""), r["path"]) for r in roots_cfg]
        rules = [] if a.no_discover else rules_cfg
        if not fixed and not rules:
            if a.config_only:
                ui.die(f"no [[http.roots]] or [[http.discover]] in {config.global_path()}", 2)
            cwd = os.getcwd()
            fixed = [(os.path.basename(cwd) or cwd, cwd)]
    app = App(s, fixed, rules, quiet=a.quiet)
    roots = app.roots()
    if not roots:
        ui.warn("no tabs: the configured roots/rules matched no directory (they are re-checked every ~10 s)")
    bind = a.bind or s.bind
    want = s.port if a.port is None else a.port
    mimetypes.init()        # the type table is built here, so the threads that serve only ever read it
    try:
        server = make_server(app, bind, want)
    except OSError as e:
        ui.die(str(e))
    port = server.server_address[1]
    host = bind if bind not in ("0.0.0.0", "::", "") else socket.gethostname()
    url = f"http://{'[' + host + ']' if ':' in host else host}:{port}/"
    try:
        with Serving(server) as serving:
            print(f"pp http: serving {url}" + (f"  (port {want} is busy)" if want and port != want else "")
                  + ("" if _loopback(bind) else ui.c(f"  (bound to {bind}: reachable from the network)", "yellow")))
            for f in files:
                for name, real in roots.items():
                    if os.path.realpath(os.path.dirname(f)) == real:
                        print(f"  file: {url.rstrip('/')}{turl(name, [os.path.basename(f)])}")
                        break
            if roots:
                print(ui.table([[n, p] for n, p in roots.items()], indent="  "))
            for r in rules:
                m = r.get("marker")
                print(f"  discover: {r.get('under')} depth {r.get('depth', 1)}"
                      + (f" marker {m if isinstance(m, str) else '|'.join(m)}" if m else "")
                      + " (re-checked every ~10 s)")
            print("Ctrl-C to stop", flush=True)
            if serving.wait() == signal.SIGINT:
                print()         # the terminal has echoed ^C, so the next prompt starts on its own line
    except KeyboardInterrupt:   # a Ctrl-C before the signals were blocked, or a second one on the way out
        print()
    finally:
        server.server_close()
    return 0


# ---- page assets ----------------------------------------------------------------------------------

CSS = r"""
:root{--bg:#fff;--fg:#1f2328;--muted:#656d76;--border:#d0d7de;--accent:#0969da;--soft:#f6f8fa;--hl:#fff8c5;
--tag:#ddf4ff;--tagfg:#0550ae;color-scheme:light dark}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;--muted:#8d96a0;--border:#30363d;--accent:#4493f8;
--soft:#161b22;--hl:#3b2e00;--tag:#12263f;--tagfg:#79c0ff}}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
header{position:sticky;top:0;z-index:5;background:var(--soft);border-bottom:1px solid var(--border)}
.tabs{display:flex;gap:2px;overflow-x:auto;padding:6px 16px 0;white-space:nowrap;scrollbar-width:thin}
.tabs a,.tabs .brand{padding:6px 12px;border-radius:6px 6px 0 0;border:1px solid transparent;border-bottom:none;font-size:14px;color:var(--muted)}
.tabs .brand{font-weight:700;color:var(--fg);padding-left:0}
.tabs a.on{background:var(--bg);color:var(--fg);border-color:var(--border);font-weight:600;margin-bottom:-1px}
.tabs a:hover{text-decoration:none;color:var(--fg)}
main{padding:16px;max-width:1280px;margin:0 auto}main:has(#pp-main.wide){max-width:none}
.crumbs{font-size:16px;margin:2px 0 8px;word-break:break-all}.crumbs .sep{color:var(--muted)}
.bar{display:flex;flex-wrap:wrap;gap:8px 14px;align-items:center;margin:0 0 12px}
.bar input{font:inherit;font-size:14px;padding:6px 10px;border:1px solid var(--border);border-radius:6px;background:var(--bg);color:var(--fg);min-width:0}
#pp-filter{flex:1 1 260px}.bar .glob{flex:0 1 160px}
button{font:inherit;font-size:13px;padding:4px 10px;border:1px solid var(--border);border-radius:6px;background:var(--soft);color:var(--fg);cursor:pointer}
button.on{background:var(--tag);color:var(--tagfg)}
.meta,.note{color:var(--muted);font-size:13px}.meta a.on{color:var(--fg);font-weight:600}
.badge{font-size:12px;background:var(--tag);color:var(--tagfg);padding:1px 8px;border-radius:10px}
.scroll{overflow-x:auto}
table.list{width:100%;border-collapse:collapse;font-size:14px}
table.list th{text-align:left;font-weight:600;color:var(--muted);border-bottom:1px solid var(--border);padding:6px 8px;font-size:12px;text-transform:uppercase;letter-spacing:.03em}
table.list td{padding:5px 8px;border-bottom:1px solid var(--border);vertical-align:top}
table.list tr:hover td{background:var(--soft)}
table.list .r{text-align:right;white-space:nowrap;color:var(--muted)}td.p{word-break:break-all}
a.d{font-weight:600}
.tag{background:var(--tag);color:var(--tagfg);padding:1px 8px;border-radius:10px;font-size:12px;white-space:nowrap}
.readme{margin-top:24px;border:1px solid var(--border);border-radius:6px;padding:8px 24px 24px}
.readme-h{font-size:13px;border-bottom:1px solid var(--border);margin:0 -24px 8px;padding:0 16px 8px}
.media{max-width:100%;height:auto;border:1px solid var(--border);border-radius:6px;background:repeating-conic-gradient(#8881 0 25%,transparent 0 50%) 0 0/16px 16px}
.pdf{width:100%;height:85vh;border:1px solid var(--border);border-radius:6px}
pre.plain{white-space:pre-wrap;word-break:break-word;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;background:var(--soft);border:1px solid var(--border);border-radius:6px;padding:12px}
.code{font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;background:var(--soft);border:1px solid var(--border);border-radius:6px;overflow-x:auto;padding:8px 0}
.code .rows{display:inline-block;min-width:100%}.code.wrap .rows{display:block}
.code .row{display:flex}
.code .ln{flex:none;width:var(--lnw,4ch);box-sizing:content-box;text-align:right;padding:0 12px 0 8px;color:var(--muted);user-select:none;position:sticky;left:0;background:var(--soft)}
.code .ln:hover{color:var(--accent);text-decoration:none}
.code .lc{white-space:pre;padding-right:16px}
.code.wrap .lc{white-space:pre-wrap;word-break:break-all;flex:1 1 auto;min-width:0}
.code .row.hl,.code .row.hl .ln{background:var(--hl)}
.hljs{background:transparent!important;padding:0!important}
.md{max-width:980px;margin:0 auto;font-size:16px;line-height:1.65;overflow-wrap:break-word}
.readme .md{max-width:none}
.md>:first-child{margin-top:0}
.md h1,.md h2,.md h3,.md h4,.md h5,.md h6{margin:1.4em 0 .6em;line-height:1.25;scroll-margin-top:64px}
.md h1,.md h2{border-bottom:1px solid var(--border);padding-bottom:.3em}
.md h1{font-size:1.9em}.md h2{font-size:1.5em}.md h3{font-size:1.25em}.md h4{font-size:1.05em}
.md .hanchor{opacity:0;margin-left:.4em;font-size:.75em;font-weight:400}
.md :is(h1,h2,h3,h4,h5,h6):hover .hanchor{opacity:1}
.md code{font:.86em ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;background:var(--soft);padding:.15em .35em;border-radius:4px}
.md pre{background:var(--soft);border:1px solid var(--border);border-radius:6px;padding:12px 14px;overflow:auto;line-height:1.45}
.md pre code{background:none;padding:0;font-size:13px;border-radius:0}
.md table{border-collapse:collapse;display:block;overflow-x:auto;max-width:100%;margin:1em 0}
.md th,.md td{border:1px solid var(--border);padding:5px 12px}
.md tr:nth-child(2n){background:var(--soft)}
.md blockquote{margin:1em 0;padding:0 1em;color:var(--muted);border-left:.25em solid var(--border)}
.md img{max-width:100%}
.md hr{border:0;border-top:1px solid var(--border);margin:1.5em 0}
.md li+li{margin-top:.2em}
.md .katex-display{overflow-x:auto;overflow-y:hidden;padding:2px 0}
.md .pp-math.raw{font-family:ui-monospace,monospace;color:var(--muted)}
.md .mermaid{text-align:center;margin:1em 0}
#pp-toc{display:none}
@media (min-width:1560px){#pp-toc{display:block;position:fixed;top:96px;right:16px;width:250px;max-height:calc(100vh - 120px);overflow:auto;font-size:13px;border-left:1px solid var(--border);padding-left:12px}
#pp-toc a{display:block;color:var(--muted);padding:2px 0}#pp-toc a:hover{color:var(--fg)}#pp-toc .l3{padding-left:12px}}
@media (max-width:640px){.hide-sm{display:none}main{padding:12px 16px}.readme{padding:8px 12px 16px}.readme-h{margin:0 -12px 8px}}
"""

JS = r"""
(function () {
'use strict';
const PAGE = JSON.parse(document.getElementById('pp-page').textContent);
const CDN = PAGE.cdn;
const $ = (s, el) => (el || document).querySelector(s);
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c]));
const store = {get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
               set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }};
function readSrc(doc) { const el = (doc || document).getElementById('pp-src'); return el ? JSON.parse(el.textContent) : null; }
let SRC = readSrc();
if (typeof SRC === 'string') SRC = SRC.replace(/\r\n/g, '\n');

const loading = {};
function loadScript(src) {
  if (!loading[src]) loading[src] = new Promise((res, rej) => {
    const s = document.createElement('script'); s.src = src; s.async = false;
    s.onload = () => res(); s.onerror = () => { delete loading[src]; rej(new Error('failed: ' + src)); };
    document.head.appendChild(s);
  });
  return loading[src];
}
const soft = p => p.then(() => true, () => false);
const wantMd = PAGE.kind === 'md' || (PAGE.kind === 'dir' && PAGE.readme);
const libs = wantMd ? Promise.all([soft(loadScript(CDN.marked)), soft(loadScript(CDN.hljs)), soft(loadScript(CDN.katex))])
           : PAGE.kind === 'code' ? soft(loadScript(CDN.hljs)) : Promise.resolve();

// ---- highlight.js ----
const ALIAS = {tex: 'latex', clj: 'clojure', cljs: 'clojure', edn: 'clojure', docker: 'dockerfile', hs: 'haskell',
  ex: 'elixir', exs: 'elixir', jl: 'julia', ml: 'ocaml', proto: 'protobuf', zsh: 'bash', sh: 'bash', console: 'shell',
  toml: 'ini', yml: 'yaml', py: 'python', js: 'javascript', ts: 'typescript', rs: 'rust', golang: 'go', 'c++': 'cpp'};
const EXTRA = new Set(['clojure', 'dockerfile', 'latex', 'haskell', 'elixir', 'erlang', 'scheme', 'lisp', 'cmake',
  'protobuf', 'nix', 'groovy', 'gradle', 'vim', 'powershell', 'ocaml', 'fsharp', 'julia', 'dart', 'scala', 'elm',
  'nginx', 'apache', 'properties', 'tcl', 'fortran', 'matlab', 'prolog', 'x86asm', 'armasm', 'llvm', 'glsl', 'awk',
  'dos', 'pgsql', 'verilog', 'vhdl', 'zephir', 'crystal', 'd', 'nim', 'haxe', 'smalltalk', 'stata', 'coq', 'isabelle']);
async function hlLang(lang) {
  if (!window.hljs || !lang) return null;
  const L = ALIAS[lang] || lang;
  if (!hljs.getLanguage(L) && EXTRA.has(L)) await soft(loadScript(CDN.hljsLang.replace('{}', L)));
  return hljs.getLanguage(L) ? L : null;
}

// ---- code view ----
function splitLines(h) {
  const lines = []; const open = []; let cur = '';
  const re = /(<span[^>]*>)|(<\/span>)|(\n)|([^<\n]+|<)/g; let m;
  while ((m = re.exec(h))) {
    if (m[1]) { open.push(m[1]); cur += m[1]; }
    else if (m[2]) { open.pop(); cur += m[2]; }
    else if (m[3]) { lines.push(cur + '</span>'.repeat(open.length)); cur = open.join(''); }
    else cur += m[4];
  }
  lines.push(cur + '</span>'.repeat(open.length));
  return lines;
}
function codeRows(el, h, src) {
  const lines = splitLines(h);
  if (src.endsWith('\n') && lines.length > 1) lines.pop();
  const wrap = store.get('pp-wrap') === '1';
  const w = String(lines.length).length;
  let out = '<div class="code' + (wrap ? ' wrap' : '') + '" style="--lnw:' + w + 'ch"><div class="rows">';
  for (let i = 0; i < lines.length; i++)
    out += '<div class="row" id="L' + (i + 1) + '"><a class="ln" href="#L' + (i + 1) + '">' + (i + 1) + '</a><span class="lc">' + (lines[i] || ' ') + '</span></div>';
  el.innerHTML = out + '</div></div>';
  const b = $('#pp-wrap'); if (b) b.classList.toggle('on', wrap);
}
async function renderCode(el, src, lang, upgrade) {
  if (!upgrade) { codeRows(el, esc(src), src); return; }
  await libs;
  let h = null;
  const L = src.length <= 500000 ? await hlLang(lang) : null;
  if (L) { try { h = hljs.highlight(src, {language: L, ignoreIllegals: true}).value; } catch (e) {} }
  codeRows(el, h === null ? esc(src) : h, src);
}
function markLine(scroll) {
  document.querySelectorAll('.row.hl').forEach(r => r.classList.remove('hl'));
  const m = /^#L(\d+)$/.exec(location.hash); if (!m) return;
  const r = document.getElementById('L' + m[1]);
  if (r) { r.classList.add('hl'); if (scroll) r.scrollIntoView({block: 'center'}); }
}
window.addEventListener('hashchange', () => markLine(true));

// ---- markdown ----
const MATH = /(`+)(?!`)[\s\S]*?(?<!`)\1(?!`)|\$\$[\s\S]+?\$\$|\\\[[\s\S]+?\\\]|\\\([\s\S]+?\\\)|\\begin\{([a-zA-Z]+\*?)\}[\s\S]+?\\end\{\2\}|(?<![\\$\w])\$(?=[^\s$])(?:[^$\\\n]|\\[\s\S])+?(?<=[^\s\\])\$(?![\d$])/g;
function frontMatter(src) {
  const m = /^---\n([\s\S]*?)\n---\n/.exec(src);
  return m ? '```yaml\n' + m[1] + '\n```\n' + src.slice(m[0].length) : src;
}
function protect(src) {
  // fenced code stays verbatim; outside it, inline code stays verbatim and math becomes a placeholder marked can't mangle
  const math = [], parts = []; let buf = [], fence = null;
  for (const line of src.split('\n')) {
    if (fence) {
      buf.push(line);
      const m = /^ {0,3}(`{3,}|~{3,})\s*$/.exec(line);
      if (m && m[1][0] === fence[0] && m[1].length >= fence.length) { parts.push([true, buf.join('\n')]); buf = []; fence = null; }
    } else {
      const m = /^ {0,3}(`{3,}|~{3,})/.exec(line);
      if (m) { if (buf.length) parts.push([false, buf.join('\n')]); buf = [line]; fence = m[1]; }
      else buf.push(line);
    }
  }
  if (buf.length) parts.push([fence !== null, buf.join('\n')]);
  const text = parts.map(([code, t]) => code ? t : t.replace(MATH, (m, ticks) => {
    if (ticks) return m;
    math.push(m); return 'PPMATH' + (math.length - 1) + 'X';
  })).join('\n');
  return [text, math];
}
function renderMath(root, math) {
  root.querySelectorAll('span.pp-math').forEach(sp => {
    const raw = math[+sp.dataset.i]; let tex = raw, display = false;
    if (raw.startsWith('$$') || raw.startsWith('\\[')) { tex = raw.slice(2, -2); display = true; }
    else if (raw.startsWith('\\(')) tex = raw.slice(2, -2);
    else if (raw.startsWith('\\begin')) display = true;
    else tex = raw.slice(1, -1);
    if (window.katex) {
      try { katex.render(tex, sp, {displayMode: display, throwOnError: false, strict: 'ignore'}); return; } catch (e) {}
    }
    sp.textContent = raw; sp.classList.add('raw');
  });
}
function fixLinks(root) {
  // as on GitHub, a link starting with one / is relative to the tab (the repo), not to this server
  const tab = v => typeof PAGE.root === 'string' && /^\/(?!\/)/.test(v) ? '/t/' + encodeURIComponent(PAGE.root) + v : v;
  root.querySelectorAll('img[src],source[src],video[src],audio[src]').forEach(n => {
    try {
      const u = new URL(tab(n.getAttribute('src')), location.href);
      if (u.origin === location.origin && u.pathname.startsWith('/t/')) { u.pathname = '/raw/' + u.pathname.slice(3); n.setAttribute('src', u.href); }
    } catch (e) {}
  });
  root.querySelectorAll('a[href]').forEach(a => {
    try {
      a.setAttribute('href', tab(a.getAttribute('href')));
      if (new URL(a.getAttribute('href'), location.href).origin !== location.origin) { a.target = '_blank'; a.rel = 'noopener'; }
    } catch (e) {}
  });
}
function anchors(el) {
  const seen = {}; const hs = [...el.querySelectorAll('h1,h2,h3,h4,h5,h6')];
  for (const h of hs) {
    let id = h.textContent.trim().toLowerCase().replace(/[^\p{L}\p{N}\s_-]/gu, '').trim().replace(/\s+/g, '-') || 'section';
    if (id in seen) id += '-' + (++seen[id]); else seen[id] = 0;
    h.id = id;
    const a = document.createElement('a'); a.href = '#' + id; a.className = 'hanchor'; a.textContent = '#'; h.appendChild(a);
  }
  return hs;
}
function toc(hs) {
  const old = $('#pp-toc'); if (old) old.remove();
  const items = hs.filter(h => /^H[123]$/.test(h.tagName));
  if (PAGE.kind !== 'md' || items.length < 4) return;
  const nav = document.createElement('nav'); nav.id = 'pp-toc';
  nav.innerHTML = items.map(h => '<a class="l' + h.tagName[1] + '" href="#' + h.id + '">' + esc(h.textContent.replace(/#$/, '')) + '</a>').join('');
  document.body.appendChild(nav);
}
async function mermaidBlocks(el) {
  const blocks = el.querySelectorAll('pre > code.language-mermaid');
  if (!blocks.length || !(await soft(loadScript(CDN.mermaid)))) return;
  const nodes = [];
  blocks.forEach(c => { const d = document.createElement('div'); d.className = 'mermaid'; d.textContent = c.textContent; c.parentElement.replaceWith(d); nodes.push(d); });
  try {
    mermaid.initialize({startOnLoad: false, securityLevel: 'strict', theme: matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'default'});
    await mermaid.run({nodes});
  } catch (e) {}
}
function plain(el, src) { el.innerHTML = '<pre class="plain">' + esc(src) + '</pre>'; }
async function renderMd(el, src, upgrade) {
  if (!upgrade) { plain(el, src); return; }
  await libs;
  if (!window.marked) { plain(el, src); return; }
  const [prot, math] = protect(frontMatter(src));
  let h = marked.parse(prot, {gfm: true});
  h = h.replace(/<[^>]*>|PPMATH(\d+)X/g, (m, i) => i === undefined
    ? m.replace(/PPMATH(\d+)X/g, (_, j) => esc(math[+j])) : '<span class="pp-math" data-i="' + i + '"></span>');
  const tpl = document.createElement('template'); tpl.innerHTML = h;   // inert: relative images don't load before fixLinks
  fixLinks(tpl.content); renderMath(tpl.content, math);
  el.replaceChildren(tpl.content); el.classList.add('md');
  toc(anchors(el));
  if (window.hljs) for (const c of el.querySelectorAll('pre code')) {
    const m = /language-([\w+#-]+)/.exec(c.className);
    if (!m || m[1] === 'mermaid') continue;
    const L = await hlLang(m[1]);
    if (L) { try { c.innerHTML = hljs.highlight(c.textContent, {language: L, ignoreIllegals: true}).value; c.classList.add('hljs'); } catch (e) {} }
  }
  mermaidBlocks(el);
}

// ---- page ----
let mode = 'rendered';
async function render(upgrade) {
  const el = $('#pp-body'); if (!el || typeof SRC !== 'string') return;
  if (PAGE.kind === 'code') await renderCode(el, SRC, PAGE.lang, upgrade);
  else if (PAGE.kind === 'md' && mode === 'source') await renderCode(el, SRC, 'markdown', upgrade);
  else if (wantMd) await renderMd(el, SRC, upgrade);
}
render(false); markLine(true);
libs.then(() => render(true)).then(() => { if (location.hash) { markLine(true); const t = document.getElementById(decodeURIComponent(location.hash.slice(1))); if (t && !/^L\d+$/.test(t.id)) t.scrollIntoView(); } });

const wrapBtn = $('#pp-wrap');
if (wrapBtn) wrapBtn.addEventListener('click', () => {
  const c = $('.code'); if (!c) return; const on = !c.classList.contains('wrap');
  c.classList.toggle('wrap', on); wrapBtn.classList.toggle('on', on); store.set('pp-wrap', on ? '1' : '0');
});
const srcBtn = $('#pp-source');
if (srcBtn) srcBtn.addEventListener('click', () => {
  mode = mode === 'source' ? 'rendered' : 'source'; srcBtn.classList.toggle('on', mode === 'source');
  const t = $('#pp-toc'); if (t) t.hidden = mode === 'source';
  render(true);
});
document.addEventListener('click', e => {
  const a = e.target.closest && e.target.closest('a.ln'); if (!a) return;
  e.preventDefault(); history.replaceState(null, '', a.getAttribute('href')); markLine(false);
});

function applyFilter() {
  const f = $('#pp-filter'); if (!f) return;
  const v = f.value.trim().toLowerCase(); let n = 0;
  document.querySelectorAll('#pp-main tr[data-k]').forEach(tr => { const show = !v || tr.dataset.k.includes(v); tr.hidden = !show; if (show) n++; });
  const c = $('#pp-count'); if (c) c.textContent = n;
}
const filt = $('#pp-filter');
if (filt) { filt.addEventListener('input', applyFilter); if (!filt.value) filt.focus(); }

// ---- live reload ----
let last = {mtime: PAGE.mtime, size: PAGE.size}, updatedAt = 0, busy = false;
function tick() {
  const b = $('#pp-upd'); if (!b || !updatedAt) return;
  const s = Math.round((Date.now() - updatedAt) / 1000);
  b.textContent = 'updated ' + (s < 60 ? s + 's' : Math.round(s / 60) + 'm') + ' ago'; b.hidden = false;
}
async function swapMain() {
  const r = await fetch(location.href, {cache: 'no-store'}); if (!r.ok) return false;
  const doc = new DOMParser().parseFromString(await r.text(), 'text/html');
  const m = doc.getElementById('pp-main'); if (!m) return false;
  document.getElementById('pp-main').replaceWith(m);
  if (PAGE.kind === 'dir') { SRC = readSrc(doc); PAGE.readme = typeof SRC === 'string'; if (PAGE.readme) await renderMd($('#pp-body'), SRC, true); }
  applyFilter();
  return true;
}
async function check() {
  if (busy || document.visibilityState !== 'visible') return;
  busy = true; const y = window.scrollY;
  try {
    if (PAGE.kind === 'recent') { await swapMain(); window.scrollTo(0, y); return; }
    if (!PAGE.root && PAGE.root !== '') return;
    const r = await fetch('/_pp/stat?root=' + encodeURIComponent(PAGE.root) + '&path=' + encodeURIComponent(PAGE.path), {cache: 'no-store'});
    if (!r.ok) { const b = $('#pp-upd'); if (b) { b.textContent = 'gone'; b.hidden = false; } return; }
    const st = await r.json();
    if (st.mtime === last.mtime && st.size === last.size) return;
    last = st;
    if (PAGE.kind === 'md' || PAGE.kind === 'code') {
      if (PAGE.truncated) return;
      const fr = await fetch(PAGE.raw, {cache: 'no-store'}); if (!fr.ok) return;
      SRC = (await fr.text()).replace(/\r\n/g, '\n'); await render(true);
    } else if (PAGE.kind === 'image') { const img = $('#pp-main img'); if (img) img.src = PAGE.raw + '?v=' + st.mtime; }
    else if (PAGE.kind === 'dir') await swapMain();
    else return;
    window.scrollTo(0, y); updatedAt = Date.now(); tick();
  } catch (e) {} finally { busy = false; }
}
if (['md', 'code', 'image', 'dir', 'recent'].includes(PAGE.kind)) {
  setInterval(check, PAGE.kind === 'recent' ? 10000 : 2000);
  setInterval(tick, 1000);
  document.addEventListener('visibilitychange', check);
}
})();
"""
