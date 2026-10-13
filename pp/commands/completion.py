"""Print a shell completion script (zsh or bash)

    zsh, in ~/.zshrc after compinit:   source <(pp completion zsh)
    bash, in ~/.bashrc:                source <(pp completion bash)

Completes command names, then each command's verbs and flags. Candidates are asked from pp itself, so new
commands of any origin appear without regenerating; pp reads them from the help text a command already has.
A candidate reaches the shell as data, and what TAB puts on the command line is that candidate quoted for the
shell, so a `$(...)` in a command's name is the name you completed and runs neither on TAB nor on the Enter
after it. Anything else (and everything after `--`) completes file names. Covers `pp` and `ppd` (the dev
tree).
"""
from __future__ import annotations

from pp.lib import ui

ZSH = r'''#compdef pp ppd
_pp() {
  local -a cmds cands
  local line
  if (( CURRENT == 2 )); then
    for line in ${(f)"$(${words[1]} --commands 2>/dev/null)"}; do
      # a name quoted for the spec _describe reads: the escape character first, then the colon it splits on,
      # so a name holding either is the name TAB inserts
      cmds+=("${${${line%%$'\t'*}//\\/\\\\}//:/\\:}:${line#*$'\t'}")
    done
    _describe -t commands 'pp command' cmds
    return
  fi
  cands=(${(f)"$(${words[1]} --complete "${(@)words[2,CURRENT]}" 2>/dev/null)"})
  if (( ${#cands} )); then
    compadd -a cands
  else
    _files
  fi
}
compdef _pp pp ppd
'''

BASH = r'''_pp() {
  local cur=${COMP_WORDS[COMP_CWORD]}
  local -a cands
  local cand quoted
  # read as data: a candidate is a name from elsewhere (a command file's, an alias's, a file's), and a word
  # list bash expands would run a `$(...)` in one of them on TAB. pp returns only what starts with $cur.
  mapfile -t cands < <("${COMP_WORDS[0]}" --complete "${COMP_WORDS[@]:1:COMP_CWORD}" 2>/dev/null)
  if [ ${#cands[@]} -gt 0 ]; then
    # quoted for the shell: what TAB inserts is left on the command line as its source, so a name holding a
    # `$(...)`, a quote, a backslash or a space is the word the user completed and nothing the shell runs
    COMPREPLY=()
    for cand in "${cands[@]}"; do printf -v quoted %q "$cand"; COMPREPLY+=("$quoted"); done
  else
    compopt -o filenames 2>/dev/null
    mapfile -t COMPREPLY < <(compgen -f -- "$cur")
  fi
}
complete -F _pp pp ppd
'''


def main(argv: list[str]) -> int:
    ap = ui.parser("completion", __doc__)
    ap.add_argument("shell", choices=["zsh", "bash"])
    a = ap.parse_args(argv)
    print(ZSH if a.shell == "zsh" else BASH, end="")
    return 0
