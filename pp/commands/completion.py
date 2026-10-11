"""Print a shell completion script (zsh or bash)

    zsh, in ~/.zshrc after compinit:   source <(pp completion zsh)
    bash, in ~/.bashrc:                source <(pp completion bash)

Completes command names, then each command's verbs and flags. Candidates are asked from pp itself, so new
commands of any origin appear without regenerating; pp reads them from the help text a command already has.
Anything else (and everything after `--`) completes file names. Covers `pp` and `ppd` (the dev tree).
"""
from __future__ import annotations

from pp.lib import ui

ZSH = r'''#compdef pp ppd
_pp() {
  local -a cmds cands
  local line
  if (( CURRENT == 2 )); then
    for line in ${(f)"$(${words[1]} --commands 2>/dev/null)"}; do
      cmds+=("${${line%%$'\t'*}//:/\\:}:${line#*$'\t'}")
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
  local IFS=$'\n' cur=${COMP_WORDS[COMP_CWORD]}
  local -a cands
  cands=($("${COMP_WORDS[0]}" --complete "${COMP_WORDS[@]:1:COMP_CWORD}" 2>/dev/null))
  if [ ${#cands[@]} -gt 0 ]; then
    COMPREPLY=($(compgen -W "${cands[*]}" -- "$cur"))
  else
    compopt -o filenames 2>/dev/null
    COMPREPLY=($(compgen -f -- "$cur"))
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
