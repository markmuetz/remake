#!/usr/bin/env bash
# PreToolUse hook (Bash matcher): block `git commit` when the staged diff
# touches many Python lines, unless an adversarial review ack marker exists.
# The marker is consumed on use so each large change needs a fresh
# /code-review. Non-Python changes (docs, config, data) don't count.
#
# Only on main and maint/** (release lanes): feature branches are reviewed
# once, when they merge into main (MM, 2026-09-25). A merge there must be
# `git merge --no-commit` + `git commit`, so the whole branch's diff is
# staged and checked here; a merge that would commit directly is blocked.
# `--ff-only` merges (catching up with the remote) pass: already reviewed.
set -u

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""')

case "$cmd" in
  *"git commit"*|*"git merge"*) ;;
  *) exit 0 ;;
esac

repo=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
branch=$(git -C "$repo" symbolic-ref --quiet --short HEAD 2>/dev/null || echo DETACHED)
case "$branch" in
  main|maint/*|DETACHED) ;;
  *) exit 0 ;;  # a feature branch: reviewed at merge into main
esac

case "$cmd" in
  *"git merge"*)
    case "$cmd" in
      *--no-commit*|*--ff-only*|*--abort*|*--continue*) exit 0 ;;
    esac
    echo "On $branch, merge with 'git merge --no-ff --no-commit <branch>' and then 'git commit', so the review hook sees the branch's whole diff (a direct merge commit bypasses it)." >&2
    exit 2 ;;
esac
lines=$(git -C "$repo" diff --cached --numstat -- '*.py' | awk '{n += $1 + $2} END {print n + 0}')
threshold=${REVIEW_ACK_THRESHOLD:-200}
ack="$repo/.claude/review-ack"

if [ "$lines" -le "$threshold" ]; then
  exit 0
fi

if [ -f "$ack" ]; then
  rm -f "$ack"
  exit 0
fi

echo "Staged diff is $lines changed Python lines (threshold $threshold). Adversarial review is required for large changes: run /code-review at medium+ effort in a fresh context, address the findings, then 'touch $ack' and retry the commit. Do not split the commit to dodge the threshold." >&2
exit 2
