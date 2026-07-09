#!/usr/bin/env bash
# PreToolUse hook (Bash matcher): block `git commit` when the staged diff is
# large, unless an adversarial review ack marker exists. The marker is
# consumed on use so each large change needs a fresh /code-review.
set -u

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""')

case "$cmd" in
  *"git commit"*) ;;
  *) exit 0 ;;
esac

repo=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
lines=$(git -C "$repo" diff --cached --numstat | awk '{n += $1 + $2} END {print n + 0}')
threshold=${REVIEW_ACK_THRESHOLD:-200}
ack="$repo/.claude/review-ack"

if [ "$lines" -le "$threshold" ]; then
  exit 0
fi

if [ -f "$ack" ]; then
  rm -f "$ack"
  exit 0
fi

echo "Staged diff is $lines changed lines (threshold $threshold). Adversarial review is required for large changes: run /code-review at medium+ effort in a fresh context, address the findings, then 'touch $ack' and retry the commit. Do not split the commit to dodge the threshold." >&2
exit 2
