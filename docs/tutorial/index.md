# Tutorial

A hands-on tour of remake, one small pipeline built up lesson by lesson:
daily temperature records from four weather stations, cleaned and then
summarised. Each lesson is a few minutes of typing commands and, more
importantly, **predicting what remake will do before you run them**.

It works for newcomers and for people who used an earlier remake and want
to catch up. You need Python ≥ 3.10, git, and remake installed
([Installation](../installation.md)).

## The mental model

Everything in the lessons comes back to one idea. remake keeps a database of
every task it has run (`.remake/remake.db`, next to your remakefile), and a
task runs again only when:

1. **it has never succeeded**: no record, or its last run failed;
2. **its definition changed**: the rule's code, its `uses=` values, or its
   inputs/outputs spec;
3. **an upstream task it reads from reruns** (or ran more recently than it).

That's the whole list. In particular remake **never looks at file
modification times**, and by default doesn't look at files at all. If that
sounds surprising, lesson 1 shows why it's useful.

## Set up the workspace

```console
$ remake-tutorial init ~/remake-tutorial
$ cd ~/remake-tutorial
```

This creates a directory with a starting remakefile (`pipeline.py`) and a
data generator (`make_data.py`). It is a **git repository**: each lesson's
starting point is tagged, so you can always compare your work with it
(`git diff lesson-2`). If you get in a muddle,
`remake-tutorial reset <lesson>` puts everything back to the start of
that lesson. Your own changes are kept in `git stash`, never thrown away.

## Optional: a tutor in the next window

If you use [Claude Code](https://claude.com/claude-code), open a second
terminal in the same directory, start `claude`, and type `/remake-tutor`.
The tutor watches each `remake` command you run in your first terminal
(through remake's own event log, `.remake/remake.jsonl`) and replies in its
window:
- it asks what you expect before a step;
- it tells you whether remake did what the lesson intended;
- it explains when you're surprised.

It follows you if you go your own way and experiment. It only reads: it never
edits your files or runs anything that changes them.

The lessons work just as well without it.

## Lessons

1. [Your first rule](lesson-1.md): run, rerun, and why a deleted file
   doesn't trigger a rerun.
2. [A matrix of tasks](lesson-2.md): one rule, many tasks; selecting and
   forcing tasks; growing the matrix.

More lessons follow: chaining rules, change detection, failures, existing
outputs, scaling up.
