# Tutorial

A hands-on tour of remake, one small pipeline built up lesson by lesson:
daily temperature records from four weather stations, cleaned and then
summarised. Each lesson is a few minutes of running commands and, more
importantly, **predicting what remake will do before you run them**.

It works for newcomers and for people who used an earlier remake and want
to catch up. You need Python ≥ 3.10, git, and remake installed
([Installation](../installation.md)).

## The idea

remake runs Python functions that turn input files into output files, and
keeps a **record of everything it has run**. It uses that record to work
out what still needs running, so running a pipeline twice doesn't redo
the work. The lessons show, one piece at a time, exactly what that record
holds and what makes remake run something again.

## Set up the workspace

```console
$ remake-tutorial init ~/remake-tutorial
$ cd ~/remake-tutorial
```

This creates a directory with a remakefile (`pipeline.py`) and a data
generator (`make_data.py`). It is a **git repository**, with each lesson's
starting point tagged, so you can compare your work with it
(`git diff lesson-2`).

Every lesson begins with one command, which sets the workspace up for that
lesson:

```console
$ remake-tutorial reset 1
```

To go on to the next lesson, run it with the next number. If you get in a
muddle, run it again with the lesson you're on to start that lesson over.
Nothing is thrown away:
- your uncommitted changes go to `git stash`;
- your commits are kept on a `backup/…` branch;
- the old `.remake/` and `data/` are moved to `.tutorial/backup/`.

## Optional: a tutor in the next window

If you use [Claude Code](https://claude.com/claude-code), open a second
terminal in the same directory, start `claude`, and type `/remake-tutor`
**before** you run `remake-tutorial reset 1`. The tutor watches each
command you run in your first terminal (through remake's event log,
`.remake/remake.jsonl`). It leads you through the lesson one step at a
time:
1. it tells you what to try next;
2. it asks what you expect to happen, which you answer in its window;
3. once the command has run, it explains what remake did.

It follows along if you go your own way and experiment. It never edits
your files or runs anything that changes them.

The lessons work just as well without it: each page has everything you
need.

## Lessons

1. [Your first rule](lesson-1.md): run, run again, and what happens when
   an output file disappears.
2. [A matrix of tasks](lesson-2.md): one rule, many tasks; selecting and
   forcing tasks; growing the pipeline.

More lessons follow: chaining rules, change detection, failures, existing
outputs, scaling up.
