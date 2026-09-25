# 1. Your first rule

Start in your tutorial workspace ([set-up](index.md#set-up-the-workspace)).

## 1.1 Make the raw data

```console
$ python make_data.py
wrote 16 files under data/raw/
```

A year of daily temperatures for each of four stations and four years, in
`data/raw/<site>/<year>.csv`. About one day in twenty has a missing
reading, written as `NA`.

## The remakefile

`pipeline.py` has one rule:

```python
--8<-- "src/remake/tutorial/files/1/pipeline.py"
```

- `@rule(...)` turns the function into a rule. `inputs` and `outputs` name
  the files it reads and writes; remake passes them in as the `inputs` and
  `outputs` arguments.
- `rmk.rules_from_current_module()` registers every rule in the file with
  `rmk`, the pipeline.
- remake creates output directories for you (`data/clean/aberdeen/`).

## 1.2 Run the pipeline

!!! question "Predict"
    How many tasks will remake run?

```console
$ remake run pipeline.py
Creating sqlite3 database: .remake/remake.db
1/1: a7b8c0bc clean[]
ran 1 task(s), 0 failed in 0.0s
```

??? success "Why"
    One rule without a matrix is **one task**: `clean[]` (the `[]` holds
    its matrix values, and there are none yet). `a7b8c0bc` is its **task
    key**, a stable ID remake uses to find it. The first run also creates
    the database, `.remake/remake.db`.

## 1.3 Run it again

!!! question "Predict"
    What will remake do this time?

```console
$ remake run pipeline.py
Nothing to do
```

??? success "Why"
    The database says `clean[]` succeeded, and the rule's code, inputs and
    outputs are exactly as they were when it ran. None of the three reasons
    to rerun ([the mental model](index.md#the-mental-model)) applies.

## 1.4 Look at what remake knows

```console
$ remake info pipeline.py
rule   tasks  up-to-date  stale  failed  pending  to run
clean  1      1           0      0       0        0
TOTAL  1      1           0      0       0        0
$ remake ls-tasks pipeline.py
a7b8c0bc clean[]
```

`info` counts each rule's tasks by state; `ls-tasks` lists the tasks
themselves.

## 1.5 Delete the output

!!! question "Predict"
    Delete the cleaned file, then run again. Will remake rerun the task?

```console
$ rm data/clean/aberdeen/2020.csv
$ remake run pipeline.py
Nothing to do
```

??? success "Why"
    **No.** remake decides from its database, not from the filesystem. The
    task succeeded and nothing about it has changed, so as far as remake is
    concerned it's done.

    This is deliberate. Pipelines on clusters have millions of files on
    slow shared filesystems, where checking them all would take minutes,
    and file times get scrambled by copies and restores. remake trusts
    its record instead. It's also why remake never reruns something just
    because a file was touched.

## 1.6 Ask remake to check the files

```console
$ remake run pipeline.py --check-outputs
1/1: a7b8c0bc clean[]
ran 1 task(s), 0 failed in 0.0s
```

When you do want the files checked, `--check-outputs` verifies that each
completed task's outputs exist and reruns the tasks whose outputs are
missing.

## What you learnt

- A rule with no matrix is one task; remake records every run in
  `.remake/remake.db`.
- A task reruns only when it never succeeded, its definition changed, or
  something upstream it reads from reran.
- remake trusts its database, not the files. `--check-outputs` asks it to
  look.

Next: [2. A matrix of tasks](lesson-2.md).
