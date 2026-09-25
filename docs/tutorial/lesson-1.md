# 1. Your first rule

In your tutorial workspace ([set-up](index.md#set-up-the-workspace)), start
the lesson:

```console
$ remake-tutorial reset 1
Ready for lesson 1: Your first rule.
```

This puts the lesson's remakefile in place and makes the raw data.

## 1.1 Look at the raw data

```console
$ ls data/raw data/raw/aberdeen
data/raw:
aberdeen
cambridge
durham
exeter

data/raw/aberdeen:
2020.csv
2021.csv
2022.csv
2023.csv
$ head data/raw/aberdeen/2020.csv
day,temp_c
1,1.4
2,2.4
3,2.5
4,NA
5,0.5
6,4.3
7,6.1
8,1.8
9,4.7
$ grep NA data/raw/aberdeen/2020.csv
4,NA
25,NA
71,NA
...
```

!!! question "Look"
    What does a row hold? What do you think a rule called `clean` will
    remove?

??? success "What's there"
    One CSV per site and year: each row is a day number and that day's
    temperature, for four weather stations over four years. About one day
    in twenty has a missing reading, written `NA`. Those are the rows
    `clean` drops.

## The remakefile

`pipeline.py` has one rule:

```python
--8<-- "src/remake/tutorial/files/1/pipeline.py"
```

- `@rule(...)` turns the function into a **rule**. `inputs` and `outputs`
  name the files it reads and writes; remake passes them in as the
  `inputs` and `outputs` arguments.
- `rmk.rules_from_current_module()` registers every rule in the file with
  `rmk`, the pipeline.
- remake creates output directories for you (`data/clean/aberdeen/`).

## 1.2 Run the pipeline

!!! question "Predict"
    How many tasks do you think remake will run?

```console
$ remake run pipeline.py
Creating sqlite3 database: .remake/remake.db
1/1: a7b8c0bc clean[]
ran 1 task(s), 0 failed in 0.0s
```

??? success "Why"
    A rule describes some work; one piece of that work is a **task**. This
    rule has one task, shown as `clean[]` (the brackets are for lesson 2).

    The first run also creates remake's database, `.remake/remake.db`,
    where it records every task it runs.

## 1.3 Look at what it made

```console
$ ls data/clean/aberdeen
2020.csv
$ wc -l data/raw/aberdeen/2020.csv data/clean/aberdeen/2020.csv
     366 data/raw/aberdeen/2020.csv
     345 data/clean/aberdeen/2020.csv
     711 total
$ ls .remake
remake.db
remake.debug.log
remake.jsonl
remake.log
```

!!! question "Look"
    How many rows did `clean` drop? What has remake put in `.remake/`?

??? success "What's there"
    The output is where `outputs` said it would be, 21 rows shorter: the
    missing days are gone.

    `.remake/` is remake's own directory, next to the remakefile:

    - `remake.db` is its record of tasks;
    - `remake.log` is a readable log of each command (try
      `cat .remake/remake.log`);
    - `remake.jsonl` and `remake.debug.log` hold the same log in more
      detail.

## 1.4 Run it again

!!! question "Predict"
    What will remake do this time?

```console
$ remake run pipeline.py
Nothing to do
```

??? success "Why"
    remake's record says `clean[]` succeeded, and nothing about the rule has
    changed since it ran, so there's nothing to do.

## 1.5 Look at what remake knows

```console
$ remake info pipeline.py
rule   tasks  up-to-date  stale  failed  pending  to run
clean  1      1           0      0       0        0
TOTAL  1      1           0      0       0        0
$ remake ls-tasks pipeline.py
a7b8c0bc clean[]
```

`info` counts each rule's tasks by state; `ls-tasks` lists the tasks, each
with its **key**: a short ID, here `a7b8c0bc`.

## 1.6 Delete the output

!!! question "Predict"
    Delete the cleaned file, then run again. Will remake run the task
    again?

```console
$ rm data/clean/aberdeen/2020.csv
$ remake run pipeline.py
Nothing to do
```

??? success "Why"
    **No.** remake decides from its record, not from the files. The task
    succeeded and nothing about it has changed, so as far as remake is
    concerned it's done. It never looks at file modification times, and by
    default it doesn't check that files exist either.

    This is deliberate. Real pipelines can have millions of files on slow
    shared filesystems, where checking them all takes minutes, and file
    times get scrambled by copies and restores. remake trusts its record
    instead.

## 1.7 Ask remake to check the files

!!! question "Predict"
    What will `--check-outputs` make remake do?

```console
$ remake run pipeline.py --check-outputs
1/1: a7b8c0bc clean[]
ran 1 task(s), 0 failed in 0.0s
```

??? success "Why"
    `--check-outputs` checks the outputs of tasks that have finished, and
    reruns the tasks whose outputs are missing.

## The mental model so far

remake keeps a record of every task it runs, in `.remake/remake.db`. A
task that succeeded doesn't run again unless something about it changes.
remake trusts its record, not the files; `--check-outputs` asks it to
look.

Next: [2. A matrix of tasks](lesson-2.md), which starts with
`remake-tutorial reset 2`.
