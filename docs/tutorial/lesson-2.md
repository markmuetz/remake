# 2. A matrix of tasks

One rule, one file isn't much of a pipeline. In this lesson one rule
cleans every site and every year.

```console
$ remake-tutorial reset 2
Ready for lesson 2: A matrix of tasks.
```

This picks up where lesson 1 left off: same remakefile, same data, same
record of what has run.

## 2.1 One task per site and year

Change `pipeline.py` to:

```python
--8<-- "src/remake/tutorial/files/2/pipeline.py"
```

What changed:

- `matrix={'site': SITES, 'year': YEARS}`: a **matrix**, one task for
  every combination of its values, 3 sites × 4 years;
- `{site}` and `{year}` in the paths are filled in per task;
- the function takes `site` and `year` as arguments. They must match the
  matrix's keys, and remake checks this when it loads the file.

(`durham` is left out on purpose; it comes back in 2.5.)

!!! question "Predict"
    How many tasks will run? And aberdeen/2020 already ran in lesson 1.
    Will it run again?

```console
$ remake run pipeline.py
 1/12: dda3a5b7 clean[site=aberdeen, year=2020]
 2/12: 834ce043 clean[site=aberdeen, year=2021]
 ...
12/12: 64c9af58 clean[site=exeter, year=2023]
ran 12 task(s), 0 failed in 0.0s
```

??? success "Why"
    All 12 run, including aberdeen/2020. A task is identified by its rule
    **and its matrix values**, which is what the brackets show:
    `clean[site=aberdeen, year=2020]` is a new task, not lesson 1's
    `clean[]`. It has a different key, and there's no record of it
    having run.

## 2.2 Look at the outputs

```console
$ find data/clean -name "*.csv" | sort
data/clean/aberdeen/2020.csv
data/clean/aberdeen/2021.csv
...
data/clean/exeter/2023.csv
```

!!! question "Look"
    How many cleaned files are there now? Where did each one's path come
    from?

??? success "What's there"
    Twelve: one per task. Each task fills `{site}` and `{year}` into the
    `outputs` template with its own values, so
    `clean[site=exeter, year=2023]` writes `data/clean/exeter/2023.csv`.

## 2.3 List and filter tasks

```console
$ remake ls-tasks pipeline.py -Q "site == 'exeter'"
70ccbf5a clean[site=exeter, year=2020]
bdf3b82f clean[site=exeter, year=2021]
3efb99b8 clean[site=exeter, year=2022]
64c9af58 clean[site=exeter, year=2023]
```

`-Q` (or `--query`) takes a Python expression over the matrix values:
`"year >= 2022"`, `"site in ['aberdeen', 'exeter']"`. Most commands
accept it.

## 2.4 Force a subset

!!! question "Predict"
    Everything is up to date. How many tasks will this run?
    ```console
    $ remake run pipeline.py -Q "year == 2023" --force
    ```

```console
$ remake run pipeline.py -Q "year == 2023" --force
1/3: 1cc31bf0 clean[site=aberdeen, year=2023]
2/3: a3a4700a clean[site=cambridge, year=2023]
3/3: 64c9af58 clean[site=exeter, year=2023]
ran 3 task(s), 0 failed in 0.0s
```

??? success "Why"
    `--force` reruns exactly the tasks the query matches, up to date or
    not: one per site for 2023.

## 2.5 Add a site

Add `'durham'` to `SITES`:

```python
SITES = ['aberdeen', 'cambridge', 'durham', 'exeter']
```

!!! question "Predict"
    How many tasks will run now?

Check your prediction without running anything: `-n` (`--dry-run`) shows
the plan. Then run it.

```console
$ remake run pipeline.py -n
a2bb31b7 clean[site=durham, year=2020]
6ecc837c clean[site=durham, year=2021]
16272ad1 clean[site=durham, year=2022]
0789db17 clean[site=durham, year=2023]
4 task(s) would run
$ remake run pipeline.py
...
ran 4 task(s), 0 failed in 0.0s
$ ls data/clean
aberdeen
cambridge
durham
exeter
```

??? success "Why"
    Only durham's 4 tasks, which are new. The other 12 are exactly as
    before. Growing the matrix adds tasks; it doesn't disturb the existing
    ones.

## The mental model so far

remake keeps a record of every task it runs. A rule with a matrix makes
many tasks, and a task is its rule plus its matrix values. remake runs
the tasks that have never succeeded; growing the matrix adds tasks, and
only those run. It trusts its record, not the files (`--check-outputs`
asks it to look). `-Q` selects tasks, `--force` reruns them, and `-n` shows
the plan.
