# 2. A matrix of tasks

One rule, one file isn't much of a pipeline. In this lesson one rule
cleans every site and every year.

If you're starting here, run `remake-tutorial reset 2` first.

## 2.1 One task per site and year

Change `pipeline.py` to:

```python
--8<-- "src/remake/tutorial/files/2/pipeline.py"
```

What changed:

- `matrix={'site': SITES, 'year': YEARS}`: one task for every combination,
  3 sites × 4 years;
- `{site}` and `{year}` in the paths are filled in per task;
- the function takes `site` and `year` as arguments. They must match the
  matrix's keys, and remake checks this when the file loads.

(`durham` is left out on purpose; it comes back in 2.4.)

!!! question "Predict"
    How many tasks will run? And `clean` for aberdeen/2020 already ran in
    lesson 1. Will it run again?

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
    **and its matrix values**: `clean[site=aberdeen, year=2020]` is a new
    task, not lesson 1's `clean[]`. It has a different key, and there's no
    record of it having run. (`clean[]` is no longer part of the pipeline,
    so `info` stops counting it.)

## 2.2 List and filter tasks

```console
$ remake ls-tasks pipeline.py -Q "site == 'exeter'"
70ccbf5a clean[site=exeter, year=2020]
bdf3b82f clean[site=exeter, year=2021]
3efb99b8 clean[site=exeter, year=2022]
64c9af58 clean[site=exeter, year=2023]
```

`-Q` (or `--query`) takes a Python expression over the matrix values, plus
`rule` for the rule's name: `"year >= 2022"`,
`"site in ['aberdeen', 'exeter']"`, `"rule == 'clean' and year == 2020"`.
Most commands accept it.

## 2.3 Force a subset

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

## 2.4 Add a site

Add `'durham'` to `SITES`:

```python
SITES = ['aberdeen', 'cambridge', 'durham', 'exeter']
```

!!! question "Predict"
    How many tasks will run now?

Check your prediction without running anything: `-n` (`--dry-run`) shows
the plan.

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
```

??? success "Why"
    Only durham's 4 tasks, which are new. The other 12 are exactly as
    before: same rule code, same inputs and outputs spec, same matrix
    values. Growing the matrix adds tasks; it doesn't disturb existing
    ones.

## What you learnt

- `matrix=` makes one task per combination of values; `{key}` in paths is
  filled in per task.
- A task's identity is its rule plus its matrix values.
- `-Q` selects tasks, `--force` reruns them regardless, `-n` shows the
  plan without running it.
- Adding matrix values adds tasks, and only those run.
