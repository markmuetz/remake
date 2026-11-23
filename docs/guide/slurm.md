# SLURM

remake can deploy a pipeline to a SLURM cluster, submitting each rule as a job
array and wiring up dependencies between rules with SLURM's `afterok`/
`aftercorr`.

```bash
remake run pipeline.py -E slurm
```

## Profiles and accounts

On JASMIN use the shipped profile, with your account in your user file
(`~/.config/remake/config.toml`):

```toml
profile = "jasmin"

[slurm]
account = "my_group_workspace"
```

or `remake --profile jasmin run pipeline.py -E slurm`. With that profile
active and no `slurm.account` anywhere, remake fails before submitting. Without
any profile the built-in LOTUS2 defaults are still used (with a warning) until
1.0. See [Configuration](configuration.md) for the full cascade (user file,
`remake.toml`, `Remake(config=)`, per-rule).

## Per-rule resources

Configure resources in the pipeline's `config`, per rule:

```python
rmk = Remake(config={
    'slurm': {
        'partition': 'standard',
        'mem': '16000',
        'time': '60',
    },
})
```

See `examples/ex8_zarr_slurm.py` for per-rule SLURM configuration alongside
Zarr outputs.

Keys are written verbatim as `#SBATCH --<key>=<value>`, so spell them exactly
as `sbatch`'s long options: `'cpus-per-task': 6`, not `cpus_per_task` (which
`sbatch` rejects as an unrecognized option). Any `sbatch` option works this way,
e.g. `'export': 'ALL,OMP_NUM_THREADS=1'` to set an environment variable in
every job.

Two keys are consumed by remake rather than passed through:

| Key | Effect |
|---|---|
| `array_throttle` | at most N array elements run at once (`--array=0-M%N`) — useful when many tasks write to a shared store |
| `array_threshold` | ignored (every rule is submitted as an array); accepted so older configs still load |

## The job environment

Each array element runs a bare `remake run-array-task ...` — there is no
environment activation in the generated script. The job inherits the
*submitting* shell's environment (SLURM's default `--export=ALL`), so submit
from a shell where `remake` and your pipeline's packages are importable:

```bash
conda activate myenv && remake run pipeline.py -E slurm
pixi run remake run pipeline.py -E slurm     # pixi: puts the env's bin/ on PATH
```

Symptom of getting this wrong: every element fails immediately with exit code
127 (`remake: command not found`) and nothing is recorded in the DB. If your
package is only on `PYTHONPATH` (a dev checkout), the `remake` executable is
still resolved from `PATH` — install both into the env you submit from.

Before submitting thousands of tasks, check what a job will import (see also
[pixi and `PYTHONPATH`](#pixi-and-pythonpath) below):

```bash
remake check-env pipeline.py          # job side runs locally, in a fresh subprocess
remake check-env pipeline.py --srun   # job side runs on a compute node
```

`check-env` loads the remakefile, then starts a fresh `remake` (resolved from
`PATH`, with your environment exported and the remakefile's directory as the
working directory, as a job would) and compares the two: Python executable
and version, remake's version and file, and the file and version of every
project module or package the remakefile imported. It prints `OK` lines and
one `MISMATCH` line per difference, and exits 0 if all match, 1 on any
mismatch (including `remake` missing from `PATH`), 2 on usage errors. `--json`
prints both reports and the list of mismatches. `--srun` wraps the job-side
command in `srun --ntasks=1 --time=00:05:00` (plus `partition`, `account` and
`qos` from the `slurm` config), so it needs a free slot on the cluster.

The job also reloads the remakefile, so anything it does at import time (e.g.
scanning input directories to build a matrix) happens again in every element.

### pixi and `PYTHONPATH`

With pixi, setting `PYTHONPATH` *outside* `pixi run` does not work. Pixi
activates the environment inside the command and replaces the variable, so the
remakefile can silently import a different checkout of your package from the
one you meant:

```bash
PYTHONPATH=$PWD pixi run -e myenv remake run pipeline.py -E slurm     # PYTHONPATH overridden
pixi run -e myenv env PYTHONPATH=$PWD remake run pipeline.py -E slurm # works
```

The second form sets the variable *after* activation, using `env`. The jobs
inherit it from the submitting shell as usual.

To check what was used, look at the header of `.remake/submit.sh`. From 0.9.0
remake writes there, as comments, the `sys.executable`, `sys.version`,
`PYTHONPATH` and `sys.path` of the submitting process, and the resolved file of
each top-level module the remakefile has imported that is not stdlib or in
site-packages (your own packages and dev checkouts). The same lines go to the run
log at the start of a submission. If a job imports the wrong code, compare them
with what you expected.

## Checking a submission before sending it

A dry run with the SLURM executor writes the sbatch scripts and `submit.sh`
without submitting:

```bash
remake run pipeline.py -E slurm -n -Q "..."
cat .remake/slurm/<rule>.sbatch
sbatch --test-only .remake/slurm/<rule>.sbatch
```

`sbatch --test-only` catches site policy rejections — wrong account, an
option spelling `sbatch` doesn't know, or a QOS that refuses the resources
(e.g. a QOS limited to one CPU per job) — before anything is queued.

## What gets written

On submission remake writes, under `.remake/` (or your `metadata_dir`, see
[Configuration](configuration.md)):

| Path | Contents |
|---|---|
| `jobs/<rule>.<run_seq>.json` | per-submission job spec: one header (rule, `run_seq`, planner environment) and one entry per task, in array-index order (format 2, 0.11; 0.9–0.10 files, a plain list, are still read). Immutable: each submission writes its own file and the sbatch script pins it, so replans never disturb a queued array |
| `slurm/<rule>.sbatch` | per-rule array script |
| `slurm/output/<rule>/` | per-element stdout/stderr, `<jobid>_<index>.out/.err` (0.11; `<index>.*` before) |
| `submit.sh` | master submission script (re-run it with `remake resubmit`) |
| `jobs/<rule>.<run_seq>.jobids.json` | one per submission (0.11): the array job id, `run_seq` and submit time; a later submission never overwrites it |
| `jobs/<rule>.jobids.json` | the rule's latest submission (same content) |
| `tasks/results/...` | per-task result **sidecars**, absorbed into the DB by the next remake invocation |

## Running again while a submission is in flight

Results from SLURM jobs reach the database late (a task's sidecar is read in by
the *next* remake command), so a task that is still queued or running has no
record and no output yet. A second `remake run -E slurm` therefore plans it as
pending. What stops a duplicate submission is a check **per rule**:

- remake reads the rule's last-submission file, `.remake/jobs/<rule>.jobids.json`,
  and asks `squeue` (once, for your user) whether any element of that array is
  still active. Pending, running, held, suspended and requeued elements all
  count; so does any state remake does not recognise.
- If so, the **whole rule is skipped**: no new spec file, no `sbatch`. The log
  says `<rule>: already queued (job N), skipping`.
- Rules downstream of a skipped rule are still submitted, and wait on the queued
  job id with `afterok`. Rules that are not queued are submitted as normal.
- If `squeue` fails or times out and any planned rule has a recorded
  submission, remake refuses to submit, rather than guess that the queue is
  empty. In a directory with no recorded submissions it warns and goes ahead.
- `remake resubmit` runs the same kind of check and refuses if anything it
  would submit is still queued.

So re-running while an array is going is safe for the rules in flight. It is
also coarse. What is **not** guarded:

- **Individual tasks.** Skipping is all-or-nothing per rule. Tasks that are new
  since the queued submission (for example the matrix grew) are not submitted
  until a later run, after the queued array has left the queue.
- **Submissions remake has no record of.** Only the *last* submission of each
  rule is recorded. If you deleted `.remake/jobs/`, ran from another directory,
  or submitted with a different user, the jobs are not seen.
- **The gap after a job ends.** Between a job leaving the queue and its results
  being read in, there is a short window in which its tasks look pending and
  its rule is not skipped. It is seconds long, and a duplicate run then costs
  compute but writes the same outputs. It is not a protection against two
  people running at the same moment.

If you want certainty, restrict the second run with `-Q`, e.g. `remake run
pipeline.py -E slurm -Q "rule in ['a', 'b']"`, to the rules that are not
running. `remake slurm-status` shows what is in the queue.

## Monitoring

```bash
remake slurm-status pipeline.py            # live squeue view per rule/job
remake slurm-status pipeline.py --json     # machine-readable
```

One row per rule's last submission: its job id, when it was submitted, its
task count, how many elements `squeue` shows pending (`PD`) and running
(`R`), how many of its tasks the DB records as done and failed (results of
finished elements are ingested first), how many are **lost** (not queued
and no result), and the pending reason (or `not queued` once nothing is
left in the queue), with a TOTAL row:

```
rule                     job      submitted         tasks   PD    R  done  failed  lost  reason
build_gridded_rhi_scans  6058085  2026-11-23 09:12    763  454  306     3       0     0  QOSMaxMemoryPerUser
compare_delta_z          6058090  2026-11-23 09:12     17    0    0    13       0     4  not queued
TOTAL                                                 780  454  306    16       0     4
```

## Tasks killed by SLURM

A task that SLURM kills (a node failure, the time or memory limit) or
cancels (because a task it depends on failed) never reports a result. From
0.11, remake marks every task it submits as **pending** until its result
arrives, so such a task is simply rerun by the next `remake run`. It is
never taken as up to date from an earlier success, and a partial output it
left is never adopted (`check_outputs`).

To see what happened, `remake why` and `remake task-info` ask SLURM about
the task's element in its rule's last submission:

```
$ remake why pipeline.py 314c51f7
314c51f7 gen[n=1]  314c51f7049d01bc1c904bb5b87d631ba1b65e48
- will run: yes
  - last run pending at 2026-11-23 17:02:11
    - SLURM job 6133148_11 (run 81): NODE_FAIL on host1234 after 13:40
```

`queued (PD)` and `running (R)` come from `squeue`; otherwise remake runs
`sacct` for that one element. `no accounting record` means it was
cancelled before it started (usually because an upstream task failed), or
SLURM no longer keeps the job. Nothing about the kill is stored: once SLURM
has forgotten the job, only "pending" remains.

While a submission is in flight, its tasks show as pending in `remake info`.

**Deferred rules.** The continuation job that plans deferred rules waits
for every job of the submission to succeed (`afterok`) and is cancelled
otherwise. If any task fails or is killed, the deferred rules are planned
by your next `remake run` instead.

## Logs

Each task writes a per-task log under
`.remake/tasks/log/<rule>/` (sharded by key; not a shared file — that
interleaves and corrupts under a wide array). Retrieve one with
[`remake task-log`](../cli.md) — or `task-log --path` to get its location.

Failures that happen before the task starts (an import error, a broken
environment) never reach the per-task log or the database; they land in the
job's own `.remake/slurm/output/<rule>/<array index>.err`.
`remake info pipeline.py --failed` lists every failed task on one line with its
last error line, falling back to that `.err` when the database has none.

## Querying `sacct` yourself

remake calls `squeue`, and `sacct` only for `why`/`task-info` on one
task. If you check finished jobs with
`sacct -j <jobid>`, also pass `-S <submission time>` (for example
`sacct -j 123456 -S 2026-10-14T09:00`): on LOTUS2 job ids are reused, and
without `-S` sacct can return an unrelated old job with the same id. The
submission time is `submitted` (epoch seconds) in
`.remake/jobs/<rule>.<run_seq>.jobids.json`.
