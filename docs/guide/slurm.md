# SLURM

remake can deploy a pipeline to a SLURM cluster, submitting each rule as a job
array and wiring up dependencies between rules with SLURM's `afterok`/
`aftercorr`.

```bash
remake run pipeline.py -E slurm
```

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

On submission remake writes, under `.remake/`:

| Path | Contents |
|---|---|
| `jobs/<rule>.<run_seq>.json` | per-submission job spec (one entry per task). Immutable: each submission writes its own file and the sbatch script pins it, so replans never disturb a queued array |
| `slurm/<rule>.sbatch` | per-rule array script |
| `slurm/output/<rule>/` | per-element stdout/stderr |
| `submit.sh` | master submission script (re-run it with `remake resubmit`) |
| `jobs/<rule>.jobids.json` | submitted job ids + the submission's `run_seq` (written at submission) |
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

This reads the recorded job ids and queries `squeue` for current states and
reasons.

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

remake itself only calls `squeue`. If you check finished jobs with
`sacct -j <jobid>`, also pass `-S <submission time>` (for example
`sacct -j 123456 -S 2026-10-14T09:00`): on LOTUS2 job ids are reused, and
without `-S` sacct can return an unrelated old job with the same id. The
submission time is the modification time of `.remake/jobs/<rule>.jobids.json`.
