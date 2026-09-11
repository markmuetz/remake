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
