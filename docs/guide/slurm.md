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
