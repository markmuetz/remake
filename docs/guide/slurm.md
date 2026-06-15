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

See `examples/ex4_zarr_slurm.py` for per-rule SLURM configuration alongside
Zarr outputs.

## What gets written

On submission remake writes, under `.remake/`:

| Path | Contents |
|---|---|
| `slurm/<rule>.sbatch` | per-rule array script |
| `slurm/output/<rule>/` | per-element stdout/stderr |
| `jobs/<rule>.jobids.json` | submitted job ids (sidecar) |
| `tasks/results/...` | per-task result **sidecars** |

## Monitoring

```bash
remake slurm-status pipeline.py            # live squeue view per rule/job
remake slurm-status pipeline.py --json     # machine-readable
```

This reads the recorded job ids and queries `squeue` for current states and
reasons.

## Logs

Each task writes a per-task log at
`.remake/tasks/log/<rule>/<key>.log` (not a shared file — that interleaves and
corrupts under a wide array). Retrieve one with
[`remake task-log`](../cli.md).
