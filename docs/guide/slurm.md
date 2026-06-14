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

## Sidecar / ingest

Array tasks do **not** open the metadata DB. At the concurrency a wide cluster
job reaches, many processes contending on one SQLite file livelocks. Instead
each task writes its result to a sidecar file under `.remake/tasks/results/`,
and the next `plan`/`run`/`info` batch-ingests them with a single writer.

The practical consequence: after a cluster run, run any read command (e.g.
`remake info`) to ingest results before relying on the recorded state.

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
