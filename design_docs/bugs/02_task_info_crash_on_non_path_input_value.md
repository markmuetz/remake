# Bug 02 — `remake task-info` crashes when an input value is not a single path

**Status:** open — reported 2026-06-30.
**Affects:** `remake task-info` (CLI), local and SLURM. The pipeline run
itself is **not** affected — only introspection of the task.
**Reported by:** Mark Muetzelfeldt — hit while debugging
`wescon_tools/ctrl/remakefiles/wescon_radar_dev.py` (`regrid_camra_kepler_l1`).

## The bug

`Remake.task_info` assumes every value in `task.inputs` is a single
path-like object and wraps it in `Path(...)`:

```python
# src/remake/core/remake.py:320
inputs = (
    {k: {'path': str(v), 'exists': Path(v).exists()} for k, v in task.inputs.items()}
    if task.rule.inputs is not None
    else {}
)
```

If an `inputs=` callable returns a value that is a **collection** of
paths rather than a single path, `Path(v)` raises:

```
TypeError: argument should be a str or an os.PathLike object
where __fspath__ returns a str, not 'tuple'
```

and `task-info` aborts before printing anything useful.

### Reproducer (from the wild)

`regrid_camra_kepler_l1` declares an `inputs` builder that groups a batch
of source files under a single key:

```python
def regrid_inputs(case, radar, batch_idx):
    paths = cpmap(case, radar)[batch_idx]   # a tuple, from itertools.batched(...)
    return {
        'radar_paths': paths,               # <-- tuple of PosixPath, not one path
        'radarnet': conf.radarnet_path(case),
    }
```

`remake task-info <file> <regrid-task>` → `TypeError` as above. The rule
*runs* fine (the body iterates `for radar_path in inputs['radar_paths']`).

## Related symptom in `lint`

`remake lint` already surfaces the same shape, harmlessly but confusingly:
it reports the tuple as a single un-produced external input,
`"(PosixPath(...), PosixPath(...), ...)"`, instead of treating each element
as its own input. Same root cause: input *values* are assumed scalar.

## Question for the design

Is a non-scalar input value (a tuple/list of paths under one key)
**supported**? Two coherent positions:

1. **Supported** — then `task_info` (and `lint`, and anything else that
   walks `task.inputs`) must normalise each value to an iterable of
   tokens before stat-ing. Fix is localised: a helper that yields
   `(key, path)` pairs over scalar-or-iterable values.
2. **Not supported** — then it should be rejected loudly at decoration /
   plan time (like the empty-dict and signature checks), with a message
   pointing the author at the callable-inputs idiom (`{f'radar_{i}': p
   for i, p in enumerate(paths)}`), rather than failing only later in
   `task-info`/`lint` with a raw `TypeError`.

Either is fine; the current middle state (runs, but introspection
crashes with a bare `TypeError`) is the worst of both.
