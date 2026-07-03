# Getting started

This walkthrough builds a small two-rule pipeline and shows remake's smart
rerunning. The complete examples live in
[`examples/`](https://github.com/markmuetz/remake/tree/main/examples).

## 1. Write a remakefile

A *remakefile* is an ordinary Python module that defines `@rule`s and
registers them on a `Remake` object. Save this as `pipeline.py`:

```python
from pathlib import Path
from remake import Remake, rule

rmk = Remake()


@rule(
    inputs  = {'raw': 'data/raw/measurements.csv'},
    outputs = {'clean': 'data/processed/measurements.csv'},
)
def preprocess(inputs, outputs):
    lines = Path(inputs['raw']).read_text().splitlines()
    cleaned = [l for l in lines if not l.startswith('#')]
    Path(outputs['clean']).write_text('\n'.join(cleaned))


@rule(
    inputs     = preprocess.outputs,
    outputs    = {'summary': 'data/results/summary.txt'},
    depends_on = [preprocess],
)
def summarise(inputs, outputs):
    lines = Path(inputs['clean']).read_text().splitlines()
    Path(outputs['summary']).write_text(f'rows: {len(lines)}\n')


rmk.rules_from_current_module()
```

The DAG is `preprocess → summarise`. Note how `summarise` reuses
`preprocess.outputs` as its inputs and declares `depends_on = [preprocess]` —
that is the dependency edge.

## 2. Run it

```bash
mkdir -p data/raw
printf '# comment\n1,2\n3,4\n' > data/raw/measurements.csv

remake run pipeline.py
```

remake plans the DAG, sees nothing is built, and runs both tasks.

## 3. See smart rerunning

Run it again:

```bash
remake run pipeline.py     # "nothing to do" — outputs are up to date
```

Now change the *code* of `preprocess` (e.g. also strip blank lines) and rerun:

```bash
remake run pipeline.py     # preprocess reruns, and so does summarise
```

remake hashes each rule's code and the values it depends on, so a logic change
reruns exactly the affected tasks — not a timestamp in sight.

## 4. Inspect status

```bash
remake info pipeline.py          # per-rule task counts and states
remake ls-tasks pipeline.py      # list tasks
remake why pipeline.py <task>    # why a task will (or won't) rerun
```

## Next steps

- [Rules and tasks](guide/rules-and-tasks.md) — matrices, fan-in/out, `uses`
- [Running pipelines](guide/running.md) — executors, parallelism, exit codes
- [SLURM](guide/slurm.md) — deploying to a cluster
- [Debugging](guide/debugging.md) — failures, logs, post-mortem
