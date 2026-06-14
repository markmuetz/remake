# remake

Remake is a smart Python build tool, similar to `make`. It makes it easy to
build pipelines with complex dependencies and deploy them to high-performance
computing systems. It is particularly suited to scientific workflows, because
it reliably recreates any set of output files by running only those tasks that
are actually necessary.

## Why remake

- **Smart rerunning** — remake tracks inputs, outputs and the *code* of each
  rule. Change a function or a constant it depends on, and only the affected
  tasks rerun.
- **Matrix pipelines** — express a whole grid of tasks (sites × years × …)
  declaratively, with fan-in and fan-out across the matrix.
- **HPC-native** — run the same pipeline locally (single process or
  multiprocess) or on a SLURM cluster, with per-rule resource configuration.
- **Reproducible** — the metadata DB knows what produced every output, so you
  can always rebuild exactly what is missing or stale.

## A taste

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


rmk.rules_from_current_module()
```

```bash
remake run pipeline.py     # runs what is needed
remake run pipeline.py     # nothing to do — outputs are up to date
remake info pipeline.py    # status of every task
```

Head to [Getting started](getting-started.md) to build a real pipeline, or
[Installation](installation.md) to set up.

!!! note "Status"
    remake3 is a clean-break redesign (`0.8.0a0`, alpha). The
    [design notes](https://github.com/markmuetz/remake/tree/remake3/design_docs)
    record the rationale behind the current implementation.
