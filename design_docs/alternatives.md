# Alternatives

A short survey of other workflow / build tools and how they relate to remake's
niche: **large task graphs and/or many files, needing both SLURM execution and
make-style stale rebuild, in plain Python.** Fuller comparisons (with runnable
example translations and a measured scale benchmark) live in the separate
`remake_vs` repo. Two legs that pin remake down: among Python tools, native
SLURM submission *and* make-style incremental rebuild together is rare, and of
the tools that have both, remake scales best to very large graphs.

## File-DAG incremental rebuild — the direct peers

- **Snakemake** — the closest functional competitor: file-pattern DAG, native
  SLURM executor, mtime-based staleness, mature ecosystem (conda/containers,
  cloud, wrappers). Pays for it with a Python-superset DSL and a full task-DAG
  built in memory, so it scales worse to large graphs (measured ~30× slower
  planning / superlinear at 20k tasks; see `remake_vs`).
- **luigi** — class-based Python `Task`s with `requires`/`output`/`run`;
  structurally the closest to remake, with pluggable `Target`s (custom outputs)
  and dynamic deps. But no native SLURM and essentially no staleness (a task is
  done iff its output exists), so the thinnest incremental-rebuild story.
- **doit** — pure-Python "make": tasks with `file_dep`/`targets`/`uptodate`.
  Philosophically the nearest of all, but local-only — no cluster awareness, no
  matrix, smaller in scope.
- **DVC** — `dvc.yaml` pipeline stages with **content-hash** staleness plus
  git-based data/model versioning. No HPC dispatch (you run DVC inside an
  allocation); reproducibility/versioning focus rather than scale.
- **redun** — incremental via hashing/memoization of both files and values,
  expressive nested workflows, cloud (AWS Batch) executors. SLURM is not a
  core executor; the "what if remake hashed instead of mtime'd" point of
  comparison.
- **jug** — memoizes function results to a shared store; the HPC idiom is
  submitting many `jug execute` workers as a SLURM array against that store.
  Simple and science-friendly, but it doesn't generate jobs itself.
- **ploomber** — DAGs of scripts / functions / notebooks with incremental
  execution; notebook-centric. SLURM only via the `soopervisor` export layer.

## Orchestrators and dataflow engines — adjacent, not the same niche

- **Dagster** — the most conceptually aligned modern orchestrator: "software-
  defined assets" ≈ tracked outputs, **partitions** ≈ remake's matrix, with
  asset staleness, lineage and a UI. SLURM only through a Dask backend.
- **Prefect** — modern Python orchestrator (dynamic flows, scheduling, UI).
  Optimises for running flows on a schedule, not minimal rebuilds; SLURM via
  `dask-jobqueue`.
- **Airflow** — schedules DAGs of operators (cron + monitoring). Different
  niche entirely: no stale-rebuild model, SLURM only by shelling out to
  `sbatch`.
- **Parsl** — HPC-native parallel dataflow with a first-class `SlurmProvider`.
  Has app memoization + checkpointing, but that is value-memoization, not
  file-DAG staleness (it won't notice a deleted/changed output).
- **Metaflow** — step-based flows with versioning; AWS Batch / k8s first,
  SLURM via a newer add-on. Experiment/versioning emphasis over smart rebuilds.
- **Kedro** — pipeline structuring + a data catalog for DS/ML projects; gives
  structure, not incremental rebuilds or native scheduling.
- **Dask / Ray** — task-graph *execution* engines, not rebuild tools. remake
  sits on top of Dask as one of its executors (`dask-jobqueue` reaches SLURM);
  you would build any staleness logic yourself.

## Not Python (for context)

- **Nextflow** — the other bioinformatics giant: a Groovy/JVM DSL with
  excellent `-resume` content-hash caching and a first-class `slurm` executor —
  i.e. it has *both* legs, but it is not a Python package.
- **GNU Make** — the ancestor remake is named for: timestamp-based rebuilds of
  file targets via a Makefile. No Python, no matrix, no cluster awareness.
