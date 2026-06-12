# Discussion

High-level ideas to return to. Not commitments — each needs its own
design discussion before any work starts.

- **Terminal output** — richer progress display for `remake run` (live
  task counts, per-rule progress, colour); what the right level of
  polish is for a batch tool.
- **SLURM monitor** — live view of queued/running/completed cluster jobs
  (remake2 had `monitor.py`); how it relates to `info` and the jobid
  sidecar files.
- **Web interface** — out of scope per the design doc, but the SQLite DB
  is queryable by external tools; revisit whether a thin read-only
  viewer is worth it.
- **Dask integration** — re-add a dask executor against the new Executor
  ABC (old one deleted as incomplete).
- **SLURM job ids written to file** — already in the SLURM design
  (`.remake/jobs/<rule>.jobids.json` sidecars); confirm the format
  serves the monitor and resubmission use cases when implementing.
- **CLI interface** — assorted behaviours to decide:
  - `remake` on missing file: sensible default when no remakefile is
    given (search cwd? `.remake/config` default, as remake2 had?).
  - "remake only if not run": a mode/flag that runs only never-run
    tasks, ignoring code/uses changes.
- **Grab code version** — record the pipeline repo's git hash/status in
  task metadata at run time (remake2's `get_git_info` did this; dropped
  in the trim).
- **Get python module state** — record the environment alongside runs:
  conda/pip/uv/pixi lockfile or `pip freeze` snapshot; how much is
  remake's job vs the user's.
- **Integrate RO-Crate** — package outputs + metadata + provenance as an
  RO-Crate for publication/archival; natural successor to remake2's
  archive feature.
- **`.remake` folder next to output artefacts** — metadata colocated
  with the data it describes rather than the cwd the pipeline ran from;
  interacts with shared stores and multiple pipelines per data tree.
- **Plugins** — entry-point-based discovery of third-party executors,
  tokens and metadata backends (the dotted-path executor injection is a
  first step).
- **.remake** — currently there is one single .remake folder for all
  files within a directory, with one single remake.db. Is this correct?
- **Per-task logging under SLURM arrays** — the shared `.remake/remake.log`
  corrupts under concurrent array-job writers (see todos.md, Smaller
  debts, found on JASMIN 2026-06-12). Likely fix is per-task log files,
  alongside the existing `.remake/slurm/output/<rule>/%a.out`/`.err` —
  ties into the `.remake` layout question above.
- **configuration** - there should be three levels of config: 
  `~/.remake/config.yaml`, `<project>/.remake/config.yaml`, and potentially
  within a remakefile, with cascade from general to specific.
