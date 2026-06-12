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
- **CLI interface** — assorted behaviours to decide:
  - `remake` on missing file: sensible default when no remakefile is
    given (search cwd? `.remake/config` default, as remake2 had?).
  - ~~"only if not run"~~ done: `run --ignore-code-changes/-I` — rerun
    only what has never *succeeded* (failed reruns; upstream propagation
    stays on so fan-ins pick up newly-run elements).
  - ~~record-existing-outputs command~~ done, generalised to
    `set-state -Q <query> (--success [--check-outputs] | --pending)`;
    migration adoption = `set-state file -Q True --success
    --check-outputs`.
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
  Interacts with the "`.remake` next to artefacts" item above.
- **configuration** - there should be three levels of config:
  `~/.remake/config.yaml`, `<project>/.remake/config.yaml`, and potentially
  within a remakefile, with cascade from general to specific.
- **logging** - Perhaps the rule decorator could have a logger=True line, that
  passes in a loguru logger to the function? Or just say that the user can
  set up a loguru logger then use that as using `uses`.
- **intra-rule task dependency** - Should this be possible? A sequentially
  defined rule where each task depends on the one before? Challenges the
  no-task-DAG principle that planning memory, SLURM array eligibility and
  failure-skip propagation all lean on — needs a real design discussion.

## Graduated (designed and implemented; kept for the record)

- **SLURM job ids written to file** — `.remake/jobs/<rule>.jobids.json`
  sidecars; consumed by `slurm-status`, `task-info`, resubmission and
  already-queued detection.
- **Per-task logging under SLURM arrays** — per-task key-named log files;
  see design_docs/per_task_logging.md.
- **Task inspection/validation** — `remake lint` (near-miss input wiring,
  missing depends_on).
