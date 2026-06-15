# Discussion

High-level ideas to return to. Not commitments — each needs its own
design discussion before any work starts.

- **Terminal output** — richer progress display for `remake run` (live
  task counts, per-rule progress, colour); what the right level of
  polish is for a batch tool.
- **SLURM monitor** — live view of queued/running/completed cluster jobs
  (remake2 had `monitor.py`); how it relates to `info` and the jobid
  sidecar files.
  - **Completed-job resource audit (`sacct`).** The live `slurm-status`
    only sees the queue (`squeue`); once a job finishes it's gone. The
    post-mortem counterpart reads the accounting DB via `sacct` to report,
    per rule (and per array element), how much was actually used vs
    requested — and advise on right-sizing. The wiring already exists: job
    ids are persisted at submission (`.remake/jobs/<rule>.jobids.json`) and
    `slurm-status` keys off them; this is the same pattern with a
    `sacct_snapshot()` next to `squeue_snapshot()` in `slurm_executor.py`.
    Likely shape: `remake slurm-resources` (or a `--completed`/`--resources`
    flag on `slurm-status`), `--json` for scripting, human view giving
    over/under-allocation advice.
    - Fields: `MaxRSS` (peak RAM used) vs `ReqMem`; `Elapsed`/`TotalCPU`
      vs `Timelimit`; `State`/`ExitCode` to catch `OUT_OF_MEMORY` /
      `TIMEOUT`; `AllocCPUS` vs `TotalCPU` for CPU efficiency.
    - Suggestion logic: peak `MaxRSS` across elements × safety factor
      (~1.3), rounded up, for `--mem`; similarly for `--time`. Flag OOM /
      timeout as *under*-allocated so it advises in both directions, not
      just "you wasted RAM".
    - Gotchas (the classic `sacct`-parsing traps): `MaxRSS` lives on the
      `.batch`/`.extern` step sub-rows, not the parent row (walk steps,
      take the max); array elements report individually (`<jobid>_<n>`) so
      report the distribution (max/median) across them — the heavy element
      sets the `--mem` everyone pays for; `sacct`/accounting may be
      disabled or jobs may have aged out of the DB (degrade gracefully like
      `squeue_snapshot`); `ReqMem` units/semantics shifted between Slurm
      versions (`16Gn` vs `16G`, per-node vs per-cpu) — normalise output
      with `--units=M`, parse the requested side carefully.
    - **Perfect fit for a Claude skill.** The mechanical part (one `sacct`
      call per jobid, `-P` parsing, max-across-steps) belongs in remake;
      but turning the numbers into *advice* — "this rule used 11% of its
      RAM ask across 128 elements, drop `--mem` to 3G; this one OOM'd on 2
      elements, bump it" — is exactly the judgement a skill is good at. A
      `--json` resource dump gives the skill clean input; the skill reasons
      over the distribution, weighs headroom against OOM risk, and proposes
      concrete config edits to the remakefile. Sits naturally alongside the
      existing `remake` skill (operate/debug/author pipelines).
- **Web interface** — out of scope per the design doc, but the SQLite DB
  is queryable by external tools; revisit whether a thin read-only
  viewer is worth it.
- **Dask integration — long grass.** A basic dask executor exists
  (2026-06-12: spec-based like multiproc/SLURM, LocalCluster or a
  configured scheduler address) and that is where it stops: dask is a
  nightmare on JASMIN (MM), which is remake's primary target, so
  dask-*native* integration (inter-rule futures instead of per-rule
  barriers, dask-jobqueue, long-lived-worker staleness) is deliberately
  parked. Do not pick this up without a concrete user need on a platform
  where dask actually behaves.
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
  - **Rerun reasons (remake2's `info --reasons`).** remake3 has a dedicated
    `why` verb, so split along that seam rather than overloading `info`:
    - ~~*Per-task detail → multi-task `why`.*~~ **Done 2026-06-15.** `why -Q
      <query>` explains every match (block per task + summary); bare `why`
      explains the runnable set; `why <key>` is the unchanged N=1 case.
      `Remake.explain_tasks(tasks=None)` plans *once* and passes the runnable
      list into the module-level `explain_task(..., runnable=...)` per task,
      so it's plan-cost not N*plan; scope is bounded by the query or the
      runnable default (never silently stats the whole matrix). Dissolved
      the `RemakeError`-as-traceback nit for the >1 case (no longer an
      error). Tests in test_cli.py.
    - ~~*Aggregate rollup → `info --reasons`.*~~ **Done 2026-06-15.** `info
      --reasons` adds a per-rule tally of would-run reason *categories*
      (e.g. `stage1: 4 last-run-failed`), reusing the single `plan()` info
      already does (plan-cost, not N*plan). Categories come from the planner
      itself: `explain_task` now returns `Reason(category, message)` tuples
      (`why` prints the message, this reads the category), so the buckets
      are authoritative, not string-matched. A task can contribute several
      categories, so counts may exceed the to-run total (documented).
      `--json` puts a `reasons` dict on each rule row. `ls-tasks` stays pure
      selection.
  - ~~**Dedup `info -F` failures (remake2's unwieldy `info -F`).**~~ **Done
    2026-06-15.** `-F` now groups failed tasks by a message-*insensitive*
    signature (exception type + the traceback's frame locations) — so
    `ValueError ... i=0/1/2/...` collapse into one group "ValueError at
    stage1.py:9 ×N" with one representative traceback (real message intact),
    its log, and `+N more: <tasks>`. `--all-failures` keeps the exhaustive
    per-task dump; `--json` emits grouped (or the full list under
    `--all-failures`). `_traceback_signature`/`_group_failures` in
    remake_cmd.py; tests in test_cli.py.
    - A log-template miner like **Drain3** is *an* approach (clusters the
      message text itself, masks variable tokens → `... i=<*>`, recovers the
      per-message values); it handles partially-similar failures well. But
      we used the dep-free `(exception type, frame locations)` signature —
      keep deps down; it covers the "one bug, N tasks" case that matters and
      needs no runtime dependency.
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
- **query by status** — `-Q 'status == "failed"'` is not possible: queries
  are evaluated at matrix expansion, before the DB is consulted. `run -I`
  covers the main case (failed ∪ never-run), but selecting tasks by
  recorded status (failures-only for `set-state`/`ls-tasks`, say) would
  need plan-time filtering — decide whether it earns the complexity.
- **logging** - Perhaps the rule decorator could have a logger=True line, that
  passes in a loguru logger to the function? Or just say that the user can
  set up a loguru logger then use that as using `uses`.
- **intra-rule task dependency** - Should this be possible? A sequentially
  defined rule where each task depends on the one before? Challenges the
  no-task-DAG principle that planning memory, SLURM array eligibility and
  failure-skip propagation all lean on — needs a real design discussion.

- **Per-rule housekeeping job (SLURM) — probably to be implemented.** A
  one-shot (non-array) SLURM job depending `aftercorr`/`afterok` on a
  rule's array, running once after all its elements complete. Makes the
  edge between rule N's array and rule N+1's array an explicit, first-class
  job rather than relying solely on the global continuation job.
  - *Primary action: DB sync.* It is the natural single-writer point to
    ingest that rule's sidecars — exactly the low-concurrency path the
    sidecar design assumes, no new contention. The next plan/run already
    ingests, so the win is **timeliness** (mid-pipeline `info`/
    `slurm-status` reflect reality without waiting for the next manual
    command) plus being the *vehicle* for the cleanup actions below — not
    ingest correctness, which sidecars already give us.
  - *Fits the model.* Still submit-and-log-out: SLURM owns the graph for
    the pipeline's life; this is just another node in it. **Not** the
    rejected orchestrator daemon (see below) — nothing long-lived on a
    login node, no IPC, recovery is still replan-from-DB + `squeue`.
  - *Design question:* does housekeeping **replace** the single global
    continuation job (becoming a finer-grained per-rule version of it), or
    sit alongside it? Lean towards generalising the continuation into this.
  - Relates to the **SLURM monitor** item (a place to surface progress) and
    is the enabling mechanism for **temporary files** below.

- **Temporary / scratch intermediate files — desirable feature, at some
  point.** Let a pipeline mark intermediate outputs as deletable once fully
  consumed (HPC scratch pressure: huge intermediates that only exist to
  feed the next stage). Closest prior art: Snakemake's `temp()`, make's
  `.INTERMEDIATE`/`.SECONDARY`. Likely API: a per-output marker, e.g.
  `outputs={'x': temp('scratch/{site}.nc')}` or a rule-level
  `temporary=[...]`. The per-rule housekeeping job above is the deletion
  vehicle. This is **not** a SLURM add-on — it is a core-semantics change
  and must behave identically under `multiproc`/`singleproc`. The sharp
  parts:
  - *Delete after the last **consumer**, not the last **producer**.* A file
    may feed several downstream rules; safe deletion is "after the final
    consumer completes", a different DAG edge than "after the producing
    rule". So the deletion belongs on the file's last consumer, computed
    from the graph.
  - *The planner must not then try to rebuild it.* With the default
    `check_outputs`, a missing output makes remake want to rerun the
    producer — defeating the point. Temporary files need a distinct DB
    lifecycle: **produced, consumed, intentionally deleted — do not rebuild
    unless a consumer actually needs rerunning**, and when one does, rebuild
    the temporary input first (recursive, possibly back several temp stages).
  - *Failure interaction.* Deletion must be gated on consumers **succeeding**
    (`afterok`/`aftercorr`, element-wise where matrices align), never
    `afterany` — a partially-failed array must keep its inputs so the failed
    elements can rerun.
  - *Scratch is the flip side, design together.* Scratch files vanish on
    their own (system purge after N days), so even non-`temp()` inputs may
    be absent. Frame both as one **storage-class / lifecycle** attribute on
    a token: "complete in the DB, may be absent on disk, rebuildable from
    upstream" — `temp()` (we delete it) and scratch (the system deletes it)
    become two cases of the same machinery, rather than two overlapping
    mechanisms. Builds on the existing `check_outputs='fallback'` thinking.

- **Per-element SLURM startup cost (lazy networkx).** Each array element is
  a fresh `remake run-array-task` process whose wall-time is dominated by
  Python startup + imports, not the task. Measured cold: `import
  remake.remake_cmd` ≈ 336 ms, of which ~260 ms is **networkx**, pulled in
  eagerly by `remake.core.dag` at package load. But `run-array-task` never
  builds or traverses the DAG — it loads one task spec (`finalize=False`)
  and runs it. On JASMIN this bit hard during the failure-propagation test
  (2026-06-14): 40 elements landed on one node and cold-imported the same
  large module tree from the shared filesystem simultaneously — an import
  storm that turned sub-second imports into minutes and pushed several past
  a 20-min wall clock (`TIMEOUT`). Throttling (`--array=0-N%T`) caps
  concurrency and removes the storm, but each element still pays the import.
  Lazy-importing networkx (or keeping it off the `run-array-task` path)
  would roughly halve per-element startup and ease the storm. Mostly a
  benchmark artefact — real tasks compute for minutes, so startup is noise
  (cf. the 1e6/real-pipeline benchmarks, where startup wasn't the
  bottleneck) — so this is an optimisation, not a correctness issue. Needs
  a design pass: which imports are truly needed per task path, and whether
  to split a lean `run-array-task` entry point from the full CLI.

- **Fragile `remake` on PATH in SLURM jobs.** The generated sbatch payload
  invokes a bare `remake run-array-task ...`. The jobs only find it because
  `submit.sh` runs inside the `uv` venv and sbatch inherits that PATH via
  the default `--export=ALL`; submitted from a plain shell, `remake` is not
  on PATH (`which remake` finds nothing on a JASMIN login node — it lives
  only in the venv). Options to harden: emit `python -m remake.remake_cmd`
  instead of the console script; or capture the resolved interpreter/venv
  path at submission time and bake it into the sbatch script. Relates to
  the per-element startup item (a `python -m` entry point is also where a
  lean import path would live).

- **Orchestrator daemon — considered and rejected as load-bearing
  (2026-06-13).** Proposal: invoke a `remake-daemon` on most `remake run`
  to orchestrate tasks — a listener/responder subprocess + a process-runner
  subprocess, the single reader/writer of `remake.db` while active,
  monitoring SLURM queues and restarting failed jobs. The remake CLI would
  talk to the listener, which talks to the DB. Pitched benefits: single DB
  writer (no sidecars), live failed-job restart, live monitoring.

  Decision: **do not make a daemon default or load-bearing, especially for
  SLURM.** Reasoning:
  - *The "no sidecars for SLURM" benefit is largely illusory.* The
    contention problem was concurrent SQLite *writers*; sidecars fix it by
    construction (independent file writes, single-threaded ingest). A daemon
    serializes ingest too, but array jobs still run on compute nodes and
    their results must cross the compute→login boundary. The options are:
    write sidecars and let the daemon ingest them (still sidecars); open
    800 sockets back to one login-node listener (worse contention than 800
    independent FS writes, over a flaky/firewalled compute→login path, with
    backpressure we now own); or poll `sacct` (can't recover
    `uses_hash`/exception without the task writing a sidecar). So SLURM
    would almost certainly still consume sidecars — the daemon only
    duplicates what they already do, cheaply (validated ~2.5 ms/sidecar,
    linear, no cliff at 800-way).
  - *It fights JASMIN reality.* The current SLURM design's superpower is
    submit-and-log-out: the continuation job replans itself and SLURM (an
    HA, long-lived, cluster-wide scheduler) owns the dependency graph for
    the multi-day life of the pipeline. A daemon must instead stay alive on
    a login/sci node for that whole duration — exactly what JASMIN
    discourages (process-killers, memory caps, reboots). If it dies, cold
    recovery from DB + `squeue` *is* `remake run` replanning today — so the
    daemon adds a fragile layer on top of the stateless recovery it can't
    remove.
  - *Costs:* two execution models maintained forever (small/local and `-X`
    runs want a no-daemon fast path); IPC surface (singleton lock with
    NFS stale-detection — the same hard problem SQLite locking was — stale
    sockets, protocol versioning, crash recovery); a testability regression
    versus the current pure-function + golden-file SLURM tests; and forced
    coordination between concurrent invocations/users (today idempotent
    ingest + squeue de-dup make these safe-ish).
  - *Per-executor:* singleproc is already one writer (daemon = pure
    overhead + breaks `-X`); multiproc's coordinator wants the *parent* as
    sole writer via a `multiprocessing` queue, not a bespoke daemon; dask
    *already has* a daemon (its scheduler) with futures back to the client;
    SLURM is the only real target and the worst fit. For every executor
    except SLURM, single-writer is trivial or already provided by the
    runtime.

  What the daemon *is* good for — interactive, adaptive orchestration (live
  retry of transient failures, rich progress, continuous in-process
  replanning instead of continuation jobs, live SLURM monitor) — is real,
  but should be an **optional, non-load-bearing layer**, never the sole DB
  writer and never required for correctness:
  - A foreground, restartable `remake monitor`/`watch`: live view +
    opportunistic resubmit of failed/transient jobs. The pipeline stays
    correct and crash-recoverable without it. (You cannot have both "daemon
    is sole writer" and "works when the daemon dies"; on JASMIN you need
    the latter.) Relates to the **SLURM monitor** item above.
  - Transient SLURM failures: prefer sbatch `--requeue`/retry — SLURM does
    node-death/preemption requeue better than a login-node daemon could.
  - multiproc: drop local sidecars by making the parent the sole writer via
    a queue (a real simplification, no daemon needed).

- **Opt-in frame-locals dump on exception** — capture variable state at
  the moment a task raises, written alongside the stored traceback, as the
  *non-interactive* complement to `-X`.
  - Motivation: remake already has the stored traceback (`info -F`) and
    interactive post-mortem (`-X`, which forces singleproc, runs
    in-process and needs the failure reproduced at a terminal). The gap is
    a **failed SLURM array task that died on the cluster** — you can't pdb
    into it, but a locals dump captured at failure time lets you inspect
    what went wrong after the fact. That HPC case is the real value; this
    is not redundant with `-X`.
  - Design hazard — serialisation. "Full variable dump" taken literally is
    dangerous: rule locals routinely hold multi-GB xarray Datasets, open
    file handles, DB connections, unpicklable objects. Naive pickling
    either explodes the dump size or crashes *inside the failure handler*.
    So: best-effort **safe `repr()` with per-value truncation**, never
    pickle live objects; default to the **innermost frame + the rule
    function's frame**, not the whole chain at full depth.
  - Opt-in (off by default): size/cost, and a dump can write sensitive
    data/secrets to disk. A `config={'debug': {...}}` flag or a `run`
    option.
  - Lands alongside the per-task traceback under `.remake/tasks/log/...`
    so `task-info`/`task-log` surface it; rides the existing sidecar
    result path, so it works under SLURM arrays.
  - Prior art: `stackprinter` renders tracebacks with truncated per-frame
    values — a good dependency or reference implementation. (`cgitb`, the
    old stdlib answer, is removed in 3.13 — don't reach for it.)

## Graduated (designed and implemented; kept for the record)

- **SLURM job ids written to file** — `.remake/jobs/<rule>.jobids.json`
  sidecars; consumed by `slurm-status`, `task-info`, resubmission and
  already-queued detection.
- **Per-task logging under SLURM arrays** — per-task key-named log files;
  see design_docs/per_task_logging.md.
- **Task inspection/validation** — `remake lint` (near-miss input wiring,
  missing depends_on).
