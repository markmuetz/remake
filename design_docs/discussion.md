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
- **Web interface** — *being actively reconsidered (2026-06-23); scheduled
  as a 0.12.x exploration in [roadmap.md](roadmap.md).* The original design
  doc rules a GUI/dashboard out of scope, and a *passive read-only viewer*
  over the SQLite DB remains low-value (external tools can already query it).
  The idea now on the table is bigger and changes the calculus: a
  **single-page, interactive control plane** — launch and cancel runs, watch
  tasks change state in real time, drill into a failing task's log/traceback,
  re-run or `set-state` a selection, all from the browser. That is a genuine
  differentiator (no SLURM-native, stale-rebuild-aware tool offers live
  interaction at remake's scale), and the just-built programmatic `Remake`
  API is the natural backend for it — the web layer would be another *render
  + drive* client over the same methods the CLI uses, consistent with the
  "CLI is a thin render layer" principle (see compatibility.md / design doc).
  - *Open tensions to resolve before any work:* it reverses the "static
    reports, not a server" discipline that has kept remake lean (a live UI
    needs a running process — how does that coexist with a batch tool whose
    runs are detached SLURM submissions?); real-time task state under SLURM
    means polling `squeue`/sidecars or a push channel; auth/exposure on a
    shared HPC login node; and keeping it strictly optional (an extra, never
    a dependency of the core). Decide whether "real-time" means *observe* a
    detached run or *also drive* one, and whether the server is local-only
    (developer laptop / `ssh -L` tunnel) vs deployed.
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
  [RO-Crate](https://www.researchobject.org/ro-crate/) for
  publication/archival; natural successor to remake2's archive feature.
  Key realisation: remake already *holds* almost everything RO-Crate wants
  (rules, tasks, kwargs, input/output paths, code/uses/io hashes,
  timestamps, status) — the feature is mostly a **serialiser** over the
  existing metadata, not new bookkeeping.
  - *The mapping (remake → RO-Crate / schema.org):*
    - pipeline → the crate root `Dataset`; the remakefile → the
      `ComputationalWorkflow` / `SoftwareSourceCode` `mainEntity` (language
      Python; remake itself a `SoftwareApplication` with its version).
    - each rule → a `HowToStep` / `SoftwareApplication` carrying the rule's
      `run` source (already stored in `rule.source`).
    - each completed task → a `CreateAction`: `instrument` = the rule,
      `object` = input `File`s, `result` = output `File`s, `startTime`/
      `endTime` from metadata, `actionStatus` = Completed/Failed from task
      status, kwargs → `PropertyValue` parameters, `agent` = user/host.
    - each output → a `File` (`contentSize`, `dateModified`, optional
      `sha256`, `encodingFormat` by extension); a zarr/multi-file output →
      a `Dataset`, an S3 output → referenced by URL (the token type already
      tells us which: FileToken / ZarrStore / S3Object).
  - *Target profile:* the **Workflow Run Crate** profile (declare
    `conformsTo` its URI); it exists precisely for "a workflow plus a record
    of running it".
  - *Two modes:* **reference** (default) — metadata-only crate whose `File`
    entities point at data in place (cheap, for an existing tree); and
    **`--include-data`** — copy outputs into the crate dir / zip
    (self-contained, for archival/publication, the heavy path).
  - *Shape:* `remake ro-crate [remakefile] [-o DIR] [--zip] [-Q query]
    [--include-data] [--checksums]`. `-Q` scopes which tasks are crated.
  - *Implementation:* a new `remake/export/rocrate.py` that walks
    rules/tasks/metadata and emits `ro-crate-metadata.json`. **Hand-roll the
    JSON-LD** (the `@graph` is a small list of dict entities we fully
    control) rather than depend on `ro-crate-py` — consistent with the
    dep-averse stance elsewhere (cf. the Drain3-vs-hand-rolled call); pull
    `ro-crate-py` in later only if validation/round-trip earns it. Optional
    extra either way (`remake[rocrate]` if a dep is used).
  - *Checksums belong at workflow time, not crate time (own capability).*
    Hashing should happen **on the node that produced the output, right
    after it is written** — the bytes are local and hot in page cache, the
    work is distributed across the array, and the result is captured *as
    produced* (so later drift/corruption is detectable). Re-hashing at
    `remake ro-crate` time means a cold serial re-read of the whole tree
    over the network — or the data has been purged off scratch and can't be
    hashed at all. So a stored output checksum is a **general capability**,
    not an RO-Crate detail: `verify --checksum` (corruption, not just
    existence), output-versioning **(B) content-addressing**, the stats
    store, and dedup all consume it; RO-Crate is one reader. Tri-state, since
    it can't be unconditionally on (hashing multi-GB netCDF/zarr every run
    costs everyone): **off** (default; `ro-crate --checksums` stays as the
    lazy cold-re-read fallback), **on at run time** (opt-in
    `config={'checksum': 'sha256'}` / `run --checksum` → `run_task` hashes
    outputs post-success and ships the digest in the **sidecar payload**,
    computed compute-side), and a **hybrid read** (consumers use stored
    digests when present, else compute-with-warning or omit). sha256 for
    crate conformance; streamed post-write read rather than wrapping the
    write handle.
  - *Sharp edges:* **Scale:** 1e6 tasks → 1e6 `CreateAction`s is an enormous
    JSON-LD — so `-Q`-scope by default, warn past a threshold, and offer a
    `--summary` mode that emits one `CreateAction` per *rule* (with a task
    count) instead of per task. Incomplete/deferred tasks are omitted (or
    marked); reference-mode `File`s may be absent on scratch (degrade like
    `check_outputs`).
  - *Relates to:* the **stats store** (richer `CreateAction` timing /
    agent / parameters when present — but degrade to `last_run_timestamp`
    when not) and **grab code version / python module state** (workflow
    provenance: git hash + env → `SoftwareSourceCode.version` /
    environment). Degrade gracefully when those aren't recorded.
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

- **No-inputs/outputs orchestration pattern.** The hk26 pyflextrkr
  migration (2026-06-19) uses rules with only `depends_on` and `matrix` —
  no `inputs=` or `outputs=`. pyflextrkr manages its own file layout
  internally via `root_path`; duplicating those paths in remake outputs
  would be fragile and add no value. remake3 tracks completion via the
  metadata DB alone. This is a valid and likely common pattern when
  wrapping tools that own their own I/O (climate models, simulation
  frameworks, etc.). Worth documenting as a first-class pattern — the
  current examples all use inputs/outputs, which may give the impression
  they're required. A minimal example (e.g. `ex7_orchestration_only.py`)
  would help.

- **Variant-dict matrix pattern.** Same migration: domain/experiment
  overlay combinations are parameterised as a `VARIANTS` dict mapping
  labels to config file lists (`{'global': [], 'sahel_z10':
  ['configs/domains/sahel_z10.yml'], ...}`), with
  `list(VARIANTS.keys())` as the matrix dimension and the dict passed
  via `uses`. Adding a new variant is a one-line dict entry. This is a
  clean pattern for "matrix over configurations" (as opposed to simple
  scalars) that could be documented alongside the callable-matrix
  examples.

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

- **Output versioning** — a sanctioned mechanism for keeping (not
  silently clobbering) old outputs when a rule's code or inputs change.
  Today remake's stale-rebuild *detects* the change (run-code / `uses=` /
  now `io_hash`) and reruns, overwriting the previous output in place;
  the prior result is gone unless the user versioned the path by hand.
  The motivating want: re-run an analysis after a code change and still
  be able to compare against, or fall back to, the previous output.
  Options, roughly in increasing order of magic:
  - **(A) Manual `version=` matrix axis (no engine change).** The user
    threads a `version`/`v` kwarg through `matrix=` and bakes it into the
    output path (`out_{version}.nc`). Zero new machinery — it's just a
    normal matrix key — and fully explicit. Cost: entirely on the user to
    bump it, and every downstream rule must carry the axis too. This is
    the status-quo "do it yourself"; worth documenting as the blessed
    pattern even if we build nothing.
  - **(B) Content/code-addressed output paths.** remake already computes
    the hashes that change on a rerun (run-code, `uses_hash`, `io_hash`);
    fold a short digest into the output path automatically
    (`out.<hash8>.nc`), so a changed task writes a *new* file and the old
    one survives. Pros: automatic, never clobbers, natural provenance.
    Cons: paths become opaque; downstream resolution must agree on which
    hash is "current"; explodes file count; users often *want* a stable
    path to hand to external tools. Probably opt-in per rule.
  - **(C) Archive-on-overwrite.** Before a rerun overwrites an output,
    move the existing file to `.remake/archive/<key>/<timestamp>/` (or a
    user-set archive root). Keeps the live path stable (best of both),
    keeps history, and a `remake gc`/`--keep-last=N` prunes it. Cons:
    doubles IO on rerun for large files; archive can balloon; restore is
    a manual copy unless we add `remake restore`. Needs a retention
    policy from day one.
  - **(D) Metadata-tracked provenance only.** Don't touch the files;
    record per-run (timestamp, code hash, io_hash, output paths) in the
    DB and expose `remake versions <task>` to show the history. Cheapest,
    and a prerequisite for (C)'s restore anyway, but on its own it only
    *tells* you the output changed — it can't give the old bytes back.
  - Cross-cutting questions for any of these: does a version bump
    propagate downstream (almost certainly yes — it's a rerun trigger);
    how does it interact with `check_outputs` (an archived/old output is
    not "complete" at the live path); and SLURM-safety (archive move must
    be atomic and idempotent across array elements, like the sidecars).
  - Leaning: ship **(A)** as documentation now, build **(D)** next (it's
    low-risk and unlocks inspection), and treat **(C)** as the real
    feature once retention/restore is designed. **(B)** stays optional —
    powerful but the opacity makes it a poor default.

- **Optimistic direct DB write under SLURM, with sidecar fallback.**
  Today per-task array processes never touch the DB: they write JSON
  sidecars (design_docs/slurm_implementation.md), ingested in batch by
  the next DB-reader. That is robust but defers visibility — results are
  not in the DB until a *later* invocation (continuation job, next
  `run`/`info`) ingests them; a terminal wave needs a follow-up `remake
  info` before anything shows. Proposal: a task tries to write its result
  directly to `remake.db`, and falls back to a sidecar only if it fails.
  - The win is **latency-to-visibility**, not DB pressure relief.
    Results land the instant a task finishes, in the common (low-
    contention) case.
  - Why it is self-regulating rather than a thundering herd: the
    *fallback caps the herd*. A writer that fails gives up and goes quiet
    (writes a sidecar) instead of continuing to contend, so contention
    cannot escalate. And most real jobs are O(1–10 min) with natural
    jitter around the mean, so completion times — and thus write
    attempts — spread out rather than arriving as one synchronised burst.
    Quiet period → direct write succeeds; busy period → fast-fail and
    degrade gracefully to exactly today's sidecar behaviour.
  - **Hard requirement: the retry must be bounded and fast-failing.** The
    existing `retry_lock_commit` (sqlite3_backend.py) has *unbounded*
    exponential backoff — it never gives up, just slows down — which is
    the wrong primitive here. The direct-write path needs a separate
    `try_commit(max_attempts=2, busy_timeout=short)` that raises quickly,
    at which point the sidecar fallback engages.
  - Correctness is never at risk: the sidecar net still catches any
    failed/abandoned write, and the upsert is idempotent. Make ingest
    last-writer-wins by timestamp so a stale sidecar can't clobber a
    newer direct write.
  - Caveats: SQLite locking over NFS/Lustre is the real JASMIN hazard
    (flaky POSIX locks) — a bounded fast-fail could in principle falsely
    fail/succeed there, so this needs a cluster validation run before the
    default flips (`bench_sqlite_contention.py` extends to it). Don't
    reach for WAL as an alternative — WAL is unsafe over NFS. Likely
    shipped behind a config flag, perhaps auto-off above an array-size
    threshold; sidecar-only stays the conservative default until proven.

- **I/O verification / reconcile subcommand** (`remake verify`,
  `-Q`-composable). On-demand reconciliation of filesystem reality into
  the DB — snakemake-like, but opt-in rather than the only model. Three
  distinct operations, only one of which is risky; they should be split,
  not bundled:
  - **(a) Output reconciliation (`--outputs`)** — DB says success but the
    output is missing/incomplete on disk → mark the task pending. Safe;
    the persistent, explicit sibling of the transient
    `check_outputs='always'` plan-time check. Scratch-purge recovery.
  - **(c) Adoption (`--adopt`)** — output complete on disk but no DB
    record → mark success. Safe; persistent sibling of
    `check_outputs='fallback'`. Migrates an existing output tree into
    remake.
  - **(b) mtime staleness (`--mtime`, off unless asked)** — input newer
    than output → mark stale. This is the model remake deliberately
    rejected: mtime is unreliable on HPC (rsync, tar restore, scratch
    migration, Lustre all scramble it — the reason remake is DB-first).
    Two scoping rules if built: only meaningful for **external/source
    inputs** (rule-chained inputs are already ordered by the DB; mtime
    there adds only false-staleness risk), and "the output's mtime" for a
    zarr/multi-file output needs a defined answer (oldest part of tree).
  - State-mutating, so **report-by-default, `--apply` to write** — the
    state-writing sibling of the read-only `ls-tasks --check`. (a)+(c) are
    the safe core to build first; (b) stays behind its flag, scoped to
    external inputs, never default.

- **Stats / run-history store (`remake stats`).** Record what *happened*
  over time — observability/history, a fundamentally different concern
  from `remake.db`, which holds mutable *operational state* ("what needs
  rerunning right now?"). That distinction drives the design.
  - *What to record, by grain:*
    - **Per-run** (one `remake run`): `run_id`, start/end + wall duration,
      host, user, remake version, executor, CLI args / `-Q` query, and
      counts (planned / runnable / deferred / run / succeeded / failed /
      skipped-upstream-failed), number of replan **waves**, sidecars
      ingested. It may also *copy in* the pipeline's git hash and env hash
      per run, but it does **not own** them — see *grab code version* and
      *get python module state*, which are reproducibility data that should
      be recorded for everyone (in `remake.db` / task metadata), not gated
      behind this opt-in store.
    - **Per-task-execution** — the richest seam (today only a single
      `last_run_timestamp` is kept). One row *per execution* (full history,
      not last-only): wall duration, CPU time, **peak RSS**, **output bytes
      written**, status, exception type/signature (the `info -F` signature
      already exists), the **rerun-reason category** (`explain_task` already
      emits `Reason(category, …)` — recording it gives "58% code-changed,
      31% upstream, 11% never-run" rollups), host/node, owning `run_id`,
      attempt count.
  - *Derived aggregates (the fun part):* per-rule mean/median/**p95**
    duration, failure rate, total bytes, most-expensive rule;
    **flakiness** (tasks that have both succeeded and failed historically);
    **churn** (most-rerun tasks — smells of a volatile `uses` or poor
    isolation); throughput; full-pipeline time-to-completion. The headline
    value metric: **compute saved by stale-skipping** =
    `count(up-to-date skips) × historical mean duration` → "remake saved
    you ~14 CPU-hours this run" — the one number that justifies the tool to
    a sceptic.
  - *Separate DB (`.remake/stats.db`), not an extension to `remake.db`.*
    Different lifecycle (append-only + **disposable** vs mutable +
    mandatory), write pattern (pure inserts vs upsert-heavy), and schema
    churn (will keep growing fields vs stable). Keeping them apart means
    years of history can't bloat or threaten operational state, "stats off"
    is just *don't open the file*, and a stats migration never touches
    `remake.db`. Cost: no easy SQL join between current state and history —
    cheap to live without (`ATTACH` if ever needed).
  - *Write path rides existing machinery (no new contention).* `run_task`
    (remake.py) is the single execution chokepoint — wrap it to time the
    task, read RSS (`resource.getrusage`), stat output sizes, emit a stats
    row alongside the metadata write. Under SLURM, **extend the sidecar
    payload** (already carries status/timestamp/io_hash) with
    duration/RSS/bytes/reason, and have `ingest_sidecars` write `stats.db`
    in the same batched transaction — so stats inherit the sidecar
    contention solution for free (and piggyback on the optimistic
    direct-write hybrid if that ships).
  - *Read path:* `remake stats` (lifetime totals + this-run summary +
    per-rule table), `--rule X` / `--task <key>` for history, `--json` for
    scripting — ideal skill input (same pattern as the sacct audit: remake
    emits clean numbers, a skill turns them into advice).
  - *Guardrails:* opt-in / cheaply disable-able (one config flag);
    **retention/rotation from day one** (`stats prune --older-than`, or a
    row cap) or it's the unbounded-growth trap logs have; peak-RSS is
    approximate locally (`RUSAGE_CHILDREN` for multiproc), exact on the
    SLURM/`sacct` path; args/git/host can leak — same privacy care as the
    exception-dump item.
  - *Overlaps (references, does not subsume):* *grab code version* and
    *python module state* stay independent, always-on-if-implemented
    reproducibility features (this opt-in store may copy their values per
    run but must not be the only place they live); gives the **sacct
    resource audit** a place to persist used-vs-requested history; the
    `task_run` table *is* provenance history, feeding the output-versioning
    **(D) metadata-tracked provenance** option directly.

- **`check_outputs='fallback'` silently adopts stale outputs after code
  changes — defeats iterative development.** Discovered 2026-06-18 during
  `theta_e_analysis.py` development: editing the plot function (changing
  y-axis orientation, switching pcolormesh→contourf, etc.) then running
  `set-state --pending` + `remake run` repeatedly produced apparently
  identical output. The task's output file already existed on disk from a
  previous run; the `--pending` cleared its DB record; but the next `run`
  saw "no DB record + output exists" and silently re-adopted the stale
  file under the default `check_outputs='fallback'` mode — so the new code
  never actually executed. Only `remake run --force` bypassed the adoption
  and ran the updated code.

  This is clearly wrong behaviour for iterative work. The `fallback` mode
  was designed for **migration** (adopt a pre-existing output tree into a
  fresh `.remake/` without rerunning everything), and it works well for
  that one-shot case. But as the default mode during normal development it
  creates a trap: the user edits code, the planner sees "output complete,
  no record → adopt", and the edit is silently ignored. The user sees
  success, the output looks unchanged, and has no signal that the code
  never ran — the tool lies by omission.

  **Recommendation: default to `check_outputs='never'` for normal
  operation.** Migration adoption should be an explicit opt-in step
  (`set-state -Q True --success --check-outputs`, or a one-shot
  `check_outputs='fallback'` on the first run of a migrated pipeline),
  not an always-on default that silently swallows code changes. The
  `'fallback'` mode's invariant — "if the output exists, the task
  succeeded" — is only true when the code that produced it hasn't changed,
  and the planner *cannot check that* when there is no DB record to compare
  hashes against.

  Alternatively, if `'fallback'` stays the default: at minimum, adoption
  should be **loudly reported** (a per-task warning or an `info` summary
  line: "N tasks adopted from disk without running"), so the user knows
  their code was bypassed. But the deeper issue is that adoption is
  semantically wrong when the user *intends* a rerun — there is no way
  to distinguish "legacy output from a prior tool" from "stale output
  from the current tool's previous run with different code", and the
  default should not guess.

- **Key-concepts documentation.** The docs site is currently task-oriented
  (getting-started, running, SLURM, debugging) — it teaches you *how* to do
  things but never lays out *what the pieces are* and how they fit. A reader
  who hits an unfamiliar term (`token`, `Defer`, "rule-level DAG", `io_hash`)
  has nowhere to look it up. Propose a dedicated **Concepts** page (or short
  cluster of pages) sitting between "Getting started" and the "User guide",
  explaining each core idea once, in prose, with a one-line definition + a
  small example + links to the how-to guide that uses it. The model worth
  cribbing is Django's "topic guides" or Snakemake's "Rules" reference: a
  glossary you can read top-to-bottom *or* jump into.

  *The concept list (the ask, plus what I'd add):*
  - *Top-level objects:*
    - **Remakefile** — the Python file defining a pipeline; the `Remake`
      object; how rules get registered (`rules_from_current_module` /
      `add_rules` / `Remake(rules=...)`). **Must cover the cwd / `.remake`
      gotcha** (see the related todo): `remake run path/to/rmk.py` creates
      `.remake/` in the *current* directory, not next to the remakefile —
      this surprises everyone (flagged in the examples README too).
    - **Rule** — the `@rule`-decorated function; a template, not a unit of
      work.
    - **Task** — one concrete `(rule, kwargs)` instance; the actual unit of
      work; its identity/`key`; the relationship rule → many tasks.
    - **Matrix** — how a rule expands into tasks; the forms (dict product,
      `list[dict]`, tuple-keys, callable, `@deferrable`); fan-out/fan-in.
    - **The planner** — what decides what runs: DB-as-source-of-truth, the
      **rule-level (not task-level) DAG** and *why* (a deliberate design
      choice that SLURM-array eligibility, planning memory and failure-skip
      propagation all lean on), waves/replanning, deferral.
    - **Executors** — `singleproc` / `multiproc` / `slurm` / `dask` and the
      custom-executor injection point; what each is for; the
      submit-and-log-out SLURM model at a conceptual level.
    - **Metadata DB / tokens** — `.remake/remake.db` as operational state;
      **output tokens** (`FileToken`, `ZarrStore`, `S3Object`, custom
      `OutputToken`) and the `is_complete()` contract for non-file outputs.
  - *`@rule` parameters (the lower-level ask):* `inputs`, `outputs`,
    `matrix`, `depends_on` (incl. by-name strings), `uses`, `config`,
    `name`, `strict_scope` — each with the when/why, not just the type.
  - *Cross-cutting concepts I'd add:*
    - **Stale-rebuild / change detection** — *the* core idea: a task reruns
      when its run-code, `uses=` values or `inputs`/`outputs` spec change,
      compared by **content not timestamp** (`run` code / `uses_hash` /
      `io_hash`, AST-normalised so cosmetic edits don't count). This is the
      headline concept and currently only implied across scattered pages.
    - **Scope analysis & `uses`** — why free variables must be declared, what
      `ScopeWarning`/`strict_scope` do, the one-level-deep rule, the closure
      caveat.
    - **`check_outputs` modes** (`never`/`fallback`/`always`) — now documented
      in the running guide; the concept page should cross-link, not
      duplicate.
    - **Deferral** — `Defer` / `@deferrable` / dynamic matrices, as a named
      concept rather than only an example.
    - **Queries (`-Q`)** — the kwargs/rule predicate language shared by
      `run`/`info`/`why`/`set-state`/`ls-tasks`; its one evaluation-time
      limit (matrix-expansion, before the DB — so no `status ==` queries).
  - *Open questions for the design pass:* one long glossary page vs. a
    Concepts section of short pages; how much overlaps existing guide pages
    (rules-and-tasks already covers some) and whether to **move** that
    material into Concepts and leave the guides purely how-to; whether to
    auto-link glossary terms across the docs. The mkdocstrings API reference
    stays the source of truth for signatures — Concepts explains *meaning*,
    not types.

- **Explainer: task state and how to find it out.** A companion how-to (or a
  Concepts sub-page) for the single most common user question: *"what's going
  on, and what happens if I hit run?"* Today the answer is spread across five
  verbs with no page tying them together, and newcomers don't know which to
  reach for. The explainer's job is to draw the one distinction that makes it
  all click: **recorded state (the past, from the DB)** vs. **the plan (the
  future, computed by the planner)** vs. **the task set (what exists,
  independent of either)** — and show which tool answers which question.

  *The three questions and their tools:*
  - *"What is the recorded state?"* (past — reads the DB, no planning)
    - **`remake info`** — the headline overview: per-rule counts of
      `pending` / `success` / `failed` (the three recorded statuses) plus
      `to_run`, in dependency order, with a totals row. `-t` lists individual
      tasks with status; `-F` groups failures by traceback signature;
      `--json` for scripting.
    - **`remake task-info <key>`** — the deep dive on one task: status,
      resolved input/output paths, log location, SLURM job id.
  - *"What exists?"* (the matrix, independent of state)
    - **`remake ls-tasks`** — pure selection: materialise the matrices and
      list the tasks (optionally `-i`/`-o` their inputs/outputs, `--check` to
      stat them on disk). Deliberately says nothing about rerun logic — it
      answers "which tasks does this pipeline define", the denominator the
      other verbs report against.
  - *"What will happen if I run, and why?"* (future — runs the planner)
    - **`remake run --dry-run`** — the authoritative answer: the exact plan
      (what would run, counts, deferrals) without executing. The thing to
      reach for before a real or SLURM run.
    - **`remake why`** — the *reasoning* behind the plan: per task (`why
      <key>`), a query set (`why -Q ...`), or the runnable set (bare `why`),
      each with the planner's `Reason` (never-run / last-run-failed /
      run-code-changed / uses-changed / io-changed / upstream-rerunning / …).
    - **`remake info --reasons`** — the same reasons rolled up: a per-rule
      tally of *why* the to-run tasks would rerun ("stage1: 4 last-run-failed,
      2 code-changed"). The aggregate companion to per-task `why`.

  *The mental model the page must land:* recorded status is only an input.
  The planner combines it with code/`uses`/`io` hashes, upstream reruns, the
  `check_outputs` mode and deferral to reach a **verdict** — *will-run*,
  *up-to-date*, *deferred*, or *skipped (upstream failed)* — and the verdict,
  not the stored status, is what `run` acts on. So a `success` task can still
  rerun (its code changed) and a `pending` task can be skipped (its upstream
  failed). `info` shows you the status; `why` / `--dry-run` show you the
  verdict and the reason. A worked example — run, edit one rule, then walk
  `info` → `why` → `run --dry-run` on the same pipeline — is the spine of the
  page. Relates to the **Queries (`-Q`)** concept (the selector every one of
  these verbs shares) and the **query-by-status** discussion item (why you
  *can't* yet ask `-Q 'status == "failed"'` and must use `info -F` /
  `run --ignore-code-changes` instead).

- **Propagation gap: upstream→downstream rerun propagation is not durable.**
  Moved to a tracked bug doc: see
  [design_docs/bugs/01_durable_rerun_propagation.md](bugs/01_durable_rerun_propagation.md)
  (crash + partial-target scenarios, fix = run-sequence id).

## Graduated (designed and implemented; kept for the record)

- **Single `.remake/` per directory: rule provenance + duplicate-rule-name
  guard.** **Done 2026-06-22.** One `.remake/remake.db` is shared by everything
  run in a directory, and identity is namespaced only by rule name + kwargs
  (`Task.key = sha1('<rule>:<kwargs>')`, task.py; `rule` looked up by name,
  sqlite3_backend.py) — *no* remakefile component. This is deliberate: it lets a
  pipeline split across imported files compose over one shared state/output
  store. The cost is when two *different* co-located remakefiles define a rule
  under the same name: they silently clobber each other's `rule`/`task` rows
  (spurious code-change thrash), `.remake/jobs/<rule>.json`, and the per-task
  log / SLURM-output dirs — and a code edit is indistinguishable from a
  collision without provenance. Shipped: a `remakefile` column on the `rule`
  table (recording which file last defined each rule; forward-compat migration
  in `_add_missing_columns`), threaded via `ensure_rules(..., remakefile=)`. The
  guard warns in `_ensure_rule` when a same-named rule's code differs *and* was
  last written by a different known remakefile — distinguishing a collision from
  an ordinary same-file edit; identical-source shared rules never warn. Tests in
  test_metadata.py. Provenance is independently useful (inspection, future GC of
  orphaned records once a remakefile is deleted).
  - *Still open (parked):* surface the same check statically in `remake lint`
    (pre-run, scans sibling remakefiles); and a `Remake(name=…)` namespace as an
    opt-in escape hatch if real collisions warrant full isolation. Default stays
    a warning (not an error) — co-location is rare and sometimes intentional.

- **Dynamic matrices: defer on *stale* upstream, not just *absent* (Fix
  A+B).** **Done 2026-06-17.** A callable matrix expanded at plan time from
  an on-disk output that an upstream was about to overwrite ran the wrong
  task set; the local replan loop self-healed but the single-plan SLURM
  path did not. Shipped the `@deferrable` marker refinement: the exception
  was renamed `MatrixNotReady → Defer` (no longer a `RemakeError` — it's a
  control-flow signal), and `@deferrable` (rule.py) marks a matrix as one
  that derives its task list from upstream outputs. Raising `Defer` from an
  unmarked matrix is a `SignatureError` (resolve_matrix, dag.py); the
  planner defers a `@deferrable` rule when any `depends_on` upstream is
  rerunning this wave (`_upstream_rerunning`, planner.py), not only when an
  upstream output is absent — so ordinary product callables are never
  over-deferred. Tests in test_pipeline.py (defer-on-rerun + the
  non-deferrable contrast), test_dag.py, test_rule_signature.py.
  - *Still open (parked):* could `@deferrable` optionally name *which*
    upstream outputs it reads, so the planner defers precisely on those
    rather than on any `depends_on` rerun? Currently any upstream rerun
    defers the rule (safe, occasionally an extra wave).

- **SLURM job ids written to file** — `.remake/jobs/<rule>.jobids.json`
  sidecars; consumed by `slurm-status`, `task-info`, resubmission and
  already-queued detection.
- **Per-task logging under SLURM arrays** — per-task key-named log files;
  see design_docs/per_task_logging.md.
- **Task inspection/validation** — `remake lint` (near-miss input wiring,
  missing depends_on).
