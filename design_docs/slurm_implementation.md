# SLURM executor — implementation plan

Implementation plan for the SLURM executor item, to be developed and
validated on JASMIN. The behaviour is fully specified in
[remake3_design.md](remake3_design.md) (SLURM executor and dynamic
matrices sections) and scoped in
[detailed_code_implementation.md](detailed_code_implementation.md) §4 —
this doc covers how the work proceeds, what is testable locally, and the
cluster-specific risks.

Settled questions, not to be reopened:

- Continuation-job replanning cost is measured and acceptable (~14 s for a
  fully populated 1e6-task DB; see `tests/benchmarks/`). No per-rule
  incremental planning machinery.
- Task lookup in array jobs uses direct construction
  (`Remake.task_from_spec(rule_name, kwargs)`, 0.04 ms at 1e6 tasks), not
  key search. The per-rule JSON specs carry `rule` + `kwargs` precisely
  for this.

## Implementation pieces

1. **Rewrite `executors/slurm_executor.py`** (the stale remake2 file is
   deleted as this lands). Mostly deterministic file generation:
   - `.remake/jobs/<rule>.json` — array of `{task_key, rule, kwargs}`;
     SLURM array index = position in the array.
   - `.remake/jobs/<rule>.jobids.json` — sidecar written at submission.
   - `.remake/slurm/<rule>.sbatch` — per-rule script, array-parameterised
     where eligible; SLURM options merged from Remake-level config and
     per-rule `config={'slurm': {...}}`.
   - `.remake/submit.sh` — master script: `sbatch --parsable` capture,
     `--dependency=aftercorr:`/`afterok:` wiring, jobid sidecar writes.
   - Array eligibility: no intra-rule deps ∧ same matrix as upstream ∧
     size ≥ threshold (default 10). Fan-in rules fall back to individual
     jobs with `afterok`.
   - Continuation job: rules deferred at generation time are excluded
     from `submit.sh`; a `continuation.sbatch` re-invoking
     `remake run --executor slurm` is appended after the last submitted
     wave. Idempotent replanning handles arbitrarily deep chains.
   - Already-queued detection: read jobid sidecars, check `squeue` by job
     ID + array index (never by job name).

2. **CLI additions**:
   - `resubmit <remakefile>` — re-execute `.remake/submit.sh`, no
     replanning.
   - `run --executor slurm --dry-run` — generate `.remake/jobs/`,
     `.remake/slurm/` and `submit.sh`, submit nothing.
   - `run-task` payload in sbatch scripts: pass rule + array index; the
     task spec is read from the rule's JSON and constructed via
     `task_from_spec` (extend `run-task` or add a `run-array-task`
     internal command — decide during implementation).

## Local-first testing (no cluster)

Nearly all of the executor is testable on a laptop; cluster time should
be spent only on cluster-shaped problems.

- Golden-file tests: generate specs/sbatch/submit.sh for known pipelines
  (ex2/ex4 shapes) and assert content — array eligibility, dependency
  wiring, config merging, continuation emission for ex8-style dynamic
  rules.
- Fake `sbatch`/`squeue` shims on PATH: test submission flow, `--parsable`
  jobid capture, sidecar writing, already-queued detection, `resubmit`.

## Pre-flight items (gate the cluster work, in order)

1. **Packaging** — `pip install -e .` has never been run; the dynamic
   version in pyproject may import the package at build time. A working
   install in a JASMIN env is needed on day one. (todos.md, Packaging.)
2. **Tracebacks** — `run_task` stores `repr(e)` only; debugging failed
   cluster jobs without tracebacks is miserable. Store full tracebacks
   and add `info --show-failures` first. (todos.md, Failure UX.)
3. **File logging** — restore CLI file logging (per-job logs are how
   cluster failures get debugged). (todos.md, Smaller debts.)

## Cluster-shaped risks

- **SQLite on shared filesystems — the big one.** Every array element
  does an EXCLUSIVE-locked write to `.remake/remake.db`. SQLite locking
  over Lustre/NFS-class filesystems is notoriously unreliable (locks not
  honoured, or pathological serialisation under hundreds of concurrent
  writers). The retry/backoff machinery exists but has never faced real
  contention. Probe this actively and early on JASMIN rather than
  discovering it. Possible responses, escalating:
  1. it works — ship it;
  2. tune (longer backoff, busy_timeout, journal mode);
  3. per-job sidecar result files merged into the DB by the next
     plan/continuation run (no concurrent DB writers at all).
- **Scheduler quirks**: `aftercorr` semantics with partially-failed
  upstream arrays; JASMIN array-size and queued-job limits; partition
  names and accounting. The examples already use real JASMIN partition
  names (`short-serial`).

## Suggested order

1. Pre-flight: packaging verification, tracebacks, file logging.
2. Executor generation (stage 1+2) + golden-file tests, locally.
3. Submission flow + fake sbatch/squeue shim tests, locally.
4. JASMIN: install, run ex2/ex4 for real; probe SQLite contention with a
   wide array job; then ex8-style continuation chains.
5. Delete the stale remake2 slurm executor; tick the implementation-plan
   item.
