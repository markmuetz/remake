# `temp()` outputs: can they be added after 1.0?

> **Status: Design check (0.10 item 6), concluded.** Class: **Design**.
> Verdict: **additive; nothing to change in 0.10.** `temp()` is not built
> in 0.10 and not scheduled. Claims below were checked against the code at
> `release/0.10.0` (2026-11-22); the discussion is in
> [discussion.md](../discussion.md) "Temporary / scratch intermediate files".

**Case:** an output that is complete in the DB, may be absent on disk, and
is rebuilt only if a consumer reruns.

## DSL

`outputs={'x': temp('scratch/{i}.nc')}` as a token wrapper, built like
`Ensure`: copy the token and set a class-level default attribute
(`temp = False` on `OutputToken`, next to `nonempty`/`min_size`/`checksum`).
Nothing blocks this:

- `Ensure` copies tokens with `copy.copy`, and `PathToken.format` and
  `Dir.format` copy too, so `temp(Ensure(Dir(...)))` and `Ensure(temp(...))`
  both keep both flags through matrix formatting.
- It must stay a **flag, not a subclass**. `canonical.io_part` hashes
  `type(v) is FileToken` as its bare path and renders other tokens by
  `repr`; a `TempFile` subclass would change `io_hash`, so adding or
  removing `temp()` would rerun the task. With a flag, `temp('a.nc')` hashes
  like `'a.nc'`, as `Ensure` does. (Whether toggling `temp` should rerun is
  then a free choice: it should not.)
- `temp` needs a way to delete: a non-abstract `OutputToken.delete()`,
  default `NotImplementedError`, implemented on `PathToken` (file, or
  `Dir` including its manifest). Like `Ensure`, `temp()` raises `TypeError`
  for a token that has none (`S3Object`, `ZarrStore` until they get one).
  Adding a non-abstract method does not break third-party subclasses.
- `is_complete()` keeps meaning "physically complete on disk". The planner,
  not the token, applies the DB's "intentionally deleted" state, so the
  frozen token interface (item 5a: `identity`, `is_complete`, `format`,
  `before_run`/`validate`/`after_success`, `stat`, `complete_many`, `remote`)
  needs no addition now.

## Planner

Today the planner never looks at the disk by default (`plan()` is DB-first;
`_outputs_complete(task)` is called only under `check_outputs` 'fallback' /
'always', from `plan()` and `explain_task()`). So under the default a
missing temp output already does *not* rerun its producer; the work is in
the opt-in modes and in the rebuild:

1. **No rerun for a deleted temp.** `_outputs_complete` is the single test
   used by `plan()` (both the "no record" adoption and the `'always'`
   branch), `explain_task()`, and `Remake.set_state` (`adoptable`). It would
   skip a temp token whose row is marked deleted. Needs the deleted set from
   metadata (one query, batched per rule like `_fetch_run_seqs`), passed in
   like the other injected reads.
2. **Rebuild on demand.** `plan()` runs in topological order and decides
   per task. After a rule's loop, a new step: for each task that will rerun,
   find upstream tasks (`edges.upstream_of(dep, task)`) whose temp outputs
   are marked deleted, and add them to `runnable`, recursively (their own
   deleted temp inputs first). Rule-level `rerun_kwargs` must not record
   these as reruns, or `upstream reruns` would rerun every other consumer.
3. **The run_seq backstop is the real hazard.** A rebuilt producer would get
   a fresh `run_seq`; the backstop (`_max_upstream_run_seq`, "upstream ran
   more recently") would then rerun all its other, up-to-date consumers on
   the next plan. A rebuild must therefore keep the task's `run_seq` and its
   record, and only clear the deleted marker. There is precedent:
   `mark_pending` calls `_commit_updates(..., run_seq=None)` to keep each
   record's `run_seq` for exactly this reason. A "rematerialise" run mode in
   `Remake.run` and the metadata write (new backend method, no schema change)
   would do this.
4. **Missing-inputs check.** `missing_external_inputs` never reports an
   input that some rule produces, so absent temp inputs are not flagged as
   external. No change.
5. **Rebuilt output differs.** Consumers are not rerun (that is the point).
   `output_stat.checksum` (if recorded) lets a rematerialise warn when it
   differs from the first build. Optional.

Functions that would change: `_outputs_complete`, `plan`, `explain_task`
(planner.py); `Remake.set_state` (`adoptable`), `Remake.run` and the
executors' task wrapper (rematerialise mode); a new method on the metadata
backend. None is frozen API.

## Schema

`output_stat(task_id, name, n_files, total_bytes, checksum)`. Candidate:
`ALTER TABLE output_stat ADD COLUMN deleted_at TIMESTAMP` (nullable; NULL =
present as far as the DB knows). `_add_missing_columns` already adds
nullable columns this way, and the minor bumps (`SCHEMA_MINOR`). A newer
minor is opened by older 1.x (the version check refuses only a newer
*major*, and it never stamps down).

**One catch, to handle when building, not now.** `_write_output_stats`
aggregates plain-file outputs into one row named `'*'`, and writes a named
row only for Dirs and checksummed files. A temp output needs a row of its
own so it carries its own flag: the writer must emit a named row for
`temp` tokens. That is writer behaviour, not schema; old readers see an
ordinary named row.

**What an older 1.x does with a DB that uses it:**

- It never reads `deleted_at`. Its plan is DB-first: task `success`, so
  skip. Same as a new remake.
- Its `output_stat` writes name their columns, so the new column defaults to
  NULL. When it reruns a producer it does `DELETE FROM output_stat WHERE
  task_id`, then reinserts: the deleted marker is cleared, which is right,
  because the output has just been rewritten. (A separate side table would
  *not* be cleared by an old writer and would leave a stale "deleted" flag
  on a present file: the reason to prefer the column.)
- With `check_outputs='always'/'fallback'`, or `verify --outputs --apply`
  (item 2), it sees the absent file and reruns the producer. That is wasted
  compute, or a failed run if the producer's own inputs are deleted temps
  (the task is left failed or pending and the pipeline is intact). It never
  deletes data: no remake code path removes outputs of a task that is not
  running (`Dir` never deletes files; only the executors' marker files are
  unlinked).
- The failure mode is therefore recompute, never loss.

Alternatives, if needed: a new table `output_state(task_id, name, state,
at)` (also additive, but not cleared by old writers, see above), or
`task.outputs_deleted` (per task, coarser). The column is preferred.

## `remake verify --outputs` (item 2)

Written after the item 2 design: "DB says success, output missing or
failing `Ensure`: report; `--apply` marks the task pending". In 0.10 no
temp exists, so it is correct as specified. When `temp()` ships, `verify`
reads the same `_outputs_complete`-style test (point 1), so a deleted temp
output is "expected absent", not a finding; a temp output that is absent
but *not* marked deleted (someone removed it by hand, or a scratch purge)
is reported as before. A temp output present on disk while marked deleted
clears the marker (report only). Because item 5a's batched
`complete_many` listing is what `verify` uses, build the "deleted" lookup
on the same pass. **Constraint on 0.10:** implement `verify --outputs`
through one shared function over (task, token), not inline, so the
`temp` rule has one place to land.

## Who deletes, and does it need anything frozen now?

Sensible default: **after every consumer has succeeded in the same
invocation**, from the last-consumer edge in the DAG (per matrix element
where `deps.Edge` is element-wise), never after a failure. A consumer in
another invocation or with a deferred matrix that has not resolved holds
the file (deletion is conservative: keep). Plus `remake clean --temp`
(report-only without `--apply`), which deletes temp outputs whose
consumers all succeeded, and sets `deleted_at`. In SLURM the deletion is
the per-rule housekeeping job (`afterok`, discussion.md), identical
semantics under multiproc/singleproc.

None of this needs to be frozen now: it is behaviour of new commands and a
new run step, not of the token interface, `--json` shapes (new fields are
additive after 1.0) or exit codes. The decision is parked until the feature
is built; the opt-in `clean --temp` can ship before the automatic deletion.

## Verdict

**Additive; nothing to change in 0.10.**

- DSL: a flag on tokens, as `Ensure`; no subclass, so `io_hash` is
  unchanged.
- Schema: nullable `output_stat.deleted_at` and a schema minor bump;
  older 1.x opens it and fails safe (recomputes, never deletes).
- Planner and `verify`: changes confined to new behaviour behind a
  per-token flag.

Two things to keep true in 0.10 so this stays cheap (no change needed now,
only do not regress): `verify --outputs` goes through one shared
completeness function (item 2), and `canonical.io_part` keeps treating
token options as non-identity (item 5a).
