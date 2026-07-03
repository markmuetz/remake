# Bug 05 — SLURM sidecar ingestion stamps the *current* run-code, not the code that ran

**Status:** open — reported 2026-07-03.
**Affects:** SLURM executor only (the sidecar → DB ingest path). Silently
produces **stale outputs with no rerun**: a completed task is recorded as
having run source it did not run, so a genuine code change is missed and
the planner reports "code unchanged". Local execution is **not** affected
(it records run-code in the same process that ran the task).
**Reported by:** Mark Muetzelfeldt — hit while adding
`gather_all_cloud_object_stats` to
`wescon_tools/ctrl/remakefiles/wescon_radar_dev.py`: ran the rule on
SLURM, then edited the rule body (plot change), and `remake why` insisted
the code was unchanged and refused to rerun.

## The bug

A SLURM array task records its result by writing a **sidecar** JSON on the
compute node (`metadata/sidecar.py`), which a later `remake` invocation
ingests into the DB (`sqlite3_backend.ingest_sidecars` →
`_ingest_records`). The sidecar carries the `uses` and `io` code
representations as text — computed on the compute node, against the code
that actually ran — but it does **not** carry the run-code source. At
ingest, the run-code id is taken from the *ingesting process's* current
rule state instead.

If the ingesting invocation runs against **edited** source (the ordinary
"run overnight, tweak in the morning, re-run" workflow), the just-completed
task is stamped with the new source's `run_code_id`. The planner then
compares new-vs-new and concludes "code unchanged" — so the edit does not
trigger the rerun it should, and the on-disk output is silently stale.

### Reproducer (from the wild)

1. Define a rule, run it on SLURM. The array element writes a sidecar
   holding `status/uses_hash/io_hash/run_seq/timestamp` — **no run
   source**.
2. Before any invocation ingests that sidecar, edit the rule *body*
   (a real logic change, e.g. rewrite the plotting).
3. Run `remake info` / `remake why` / `remake run`. This invocation:
   - calls `ensure_rules`, which interns the **new** source and sets
     `self.rule_ids[rule.name].run_code_id` → new;
   - then ingests the pending sidecar, stamping the task with that new
     `run_code_id`.
4. `remake why <task>` → `will run: no — code and uses unchanged`. The
   output on disk was produced by the **old** code and never regenerates.

Observed: `gather_all_cloud_object_stats` ran on SLURM (old plot), source
edited, next `remake info` ingested the sidecar; `why` reported code
unchanged despite a substantial body change.

## Root cause

The sidecar payload omits the run source
(`metadata/sidecar.py:52–60`):

```python
payload = {
    'status': status,
    'exception': exception,
    'uses_hash': compute_uses_hash(task.rule.uses),   # what ran
    'io_hash': compute_io_hash(task.rule),            # what ran
    'run_seq': self.run_seq,
    'timestamp': ...,
    # no run source
}
```

and ingest fills `run_code_id` from the current process, while correctly
interning `uses`/`io` from the payload
(`metadata/sqlite3_backend.py:483–505`):

```python
for rule, key, payload, _ in pending:
    rule_id, run_code_id, _, cur_io_code_id = self.rule_ids[rule.name]  # ← current disk state
    io_text = payload.get('io_hash')
    self.conn.execute(
        'INSERT INTO task(... run_code_id, uses_code_id, io_code_id ...) ...',
        (
            key, rule_id,
            run_code_id,                                   # ← NOT from the sidecar
            intern(payload.get('uses_hash', '')),          # ← from the sidecar ✓
            intern(io_text) if io_text else cur_io_code_id,# ← from the sidecar ✓
            ...
        ),
    )
```

The planner then compares against current source
(`core/planner.py:382–397`): `run_unchanged` uses
`code_comparer(stored_run, run_src)`. Because `stored_run` was resolved
from the ingest-time `rule_ids`, it *is* `run_src` — always "unchanged"
whenever a sidecar is ingested after a source edit.

## Do `uses` and `io` have the same problem?

**No — they are already immune**, and that asymmetry is the tell that the
run-code omission was an oversight rather than a design choice.

- `uses`: the sidecar carries `uses_hash` (the normalised uses text,
  computed on the compute node). Ingest interns *that* into the task's
  `uses_code_id`. The planner compares the stored text against
  `uses_hash(rule.uses)` recomputed from current source
  (`planner.py:392–393`). Stored = what ran, current = now; a changed
  helper ⇒ different text ⇒ rerun. Correct.
- `io`: identical, via `io_hash` (`planner.py:395–397`,
  `sqlite3_backend.py:505`). One narrow caveat: a **pre-`io_hash`-era**
  sidecar (no `io_hash` key) hits the `else cur_io_code_id` fallback and
  *would* exhibit the same bug — but every modern sidecar writes
  `io_hash`, so this only affects historical migration-window files.

So the fix is to make run-code follow the pattern uses/io already use, not
to add anything for uses/io.

## Fix

Thread the run source through the sidecar and intern it at ingest,
symmetric with `uses`/`io`:

1. `metadata/sidecar.py` — add to the payload:

   ```python
   'run_hash': task.rule.source['run'],   # raw source; run_code_id points at raw text
   ```

   (`source['run']` is `function_source(task.rule.fn)` — the same string
   `_ensure_rule` interns for the `'run'` part locally.)

2. `metadata/sqlite3_backend._ingest_records` — prefer the sidecar's run
   source, falling back to the current id only for pre-fix sidecars (same
   shape as the `io_text` fallback):

   ```python
   run_text = payload.get('run_hash')
   run_id = intern(run_text) if run_text else run_code_id
   ...
   run_id,   # in place of run_code_id in the INSERT
   ```

After this, all three code dimensions (run / uses / io) are recorded from
what actually executed on the compute node, independent of what the
ingesting process has on disk.

### Suggested regression test

Simulate ingest-after-edit: build a rule, write a sidecar with the
*original* `source['run']`, mutate the rule's function to new source,
`ingest_sidecars`, then assert the planner marks the task `run code
changed` (currently it reports unchanged). Guards the run dimension the
same way existing tests guard uses/io.

## Workaround (until fixed)

The recorded state is wrong, so force the rerun rather than relying on
change detection:

```
remake run <file> -E slurm --force -Q 'rule == "<edited_rule>"'
```

Only rules whose source changed *between* the SLURM run and the ingest are
affected; unchanged rules are stamped with an identical id, so the
mis-attribution is a no-op for them.
