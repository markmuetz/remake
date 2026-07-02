# Bug 03 — schema change forces a fresh `.remake/`, and the `never` default then wants to rerun everything

**Status:** open (UX / docs) — reported 2026-06-30.
**Affects:** anyone upgrading remake3 across a schema change with an
existing output tree. Local and SLURM.
**Reported by:** Mark Muetzelfeldt — hit on
`wescon_tools/ctrl/remakefiles/wescon_radar_dev.py` after a remake3 upgrade.

## What happened

1. An older `.remake/remake.db` had the pre-`uses_hash` `task` schema
   (`code_id`, no `uses_hash`/`run_code_id`). The newer backend queries
   `task.uses_hash`, so **every** command died with:

   ```
   sqlite3.OperationalError: no such column: task.uses_hash
   ```

   The backend docstring already says there is no migration support
   pre-release — "delete `.remake/remake.db` and rerun". So the recovery
   is correct and documented. (In this case the `task` table had 0 rows,
   so nothing was lost — but that is luck, not design.)

2. After deleting the DB, `remake run -n` wanted to run **all 1566 tasks**
   even though the full v13 output tree (7600+ files) was present on disk.
   Cause: this build defaults to `check_outputs='never'` (correctly —
   the `'fallback'`-as-default trap is documented in
   `remake3_0.8.0_release.md` §E.3 and `discussion.md`). With `never`, a
   no-DB-record task always reruns; existing outputs are not adopted.

3. Recovery was the migration-adoption idiom:

   ```
   remake set-state <file> -Q True --success --check-outputs
   ```

   which stat-verified each output and stamped 3321 tasks success, with
   no recompute.

## Why file this

Two rough edges, neither a logic bug, both worth smoothing:

- **The schema break is a cliff, not a ramp.** A partial historical
  migration left `rule` upgraded (it had `remakefile`) but `task` not
  (no `uses_hash`) — so the failure was a raw `sqlite3.OperationalError`
  from deep in the backend, not a remake-level "your `.remake/` predates
  this version; delete it and re-adopt with `set-state … --check-outputs`"
  message. A startup schema-version check that detects the mismatch and
  prints that sentence would turn a confusing crash into a one-line fix.

- **Doc drift on the default.** The design docs correctly pin the default
  to `'never'`, but the bundled Claude skill text
  (`design_docs/claude_remake_skill.md:117`, and the deployed `SKILL.md`)
  still tells the reader to rely on `check_outputs='fallback'` "so
  existing on-disk outputs are recognised without rerunning" as if it
  were ambient. Post-flip, adoption is explicit: `set-state … --success
  --check-outputs` (or a one-off `run --check-outputs`). The skill /
  migration note should say so, so the next person doesn't read a
  fresh-DB mass-rerun as a bug.

## Suggested fix

1. Backend: on connect, compare a stored schema version (or probe for
   `task.uses_hash`) and, on mismatch, raise a remake-level error naming
   the fix (delete + `set-state --check-outputs`) instead of letting the
   `OperationalError` escape.
2. Skill/migration docs: replace the "fallback is ambient" phrasing with
   the explicit `set-state --success --check-outputs` adoption step.
