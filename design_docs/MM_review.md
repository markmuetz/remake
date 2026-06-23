# MM review log

A running record of Mark's source reviews: which files have been read
through, what came out of each, and the follow-ups still open. Reviews are
done by dropping `# MM:` comments inline; this doc tracks them at the file
level so progress is visible without grepping the tree.

## Files reviewed

| File | Date | Status | Outcome |
| --- | --- | --- | --- |
| `src/remake/remake_cmd.py` | 2026-06-23 | Reviewed | Refactor done — commits `933b5f0`, `4281192`. Two `# MM:` notes left intentionally (see below). |

## Outcomes from `remake_cmd.py`

The `# MM:` comments raised one architectural theme — *Remake objects should
expose the same functionality as the CLI, through Python* — plus some local
tidies. Addressed:

- **`933b5f0`** — moved the logic of `set-state`, `select-task` and `info`
  onto `Remake` (`set_state`, `_cascade_descendants`, `select_task`,
  `status_summary`); CLI methods now only render. `STATUS_NAMES` moved to
  `metadata_manager.py`.
- **`4281192`** — `_slurm_submission` → `slurm_executor.last_submission()`
  (sidecar reading is SLURM-executor knowledge); `import json` hoisted to
  module level. Heavy/optional imports (networkx, executor stack) kept local
  by design.

### Still open

- `planner.py:27` — `# MM:` question about `make_predicate` using
  `compile`/`eval` for `-Q` queries. Flagged as a possible risk; review of
  that path deferred (see the planner row once started).
- `remake_cmd.py:55` — `# MM: this is neat syntax`. No action (note only).

## Python-API parity assessment (2026-06-23)

Principle agreed during review: **a `Remake` method should exist for anything
that computes structured data a programmatic user would want; pure terminal
rendering (table layout, exit codes, argparse glue) stays in the CLI.** Test:
"would a notebook user want this back as a dict/list?"

Status of the remaining read-only commands against that test:

| Command | Verdict | Notes |
| --- | --- | --- |
| `why` | Already done | Backed by `Remake.explain_tasks()`; CLI only selects + prints. Minor tidy possible (its selection block partly duplicates `select_task`). |
| `lint` | Done | `Remake.lint()` → findings rows; CLI keeps formatting + `1 if problems` exit code. |
| `task-info` | Done | `Remake.task_info(task)` → data dict (same shape as `status_summary`); CLI renders. |
| `rule-dag` | Done | `Remake.rule_dag(with_matrix=...)` → `{order, edges, matrix_info?}`; builds a fresh DAG, no finalize/metadata needed. |
| `ls-tasks` | Covered | `Remake.iter_tasks()` already exists; input/output detail is presentation. |
| `slurm-status` | Covered | `squeue_snapshot` + `last_submission` already accessible. |

`lint`/`task-info`/`rule-dag` lifted 2026-06-23. Remaining optional: `why`
selection-logic tidy (low value — data already exposed).
