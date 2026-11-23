# SLURM: tasks killed without a result

**Status: Implemented (simple plan), 2026-11-23. Release 0.11.0, item 9.**

A node failure in the 0.11 soak killed SLURM array elements, which recorded
nothing, and every remake command misreported what had happened
(design_docs/records/soak_0_11.md, finding 7). This folder holds the fix.

| Doc | What it is |
|---|---|
| [plan.md](plan.md) | The design: PENDING at submission, live state from `squeue`, the cause from `sacct` on demand. |
| [testing.md](testing.md) | The tests: a simulated SLURM cluster, scenario tests, the live JASMIN probe. |
| [implementation.md](implementation.md) | A log of difficulties and changes of plan, including why a much larger first plan was dropped. |

Decisions (MM, 2026-11-23):

1. **Simple over complete.** A first plan (a reconcile step classifying
   every element from `sacct`, storing killed tasks as failed, an
   end-of-submission job) was dropped after ~400 lines as too complex to
   debug. Two advisors (Fable, Opus) independently agreed.
2. Submitted tasks are **PENDING until their results arrive**; nothing
   records why a task died. The cause is read from `sacct` when asked.
3. **No worker-written "started" marker**: `squeue` gives the live state.
4. The continuation being cancelled by any failure (soak finding 7, "also")
   is a **separate** issue, not fixed here: documented.
5. **Docstrings count** in code hashes (`uses=` and rule code): unchanged
   (soak finding 8).
