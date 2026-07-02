# Debugging

When a task fails, remake records the failure (with its traceback) rather than
stopping the world, and skips downstream tasks that depended on it.

## See what failed

```bash
remake info pipeline.py --show-failures
```

groups failed tasks by their unique traceback signature with a count (so one
bug across a thousand tasks is one entry), each with its stored traceback and
timestamp. Add `--all-failures` to list every failed task individually instead
of grouping. `remake info pipeline.py --reasons` gives the complementary view:
a per-rule tally of *why* the to-run tasks would rerun.

## Why did (or didn't) a task run?

```bash
remake why pipeline.py <task>
```

explains the planner's decision for a task — up to date, never run, last run
failed/pending, run code changed, `uses=` changed, inputs/outputs spec changed,
outputs missing, or an upstream rerunning (remake never consults file mtimes).
`<task>` is a task-key prefix; or pass `-Q "<query>"` to explain every matching
task, or omit both to explain the whole runnable set.

One reason worth knowing is **upstream-newer**: an upstream was rebuilt in a
later invocation than this task without rerunning it in the same pass — for
example you ran the upstream alone with `run -Q`, or a crash killed the
consumer first. remake tracks this with a per-run counter, so the consumer is
not silently left looking up-to-date; it reruns, and `why` reports "an upstream
ran more recently … output may be stale". If the upstream's output did *not*
actually change and you want to stop the rebuild, mark the consumer complete
with `set-state -Q '<consumer>' --success` (which by default also settles its
downstream).

## Per-task logs

```bash
remake task-log pipeline.py <task>
remake task-info pipeline.py <task>
```

`task-log` prints the task's log file; `task-info` shows its recorded metadata
(status, paths, jobid/array index for SLURM tasks).

## Post-mortem debugging with `-X`

```bash
remake run pipeline.py -X
```

`-X/--debug-exception` forces in-process execution (`singleproc`) and drops
into `pdb`/`ipdb` at the first exception, with the original traceback intact.
Because the normal executors catch failures by design, `-X` is the way to get a
debugger on a failing task.

!!! note
    `-X` is a flag on `run`. Install the `debug` extra for `ipdb`:
    `pip install "remake[debug]"`.

For a non-interactive context (CI, a script, or when you just want the
traceback and exit) use `--raise` instead: like `-X` it forces `singleproc`
and re-raises the first task failure with its original traceback, but it does
not attach the `pdb`/`ipdb` excepthook.

```bash
remake run pipeline.py --raise
```

## Fixing one failure and continuing

After fixing the cause, just rerun — remake picks up the previously-failed
task (and anything downstream that was skipped):

```bash
remake run pipeline.py
```

Or restrict to the failed rule with a query:

```bash
remake run pipeline.py -Q "rule == 'process'"
```

## The remake logs

Every remakefile subcommand (on the local, non-array paths) writes three
rotated logs next to the metadata DB:

- `.remake/remake.log` — the human-facing run narrative (INFO and above).
- `.remake/remake.debug.log` — the DEBUG firehose (TRACE under `-T`):
  timings, per-rule planning detail, the invocation's argv.
- `.remake/remake.jsonl` — a structured mirror of the debug stream, one
  JSON object per record. Metrics are real fields under `record.extra`
  (e.g. `event: "plan"`, `nrunnable`, `seconds`), and every record from
  one invocation shares a `run_id` — so mining is `jq`, not regex:

```bash
jq -r 'select(.record.extra.event == "status_query") | .record.extra.seconds' \
    .remake/remake.jsonl
```

Status-query timing lines only reach DEBUG when the query is slow
(>100 ms); the rest are TRACE, so the debug stream stays focused on
outliers.
