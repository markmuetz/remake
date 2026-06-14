# Debugging

When a task fails, remake records the failure (with its traceback) rather than
stopping the world, and skips downstream tasks that depended on it.

## See what failed

```bash
remake info pipeline.py --show-failures
```

prints each failed task with its stored traceback and failure timestamp.

## Why did (or didn't) a task run?

```bash
remake why pipeline.py <task>
```

explains the planner's decision for a task — up to date, code changed, input
newer, never run, upstream failed, and so on.

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

## The remake log

Every remakefile subcommand writes a rotated DEBUG log at
`.remake/remake.log` for the local (non-array) paths.
