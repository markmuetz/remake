# examples/ex16_diagnostics.py
#
# Output checks on plain files, fail-fast on missing inputs, and the
# diagnostics commands for when a pipeline misbehaves.
#
#   - summarise16 declares its output as Ensure('...', min_size=, checksum=True):
#     when the task finishes, remake checks the file (non-empty, at least
#     min_size bytes) and fails the task if the check fails. checksum=True
#     records the file's sha256, shown by task-info (recorded, never compared).
#   - summarise16's input data/ex16/readings.txt is external: no rule makes
#     it (make_example_data.py does). If it is missing, `remake run` stops
#     before running anything, with one error (exit 2) and the
#     external_inputs_hint below, instead of one FileNotFoundError per task.
#   - calibrate16 fails only if the environment variable REMAKE_EX16_FAIL is
#     set, so the default run succeeds. Set it to "raise" to make calibrate16
#     raise, or to "empty" to make summarise16 write an empty file, which its
#     Ensure then rejects.
#   - Every task execution records wall time, CPU time and peak memory
#     (peak memory is sampled from /proc, so only shown on Linux).
#
# DAG: summarise16 --> calibrate16 --> report16
#
# Try (from the directory holding make_example_data.py):
#   python make_example_data.py ex16
#   remake dag ex16_diagnostics.py --rules       # the rule graph as DOT
#   remake dag ex16_diagnostics.py --tasks -o dag.dot
#   remake check-env ex16_diagnostics.py         # would a fresh job-style
#                                                #   process import the same code?
#   remake run ex16_diagnostics.py
#   remake task-info ex16_diagnostics.py <key>   # resources: wall/CPU/peak RSS;
#                                                #   size: bytes and sha256
#                                                #   (keys: remake ls-tasks)
#   remake info ex16_diagnostics.py --sizes      # output size per rule
#
# Make it fail, then diagnose:
#   REMAKE_EX16_FAIL=raise remake run -f ex16_diagnostics.py
#   remake info ex16_diagnostics.py --failed     # one line per failed task:
#                                                #   rule, key, last error line, log
#   remake export-task ex16_diagnostics.py <key> # the task's resolved spec as
#                                                #   JSON, to run its body from
#                                                #   another interpreter
#   remake run ex16_diagnostics.py               # fixed (variable unset): reruns
#                                                #   only what failed or was
#                                                #   downstream of it
# Run history (0.10): every execution of a task is kept, not just the last:
#   remake history ex16_diagnostics.py <key>    # calibrate16's executions, newest
#                                               #   first: run number, status,
#                                               #   resources, [rerun reason,
#                                               #   host], and the error
#                                               #   signature of a failed one
#   remake history ex16_diagnostics.py --runs   # one line per run: when, command,
#                                               #   how many tasks ran or failed
#   remake history ex16_diagnostics.py <key> --json   # the same, for scripts
# After the failing and fixed runs above, calibrate16 has a "never-run"
# success, a "forced" failure (RuntimeError at ...) and a "last-run-failed"
# success. `remake task-info` shows the last execution, with the git commit
# and environment of the run that made it. None of this triggers a rerun.
# See docs/guide/provenance.md.
# An Ensure failure looks the same:
#   REMAKE_EX16_FAIL=empty remake run -f ex16_diagnostics.py
#   remake info ex16_diagnostics.py --failed
#
# Fail-fast on a missing external input:
#   mv data/ex16/readings.txt data/ex16/readings.bak
#   remake run -f ex16_diagnostics.py     # exit 2, one error plus the hint
#   remake run -n -f ex16_diagnostics.py  # warns, still shows the plan
# (-f because up-to-date tasks are not re-checked for their inputs.)
#   mv data/ex16/readings.bak data/ex16/readings.txt

import os
import time
from pathlib import Path

from remake import Ensure, Remake, rule

rmk = Remake(config={
    'external_inputs_hint': 'Run `python make_example_data.py ex16` first.',
})


@rule(
    inputs={'readings': 'data/ex16/readings.txt'},
    outputs={'summary': Ensure('data/ex16/summary.txt', min_size=10, checksum=True)},
)
def summarise16(inputs, outputs):
    """Summarise the readings; burn a little memory so resource capture has
    something to show. REMAKE_EX16_FAIL=empty writes an empty file, which
    the output's Ensure rejects."""
    values = [float(x) for x in Path(inputs['readings']).read_text().split()]
    ballast = bytearray(50 * 1024 * 1024)  # 50 MB, to show up in peak RSS
    ballast[::4096] = b'x' * len(ballast[::4096])
    time.sleep(0.3)  # peak memory is sampled every 100 ms: stay long enough
    if os.environ.get('REMAKE_EX16_FAIL') == 'empty':
        Path(outputs['summary']).write_text('')
        return
    Path(outputs['summary']).write_text(
        f'n {len(values)}\nmean {sum(values) / len(values):.3f}\n'
        f'max {max(values):.3f}\n')


@rule(
    inputs={'summary': 'data/ex16/summary.txt'},
    outputs={'calibrated': 'data/ex16/calibrated.txt'},
    depends_on=[summarise16],
)
def calibrate16(inputs, outputs):
    """Scale the mean by a constant. Fails if REMAKE_EX16_FAIL=raise, as a
    stand-in for a real-world failure (bad data, a missing library, ...)."""
    if os.environ.get('REMAKE_EX16_FAIL') == 'raise':
        raise RuntimeError('calibration failed: REMAKE_EX16_FAIL is set')
    fields = dict(line.split() for line in Path(inputs['summary']).read_text().splitlines())
    Path(outputs['calibrated']).write_text(f"{float(fields['mean']) * 1.05:.3f}\n")


@rule(
    inputs={'calibrated': 'data/ex16/calibrated.txt'},
    outputs={'report': 'data/results/report16.txt'},
    depends_on=[calibrate16],
)
def report16(inputs, outputs):
    """Write the final report."""
    value = Path(inputs['calibrated']).read_text().strip()
    Path(outputs['report']).write_text(f'calibrated mean: {value}\n')


rmk.rules_from_current_module()
