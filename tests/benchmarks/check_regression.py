"""Fail if a benchmark run is slower than a stored baseline.

Usage::

    python tests/benchmarks/check_regression.py RESULT.json \\
        [--baseline tests/benchmarks/baseline_field_scale.json] \\
        [--factor 2.0] [--min-slack 0.15]

Both files come from ``bench_field_scale.py --json``: ``{"calibration": s,
"phases": {name: s}, "meta": {...}}``.

Runner-speed normalisation: each file records ``calibration``, the best of
five runs of a fixed pure-Python loop. The baseline is scaled by
``result.calibration / baseline.calibration`` before comparing, so a runner
that is 1.5x slower at plain Python is allowed 1.5x more time. A pure-Python
loop is used (not I/O) because the benchmarked phases are CPU-bound, and
best-of-5 is robust to noisy neighbours. The scale is clamped to
[0.5, 4] so a freak calibration cannot disable the gate.

A phase fails when ``t > factor * scaled_baseline + min_slack``. The slack
(seconds) stops millisecond-scale phases tripping on timer noise. A phase
missing from the baseline is a warning, not a failure. Exit status 1 on any
failure, else 0.
"""
import argparse
import json
import sys
from pathlib import Path

DEFAULT_BASELINE = Path(__file__).with_name('baseline_field_scale.json')


def check(result, baseline, factor, min_slack):
    """Compare result to baseline; return (failures, warnings) as message lists."""
    scale = result['calibration'] / baseline['calibration']
    scale = min(max(scale, 0.5), 4.0)
    print(f'runner speed scale vs baseline machine: {scale:.2f}x')
    failures, warnings = [], []
    for name, t in result['phases'].items():
        base = baseline['phases'].get(name)
        if base is None:
            warnings.append(f'{name}: not in baseline ({t:.3f}s), skipped')
            continue
        limit = factor * base * scale + min_slack
        status = 'FAIL' if t > limit else 'ok'
        print(f'{status:4} {name:22} {t:8.3f}s  baseline {base * scale:8.3f}s  limit {limit:8.3f}s')
        if t > limit:
            failures.append(f'{name}: {t:.3f}s > limit {limit:.3f}s')
    return failures, warnings


def main(argv=None):
    """Parse arguments, run the comparison and return the exit status."""
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('result', type=Path)
    ap.add_argument('--baseline', type=Path, default=DEFAULT_BASELINE)
    ap.add_argument('--factor', type=float, default=2.0)
    ap.add_argument('--min-slack', type=float, default=0.15)
    args = ap.parse_args(argv)
    result = json.loads(args.result.read_text())
    baseline = json.loads(args.baseline.read_text())
    failures, warnings = check(result, baseline, args.factor, args.min_slack)
    for w in warnings:
        print(f'WARNING {w}')
    for f in failures:
        print(f'REGRESSION {f}')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
