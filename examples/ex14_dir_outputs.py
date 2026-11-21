# examples/ex14_dir_outputs.py
#
# Directory outputs: a task that writes a variable number of files.
#
#   - find_peaks writes one file per detected peak. How many is not known
#     until it runs, so the outputs cannot be a fixed dict of files. Instead
#     it declares a Dir: every file in it belongs to this task.
#   - A Dir is complete when remake has written its hidden manifest
#     (.remake-manifest.json) after the rule returned, so a task killed
#     part-way is never mistaken for finished. remake never deletes your
#     files: on a rerun only the manifest goes, and files an earlier run
#     left behind are recorded as stale (with a warning).
#   - Ensure(Dir(...)) adds a check: the task fails if it wrote no files.
#   - The downstream rules enumerate the Dir with a @deferrable matrix,
#     once find_peaks is complete.
#
# DAG: find_peaks[year] --> describe_peak[year, peak] --> summarise_peaks
#
# Try: remake task-info ex14_dir_outputs.py <task key>   (files, bytes, stale)
#      remake info ex14_dir_outputs.py                   (per-rule output size)

import json
import random
from pathlib import Path

from remake import Defer, Dir, Ensure, Remake, deferrable, rule

rmk = Remake()

YEARS = [2020, 2021]


@rule(
    outputs={'plots': Ensure(Dir('data/peaks/{year}'))},
    matrix={'year': YEARS},
)
def find_peaks(outputs, year):
    rng = random.Random(year)
    for i in range(rng.randint(2, 4)):
        # The Dir is a path: write into it directly (it exists already).
        (Path(outputs['plots']) / f'peak_{i}.json').write_text(
            json.dumps({'year': year, 'height': round(rng.uniform(1, 9), 2)}))


@deferrable
def peak_matrix():
    rows = []
    for year in YEARS:
        # The manifest marks completion: wait for it, not just for the dir.
        if not (Path(f'data/peaks/{year}') / '.remake-manifest.json').exists():
            raise Defer(f'data/peaks/{year}')
        for path in sorted(Path(f'data/peaks/{year}').glob('peak_*.json')):
            rows.append({'year': year, 'peak': path.stem})
    return rows


@rule(
    inputs={'peak': 'data/peaks/{year}/{peak}.json'},
    outputs={'desc': 'data/peak_desc/{year}_{peak}.txt'},
    matrix=peak_matrix,
    depends_on=[find_peaks],
)
def describe_peak(inputs, outputs, year, peak):
    height = json.loads(Path(inputs['peak']).read_text())['height']
    Path(outputs['desc']).write_text(f'{year} {peak}: height {height}\n')


def summary_inputs():
    return {f"{r['year']}_{r['peak']}": f"data/peak_desc/{r['year']}_{r['peak']}.txt"
            for r in peak_matrix()}


@rule(
    inputs=summary_inputs,
    outputs={'summary': 'data/results/summary14.txt'},
    depends_on=[describe_peak],
)
def summarise_peaks(inputs, outputs):
    lines = [Path(p).read_text() for _, p in sorted(inputs.items())]
    Path(outputs['summary']).write_text(''.join(lines))


rmk.rules_from_current_module()
