# remake3_examples/ex3_uses_scope.py
#
# Demonstrates the `uses` mechanism for tracking external dependencies.
#
# Without `uses`, changing THRESHOLD or the normalise() helper would not
# be detected by the AST comparison of the rule_run function body alone,
# and tasks would not be rerun. Declaring them in `uses` fixes this.
#
# Run with `Remake(strict_scope=True)` to turn undeclared free variables
# into errors rather than warnings.

from pathlib import Path
from remake3 import Remake

rmk = Remake()          # use Remake(strict_scope=True) to enforce hard boundary

YEARS = list(range(2000, 2010))

# --- external dependencies we want to track ---

THRESHOLD = 0.5         # changing this should trigger reruns of filter_data

def normalise(values: list[float]) -> list[float]:
    """Changing this function body triggers reruns of filter_data."""
    lo, hi = min(values), max(values)
    return [(v - lo) / (hi - lo) for v in values]


# --- rules ---

@rmk.rule(
    inputs  = {'src': 'data/raw/{year}.csv'},
    outputs = {'out': 'data/filtered/{year}.csv'},
    matrix  = {'year': YEARS},
    uses    = {'threshold': THRESHOLD, 'normalise': normalise},
)
def filter_data(inputs, outputs, year):
    import csv
    rows = list(csv.DictReader(Path(inputs['src']).open()))
    kept = [r for r in rows if float(r['value']) > threshold]
    values = [float(r['value']) for r in kept]
    normed = normalise(values)
    Path(outputs['out']).parent.mkdir(parents=True, exist_ok=True)
    with Path(outputs['out']).open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['year', 'value', 'normed'])
        for r, n in zip(kept, normed):
            w.writerow([year, r['value'], f'{n:.4f}'])


@rmk.rule(
    inputs     = filter_data.outputs,
    outputs    = {'combined': 'data/results/all_years.csv'},
    matrix     = {},
    depends_on = [filter_data],
)
def combine(inputs, outputs):
    import csv
    Path(outputs['combined']).parent.mkdir(parents=True, exist_ok=True)
    all_rows = []
    for path in inputs.values():
        all_rows.extend(list(csv.DictReader(Path(path).open())))
    with Path(outputs['combined']).open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['year', 'value', 'normed'])
        w.writeheader()
        w.writerows(all_rows)
