# examples/ex3_uses_scope.py
#
# Demonstrates the `uses` mechanism for tracking external dependencies.
#
# Without `uses`, changing THRESHOLD or the normalise() helper would not
# be detected: remake only compares the code (structure) of the rule
# function's own body, not the values and helpers it references. Declaring
# them in `uses` brings them into that comparison, so a change reruns.
#
# Undeclared free variables are warnings by default. `combine` below opts
# into strict_scope=True per-rule (errors at decoration time); pass
# Remake(strict_scope=True) to enforce that for every rule.

from pathlib import Path
from remake import Remake, rule

# Use Remake(strict_scope=True) to enforce hard boundary
# If you do this, any variable declared outside a rule's body then used in
# the body will raise an Exception.
rmk = Remake()

YEARS = list(range(2000, 2010))

# --- external dependencies we want to track ---

# Variable declared outside the rule's body.
THRESHOLD = 0.5         # changing this should trigger reruns of filter_data

def normalise(values: list[float]) -> list[float]:
    """Normalise the values to between 0 and 1"""
    # Changing this function body triggers reruns of filter_data.
    lo, hi = min(values), max(values)
    return [(v - lo) / (hi - lo) for v in values]


# --- rules ---

@rule(
    inputs  = {'src': 'data/raw/{year}.csv'},
    outputs = {'out': 'data/filtered/{year}.csv'},
    matrix  = {'year': YEARS},
    # Pass in these variables to the rule's body.
    # Try commenting out this line to see what happens.
    # What about if you use `strict_scope=True` above?
    uses    = {'threshold': THRESHOLD, 'normalise': normalise},
)
def filter_data(inputs, outputs, year):
    import csv
    with Path(inputs['src']).open() as f:
        rows = list(csv.DictReader(f))
    kept = [r for r in rows if float(r['value']) > threshold]
    values = [float(r['value']) for r in kept]
    normed = normalise(values)
    with Path(outputs['out']).open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['year', 'value', 'normed'])
        for r, n in zip(kept, normed):
            w.writerow([year, r['value'], f'{n:.4f}'])


@rule(
    # Fan in over years.
    inputs       = {str(year): f'data/filtered/{year}.csv' for year in YEARS},
    outputs      = {'combined': 'data/results/all_years.csv'},
    depends_on   = [filter_data],
    strict_scope = True,   # undeclared outer-scope names are an error here
)
def combine(inputs, outputs):
    # Try uncommenting this line: what happens?
    # print(THRESHOLD)
    import csv
    all_rows = []
    for path in inputs.values():
        with Path(path).open() as f:
            all_rows.extend(list(csv.DictReader(f)))
    with Path(outputs['combined']).open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['year', 'value', 'normed'])
        w.writeheader()
        w.writerows(all_rows)


rmk.rules_from_current_module()
