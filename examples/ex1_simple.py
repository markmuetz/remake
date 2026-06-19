# examples/ex1_simple.py
#
# Simplest possible pipeline: one input file, two chained rules.
# Shows the basic decorator syntax, no matrix.
#
# DAG: preprocess --> summarise

from pathlib import Path
from remake import Remake, rule

rmk = Remake()


@rule(
    inputs  = {'raw': 'data/raw/measurements.csv'},
    outputs = {'clean': 'data/processed/measurements.csv'},
)
def preprocess(inputs, outputs):
    # inputs['raw'] == 'data/raw/measurements.csv'
    lines = Path(inputs['raw']).read_text().splitlines()
    cleaned = [l for l in lines if not l.startswith('#')]
    # outputs['clean'] == 'data/processed/measurements.csv'
    Path(outputs['clean']).write_text('\n'.join(cleaned))


@rule(
    # You can access a previous rule's outputs.
    inputs     = preprocess.outputs,
    outputs    = {'summary': 'data/results/summary.txt'},
    # This is how you build the dependencies between rules.
    depends_on = [preprocess],
)
def summarise(inputs, outputs):
    lines = Path(inputs['clean']).read_text().splitlines()
    Path(outputs['summary']).write_text(f'rows: {len(lines)}\n')


# This finds all rules in the current scope and adds them to `rmk`.
rmk.rules_from_current_module()
