# remake3_examples/ex1_simple.py
#
# Simplest possible pipeline: one input file, two chained rules.
# Shows the basic decorator syntax, no matrix.
#
# DAG: preprocess --> summarise

from pathlib import Path
from remake3 import Remake, rule

rmk = Remake()


@rule(
    inputs  = {'raw': 'data/raw/measurements.csv'},
    outputs = {'clean': 'data/processed/measurements.csv'},
)
def preprocess(inputs, outputs):
    lines = Path(inputs['raw']).read_text().splitlines()
    cleaned = [l for l in lines if not l.startswith('#')]
    Path(outputs['clean']).write_text('\n'.join(cleaned))


@rule(
    inputs     = preprocess.outputs,
    outputs    = {'summary': 'data/results/summary.txt'},
    depends_on = [preprocess],
)
def summarise(inputs, outputs):
    lines = Path(inputs['clean']).read_text().splitlines()
    Path(outputs['summary']).write_text(f'rows: {len(lines)}\n')


rmk.rules_from_current_module()
