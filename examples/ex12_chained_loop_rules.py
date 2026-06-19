# examples/ex12_chained_loop_rules.py
#
# Loop-generated rules with chained dependencies.
#
# Pattern: when a pipeline has N stages that form a sequential chain
# (stage N must complete before stage N-1 can start), generate one
# rule per stage in a loop with each depending on the previous.
# Within each stage, tasks run in parallel over a matrix.
#
# Real-world use case: HEALPix coarsening, where zoom level Z is
# built by averaging zoom level Z+1.  Each zoom level's tasks run
# in parallel, but the levels themselves must run in order.
#
# DAG:
#   generate (1 task — creates source data for each item)
#       |
#   stage_4[item]  (N tasks in parallel)
#       |
#   stage_3[item]  (N tasks in parallel)
#       |
#   stage_2[item]  (N tasks in parallel)
#       |
#   ...
#
# Key techniques:
#   - name= gives each loop-generated rule a distinct identity
#   - String depends_on= references a rule by name (resolved at
#     finalize time), so the loop doesn't need to thread Rule objects
#   - uses= tracks the captured stage value for change detection

from pathlib import Path

from remake import Remake, rule

rmk = Remake()

N_STAGES = 5
ITEMS = ['alpha', 'beta', 'gamma']


@rule(
    outputs={item: f'data/stage5/{item}.txt' for item in ITEMS},
)
def generate(outputs):
    for item, path in outputs.items():
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(f'{item}: ' + ','.join(str(x) for x in range(32)))


def _process(inputs, outputs, item):
    values = Path(inputs['data']).read_text().split(': ', 1)[1]
    nums = [int(x) for x in values.split(',')]
    reduced = [nums[i] + nums[i + 1] for i in range(0, len(nums), 2)]
    Path(outputs['data']).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs['data']).write_text(f'{item}: ' + ','.join(str(x) for x in reduced))


chain_rules = []
for stage in range(N_STAGES - 1, -1, -1):
    upstream = 'generate' if stage == N_STAGES - 1 else f'stage_{stage + 1}'

    @rule(
        name=f'stage_{stage}',
        inputs={'data': f'data/stage{stage + 1}/{{item}}.txt'},
        outputs={'data': f'data/stage{stage}/{{item}}.txt'},
        matrix={'item': ITEMS},
        depends_on=[upstream],
        uses={'stage': stage, '_process': _process},
    )
    def process(inputs, outputs, item):
        _process(inputs, outputs, item)

    chain_rules.append(process)

rmk.add_rules(chain_rules)
rmk.add_rules([generate])
