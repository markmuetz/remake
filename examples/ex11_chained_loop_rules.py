# examples/ex11_chained_loop_rules.py
#
# Loop-generated rules with chained dependencies.
#
# Pattern: when a pipeline has N stages that form a sequential chain
# (stage N-1 must complete before stage N-2 can start), generate one
# rule per stage in a loop with each depending on the previous.
#
# Real-world use case: HEALPix coarsening, where zoom level Z is
# built by averaging zoom level Z+1.  Each zoom level's tasks run in
# parallel, but the levels themselves must run in order.
#
# DAG:
#   generate (1 task — creates source data)
#       |
#   stage_4  (N tasks in parallel)
#       |
#   stage_3  (N tasks in parallel)
#       |
#   stage_2  (N tasks in parallel)
#       |
#   ...
#
# Key techniques:
#   - Factory function returns a Rule from @rule inside a closure
#   - Default argument (_stage=stage) captures the loop variable
#   - rmk.add_rules() instead of rules_from_current_module() since
#     loop-generated rules aren't top-level module variables
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


def _make_stage_rule(stage, upstream):
    @rule(
        inputs={item: f'data/stage{stage + 1}/{item}.txt' for item in ITEMS},
        outputs={item: f'data/stage{stage}/{item}.txt' for item in ITEMS},
        depends_on=[upstream],
        uses={'stage': stage, 'ITEMS': ITEMS},
    )
    def process(inputs, outputs):
        for item in ITEMS:
            values = Path(inputs[item]).read_text().split(': ', 1)[1]
            nums = [int(x) for x in values.split(',')]
            # Reduce by summing pairs (halves the list each stage)
            reduced = [nums[i] + nums[i + 1] for i in range(0, len(nums), 2)]
            Path(outputs[item]).parent.mkdir(parents=True, exist_ok=True)
            Path(outputs[item]).write_text(f'{item}: ' + ','.join(str(x) for x in reduced))

    process.fn.__name__ = f'stage_{stage}'
    process.fn.__qualname__ = f'stage_{stage}'
    return process


chain_rules = []
prev = generate
for stage in range(N_STAGES - 1, -1, -1):
    r = _make_stage_rule(stage, prev)
    chain_rules.append(r)
    prev = r

rmk.add_rules(chain_rules)
rmk.add_rules([generate])
