# examples/ex13_rule_factory.py
#
# Rule factories: one function that generates several near-identical rules.
#
# DAG:
#   make_data[expt]            (one task per experiment)
#       |
#   stats_mean[group]          \
#   stats_max[group]            > made by one factory, one rule per analyser
#   stats_range[group]         /
#       |
#   summary                    (fan-in over every group of every analyser)
#
# Key techniques:
#   - name= gives each generated rule its own identity (the function name
#     is the same for all of them)
#   - Every generated rule shares ONE function source, so a change to an
#     analyser is only seen through `uses`: the analyser goes in
#     uses={'analyser': ...}
#   - The factory's own parameters must not share a name with a `uses` key
#     or with anything the rule body reads. A `uses` key that is also a
#     closure variable of the factory is a trap: the body sees the closure
#     value (a plain list, here), not the uses value. So the factory
#     parameters are called `analyser_name` and `group_names`, and the
#     `uses` keys `analyser` and `groups` are used only inside the body.
#   - Callables made in the factory (the `inputs` lambda) read only their
#     own kwargs, never factory variables: a closure's value is invisible
#     to change detection.
#   - Rules held only in a loop or a factory are registered with
#     rmk.add_rules(...). rules_from_current_module() would find only the
#     one the module-level name `stats` still points to.

from pathlib import Path

from remake import Remake, rule

rmk = Remake()

EXPTS = ['e1', 'e2', 'e3', 'e4']
GROUPS = {'low': ['e1', 'e2'], 'high': ['e3', 'e4']}


class Mean:
    @staticmethod
    def reduce(values):
        return sum(values) / len(values)


class Max:
    @staticmethod
    def reduce(values):
        return max(values)


class Range:
    @staticmethod
    def reduce(values):
        return max(values) - min(values)


ANALYSERS = {'mean': Mean, 'max': Max, 'range': Range}


@rule(
    outputs={'data': 'data/raw13/{expt}.txt'},
    matrix={'expt': EXPTS},
)
def make_data(outputs, expt):
    seed = EXPTS.index(expt) + 1
    Path(outputs['data']).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs['data']).write_text(','.join(str(seed * i) for i in range(1, 6)))


def stats_rule(analyser_name, group_names):
    """Make the rule `stats_<analyser_name>`, one task per group."""

    @rule(
        name=f'stats_{analyser_name}',
        # Callable inputs use only matrix kwargs (here `group`) and the
        # module-level GROUPS: nothing from this factory's scope.
        inputs=lambda group: {e: f'data/raw13/{e}.txt' for e in GROUPS[group]},
        outputs={'stat': f'data/stats13/{analyser_name}/{{group}}.txt'},
        matrix={'group': list(group_names)},
        depends_on=[make_data],
        uses={'analyser': ANALYSERS[analyser_name], 'groups': GROUPS},
    )
    def run(inputs, outputs, group):
        values = []
        for expt in groups[group]:
            values += [float(x) for x in Path(inputs[expt]).read_text().split(',')]
        Path(outputs['stat']).parent.mkdir(parents=True, exist_ok=True)
        Path(outputs['stat']).write_text(str(analyser.reduce(values)))

    return run


stats_rules = [stats_rule(name, GROUPS) for name in ANALYSERS]


@rule(
    inputs={
        f'{name}_{group}': f'data/stats13/{name}/{group}.txt'
        for name in ANALYSERS
        for group in GROUPS
    },
    outputs={'report': 'data/results/summary13.txt'},
    depends_on=stats_rules,
)
def summary(inputs, outputs):
    lines = [f'{key}: {Path(path).read_text()}' for key, path in inputs.items()]
    Path(outputs['report']).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs['report']).write_text('\n'.join(lines) + '\n')


rmk.add_rules([make_data, *stats_rules, summary])
