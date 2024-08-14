import inspect
import itertools
import math
import traceback

from loguru import logger
import networkx as nx

from .code_compare import dedent
from .sqlite3_metadata_manager import Sqlite3MetadataManager
from .rule import Rule
from .task import Task

def all_descendants(task_dag, tasks):
    task_descendants = set()
    for task in tasks:
        if task in task_descendants:
            continue
        task_descendants |= descendants(task_dag, task)
    return task_descendants


def descendants(task_dag, task):
    return set(nx.bfs_tree(task_dag, task))


class Remake:
    def __init__(self):
        self.rules = []
        self.tasks = []
        self._inputs = {}
        self._outputs = {}
        self.task_dag = nx.DiGraph()
        self.metadata_manager = Sqlite3MetadataManager()

    def autoload_rules(self):
        stack = next(traceback.walk_stack(None))
        frame = stack[0]
        rules = []
        for varname, var in frame.f_locals.items():
            if isinstance(var, type) and issubclass(var, Rule) and not var is Rule:
                rules.append(var)
        return self.load_rules(rules)

    def load_rules(self, rules):
        logger.info('Loading rules')
        for rule in rules:
            self.load_rule(rule)
            setattr(self, rule.__name__, rule)

        for task in self.tasks:
            # import ipdb; ipdb.set_trace()
            input_tasks = []
            for input_ in task.inputs.values():
                if input_ in self._outputs:
                    prev_task = self._outputs[input_]
                    task.prev_tasks.append(prev_task)
                    prev_task.next_tasks.append(task)

                    self.task_dag.add_edge(prev_task, task)

                self._inputs[input_] = task

        assert nx.is_directed_acyclic_graph(self.task_dag), 'Not a dag!'
        self.input_tasks = [v for v, d in self.task_dag.in_degree() if d == 0]
        self.topo_tasks = list(nx.topological_sort(self.task_dag))
        self.finalize()
        logger.info('Loaded rules')
        return self

    def load_rule(self, rule):
        rule.remake = self
        self.rules.append(rule)
        assert hasattr(rule, 'var_matrix')

        for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
            assert hasattr(rule, req_method)
            # assert getattr(rule, req_method).is_rule_dec

        rule_vars = list(itertools.product(*rule.var_matrix.values()))
        rule.tasks = []

        for rule_var in rule_vars:
            task_kwargs = {k: v for k, v in zip(rule.var_matrix.keys(), rule_var)}

            inputs = rule.rule_inputs(**task_kwargs)
            outputs = rule.rule_outputs(**task_kwargs)
            task = Task(rule, inputs, outputs, task_kwargs, [], [])

            rule.tasks.append(task)
            self.tasks.append(task)

            for output in outputs.values():
                if output in self._outputs:
                    raise Exception(output)
                self._outputs[output] = task

    def finalize(self):
        logger.debug('inserting rules')
        for rule in self.rules:
            rule.source = {}
            for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
                rule.source[req_method] = dedent(inspect.getsource(getattr(rule, req_method)))
            self.metadata_manager.get_or_create_rule_metadata(rule)

        logger.debug('getting task status')

        self.metadata_manager.tasks_requires_rerun(self.topo_tasks)

        descendant_tasks = all_descendants(self.task_dag, [t for t in self.topo_tasks if t.requires_rerun])
        for descendant_task in descendant_tasks:
            descendant_task.requires_rerun = True

        self.metadata_manager.update_tasks([t for t in self.topo_tasks if t.requires_rerun], True)


    def update_task(self, task):
        if task.is_run:
            self.metadata_manager.update_task_metadata(task)

    def run(self):
        rerun_tasks = [t for t in self.topo_tasks if t.requires_rerun]
        if not rerun_tasks:
            return
        ntasks = len(rerun_tasks)
        ndigits = math.floor(math.log10(ntasks)) + 1
        for i, task in enumerate(rerun_tasks):
            print(f'{i + 1:>{ndigits}}/{ntasks}: {task}')
            task.run()





