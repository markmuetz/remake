import inspect
import itertools
import math
import traceback

from loguru import logger
import networkx as nx

from remake.util import format_path

from .code_compare import dedent
from .executor import Executor, SingleprocExecutor, SlurmExecutor
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
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.rules = []
        self.tasks = []
        self._inputs = {}
        self._outputs = {}
        self.task_dag = nx.DiGraph()
        self.metadata_manager = Sqlite3MetadataManager()
        stack = next(traceback.walk_stack(None))
        frame = stack[0]
        self.name = frame.f_globals['__file__']
        self.task_key_map = {}

    def autoload_rules(self, finalize=True):
        stack = next(traceback.walk_stack(None))
        frame = stack[0]
        rules = []
        for varname, var in frame.f_locals.items():
            if isinstance(var, type) and issubclass(var, Rule) and not var is Rule:
                rules.append(var)
        return self.load_rules(rules)

    def load_rules(self, rules, finalize=True):
        logger.debug('Loading rules')
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

        logger.debug('inserting rules')
        for rule in self.rules:
            rule.source = {}
            for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
                method = getattr(rule, req_method)
                if callable(method):
                    rule.source[req_method] = dedent(inspect.getsource(method))
                else:
                    rule.source[req_method] = ''
            self.metadata_manager.get_or_create_rule_metadata(rule)

        if finalize:
            self.finalize()
        logger.debug('Loaded rules')
        return self

    @staticmethod
    def _check_modify_fmt_dict(fmt_dict):
        errors = []
        new_fmt_dict = {**fmt_dict}
        for key, value in fmt_dict.items():
            if isinstance(key, str):
                continue
            elif isinstance(key, tuple):
                if not len(key) == len(value):
                    errors.append(('length mismatch', key, value))
                for kk, vv in zip(key, value):
                    if not isinstance(kk, str):
                        errors.append(('not tuple of strings', key, value))
                        break
                    else:
                        new_fmt_dict[kk] = vv
                new_fmt_dict.pop(key)
            else:
                errors.append(('not string', key, value))
        if errors:
            error_str = '\n'.join([f'  {msg}: {k}, {v}' for msg, k, v in errors])
            raise Exception(f'input_rule/output_rule keys must be strings or tuples of strings:\n'
                            f'{error_str}')
        return new_fmt_dict

    def load_rule(self, rule):
        rule.remake = self
        self.rules.append(rule)
        for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
            assert hasattr(rule, req_method)
            # assert getattr(rule, req_method).is_rule_dec

        rule.tasks = []
        if hasattr(rule, 'var_matrix'):
            rule_vars = list(itertools.product(*rule.var_matrix.values()))

            for rule_var in rule_vars:
                task_kwargs = {k: v for k, v in zip(rule.var_matrix.keys(), rule_var)}
                task_kwargs = self._check_modify_fmt_dict(task_kwargs)

                inputs = self.get_inputs_outputs(rule.rule_inputs, task_kwargs)
                outputs = self.get_inputs_outputs(rule.rule_outputs, task_kwargs)
                task = Task(rule, inputs, outputs, task_kwargs, [], [])
                self.task_key_map[task.key()] = task

                rule.tasks.append(task)
                self.tasks.append(task)

                for output in outputs.values():
                    if output in self._outputs:
                        raise Exception(output)
                    self._outputs[output] = task
        else:
            inputs = self.get_inputs_outputs(rule.rule_inputs, {})
            outputs = self.get_inputs_outputs(rule.rule_outputs, {})
            task = Task(rule, inputs, outputs, {}, [], [])
            self.task_key_map[task.key()] = task

            rule.tasks.append(task)
            self.tasks.append(task)

            for output in outputs.values():
                if output in self._outputs:
                    raise Exception(output)
                self._outputs[output] = task

    def get_inputs_outputs(self, inputs_outputs_fn_or_dict, fmt_dict):
        if callable(inputs_outputs_fn_or_dict):
            return inputs_outputs_fn_or_dict(**fmt_dict)
        else:
            return {k.format(**fmt_dict): format_path(v, **fmt_dict)
                    for k, v in inputs_outputs_fn_or_dict.items()}

    def finalize(self):
        logger.debug('getting task status')
        self.metadata_manager.tasks_requires_rerun(self.topo_tasks)

        descendant_tasks = all_descendants(self.task_dag, [t for t in self.topo_tasks if t.requires_rerun])
        for descendant_task in descendant_tasks:
            descendant_task.requires_rerun = True

        self.metadata_manager.update_tasks([t for t in self.topo_tasks if t.requires_rerun], True)

    def update_task(self, task):
        if task.is_run:
            self.metadata_manager.update_task_metadata(task)

    def _get_executor(self, executor):
        if isinstance(executor, str):
            if executor == 'SingleprocExecutor':
                executor = SingleprocExecutor(self)
            elif executor == 'SlurmExecutor':
                executor = SlurmExecutor(self)
            else:
                raise ValueError(f'{executor} not a valid executor')
        elif not isinstance(executor, Executor):
            raise ValueError(f'{executor} must be a string or a subtype of Executor')
        return executor

    def run(self, executor='SingleprocExecutor'):
        rerun_tasks = [t for t in self.topo_tasks if t.requires_rerun]
        if not rerun_tasks:
            logger.info('No tasks require rerun')
            return
        logger.info(f'Running {len(rerun_tasks)} tasks')
        executor = self._get_executor(executor)
        logger.debug(f'using {executor}')
        executor.run_tasks(rerun_tasks)

    def run_tasks_from_keys(self, task_keys, executor='SingleprocExecutor'):
        tasks = [self.task_key_map[task_key] for task_key in task_keys]
        logger.info(f'Running {len(tasks)} tasks')
        executor = self._get_executor(executor)
        logger.debug(f'using {executor}')
        executor.run_tasks(tasks)

