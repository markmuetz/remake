import sys
import inspect
import itertools
import math
import traceback
from pathlib import Path

from loguru import logger
import networkx as nx

from pyquerylist import QueryList

# from remake.util import format_path

from .code_compare import dedent
from .config import Config
from .executors import Executor, SingleprocExecutor, SlurmExecutor, DaskExecutor, MultiprocExecutor
from .sqlite3_metadata_manager import Sqlite3MetadataManager
from .rule import Rule
from .task import Task

logger.remove()
logger.add(sys.stdout, format='<bold>{message}</bold>', level='INFO')


def all_descendants(task_dag, tasks):
    task_descendants = set()
    for task in tasks:
        if task in task_descendants:
            continue
        task_descendants |= descendants(task_dag, task)
    return task_descendants


def descendants(task_dag, task):
    return set(nx.bfs_tree(task_dag, task))


def compare_task_timestamps(t1, t2):
    if t1 is None or t2 is None:
        return False
    else:
        return t1 < t2


class Remake:
    def __init__(self, config, *args, **kwargs):
        self.config = Config({
            'slurm': {},
            'old_style_class': False,
            'content_checks': False,
            'check_inputs_exist': False,
            'check_outputs_exist': False,
            'check_outputs_older_than_inputs': False,
        })
        self.config.update('remake_config', config)
        self.args = args
        self.kwargs = kwargs
        self.rules = []
        self.tasks = []
        self._inputs = {}
        self._outputs = {}
        self.task_dag = nx.DiGraph()
        self.rule_dag = nx.DiGraph()
        self.metadata_manager = Sqlite3MetadataManager()
        # Different between Python 3.10 and 3.12.
        if True:
            stack = next(traceback.walk_stack(None))
            frame = stack[0]
            self.full_path = frame.f_locals['module'].__file__
            self.name = Path(self.full_path).name
        else:
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
            if hasattr(rule, 'enabled') and not rule.enabled:
                continue
            self.load_rule(rule)
            setattr(self, rule.__name__, rule)
            self.rule_dag.add_node(rule)

        for task in self.tasks:
            # import ipdb; ipdb.set_trace()
            self.task_dag.add_node(task)
            input_tasks = []
            for input_ in task.inputs.values():
                if input_ in self._outputs:
                    prev_task = self._outputs[input_]
                    if prev_task not in task.prev_tasks:
                        task.prev_tasks.append(prev_task)
                    if task not in prev_task.next_tasks:
                        prev_task.next_tasks.append(task)

                    self.task_dag.add_edge(prev_task, task)
                    self.rule_dag.add_edge(prev_task.rule, task.rule)

                self._inputs[input_] = task

        assert nx.is_directed_acyclic_graph(self.task_dag), 'Not a dag!'
        assert nx.is_directed_acyclic_graph(self.rule_dag), 'Not a dag!'
        self.input_tasks = [v for v, d in self.task_dag.in_degree() if d == 0]
        self.topo_tasks = QueryList(nx.topological_sort(self.task_dag))

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
        logger.debug(f'loading rule: {rule}')
        rule.remake = self
        self.rules.append(rule)
        for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
            assert hasattr(rule, req_method), f'{rule} does not have method {req_method}'
            if not self.config['old_style_class']:
                assert callable(getattr(rule, req_method))
            # assert getattr(rule, req_method).is_rule_dec

        rule.tasks = QueryList()
        if hasattr(rule, 'var_matrix'):
            assert self.config['old_style_class']
            rule_vars = list(itertools.product(*rule.var_matrix.values()))
            rule_matrix_keys = rule.var_matrix.keys()
        elif hasattr(rule, 'rule_matrix'):
            assert not self.config['old_style_class']
            rule_matrix = rule.rule_matrix()
            rule_vars = list(itertools.product(*rule_matrix.values()))
            rule_matrix_keys = rule_matrix.keys()
        else:
            rule_vars = [None]
            rule_matrix_keys = []

            # inputs = self._get_inputs_outputs(rule.rule_inputs, {})
            # outputs = self._get_inputs_outputs(rule.rule_outputs, {})
            # task = Task(rule, inputs, outputs, {}, [], [])
            # self.task_key_map[task.key()] = task

            # rule.tasks.append(task)
            # self.tasks.append(task)

            # for output in outputs.values():
            #     if output in self._outputs:
            #         raise Exception(output)
            #     self._outputs[output] = task

        for rule_var in rule_vars:
            if rule_var is not None:
                task_kwargs = {k: v for k, v in zip(rule_matrix_keys, rule_var)}
                task_kwargs = self._check_modify_fmt_dict(task_kwargs)
            else:
                task_kwargs = {}

            inputs = self._get_inputs_outputs(rule.rule_inputs, task_kwargs)
            outputs = self._get_inputs_outputs(rule.rule_outputs, task_kwargs)
            task = Task(rule, inputs, outputs, task_kwargs, [], [])
            task.remake = self
            for k, v in task_kwargs.items():
                setattr(task, k, v)
            self.task_key_map[task.key()] = task

            rule.tasks.append(task)
            self.tasks.append(task)

            for output in outputs.values():
                if output in self._outputs:
                    raise Exception(output)
                self._outputs[output] = task

    def _get_inputs_outputs(self, inputs_outputs_fn_or_dict, fmt_dict):
        if callable(inputs_outputs_fn_or_dict):
            return inputs_outputs_fn_or_dict(**fmt_dict)
        else:
            return {k.format(**fmt_dict): v.format(**fmt_dict)
                    for k, v in inputs_outputs_fn_or_dict.items()}
            # return {k.format(**fmt_dict): format_path(v, **fmt_dict)
            #         for k, v in inputs_outputs_fn_or_dict.items()}

    def _set_task_statuses(self, tasks):
        for task in tasks:
            if hasattr(task.rule, 'config'):
                config = self.config.copy()
                config.update(str(task), task.rule.config)
            else:
                config = self.config

            requires_rerun = False
            rerun_reasons = []
            if task.last_run_status == 0:
                requires_rerun = True
                rerun_reasons.append('task_not_run')
            elif task.last_run_status == 2:
                # Note, we do not know *why* the task failed.
                # It could have nothing to do with the Python code, e.g. out of memory/time etc.
                # Mark as requires_rerun so that if these have been fixed, the task can be rerun.
                requires_rerun = True
                rerun_reasons.append('task_failed')

            prev_task_requires_rerun = False
            for prev_task in task.prev_tasks:
                if prev_task.inputs_missing:
                    task.inputs_missing = True
                    rerun_reasons.append(f'prev_task_input_missing {prev_task}')
                if prev_task.requires_rerun:
                    prev_task_requires_rerun = True
                    requires_rerun = True
                    rerun_reasons.append(f'prev_task_requires_rerun {prev_task}')
                if compare_task_timestamps(task.last_run_timestamp, prev_task.last_run_timestamp):
                    requires_rerun = True
                    rerun_reasons.append(f'prev_task_run_more_recently {prev_task}')

            earliest_output_path_mtime = float('inf')
            latest_input_path_mtime = 0
            if config['check_outputs_older_than_inputs'] or config['check_inputs_exist']:
                for path in task.inputs.values():
                    if not Path(path).exists():
                        if path in self._outputs and self._outputs[path].requires_rerun:
                            pass
                        else:
                            requires_rerun = False
                            rerun_reasons.append(f'input_missing {path}')
                            task.inputs_missing = True
                    else:
                        latest_input_path_mtime = max(latest_input_path_mtime, Path(path).lstat().st_mtime)

            if config['check_outputs_older_than_inputs'] or config['check_outputs_exist']:
                for path in task.outputs.values():
                    if not Path(path).exists():
                        requires_rerun = True
                        rerun_reasons.append(f'output_missing {path}')
                    else:
                        earliest_output_path_mtime = min(earliest_output_path_mtime, Path(path).lstat().st_mtime)

            if config['check_outputs_older_than_inputs']:
                if latest_input_path_mtime > earliest_output_path_mtime:
                    requires_rerun = True
                    rerun_reasons.append('input_is_older_than_output')

            if not self.metadata_manager.code_comparer(task.last_run_code, task.rule.source['rule_run']):
                requires_rerun = True
                rerun_reasons.append('task_run_source_changed')
            task.requires_rerun = requires_rerun
            task.rerun_reasons = rerun_reasons

    def finalize(self):
        logger.debug('getting task status')
        self.metadata_manager.get_or_create_tasks_metadata(self.topo_tasks)
        self._set_task_statuses(self.topo_tasks)

    def update_task(self, task, exception=''):
        self.metadata_manager.update_task_metadata(task, exception)

    def _get_executor(self, executor):
        if isinstance(executor, str):
            if executor == 'SingleprocExecutor':
                executor = SingleprocExecutor(self)
            elif executor == 'SlurmExecutor':
                executor = SlurmExecutor(self, self.config.get('slurm', {}))
            elif executor == 'DaskExecutor':
                executor = DaskExecutor(self, self.config.get('dask', {}))
            elif executor == 'MultiprocExecutor':
                executor = MultiprocExecutor(self, self.config.get('multiproc', {}))
            else:
                raise ValueError(f'{executor} not a valid executor')
        elif not isinstance(executor, Executor):
            raise ValueError(f'{executor} must be a string or a subtype of Executor')
        return executor

    def run(self, executor='SingleprocExecutor', query='', force=False):
        if query:
            tasks = self.topo_tasks.where(query[0])
        else:
            tasks = self.topo_tasks

        if not force:
            rerun_tasks = [t for t in tasks if t.requires_rerun]
        else:
            rerun_tasks = tasks

        if not rerun_tasks:
            logger.info('No tasks require rerun')
            return
        logger.info(f'Running {len(rerun_tasks)} tasks using {executor}')
        executor = self._get_executor(executor)
        logger.debug(f'using {executor}')
        executor.run_tasks(rerun_tasks)

    def run_tasks_from_keys(self, task_keys, executor='SingleprocExecutor'):
        tasks = [self.task_key_map[task_key] for task_key in task_keys]
        logger.info(f'Running {len(tasks)} tasks')
        executor = self._get_executor(executor)
        logger.debug(f'using {executor}')
        executor.run_tasks(tasks)

