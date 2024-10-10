import sys
import inspect
import itertools
import math
import traceback
from collections import Counter
from pathlib import Path

from loguru import logger
import networkx as nx
from tabulate import tabulate, SEPARATING_LINE

from pyquerylist import QueryList

from .executors import Executor, SingleprocExecutor, SlurmExecutor, DaskExecutor, MultiprocExecutor
from .metadata import Sqlite3MetadataManager
from .rule import Rule
from .task import Task
from .util import dedent, Config, load_module

logger.remove()
logger.add(sys.stdout, format='<bold><lvl>{message}</lvl></bold>', level='INFO')


def all_descendants(task_dag, tasks):
    task_descendants = set()
    for task in tasks:
        if task in task_descendants:
            continue
        task_descendants |= descendants(task_dag, task)
    return task_descendants


def descendants(task_dag, task):
    return set(nx.bfs_tree(task_dag, task))


def _compare_task_timestamps(t1, t2):
    if t1 is None or t2 is None:
        return False
    else:
        return t1 < t2


def _get_inputs_outputs(inputs_outputs_fn_or_dict, fmt_dict):
    if callable(inputs_outputs_fn_or_dict):
        return inputs_outputs_fn_or_dict(**fmt_dict)
    else:
        return {
            k.format(**fmt_dict): v.format(**fmt_dict) for k, v in inputs_outputs_fn_or_dict.items()
        }


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
        raise Exception(
            f'input_rule/output_rule keys must be strings or tuples of strings:\n' f'{error_str}'
        )
    return new_fmt_dict


class Remake:
    def __init__(self, config, *args, **kwargs):
        self.config = Config(
            {
                'slurm': {},
                'old_style_class': False,
                'content_checks': False,
                'check_inputs_exist': False,
                'check_outputs_exist': False,
                'check_outputs_older_than_inputs': False,
            }
        )
        self.config.update('remake_config', config)
        self.args = args
        self.kwargs = kwargs
        self.rules = []
        self.tasks = QueryList()
        self._inputs = {}
        self._outputs = {}
        self.task_dag = nx.DiGraph()
        self.rule_dg = nx.DiGraph()  # Not nec. a DAG!
        self.task_key_map = {}

        self.metadata_manager = Sqlite3MetadataManager()
        # Different between Python 3.10 and 3.12.
        if sys.version_info[0] == 3 and sys.version_info[1] == 12:
            stack = next(traceback.walk_stack(None))
            frame = stack[0]
            self.full_path = frame.f_locals['module'].__file__
            self.name = Path(self.full_path).name
        elif sys.version_info[0] == 3 and sys.version_info[1] == 10:
            stack = next(traceback.walk_stack(None))
            frame = stack[0]
            self.full_path = frame.f_globals['__file__']
            self.name = Path(self.full_path).name

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
                logger.trace(f'skipping disabled rule: {rule}')
                continue
            self.load_rule(rule)
            setattr(self, rule.__name__, rule)
            self.rule_dg.add_node(rule)

        for task in self.tasks:
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
                    self.rule_dg.add_edge(prev_task.rule, task.rule)

                self._inputs[input_] = task

        assert nx.is_directed_acyclic_graph(self.task_dag), 'Not a dag!'
        # rule_dg is not necessarily a DAG!
        # assert nx.is_directed_acyclic_graph(self.rule_dg), 'Not a dag!'
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

    def load_rule(self, rule):
        logger.debug(f'loading rule: {rule}')
        rule.remake = self
        self.rules.append(rule)
        for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
            assert hasattr(rule, req_method), f'{rule} does not have method {req_method}'
            # if not self.config['old_style_class']:
            #     assert callable(getattr(rule, req_method))
            # assert getattr(rule, req_method).is_rule_dec

        rule.tasks = QueryList()
        if hasattr(rule, 'var_matrix'):
            assert self.config['old_style_class']
            rule_vars = list(itertools.product(*rule.var_matrix.values()))
            rule_matrix_keys = rule.var_matrix.keys()
        elif hasattr(rule, 'rule_matrix'):
            assert not self.config['old_style_class']
            if callable(rule.rule_matrix):
                rule_matrix = rule.rule_matrix()
            else:
                rule_matrix = rule.rule_matrix
            rule_matrix_keys = rule_matrix.keys()
            rule_vars = list(itertools.product(*rule_matrix.values()))
        else:
            rule_matrix_keys = []
            rule_vars = [None]

        for rule_var in rule_vars:
            if rule_var is not None:
                task_kwargs = {k: v for k, v in zip(rule_matrix_keys, rule_var)}
                task_kwargs = _check_modify_fmt_dict(task_kwargs)
            else:
                task_kwargs = {}

            inputs = _get_inputs_outputs(rule.rule_inputs, task_kwargs)
            outputs = _get_inputs_outputs(rule.rule_outputs, task_kwargs)
            task = Task(rule, inputs, outputs, task_kwargs, prev_tasks=[], next_tasks=[])

            # Makes task queryable using QueryList
            for k, v in task_kwargs.items():
                if k == 'kwargs':
                    raise Exception('Cannot have a rule_matrix key called "kwargs"')
                setattr(task, k, v)
            self.task_key_map[task.key()] = task

            rule.tasks.append(task)
            self.tasks.append(task)

            for output in outputs.values():
                if output in self._outputs:
                    raise Exception(output)
                self._outputs[output] = task

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
            for prev_task in task.prev_tasks:
                if prev_task.inputs_missing:
                    task.inputs_missing = True
                    rerun_reasons.append(f'prev_task_input_missing {prev_task}')
                    requires_rerun = False
                if prev_task.requires_rerun and not prev_task.inputs_missing:
                    requires_rerun = True
                    rerun_reasons.append(f'prev_task_requires_rerun {prev_task}')
                if _compare_task_timestamps(task.last_run_timestamp, prev_task.last_run_timestamp):
                    requires_rerun = True
                    rerun_reasons.append(f'prev_task_run_more_recently {prev_task}')

            earliest_output_path_mtime = float('inf')
            latest_input_path_mtime = 0
            if config['check_outputs_older_than_inputs'] or config['check_inputs_exist']:
                for path in task.inputs.values():
                    all_inputs_present = True
                    if not Path(path).exists():
                        if path in self._outputs and self._outputs[path].requires_rerun:
                            pass
                        else:
                            requires_rerun = False
                            rerun_reasons.append(f'input_missing {path}')
                            task.inputs_missing = True
                        all_inputs_present = False
                    else:
                        latest_input_path_mtime = max(
                            latest_input_path_mtime, Path(path).lstat().st_mtime
                        )
                    if all_inputs_present:
                        task.inputs_missing = False
                        rerun_reasons = [r for r in rerun_reasons if not r.startswith('prev_task_input_missing')]

            if config['check_outputs_older_than_inputs'] or config['check_outputs_exist']:
                for path in task.outputs.values():
                    if not Path(path).exists():
                        requires_rerun = True
                        rerun_reasons.append(f'output_missing {path}')
                    else:
                        earliest_output_path_mtime = min(
                            earliest_output_path_mtime, Path(path).lstat().st_mtime
                        )

            if config['check_outputs_older_than_inputs']:
                if latest_input_path_mtime > earliest_output_path_mtime:
                    requires_rerun = True
                    rerun_reasons.append('input_is_older_than_output')

            if not self.metadata_manager.code_comparer(
                task.last_run_code, task.rule.source['rule_run']
            ):
                requires_rerun = True
                rerun_reasons.append('task_run_source_changed')

            task.requires_rerun = requires_rerun
            task.rerun_reasons = rerun_reasons

    def finalize(self):
        logger.debug('finalize')
        self.metadata_manager.get_or_create_tasks_metadata(self.topo_tasks)
        logger.debug('getting task status')
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

    def show_task_reasons(self, task):
        for reason in task.rerun_reasons:
            logger.info(f'  -- {reason}')

    def show_task_failure(self, task):
        logger.error('==>  FAILURE TRACEBACK  <==')
        logger.error(task.last_run_exception)
        logger.error('==>END FAILURE TRACEBACK<==')

    def show_task_code_diff(self, task, diffs=None):
        if not diffs:
            diffs = {}
        diff_lines = task.diff()
        diff = '\n'.join(diff_lines)
        if diff in diffs:
            prev_task_with_same_diff = diffs[diff]
            logger.info('==>  DIFF  <==')
            logger.info(f'Diff of {task} the same as {prev_task_with_same_diff}')
            logger.info('==>END DIFF<==')
            return diffs
        diffs[diff] = task
        try:
            logger.level('DIFF_ADDED')
        except ValueError:
            logger.level('DIFF_ADDED', no=42, color='<green>')
            logger.level('DIFF_REMOVED', no=43, color='<red>')
            logger.level('DIFF_Q', no=44, color='<yellow>')
        logger.info('==>  DIFF  <==')
        for line in task.diff():
            if line and line[0] == '-':
                logger.log('DIFF_REMOVED', line)
            elif line and line[0] == '+':
                logger.log('DIFF_ADDED', line)
            elif line and line[0] == '?':
                logger.log('DIFF_Q', line)
            else:
                logger.info(line)
        logger.info('==>END DIFF<==')
        return diffs

    def info(self, query, show_failures, show_reasons, show_task_code_diff, short, rule):
        # print(rmk.name)
        status_map = {
            0: 'R',
            1: 'C',
            2: 'RF',
        }
        status_loggers = {
            'R': 'RERUN',
            'C': 'COMPLETE',
            'RF': 'FAILED',
            'XR': 'FAILED',
            'XC': 'FAILED',
            'XRF': 'FAILED',
        }
        try:
            logger.level('RERUN')
        except ValueError:
            logger.level('RERUN', no=45, color='<blue>')
            logger.level('COMPLETE', no=46, color='<green>')
            logger.level('FAILED', no=47, color='<red>')

        counter = Counter()
        logger.info(f'==> {self.name} <==')
        if query and not rule:
            logger.info(f'Filter on: {query}')
            filtered_tasks = self.topo_tasks.where(query)
        else:
            filtered_tasks = self.topo_tasks

        for task in filtered_tasks:
            status = status_map[task.last_run_status]
            if task.requires_rerun and 'R' not in status:
                status = 'R'
            if task.inputs_missing:
                status = 'X' + status
            task.status = status
            counter[status] += 1

        status_keys = ['C', 'R', 'RF', 'XC', 'XR', 'XRF']
        if short:
            for k in status_keys:
                level = status_loggers[k]
                logger.log(level, f'{k:<3}: {counter.get(k, 0)}')
            return

        if rule:
            rows = []
            row = ['name', 'ntasks', *status_keys]
            rows.append(row)
            rows.append(SEPARATING_LINE)

            statuses = []
            for rule in self.rules:
                max_status = 0
                rule_counter = Counter()
                for task in rule.tasks:
                    rule_counter[task.status] += 1
                    max_status = max(status_keys.index(task.status), max_status)
                statuses.append(max_status)
                row = [
                    rule.__name__,
                    len(rule.tasks),
                    *[rule_counter.get(k, 0) for k in status_keys],
                ]
                rows.append(row)
            rows.append(SEPARATING_LINE)
            row = ['Total', len(self.tasks), *[counter.get(k, 0) for k in status_keys]]
            rows.append(row)
            lines = tabulate(rows).split('\n')
            for line in lines[:3]:
                logger.info(line)
            for status, line in zip(statuses, lines[3:-3]):
                level = status_loggers[status_keys[status]]
                logger.log(level, line)
            logger.info(lines[-3])
            level = status_loggers[status_keys[max(statuses)]]
            logger.log(level, lines[-2])
            logger.info(lines[-1])
            return

        diffs = {}
        for task in filtered_tasks:
            level = status_loggers[task.status]
            logger.log(level, f'{task.status:<2s} {task}')
            if 'F' in task.status and show_failures:
                self.show_task_failure(task)
            if ('R' in task.status or 'X' in task.status) and show_reasons:
                self.show_task_reasons(task)
            if show_task_code_diff and 'task_run_source_changed' in task.rerun_reasons:
                diffs = self.show_task_code_diff(task, diffs)

    def touch(self, input_files=True, all_files=False):
        if all_files:
            tasks = self.topo_tasks
        elif input_files:
            tasks = self.input_tasks

        io_dirs = set()
        topo_paths = []
        for task in tasks:
            for input_file in task.inputs.values():
                ipath = Path(input_file)
                io_dirs.add(ipath.parent)
                topo_paths.append(ipath)
            if all_files:
                for output_file in task.outputs.values():
                    opath = Path(output_file)
                    io_dirs.add(opath.parent)
                    topo_paths.append(opath)
        for d in io_dirs:
            d.mkdir(exist_ok=True, parents=True)

        for path in topo_paths:
            path.touch()

    def run(
        self,
        executor='SingleprocExecutor',
        query='',
        force=False,
        show_reasons=False,
        show_task_code_diff=False,
        stdout_to_log=False,
    ):
        if query:
            tasks = self.topo_tasks.where(query)
        else:
            tasks = self.topo_tasks

        if not force:
            rerun_tasks = [t for t in tasks if t.requires_rerun and not t.inputs_missing]
        else:
            rerun_tasks = tasks
            for task in rerun_tasks:
                task.rerun_reasons.insert(0, 'force_rerun')

        logger.info(f'==> {self.name} <==')
        if not rerun_tasks:
            logger.info('No tasks require rerun')
            return
        logger.info(f'Running {len(rerun_tasks)} tasks using {executor}')
        executor = self._get_executor(executor)
        logger.debug(f'using {executor}')
        executor.run_tasks(
            rerun_tasks,
            show_reasons=show_reasons,
            show_task_code_diff=show_task_code_diff,
            stdout_to_log=stdout_to_log,
        )

    def run_tasks_from_keys(self, task_keys, executor='SingleprocExecutor'):
        tasks = [self.task_key_map[task_key] for task_key in task_keys]
        logger.info(f'Running {len(tasks)} tasks')
        executor = self._get_executor(executor)
        logger.debug(f'using {executor}')
        executor.run_tasks(tasks)
