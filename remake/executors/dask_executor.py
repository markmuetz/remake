import os
from pathlib import Path

from loguru import logger

from ..loader import load_remake

from .executor import Executor

def dask_task_run(remake_path, task_key, *input_task_keys):
    print(f'dask_task_run: {remake_path} {task_key} {input_task_keys}')
    cwd = os.getcwd()
    remake_path = Path(remake_path)
    os.chdir(remake_path.parent)
    rmk = load_remake(remake_path.name, finalize=False)
    rmk.run_tasks_from_keys([task_key], 'SingleprocExecutor')
    os.chdir(cwd)

    return task_key

def complete(*input_task_keys):
    print(f'complete: {input_task_keys}')


class DaskExecutor(Executor):
    def __init__(self, rmk, dask_config=None):
        if not dask_config:
            dask_config = {}
        self.dask_config = dask_config
        super().__init__(rmk)

    def run_tasks(self, rerun_tasks):
        import dask
        from dask.distributed import LocalCluster

        # raise
        remake_path = Path(self.rmk.full_path).absolute()
        print(remake_path)

        # remake_path = Path.cwd() / f'{self.rmk.name}.py'
        # remake_path = Path('/home/markmuetz/projects/remake2_examples/ex1/') / 'ex1.py'

        dask_task_graph = {}
        rules = {t: t.rule for t in rerun_tasks}
        for task in rerun_tasks:
            input_task_keys = [t.key() for t in task.prev_tasks]
            dask_task_graph[task.key()] = (dask_task_run, str(remake_path), task.key(), *input_task_keys)

        dask_task_graph['complete'] = (complete, *[t.key() for t in rerun_tasks])
        for k, v in dask_task_graph.items():
            logger.trace(k, v)

        if 'cluster' in self.dask_config:
            cluster = self.dast_config['cluster']
        else:
            cluster = LocalCluster(**self.dask_config)          # Fully-featured local Dask cluster
        if 'client' in self.dask_config:
            client = self.dast_config['client']
        else:
            client = cluster.get_client()
        client.get(dask_task_graph, 'complete')

