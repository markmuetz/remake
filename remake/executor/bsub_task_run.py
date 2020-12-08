import sys
from hashlib import sha1
from pathlib import Path

from cosmic.util import load_module

from remake.setup_logging import setup_stdout_logging


def main(config_filename, task_path_hash_key, config_path_hash):
    # Logging to stdout is fine -- it will end up in the output captured by bsub.
    setup_stdout_logging('DEBUG')

    config_path = Path(config_filename).absolute()
    curr_config_path_hash = sha1(config_path.read_bytes()).hexdigest()
    if config_path_hash != curr_config_path_hash:
        raise Exception(f'config file {config_path} has changed -- cannot run task.')

    config = load_module(config_filename)
    task_ctrl = config.gen_task_ctrl()
    assert not task_ctrl.finalized, f'task control {task_ctrl} already finalized'
    # TODO: Problmatic. Can potentially read metadata that is being written.
    # This is because another task could be finishing, and writing its output's metadata
    # when this is called, and finalize can be trying to read it at the same time.
    # Can perhaps fix if instead Task is responsible for working out if rerun needed,
    # and removing finalize here.
    task_ctrl.finalize()
    task = task_ctrl.task_from_path_hash_key[task_path_hash_key]
    task_ctrl.run_task(task)


if __name__ == '__main__':
    print(sys.argv)
    main(sys.argv[1], sys.argv[2], sys.argv[3])
