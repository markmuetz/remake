import sys
import logging
from pathlib import Path


def setup_stdout_logging(level='INFO'):
    remake_root = logging.getLogger('remake')
    if getattr(remake_root, 'is_setup_stream_logging', False):
        return
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s %(name)-30s %(levelname)-8s %(message)s')
    remake_root.setLevel(level)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    remake_root.addHandler(handler)
    setattr(remake_root, 'is_setup_stream_logging', True)


def add_file_logging(log_path, level='INFO'):
    log_path = Path(log_path)
    remake_root = logging.getLogger('remake')
    remake_root.setLevel(level)

    remake_root.debug(f'Adding file handler {log_path}')
    formatter = logging.Formatter('%(asctime)s %(processName)-15s %(name)-40s %(levelname)-8s %(message)s')
    handler = logging.FileHandler(str(log_path.absolute()), mode='a')
    handler.setFormatter(formatter)
    handler.setLevel(level)

    remake_root.addHandler(handler)


def remove_file_logging(log_path):
    log_path = Path(log_path)
    remake_root = logging.getLogger('remake')
    handlers = [h for h in remake_root.handlers
                if isinstance(h, logging.FileHandler) and h.baseFilename == str(log_path.absolute())]
    assert len(handlers) == 1
    handler = handlers[0]
    remake_root.handlers.remove(handler)
    remake_root.debug(f'Removed file handler {log_path}')

