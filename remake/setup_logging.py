import logging


def setup_stream_logging(level):
    remake_root = logging.getLogger('remake')
    if getattr(remake_root, 'is_setup_stream_logging', False):
        return
    h = logging.StreamHandler()
    f = logging.Formatter('%(asctime)s %(processName)-15s %(name)-40s %(levelname)-8s %(message)s')
    remake_root.setLevel(level)
    h.setFormatter(f)
    remake_root.addHandler(h)
    setattr(remake_root, 'is_setup_stream_logging', True)


def add_file_logging(log_path):
    remake_root = logging.getLogger('remake')
    remake_root.setLevel(logging.DEBUG)

    remake_root.debug(f'Adding file handler {log_path}')
    file_formatter = logging.Formatter('%(asctime)s %(processName)-15s %(name)-40s %(levelname)-8s %(message)s')
    fileHandler = logging.FileHandler(str(log_path.absolute()), mode='a')
    fileHandler.setFormatter(file_formatter)
    fileHandler.setLevel(logging.DEBUG)

    remake_root.addHandler(fileHandler)


def remove_file_logging(log_path):
    remake_root = logging.getLogger('remake')
    handlers = [h for h in remake_root.handlers
                if isinstance(h, logging.FileHandler) and h.baseFilename == str(log_path.absolute())]
    assert len(handlers) == 1
    handler = handlers[0]
    remake_root.handlers.remove(handler)
    remake_root.debug(f'Removed file handler {log_path}')

