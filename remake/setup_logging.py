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
