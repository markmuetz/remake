import sys
import logging
from pathlib import Path

from remake.bcolors import bcolors


# Thanks: # http://stackoverflow.com/a/8349076/54557
class ColourConsoleFormatter(logging.Formatter):
    """Format messages in colour based on their level"""
    colour_start = {
        'DEBUG': bcolors.OKGREEN,
        'INFO': bcolors.OKBLUE,
        'WARNING': bcolors.WARNING + bcolors.BOLD,
        'ERROR': bcolors.FAIL + bcolors.BOLD,
    }

    def __init__(self, **kwargs):
        logging.Formatter.__init__(self, fmt='%(levelname)s: %(msg)s')
        self.fmts = {
            getattr(logging, level): self.colour_start[level] + fmt + bcolors.ENDC
            for level, fmt in kwargs.items()
        }

    def format(self, record):
        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._fmt
        # Fix colour formatting for new versions of logging.
        if hasattr(self, '_style'):
            style_format_orig = self._style._fmt

        # Replace the original format with one customized by logging level
        self._fmt = self.fmts[record.levelno]

        if hasattr(self, '_style'):
            self._style._fmt = self._fmt
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._fmt = format_orig
        if hasattr(self, '_style'):
            self._style._fmt = style_format_orig

        return result


def setup_stdout_logging(level='INFO', colour=True, detailed=False):
    remake_root = logging.getLogger('remake')
    if getattr(remake_root, 'is_setup_stream_logging', False):
        return
    handler = logging.StreamHandler(sys.stdout)
    if colour and detailed:
        raise ValueError('Only one of colour and detailed can be chosen')
    if colour:
        if level == 'DEBUG':
            formatter = ColourConsoleFormatter(
                DEBUG='%(name)-40s %(levelname)-8s: %(message)s',
                INFO='%(name)-40s %(levelname)-8s: %(message)s',
                WARNING='%(name)-40s %(levelname)-8s: %(message)s',
                ERROR='%(name)-40s %(levelname)-8s: %(message)s',
            )
        else:
            formatter = ColourConsoleFormatter(
                DEBUG='%(asctime)s %(name)-40s %(levelname)-8s: %(message)s',
                INFO='%(message)s',
                WARNING='WARNING: %(message)s',
                ERROR='ERROR: %(message)s',
            )
    elif detailed:
        formatter = logging.Formatter('%(asctime)s %(name)-40s %(levelname)-8s %(message)s')
    else:
        formatter = logging.Formatter('%(levelname)-8s: %(message)s')
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

