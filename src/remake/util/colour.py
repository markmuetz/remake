"""Tiny ANSI colour helper for CLI output.

Deliberately stdlib-only (no rich/colorama): the vocabulary remake needs to
colour is tiny (a handful of task statuses), so a ~1-screen module beats a
dependency. loguru already colours stderr; this is for stdout (command output),
where colour must stay opt-out-able so `--json` and pipes remain clean.

Alignment caveat: ANSI escapes have zero display width but non-zero len(), so
always pad/width-calc on the *uncoloured* text and apply colour last.
"""
import os
import sys

_CODES = {
    'green': 32,
    'red': 31,
    'yellow': 33,
    'cyan': 36,
    'dim': 2,
    'bold': 1,
}

# Per-status styling shared by every command that renders a task status.
STATUS_STYLE = {
    'success': ('green',),
    'failed': ('red', 'bold'),
    'pending': ('yellow',),
}


def _auto_enabled(stream):
    # NO_COLOR (https://no-color.org) wins; FORCE_COLOR overrides a non-TTY
    # (handy for CI logs / `less -R`); otherwise colour only on a real terminal.
    if os.environ.get('NO_COLOR'):
        return False
    if os.environ.get('FORCE_COLOR'):
        return True
    return hasattr(stream, 'isatty') and stream.isatty()


class Painter:
    """Callable that wraps text in ANSI styles when colour is enabled.

    mode: 'auto' (TTY-detect, the default), 'always', or 'never'.
    """

    def __init__(self, mode='auto', stream=None):
        stream = stream if stream is not None else sys.stdout
        if mode == 'always':
            self.enabled = True
        elif mode == 'never':
            self.enabled = False
        else:
            self.enabled = _auto_enabled(stream)

    def __call__(self, text, *styles):
        if not self.enabled or not styles:
            return text
        codes = ';'.join(str(_CODES[s]) for s in styles)
        return f'\033[{codes}m{text}\033[0m'

    def status(self, name, text=None):
        """Colour a status token (text defaults to the status name itself)."""
        return self(name if text is None else text, *STATUS_STYLE.get(name, ()))
