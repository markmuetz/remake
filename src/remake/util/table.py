"""One table renderer for every multi-row CLI output (0.11 item 8a).

Header row, aligned columns (numbers right-aligned), an optional TOTAL row,
colour applied after padding (so ANSI codes never skew the alignment), and,
when the terminal width is known, the widest text column shortened with `…`
instead of wrapping.
"""
import os
import shutil
import sys

ELLIPSIS = '…'


def terminal_width(stream=None):
    """The width to fit output to: $COLUMNS if set, else the terminal's when
    `stream` (default stdout) is a TTY, else None (a pipe or file: no limit)."""
    cols = os.environ.get('COLUMNS', '')
    if cols.isdigit() and int(cols) > 0:
        return int(cols)
    stream = stream if stream is not None else sys.stdout
    try:
        if stream.isatty():
            return shutil.get_terminal_size().columns
    except (AttributeError, ValueError, OSError):
        pass
    return None


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def shorten(text, width, mode='end'):
    """`text` cut to `width` characters, or as is if it fits: mode 'end' keeps
    the start (a name), 'middle' the head and the tail (a path keeps its file
    name)."""
    if width <= 0:
        return ''
    if len(text) <= width:
        return text
    if width == 1:
        return ELLIPSIS
    if mode == 'end':
        return text[:width - 1] + ELLIPSIS
    head = (width - 1) // 2
    tail = width - 1 - head
    return text[:head] + ELLIPSIS + (text[-tail:] if tail else '')


def render_table(header, rows, *, total=None, paint=None, styles=None, width=None,
                 shrink=None, shrink_mode='end', gap='  '):
    """Lines of a table.

    header: column names. rows: lists of cells (str or numbers; None shows as
    '-'). total: an optional last row, shown bold in its first cell.
    styles: per column, None or a function cell -> tuple of Painter styles
    (or None); zero counts in a styled column are dimmed. width: fit to this
    many characters by shortening column `shrink` (default: the widest text
    column) as `shorten(mode=shrink_mode)` does; None means no limit. Numeric columns (any number in them) are
    right-aligned. Trailing spaces are stripped."""
    ncol = len(header)
    body = [list(r) for r in rows] + ([list(total)] if total is not None else [])
    numeric = [any(_is_number(r[i]) for r in body) for i in range(ncol)]
    text = [[('-' if c is None else str(c)) for c in r] for r in [list(header)] + body]
    widths = [max(len(r[i]) for r in text) for i in range(ncol)]

    if width is not None:
        over = sum(widths) + len(gap) * (ncol - 1) - width
        if over > 0:
            col = shrink
            if col is None:
                candidates = [i for i in range(ncol) if not numeric[i]]
                col = max(candidates, key=lambda i: widths[i]) if candidates else None
            if col is not None:
                new = max(widths[col] - over, len(header[col]), 8)
                widths[col] = new
                for r in text:
                    r[col] = shorten(r[col], new, shrink_mode)

    def cell(i, value, raw, row_is_header):
        # In a number column: numbers (and the '?' / '-' placeholders, and
        # the header) right-aligned; words (`deferred (x)`) left-aligned.
        if numeric[i] and (row_is_header or _is_number(raw) or raw is None
                           or value in ('?', '-')):
            padded = value.rjust(widths[i])
        elif i == ncol - 1:
            padded = value  # nothing to align after it: no trailing spaces
        else:
            padded = value.ljust(widths[i])
        if paint is None or row_is_header:
            return padded
        style = styles[i](raw) if styles and styles[i] else None
        if style:
            return paint(padded, *style)
        if styles and styles[i] and value == '0':
            return paint(padded, 'dim')
        return padded

    lines = []
    for n, (cells, raw) in enumerate(zip(text, [list(header)] + body)):
        is_header = n == 0
        out = [cell(i, v, raw[i], is_header) for i, v in enumerate(cells)]
        if total is not None and n == len(text) - 1 and paint is not None:
            out[0] = paint(out[0], 'bold')
        lines.append(gap.join(out).rstrip())
    return lines
