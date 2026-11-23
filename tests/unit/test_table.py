"""remake.util.table (0.11 item 8): the shared CLI table renderer."""
import re

from remake.util.colour import Painter
from remake.util.table import render_table, shorten, terminal_width

ANSI = re.compile(r'\x1b\[[0-9;]*m')


def test_numbers_right_aligned_words_left_and_total():
    lines = render_table(('rule', 'n', 'note'), [('a', 1, 'x'), ('bbbb', 123, 'held by y')],
                         total=('TOTAL', 124, ''))
    assert lines == ['rule     n  note',
                     'a        1  x',
                     'bbbb   123  held by y',
                     'TOTAL  124']


def test_empty_column_left_out():
    lines = render_table(('a', 'git', 'b'), [('x', '', 1), ('y', None, 2)])
    assert lines[0].split() == ['a', 'b']


def test_fitting_never_widens_and_shrinks_several_columns():
    # Review 2026-11-23: a narrow text column was raised to 8 characters.
    plain = render_table(('a', 'n1', 'n2'), [('abc', 123456789012, 123456789012)])
    fitted = render_table(('a', 'n1', 'n2'), [('abc', 123456789012, 123456789012)], width=20)
    assert max(map(len, fitted)) <= max(map(len, plain))
    two = render_table(('first', 'second'), [('x' * 30, 'y' * 30)], width=40)
    assert max(map(len, two)) <= 40 and '…' in two[1]


def test_colour_never_changes_text_with_trailing_empty_cells():
    # Review 2026-11-23: a painted cell kept its padding when the cells after
    # it in that row were empty.
    def st(v):
        return ('red',)

    rows = [(1, 'failed', ''), (2, 'success', 'h')]
    plain = render_table(('run', 'status', 'host'), rows, paint=Painter('never'),
                         styles=[None, st, None])
    coloured = render_table(('run', 'status', 'host'), rows, paint=Painter('always'),
                            styles=[None, st, None])
    assert [ANSI.sub('', ln) for ln in coloured] == plain


def test_shorten_modes():
    assert shorten('abcdefghij', 5) == 'abcd…'
    assert shorten('abcdefghij', 5, 'middle') == 'ab…ij'
    assert shorten('abc', 5) == 'abc'


def test_terminal_width_from_columns(monkeypatch):
    monkeypatch.setenv('COLUMNS', '73')
    assert terminal_width() == 73
    monkeypatch.setenv('COLUMNS', 'abc')
    assert terminal_width() is None  # pytest's stdout is not a TTY
