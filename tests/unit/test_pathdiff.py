"""remake.util.pathdiff: where two paths differ (soak finding 4)."""
from remake.util.colour import Painter
from remake.util.pathdiff import diff_components, render_path_diff

BASE = '/gws/ssde/j25b/afesp/users/mmuetz/upflo/v17/20230612/camra/deltaZ_candidate/'


def _changed(components):
    return ['/'.join(''.join(t for t, _ in c) for c in [comp]) for comp in components
            if any(ch for _, ch in comp)]


def test_inserted_directory_is_one_change_and_prefix_collapses():
    a = BASE + 'comparison/gathered_dZ_stats.hdf'
    b = BASE + 'compare_rhis_to_radarnet.hdf'
    ca, cb = diff_components(a, b)
    assert ca[0] == [('…', False)] and ca[1] == [('deltaZ_candidate', False)]
    assert _changed(ca) == ['comparison', 'gathered_dZ_stats.hdf']
    assert _changed(cb) == ['compare_rhis_to_radarnet.hdf']


def test_similar_components_diff_by_character():
    ca, cb = diff_components('/a/b/c/v16/thresh_1.0/x.nc', '/a/b/c/v17/thresh_3.0/x.nc')
    assert ['v1', '6'] == [t for t, _ in ca[2]] and ca[2][1] == ('6', True)
    assert ('3', True) in cb[3]
    assert ca[-1] == [('x.nc', False)]


def test_no_colour_marks_with_carets_and_colour_uses_ansi():
    lines = render_path_diff('data/genn_1.txt', 'data/gen_1.txt', Painter('never'))
    assert lines == ['    wants:   data/genn_1.txt',
                     '                    ^',
                     '    nearest: data/gen_1.txt']
    coloured = render_path_diff('data/a.txt', 'data/b.txt', Painter('always'))
    assert len(coloured) == 2 and '\033[' in coloured[0] and '^' not in ''.join(coloured)


def test_short_paths_are_not_collapsed():
    ca, _ = diff_components('a/b.txt', 'a/c.txt')
    assert ca[0] == [('a', False)]
