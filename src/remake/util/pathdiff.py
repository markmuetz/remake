"""Show where two paths differ: for `remake lint`'s near misses, where two
~150-character paths that share a long prefix are hard to compare by eye
(soak finding 4, 2026-11-23).

Paths are compared by component (split on '/'), so an inserted or removed
directory is one change and doesn't misalign the rest; inside a component
that changed into one similar component, by character (`v16` vs `v17`). The
shared leading components are collapsed to `…`, keeping the last one for
context.
"""
from difflib import SequenceMatcher

# A changed component is diffed by character only when the two spellings are
# this similar; below it, the whole component is marked (no confetti).
_CHAR_DIFF_RATIO = 0.5
# Shared leading components kept before the collapse to `…`.
_KEEP_CONTEXT = 1


def _char_segments(x, y):
    """Segments [(text, changed)] of x and of y, by character."""
    sm = SequenceMatcher(None, x, y, autojunk=False)
    if sm.ratio() < _CHAR_DIFF_RATIO:
        return [(x, True)], [(y, True)]
    xs, ys = [], []
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if i2 > i1:
            xs.append((x[i1:i2], op != 'equal'))
        if j2 > j1:
            ys.append((y[j1:j2], op != 'equal'))
    return xs, ys


def diff_components(a, b):
    """Each path as a list of components, each a list of (text, changed)
    segments, with shared leading components collapsed to `…`."""
    ca, cb = a.split('/'), b.split('/')
    sm = SequenceMatcher(None, ca, cb, autojunk=False)
    out_a, out_b = [], []
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if op == 'equal':
            out_a += [[(c, False)] for c in ca[i1:i2]]
            out_b += [[(c, False)] for c in cb[j1:j2]]
        elif op == 'replace' and i2 - i1 == j2 - j1:
            for x, y in zip(ca[i1:i2], cb[j1:j2]):
                xs, ys = _char_segments(x, y)
                out_a.append(xs)
                out_b.append(ys)
        else:
            out_a += [[(c, True)] for c in ca[i1:i2]]
            out_b += [[(c, True)] for c in cb[j1:j2]]
    shared = 0
    for x, y in zip(out_a, out_b):
        if x != y or any(changed for _, changed in x):
            break
        shared += 1
    cut = shared - _KEEP_CONTEXT
    if cut > 1 or (cut == 1 and out_a[0] != [('', False)]):
        out_a = [[('…', False)]] + out_a[cut:]
        out_b = [[('…', False)]] + out_b[cut:]
    return out_a, out_b


def _flatten(components):
    segs = []
    for i, comp in enumerate(components):
        if i:
            segs.append(('/', False))
        segs.extend(comp)
    return segs


def render_path_diff(a, b, paint, labels=('wants', 'nearest'), indent='    '):
    """Lines showing a over b, aligned and labelled, the differing spans red
    (in a) and green (in b), the shared parts dimmed. With colour off, a
    line of `^` under each path marks its differing spans instead."""
    comps_a, comps_b = diff_components(a, b)
    width = max(len(label) for label in labels) + 2
    lines = []
    for label, comps, style in ((labels[0], comps_a, 'red'), (labels[1], comps_b, 'green')):
        segs = _flatten(comps)
        head = f'{indent}{label + ":":<{width}}'
        if paint.enabled:
            lines.append(head + ''.join(paint(t, style, 'bold') if changed else paint(t, 'dim')
                                        for t, changed in segs))
        else:
            lines.append(head + ''.join(t for t, _ in segs))
            marks = ''.join(('^' if changed else ' ') * len(t) for t, changed in segs)
            if marks.strip():
                lines.append(' ' * len(head) + marks.rstrip())
    return lines


def display_path(path, base=None, width=None):
    """A path as shown to a person: relative to `base` (the project
    directory) when it is inside it, then cut in the middle to `width`
    characters if given (head and file name kept)."""
    import os

    from .table import shorten

    text = str(path)
    if base and os.path.isabs(text):
        rel = os.path.relpath(text, base)
        if not rel.startswith('..'):
            text = rel
    return shorten(text, width, 'middle') if width else text


def plural(n, word, words=None):
    """'1 task', '2 tasks'."""
    return f'{n} {word if n == 1 else (words or word + "s")}'
