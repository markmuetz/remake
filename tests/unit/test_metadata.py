from pathlib import Path

from loguru import logger

from remake import Sqlite3Backend, rule


def _capture_warnings(fn):
    """Run fn, returning the WARNING-level loguru messages it emits."""
    msgs = []
    sink = logger.add(msgs.append, level='WARNING', format='{message}')
    try:
        fn()
    finally:
        logger.remove(sink)
    return msgs


def _two_same_named_rules(tmp_path):
    # Two distinct rules that share the name 'process' (the function name) but
    # have genuinely different source bodies.
    @rule(outputs={'o': str(tmp_path / 'o.txt')})
    def process(outputs):
        Path(outputs['o']).write_text('one')

    r1 = process

    @rule(outputs={'o': str(tmp_path / 'o.txt')})
    def process(outputs):  # noqa: F811 — deliberately same name
        Path(outputs['o']).write_text('two')

    return r1, process


def test_duplicate_rule_name_across_remakefiles_warns(tmp_path):
    # Two co-located remakefiles defining a different rule under the same name
    # share one .remake/ store and would clobber each other — ensure_rules must
    # warn (the duplicate-rule-name guard).
    meta = Sqlite3Backend(':memory:')
    r1, r2 = _two_same_named_rules(tmp_path)

    meta.ensure_rules([r1], remakefile='a.py')
    msgs = _capture_warnings(lambda: meta.ensure_rules([r2], remakefile='b.py'))

    text = ' '.join(msgs)
    assert 'process' in text and 'a.py' in text and 'b.py' in text


def test_same_remakefile_edit_does_not_warn(tmp_path):
    # The same name with changed code from the *same* remakefile is an ordinary
    # edit, not a collision — no warning.
    meta = Sqlite3Backend(':memory:')
    r1, r2 = _two_same_named_rules(tmp_path)

    meta.ensure_rules([r1], remakefile='a.py')
    msgs = _capture_warnings(lambda: meta.ensure_rules([r2], remakefile='a.py'))

    assert not any('defined in both' in m for m in msgs)


def test_identical_shared_rule_does_not_warn(tmp_path):
    # The same rule object registered from two remakefiles (identical source) is
    # legitimate sharing — provenance moves, but no collision warning.
    meta = Sqlite3Backend(':memory:')

    @rule(outputs={'o': str(tmp_path / 'o.txt')})
    def process(outputs):
        Path(outputs['o']).write_text('shared')

    meta.ensure_rules([process], remakefile='a.py')
    msgs = _capture_warnings(lambda: meta.ensure_rules([process], remakefile='b.py'))

    assert not any('defined in both' in m for m in msgs)
