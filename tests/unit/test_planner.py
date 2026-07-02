from pathlib import Path

from remake import Remake, Sqlite3Backend, rule
from remake.core.planner import make_predicate
from remake.metadata import TASK_STATUS_FAILED


def make_pipeline(tmp_path, **remake_kwargs):
    @rule(outputs={'o': str(tmp_path / 'a_{n}.txt')}, matrix={'n': [1, 2]})
    def rule_a(outputs, n):
        Path(outputs['o']).write_text(str(n))

    @rule(
        inputs=rule_a.outputs,
        outputs={'o': str(tmp_path / 'b_{n}.txt')},
        matrix=rule_a.matrix,
        depends_on=[rule_a],
    )
    def rule_b(inputs, outputs, n):
        Path(outputs['o']).write_text(Path(inputs['o']).read_text())

    def fan_in_inputs():
        return {str(n): str(tmp_path / f'b_{n}.txt') for n in [1, 2]}

    @rule(inputs=fan_in_inputs, outputs={'o': str(tmp_path / 'c.txt')}, depends_on=[rule_b])
    def rule_c(inputs, outputs):
        Path(outputs['o']).write_text('done')

    remake_kwargs.setdefault('metadata', Sqlite3Backend(':memory:'))
    rmk = Remake(rules=[rule_a, rule_b, rule_c], **remake_kwargs)
    return rmk, rule_a, rule_b, rule_c


def test_never_run_all_runnable(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    runnable, deferred = rmk.plan()
    assert len(runnable) == 5 and not deferred


def test_topological_ordering(tmp_path):
    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    runnable, _ = rmk.plan()
    rule_order = [t.rule.name for t in runnable]
    assert rule_order == ['rule_a', 'rule_a', 'rule_b', 'rule_b', 'rule_c']


def test_complete_run_replans_empty(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    rmk.run()
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred


def test_failed_task_replans(tmp_path):
    rmk, rule_a, *_ = make_pipeline(tmp_path)
    rmk.run()
    task = next(t for t in rmk.tasks() if t.rule is rule_a and t.kwargs == {'n': 1})
    rmk.metadata.update_task(task, TASK_STATUS_FAILED, exception='boom')
    runnable, _ = rmk.plan()
    assert task in runnable


def test_elementwise_propagation_same_matrix(tmp_path):
    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.run()
    task = next(t for t in rmk.tasks() if t.rule is rule_a and t.kwargs == {'n': 1})
    rmk.metadata.update_task(task, TASK_STATUS_FAILED)
    runnable, _ = rmk.plan()
    by_rule = {}
    for t in runnable:
        by_rule.setdefault(t.rule.name, []).append(t.kwargs)
    # rule_b shares rule_a's matrix: only n=1 propagates.
    assert by_rule['rule_a'] == [{'n': 1}]
    assert by_rule['rule_b'] == [{'n': 1}]
    # rule_c has a different matrix (fan-in): conservative, reruns.
    assert by_rule['rule_c'] == [{}]


def test_uses_change_triggers_rerun(tmp_path):
    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.run()
    rule_b.uses = {'mode': 'changed'}
    runnable, _ = rmk.plan()
    names = sorted({t.rule.name for t in runnable})
    assert names == ['rule_b', 'rule_c']  # rule_b and downstream, not rule_a
    assert len([t for t in runnable if t.rule is rule_b]) == 2


def test_uses_change_message_names_changed_keys():
    from remake.core.planner import _uses_change_message
    from remake.core.scope import parse_uses_hash, uses_hash, uses_parts

    def f(x):
        return x * 2

    def f2(x):
        return x * 3

    old = {'THRESHOLD': 0.5, 'helper': f, 'GONE': 1}
    old_hash = uses_hash(old)
    # The stored hash round-trips back to the per-key parts.
    assert parse_uses_hash(old_hash) == uses_parts(old)

    msg = _uses_change_message(old_hash, {'THRESHOLD': 0.7, 'helper': f2, 'SCALE': 10})
    assert 'THRESHOLD: 0.5 → 0.7' in msg  # plain value: before → after
    assert 'helper (body)' in msg  # callable: body change, not an AST dump
    assert 'GONE (removed)' in msg
    assert 'SCALE (added)' in msg


def test_uses_change_explain_reason(tmp_path):
    from remake.core.planner import explain_task

    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.run()
    rule_b.uses = {'THRESHOLD': 0.7}
    task = next(t for t in rmk.tasks() if t.rule is rule_b and t.kwargs == {'n': 1})
    _, reasons = explain_task(rmk.rules, rmk.dag, rmk.metadata, task)
    uses_reasons = [r for r in reasons if r.category == 'uses-changed']
    assert uses_reasons and 'THRESHOLD (added)' in uses_reasons[0].message


def _scale_v1(x):
    return x * 1


def _scale_v2(x):
    return x * 2


def _uses_pipeline(tmp_path, helper):
    @rule(outputs={'o': str(tmp_path / '{n}.txt')}, matrix={'n': [1]},
          uses={'scale': helper})
    def proc(outputs, n):
        Path(outputs['o']).write_text(str(scale(n)))  # noqa: F821 — injected

    return proc


def test_uses_helper_edit_shows_raw_source_diff(tmp_path):
    # The uses_manifest records each helper's *raw* source per uses version,
    # so `why` can show a real unified diff of the helper body — not just
    # "(body)".
    from remake import Remake, Sqlite3Backend
    from remake.core.planner import explain_task

    proc = _uses_pipeline(tmp_path, _scale_v1)
    rmk = Remake(rules=[proc], metadata=Sqlite3Backend(':memory:'))
    rmk.run()
    proc.uses = {'scale': _scale_v2}
    task = next(iter(rmk.tasks()))
    _, reasons = explain_task([proc], rmk.dag, rmk.metadata, task)
    (msg,) = [r.message for r in reasons if r.category == 'uses-changed']
    assert 'scale (body):' in msg
    assert '-    return x * 1' in msg and '+    return x * 2' in msg


def test_uses_sourceless_helper_says_source_unavailable(tmp_path):
    from remake import Remake, Sqlite3Backend
    from remake.core.planner import explain_task

    # exec/REPL-style: no retrievable source. NB the bodies must differ in
    # *opcodes*, not just constants — sourceless tracking hashes co_code,
    # which doesn't cover co_consts (so `x * 1` vs `x * 2` reads unchanged).
    v1 = eval('lambda x: x + x')
    v2 = eval('lambda x: x * 2')
    proc = _uses_pipeline(tmp_path, v1)
    rmk = Remake(rules=[proc], metadata=Sqlite3Backend(':memory:'))
    rmk.run()
    proc.uses = {'scale': v2}
    task = next(iter(rmk.tasks()))
    _, reasons = explain_task([proc], rmk.dag, rmk.metadata, task)
    (msg,) = [r.message for r in reasons if r.category == 'uses-changed']
    assert 'scale (body; source unavailable)' in msg


def test_uses_change_message_without_manifest_falls_back(tmp_path):
    # Records written before the uses_manifest table existed have no raw
    # sources on record: the message degrades to the bare "(body)" form.
    from remake.core.planner import _uses_change_message
    from remake.core.scope import uses_hash

    old_hash = uses_hash({'scale': _scale_v1})
    msg = _uses_change_message(old_hash, {'scale': _scale_v2}, old_manifest={})
    assert 'scale (body)' in msg and 'return x * 2' not in msg


def test_io_change_triggers_rerun(tmp_path):
    # Editing the outputs spec (here via the attribute, so the run function
    # source is untouched) must rerun the task and its downstream — the gap
    # io_hash closes (run-code/uses tracking alone missed it).
    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.run()
    rule_b.outputs = {'o': str(tmp_path / 'b_moved_{n}.txt')}
    runnable, _ = rmk.plan()
    names = sorted({t.rule.name for t in runnable})
    assert names == ['rule_b', 'rule_c']  # rule_b and downstream, not rule_a
    assert len([t for t in runnable if t.rule is rule_b]) == 2


def test_ignore_code_changes(tmp_path):
    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.run()
    rule_b.uses = {'mode': 'changed'}  # would normally rerun b + downstream
    runnable, _ = rmk.plan(ignore_code_changes=True)
    assert not runnable  # freshness checks off

    # Failed = not succeeded: reruns; and dataflow stays on — the rerun
    # propagates element-wise to b[n=1] and conservatively to the fan-in,
    # even though both succeeded.
    task = next(t for t in rmk.tasks() if t.rule is rule_a and t.kwargs == {'n': 1})
    rmk.metadata.update_task(task, TASK_STATUS_FAILED)
    runnable, _ = rmk.plan(ignore_code_changes=True)
    assert sorted([(t.rule.name, t.kwargs.get('n')) for t in runnable], key=str) == [
        ('rule_a', 1), ('rule_b', 1), ('rule_c', None)
    ]


def test_force_reruns_everything(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    rmk.run()
    runnable, _ = rmk.plan(force=True)
    assert len(runnable) == 5


def test_query_filters_and_missing_name_means_no_match(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    runnable, _ = rmk.plan(query='n == 2')
    # rule_c has no 'n' kwarg: excluded, not an error.
    assert sorted(t.rule.name for t in runnable) == ['rule_a', 'rule_b']
    assert all(t.kwargs['n'] == 2 for t in runnable)


def test_durable_propagation_after_partial_upstream_run(tmp_path):
    # Rerun rule_a alone in a *later* invocation (query excludes b/c), so the
    # in-pass propagation signal never reaches b. b must still rerun on the
    # next plan because a's stored run_seq is now greater than b's (the
    # cross-pass backstop). See bugs/01_durable_rerun_propagation.md.
    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.run()
    rmk.run(query='rule == "rule_a"', force=True)  # a only, fresh run_seq
    runnable, _ = rmk.plan()
    names = sorted({t.rule.name for t in runnable})
    assert names == ['rule_b', 'rule_c']  # b via run_seq, c via in-pass from b
    assert len([t for t in runnable if t.rule is rule_b]) == 2


def test_explain_reports_upstream_newer(tmp_path):
    from remake.core.planner import explain_task

    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.run()
    rmk.run(query='rule == "rule_a"', force=True)
    task = next(t for t in rmk.tasks() if t.rule is rule_b and t.kwargs == {'n': 1})
    will_run, reasons = explain_task(rmk.rules, rmk.dag, rmk.metadata, task)
    cats = [r.category for r in reasons]
    assert will_run and 'upstream-newer' in cats
    msg = next(r.message for r in reasons if r.category == 'upstream-newer')
    assert 'ran more recently' in msg


def make_diamond(tmp_path):
    # up1, up2 -> down -> tail. No matrices: one task per rule.
    @rule(outputs={'o': str(tmp_path / 'up1.txt')})
    def up1(outputs):
        Path(outputs['o']).write_text('1')

    @rule(outputs={'o': str(tmp_path / 'up2.txt')})
    def up2(outputs):
        Path(outputs['o']).write_text('2')

    @rule(inputs={'a': str(tmp_path / 'up1.txt'), 'b': str(tmp_path / 'up2.txt')},
          outputs={'o': str(tmp_path / 'down.txt')}, depends_on=[up1, up2])
    def down(inputs, outputs):
        Path(outputs['o']).write_text('d')

    @rule(inputs=down.outputs, outputs={'o': str(tmp_path / 'tail.txt')}, depends_on=[down])
    def tail(inputs, outputs):
        Path(outputs['o']).write_text('t')

    rmk = Remake(rules=[up1, up2, down, tail], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    return rmk, up1, up2, down, tail


def test_cascade_settled_linear_stamps_descendants(tmp_path):
    from remake.core.planner import cascade_settled
    from remake.metadata import TASK_STATUS_SUCCESS

    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.finalize()
    fs = frozenset()
    a1, a2 = frozenset({'n': 1}.items()), frozenset({'n': 2}.items())
    # a re-stamped newer (5); b, c older (1). All SUCCESS.
    run_seq = {rule_a: {a1: 5, a2: 5}, rule_b: {a1: 1, a2: 1}, rule_c: {fs: 1}}
    status = {r: {k: TASK_STATUS_SUCCESS for k in run_seq[r]} for r in run_seq}
    selected = {rule_a: {a1, a2}}
    settled = cascade_settled(set(rmk.rules), rmk.dag, selected, run_seq, status)
    # Single-dep descendants: guard never fires, so b and c are both cascaded.
    assert settled[rule_b] == {a1, a2}
    assert settled[rule_c] == {fs}


def test_cascade_settled_diamond_guard_protects_independent_upstream(tmp_path):
    from remake.core.planner import cascade_settled
    from remake.metadata import TASK_STATUS_SUCCESS

    rmk, up1, up2, down, tail = make_diamond(tmp_path)
    fs = frozenset()
    # Settle up1 (=5). up2 reran independently (=4) and is NOT settled; down/tail
    # old (=2). The guard must skip `down` (up2 newer & unsettled) — and so its
    # descendant `tail` is not reached either.
    run_seq = {up1: {fs: 5}, up2: {fs: 4}, down: {fs: 2}, tail: {fs: 2}}
    status = {r: {fs: TASK_STATUS_SUCCESS} for r in run_seq}
    settled = cascade_settled(set(rmk.rules), rmk.dag, {up1: {fs}}, run_seq, status)
    assert settled.get(down, set()) == set()   # guarded: down left to rerun
    assert settled.get(tail, set()) == set()

    # If up2 is *also* settled, down's only newer upstreams are settled → cascade.
    settled = cascade_settled(set(rmk.rules), rmk.dag, {up1: {fs}, up2: {fs}},
                              run_seq, status)
    assert settled[down] == {fs} and settled[tail] == {fs}


def test_cascade_settled_skips_non_success_descendant(tmp_path):
    from remake.core.planner import cascade_settled
    from remake.metadata import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS

    rmk, rule_a, rule_b, rule_c = make_pipeline(tmp_path)
    rmk.finalize()
    fs = frozenset()
    a1, a2 = frozenset({'n': 1}.items()), frozenset({'n': 2}.items())
    run_seq = {rule_a: {a1: 5, a2: 5}, rule_b: {a1: 1, a2: 1}, rule_c: {fs: 1}}
    status = {rule_a: {a1: TASK_STATUS_SUCCESS, a2: TASK_STATUS_SUCCESS},
              rule_b: {a1: TASK_STATUS_SUCCESS, a2: TASK_STATUS_FAILED},
              rule_c: {fs: TASK_STATUS_SUCCESS}}
    settled = cascade_settled(set(rmk.rules), rmk.dag, {rule_a: {a1, a2}},
                              run_seq, status)
    assert settled[rule_b] == {a1}  # the failed b[n=2] is left to rerun


def test_make_predicate():
    pred = make_predicate("n > 1 and model == 'era5'")
    assert pred({'n': 2, 'model': 'era5'})
    assert not pred({'n': 1, 'model': 'era5'})
    assert not pred({'other': 1})  # missing names: no match


def test_query_selects_by_rule_name(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    runnable, _ = rmk.plan(query='rule == "rule_a"')
    assert sorted(t.kwargs['n'] for t in runnable) == [1, 2]
    assert all(t.rule.name == 'rule_a' for t in runnable)

    # Multi-rule selection, composable with kwargs.
    runnable, _ = rmk.plan(query="rule in ['rule_a', 'rule_b'] and n == 2")
    assert sorted(t.rule.name for t in runnable) == ['rule_a', 'rule_b']
    assert all(t.kwargs == {'n': 2} for t in runnable)

    # rule_c has no matrix kwargs at all: rule name still selects it.
    runnable, _ = rmk.plan(query='rule == "rule_c"')
    assert [t.rule.name for t in runnable] == ['rule_c']


def test_get_tasks_status_batches_across_chunk_boundary(tmp_path):
    """Bulk status lookup with more tasks than one SELECT chunk."""
    from remake.metadata import TASK_STATUS_SUCCESS
    from remake.metadata.sqlite3_backend import Sqlite3Backend

    n = Sqlite3Backend.SELECT_CHUNK * 2 + 7

    @rule(outputs={'o': str(tmp_path / '{i}.txt')}, matrix={'i': list(range(n))})
    def big(outputs, i):
        pass

    rmk = Remake(rules=[big], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    tasks = rmk.tasks()
    # Record success for every third task only.
    recorded = tasks[::3]
    for task in recorded:
        rmk.metadata.update_task(task, TASK_STATUS_SUCCESS)

    records = rmk.metadata.get_tasks_status(tasks)
    assert len(records) == len(recorded)
    assert all(t.key in records for t in recorded)
    assert all(r.status == TASK_STATUS_SUCCESS for r in records.values())


def test_task_from_spec_and_key_lookup(tmp_path):
    @rule(outputs={'o': str(tmp_path / '{n}.txt')}, matrix={'n': list(range(50))})
    def r(outputs, n):
        pass

    rmk = Remake(rules=[r], metadata=Sqlite3Backend(':memory:'))

    direct = rmk.task_from_spec('r', {'n': 17})
    assert direct.kwargs == {'n': 17}
    # Direct construction agrees with search by full key and by prefix.
    assert rmk.task_from_key(direct.key) == direct
    assert rmk.task_from_key(direct.key[:12]) == direct

    import pytest as _pytest

    with _pytest.raises(Exception, match='No rule named'):
        rmk.task_from_spec('nope', {})


# --- check_outputs modes ---


def test_fallback_recognises_outputs_with_fresh_db(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    rmk.run()
    # Same rules, fresh DB: opt-in fallback recognises completed outputs.
    rmk2, *_ = make_pipeline(tmp_path, check_outputs='fallback')
    runnable, _ = rmk2.plan()
    assert not runnable


def test_never_mode_reruns_with_fresh_db(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    rmk.run()
    rmk2, *_ = make_pipeline(tmp_path, check_outputs='never')
    runnable, _ = rmk2.plan()
    assert len(runnable) == 5


def test_always_mode_detects_deleted_output(tmp_path):
    rmk, *_ = make_pipeline(tmp_path)
    rmk.run()
    (tmp_path / 'a_1.txt').unlink()  # simulate scratch purge
    runnable, _ = rmk.plan()
    assert not runnable  # fallback: DB record exists, trusted
    rmk.check_outputs = 'always'
    runnable, _ = rmk.plan()
    assert {t.kwargs.get('n') for t in runnable if t.rule.name == 'rule_a'} == {1}
