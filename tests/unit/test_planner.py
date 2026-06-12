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
    # Same rules, fresh DB: fallback recognises completed outputs.
    rmk2, *_ = make_pipeline(tmp_path)
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
