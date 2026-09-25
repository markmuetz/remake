"""Path-derived task-to-task dependencies (review 2026-09-24 H3, Appendix A)."""
from pathlib import Path

import pytest

from remake import Remake, Sqlite3Backend, ZarrStore, rule
from remake.core.dag import expand_rule
from remake.core.deps import ALL, Edge, _classify, _disjoint, task_id
from remake.core.planner import cascade_settled, explain_task
from remake.metadata import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS

YEARS = [2000, 2001, 2002, 2003]


def tid(**kwargs):
    return frozenset(kwargs.items())


def edge(dep, rule_):
    return Edge(dep, rule_, lambda: expand_rule(dep))


def ups(dep, rule_, **kwargs):
    task = next(t for t in expand_rule(rule_) if t.kwargs == kwargs)
    return edge(dep, rule_).upstream_of(task)


def shifted(tmp_path):
    """b[year] reads a[year-1] — a shared-shape pipeline the matrix can't
    describe (b's matrix is a's minus the first year)."""
    @rule(outputs={'o': str(tmp_path / 'a_{year}.txt')}, matrix={'year': YEARS})
    def a(outputs, year):
        Path(outputs['o']).write_text(str(year))

    def b_inputs(year):
        return {'prev': str(tmp_path / f'a_{year - 1}.txt')}

    @rule(inputs=b_inputs, outputs={'o': str(tmp_path / 'b_{year}.txt')},
          matrix={'year': YEARS[1:]}, depends_on=[a])
    def b(inputs, outputs, year):
        Path(outputs['o']).write_text(Path(inputs['prev']).read_text())

    rmk = Remake(rules=[a, b], metadata=Sqlite3Backend(':memory:'))
    return rmk, a, b


def task_of(rmk, rule_, **kwargs):
    return next(t for t in rmk.tasks() if t.rule is rule_ and t.kwargs == kwargs)


def runnable_of(rmk, rule_, **plan_kwargs):
    return sorted(t.kwargs['year'] for t in rmk.plan(**plan_kwargs)[0] if t.rule is rule_)


# --- the map --------------------------------------------------------------

def test_template_classification(tmp_path):
    @rule(outputs={'o': str(tmp_path / 'a/{n}.txt')}, matrix={'n': [1, 2]})
    def a(outputs, n):
        pass

    @rule(inputs=a.outputs, outputs={'o': str(tmp_path / 'b/{n}.txt')},
          matrix=a.matrix, depends_on=[a])
    def idiom(inputs, outputs, n):
        pass

    @rule(inputs={**a.outputs, 'raw': str(tmp_path / 'raw/{n}.nc')},
          outputs={'o': str(tmp_path / 'c/{n}.txt')}, matrix=a.matrix, depends_on=[a])
    def with_external(inputs, outputs, n):
        pass

    @rule(outputs={'o': str(tmp_path / 'd/{n}.txt')}, matrix=a.matrix, depends_on=[a])
    def ordering(outputs, n):
        pass

    @rule(inputs={'x': str(tmp_path / 'raw/{n}.nc')},
          outputs={'o': str(tmp_path / 'e/{n}.txt')}, matrix=a.matrix, depends_on=[a])
    def disjoint_only(inputs, outputs, n):
        pass

    @rule(inputs=lambda n: {'x': str(tmp_path / f'a/{n}.txt')},
          outputs={'o': str(tmp_path / 'f/{n}.txt')}, matrix=a.matrix, depends_on=[a])
    def callable_inputs(inputs, outputs, n):
        pass

    assert _classify(a, idiom) == 'elementwise'
    assert _classify(a, with_external) == 'elementwise'
    assert _classify(a, ordering) == 'ordering'
    assert _classify(a, disjoint_only) == 'ordering'
    assert _classify(a, callable_inputs) == 'resolve'
    # Resolution agrees with the template shortcut.
    assert ups(a, callable_inputs, n=2) == {tid(n=2)}
    assert ups(a, ordering, n=2) is ALL


def test_template_missing_a_key_is_not_elementwise(tmp_path):
    # a's output ignores `model`: a[year, model=*] all write one file, so the
    # equal template is shared, not element-wise.
    m = {'year': [1, 2], 'model': ['x', 'y']}

    @rule(outputs={'o': str(tmp_path / 'a_{year}.txt')}, matrix=m)
    def a(outputs, year, model):
        pass

    @rule(inputs=a.outputs, outputs={'o': str(tmp_path / 'b_{year}_{model}.txt')},
          matrix=m, depends_on=[a])
    def b(inputs, outputs, year, model):
        pass

    assert _classify(a, b) == 'resolve'
    assert ups(a, b, year=1, model='x') is ALL  # a shared output


def test_disjoint_is_conservative_about_normalisation():
    assert _disjoint('raw/{n}.nc', 'a/{n}.txt')
    assert _disjoint('a/{n}.nc', 'a/{n}.txt')  # suffixes differ
    assert not _disjoint('a/{n}.txt', 'a/x{n}.txt')
    # normpath('./a/1') == 'a/1': literal prefixes differ but paths may not.
    assert not _disjoint('./a/{n}.txt', 'a/{n}.txt')
    assert not _disjoint('a//{n}.txt', 'a/{n}.txt')


def test_shifted_stencil_and_all_readers(tmp_path):
    rmk, a, b = shifted(tmp_path)
    assert ups(a, b, year=2002) == {tid(year=2001)}

    @rule(inputs=lambda t: {str(d): str(tmp_path / f'a_{t + d}.txt') for d in (-1, 0, 1)},
          outputs={'o': str(tmp_path / 's_{t}.txt')}, matrix={'t': [2001, 2002]}, depends_on=[a])
    def stencil(inputs, outputs, t):
        pass

    assert ups(a, stencil, t=2001) == {tid(year=y) for y in (2000, 2001, 2002)}

    @rule(inputs={str(y): str(tmp_path / f'a_{y}.txt') for y in YEARS},
          outputs={'o': str(tmp_path / 'clim.txt')}, depends_on=[a])
    def clim(inputs, outputs):
        pass

    assert ups(a, clim) == {tid(year=y) for y in YEARS}


def test_shared_zarr_output_depends_on_all(tmp_path):
    @rule(outputs={'z': ZarrStore(str(tmp_path / 'store.zarr'))}, matrix={'n': [1, 2]})
    def region(outputs, n):
        pass

    @rule(inputs=region.outputs, outputs={'o': str(tmp_path / 'r_{n}.txt')},
          matrix=region.matrix, depends_on=[region])
    def reader(inputs, outputs, n):
        pass

    assert ups(region, reader, n=1) is ALL


# --- the four consumers ---------------------------------------------------

def test_same_pass_propagation_follows_paths(tmp_path):
    rmk, a, b = shifted(tmp_path)
    rmk.run()
    rmk.metadata.update_task(task_of(rmk, a, year=2001), TASK_STATUS_FAILED)
    # Before H3: b[2001] (same kwargs) reran and b[2002] — the reader — didn't.
    assert runnable_of(rmk, b) == [2002]


def test_durable_backstop_follows_paths(tmp_path):
    rmk, a, b = shifted(tmp_path)
    rmk.run()
    rmk.run(query='rule == "a" and year == 2001', force=True)
    assert runnable_of(rmk, b) == [2002]
    # ... and under a query that filters out the upstream task b[2002] reads.
    assert runnable_of(rmk, b, query='year == 2002') == [2002]


def test_why_names_the_upstream_task_read(tmp_path):
    rmk, a, b = shifted(tmp_path)
    rmk.run()
    rmk.metadata.update_task(task_of(rmk, a, year=2001), TASK_STATUS_FAILED)
    will_run, reasons = explain_task(rmk.rules, rmk.dag, rmk.metadata, task_of(rmk, b, year=2002))
    assert will_run
    assert any('a[year=2001]' in r.message for r in reasons if r.category == 'upstream-rerun')
    will_run, reasons = explain_task(rmk.rules, rmk.dag, rmk.metadata, task_of(rmk, b, year=2001))
    assert not will_run and not any(r.category == 'upstream-rerun' for r in reasons)


def test_cascade_follows_paths(tmp_path):
    rmk, a, b = shifted(tmp_path)
    rmk.finalize()
    run_seq = {a: {tid(year=y): 1 for y in YEARS}, b: {tid(year=y): 1 for y in YEARS[1:]}}
    status = {r: {k: TASK_STATUS_SUCCESS for k in run_seq[r]} for r in run_seq}
    settled = cascade_settled(set(rmk.rules), rmk.dag, {a: {tid(year=2001)}}, run_seq, status)
    assert settled[b] == {tid(year=2002)}


def test_ordering_only_dependency_is_conservative(tmp_path):
    # No path links b to a: which a task a b task needs is unknowable, so
    # any a rerun reruns all of b (previously element-wise by matrix).
    @rule(outputs={'o': str(tmp_path / 'a_{n}.txt')}, matrix={'n': [1, 2]})
    def a(outputs, n):
        Path(outputs['o']).write_text('a')

    @rule(outputs={'o': str(tmp_path / 'b_{n}.txt')}, matrix=a.matrix, depends_on=[a])
    def b(outputs, n):
        Path(outputs['o']).write_text('b')

    rmk = Remake(rules=[a, b], metadata=Sqlite3Backend(':memory:'))
    rmk.run()
    rmk.metadata.update_task(task_of(rmk, a, n=1), TASK_STATUS_FAILED)
    assert sorted(t.kwargs['n'] for t in rmk.plan()[0] if t.rule is b) == [1, 2]


def test_precise_fan_in_across_matrices(tmp_path):
    sites, years = ['s1', 's2'], [1, 2]

    @rule(outputs={'o': str(tmp_path / 'p_{site}_{year}.txt')},
          matrix={'site': sites, 'year': years})
    def process(outputs, site, year):
        Path(outputs['o']).write_text('p')

    @rule(inputs=lambda site: {str(y): str(tmp_path / f'p_{site}_{y}.txt') for y in years},
          outputs={'o': str(tmp_path / 'agg_{site}.txt')}, matrix={'site': sites},
          depends_on=[process])
    def aggregate(inputs, outputs, site):
        Path(outputs['o']).write_text('agg')

    rmk = Remake(rules=[process, aggregate], metadata=Sqlite3Backend(':memory:'))
    rmk.run()
    rmk.metadata.update_task(task_of(rmk, process, site='s1', year=2), TASK_STATUS_FAILED)
    assert [t.kwargs for t in rmk.plan()[0] if t.rule is aggregate] == [{'site': 's1'}]


def _failure_pipeline(tmp_path, ordering_only):
    @rule(outputs={'o': str(tmp_path / 'a_{n}.txt')}, matrix={'n': [1, 2, 3]})
    def a(outputs, n):
        if n == 2:
            raise ValueError('boom')
        Path(outputs['o']).write_text('a')

    if ordering_only:
        @rule(outputs={'o': str(tmp_path / 'b_{n}.txt')}, matrix=a.matrix, depends_on=[a])
        def b(outputs, n):
            Path(outputs['o']).write_text('b')
    else:
        @rule(inputs=a.outputs, outputs={'o': str(tmp_path / 'b_{n}.txt')},
              matrix=a.matrix, depends_on=[a])
        def b(inputs, outputs, n):
            Path(outputs['o']).write_text('b')

    return Remake(rules=[a, b], metadata=Sqlite3Backend(':memory:')), b


@pytest.mark.parametrize('ordering_only, ran', [(False, [1, 3]), (True, [])])
def test_failure_skip_follows_paths(tmp_path, ordering_only, ran):
    # a[2] fails. An element-wise reader skips only b[2]; with no path
    # linking the rules every b task is skipped (it may need a[2]).
    rmk, b = _failure_pipeline(tmp_path, ordering_only)
    rmk.run()
    assert sorted(n for n in (1, 2, 3) if (tmp_path / f'b_{n}.txt').exists()) == ran
