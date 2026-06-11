import networkx as nx
import pytest

from remake import MatrixNotReady, rule
from remake.core.dag import build_rule_dag, expand_rule, resolve_matrix


def make_chain():
    @rule(outputs={'a': 'a.txt'})
    def rule_a(outputs):
        pass

    @rule(inputs=rule_a.outputs, outputs={'b': 'b.txt'}, depends_on=[rule_a])
    def rule_b(inputs, outputs):
        pass

    return rule_a, rule_b


def test_build_rule_dag_topological():
    rule_a, rule_b = make_chain()
    dag = build_rule_dag([rule_b, rule_a])  # order-independent
    assert list(nx.topological_sort(dag)) == [rule_a, rule_b]


def test_cycle_detected():
    rule_a, rule_b = make_chain()
    rule_a.depends_on = [rule_b]
    with pytest.raises(ValueError, match='cycle'):
        build_rule_dag([rule_a, rule_b])


def test_resolve_matrix_forms():
    assert resolve_matrix(None) == [{}]
    assert resolve_matrix({}) == [{}]
    assert resolve_matrix({'x': [1, 2], 'y': ['a']}) == [
        {'x': 1, 'y': 'a'},
        {'x': 2, 'y': 'a'},
    ]
    explicit = [{'x': 1}, {'x': 5}]
    assert resolve_matrix(explicit) is explicit
    assert resolve_matrix(lambda: [{'x': 9}]) == [{'x': 9}]
    with pytest.raises(MatrixNotReady):
        resolve_matrix(lambda: (_ for _ in ()).throw(MatrixNotReady('p')))


def test_expand_rule_cartesian():
    @rule(outputs={'o': '{model}_{year}.txt'}, matrix={'model': ['a', 'b'], 'year': [1, 2]})
    def r(outputs, model, year):
        pass

    tasks = expand_rule(r)
    assert len(tasks) == 4
    assert {t.kwargs['model'] for t in tasks} == {'a', 'b'}
    assert len({t.key for t in tasks}) == 4


def test_expand_rule_predicate_filters_before_construction():
    @rule(outputs={'o': '{n}.txt'}, matrix={'n': list(range(10))})
    def r(outputs, n):
        pass

    tasks = expand_rule(r, predicate=lambda kw: kw['n'] % 2 == 0)
    assert [t.kwargs['n'] for t in tasks] == [0, 2, 4, 6, 8]


def test_no_matrix_is_single_task():
    @rule(outputs={'o': 'o.txt'})
    def r(outputs):
        pass

    assert len(expand_rule(r)) == 1
