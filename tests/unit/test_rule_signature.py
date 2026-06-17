import pytest

from remake import Defer, SignatureError, deferrable, rule
from remake.core.dag import expand_rule


def test_minimal_rule_no_declarations():
    @rule()
    def r():
        pass

    assert r.inputs is None and r.outputs is None and r.matrix is None


def test_decorator_returns_rule_object():
    @rule(outputs={'o': 'o.txt'})
    def r(outputs):
        pass

    assert r.name == 'r'
    assert r.outputs == {'o': 'o.txt'}
    assert 'def r(outputs)' in r.source['run']


def test_undeclared_inputs_param_rejected():
    with pytest.raises(SignatureError, match='inputs'):

        @rule(outputs={'o': 'o.txt'})
        def r(inputs, outputs):
            pass


def test_missing_outputs_param_rejected():
    with pytest.raises(SignatureError):

        @rule(outputs={'o': 'o.txt'})
        def r():
            pass


def test_matrix_key_mismatch_rejected():
    with pytest.raises(SignatureError, match='matrix keys'):

        @rule(outputs={'o': '{x}.txt'}, matrix={'x': [1]})
        def r(outputs, y):
            pass


def test_matrix_param_order_free():
    @rule(outputs={'o': '{x}_{y}.txt'}, matrix={'x': [1], 'y': [2]})
    def r(outputs, y, x):
        pass


def test_inputs_outputs_order_enforced():
    with pytest.raises(SignatureError, match='must start with'):

        @rule(inputs={'i': 'i.txt'}, outputs={'o': 'o.txt'})
        def r(outputs, inputs):
            pass


def test_empty_dicts_rejected():
    with pytest.raises(SignatureError, match='ambiguous'):

        @rule(inputs={}, outputs={'o': 'o.txt'})
        def r(inputs, outputs):
            pass

    with pytest.raises(SignatureError, match='ambiguous'):

        @rule(outputs={})
        def r2(outputs):
            pass


def test_var_args_rejected():
    with pytest.raises(SignatureError, match='args'):

        @rule(outputs={'o': 'o.txt'})
        def r(outputs, **kwargs):
            pass


def test_callable_matrix_checked_at_expansion():
    # Parameter names are unknowable at decoration for callable matrices...
    @rule(outputs={'o': '{x}.txt'}, matrix=lambda: [{'y': 1}])
    def r(outputs, x):
        pass

    # ...so the mismatch is caught when the matrix resolves.
    with pytest.raises(SignatureError, match='matrix keys'):
        expand_rule(r)


def test_deferrable_matrix_defer_propagates():
    @deferrable
    def matrix():
        raise Defer('blocking/path.json')

    @rule(outputs={'o': '{x}.txt'}, matrix=matrix)
    def r(outputs, x):
        pass

    with pytest.raises(Defer, match='blocking/path.json'):
        expand_rule(r)


def test_unmarked_matrix_defer_is_error():
    def matrix():
        raise Defer('blocking/path.json')

    @rule(outputs={'o': '{x}.txt'}, matrix=matrix)
    def r(outputs, x):
        pass

    with pytest.raises(SignatureError, match='not marked @deferrable'):
        expand_rule(r)
