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


# --- rule plumbing: inputs/outputs specs vs matrix (fail once, early) ---


def test_inputs_fn_param_not_in_matrix_rejected_at_decoration():
    # Rule plumbing, not a task error: the inputs fn is called with the
    # matrix kwargs it names, so a mismatched signature would fail every
    # task identically at run time. Caught once, at decoration.
    def bad_inputs(case):
        return {'i': f'in/{case}.txt'}

    with pytest.raises(SignatureError, match=r"inputs function.*\['case'\].*matrix"):

        @rule(inputs=bad_inputs, outputs={'o': 'out/{n}.txt'}, matrix={'n': [1, 2]})
        def r(inputs, outputs, n):
            pass


def test_inputs_fn_defaults_and_subset_params_accepted():
    # Params with defaults and zero-arg fan-ins are fine; so is naming only
    # a subset of the matrix keys.
    def fan_in():
        return {'i': 'all.txt'}

    def subset(n, scale=2):
        return {'i': f'in/{n}_{scale}.txt'}

    @rule(inputs=fan_in, outputs={'o': 'out/{n}.txt'}, matrix={'n': [1]})
    def r1(inputs, outputs, n):
        pass

    @rule(inputs=subset, outputs={'o': 'out/{n}_{m}.txt'},
          matrix={'n': [1], 'm': [2]})
    def r2(inputs, outputs, n, m):
        pass


def test_template_field_not_in_matrix_rejected_at_decoration():
    with pytest.raises(SignatureError, match=r"outputs template.*\['typo'\].*matrix"):

        @rule(outputs={'o': 'out/{typo}.txt'}, matrix={'n': [1, 2]})
        def r(outputs, n):
            pass


def test_template_format_spec_and_access_allowed():
    # '{n:03d}' is still just the field 'n'; matching fields pass.
    @rule(outputs={'o': 'out/{n:03d}.txt'}, matrix={'n': [1]})
    def r(outputs, n):
        pass


def test_callable_matrix_io_spec_checked_at_expansion():
    def bad_inputs(case):
        return {'i': f'in/{case}.txt'}

    @rule(inputs=bad_inputs, outputs={'o': 'out/{n}.txt'},
          matrix=lambda: [{'n': 1}])
    def r(inputs, outputs, n):
        pass

    with pytest.raises(SignatureError, match="inputs function"):
        expand_rule(r)
