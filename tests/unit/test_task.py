from remake import FileToken, ZarrStore, rule
from remake.core.task import Task


@rule(
    inputs={'raw': 'data/raw/{model}/{year}.nc'},
    outputs={'clean': 'data/clean/{model}/{year}.nc'},
    matrix={'model': ['era5'], 'year': [1980]},
)
def example(inputs, outputs, model, year):
    pass


def test_key_is_stable_under_kwargs_ordering():
    t1 = Task(rule=example, kwargs={'model': 'era5', 'year': 1980})
    t2 = Task(rule=example, kwargs={'year': 1980, 'model': 'era5'})
    assert t1.key == t2.key
    assert t1 == t2


def test_key_differs_for_different_kwargs():
    t1 = Task(rule=example, kwargs={'model': 'era5', 'year': 1980})
    t2 = Task(rule=example, kwargs={'model': 'era5', 'year': 1981})
    assert t1.key != t2.key


def test_format_string_resolution():
    t = Task(rule=example, kwargs={'model': 'era5', 'year': 1980})
    assert t.inputs == {'raw': 'data/raw/era5/1980.nc'}
    assert str(t.outputs['clean']) == 'data/clean/era5/1980.nc'


def test_outputs_wrapped_as_tokens_inputs_not():
    t = Task(rule=example, kwargs={'model': 'era5', 'year': 1980})
    assert isinstance(t.outputs['clean'], FileToken)
    assert isinstance(t.inputs['raw'], str)


def test_token_specs_formatted():
    @rule(
        outputs={'z': ZarrStore('data/{year}.zarr')},
        matrix={'year': [1980]},
    )
    def zarr_rule(outputs, year):
        pass

    t = Task(rule=zarr_rule, kwargs={'year': 1980})
    assert isinstance(t.outputs['z'], ZarrStore)
    assert t.outputs['z'].path == 'data/1980.zarr'


def test_callable_spec_receives_signature_subset():
    def inputs_fn(model):
        # Only declares model, not year — called with the matching subset.
        return {'raw': f'data/{model}.nc'}

    @rule(
        inputs=inputs_fn,
        outputs={'o': 'out/{model}/{year}.nc'},
        matrix={'model': ['era5'], 'year': [1980]},
    )
    def r(inputs, outputs, model, year):
        pass

    t = Task(rule=r, kwargs={'model': 'era5', 'year': 1980})
    assert t.inputs == {'raw': 'data/era5.nc'}


def test_no_declarations_resolve_empty():
    @rule(matrix={'n': [1]})
    def r(n):
        pass

    t = Task(rule=r, kwargs={'n': 1})
    assert t.inputs == {} and t.outputs == {}
