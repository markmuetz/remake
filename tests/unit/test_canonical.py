"""0.9.0 item 2: canonical change-detection renderings, and matching state
stored by remake <= 0.9.0a1 without a mass rerun."""
import functools
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from remake import Remake, Sqlite3Backend, rule
from remake.core import canonical
from remake.core.scope import (
    io_hash,
    io_match,
    legacy_io_hash,
    legacy_uses_hash,
    rule_uses_hash,
    uses_hash,
    uses_match,
)
from remake.util.code_compare import CodeComparer

# Captured from Python 3.12 (`ast.dump(ast.parse(...))`, as remake <= 0.9.0a1
# stored it). 3.13+ dumps differ: they omit empty fields (review H5).
PY312_DICT_DUMP = (
    "Module(body=[Expr(value=Dict(keys=[Constant(value='b'), Constant(value='a')], "
    "values=[Constant(value='out/{year}.txt'), Constant(value='in/{year}.txt')]))], "
    "type_ignores=[])"
)
PY312_SCALE_DUMP = (
    "Module(body=[FunctionDef(name='scale', args=arguments(posonlyargs=[], "
    "args=[arg(arg='x')], kwonlyargs=[], kw_defaults=[], defaults=[]), "
    "body=[Expr(value=Constant(value='Doc.')), Return(value=BinOp(left=Name(id='x', "
    "ctx=Load()), op=Mult(), right=Constant(value=2)))], decorator_list=[], "
    "type_params=[])], type_ignores=[])"
)


def scale(x):
    """Doc."""
    return x * 2


def _rule_ns(**uses):
    return SimpleNamespace(uses=uses, remake=None)


# --- values -----------------------------------------------------------------

def test_set_rendering_is_independent_of_hash_seed():
    # H4: str hashing is randomised per process, so repr(set) is not stable.
    code = ("from remake.core.scope import uses_hash; "
            "print(uses_hash({'VARS': {'tas', 'pr', 'ua', 'va', 'rlut'}}))")
    outs = {subprocess.run([sys.executable, '-c', code], capture_output=True, text=True,
                           env={'PYTHONHASHSEED': str(seed), 'PATH': ''},
                           check=True).stdout
            for seed in (1, 2, 3, 4)}
    assert len(outs) == 1


def test_dict_key_order_is_not_a_change():
    assert uses_hash({'d': {'a': 1, 'b': 2}}) == uses_hash({'d': {'b': 2, 'a': 1}})


def test_numpy_array_change_beyond_repr_truncation_is_seen():
    np = pytest.importorskip('numpy')
    a = np.zeros(5000)
    b = a.copy()
    b[1000] = 5
    assert uses_hash({'W': a}) != uses_hash({'W': b})
    # Precision beyond repr's 8 significant digits (M4).
    assert uses_hash({'x': np.array([0.123456789])}) != uses_hash({'x': np.array([0.123456788])})
    assert uses_hash({'W': a}) == uses_hash({'W': a.copy()})


def test_numpy_object_array_is_stable():
    np = pytest.importorskip('numpy')
    arr = np.array([{'a': 1}, 'x'], dtype=object)
    assert uses_hash({'o': arr}) == uses_hash({'o': np.array([{'a': 1}, 'x'], dtype=object)})


def test_partial_ufunc_and_builtin_changes_are_seen():
    # M3: these used to collapse to a type label.
    assert uses_hash({'f': functools.partial(scale, 2)}) != uses_hash(
        {'f': functools.partial(scale, 3)})
    import math
    assert uses_hash({'f': math.sqrt}) != uses_hash({'f': math.exp})
    np = pytest.importorskip('numpy')
    assert uses_hash({'f': np.log}) != uses_hash({'f': np.log10})
    assert uses_hash({'t': np.float32}) != uses_hash({'t': np.float64})


def test_callable_instance_with_address_repr_is_stable():
    class Scaler:
        def __init__(self, k):
            self.k = k

        def __call__(self, x):
            return x * self.k

    assert uses_hash({'s': Scaler(2)}) == uses_hash({'s': Scaler(2)})
    assert uses_hash({'s': Scaler(2)}) != uses_hash({'s': Scaler(3)})


def test_containers_of_functions_render_by_source():
    def g(x):
        return x + 1
    rendered = uses_hash({'fns': [scale, g]})
    assert 'Return' in rendered and ' at 0x' not in rendered


def test_remake_hash_hook():
    class Model:
        def __init__(self, version):
            self.version = version

        def __remake_hash__(self):
            return f'v{self.version}'

    assert uses_hash({'m': Model(1)}) == uses_hash({'m': Model(1)})
    assert uses_hash({'m': Model(1)}) != uses_hash({'m': Model(2)})


def test_function_docstring_is_not_a_change():
    def f1(x):
        """One."""
        return x

    def f2(x):
        """Two."""
        return x

    assert (canonical.callable_source_ast(f1).replace('f1', 'f')
            == canonical.callable_source_ast(f2).replace('f2', 'f'))


# --- run code (H2, L7) --------------------------------------------------------

RUN_V1 = '''
@rule(outputs={'o': 'o/{year}.txt'}, matrix={'year': [2000, 2001]},
      config={'slurm': {'mem': '1G'}})
def proc(outputs, year):
    """Process."""
    return year
'''


def test_rule_decorator_edits_are_not_run_code_changes():
    v2 = RUN_V1.replace("[2000, 2001]", "[2000, 2001, 2002]").replace("'1G'", "'8G'")
    v2 = v2.replace('"""Process."""', '"""Process one year."""')
    assert CodeComparer()(RUN_V1, v2)


def test_decorator_below_rule_is_a_run_code_change():
    v1 = RUN_V1.replace('def proc', '@retry(3)\ndef proc')
    v2 = RUN_V1.replace('def proc', '@retry(4)\ndef proc')
    assert not CodeComparer()(v1, v2)


def test_body_edit_is_a_run_code_change():
    assert not CodeComparer()(RUN_V1, RUN_V1.replace('return year', 'return year + 1'))


# --- legacy stored state (upgrade without a mass rerun) ----------------------

def test_legacy_uses_from_this_python_matches():
    r = _rule_ns(THRESHOLD=0.5, scale=scale, NAMES=['a', 'b'])
    assert uses_match(legacy_uses_hash(r.uses), r) == 'legacy'
    assert uses_match(rule_uses_hash(r), r) == 'exact'


def test_legacy_uses_set_in_any_order_matches():
    r = _rule_ns(VARS={'tas', 'pr', 'ua'})
    for stored in ("VARS={'pr', 'tas', 'ua'}", "VARS={'ua', 'tas', 'pr'}"):
        assert uses_match(stored, r) == 'legacy'
    assert uses_match("VARS={'pr', 'tas'}", r) is None


def test_legacy_uses_function_dump_from_python_312_matches():
    r = _rule_ns(scale=scale)
    assert uses_match(f'scale={PY312_SCALE_DUMP}', r) == 'legacy'
    changed = PY312_SCALE_DUMP.replace('Constant(value=2)', 'Constant(value=3)')
    assert uses_match(f'scale={changed}', r) is None


def test_legacy_io_dump_from_python_312_matches():
    @rule(inputs={'a': 'in/{year}.txt'}, outputs={'b': 'out/{year}.txt'},
          matrix={'year': [1]})
    def r(inputs, outputs, year):
        pass

    # inputs/outputs dumps as 3.12 would have stored them (keys sorted
    # in our golden dump for each part).
    one = "Module(body=[Expr(value=Dict(keys=[Constant(value='a')], values=[Constant(value='in/{year}.txt')]))], type_ignores=[])"
    two = "Module(body=[Expr(value=Dict(keys=[Constant(value='b')], values=[Constant(value='out/{year}.txt')]))], type_ignores=[])"
    assert io_match(f'inputs={one}\noutputs={two}', r) == 'legacy'
    assert io_match(legacy_io_hash(r), r) == 'legacy'
    assert io_match(io_hash(r), r) == 'exact'
    wrong = two.replace('out/', 'elsewhere/')
    assert io_match(f'inputs={one}\noutputs={wrong}', r) is None


def test_io_dict_key_order_is_not_a_change():
    @rule(outputs={'b': 'out/{year}.txt', 'a': 'in/{year}.txt'}, matrix={'year': [1]})
    def r1(outputs, year):
        pass

    @rule(outputs={'a': 'in/{year}.txt', 'b': 'out/{year}.txt'}, matrix={'year': [1]})
    def r2(outputs, year):
        pass

    assert io_hash(r1) == io_hash(r2)
    assert io_match(f'inputs=\noutputs={PY312_DICT_DUMP}', r2) == 'legacy'


def test_lambda_spec_is_cut_from_its_line():
    # A lambda's getsource is its whole line: the matrix next to it must not
    # count as an io change.
    @rule(inputs=lambda year: {'p': f'a/{year - 1}.txt'}, outputs={'o': 'b/{year}.txt'}, matrix={'year': [2001]})
    def r1(inputs, outputs, year):
        pass

    @rule(inputs=lambda year: {'p': f'a/{year - 1}.txt'}, outputs={'o': 'b/{year}.txt'}, matrix={'year': [2001, 2002]})
    def r2(inputs, outputs, year):
        pass

    assert canonical.io_part(r1.inputs) == canonical.io_part(r2.inputs)


def _pipeline(tmp_path):
    @rule(outputs={'o': str(tmp_path / 'a_{year}.txt')}, matrix={'year': [1, 2]},
          uses={'VARS': {'x', 'y', 'z'}, 'scale': scale})
    def a(outputs, year):
        Path(outputs['o']).write_text(str(scale(year)))

    db = Sqlite3Backend(tmp_path / 'remake.db')
    return Remake(rules=[a], metadata=db), a, db


def test_upgrade_from_legacy_records_reruns_nothing_and_restamps(tmp_path):
    rmk, a, db = _pipeline(tmp_path)
    assert rmk.run() == 0
    # Rewrite the records as remake <= 0.9.0a1 stored them.
    legacy_uses = db._intern_code(legacy_uses_hash(a.uses))
    legacy_io = db._intern_code(legacy_io_hash(a))
    db.conn.execute('UPDATE task SET uses_code_id = ?, io_code_id = ?', (legacy_uses, legacy_io))
    db.conn.commit()

    runnable, _ = rmk.plan()
    assert runnable == []
    assert rmk.run() == 0  # nothing to do, but restamps
    ids = set(db.conn.execute('SELECT uses_code_id, io_code_id FROM task'))
    assert ids == {(db.rule_ids['a'][2], db.rule_ids['a'][3])}


def test_real_change_after_upgrade_still_reruns(tmp_path):
    rmk, a, db = _pipeline(tmp_path)
    assert rmk.run() == 0
    legacy_uses = db._intern_code(legacy_uses_hash({**a.uses, 'VARS': {'x', 'y'}}))
    db.conn.execute('UPDATE task SET uses_code_id = ?', (legacy_uses,))
    db.conn.commit()
    runnable, _ = rmk.plan()
    assert len(runnable) == 2


# --- M8: resolved-path digest --------------------------------------------------

YEARS = [2000, 2001, 2002]


def _fan_in(tmp_path):
    @rule(inputs=lambda: {y: str(tmp_path / f'in_{y}.txt') for y in YEARS},
          outputs={'o': str(tmp_path / 'agg.txt')})
    def agg(inputs, outputs):
        Path(outputs['o']).write_text(str(len(inputs)))

    for y in (2000, 2001, 2002):
        (tmp_path / f'in_{y}.txt').write_text('x')
    db = Sqlite3Backend(tmp_path / 'remake.db')
    return Remake(rules=[agg], metadata=db), agg, db


def test_fan_in_reruns_when_its_callable_spec_resolves_differently(tmp_path):
    global YEARS
    rmk, agg, db = _fan_in(tmp_path)
    assert rmk.run() == 0
    assert rmk.plan()[0] == []
    YEARS = [2000, 2001]
    try:
        runnable, _ = rmk.plan()
        assert len(runnable) == 1
        _, reasons = rmk.explain_task(runnable[0])
        assert any('inputs 3 → 2' in r.message for r in reasons)
    finally:
        YEARS = [2000, 2001, 2002]


def test_paths_digest_is_backfilled_for_old_records(tmp_path):
    global YEARS
    rmk, agg, db = _fan_in(tmp_path)
    assert rmk.run() == 0
    db.conn.execute('UPDATE task SET paths_digest = NULL')
    db.conn.commit()
    assert rmk.plan()[0] == []  # NULL: not a rerun cause
    assert rmk.run() == 0       # ...but backfilled
    assert db.conn.execute('SELECT paths_digest FROM task').fetchone()[0] is not None
    YEARS = [2000]
    try:
        assert len(rmk.plan()[0]) == 1
    finally:
        YEARS = [2000, 2001, 2002]


# --- M19: canonical task keys ----------------------------------------------------

def test_numpy_and_enum_matrix_values_get_plain_keys(tmp_path):
    import enum
    np = pytest.importorskip('numpy')

    class Site(str, enum.Enum):
        A = 'oxford'

    @rule(outputs={'o': str(tmp_path / 'o_{x}_{s}.txt')},
          matrix={'x': [np.float64(0.5)], 's': [Site.A]})
    def r(outputs, x, s):
        assert type(x) is float and type(s) is str
        Path(outputs['o']).write_text('x')

    from remake.core.dag import expand_rule
    (task,) = expand_rule(r)
    assert task.kwargs == {'x': 0.5, 's': 'oxford'}
    assert task.legacy_key is not None and task.legacy_key != task.key


def test_record_under_legacy_key_is_found_and_rekeyed(tmp_path):
    np = pytest.importorskip('numpy')

    @rule(outputs={'o': str(tmp_path / 'o_{x}.txt')}, matrix={'x': [np.float64(0.5)]})
    def r(outputs, x):
        Path(outputs['o']).write_text('x')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[r], metadata=db)
    assert rmk.run() == 0
    from remake.core.dag import expand_rule
    (task,) = expand_rule(r)
    # As stored by remake <= 0.9.0a1 under numpy 2: the np.float64 repr key.
    db.conn.execute('UPDATE task SET key = ?', (task.legacy_key,))
    db.conn.commit()
    assert rmk.plan()[0] == []
    rmk.run(force=True)
    assert [k for (k,) in db.conn.execute('SELECT key FROM task')] == [task.key]
