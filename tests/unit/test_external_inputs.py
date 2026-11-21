from pathlib import Path

from remake import Remake, rule
from remake.core.tokens import FileToken


def _rmk(tmp_path, monkeypatch, hint=None):
    monkeypatch.chdir(tmp_path)

    @rule(inputs={'raw': 'raw_{n}.txt'}, outputs={'mid': 'mid_{n}.txt'},
          matrix={'n': [0, 1]})
    def first(inputs, outputs, n):
        pass

    @rule(inputs={'mid': 'mid_{n}.txt', 'tok': FileToken('extra_{n}.bin')},
          outputs={'out': 'out_{n}.txt'}, matrix={'n': [0, 1]}, depends_on=[first])
    def second(inputs, outputs, n):
        pass

    config = {'external_inputs_hint': hint} if hint else None
    rmk = Remake(rules=[first, second], config=config)
    rmk.finalize()
    return rmk


def test_flags_only_unproduced_missing_inputs(tmp_path, monkeypatch):
    rmk = _rmk(tmp_path, monkeypatch)
    Path('raw_0.txt').write_text('x')
    runnable, _ = rmk.plan()
    found = rmk.missing_external_inputs(runnable)
    # mid_* are produced by `first` (absent now) and must not be flagged.
    assert found == {'first': ['raw_1.txt'], 'second': ['extra_0.bin', 'extra_1.bin']}


def test_existing_inputs_clean_and_hint(tmp_path, monkeypatch):
    rmk = _rmk(tmp_path, monkeypatch, hint='run get_data.py')
    runnable, _ = rmk.plan()
    msg = rmk.external_inputs_error(rmk.missing_external_inputs(runnable))
    assert msg.startswith('4 input file(s) are missing') and msg.endswith('run get_data.py')
    for n in (0, 1):
        Path(f'raw_{n}.txt').write_text('x')
        Path(f'extra_{n}.bin').write_text('x')
    runnable, _ = rmk.plan()
    assert rmk.missing_external_inputs(runnable) == {}
    assert rmk.external_inputs_error({}) is None
