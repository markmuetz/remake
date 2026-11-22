"""Module scans (`rules_from_current_module`, `rules_from_modules`) claim only
rules defined in the scanned file, and warn about the rest."""
import sys

import pytest
from loguru import logger

from remake.loader import load_remake

RULES_A = '''
from remake import rule

@rule(outputs={'o': 'a.txt'})
def make_a(outputs):
    pass
'''

PIPELINE = '''
from remake import Remake, rule
from rules_a import make_a

rmk = Remake()

@rule(outputs={'o': 'b.txt'})
def make_b(outputs):
    pass

rmk.rules_from_current_module()
'''

LISTED = '''
from remake import Remake, rule

def make(n):
    @rule(name=f'r{n}', outputs={'o': f'{n}.txt'})
    def f(outputs):
        pass
    return f

rules = [make(1), make(2)]
d = {'x': make(3)}
rmk = Remake()
%s
rmk.rules_from_current_module()
'''


@pytest.fixture
def warnings_log():
    """Messages logged at WARNING+ during the test."""
    msgs = []
    sink = logger.add(lambda m: msgs.append(str(m)), level='WARNING')
    yield msgs
    logger.remove(sink)


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    """A cwd that is also importable, with its modules forgotten afterwards."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    for name in ('rules_a', 'pipe', 'listed'):
        sys.modules.pop(name, None)


def test_imported_rule_not_claimed_and_warned(workdir, warnings_log):
    (workdir / 'rules_a.py').write_text(RULES_A)
    (workdir / 'pipe.py').write_text(PIPELINE)
    rmk = load_remake('pipe.py', finalize=False)
    assert [r.name for r in rmk.rules] == ['make_b']
    assert len(warnings_log) == 1
    assert 'make_a' in warnings_log[0] and 'add_rules' in warnings_log[0]


def test_imported_rule_registered_explicitly_is_silent(workdir, warnings_log):
    (workdir / 'rules_a.py').write_text(RULES_A)
    (workdir / 'pipe.py').write_text(
        PIPELINE.replace('rmk.rules_from_current_module()',
                         'rmk.add_rules([make_a])\nrmk.rules_from_current_module()'))
    rmk = load_remake('pipe.py', finalize=False)
    assert sorted(r.name for r in rmk.rules) == ['make_a', 'make_b']
    assert warnings_log == []


def test_rules_in_containers_warned_not_claimed(workdir, warnings_log):
    (workdir / 'listed.py').write_text(LISTED % '')
    rmk = load_remake('listed.py', finalize=False)
    assert rmk.rules == []
    assert len(warnings_log) == 1
    for ref in ('rules[0]', 'rules[1]', "d['x']"):
        assert ref in warnings_log[0]


def test_rules_in_containers_registered_first_are_silent(workdir, warnings_log):
    (workdir / 'listed.py').write_text(LISTED % 'rmk.add_rules([*rules, *d.values()])')
    rmk = load_remake('listed.py', finalize=False)
    assert len(rmk.rules) == 3
    assert warnings_log == []


def test_rules_from_modules_claims_only_defined_there(workdir, warnings_log):
    (workdir / 'rules_a.py').write_text(RULES_A)
    (workdir / 'pipe.py').write_text(
        'from remake import Remake\nimport rules_a\n'
        'rmk = Remake()\nrmk.rules_from_modules(rules_a)\n')
    rmk = load_remake('pipe.py', finalize=False)
    assert [r.name for r in rmk.rules] == ['make_a']
    assert warnings_log == []
