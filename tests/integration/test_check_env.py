"""`remake check-env`: does a job import the same code as the planner?

The job side is a real subprocess (a `remake` shim on a tmp PATH that runs
this checkout's remake_cmd); srun is mocked by a shim that records its argv
and execs the wrapped command, so real SLURM is never called.
"""
import json
import os
import sys
from pathlib import Path

import pytest

import remake
from remake.remake_cmd import remake_cmd

SRC = str(Path(remake.__file__).resolve().parent.parent)

PIPELINE = """
import mymod
from pathlib import Path
from remake import Remake, rule

rmk = Remake(config={'slurm': {'partition': 'short', 'account': 'myacc'}})

@rule(outputs={'o': 'out.txt'})
def Write(outputs):
    Path(outputs['o']).write_text(mymod.WHERE)

rmk.rules_from_current_module()
"""

REMAKE_SHIM = f"""#!{sys.executable}
import sys
sys.path.insert(0, {SRC!r})
from remake.remake_cmd import remake_cmd
sys.exit(remake_cmd())
"""

SRUN_SHIM = """#!/bin/bash
echo "$@" >> "$(dirname "$0")/srun.log"
while [[ "$1" == --* ]]; do shift; done
exec "$@"
"""


def _make_mod(root, label):
    root.mkdir(parents=True)
    (root / 'mymod.py').write_text(f"WHERE = {label!r}\n")
    return root


@pytest.fixture
def env(tmp_path, monkeypatch):
    work = tmp_path / 'work'
    work.mkdir()
    shim = tmp_path / 'shim'
    shim.mkdir()
    for name, text in [('remake', REMAKE_SHIM), ('srun', SRUN_SHIM)]:
        (shim / name).write_text(text)
        (shim / name).chmod(0o755)
    copy_a = _make_mod(tmp_path / 'a', 'a')
    copy_b = _make_mod(tmp_path / 'b', 'b')
    (work / 'pipeline.py').write_text(PIPELINE)
    monkeypatch.chdir(work)
    monkeypatch.setenv('PATH', f'{shim}{os.pathsep}{os.environ["PATH"]}')
    monkeypatch.setenv('PYTHONPATH', str(copy_a))
    monkeypatch.syspath_prepend(str(copy_a))
    for name in ('mymod', 'pipeline'):
        monkeypatch.delitem(sys.modules, name, raising=False)
    yield {'work': work, 'shim': shim, 'a': copy_a, 'b': copy_b}
    sys.modules.pop('mymod', None)
    sys.modules.pop('pipeline', None)


def test_matching_environments_exit_0(env, capsys):
    assert remake_cmd(['remake', 'check-env', 'pipeline.py']) == 0
    out = capsys.readouterr().out
    assert 'MISMATCH' not in out
    assert 'OK  module mymod' in out
    assert 'All match.' in out


def test_different_project_module_copy_is_mismatch(env, monkeypatch, capsys):
    # Planning imported copy a; the job's environment points at copy b.
    monkeypatch.setenv('PYTHONPATH', str(env['b']))
    assert remake_cmd(['remake', 'check-env', 'pipeline.py']) == 1
    out = capsys.readouterr().out
    assert 'MISMATCH  module mymod resolves to a different file' in out
    assert str(env['a'].resolve()) in out and str(env['b'].resolve()) in out


def test_json_output(env, monkeypatch, capsys):
    monkeypatch.setenv('PYTHONPATH', str(env['b']))
    assert remake_cmd(['remake', 'check-env', 'pipeline.py', '--json']) == 1
    data = json.loads(capsys.readouterr().out)
    assert set(data) == {'format', 'planning', 'job', 'mismatches'}
    assert data['planning']['modules']['mymod']['file'].endswith('a/mymod.py')
    assert data['job']['modules']['mymod']['file'].endswith('b/mymod.py')
    assert data['planning']['remake_version'] == data['job']['remake_version']
    assert len(data['mismatches']) == 1


def test_remake_missing_from_path_is_mismatch(env, monkeypatch, capsys):
    monkeypatch.setenv('PATH', str(env['work']))
    assert remake_cmd(['remake', 'check-env', 'pipeline.py']) == 1
    out = capsys.readouterr().out
    assert 'MISMATCH' in out and 'not on PATH' in out


def test_srun_wraps_job_command(env, capsys):
    assert remake_cmd(['remake', 'check-env', 'pipeline.py', '--srun']) == 0
    calls = (env['shim'] / 'srun.log').read_text().splitlines()
    assert len(calls) == 1
    assert calls[0].startswith('--ntasks=1 --time=00:05:00 --partition=short '
                               '--account=myacc ')
    assert calls[0].endswith('_env-report pipeline.py --json')
    assert 'All match.' in capsys.readouterr().out


def test_without_srun_srun_is_not_called(env):
    remake_cmd(['remake', 'check-env', 'pipeline.py'])
    assert not (env['shim'] / 'srun.log').exists()


def test_bad_remakefile_is_usage_error(env, capsys):
    assert remake_cmd(['remake', 'check-env', 'nope.py']) == 2


def test_package_fingerprint_difference_is_named(env, monkeypatch, capsys):
    # Planning sees an extra package and a different version of another.
    from remake import provenance

    real = provenance.current_environment()
    packages = [p for p in real[1] if not p.startswith('pytest==')]
    packages += ['zzz-extra==1.0', 'pytest==0.0.1']
    monkeypatch.setattr(provenance, 'current_environment',
                        lambda: (provenance._sha256('x'), tuple(packages)))
    assert remake_cmd(['remake', 'check-env', 'pipeline.py']) == 1
    out = capsys.readouterr().out
    assert 'MISMATCH  installed packages differ' in out
    assert 'zzz-extra==1.0 only in planning' in out
    assert 'pytest: planning 0.0.1' in out


def test_same_packages_reported(env, capsys):
    assert remake_cmd(['remake', 'check-env', 'pipeline.py']) == 0
    assert 'OK  same packages' in capsys.readouterr().out
