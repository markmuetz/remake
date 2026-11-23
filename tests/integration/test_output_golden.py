"""Golden (snapshot) tests of every human-facing command's text output
(0.11 item 8): a small fixture project, each command at 80 and 120 columns
with --colour never, compared with tests/integration/golden/. MM reviewed
the snapshots once; a change to them is a change to what users see.

Regenerate after an intended change, then review the diff:

    UPDATE_GOLDEN=1 uv run pytest tests/integration/test_output_golden.py

Times, the host name, durations and versions are normalised to fixed text
of the same width (so alignment is still checked).
"""
import contextlib
import io
import os
import re
import socket
import sys
from pathlib import Path

import pytest

from remake.remake_cmd import remake_cmd

GOLDEN = Path(__file__).parent / 'golden'
UPDATE = os.environ.get('UPDATE_GOLDEN') == '1'
WIDTHS = (80, 120)

PIPELINE = '''
from pathlib import Path
from remake import Defer, Remake, deferrable, rule

rmk = Remake()
SITES = ['oxford', 'cambridge']


@rule(inputs={'raw': 'raw/{site}.txt'}, outputs={'clean': 'out/clean/{site}.txt'},
      matrix={'site': SITES})
def clean(inputs, outputs, site):
    """Strip blank lines from a site's raw readings."""
    text = Path(inputs['raw']).read_text()
    Path(outputs['clean']).write_text(''.join(l for l in text.splitlines(True) if l.strip()))


@rule(inputs={'clean': 'out/clean/{site}.txt'}, outputs={'stats': 'out/stats/{site}.txt'},
      matrix={'site': SITES}, depends_on=[clean])
def stats(inputs, outputs, site):
    if site == 'cambridge':
        raise ValueError(f'bad readings for {site}')
    n = len(Path(inputs['clean']).read_text().splitlines())
    Path(outputs['stats']).write_text(f'{n}\\n')


@deferrable
def found():
    raise Defer('out/stats/cambridge.txt')


@rule(inputs={'stats': 'out/stats/oxford.txt'}, outputs={'o': 'out/dyn/{k}.txt'},
      matrix=found, depends_on=[stats])
def dyn(inputs, outputs, k):
    pass


@rule(inputs={'clean': 'out/clean/{site}.txt'}, outputs={'r': 'out/report_{site}.txt'},
      matrix={'site': SITES}, depends_on=[dyn])
def report(inputs, outputs, site):
    pass


rmk.rules_from_current_module()
'''

# A second remakefile for lint's findings (a pipeline with a typo can't run).
MISWIRED = '''
from remake import Remake, rule

rmk = Remake()


@rule(outputs={'o': 'out/very/long/directory/name/for/wrapping/gen_{n}.txt'},
      matrix={'n': [1, 2]})
def gen(outputs, n):
    pass


@rule(inputs={'i': 'out/very/long/directory/name/for/wrapping/genn_{n}.txt'},
      outputs={'o': 'out/proc_{n}.txt'}, matrix={'n': [1, 2]}, depends_on=[gen])
def proc(inputs, outputs, n):
    pass


@rule(inputs={'i': 'out/proc_{n}.txt', 'ext': 'raw/shared.txt'},
      outputs={'o': 'out/agg_{n}.txt'}, matrix={'n': [1, 2]})
def agg(inputs, outputs, n):
    pass


rmk.rules_from_current_module()
'''

TASK = 'stats[site=oxford]'


def _run(argv, columns, colour='never'):
    out, err = io.StringIO(), io.StringIO()
    old = os.environ.get('COLUMNS')
    os.environ['COLUMNS'] = str(columns)
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = remake_cmd(['remake', '--colour', colour, *argv])
    finally:
        if old is None:
            os.environ.pop('COLUMNS', None)
        else:
            os.environ['COLUMNS'] = old
    return code, out.getvalue()


@pytest.fixture(scope='module')
def project(tmp_path_factory):
    root = tmp_path_factory.mktemp('golden')
    cwd = os.getcwd()
    os.chdir(root)
    try:
        Path('pipeline.py').write_text(PIPELINE)
        Path('miswired.py').write_text(MISWIRED)
        Path('raw').mkdir()
        for site in ('oxford', 'cambridge'):
            Path(f'raw/{site}.txt').write_text('1\n\n2\n3\n')
        _run(['run', 'pipeline.py'], 120)  # stats[cambridge] fails
        _, out = _run(['ls-tasks', '--json', '-Q', "rule == 'stats' and site == 'oxford'",
                       'pipeline.py'], 120)
        import json

        key = json.loads(out)['tasks'][0]['key'][:8]
    finally:
        os.chdir(cwd)
    return root, key


def commands(key):
    return {
        'info': ['info', 'pipeline.py'],
        'info_tasks': ['info', '-t', '-Q', "site == 'oxford'", 'pipeline.py'],
        'info_sizes_reasons': ['info', '--sizes', '--reasons', 'pipeline.py'],
        'info_failed': ['info', '--failed', 'pipeline.py'],
        'lint': ['lint', 'miswired.py'],
        'why': ['why', 'pipeline.py', key],
        'why_runnable': ['why', 'pipeline.py'],
        'history_runs': ['history', '--runs', 'pipeline.py'],
        'history_task': ['history', 'pipeline.py', key],
        'rule_dag': ['rule-dag', '-N', '-M', 'pipeline.py'],
        'ls_tasks': ['ls-tasks', 'pipeline.py'],
        'ls_tasks_outputs': ['ls-tasks', '-o', '--check', '-Q', "rule == 'clean'", 'pipeline.py'],
        'rule_info': ['rule-info', 'pipeline.py', 'clean'],
        'task_info': ['task-info', 'pipeline.py', key],
        'verify': ['verify', '--outputs', 'pipeline.py'],
        'run_dry': ['run', '-n', 'pipeline.py'],
        'set_state_dry': ['set-state', '-Q', "rule == 'clean'", '--pending', '--dry-run',
                          'pipeline.py'],
    }


_TIME = re.compile(r'\d{4}-\d\d-\d\d[ T]\d\d:\d\d(:\d\d(\.\d+)?)?')
_DURATION = re.compile(r'\b(\d+\.\d+)s\b')
_SIZE = re.compile(r'\b\d+(\.\d+)?[KMG]?B?\b(?= )')


def _same_width(pattern, text, fill):
    return pattern.sub(lambda m: (fill * len(m.group(0)))[:len(m.group(0))], text)


def normalise(text, root):
    text = text.replace(str(root), '<PROJECT>')
    text = _same_width(_TIME, text, 'T')
    text = _same_width(_DURATION, text, 'D')
    host = socket.gethostname()
    if host:
        text = text.replace(host, ('H' * len(host)))
    v = sys.version.split()[0]
    text = text.replace(f'Python {v}', 'Python ' + 'V' * len(v))
    from remake.version import __version__

    text = text.replace(f'remake {__version__}', 'remake ' + 'V' * len(__version__))
    return text


def _cases():
    return [(name, w) for name in commands('k') for w in WIDTHS]


@pytest.mark.parametrize('name,width', _cases())
def test_golden_output(project, name, width, monkeypatch):
    root, key = project
    monkeypatch.chdir(root)
    _, out = _run(commands(key)[name], width)
    got = normalise(out, root)
    path = GOLDEN / f'{name}.{width}.txt'
    if UPDATE:
        GOLDEN.mkdir(exist_ok=True)
        path.write_text(got)
        return
    assert path.exists(), f'no snapshot {path.name}: run with UPDATE_GOLDEN=1 and review it'
    assert got == path.read_text()


@pytest.mark.parametrize('name,width', _cases())
def test_output_fits_width(project, name, width, monkeypatch):
    # No line wider than the terminal, except one holding an unbreakable
    # absolute path of the project (`config`, `check-env`).
    root, key = project
    monkeypatch.chdir(root)
    _, out = _run(commands(key)[name], width)
    long = [ln for ln in out.splitlines() if len(ln) > width and str(root) not in ln]
    assert not long, f'{name} at {width}: {long}'


_ANSI = re.compile(r'\x1b\[[0-9;]*m')


@pytest.mark.parametrize('name', list(commands('k')))
def test_colour_only_adds_escape_codes(project, name, monkeypatch):
    # --colour always, with the ANSI codes stripped, is exactly --colour never:
    # colour never shifts alignment or changes the text.
    root, key = project
    monkeypatch.chdir(root)
    _, plain = _run(commands(key)[name], 120)
    _, coloured = _run(commands(key)[name], 120, colour='always')
    # Except the `^` lines lint's path diff prints only without colour (with
    # colour the difference is the colour).
    plain = '\n'.join(ln for ln in plain.split('\n') if set(ln) != {' ', '^'})
    assert _ANSI.sub('', coloured) == plain
