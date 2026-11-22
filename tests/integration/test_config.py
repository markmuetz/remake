"""Config cascade, profiles and `remake config` (util/config.py)."""
import json
import os
from pathlib import Path

import pytest
from loguru import logger

from remake import Remake, Sqlite3Backend
from remake.core.exceptions import RemakeError
from remake.remake_cmd import remake_cmd
from remake.util import config as cfgmod

PIPELINE = '''
from pathlib import Path
from remake import Remake, rule

rmk = Remake({cfg})

@rule(outputs={{'o': 'out.txt'}}{rule_cfg})
def make(outputs):
    Path(outputs['o']).write_text('x')

rmk.rules_from_current_module()
'''


def write_pipeline(cfg='', rule_cfg=''):
    Path('p.py').write_text(PIPELINE.format(cfg=f'config={cfg}' if cfg else '',
                                            rule_cfg=rule_cfg))


def cli(*args):
    return remake_cmd(['remake', *args])


@pytest.fixture
def proj(tmp_path, monkeypatch):
    xdg = tmp_path / 'xdg'
    (xdg / 'remake').mkdir(parents=True)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(xdg))
    monkeypatch.delenv('REMAKE_PROFILE', raising=False)
    proj = tmp_path / 'proj'
    proj.mkdir()
    monkeypatch.chdir(proj)
    return proj


def user_file(text):
    cfgmod.user_config_path().write_text(text)


def project_file(text):
    Path('remake.toml').write_text(text)


def eff(**kw):
    return cfgmod.resolve(Path.cwd(), **kw)


def warnings_during(fn):
    # Not a loguru sink: the CLI resets all handlers at start-up.
    msgs = []
    sink = logger.add(lambda m: msgs.append(str(m)), level='WARNING')
    try:
        fn()
    finally:
        try:
            logger.remove(sink)
        except ValueError:
            pass
    return msgs


# --- precedence and merging ---


def test_defaults_only(proj):
    e = eff()
    assert e.values == {'slurm': {'partition': 'standard', 'qos': 'standard',
                                  'time': '4:00:00', 'mem': '4G'}}
    assert e.profile is None
    assert e.provenance['slurm.mem'] == 'builtin default'


def test_every_layer_precedence(proj):
    user_file('profile = "p"\n[profiles.p.slurm]\nmem = "profile"\ntime = "1:00:00"\n'
              'qos = "profile"\npartition = "profile"\n'
              '[slurm]\ntime = "user"\nqos = "user"\npartition = "user"\n')
    project_file('[slurm]\nqos = "project"\npartition = "project"\n')
    e = eff(programmatic={'slurm': {'partition': 'remakefile'}})
    assert e.values['slurm'] == {'partition': 'remakefile', 'qos': 'project',
                                 'time': 'user', 'mem': 'profile'}
    prov = e.provenance
    assert prov['slurm.partition'] == 'Remake(config=)'
    assert prov['slurm.qos'] == 'project file'
    assert prov['slurm.time'] == 'user file'
    assert prov['slurm.mem'] == 'profile p (user file)'


def test_profile_layer_below_user_file(proj):
    e = eff(profile='jasmin')
    assert e.provenance['slurm.mem'] == 'profile jasmin'
    user_file('[slurm]\nmem = "9G"\n')
    assert eff(profile='jasmin').values['slurm']['mem'] == '9G'


def test_recursive_merge_and_list_replace(proj):
    user_file('[slurm]\naccount = "me"\nmem = "8G"\n[multiproc]\nnproc = 2\n')
    project_file('[slurm]\nmem = "16G"\n')
    e = eff(programmatic={'dask': {'nproc': 3}})
    assert e.values['slurm']['account'] == 'me'  # sibling kept
    assert e.values['slurm']['mem'] == '16G'
    assert e.values['multiproc'] == {'nproc': 2} and e.values['dask'] == {'nproc': 3}
    a, b = {'x': [1, 2], 'd': {'k': 1}}, {'x': [3], 'd': {'j': 2}}
    cfgmod.merge(a, b, 'l', {})
    assert a == {'x': [3], 'd': {'k': 1, 'j': 2}}


def test_checksum_outputs_project_not_user(proj):
    project_file('checksum_outputs = true\n')
    assert eff().values['checksum_outputs'] is True
    project_file('')
    user_file('checksum_outputs = true\n')
    with pytest.raises(RemakeError, match='checksum_outputs'):
        eff()


# --- validation ---


def test_unknown_key_in_user_file(proj):
    user_file('[slurm]\nmem = "1G"\n[nonsense]\na = 1\n')
    with pytest.raises(RemakeError) as ei:
        eff()
    assert 'config.toml' in str(ei.value) and "'nonsense'" in str(ei.value)


def test_pipeline_key_not_allowed_in_user_file(proj):
    user_file('external_inputs_hint = "hi"\n')
    with pytest.raises(RemakeError, match='config.toml.*external_inputs_hint'):
        eff()


def test_unknown_key_in_project_file(proj):
    project_file('[multiproc]\nnproc = 2\nthreads = 3\n')
    with pytest.raises(RemakeError, match=r'remake\.toml.*multiproc\.threads'):
        eff()


def test_wrong_type_and_bad_toml(proj):
    project_file('[multiproc]\nnproc = "four"\n')
    with pytest.raises(RemakeError, match='nproc must be an integer'):
        eff()
    project_file('[multiproc\n')
    with pytest.raises(RemakeError, match='invalid TOML'):
        eff()


def test_unknown_key_in_profile_table(proj):
    project_file('[profiles.x]\nfoo = 1\n')
    with pytest.raises(RemakeError, match=r'profiles\.x.*foo'):
        eff()


def test_programmatic_unknown_key_warns(proj):
    box = []
    msgs = warnings_during(lambda: box.append(Remake(config={'mine': 1, 'slurm': {'mem': '1G'}})))
    assert any("unknown key 'mine'" in m for m in msgs)
    assert box[0].config['slurm']['mem'] == '1G'


# --- profiles ---


def test_shipped_jasmin_profile_loads_without_account(proj):
    assert 'jasmin' in cfgmod.shipped_profile_names()
    e = eff(profile='jasmin')
    assert e.profile == 'jasmin'
    assert e.values['slurm'] == {'partition': 'standard', 'qos': 'standard',
                                 'time': '4:00:00', 'mem': '4G'}
    assert 'account' not in e.values['slurm']


def test_profile_selection_order(proj, monkeypatch):
    user_file('profile = "u"\n[profiles.u.slurm]\nmem = "1G"\n'
              '[profiles.p.slurm]\nmem = "2G"\n[profiles.e.slurm]\nmem = "3G"\n'
              '[profiles.c.slurm]\nmem = "4G"\n')
    assert eff().profile == 'u'
    project_file('profile = "p"\n')
    assert eff().profile == 'p'
    monkeypatch.setenv('REMAKE_PROFILE', 'e')
    assert eff().profile == 'e'
    e = eff(profile='c')
    assert e.profile == 'c' and e.profile_source == 'cli --profile'
    assert e.values['slurm']['mem'] == '4G'


def test_unknown_profile_lists_available(proj):
    user_file('[profiles.mine.slurm]\nmem = "1G"\n')
    with pytest.raises(RemakeError) as ei:
        eff(profile='nope')
    msg = str(ei.value)
    assert "'nope'" in msg and 'jasmin' in msg and 'mine' in msg


def test_project_profile_overrides_shipped(proj):
    project_file('[profiles.jasmin.slurm]\ntime = "1:00:00"\n')
    s = eff(profile='jasmin').values['slurm']
    assert s['time'] == '1:00:00' and s['mem'] == '4G'


def test_profiles_ship_as_package_data():
    try:
        import tomllib
    except ImportError:  # Python 3.10
        import tomli as tomllib
    toml = tomllib.loads((Path(__file__).parents[2] / 'pyproject.toml').read_text())
    assert 'profiles/*.toml' in toml['tool']['setuptools']['package-data']['remake']


# --- CLI ---


def test_cli_config_json(proj, capsys):
    write_pipeline("{'slurm': {'mem': '2G'}}")
    user_file('[slurm]\naccount = "acc"\n')
    assert cli('--profile', 'jasmin', 'config', 'p.py', '--json') == 0
    data = json.loads(capsys.readouterr().out)
    assert data['format'] == 1
    assert data['profile'] == 'jasmin'
    assert data['config']['slurm']['mem'] == '2G'
    assert data['provenance']['slurm.mem'] == 'Remake(config=)'
    assert data['provenance']['slurm.account'] == 'user file'
    assert data['provenance']['slurm.qos'] == 'profile jasmin'
    layers = {f['layer']: f for f in data['files']}
    assert layers['user file']['exists'] and not layers['project file']['exists']
    assert layers['project file']['path'].endswith('remake.toml')


def test_cli_config_text_and_xdg(proj, capsys):
    write_pipeline()
    project_file('[multiproc]\nnproc = 3\n')
    assert cli('config', 'p.py') == 0
    out = capsys.readouterr().out
    assert 'multiproc.nproc = 3  [project file]' in out
    assert str(cfgmod.user_config_path()) in out and 'not found' in out
    assert os.environ['XDG_CONFIG_HOME'] in out


def test_cli_config_unknown_profile_exit2(proj, capsys):
    write_pipeline()
    assert cli('--profile', 'zzz', 'config', 'p.py') == 2
    assert 'zzz' in capsys.readouterr().err


def test_xdg_default_is_home_dot_config(proj, monkeypatch, tmp_path):
    monkeypatch.delenv('XDG_CONFIG_HOME')
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    assert cfgmod.user_config_path() == tmp_path / 'home/.config/remake/config.toml'


def test_project_file_next_to_remakefile(proj, capsys):
    sub = proj / 'sub'
    sub.mkdir()
    (sub / 'remake.toml').write_text('[multiproc]\nnproc = 5\n')
    (sub / 'p.py').write_text(PIPELINE.format(cfg='', rule_cfg=''))
    assert cli('config', 'sub/p.py', '--json') == 0
    assert json.loads(capsys.readouterr().out)['config']['multiproc'] == {'nproc': 5}


# --- recording, no-rerun ---


def runs_recorded():
    db = Sqlite3Backend('.remake/remake.db')
    try:
        return [db.get_run(i) for i in range(1, 6) if db.get_run(i)]
    finally:
        db.close()


def test_record_run_gets_merged_config(proj):
    write_pipeline("{'slurm': {'mem': '2G'}}")
    project_file('[multiproc]\nnproc = 2\n')
    assert cli('run', 'p.py') == 0
    (rec,) = runs_recorded()
    assert rec['command'] == 'run' and rec['remakefile'] == 'p.py'
    assert rec['config']['slurm']['mem'] == '2G'
    assert rec['config']['multiproc'] == {'nproc': 2}


def test_dry_run_records_nothing(proj):
    write_pipeline()
    cli('run', 'p.py', '--dry-run')
    assert runs_recorded() == []


def test_set_state_records_run(proj):
    write_pipeline()
    cli('run', 'p.py')
    cli('set-state', 'p.py', '--pending', '-Q', 'make')
    assert [r['command'] for r in runs_recorded()] == ['run', 'set-state']


def test_config_change_never_reruns(proj, capsys):
    write_pipeline("{'slurm': {'mem': '2G'}}", rule_cfg=", config={'slurm': {'mem': '1G'}}")
    assert cli('run', 'p.py') == 0
    capsys.readouterr()
    user_file('[slurm]\nmem = "9G"\n[profiles.q.slurm]\nmem = "1G"\n')
    project_file('[multiproc]\nnproc = 4\n[slurm]\ntime = "9:00:00"\n')
    write_pipeline("{'slurm': {'mem': '3G'}}", rule_cfg=", config={'slurm': {'mem': '7G'}}")
    assert cli('--profile', 'jasmin', 'run', 'p.py', '--dry-run') in (0, None)
    assert '0 task(s) would run' in capsys.readouterr().out


# --- SLURM ---


def _slurm_setup(proj, monkeypatch):
    shim = proj / 'shim'
    shim.mkdir()
    sb = shim / 'sbatch'
    sb.write_text('#!/bin/bash\necho "$@" >> "$(dirname "$0")/sbatch.log"\necho 1001\n')
    sb.chmod(0o755)
    sq = shim / 'squeue'
    sq.write_text('#!/bin/bash\nexit 0\n')
    sq.chmod(0o755)
    monkeypatch.setenv('PATH', f'{shim}{os.pathsep}{os.environ["PATH"]}')
    write_pipeline()


def test_jasmin_without_account_fails_before_sbatch(proj, monkeypatch, capsys):
    _slurm_setup(proj, monkeypatch)
    assert cli('--profile', 'jasmin', 'run', 'p.py', '-E', 'slurm') == 2
    err = capsys.readouterr().err
    assert 'account' in err and 'config.toml' in err
    assert not (proj / 'shim/sbatch.log').exists()


def test_jasmin_with_account_submits(proj, monkeypatch):
    _slurm_setup(proj, monkeypatch)
    user_file('[slurm]\naccount = "acc"\n')
    assert cli('--profile', 'jasmin', 'run', 'p.py', '-E', 'slurm') == 0
    assert (proj / 'shim/sbatch.log').exists()
    assert '#SBATCH --account=acc' in Path('.remake/slurm/make.sbatch').read_text()
    assert 'REMAKE_PROFILE' not in os.environ  # restored after the command


def test_builtin_defaults_warn_without_profile(proj, monkeypatch, capsys):
    _slurm_setup(proj, monkeypatch)
    cli('run', 'p.py', '-E', 'slurm', '--dry-run')
    assert capsys.readouterr().err.count('built-in SLURM defaults') == 1


def test_no_warning_when_partition_set(proj, monkeypatch, capsys):
    _slurm_setup(proj, monkeypatch)
    project_file('[slurm]\npartition = "mine"\n')
    cli('run', 'p.py', '-E', 'slurm', '--dry-run')
    assert 'built-in SLURM defaults' not in capsys.readouterr().err
