"""Environment report and comparison for `remake check-env`.

Answers "will a SLURM job import the same code I'm planning with?". The
report is produced twice: in the calling process, and in a fresh subprocess
started the way a SLURM array element starts (``remake`` resolved from PATH,
the submitting environment exported, the remakefile's directory as cwd).
`compare_reports` lists the differences.
"""
import json
import os
import shutil
import subprocess as sp
import sys
from pathlib import Path

from .version import __version__

SRUN_TIME = '00:05:00'
# Subprocess/`srun` timeout in seconds (srun may queue for a while).
JOB_TIMEOUT = 600


def _resolve(path):
    try:
        return str(Path(path).resolve())
    except OSError:
        return str(path)


def _module_file(mod):
    f = getattr(mod, '__file__', None)
    if f:
        return _resolve(f)
    paths = list(getattr(mod, '__path__', None) or [])  # namespace package
    return _resolve(paths[0]) if paths else None


def _versions_by_module():
    from importlib import metadata

    try:
        dists = metadata.packages_distributions()
    except Exception:
        return {}
    out = {}
    for top, names in dists.items():
        for name in names:
            try:
                out[top] = metadata.version(name)
                break
            except Exception:
                continue
    return out


def loaded_modules():
    """Top-level non-stdlib modules currently imported, as
    {name: {'file': resolved path or None, 'version': str or None}}.
    Project modules and third-party packages alike; remake itself is
    reported separately."""
    stdlib = getattr(sys, 'stdlib_module_names', frozenset())
    versions = _versions_by_module()
    out = {}
    for name, mod in list(sys.modules.items()):
        if '.' in name or name.startswith('_') or name in stdlib or name == 'remake':
            continue
        if mod is None or not hasattr(mod, '__spec__'):
            continue
        f = _module_file(mod)
        if f is None:
            continue  # builtin / frozen
        out[name] = {'file': f, 'version': versions.get(name)}
    return dict(sorted(out.items()))


def environment_report():
    """The in-process report. Call after the remakefile has been loaded so
    the modules it imports are included."""
    import remake

    return {
        'executable': sys.executable,
        'python_version': sys.version.split()[0],
        'cwd': os.getcwd(),
        'pythonpath': os.environ.get('PYTHONPATH', ''),
        'sys_path': list(sys.path),
        'remake_version': __version__,
        'remake_file': _resolve(remake.__file__),
        'remake_on_path': shutil.which('remake'),
        'argv0': sys.argv[0] if sys.argv else None,
        'modules': loaded_modules(),
    }


def srun_prefix(slurm_config):
    cmd = ['srun', '--ntasks=1', f'--time={SRUN_TIME}']
    for key in ('partition', 'account', 'qos'):
        if slurm_config.get(key):
            cmd.append(f'--{key}={slurm_config[key]}')
    return cmd


def job_command(remakefile, srun=False, slurm_config=None):
    """argv that reproduces how a SLURM job starts remake, or None if
    `remake` isn't on PATH."""
    exe = shutil.which('remake')
    if exe is None:
        return None
    cmd = [exe, '_env-report', str(remakefile), '--json']
    if srun:
        cmd = srun_prefix(slurm_config or {}) + cmd
    return cmd


def job_report(remakefile, srun=False, slurm_config=None):
    """Run the report in a fresh subprocess. Returns (report, error)."""
    cmd = job_command(remakefile, srun, slurm_config)
    if cmd is None:
        return None, 'remake not found on PATH'
    try:
        proc = sp.run(
            cmd, env=dict(os.environ), cwd=os.getcwd(), capture_output=True,
            text=True, timeout=JOB_TIMEOUT,
        )
    except (OSError, sp.TimeoutExpired) as e:
        return None, f'could not run {" ".join(cmd)}: {e}'
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-5:]
        return None, (f'{" ".join(cmd)} exited {proc.returncode}: '
                      + ' | '.join(tail))
    for text in (proc.stdout, *reversed(proc.stdout.splitlines())):
        try:
            return json.loads(text), None
        except ValueError:
            continue
    return None, f'no JSON report in output of {" ".join(cmd)}'


_FIELDS = (('executable', 'Python executable'),
           ('python_version', 'Python version'),
           ('remake_version', 'remake version'),
           ('remake_file', 'remake file'))


def compare_reports(local, job, job_error=None):
    """List of MISMATCH messages; empty means all match."""
    out = []
    which = local.get('remake_on_path')
    if which is None:
        out.append('`remake` is not on PATH: SLURM jobs run a bare `remake`')
    else:
        argv0 = local.get('argv0') or ''
        if Path(argv0).name == 'remake' and _resolve(argv0) != _resolve(which):
            out.append(f'`remake` on PATH ({which}) is not the one running '
                       f'now ({argv0})')
    if job_error and which is not None:
        out.append(f'job-side report failed: {job_error}')
    if job is None:
        return out
    for key, label in _FIELDS:
        if local[key] != job[key]:
            out.append(f'{label} differs: planning {local[key]}, job {job[key]}')
    lm, jm = local['modules'], job['modules']
    for name in sorted(set(lm) & set(jm)):
        a, b = lm[name], jm[name]
        if a['file'] != b['file']:
            out.append(f'module {name} resolves to a different file: '
                       f'planning {a["file"]}, job {b["file"]}')
        elif a['version'] != b['version']:
            out.append(f'module {name} version differs: planning '
                       f'{a["version"]}, job {b["version"]}')
    return out


def format_text(local, job, mismatches):
    lines = []
    lm = local['modules']
    jm = job['modules'] if job else {}
    if job:
        for key, label in _FIELDS:
            if local[key] == job[key]:
                lines.append(f'OK  {label}: {local[key]}')
        for name in sorted(set(lm) & set(jm)):
            if lm[name] == jm[name]:
                v = f' {lm[name]["version"]}' if lm[name]['version'] else ''
                lines.append(f'OK  module {name}{v}: {lm[name]["file"]}')
    if local.get('remake_on_path'):
        lines.append(f'OK  remake on PATH: {local["remake_on_path"]}')
    lines += [f'MISMATCH  {m}' for m in mismatches]
    lines.append('All match.' if not mismatches
                 else f'{len(mismatches)} mismatch(es): jobs may not import '
                      'the code you are planning with.')
    return '\n'.join(lines)
