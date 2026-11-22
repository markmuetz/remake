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

from . import provenance
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
    """The one environment report, as a dict. Call after the remakefile has
    been loaded so the modules it imports are included. Used for the SLURM
    submit header and run log (via `format_header_lines`), for `check-env`,
    and (as JSON) by `_env-report` on the job side. Besides interpreter,
    paths and imported modules it carries the `remake.provenance`
    environment fingerprint, the installed `name==version` list it hashes,
    and the nearest lockfile (name and hash, or None)."""
    import remake

    fingerprint, packages = provenance.current_environment()
    in_repo = bool(provenance.git_state(os.getcwd()))
    lockfile, lock_hash = provenance.find_lockfile(os.getcwd(), in_repo=in_repo)
    return {
        'executable': sys.executable,
        'python_version': sys.version.split()[0],
        'sys_version': sys.version.replace('\n', ' '),
        'cwd': os.getcwd(),
        'pythonpath': os.environ.get('PYTHONPATH', ''),
        'sys_path': list(sys.path),
        'remake_version': __version__,
        'remake_file': _resolve(remake.__file__),
        'remake_on_path': shutil.which('remake'),
        'argv0': sys.argv[0] if sys.argv else None,
        'modules': loaded_modules(),
        'fingerprint': fingerprint,
        'packages': list(packages),
        'lockfile': lockfile.name if lockfile else None,
        'lockfile_hash': lock_hash,
    }


def _is_user_module(info):
    """True for a module that is not in site-packages: a project module or
    a development checkout."""
    f = info['file']
    return not ('site-packages' in f or 'dist-packages' in f)


def format_header_lines(report):
    """Human-readable lines describing a report, for the SLURM submit.sh
    header and log: interpreter, remake, PYTHONPATH, sys.path, environment
    fingerprint, lockfile, then each of the user's own top-level modules
    (not stdlib, not in site-packages) with its resolved file, so a wrong
    interpreter or checkout is visible after the fact."""
    lines = [
        f'sys.executable: {report["executable"]}',
        f'sys.version: {report["sys_version"]}',
        f'remake: {report["remake_version"]} ({report["remake_file"]})',
        f'PYTHONPATH: {report["pythonpath"]}',
        f'sys.path: {report["sys_path"]}',
        f'environment fingerprint: {report["fingerprint"][:16]} '
        f'({len(report["packages"])} packages)',
    ]
    if report['lockfile']:
        lines.append(f'lockfile: {report["lockfile"]} '
                     f'(sha256 {(report["lockfile_hash"] or "unreadable")[:16]})')
    for name, info in report['modules'].items():
        if _is_user_module(info):
            lines.append(f'module {name}: {info["file"]}')
    return lines


def _parse_packages(packages):
    """{name: version} from `name==version` strings."""
    return dict(p.split('==', 1) for p in packages or [] if '==' in p)


def diff_packages(planning, job, limit=20):
    """Messages naming the `name==version` entries that differ between two
    package lists: version changes, and packages in only one. At most
    `limit` are listed, then a count of the rest."""
    a, b = _parse_packages(planning), _parse_packages(job)
    msgs = []
    for name in sorted(set(a) | set(b)):
        if name not in b:
            msgs.append(f'{name}=={a[name]} only in planning')
        elif name not in a:
            msgs.append(f'{name}=={b[name]} only in job')
        elif a[name] != b[name]:
            msgs.append(f'{name}: planning {a[name]}, job {b[name]}')
    if len(msgs) > limit:
        msgs = msgs[:limit] + [f'... and {len(msgs) - limit} more']
    return msgs


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
    fp_local, fp_job = local.get('fingerprint'), job.get('fingerprint')
    if fp_local and fp_job and fp_local != fp_job:
        # The fingerprint also covers the Python version, which is reported
        # above; only say more if the packages themselves differ.
        diffs = diff_packages(local.get('packages'), job.get('packages'))
        if diffs:
            out.append('installed packages differ: ' + '; '.join(diffs))
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
    if (job and local.get('fingerprint') and job.get('fingerprint')
            and not diff_packages(local.get('packages'), job.get('packages'), limit=1)):
        lines.append(f'OK  same packages: {len(local["packages"])} installed '
                     f'(fingerprint {local["fingerprint"][:16]})')
    if local.get('remake_on_path'):
        lines.append(f'OK  remake on PATH: {local["remake_on_path"]}')
    lines += [f'MISMATCH  {m}' for m in mismatches]
    lines.append('All match.' if not mismatches
                 else f'{len(mismatches)} mismatch(es): jobs may not import '
                      'the code you are planning with.')
    return '\n'.join(lines)
