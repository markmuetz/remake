"""Upgrade check: upgrading remake must rerun nothing.

Installs the previous release from PyPI into a throwaway venv (with uv), runs
every stdlib-only example plus the upgrade corpus (``tests/upgrade/corpus/``)
with it, then plans each one with the checkout (``remake run -n`` via
``uv run``) and fails listing every pipeline where any task would run.

    uv run python tests/upgrade/check_upgrade.py                # latest v* tag
    uv run python tests/upgrade/check_upgrade.py --from 0.10.0
    uv run python tests/upgrade/check_upgrade.py --keep         # keep the temp dir

A pipeline the old release cannot load or run (it uses features newer than
``--from``) is skipped with a note, not failed. A pipeline that fails under
the NEW remake, or would rerun tasks, is a failure: report it, do not
paper over it. Exit status 0 = nothing would rerun, 1 = something would.

This replaces the manual "upgrading reruns nothing" check in
design_docs/releasing.md. Needs ``uv`` and network access to PyPI.
"""
import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / 'examples'
CORPUS = Path(__file__).resolve().parent / 'corpus'

# The stdlib-only examples (examples/README.md, "need only the stdlib"), plus
# ex4, which needs only pyyaml (installed into the old venv).
# (path relative to examples/, directory to run in or None for the temp root)
EXAMPLE_CASES = [
    ('ex1_simple.py', None),
    ('ex3_uses_scope.py', None),
    ('ex4_callable_inputs.py', None),
    ('ex6_tuple_matrix.py', None),
    ('ex7_orchestration_only.py', None),
    ('ex10_dynamic_matrix.py', None),
    ('ex11_custom_token.py', None),
    ('ex12_chained_loop_rules.py', None),
    ('ex13_rule_factory.py', None),
    ('ex14_dir_outputs.py', None),
    ('ex15_config', 'pipeline.py'),
    ('ex16_diagnostics.py', None),
    ('ex17_list_values.py', None),
    ('ex18_maintenance', 'pipeline.py'),
]
# make_example_data.py arguments for the examples above.
DATA_ARGS = ['ex1', 'ex3', 'ex4', 'ex16']

# Old-release output that means "this release cannot load the pipeline".
CANNOT_LOAD = re.compile(
    r'ImportError|cannot import name|unexpected keyword|AttributeError|'
    r'NameError|is not defined|ModuleNotFoundError')


def run(cmd, cwd=None, check=True):
    """Run a command, returning the CompletedProcess (stdout and stderr merged)."""
    proc = subprocess.run(cmd, cwd=cwd, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT)
    if check and proc.returncode:
        sys.exit(f'command failed ({proc.returncode}): {" ".join(map(str, cmd))}\n{proc.stdout}')
    return proc


def latest_release_tag():
    """The newest final-release version from the repo's ``v*`` git tags."""
    out = run(['git', 'tag', '--list', 'v*', '--sort=-v:refname'], cwd=REPO).stdout.split()
    finals = [t[1:] for t in out if re.fullmatch(r'v\d+\.\d+\.\d+', t)]
    if not finals:
        sys.exit('no final v* tag found (shallow clone? use fetch-depth: 0), '
                 'or pass --from VERSION')
    return finals[0]


def build_cases(work):
    """Copy the stdlib examples and the corpus into ``work``.

    Returns [(label, directory to run in, remakefile name)].
    """
    cases = []
    shutil.copy(EXAMPLES / 'make_example_data.py', work)
    for name, inner in EXAMPLE_CASES:
        if inner is None:
            shutil.copy(EXAMPLES / name, work)
            cases.append((name, work, name))
        else:
            shutil.copytree(EXAMPLES / name, work / name)
            cases.append((f'{name}/{inner}', work / name, inner))
    for path in sorted(CORPUS.glob('*.py')):
        shutil.copy(path, work)
        cases.append((f'corpus/{path.name}', work, path.name))
    # A Dir that is a symlink to another directory (corpus/up4_dir.py).
    (work / 'data' / 'up4' / 'target').mkdir(parents=True)
    (work / 'data' / 'up4' / 'linked').symlink_to('target', target_is_directory=True)
    return cases


def main():
    """Run the check; return the process exit status."""
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--from', dest='old', metavar='VERSION',
                    help='previous release to upgrade from (default: latest v* tag)')
    ap.add_argument('--python', default='3.13', help='Python for both venvs')
    ap.add_argument('--keep', action='store_true', help='keep the temp dir')
    args = ap.parse_args()
    old = args.old or latest_release_tag()
    print(f'upgrade check: remake {old} -> checkout {REPO}')

    root = Path(tempfile.mkdtemp(prefix='remake-upgrade-'))
    try:
        venv = root / 'old-venv'
        run(['uv', 'venv', '-q', '--python', args.python, venv])
        run(['uv', 'pip', 'install', '-q', '--python', venv / 'bin' / 'python',
             f'remake=={old}', 'pyyaml'])
        old_remake = str(venv / 'bin' / 'remake')
        new_remake = ['uv', 'run', '-q', '--project', str(REPO), '--python', args.python,
                      '--with', 'pyyaml', 'remake']

        work = root / 'work'
        work.mkdir()
        cases = build_cases(work)
        run([venv / 'bin' / 'python', 'make_example_data.py', *DATA_ARGS], cwd=work)

        failures, skipped, ok = [], [], []
        for label, cwd, fname in cases:
            proc = run([old_remake, 'run', fname], cwd=cwd, check=False)
            if proc.returncode:
                if CANNOT_LOAD.search(proc.stdout):
                    skipped.append(label)
                    print(f'SKIP  {label}: not loadable by remake {old}')
                else:
                    failures.append((label, f'old release failed to run it:\n{proc.stdout}'))
                    print(f'FAIL  {label}: remake {old} failed to run it')
                continue
            proc = run([*new_remake, 'run', '-n', fname], cwd=cwd, check=False)
            m = re.search(r'^(\d+) task\(s\) would run', proc.stdout, re.M)
            if proc.returncode or not m:
                failures.append((label, f'new remake failed to plan it:\n{proc.stdout}'))
                print(f'FAIL  {label}: new remake could not plan it')
            elif int(m.group(1)):
                failures.append((label, f'{m.group(1)} task(s) would rerun:\n{proc.stdout}'))
                print(f'FAIL  {label}: {m.group(1)} task(s) would rerun')
            else:
                ok.append(label)
                print(f'ok    {label}')

        print(f'\n{len(ok)} ok, {len(skipped)} skipped, {len(failures)} failed')
        for label, why in failures:
            print(f'\n--- {label}\n{why}')
        return 1 if failures else 0
    finally:
        if args.keep:
            print(f'kept {root}')
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())
