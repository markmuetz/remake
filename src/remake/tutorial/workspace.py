"""The tutorial workspace: a git repo the learner works in.

`init` copies the starting files, installs the tutor skill, and records
each lesson's reference remakefile as a tagged commit (`lesson-1`,
`lesson-2`, ... plus one tag per mid-lesson edit, `snapshot-<name>`)
without touching the working tree — so `git diff lesson-2` shows how far
the learner's file is from the lesson's start, and the tutor can do the
same. `reset` restores a lesson's starting state without losing anything:
uncommitted changes are stashed, the learner's own commits are kept on a
`backup/...` branch, and the old .remake/ and data/ are moved into
.tutorial/backup/<time>/. It then rebuilds .remake/ and data/ by replaying
the earlier lessons (tagged REMAKE_ORIGIN=tutor, so the watcher ignores
them) and checks out the lesson's remakefile.
"""
import io
import json
import os
import shlex
import shutil
import subprocess
import time
from contextlib import contextmanager, redirect_stdout
from importlib import resources
from pathlib import Path

from .lessons import LESSONS, SETUP, lesson

FILES = resources.files('remake.tutorial') / 'files'
SKILL = resources.files('remake.tutorial') / 'skill' / 'SKILL.md'
GITIGNORE = '.remake/\ndata/\n.tutorial/\n.claude/\n__pycache__/\n'
GIT_ID = ['-c', 'user.name=remake tutorial', '-c', 'user.email=tutorial@remake.invalid']


def git(ws, *args, env=None, input=None, check=True):
    return subprocess.run(['git', *GIT_ID, *args], cwd=ws, check=check, capture_output=True,
                          text=True, env=env, input=input).stdout.strip()


def _tree_files(snapshot):
    """{relative path: text} of a snapshot tree: common files + the snapshot."""
    files = {'.gitignore': GITIGNORE}
    for src in (FILES / 'common', FILES / snapshot):
        for f in src.iterdir():
            if f.is_file():
                files[f.name] = f.read_text()
    return files


def _commit_tree(ws, files, parent, message):
    """A commit of exactly `files`, via a scratch index (the working tree and
    the real index are untouched)."""
    env = {**os.environ, 'GIT_INDEX_FILE': str(Path(ws, '.git', 'tutorial-index'))}
    git(ws, 'read-tree', '--empty', env=env)
    for name, text in files.items():
        blob = git(ws, 'hash-object', '-w', '--stdin', input=text)
        git(ws, 'update-index', '--add', '--cacheinfo', f'100644,{blob},{name}', env=env)
    tree = git(ws, 'write-tree', env=env)
    Path(env['GIT_INDEX_FILE']).unlink()
    return git(ws, 'commit-tree', tree, *(['-p', parent] if parent else []), '-m', message)


def snapshots():
    """(tag, snapshot, message) in lesson order."""
    out = []
    for les in LESSONS:
        out.append((f'lesson-{les.number}', les.snapshot,
                    f'Lesson {les.number} start: {les.title}'))
        for s in les.steps:
            if s.snapshot and s.snapshot != les.snapshot:
                out.append((f'snapshot-{s.snapshot}', s.snapshot, f'Step {s.id}: {s.title}'))
    return out


def init(ws):
    # Absolute: git runs with cwd=ws, so a relative path (and the scratch
    # GIT_INDEX_FILE built from it) would resolve twice.
    ws = Path(ws).expanduser().resolve()
    if ws.exists() and (not ws.is_dir() or any(ws.iterdir())):
        raise SystemExit(f'{ws} exists and is not an empty directory')
    ws.mkdir(parents=True, exist_ok=True)
    git(ws, 'init', '-q', '-b', 'main')
    parent, first = None, None
    for tag, snap, message in snapshots():
        parent = _commit_tree(ws, _tree_files(snap), parent, message)
        git(ws, 'tag', tag, parent)
        first = first or parent
    # The learner starts at lesson 1: main points there, files checked out.
    git(ws, 'reset', '-q', '--hard', first)
    (ws / '.tutorial').mkdir()
    (ws / '.tutorial' / 'workspace.json').write_text(json.dumps({'lesson': 1}) + '\n')
    skill_dir = ws / '.claude' / 'skills' / 'remake-tutor'
    skill_dir.mkdir(parents=True)
    (skill_dir / 'SKILL.md').write_text(SKILL.read_text())
    return ws


@contextmanager
def _in(ws):
    old = os.getcwd()
    os.chdir(ws)
    try:
        yield
    finally:
        os.chdir(old)


def run_command(ws, cmd, quiet=False):
    """Run one lesson command in the workspace; returns the exit code.
    `remake ...` runs in-process (the same CLI entry point); anything else
    through the shell. `quiet`: warnings only on stderr (the log files are
    unaffected)."""
    if cmd.startswith('remake '):
        from ..remake_cmd import remake_cmd
        with _in(ws):
            return remake_cmd(['remake', *(['--warning'] if quiet else []),
                               *shlex.split(cmd)[1:]])
    return subprocess.run(cmd, shell=True, cwd=ws, capture_output=True).returncode


def apply_snapshot(ws, snapshot):
    for name, text in _tree_files(snapshot).items():
        if name != '.gitignore':
            Path(ws, name).write_text(text)


def emit(ws, event, **fields):
    """Append a `remake-tutorial` event for the watcher (watch.EVENTS)."""
    path = Path(ws, '.tutorial', 'events.jsonl')
    record = {'event': event, 'time': time.time(), 'pid': os.getpid(), **fields}
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record) + '\n')


def replay(ws, until_lesson):
    """Run the setup, then replay every lesson before `until_lesson`,
    silently and as the tutor's origin (the watcher skips these)."""
    old = os.environ.get('REMAKE_ORIGIN')
    os.environ['REMAKE_ORIGIN'] = 'tutor'
    # The replayed commands' output isn't the learner's. Only stdout is
    # redirected: loguru binds its stderr sink to the stream object itself,
    # so a redirected stderr would leave it logging into a dead buffer.
    try:
        with redirect_stdout(io.StringIO()):
            for cmd in SETUP:
                if run_command(ws, cmd, quiet=True) != 0:
                    raise SystemExit(f'setup: `{cmd}` failed')
            for les in LESSONS:
                if les.number >= until_lesson:
                    break
                apply_snapshot(ws, les.snapshot)
                for s in les.steps:
                    if s.snapshot:
                        apply_snapshot(ws, s.snapshot)
                    for c in s.commands:
                        code = run_command(ws, c.cmd, quiet=True)
                        if code != c.expect.get('exit', 0):
                            raise SystemExit(
                                f'replaying step {s.id}: `{c.cmd}` exited {code}; '
                                f'the workspace is only partly rebuilt')
    finally:
        if old is None:
            os.environ.pop('REMAKE_ORIGIN', None)
        else:
            os.environ['REMAKE_ORIGIN'] = old


def reset(ws, number):
    """Restore lesson `number`'s starting state. Returns a list of what was
    kept and where (for the learner)."""
    ws = Path(ws).resolve()
    les = lesson(number)
    stamp = time.strftime('%Y%m%d-%H%M%S')
    kept = []
    # Stash only real changes: a tree that already matches the lesson's
    # start (the learner just finished the previous lesson) has none.
    changed = (git(ws, 'diff', f'lesson-{les.number}', '--name-only')
               or git(ws, 'ls-files', '--others', '--exclude-standard'))
    if changed:
        message = f'before reset to lesson {number}'
        before = git(ws, 'rev-parse', '-q', '--verify', 'refs/stash', check=False)
        git(ws, 'stash', 'push', '-u', '-m', message)
        # Nothing to stash when the difference is the learner's own commits
        # (kept on a backup branch below): only claim a stash that was made.
        if git(ws, 'rev-parse', '-q', '--verify', 'refs/stash', check=False) != before:
            kept.append(f"uncommitted changes: git stash ('{message}')")
    # The learner's own commits: HEAD not reachable from any lesson tag.
    if git(ws, 'rev-list', 'HEAD', '--not', '--tags'):
        branch = f'backup/before-reset-{stamp}'
        git(ws, 'branch', branch, 'HEAD')
        kept.append(f'your commits: branch {branch}')
    backup = ws / '.tutorial' / 'backup' / stamp
    for d in ('.remake', 'data'):
        if (ws / d).exists():
            backup.mkdir(parents=True, exist_ok=True)
            shutil.move(ws / d, backup / d)
    if backup.exists():
        kept.append(f'the previous .remake/ and data/ (rebuilt for this lesson): '
                    f'{backup.relative_to(ws)}/')
    git(ws, 'reset', '-q', '--hard', f'lesson-{les.number}')
    replay(ws, number)
    git(ws, 'reset', '-q', '--hard', f'lesson-{les.number}')
    (ws / '.tutorial' / 'workspace.json').write_text(json.dumps({'lesson': number}) + '\n')
    return kept
