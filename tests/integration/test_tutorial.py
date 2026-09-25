"""The tutorial's lessons replayed against their spec: a behaviour change
that would make a lesson wrong fails here, not in front of a learner."""
import io
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from remake.tutorial import watch, workspace
from remake.tutorial.cli import main as tutorial_cli
from remake.tutorial.lessons import LESSONS

pytestmark = pytest.mark.skipif(shutil.which('git') is None, reason='needs git')


@pytest.fixture
def ws(tmp_path, monkeypatch):
    monkeypatch.delenv('REMAKE_ORIGIN', raising=False)
    return workspace.init(tmp_path / 'tut')


def last_invocation(ws):
    return watch.read_invocations(ws / '.remake/remake.jsonl')[-1][1]


def test_lessons_replay_as_specified(ws):
    for les in LESSONS:
        assert (ws / 'pipeline.py').read_text() == \
            workspace.git(ws, 'show', f'lesson-{les.number}:pipeline.py') + '\n'
        for step in les.steps:
            if step.snapshot:
                workspace.apply_snapshot(ws, step.snapshot)
            for c in step.commands:
                code = workspace.run_command(ws, c.cmd)
                if not c.cmd.startswith('remake '):
                    assert code == 0, c.cmd
                    continue
                got = last_invocation(ws)
                for key, want in c.expect.items():
                    assert got[key] == want, (step.id, c.cmd, key, got)
        # The next lesson starts from its own reference remakefile.
        next_les = next((n for n in LESSONS if n.number == les.number + 1), None)
        if next_les:
            workspace.apply_snapshot(ws, next_les.snapshot)
            workspace.git(ws, 'reset', '-q', f'lesson-{next_les.number}')


def test_init_tags_and_starts_at_lesson_1(ws):
    tags = workspace.git(ws, 'tag').split()
    assert {'lesson-1', 'lesson-2', 'snapshot-2-durham'} <= set(tags)
    assert workspace.git(ws, 'status', '--porcelain') == ''  # clean, at lesson 1
    assert (ws / '.claude/skills/remake-tutor/SKILL.md').exists()
    with pytest.raises(SystemExit, match='not an empty directory'):
        workspace.init(ws)


def test_reset_stashes_and_rebuilds(ws, monkeypatch):
    (ws / 'pipeline.py').write_text('# my broken attempt\n')
    monkeypatch.chdir(ws)
    assert tutorial_cli(['reset', '2']) == 0
    assert 'before reset to lesson 2' in workspace.git(ws, 'stash', 'list')
    assert workspace.git(ws, 'diff', 'lesson-2') == ''
    # Lesson 1 was replayed: aberdeen/2020 is clean, the lesson-2 run plans 12.
    assert (ws / 'data/clean/aberdeen/2020.csv').exists()
    assert workspace.run_command(ws, 'remake run pipeline.py -n') == 0
    assert last_invocation(ws)['planned'] == 12
    assert json.loads((ws / '.tutorial/workspace.json').read_text()) == {'lesson': 2}


def test_watcher_reports_learner_commands_only(ws, monkeypatch):
    workspace.run_command(ws, 'python make_data.py')
    workspace.run_command(ws, 'remake run pipeline.py')
    monkeypatch.setenv('REMAKE_ORIGIN', 'tutor')
    workspace.run_command(ws, 'remake run pipeline.py -n')
    out, pending = io.StringIO(), {}
    for line in (ws / '.remake/remake.jsonl').read_text().splitlines():
        watch._handle(line, pending, out)
    lines = out.getvalue().splitlines()
    assert lines == ['[remake] remake run pipeline.py -> exit 0 | planned 1 | ran clean:1']


def test_watcher_follows_a_deleted_and_recreated_log(ws, monkeypatch):
    # .remake/ deleted mid-session (lesson 6 does this on purpose): the
    # watcher says so and picks up the new log.
    monkeypatch.chdir(ws)
    workspace.run_command(ws, 'python make_data.py')
    workspace.run_command(ws, 'remake run pipeline.py')
    proc = subprocess.Popen(
        [sys.executable, '-c', 'from remake.tutorial.watch import follow; follow(poll=0.05)'],
        cwd=ws, stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().startswith('[watch] following')
        shutil.rmtree(ws / '.remake')
        assert 'gone' in proc.stdout.readline()
        workspace.run_command(ws, 'remake run pipeline.py')
        assert proc.stdout.readline().startswith('[remake] remake run pipeline.py -> exit 0')
    finally:
        proc.kill()
        proc.wait()


def test_reset_loses_nothing(ws, monkeypatch):
    # Review of the prototype: reset dropped the learner's own commits
    # (nothing to stash) and deleted files they had put under data/.
    monkeypatch.chdir(ws)
    workspace.run_command(ws, 'python make_data.py')
    (ws / 'data' / 'mine.csv').write_text('my notes')
    (ws / 'notes.txt').write_text('learner work')
    workspace.git(ws, 'add', 'notes.txt')
    workspace.git(ws, 'commit', '-q', '-m', 'my notes')
    mine = workspace.git(ws, 'rev-parse', 'HEAD')

    kept = workspace.reset(ws, 1)
    branch = next(k for k in kept if k.startswith('your commits')).split()[-1]
    assert workspace.git(ws, 'rev-parse', branch) == mine
    backup = next(k for k in kept if 'data/' in k).split()[-1]
    assert (ws / backup / 'data' / 'mine.csv').read_text() == 'my notes'
    assert not (ws / 'notes.txt').exists()  # the lesson's state, not theirs


def test_init_relative_path_and_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ws = workspace.init('my-tutorial')
    assert ws == tmp_path / 'my-tutorial' and (ws / 'pipeline.py').exists()
    (tmp_path / 'afile').write_text('x')
    with pytest.raises(SystemExit, match='not an empty directory'):
        workspace.init('afile')


def test_unknown_lesson_is_a_clean_error():
    with pytest.raises(SystemExit, match='no lesson 99'):
        tutorial_cli(['lesson', '99'])


def test_log_skips_a_partial_last_line(ws):
    workspace.run_command(ws, 'python make_data.py')
    workspace.run_command(ws, 'remake run pipeline.py')
    log = ws / '.remake/remake.jsonl'
    log.write_text(log.read_text() + '{"record": {"extra": {"run_')
    assert len(watch.read_invocations(log)) == 1


def test_watcher_reads_records_written_just_before_rotation(tmp_path):
    # A command's last records land in the file just before it is renamed
    # away (loguru rotation): they must still be reported.
    import threading

    log = tmp_path / 'remake.jsonl'
    log.write_text('')
    out = io.StringIO()
    threading.Thread(target=watch.follow, args=(log, 0.2, out), daemon=True).start()

    def rec(event, **extra):
        return json.dumps({'record': {'message': "argv: ['remake', 'run', 'p.py']",
                                      'extra': {'run_id': 'r1', 'event': event, **extra}}})

    import time
    time.sleep(0.3)  # the watcher has opened the file
    with open(log, 'a') as f:
        f.write(rec('invocation') + '\n' + rec('invocation_end', exit_code=0) + '\n')
    log.rename(tmp_path / 'remake.2026.jsonl')  # before the next poll
    log.write_text('')
    deadline = time.time() + 5
    while '[remake]' not in out.getvalue() and time.time() < deadline:
        time.sleep(0.05)
    assert '[remake] remake run p.py -> exit 0' in out.getvalue()
