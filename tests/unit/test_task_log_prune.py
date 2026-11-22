"""0.11 item 4 (Q4): per-task logs are one file per task, and the logs of
tasks no longer in the pipeline are pruned by Remake.prune_task_logs."""
from pathlib import Path

from remake import Remake, Sqlite3Backend, rule
from remake.util import task_log_path
from remake.util.paths import task_log_root


def _pipeline(tmp_path, years):
    @rule(outputs={'o': str(tmp_path / 'a_{year}.txt')}, matrix={'year': years})
    def a(outputs, year):
        Path(outputs['o']).write_text(str(year))

    return Remake(rules=[a], metadata=Sqlite3Backend(':memory:')), a


def _logs():
    return sorted(p.relative_to(task_log_root()) for p in task_log_root().rglob('*.log'))


def test_prune_keeps_current_logs_and_drops_stale_ones(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rmk, _ = _pipeline(tmp_path, [1, 2, 3])
    for t in rmk.tasks():
        path = task_log_path(t)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('log')
    gone = task_log_root() / 'removed_rule' / 'ab' / 'cd.log'
    gone.parent.mkdir(parents=True)
    gone.write_text('log')

    # The matrix shrinks: year 3's log is stale, as is the removed rule's.
    rmk2, _ = _pipeline(tmp_path, [1, 2])
    current = sorted(task_log_path(t).relative_to(task_log_root()) for t in rmk2.tasks())
    assert rmk2.prune_task_logs() == 2
    assert _logs() == current
    assert not (task_log_root() / 'removed_rule').exists()
    assert rmk2.prune_task_logs() == 0


def test_prune_without_logs_is_a_no_op(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rmk, _ = _pipeline(tmp_path, [1])
    assert rmk.prune_task_logs() == 0
