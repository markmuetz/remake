"""Per-task resource capture (design_docs/resource_capture.md).

Thresholds are deliberately generous: these are measurements on whatever
machine CI happens to give us, not deterministic values. They assert the
shape of the answer (a big task looks big, a pooled worker doesn't inherit
an earlier peak), never a precise number.
"""
import sqlite3
import time
from pathlib import Path

import pytest

from remake import Remake, Sqlite3Backend, rule
from remake.metadata import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS
from remake.util import resources as res_mod
from remake.util.resources import ResourceCapture

MB = 1024 * 1024

# The sampler needs /proc; the package targets Linux, but the module supports
# a getrusage fallback elsewhere, so don't assert sampled results where they
# cannot happen.
needs_proc = pytest.mark.skipif(
    not Path('/proc/self/statm').exists(), reason='no /proc: sampling unavailable')


def test_wall_and_cpu_time_measured():
    with ResourceCapture(interval=0.01) as cap:
        deadline = time.perf_counter() + 0.2
        while time.perf_counter() < deadline:  # busy-spin: cpu ≈ wall
            pass
    result = cap.result()
    assert 0.15 < result['wall_s'] < 5.0
    # Spinning burns CPU at ~wall rate; allow slack for a loaded CI box.
    assert result['cpu_s'] > 0.1


def test_sleep_is_wall_time_not_cpu_time():
    with ResourceCapture(interval=0.01) as cap:
        time.sleep(0.2)
    result = cap.result()
    assert result['wall_s'] > 0.15
    assert result['cpu_s'] < 0.15  # sleeping costs no CPU


@needs_proc
def test_peak_rss_sees_a_known_allocation():
    with ResourceCapture(interval=0.005) as cap:
        block = bytearray(200 * MB)
        block[::4096] = b'\xff' * len(block[::4096])  # touch pages: RSS, not VSZ
        time.sleep(0.05)  # give the sampler a look at the peak
        del block
    result = cap.result()
    assert result['rss_method'] == 'sample'
    assert result['max_rss_bytes'] > 150 * MB
    assert result['max_rss_bytes'] < 8000 * MB  # sanity, not precision


@needs_proc
def test_pooled_worker_does_not_inherit_an_earlier_peak():
    # The bug the sampler exists to prevent: ru_maxrss is a process
    # high-water mark, so a getrusage-at-end reading would report the big
    # task's peak against the small one that followed it in the same process.
    with ResourceCapture(interval=0.005) as big:
        block = bytearray(200 * MB)
        block[::4096] = b'\xff' * len(block[::4096])
        time.sleep(0.05)
        del block
    with ResourceCapture(interval=0.005) as small:
        time.sleep(0.05)
    assert big.result()['max_rss_bytes'] > 150 * MB
    assert small.result()['max_rss_bytes'] < big.result()['max_rss_bytes'] / 2


@needs_proc
def test_child_process_allocation_is_attributed():
    # Tasks that shell out (cdo, ncks) allocate in a child, which
    # /proc/self/statm never sees; RUSAGE_CHILDREN covers them.
    import subprocess as sp
    import sys

    with ResourceCapture(interval=0.005) as cap:
        sp.run([sys.executable, '-c',
                'b = bytearray(200 * 1024 * 1024); b[::4096] = b"\\xff" * len(b[::4096])'],
               check=True)
    assert cap.result()['max_rss_bytes'] > 150 * MB


@needs_proc
def test_short_task_still_gets_a_sample():
    # Shorter than one sampling interval: the forced start/stop samples mean
    # a value is always recorded.
    with ResourceCapture(interval=10) as cap:
        pass
    result = cap.result()
    assert result['max_rss_bytes'] > 0
    assert result['wall_s'] >= 0


def test_no_proc_and_reused_process_records_nothing(monkeypatch):
    # No /proc and a process that runs many tasks: NULL beats a number that
    # is wrong by construction.
    monkeypatch.setattr(res_mod, 'STATM_PATH', '/nonexistent/statm')
    with ResourceCapture(interval=0.01) as cap:
        time.sleep(0.01)
    result = cap.result()
    assert result['max_rss_bytes'] is None
    assert result['rss_method'] is None
    assert result['wall_s'] is not None  # wall/cpu are free, always measured


def test_no_proc_but_one_task_per_process_falls_back_to_rusage(monkeypatch):
    monkeypatch.setattr(res_mod, 'STATM_PATH', '/nonexistent/statm')
    with ResourceCapture(interval=0.01, one_task_per_process=True) as cap:
        time.sleep(0.01)
    result = cap.result()
    assert result['rss_method'] == 'rusage'
    assert result['max_rss_bytes'] > 0


def test_capture_disabled_still_records_wall_and_cpu():
    with ResourceCapture(interval=0.01, sample_rss=False) as cap:
        time.sleep(0.01)
    result = cap.result()
    assert result['wall_s'] is not None and result['cpu_s'] is not None
    assert result['max_rss_bytes'] is None and result['rss_method'] is None


def test_exception_propagates_and_is_still_measured():
    cap = ResourceCapture(interval=0.01)
    try:
        with cap:
            time.sleep(0.05)
            raise ValueError('boom')
    except ValueError:
        pass
    else:
        raise AssertionError('the exception was swallowed')
    assert cap.result()['wall_s'] > 0.04


def test_concurrent_captures_in_one_process_record_no_per_task_numbers():
    # A dask worker with threads_per_worker > 1 runs tasks concurrently in
    # one process, where statm and RUSAGE_SELF measure the process, not the
    # task. Recording the process total N times, labelled per-task, would be
    # worse than recording nothing.
    import threading

    results = {}
    started = threading.Barrier(2)

    def run(name):
        with ResourceCapture(interval=0.005) as cap:
            started.wait()
            time.sleep(0.1)
        results[name] = cap.result()

    threads = [threading.Thread(target=run, args=(n,)) for n in ('a', 'b')]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for result in results.values():
        assert result['wall_s'] is not None  # still this task's own elapsed
        assert result['cpu_s'] is None
        assert result['max_rss_bytes'] is None
        assert result['rss_method'] is None


def test_sequential_captures_are_not_treated_as_concurrent():
    # The overlap detection must not poison ordinary back-to-back tasks.
    for _ in range(3):
        with ResourceCapture(interval=0.005) as cap:
            time.sleep(0.01)
        result = cap.result()
        assert result['cpu_s'] is not None
        if Path('/proc/self/statm').exists():
            assert result['rss_method'] == 'sample'


def test_measurement_failure_does_not_break_the_task(monkeypatch):
    # A thread-limited machine (ulimit -u) must not turn into failed tasks:
    # degrade to no RSS figure, keep running.
    import threading

    def no_threads(self):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, 'start', no_threads)
    ran = []
    with ResourceCapture(interval=0.01) as cap:
        ran.append(True)
    assert ran  # the body ran
    result = cap.result()
    assert result['wall_s'] is not None
    assert result['max_rss_bytes'] is None and result['rss_method'] is None


def test_a_zero_or_junk_interval_cannot_spin_the_sampler():
    from remake.util.resources import MIN_INTERVAL, _clean_interval

    assert _clean_interval(0) == 0.1  # nonsense falls back to the default
    assert _clean_interval(-1) == 0.1
    assert _clean_interval('junk') == 0.1
    assert _clean_interval(None) == 0.1
    assert _clean_interval(1e-9) == MIN_INTERVAL  # clamped, never a spin loop
    assert _clean_interval('0.05') == 0.05  # a config-file string still works
    assert ResourceCapture(interval=0).interval == 0.1


def _one_task_remake(tmp_path, meta, config=None, fail=False):
    @rule(outputs={'o': str(tmp_path / 'o.txt')}, uses={'fail': fail})
    def produce(outputs):
        if fail:  # noqa: F821 — injected by uses=
            raise ValueError('boom')
        Path(outputs['o']).write_text('x')

    return Remake(rules=[produce], metadata=meta, config=config), produce


@needs_proc
def test_run_records_resources_in_the_db(tmp_path, meta):
    rmk, _ = _one_task_remake(tmp_path, meta)
    rmk.run()

    task = rmk.tasks()[0]
    record = meta.get_tasks_status([task])[task.key]
    assert record.wall_s is not None and record.wall_s >= 0
    assert record.cpu_s is not None
    assert record.max_rss_bytes > 0
    assert record.rss_method == 'sample'


def test_failed_task_records_resources(tmp_path, meta):
    # A task that fails after three hours is the most valuable duration in
    # the DB — the failure path must measure too.
    rmk, _ = _one_task_remake(tmp_path, meta, fail=True)
    rmk.run()

    task = rmk.tasks()[0]
    record = meta.get_tasks_status([task])[task.key]
    assert record.status == TASK_STATUS_FAILED
    assert record.wall_s is not None
    assert record.max_rss_bytes > 0


def test_capture_disabled_by_config(tmp_path, meta):
    rmk, _ = _one_task_remake(tmp_path, meta, config={'resources': {'capture': False}})
    rmk.run()

    task = rmk.tasks()[0]
    record = meta.get_tasks_status([task])[task.key]
    assert record.wall_s is not None  # free, always on
    assert record.max_rss_bytes is None and record.rss_method is None


def test_set_state_does_not_clear_measured_resources(tmp_path, meta):
    # Marking a task pending by hand does not un-measure what actually ran.
    rmk, _ = _one_task_remake(tmp_path, meta)
    rmk.run()
    task = rmk.tasks()[0]
    before = meta.get_tasks_status([task])[task.key]

    meta.update_tasks([task], TASK_STATUS_SUCCESS)
    after = meta.get_tasks_status([task])[task.key]

    assert after.wall_s == before.wall_s
    assert after.max_rss_bytes == before.max_rss_bytes


def test_rerun_replaces_all_four_columns(tmp_path, meta):
    # An execution replaces the resource columns verbatim, NULLs included:
    # a fresh wall_s must never be left paired with a stale peak RSS.
    rmk, produce = _one_task_remake(tmp_path, meta)
    rmk.run()
    task = rmk.tasks()[0]
    assert meta.get_tasks_status([task])[task.key].max_rss_bytes is not None

    rmk2 = Remake(rules=[produce], metadata=meta,
                  config={'resources': {'capture': False}})
    rmk2.run(force=True)
    record = meta.get_tasks_status([task])[task.key]
    assert record.wall_s is not None
    assert record.max_rss_bytes is None and record.rss_method is None


@needs_proc
def test_task_info_exposes_resources(tmp_path, meta):
    rmk, _ = _one_task_remake(tmp_path, meta)
    rmk.run()
    info = rmk.task_info(rmk.tasks()[0])
    assert info['resources']['wall_s'] is not None
    assert info['resources']['rss_method'] == 'sample'


@needs_proc
def test_sidecar_round_trip_lands_resources_in_the_db(tmp_path, meta, monkeypatch):
    # The SLURM/multiproc/dask path: measured on the compute node, written
    # to the DB at ingest.
    from remake.metadata.sidecar import SidecarWriter

    monkeypatch.chdir(tmp_path)  # sidecars are written under ./.remake
    rmk, produce = _one_task_remake(tmp_path, meta)
    rmk.metadata = SidecarWriter()
    rmk.run()
    rmk.metadata = meta
    meta.ensure_rules(rmk.rules)  # the parent interns rule code before ingest

    assert meta.ingest_sidecars(rmk.rules) == 1
    task = rmk.tasks()[0]
    record = meta.get_tasks_status([task])[task.key]
    assert record.wall_s is not None
    assert record.max_rss_bytes > 0
    assert record.rss_method == 'sample'


def test_pre_0_9_sidecar_without_resources_ingests_as_nulls(tmp_path, meta, monkeypatch):
    # A sidecar written by an in-flight pre-0.9 job during an upgrade.
    import json

    from remake.metadata.sidecar import SidecarWriter

    monkeypatch.chdir(tmp_path)
    rmk, _ = _one_task_remake(tmp_path, meta)
    rmk.metadata = SidecarWriter()
    rmk.run()
    rmk.metadata = meta

    sidecar = next(Path('.remake/tasks/results').rglob('*.json'))
    payload = json.loads(sidecar.read_text())
    del payload['resources']
    sidecar.write_text(json.dumps(payload))
    meta.ensure_rules(rmk.rules)

    assert meta.ingest_sidecars(rmk.rules) == 1
    task = rmk.tasks()[0]
    record = meta.get_tasks_status([task])[task.key]
    assert record.status == TASK_STATUS_SUCCESS
    assert record.wall_s is None and record.max_rss_bytes is None


@pytest.mark.skipif(sqlite3.sqlite_version_info < (3, 35),
                    reason='ALTER TABLE ... DROP COLUMN needs SQLite >= 3.35')
def test_upgrade_from_a_db_without_the_columns_does_not_rerun(tmp_path):
    # The 0.8.1 promise: a schema addition must not mass-rerun an existing
    # pipeline. Simulate a pre-0.9 DB by dropping the four columns.
    dbloc = tmp_path / 'remake.db'
    rmk, produce = _one_task_remake(tmp_path, Sqlite3Backend(dbloc))
    rmk.run()
    rmk.metadata.close()

    import sqlite3
    conn = sqlite3.connect(dbloc)
    for col in ('wall_s', 'cpu_s', 'max_rss_bytes', 'rss_method'):
        conn.execute(f'ALTER TABLE task DROP COLUMN {col}')
    conn.commit()
    conn.close()

    with Sqlite3Backend(dbloc) as meta:  # migrates on open
        rmk2 = Remake(rules=[produce], metadata=meta)
        runnable, deferred = rmk2.plan()
        assert not runnable and not deferred  # nothing reruns
        task = rmk2.tasks()[0]
        record = meta.get_tasks_status([task])[task.key]
        assert record.wall_s is None  # pre-upgrade record: not measured
        assert record.status == TASK_STATUS_SUCCESS
