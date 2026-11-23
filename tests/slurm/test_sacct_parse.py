"""Parsing sacct output (reconcile plan §4), against real samples: the soak
archive (run 81, a node failure) and the live probe on JASMIN."""
import subprocess as sp
from pathlib import Path

import pytest

from remake.executors.slurm_accounting import (SacctError, _indices, _seconds,
                                               parse_sacct, sacct_snapshot)

FIX = Path(__file__).parent / 'fixtures'
PROBE = FIX / 'jasmin_probe_2026-11-23'
PROBE_FIELDS = ('JobID', 'JobName', 'State', 'ExitCode', 'Start', 'End', 'Elapsed',
                'NodeList')


def ids():
    return dict(kv.split('=') for kv in (PROBE / 'ids.txt').read_text().split())


def test_soak_node_failure():
    lines = (FIX / 'sacct_run81_node_fail.psv').read_text().splitlines()
    fields = tuple(lines[0].split('|'))
    jobs = parse_sacct('\n'.join(lines[1:]), fields)
    b = jobs['6133148']
    assert {i for i, el in b.elements.items() if el.state == 'NODE_FAIL'} == {11, 12, 15, 16}
    assert b.element(11).node == 'host1234' and b.element(11).elapsed == 13 * 60 + 40
    d = jobs['6133150']
    assert d.element(14).state == 'NODE_FAIL'
    # The aftercorr dependents of the killed elements have no rows; the
    # remainder row `6133150_16` (no start) is not evidence about element 16.
    assert {11, 12, 15, 16}.isdisjoint(d.elements)
    assert [(idx, el.state) for idx, el in d.unsplit] == [(None, 'CANCELLED')]
    # Collapsed ranges, pending and cancelled.
    assert jobs['6133151'].unsplit[0][0] == list(range(34))
    assert jobs['6133152'].unsplit[0][0] == [0]
    # Steps are ignored.
    assert all('.' not in k for k in jobs)


def test_live_probe_final():
    i = ids()
    jobs = parse_sacct((PROBE / 'sacct_epoch_final.psv').read_text().replace('rc=0', ''),
                       PROBE_FIELDS)
    a = jobs[i['A']]
    assert {k: el.state for k, el in a.elements.items()} == {
        0: 'COMPLETED', 1: 'FAILED', 2: 'TIMEOUT', 3: 'CANCELLED', 4: 'OUT_OF_MEMORY',
        5: 'COMPLETED'}
    assert all(el.started for el in a.elements.values())
    assert a.element(2).end - a.element(2).start == 62
    # aftercorr dependents of A's failures (B_1-4): no rows at all.
    assert set(jobs[i['B']].elements) == {0, 5} and not jobs[i['B']].unsplit
    # afterok dependents: one collapsed cancelled row, never started.
    assert [(idx, el.state, el.started) for idx, el in jobs[i['C']].unsplit] == [
        ([0, 1, 2, 3], 'CANCELLED', False)]
    # D_1 cancelled alone while pending: gone; the rest cancelled together.
    assert jobs[i['D']].unsplit[0][0] == [0, 2, 3]


def test_live_probe_aftercorr_remainder_is_not_an_element():
    # At t15, `B_5 PENDING` stood for B_0, 2, 3, 5.
    i = ids()
    jobs = parse_sacct((PROBE / 'sacct_epoch_t15.psv').read_text().replace('rc=0', ''),
                       PROBE_FIELDS)
    b = jobs[i['B']]
    assert not b.elements and [(idx, el.state) for idx, el in b.unsplit] == [
        (None, 'PENDING')]


def test_iso_times_parse_too():
    jobs = parse_sacct((PROBE / 'sacct_final.psv').read_text(), PROBE_FIELDS)
    assert jobs[ids()['A']].element(0).start > 1_700_000_000


@pytest.mark.parametrize('text,expected', [
    ('00:13:40', 820), ('13:40', 820), ('1-00:00:01', 86401), ('05', 5)])
def test_elapsed(text, expected):
    assert _seconds(text) == expected


def test_ranges():
    assert _indices('0-3,7%4') == [0, 1, 2, 3, 7]
    assert _indices('7') == [7]


def test_malformed_line_raises():
    with pytest.raises(SacctError):
        parse_sacct('1001_1|COMPLETED')


def test_sacct_snapshot_through_the_shim(cluster):
    from remake.remake_cmd import remake_cmd

    remake_cmd(['remake', 'run', 'pipeline.py', '-E', 'slurm'])
    cluster.sync()
    gen = next(j for j in cluster.jobs.values() if j.name == 'gen')
    cluster.kill(gen.jobid, 1, 'TIMEOUT')
    snap = sacct_snapshot([gen.jobid], since=0)
    assert snap[gen.jobid].element(1).state == 'TIMEOUT'
    call = cluster.calls('sacct')[-1].split()
    assert call[1] == '-S' and call[call.index('-j') + 1] == gen.jobid
    cluster.fail('sacct')
    with pytest.raises(SacctError, match='sacct failed'):
        sacct_snapshot([gen.jobid])


def test_sacct_missing(monkeypatch):
    monkeypatch.setenv('PATH', '/nonexistent')
    with pytest.raises(SacctError, match='not found'):
        sacct_snapshot(['1'])
