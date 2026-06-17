import json
from pathlib import Path

from remake import Defer, Remake, Sqlite3Backend, deferrable, load_remake, rule
from remake.metadata import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS


def test_three_rule_pipeline_end_to_end(tmp_path, meta):
    @rule(outputs={'raw': str(tmp_path / 'raw/{year}.txt')}, matrix={'year': [2000, 2001, 2002]})
    def generate(outputs, year):
        Path(outputs['raw']).write_text(f'{year}\n')

    @rule(
        inputs=generate.outputs,
        outputs={'out': str(tmp_path / 'processed/{year}.txt')},
        matrix=generate.matrix,
        depends_on=[generate],
        uses={'scale': 2},
    )
    def process(inputs, outputs, year):
        val = int(Path(inputs['raw']).read_text()) * scale  # noqa: F821
        Path(outputs['out']).write_text(f'{val}\n')

    def combine_inputs():
        return {str(y): str(tmp_path / f'processed/{y}.txt') for y in [2000, 2001, 2002]}

    @rule(inputs=combine_inputs, outputs={'total': str(tmp_path / 'total.txt')},
          depends_on=[process])
    def combine(inputs, outputs):
        total = sum(int(Path(p).read_text()) for p in inputs.values())
        Path(outputs['total']).write_text(f'{total}\n')

    rmk = Remake(rules=[generate, process, combine], metadata=meta)
    rmk.run()

    assert (tmp_path / 'total.txt').read_text().strip() == '12006'
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred


def test_parent_dirs_created_for_outputs(tmp_path, meta):
    @rule(outputs={'o': str(tmp_path / 'deeply/nested/dirs/out.txt')})
    def r(outputs):
        # No mkdir: remake creates parent dirs before the task runs.
        Path(outputs['o']).write_text('x')

    Remake(rules=[r], metadata=meta).run()
    assert (tmp_path / 'deeply/nested/dirs/out.txt').exists()


def test_output_less_and_input_less_rules(tmp_path, meta):
    @rule(outputs={'store': str(tmp_path / 'store.txt')})
    def create_store(outputs):
        Path(outputs['store']).write_text('')

    @rule(
        inputs=create_store.outputs,
        matrix={'n': [1, 2]},
        depends_on=[create_store],
    )
    def write_region(inputs, n):
        # Side-effect rule: no outputs declared.
        with Path(inputs['store']).open('a') as f:
            f.write(f'{n}\n')

    rmk = Remake(rules=[create_store, write_region], metadata=meta)
    rmk.run()
    assert sorted((tmp_path / 'store.txt').read_text().split()) == ['1', '2']
    # Completion of output-less tasks is DB-tracked.
    runnable, _ = rmk.plan()
    assert not runnable


def test_dynamic_matrix_defer_resolve_complete(tmp_path, meta):
    @rule(outputs={'clusters': str(tmp_path / 'clusters.json')})
    def find_clusters(outputs):
        Path(outputs['clusters']).write_text(json.dumps(['c1', 'c2', 'c3']))

    @deferrable
    def cluster_matrix():
        p = tmp_path / 'clusters.json'
        if not p.exists():
            raise Defer(p)
        return [{'cid': c} for c in json.loads(p.read_text())]

    @rule(outputs={'out': str(tmp_path / 'cluster_{cid}.txt')}, matrix=cluster_matrix,
          depends_on=[find_clusters])
    def process_cluster(outputs, cid):
        Path(outputs['out']).write_text(cid.upper())

    def summarise_inputs():
        return {c: str(tmp_path / f'cluster_{c}.txt')
                for c in json.loads((tmp_path / 'clusters.json').read_text())}

    @rule(inputs=summarise_inputs, outputs={'o': str(tmp_path / 'summary.txt')},
          depends_on=[process_cluster])
    def summarise(inputs, outputs):
        names = ','.join(Path(p).read_text() for p in inputs.values())
        Path(outputs['o']).write_text(names)

    rmk = Remake(rules=[find_clusters, process_cluster, summarise], metadata=meta)
    runnable, deferred = rmk.plan()
    # summarise has a static matrix but is downstream of a deferred rule:
    # it must be deferred too, not run in the first wave.
    assert len(runnable) == 1
    assert deferred == [process_cluster, summarise]

    rmk.run()  # internal replanning loop resolves the deferred rules
    assert sorted(p.name for p in tmp_path.glob('cluster_*.txt')) == [
        'cluster_c1.txt', 'cluster_c2.txt', 'cluster_c3.txt',
    ]
    assert (tmp_path / 'summary.txt').read_text() == 'C1,C2,C3'
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred


def test_tasks_and_task_from_key_skip_deferred_matrix(tmp_path, meta):
    """tasks()/task_from_key() (used by why/task-info/set-state) must not crash
    when a rule's @deferrable matrix raises Defer — they skip the deferred
    rule rather than propagating the exception."""
    @rule(outputs={'clusters': str(tmp_path / 'clusters.json')})
    def find_clusters(outputs):
        Path(outputs['clusters']).write_text(json.dumps(['c1', 'c2']))

    @deferrable
    def cluster_matrix():
        p = tmp_path / 'clusters.json'
        if not p.exists():
            raise Defer(p)
        return [{'cid': c} for c in json.loads(p.read_text())]

    @rule(outputs={'out': str(tmp_path / 'cluster_{cid}.txt')}, matrix=cluster_matrix,
          depends_on=[find_clusters])
    def process_cluster(outputs, cid):
        Path(outputs['out']).write_text(cid.upper())

    rmk = Remake(rules=[find_clusters, process_cluster], metadata=meta)

    # clusters.json does not exist yet, so process_cluster's matrix is not ready.
    tasks = rmk.tasks()
    # The deferred rule is skipped; the expandable rule's task is still returned.
    assert [t.rule.name for t in tasks] == ['find_clusters']

    # Querying only the deferred rule yields nothing (rather than crashing).
    assert rmk.tasks(query="cid == 'c1'") == []

    # A key belonging to an expandable rule still resolves.
    key = tasks[0].key
    assert rmk.task_from_key(key).rule.name == 'find_clusters'

    # Once the upstream output exists, the matrix resolves and tasks appear.
    rmk.run()
    assert {t.rule.name for t in rmk.tasks()} == {'find_clusters', 'process_cluster'}


def test_deferrable_matrix_defers_when_upstream_reruns(tmp_path, meta):
    """The stale-upstream bug: a @deferrable matrix reads an upstream output
    that is about to be regenerated this wave. The planner must DEFER the
    rule (re-expand after the upstream reruns), not expand it from the stale
    on-disk output."""
    @rule(outputs={'clusters': str(tmp_path / 'clusters.json')})
    def find_clusters(outputs):
        Path(outputs['clusters']).write_text(json.dumps(['c1', 'c2']))

    @deferrable
    def cluster_matrix():
        p = tmp_path / 'clusters.json'
        if not p.exists():
            raise Defer(p)
        return [{'cid': c} for c in json.loads(p.read_text())]

    @rule(outputs={'out': str(tmp_path / 'cluster_{cid}.txt')}, matrix=cluster_matrix,
          depends_on=[find_clusters])
    def process_cluster(outputs, cid):
        Path(outputs['out']).write_text(cid.upper())

    rmk = Remake(rules=[find_clusters, process_cluster], metadata=meta)
    rmk.run()
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred  # all up to date

    # Make find_clusters rerun (its output is now stale). process_cluster's
    # matrix reads that output — defer rather than expand from the old value.
    task = next(t for t in rmk.tasks() if t.rule is find_clusters)
    rmk.metadata.update_task(task, TASK_STATUS_FAILED)
    runnable, deferred = rmk.plan()
    assert [t.rule.name for t in runnable] == ['find_clusters']
    assert deferred == [process_cluster]


def test_nondeferrable_callable_matrix_not_deferred_on_upstream_rerun(tmp_path, meta):
    """Fix B's narrowing: an ordinary callable matrix (no upstream reads, not
    @deferrable) is NOT deferred when an upstream reruns — it expands normally
    and reruns conservatively, no needless extra wave."""
    @rule(outputs={'seed': str(tmp_path / 'seed.txt')})
    def seed(outputs):
        Path(outputs['seed']).write_text('x')

    def fixed_matrix():  # callable, but derives nothing from upstream outputs
        return [{'i': 1}, {'i': 2}]

    @rule(outputs={'o': str(tmp_path / 'o_{i}.txt')}, matrix=fixed_matrix,
          depends_on=[seed])
    def proc(outputs, i):
        Path(outputs['o']).write_text(str(i))

    rmk = Remake(rules=[seed, proc], metadata=meta)
    rmk.run()
    task = next(t for t in rmk.tasks() if t.rule is seed)
    rmk.metadata.update_task(task, TASK_STATUS_FAILED)
    runnable, deferred = rmk.plan()
    assert not deferred  # not over-deferred
    assert 'proc' in {t.rule.name for t in runnable}  # reruns conservatively


def test_failure_recorded_and_run_continues(tmp_path, meta):
    @rule(outputs={'o': str(tmp_path / 'f_{n}.txt')}, matrix={'n': [1, 2]})
    def sometimes_fails(outputs, n):
        if n == 2:
            raise ValueError('boom')
        Path(outputs['o']).write_text('ok')

    rmk = Remake(rules=[sometimes_fails], metadata=meta)
    rmk.run()  # does not raise; failure recorded, other task still runs
    assert (tmp_path / 'f_1.txt').exists()

    tasks = {t.kwargs['n']: t for t in rmk.tasks()}
    records = rmk.metadata.get_tasks_status(tasks.values())
    assert records[tasks[1].key].status == TASK_STATUS_SUCCESS
    assert records[tasks[2].key].status == TASK_STATUS_FAILED
    # The full traceback is stored, not just the exception repr.
    stored = records[tasks[2].key].exception
    assert 'Traceback (most recent call last)' in stored
    assert "ValueError: boom" in stored
    assert 'sometimes_fails' in stored

    runnable, _ = rmk.plan()
    assert [t.kwargs for t in runnable] == [{'n': 2}]


def test_upstream_failure_skips_downstream(tmp_path, meta):
    @rule(outputs={'o': str(tmp_path / 'a_{n}.txt')}, matrix={'n': [1, 2]})
    def rule_a(outputs, n):
        if n == 1:
            raise ValueError('boom')
        Path(outputs['o']).write_text(str(n))

    @rule(inputs=rule_a.outputs, outputs={'o': str(tmp_path / 'b_{n}.txt')},
          matrix=rule_a.matrix, depends_on=[rule_a])
    def rule_b(inputs, outputs, n):
        Path(outputs['o']).write_text(Path(inputs['o']).read_text())

    def fan_in():
        return {str(n): str(tmp_path / f'b_{n}.txt') for n in [1, 2]}

    @rule(inputs=fan_in, outputs={'o': str(tmp_path / 'c.txt')}, depends_on=[rule_b])
    def rule_c(inputs, outputs):
        Path(outputs['o']).write_text('done')

    rmk = Remake(rules=[rule_a, rule_b, rule_c], metadata=meta)
    assert rmk.run() == 1  # one real failure; skips are not failures

    # Element-wise: b[n=2] ran; b[n=1] (tainted) and the fan-in c
    # (conservative, transitively tainted via b) were skipped, not run
    # into missing inputs.
    assert (tmp_path / 'b_2.txt').exists()
    assert not (tmp_path / 'b_1.txt').exists()
    assert not (tmp_path / 'c.txt').exists()

    # Skipped tasks stay unrecorded (pending), not failed.
    tasks = {(t.rule.name, t.kwargs.get('n')): t for t in rmk.tasks()}
    records = rmk.metadata.get_tasks_status(tasks.values())
    assert tasks[('rule_b', 1)].key not in records
    assert tasks[('rule_c', None)].key not in records

    # The next plan picks up exactly the failed task and its descendants.
    runnable, _ = rmk.plan()
    assert sorted([(t.rule.name, t.kwargs.get('n')) for t in runnable], key=str) == [
        ('rule_a', 1), ('rule_b', 1), ('rule_c', None)
    ]


def test_code_change_triggers_rerun_across_loads(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    pipeline_v1 = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'out_{n}.txt'}, matrix={'n': [1, 2]})
def r(outputs, n):
    Path(outputs['o']).write_text(str(n * 2))

rmk = Remake()
rmk.rules_from_current_module()
'''
    Path('pipeline.py').write_text(pipeline_v1)
    rmk = load_remake('pipeline.py')
    rmk.run()
    assert Path('out_1.txt').read_text() == '2'
    assert not load_remake('pipeline.py').plan()[0]

    # Cosmetic change: comment only — no rerun.
    Path('pipeline.py').write_text(pipeline_v1.replace(
        'def r(outputs, n):', 'def r(outputs, n):\n    # a comment'))
    assert not load_remake('pipeline.py').plan()[0]

    # Real change: body differs — rerun.
    Path('pipeline.py').write_text(pipeline_v1.replace('n * 2', 'n * 3'))
    rmk = load_remake('pipeline.py')
    assert len(rmk.plan()[0]) == 2
    rmk.run()
    assert Path('out_1.txt').read_text() == '3'


def test_custom_executor_injection(tmp_path, meta):
    from remake import Executor

    class RecordingExecutor(Executor):
        def __init__(self, rmk):
            super().__init__(rmk)
            self.seen = []

        def run_tasks(self, tasks):
            self.seen.extend(tasks)
            for task in tasks:
                self.rmk.run_task(task)

    @rule(outputs={'o': str(tmp_path / 'o_{n}.txt')}, matrix={'n': [1, 2]})
    def r(outputs, n):
        Path(outputs['o']).write_text(str(n))

    rmk = Remake(rules=[r], metadata=meta)
    executor = RecordingExecutor(rmk)
    rmk.run(executor=executor)
    assert len(executor.seen) == 2
    assert (tmp_path / 'o_1.txt').exists()


def test_rules_from_current_module_and_multi_remake(meta):
    @rule(outputs={'o': 'o_{n}.txt'}, matrix={'n': [1]})
    def r(outputs, n):
        pass

    rmk1 = Remake(metadata=meta)
    rmk1.rules_from_current_module()
    assert rmk1.rules == [r]

    # The same rule can be registered with a second Remake (one rule
    # module, two pipelines).
    rmk2 = Remake(rules=[r], metadata=Sqlite3Backend(':memory:'))
    assert rmk2.rules == [r]

    # Re-adding is a no-op.
    rmk1.add_rules([r])
    assert rmk1.rules == [r]
