import json
from pathlib import Path

from remake import MatrixNotReady, Remake, Sqlite3Backend, load_remake, rule
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

    def cluster_matrix():
        p = tmp_path / 'clusters.json'
        if not p.exists():
            raise MatrixNotReady(p)
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
    assert 'boom' in records[tasks[2].key].exception

    runnable, _ = rmk.plan()
    assert [t.kwargs for t in runnable] == [{'n': 2}]


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
