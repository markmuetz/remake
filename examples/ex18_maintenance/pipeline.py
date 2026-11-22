# examples/ex18_maintenance/pipeline.py
#
# Maintenance commands: `verify`, `history --prune`, `metadata_dir` and
# `[history] keep` (both in remake.toml next to this file), and
# `Remake(project_dir=)` (see run_from_python.py).
#
# The pipeline: make_part18[n] --> join18. Stdlib only. Its metadata lives in
# `.remake18/` (metadata_dir), never `.remake/`.
#
# Try it (from this directory):
#
#   remake run pipeline.py                      # 4 + 1 tasks; .remake18/ appears
#
# 1. Outputs lost (say, a scratch purge). `verify` compares the DB with disk:
#
#   rm data/ex18/part_2.txt
#   remake verify pipeline.py                   # reports part 2; exit code 1
#   remake verify pipeline.py --apply           # marks it pending
#   remake run pipeline.py                      # reruns that task and join18
#                                               #   (which reads it)
#
# 2. Outputs made outside remake. Forget the metadata, then adopt what is on
#    disk (`--adopt`: tasks with no record whose outputs are all complete):
#
#   rm -r .remake18
#   remake verify pipeline.py --adopt           # reports 5 adoptable; exit 1
#   remake verify pipeline.py --adopt --apply   # records them as success
#   remake run pipeline.py                      # nothing to do
#
# 3. History. Each execution of a task is recorded, capped at `history.keep`
#    (3 here, set in remake.toml). After a few forced reruns:
#
#   remake run -f pipeline.py                   # repeat 4 times
#   remake history pipeline.py -Q "rule == 'join18'"   # 3 executions
#   remake history pipeline.py --runs           # one line per run
#   remake history pipeline.py --prune --keep 1 # "pruned N history row(s)"
#
# Everything above lands under `.remake18/`; `.remake/` is never created.
#
# From Python: see run_from_python.py in this directory. A Remake anchors
# relative paths, its metadata and remake.toml at the directory of the file
# that defines it, so running this file from anywhere behaves like
# `remake run`; `Remake(project_dir=...)` chooses another directory.

from pathlib import Path

from remake import Remake, rule

rmk = Remake()

PARTS = [1, 2, 3, 4]


@rule(
    outputs={'part': 'data/ex18/part_{n}.txt'},
    matrix={'n': PARTS},
)
def make_part18(outputs, n):
    """Write the integers 1..n, one per line."""
    Path(outputs['part']).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs['part']).write_text(''.join(f'{i}\n' for i in range(1, n + 1)))


@rule(
    inputs={f'n{n}': f'data/ex18/part_{n}.txt' for n in PARTS},
    outputs={'total': 'data/results/total18.txt'},
    depends_on=[make_part18],
)
def join18(inputs, outputs):
    """Sum every number in every part."""
    s = sum(int(x) for p in inputs.values() for x in Path(p).read_text().split())
    Path(outputs['total']).parent.mkdir(parents=True, exist_ok=True)
    Path(outputs['total']).write_text(f'{s}\n')


rmk.rules_from_current_module()


if __name__ == '__main__':
    rmk.run()
