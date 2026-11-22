# examples/ex18_maintenance/run_from_python.py
#
# Library use of remake from another directory. A Remake anchors relative
# paths (inputs, outputs, the metadata dir, remake.toml) at the directory of
# the file that defines it, or at `project_dir=` if you pass one, whatever the
# current directory is. Methods change into that directory for their duration
# and back afterwards.
#
#   python run_from_python.py [PROJECT_DIR]    # from any directory
#
# PROJECT_DIR defaults to this file's directory. Pass a copy of
# ex18_maintenance/ elsewhere to keep data out of the examples.

import sys
from pathlib import Path

from remake import Remake

sys.path.insert(0, str(Path(__file__).parent))
import pipeline  # noqa: E402  (this directory's rules)


def main(project_dir=None):
    """Run the ex18 rules anchored at `project_dir`, then verify and list runs.

    Returns the dict from `Remake.verify` (empty lists after a clean run).
    """
    project_dir = Path(project_dir or Path(__file__).parent).resolve()
    # Same rules as pipeline.py, a Remake anchored at project_dir: it reads
    # project_dir/remake.toml and keeps its metadata under project_dir.
    rmk = Remake(rules=pipeline.rmk.rules, project_dir=project_dir)
    rmk.run()
    print('project_dir:', rmk.project_dir)
    report = rmk.verify(outputs=True, adopt=True, apply=False)
    print('verify:', report)
    print('runs recorded:', len(rmk.history(runs=True)))
    return report


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else None)
