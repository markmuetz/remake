# examples/ex15_config/pipeline.py
#
# The configuration cascade. Settings come from layers, lowest precedence
# first:
#
#   built-in defaults < profile < user file < remake.toml (project file)
#     < Remake(config=) < @rule(config=)
#
# Dicts merge, lists and scalars replace; unknown keys in files are errors.
# Here:
#   - remake.toml (next to this file) selects the profile "quick", turns on
#     checksum_outputs, sets multiproc.nproc and defines two profiles.
#   - Remake(config=) below sets resources.capture.
#   - one rule overrides slurm.mem for itself with @rule(config=). Only the
#     SLURM executor reads per-rule config, and `remake config` shows the
#     pipeline-level layers only, so this one is visible only in an sbatch
#     script (it is harmless here: nothing is submitted).
#   - remake.toml also shows (commented out) `metadata_dir`, which moves
#     `.remake/` elsewhere, e.g. onto a scratch filesystem.
# Config never causes reruns: editing any of it reruns nothing.
#
# The pipeline: write_numbers[n] --> total.
#
# Try (from this directory):
#   remake config pipeline.py                    # each key and the layer
#                                                #   that set it
#   remake --profile thorough config pipeline.py # profile from the CLI:
#                                                #   rss_interval changes,
#                                                #   nproc stays 2 (the
#                                                #   project file wins)
#   REMAKE_PROFILE=thorough remake config pipeline.py   # same, via the env
#   remake config pipeline.py --json             # machine-readable
#   remake run pipeline.py                       # logs executor, nproc, profile
#   remake run -E multiproc -f pipeline.py       # nproc=2, from remake.toml
#                                                #   (-j N on the CLI beats it)
#   remake task-info pipeline.py <task key>      # outputs show their sha256
#                                                #   (checksum_outputs = true)
#
# Only keys valid without SLURM are used; see docs/guide/configuration.md
# for the full list (slurm.*, dask.*, ...).

from pathlib import Path

from remake import Remake, rule

rmk = Remake(config={'resources': {'capture': True}})

SIZES = [3, 5, 8]


@rule(
    outputs={'numbers': 'data/ex15/numbers_{n}.txt'},
    matrix={'n': SIZES},
    # Per-rule config: the top layer, merged over everything above (here:
    # this rule's SLURM jobs ask for less memory than the pipeline default).
    config={'slurm': {'mem': '1G'}},
)
def write_numbers(outputs, n):
    """Write the integers 1..n, one per line."""
    Path(outputs['numbers']).write_text(''.join(f'{i}\n' for i in range(1, n + 1)))


@rule(
    inputs={f'n{n}': f'data/ex15/numbers_{n}.txt' for n in SIZES},
    outputs={'total': 'data/results/total15.txt'},
    depends_on=[write_numbers],
)
def total(inputs, outputs):
    """Sum every number in every input file."""
    s = sum(int(x) for p in inputs.values() for x in Path(p).read_text().split())
    Path(outputs['total']).write_text(f'{s}\n')


rmk.rules_from_current_module()
