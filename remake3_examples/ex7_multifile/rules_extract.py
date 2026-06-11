# remake3_examples/ex7_multifile/rules_extract.py
#
# Rules defined in a standalone module — no Remake instance here.
# @rule is the module-level decorator; it produces free-standing Rule
# objects that a top-level pipeline file imports and registers.

from pathlib import Path

from remake3 import rule

SITES = ['oxford', 'cambridge', 'bristol']
YEARS = list(range(2010, 2021))


@rule(
    inputs  = {'archive': 'data/raw/{site}/{year}.tar.gz'},
    outputs = {'nc': 'data/extracted/{site}/{year}.nc'},
    matrix  = {'site': SITES, 'year': YEARS},
)
def extract(inputs, outputs, site, year):
    import tarfile
    with tarfile.open(inputs['archive']) as tf:
        tf.extractall(Path(outputs['nc']).parent)
