# examples/ex5_multifile/pipeline.py
#
# Top-level pipeline file combining rules defined in other modules.
#
# rmk.rules_from_current_module() picks up Rule objects imported into
# this module's namespace as well as ones defined here — that is the
# multi-file composition mechanism. Alternatively, register modules
# wholesale with rmk.rules_from_modules(rules_extract, rules_analysis),
# or pass an explicit list: Remake(rules=[extract_obs, process_obs, aggregate_obs]).
#
# Run: remake run pipeline.py

from remake import Remake

from rules_extract import extract_obs
from rules_analysis import process_obs, aggregate_obs

rmk = Remake()

rmk.rules_from_current_module()
