# examples/ex5_multifile/pipeline.py
#
# Top-level pipeline file combining rules defined in other modules.
#
# Scans claim only rules *defined* in the scanned file, so a rule imported
# by name into this module is not picked up by rules_from_current_module()
# (it would warn). Register other modules' rules explicitly: either
# rmk.rules_from_modules(rules_extract, rules_analysis) as here (list a
# module after the ones it imports from), or
# rmk.add_rules([extract_obs, process_obs, aggregate_obs]).
#
# Run: remake run pipeline.py

from remake import Remake

import rules_extract
import rules_analysis

rmk = Remake()

rmk.rules_from_modules(rules_extract, rules_analysis)
