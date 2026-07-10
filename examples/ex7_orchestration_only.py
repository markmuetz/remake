# examples/ex7_orchestration_only.py
#
# Orchestration-only pattern: rules with no inputs= or outputs=.
#
# When wrapping a tool that manages its own file layout internally,
# duplicating those paths in remake inputs/outputs would be fragile
# and add no value.  remake tracks completion via the metadata DB
# alone — inputs/outputs are not required.
#
# This example simulates wrapping an external processing tool
# (e.g. a climate tracker or simulation framework) that reads and
# writes files at paths it controls.  Each rule invokes the tool
# with a config; remake only orchestrates the ordering.
#
# DAG:
#   preprocess_domain[domain]    (N tasks in parallel)
#          |
#   analyse[domain]       (N tasks in parallel)
#          |
#   postprocess           (1 task — fan-in)

from pathlib import Path

from remake import Remake, rule

rmk = Remake()

DOMAINS = ['global', 'europe', 'sahel']
ROOT = Path('data/orchestrated')


def _external_tool_preprocess(domain):
    """Simulate an external tool that owns its own output paths."""
    out = ROOT / domain / 'preprocessed.txt'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(f'preprocessed data for {domain}\n')


def _external_tool_analyse(domain):
    """Simulate an external tool reading its own prior output."""
    inp = ROOT / domain / 'preprocessed.txt'
    out = ROOT / domain / 'analysis.txt'
    data = inp.read_text()
    out.write_text(f'analysis of: {data}')


def _external_tool_postprocess(domains):
    """Simulate a tool that combines results from all domains."""
    out = ROOT / 'summary.txt'
    lines = []
    for domain in domains:
        lines.append((ROOT / domain / 'analysis.txt').read_text())
    out.write_text('\n'.join(lines))


@rule(
    matrix={'domain': DOMAINS},
    uses={'_external_tool_preprocess': _external_tool_preprocess},
)
def preprocess_domain(domain):
    _external_tool_preprocess(domain)


@rule(
    matrix={'domain': DOMAINS},
    depends_on=[preprocess_domain],
    uses={'_external_tool_analyse': _external_tool_analyse},
)
def analyse(domain):
    _external_tool_analyse(domain)


@rule(
    depends_on=[analyse],
    uses={'DOMAINS': DOMAINS, '_external_tool_postprocess': _external_tool_postprocess},
)
def postprocess():
    _external_tool_postprocess(DOMAINS)


rmk.rules_from_current_module()
