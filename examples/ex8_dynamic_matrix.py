# examples/ex8_dynamic_matrix.py
#
# Dynamic matrices: the number of tasks is not known until upstream rules
# have run.
#
#   - detect_events finds a variable number of events per year.
#   - event_matrix() is a *callable* matrix, marked @deferrable because it
#     derives its task list from upstream outputs. It raises Defer while the
#     event files do not exist yet; the planner treats that as "defer this
#     rule" and retries after each wave (and also defers it while
#     detect_events is itself rerunning, so it never expands from a stale
#     output). It returns list[dict] — the canonical matrix form, here NOT a
#     cartesian product (each year has a different number of events).
#   - summarise fans in over whatever was discovered.
#
# A single `remake run` handles the whole flow: detect_events runs first,
# then the deferred rules resolve and run.
#
# DAG: detect_events[year] --> process_event[year, event_id] --> summarise

import json
import random
from pathlib import Path

from remake import Defer, Remake, deferrable, rule

rmk = Remake()

YEARS = [2020, 2021, 2022]


@rule(outputs={'events': 'data/events/{year}.json'}, matrix={'year': YEARS})
def detect_events(outputs, year):
    # Number of events per year is "discovered" here — unknown upfront.
    rng = random.Random(year)
    events = [
        {'id': f'e{i}', 'strength': round(rng.uniform(0, 1), 3)}
        for i in range(rng.randint(2, 5))
    ]
    Path(outputs['events']).write_text(json.dumps(events))


@deferrable
def event_matrix():
    """Called during planning. Raises Defer until detect_events has produced
    its outputs; then returns one row per discovered event."""
    rows = []
    for year in YEARS:
        path = Path(f'data/events/{year}.json')
        if not path.exists():
            raise Defer(path)
        for event in json.loads(path.read_text()):
            rows.append({'year': year, 'event_id': event['id']})
    return rows  # list[dict] — not a cartesian product


@rule(
    inputs     = {'events': 'data/events/{year}.json'},
    outputs    = {'stats': 'data/event_stats/{year}_{event_id}.json'},
    matrix     = event_matrix,
    depends_on = [detect_events],
)
def process_event(inputs, outputs, year, event_id):
    events = json.loads(Path(inputs['events']).read_text())
    event = next(e for e in events if e['id'] == event_id)
    stats = {'year': year, 'event_id': event_id,
             'strength_pct': round(100 * event['strength'], 1)}
    Path(outputs['stats']).write_text(json.dumps(stats))


def summarise_inputs():
    """Dynamic fan-in: resolved lazily, after process_event has run."""
    rows = event_matrix()
    return {
        f"{r['year']}_{r['event_id']}": f"data/event_stats/{r['year']}_{r['event_id']}.json"
        for r in rows
    }


@rule(
    inputs     = summarise_inputs,
    outputs    = {'summary': 'data/results/event_summary.json'},
    depends_on = [process_event],
)
def summarise(inputs, outputs):
    stats = [json.loads(Path(p).read_text()) for p in inputs.values()]
    summary = {
        'n_events': len(stats),
        'strongest': max(stats, key=lambda s: s['strength_pct']),
    }
    Path(outputs['summary']).write_text(json.dumps(summary, indent=2))


rmk.rules_from_current_module()
