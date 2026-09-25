"""The tutorial's lessons as data — one spec, three uses: the pytest that
replays every step (tests/integration/test_tutorial.py), `remake-tutorial
reset` (which replays earlier lessons to rebuild a lesson's starting state),
and the tutor's intent for each step (`remake-tutorial step <id>`). The docs
pages (docs/tutorial/) are written by hand around the same steps.

A Step is one thing the learner does: optionally make an edit (the
`snapshot` is the remakefile as it should look afterwards — files/<name>/),
then run `commands`. A command starting with `remake ` is checked against
`expect` — a summary of its events in .remake/remake.jsonl (see
watch.summarise): exit code, tasks planned, tasks run / failed per rule.
Anything else (`python make_data.py`, `rm ...`) is a plain shell command.
"""
from dataclasses import dataclass, field


@dataclass
class Command:
    cmd: str
    expect: dict = field(default_factory=dict)  # keys: exit, planned, ran, failed


@dataclass
class Step:
    id: str
    title: str
    commands: list
    snapshot: str = None  # edit to make first: files/<snapshot>/
    predict: str = None   # ask the learner before they run it
    point: str = ''       # what the step teaches (for the tutor)


@dataclass
class Lesson:
    number: int
    title: str
    snapshot: str  # the remakefile a learner starts the lesson with
    steps: list


LESSONS = [
    Lesson(1, 'Your first rule', snapshot='1', steps=[
        Step('1.1', 'Make the raw data', [Command('python make_data.py')],
             point='Plain shell command; remake is not involved yet.'),
        Step('1.2', 'Run the pipeline', [
            Command('remake run pipeline.py', {'exit': 0, 'planned': 1, 'ran': {'clean': 1}}),
        ], predict='How many tasks will remake run?',
           point='One rule with no matrix is one task. remake records it in .remake/remake.db.'),
        Step('1.3', 'Run it again', [
            Command('remake run pipeline.py', {'exit': 0, 'planned': 0, 'ran': {}}),
        ], predict='What will remake do this time?',
           point='Nothing to do: the DB says the task succeeded and its code, '
                 'inputs and outputs are unchanged.'),
        Step('1.4', 'Look at what remake knows', [
            Command('remake info pipeline.py', {'exit': 0}),
            Command('remake ls-tasks pipeline.py', {'exit': 0}),
        ], point='info: per-rule counts by status. ls-tasks: every task, with its key.'),
        Step('1.5', 'Delete the output', [
            Command('rm data/clean/aberdeen/2020.csv'),
            Command('remake run pipeline.py', {'exit': 0, 'planned': 0, 'ran': {}}),
        ], predict='The output file is gone. Will remake rerun the task?',
           point='No: remake decides from its database, not the filesystem. It '
                 'never looks at file times, and by default not at files at all.'),
        Step('1.6', 'Ask remake to check the files', [
            Command('remake run pipeline.py --check-outputs',
                    {'exit': 0, 'planned': 1, 'ran': {'clean': 1}}),
        ], point='--check-outputs verifies recorded outputs exist and reruns '
                 'tasks whose outputs are missing.'),
    ]),
    Lesson(2, 'A matrix of tasks', snapshot='2', steps=[
        Step('2.1', 'One task per site and year', [
            Command('remake run pipeline.py', {'exit': 0, 'planned': 12, 'ran': {'clean': 12}}),
        ], snapshot='2',
           predict='The rule now has a matrix of 3 sites x 4 years. How many tasks '
                   'run — and does aberdeen/2020, which already ran, run again?',
           point='12 run. A task is identified by its rule and its matrix values: '
                 'clean[site=aberdeen, year=2020] is a new task, not the old one.'),
        Step('2.2', 'List and filter tasks', [
            Command('remake ls-tasks pipeline.py', {'exit': 0}),
            Command('remake ls-tasks pipeline.py -Q "site == \'exeter\'"', {'exit': 0}),
        ], point='-Q takes a Python expression over the matrix values (and `rule`).'),
        Step('2.3', 'Force a subset', [
            Command('remake run pipeline.py -Q "year == 2023" --force',
                    {'exit': 0, 'planned': 3, 'ran': {'clean': 3}}),
        ], predict='How many tasks will this run?',
           point='--force reruns exactly the matched tasks, up to date or not.'),
        Step('2.4', 'Add a site', [
            Command('remake run pipeline.py -n', {'exit': 0, 'planned': 4, 'ran': {}}),
            Command('remake run pipeline.py', {'exit': 0, 'planned': 4, 'ran': {'clean': 4}}),
        ], snapshot='2-durham',
           predict="Add 'durham' to SITES. How many tasks will run?",
           point='Only the 4 new tasks: the matrix grew, the rule did not change. '
                 '-n (dry run) shows the plan without running anything.'),
    ]),
]


def lesson(number):
    for les in LESSONS:
        if les.number == number:
            return les
    raise SystemExit(f'no lesson {number} (lessons: {", ".join(str(les.number) for les in LESSONS)})')
