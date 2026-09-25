"""The tutorial's lessons as data — one spec, three uses: the pytest that
replays every step (tests/integration/test_tutorial.py), `remake-tutorial
reset` (which replays earlier lessons to rebuild a lesson's starting state),
and the tutor, which walks the learner through it step by step
(`remake-tutorial spec <N>`). The docs pages (docs/tutorial/) are written by
hand around the same steps.

A Lesson starts from `snapshot` — the remakefile as the previous lesson left
it — so going on to the next lesson never undoes the learner's edits. A
Step is one thing the learner does: `do` (what the tutor tells them), an
optional edit (`snapshot`: the remakefile as it should look afterwards,
files/<name>/), then `commands`. A command starting with `remake ` is
checked against `expect`, a summary of its events in .remake/remake.jsonl
(see watch.summarise): exit code, tasks planned, tasks run / failed per
rule. Anything else (`rm ...`, `ls`, `head`) is a plain shell command, which
the watcher can't see: a step made only of those has an `ask`, a question
about what the learner sees, and they answer it in the tutor's window.

Concept order matters: a step may only use ideas introduced by an earlier
step (the `point`s, in order, are the curriculum).
"""
from dataclasses import dataclass, field

# Run by `remake-tutorial reset` before replaying: the raw data every lesson
# reads (deterministic, so re-running it changes nothing).
SETUP = ['python make_data.py']


@dataclass
class Command:
    cmd: str
    expect: dict = field(default_factory=dict)  # keys: exit, planned, ran, failed


@dataclass
class Step:
    id: str
    title: str
    do: str               # the instruction, as the tutor gives it
    commands: list
    snapshot: str = None  # edit to make first: files/<snapshot>/ (git tag snapshot-<name>)
    predict: str = None   # the learner answers this before running
    ask: str = None       # a look-around step: the learner answers this after
    point: str = ''       # what the step teaches, in words that use only earlier ideas


@dataclass
class Lesson:
    number: int
    title: str
    snapshot: str  # the remakefile the lesson starts with (= previous lesson's end)
    intro: str
    steps: list
    model: str     # the mental model so far, for the end-of-lesson recap


LESSONS = [
    Lesson(
        1, 'Your first rule', snapshot='1',
        intro='pipeline.py has one rule, `clean`: it reads one raw data file, drops '
              'the days with a missing reading, and writes a cleaned copy. The raw '
              'data is already in data/raw/ (make_data.py made it).',
        steps=[
            Step('1.1', 'Look at the raw data',
                 'Have a look at the raw data: `ls data/raw data/raw/aberdeen`, then '
                 '`head data/raw/aberdeen/2020.csv`, then `grep NA data/raw/aberdeen/2020.csv`.', [
                Command('ls data/raw data/raw/aberdeen'),
                Command('head data/raw/aberdeen/2020.csv'),
                Command('grep NA data/raw/aberdeen/2020.csv'),
            ], ask='What does a row hold, and what do you think `clean` will remove?',
               point='One CSV per site and year: a day number and a temperature. The '
                     'NA rows are the missing readings the `clean` rule drops.'),
            Step('1.2', 'Run the pipeline', 'Run `remake run pipeline.py`.', [
                Command('remake run pipeline.py', {'exit': 0, 'planned': 1, 'ran': {'clean': 1}}),
            ], predict='How many tasks do you think remake will run?',
               point='A rule describes some work; one piece of that work is a task. This '
                     'rule has one task, `clean[]` (the brackets are for lesson 2). The '
                     'first run creates remake\'s database, .remake/remake.db, where it '
                     'records every task it runs.'),
            Step('1.3', 'Look at what it made',
                 'See what the run left on disk: `ls data/clean/aberdeen`, then '
                 '`wc -l data/raw/aberdeen/2020.csv data/clean/aberdeen/2020.csv`, then '
                 '`ls .remake`.', [
                Command('ls data/clean/aberdeen'),
                Command('wc -l data/raw/aberdeen/2020.csv data/clean/aberdeen/2020.csv'),
                Command('ls .remake'),
            ], ask='How many rows did `clean` drop? And what has remake put in .remake/?',
               point='The output is where `outputs` said, 21 rows shorter (the NA days). '
                     '.remake/ is remake\'s own directory: remake.db is its record of '
                     'tasks; remake.log is a readable log of each command (remake.jsonl '
                     'and remake.debug.log are the same in more detail).'),
            Step('1.4', 'Run it again', 'Run the same command again: `remake run pipeline.py`.', [
                Command('remake run pipeline.py', {'exit': 0, 'planned': 0, 'ran': {}}),
            ], predict='What will remake do this time?',
               point='"Nothing to do": remake\'s record says the task succeeded, and '
                     'nothing about the rule has changed since.'),
            Step('1.5', 'Look at what remake knows',
                 'Try `remake info pipeline.py`, then `remake ls-tasks pipeline.py`.', [
                Command('remake info pipeline.py', {'exit': 0}),
                Command('remake ls-tasks pipeline.py', {'exit': 0}),
            ], point='`info` counts each rule\'s tasks by state; `ls-tasks` lists the '
                     'tasks, each with its key (a short ID, e.g. a7b8c0bc).'),
            Step('1.6', 'Delete the output',
                 'Delete the cleaned file with `rm data/clean/aberdeen/2020.csv`, then '
                 'run `remake run pipeline.py`.', [
                Command('rm data/clean/aberdeen/2020.csv'),
                Command('remake run pipeline.py', {'exit': 0, 'planned': 0, 'ran': {}}),
            ], predict='The output file is gone. Will remake run the task again?',
               point='No: remake decides from its record, not from the files. It never '
                     'looks at file modification times, and by default it doesn\'t '
                     'check that files exist either.'),
            Step('1.7', 'Ask remake to check the files',
                 'Run `remake run pipeline.py --check-outputs`.', [
                Command('remake run pipeline.py --check-outputs',
                        {'exit': 0, 'planned': 1, 'ran': {'clean': 1}}),
            ], predict='What will --check-outputs make remake do?',
               point='--check-outputs checks the outputs of finished tasks and reruns '
                     'those whose outputs are missing.'),
        ],
        model='remake keeps a record of every task it runs (.remake/remake.db). A '
              'task that succeeded doesn\'t run again unless something about it '
              'changes. remake trusts its record, not the files: --check-outputs asks '
              'it to look.'),
    Lesson(
        2, 'A matrix of tasks', snapshot='1',
        intro='One rule, one file isn\'t much of a pipeline. In this lesson one rule '
              'cleans every site and every year.',
        steps=[
            Step('2.1', 'One task per site and year',
                 'Change pipeline.py to use a matrix. Show them the change: '
                 '`git diff lesson-2 snapshot-2 -- pipeline.py` (or they can copy it from '
                 'the lesson page). Then run `remake run pipeline.py`.', [
                Command('remake run pipeline.py', {'exit': 0, 'planned': 12, 'ran': {'clean': 12}}),
            ], snapshot='2',
               predict='The rule now has a matrix of 3 sites x 4 years. How many tasks '
                       'will run, and will aberdeen/2020 (which already ran) run again?',
               point='12 run. `matrix=` makes one task per combination of values, and '
                     '{site} and {year} in the paths are filled in per task. A task is '
                     'identified by its rule and its matrix values, which is what goes '
                     'in the brackets: clean[site=aberdeen, year=2020] is a new task, '
                     'not lesson 1\'s clean[].'),
            Step('2.2', 'Look at the outputs',
                 'See what the matrix made on disk: `find data/clean -name "*.csv" | sort`.', [
                Command('find data/clean -name "*.csv" | sort'),
            ], ask='How many cleaned files are there now, and where did each path come from?',
               point='One file per task: each task fills {site} and {year} into the '
                     'outputs template with its own values.'),
            Step('2.3', 'List and filter tasks',
                 'Run `remake ls-tasks pipeline.py`, then filter it: '
                 '`remake ls-tasks pipeline.py -Q "site == \'exeter\'"`.', [
                Command('remake ls-tasks pipeline.py', {'exit': 0}),
                Command('remake ls-tasks pipeline.py -Q "site == \'exeter\'"', {'exit': 0}),
            ], point='-Q (--query) takes a Python expression over the matrix values; '
                     'most commands accept it.'),
            Step('2.4', 'Force a subset', 'Run `remake run pipeline.py -Q "year == 2023" --force`.', [
                Command('remake run pipeline.py -Q "year == 2023" --force',
                        {'exit': 0, 'planned': 3, 'ran': {'clean': 3}}),
            ], predict='Everything is up to date. How many tasks will this run?',
               point='--force reruns exactly the tasks the query matches, up to date or not.'),
            Step('2.5', 'Add a site',
                 "Add 'durham' to SITES in pipeline.py. Check the plan with a dry run, "
                 '`remake run pipeline.py -n`, then run it for real: `remake run pipeline.py`. '
                 'Then `ls data/clean`.', [
                Command('remake run pipeline.py -n', {'exit': 0, 'planned': 4, 'ran': {}}),
                Command('remake run pipeline.py', {'exit': 0, 'planned': 4, 'ran': {'clean': 4}}),
                Command('ls data/clean'),
            ], snapshot='2-durham',
               predict='With durham added, how many tasks will run?',
               point='Only durham\'s 4 new tasks: the other 12 are unchanged. -n (dry run) '
                     'shows the plan without running anything.'),
        ],
        model='A rule plus a matrix makes many tasks; a task is its rule plus its '
              'matrix values. remake runs the tasks that have never succeeded (or have '
              'changed); growing the matrix adds tasks and only those run. -Q selects '
              'tasks, --force reruns them, -n shows the plan.'),
]


def lesson(number):
    for les in LESSONS:
        if les.number == number:
            return les
    raise SystemExit(f'no lesson {number} (lessons: {", ".join(str(les.number) for les in LESSONS)})')


def final_snapshot(les):
    """The remakefile a lesson ends with."""
    return next((s.snapshot for s in reversed(les.steps) if s.snapshot), les.snapshot)
