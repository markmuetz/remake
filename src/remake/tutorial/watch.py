"""Follow .remake/remake.jsonl and print one line per finished remake command.

The tutor runs `remake-tutorial watch` under Claude Code's Monitor tool:
each line printed is one wake-up, so the output is one compact summary per
command (on its `invocation_end` event), never per task. Commands tagged
REMAKE_ORIGIN=tutor (the tutor's own checks, `remake-tutorial reset`'s
replays) are skipped.

Survives .remake/ being deleted and recreated, and loguru's rotation (the
file is renamed and a fresh one started): it follows the path, not the
open file.
"""
import ast
import json
import os
import shlex
import sys
import time
from pathlib import Path

LOG = Path('.remake/remake.jsonl')


def summarise(records):
    """Summary of one invocation's records (the `extra` dicts, in order):
    {'argv', 'exit', 'planned', 'ran': {rule: n}, 'failed': {rule: n},
    'seconds'}. `planned` is the first plan's runnable count (later waves
    replan); None if the command never planned."""
    out = {'argv': None, 'exit': None, 'planned': None, 'ran': {}, 'failed': {},
           'seconds': None}
    for extra, message in records:
        event = extra.get('event')
        if event == 'invocation':
            out['argv'] = _command_line(message.removeprefix('argv: '))
        elif event == 'plan' and out['planned'] is None:
            out['planned'] = extra.get('nrunnable')
        elif event in ('task_complete', 'task_failed'):
            bucket = out['ran' if event == 'task_complete' else 'failed']
            bucket[extra['rule']] = bucket.get(extra['rule'], 0) + 1
        elif event == 'invocation_end':
            out['exit'] = extra.get('exit_code')
            out['seconds'] = extra.get('seconds')
    return out


def _command_line(argv_repr):
    """"['remake', 'run', 'pipeline.py']" -> "remake run pipeline.py"."""
    try:
        argv = ast.literal_eval(argv_repr)
        return ' '.join(shlex.quote(a) for a in ['remake', *argv[1:]])
    except (ValueError, SyntaxError, TypeError):
        return argv_repr


def format_summary(s):
    def counts(d):
        return ', '.join(f'{rule}:{n}' for rule, n in d.items()) or '0'
    bits = [f"exit {s['exit']}"]
    if s['planned'] is not None:
        bits.append(f"planned {s['planned']}")
    bits.append(f"ran {counts(s['ran'])}")
    if s['failed']:
        bits.append(f"FAILED {counts(s['failed'])}")
    return f"[remake] {s['argv']} -> " + ' | '.join(bits)


def read_invocations(path=LOG):
    """Every invocation in `path`, summarised, oldest first (for tests and
    for the tutor catching up)."""
    by_run, order = {}, []
    for line in Path(path).read_text().splitlines():
        try:
            rec = json.loads(line)['record']
        except (ValueError, KeyError):
            continue  # a line still being written, or a killed writer's
        run_id = rec['extra'].get('run_id')
        if run_id is None:
            continue
        if run_id not in by_run:
            by_run[run_id] = []
            order.append(run_id)
        by_run[run_id].append((rec['extra'], rec['message']))
    return [(run_id, summarise(by_run[run_id])) for run_id in order]


def follow(path=LOG, poll=0.5, out=sys.stdout):
    """Print a summary line per finished command appended to `path` from now
    on. Runs until killed.

    Keeps the file open, so when it is rotated (renamed) or deleted the
    records written just before are still read from the old handle before
    moving to the new file."""
    pending = {}
    f, ident, buf = None, None, ''

    def drain():
        nonlocal buf
        buf += f.read()
        *lines, buf = buf.split('\n')
        for line in lines:
            _handle(line, pending, out)

    if path.exists():
        f = open(path, encoding='utf-8')  # noqa: SIM115 — held across polls
        f.seek(0, os.SEEK_END)
        ident = os.fstat(f.fileno()).st_ino
    print(f'[watch] following {path.resolve()}', file=out, flush=True)
    while True:
        if f is not None:
            drain()
        try:
            st = path.stat()
        except FileNotFoundError:
            st = None
        if f is not None and (st is None or st.st_ino != ident
                              or st.st_size < f.tell()):
            drain()  # anything written to the old file since the last read
            f.close()
            f, ident, buf = None, None, ''
            if st is None:
                print('[watch] remake.jsonl is gone (was .remake/ deleted?)',
                      file=out, flush=True)
        if f is None and st is not None:
            f = open(path, encoding='utf-8')  # noqa: SIM115
            ident = os.fstat(f.fileno()).st_ino
            continue  # read the new file straight away
        time.sleep(poll)


def _handle(line, pending, out):
    try:
        rec = json.loads(line)['record']
    except (ValueError, KeyError):
        return
    extra = rec['extra']
    run_id = extra.get('run_id')
    if run_id is None or extra.get('origin') == 'tutor':
        return
    pending.setdefault(run_id, []).append((extra, rec['message']))
    if extra.get('event') == 'invocation_end':
        print(format_summary(summarise(pending.pop(run_id))), file=out, flush=True)


def main():
    if not Path('.tutorial').is_dir():
        sys.exit('remake-tutorial watch: run it from the tutorial workspace')
    try:
        follow()
    except KeyboardInterrupt:
        os._exit(0)
