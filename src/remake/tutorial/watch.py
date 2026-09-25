"""Follow the workspace's two event logs and print one line per finished
command, for the tutor.

- .remake/remake.jsonl — remake's own structured log: one summary line per
  finished `remake` command (on its `invocation_end` event), never per
  task. Commands tagged REMAKE_ORIGIN=tutor (the tutor's own checks,
  `reset`'s replays) are skipped.
- .tutorial/events.jsonl — written by `remake-tutorial` itself: one line
  when a `reset` finishes (or fails).

The tutor runs `remake-tutorial watch` under Claude Code's Monitor tool, so
each printed line is one wake-up. Survives .remake/ being deleted and
recreated and loguru's rotation (the file is renamed and a fresh one
started): each file is followed by path, with the old handle drained
before switching.
"""
import ast
import json
import os
import shlex
import sys
import time
from pathlib import Path

LOG = Path('.remake/remake.jsonl')
EVENTS = Path('.tutorial/events.jsonl')


def summarise(records):
    """Summary of one invocation's records (the `extra` dicts, in order):
    {'argv', 'exit', 'planned', 'ran': {rule: n}, 'failed': {rule: n},
    'seconds', 'origin'} (origin: REMAKE_ORIGIN, e.g. 'tutor'). `planned` is the first plan's runnable count (later waves
    replan); None if the command never planned."""
    out = {'argv': None, 'exit': None, 'planned': None, 'ran': {}, 'failed': {},
           'seconds': None, 'origin': None}
    for extra, message in records:
        event = extra.get('event')
        out['origin'] = out['origin'] or extra.get('origin')
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


def format_event(e):
    """A `remake-tutorial` event as the tutor sees it (None: not shown)."""
    if e.get('event') != 'reset':
        return None
    if e.get('status') == 'ready':
        kept = ''.join(f'; kept {k}' for k in e.get('kept', []))
        return (f"[tutorial] remake-tutorial reset {e['lesson']} -> "
                f"lesson {e['lesson']} ready{kept}")
    return f"[tutorial] remake-tutorial reset {e['lesson']} -> FAILED: {e.get('error')}"


def _records(path):
    """(timestamp, record) for each parseable line of a remake.jsonl."""
    for line in Path(path).read_text().splitlines():
        try:
            rec = json.loads(line)['record']
            ts = float(rec['time']['timestamp'])
        except (ValueError, KeyError, TypeError):
            continue  # a line still being written, or a killed writer's
        yield ts, rec


def read_invocations(path=LOG):
    """Every invocation in `path` as (run_id, summary, start timestamp),
    oldest first (for tests and for the tutor catching up)."""
    by_run, order, when = {}, [], {}
    for ts, rec in _records(path):
        run_id = rec['extra'].get('run_id')
        if run_id is None:
            continue
        if run_id not in by_run:
            by_run[run_id], when[run_id] = [], ts
            order.append(run_id)
        by_run[run_id].append((rec['extra'], rec['message']))
    return [(run_id, summarise(by_run[run_id]), when[run_id]) for run_id in order]


def history(log=LOG, events=EVENTS):
    """Every learner command and tutorial event so far, as the watcher
    would have printed them, oldest first."""
    lines = []
    if Path(log).exists():
        for _, s, ts in read_invocations(log):
            if s['exit'] is not None and s['origin'] != 'tutor':
                lines.append((ts, format_summary(s)))
    if Path(events).exists():
        for line in Path(events).read_text().splitlines():
            try:
                e = json.loads(line)
                text, ts = format_event(e), float(e.get('time', 0))
            except (ValueError, KeyError, TypeError, AttributeError):
                continue
            if text:
                lines.append((ts, text))
    return [text for _, text in sorted(lines, key=lambda p: p[0])]


class _Tail:
    """New complete lines of a file followed by path: the open handle is
    drained before switching to a replacement (rotation) or dropping it
    (deletion)."""

    def __init__(self, path):
        self.path, self.f, self.ino, self.buf = Path(path), None, None, ''
        if self.path.exists():
            self._open()
            self.f.seek(0, os.SEEK_END)

    def _open(self):
        self.f = open(self.path, encoding='utf-8')  # noqa: SIM115 — held across polls
        self.ino = os.fstat(self.f.fileno()).st_ino

    def _read(self):
        self.buf += self.f.read()
        *lines, self.buf = self.buf.split('\n')
        return lines

    def poll(self):
        """(lines, gone): new complete lines, and whether the file just
        disappeared."""
        lines, gone = [], False
        if self.f is not None:
            lines += self._read()
        try:
            st = self.path.stat()
        except FileNotFoundError:
            st = None
        if self.f is not None and (st is None or st.st_ino != self.ino
                                   or st.st_size < self.f.tell()):
            lines += self._read()  # written to the old file since the last read
            self.f.close()
            self.f, self.ino, self.buf = None, None, ''
            gone = st is None
        if self.f is None and st is not None:
            self._open()
            lines += self._read()
        return lines, gone


def follow(path=LOG, poll=0.5, out=sys.stdout, events=EVENTS):
    """Print a line per finished command (and tutorial event) from now on.
    Runs until killed."""
    log, ev = _Tail(path), _Tail(events)
    pending, state = {}, {'resetting': None}  # pid of a reset in progress
    print(f'[watch] following {Path(path).resolve()}', file=out, flush=True)
    while True:
        # Tutorial events first: a reset announces itself before it moves
        # .remake/ aside, so that isn't reported as the learner deleting it.
        for line in ev.poll()[0]:
            _handle_event(line, state, out)
        lines, gone = log.poll()
        for line in lines:
            _handle(line, pending, out)
        if gone and not _alive(state['resetting']):
            print('[watch] remake.jsonl is gone (was .remake/ deleted?)', file=out, flush=True)
        time.sleep(poll)


def _alive(pid):
    """Is the reset that announced itself still running? (A reset killed
    outright never emits its end event; don't stay muted for it.)"""
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True  # exists, not ours to signal
    return True


def _handle_event(line, state, out):
    try:
        e = json.loads(line)
        if e.get('event') == 'reset_started':
            state['resetting'] = e.get('pid') or os.getpid()
            return
        state['resetting'] = None
        text = format_event(e)
    except (ValueError, KeyError, TypeError, AttributeError):
        return
    if text:
        print(text, file=out, flush=True)


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
