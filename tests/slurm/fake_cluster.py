"""A simulated SLURM cluster for testing remake's SLURM reconciliation
(design_docs/designs/slurm_reconcile/testing.md).

The executables on PATH (`sbatch`, `squeue`, `sacct`, `scancel`) are dumb
bash shims: `sbatch` logs its arguments, copies the script and prints a job
id; `squeue` and `sacct` print files that this controller re-renders after
every change; `scancel` logs its arguments. All modelling happens here, in
the test process, on a logical clock: no sleeping.

Finishing an element runs the real `remake run-array-task` (or whatever
`remake ...` line the job's script holds) in-process, so sidecars, logs
and outputs are the real ones.
"""
import contextlib
import os
import shlex
import time
from dataclasses import dataclass, field
from pathlib import Path

ACTIVE = frozenset({'PENDING', 'RUNNING', 'REQUEUED', 'SUSPENDED', 'COMPLETING'})
TERMINAL = frozenset({'COMPLETED', 'FAILED', 'CANCELLED', 'NODE_FAIL', 'TIMEOUT',
                      'OUT_OF_MEMORY', 'PREEMPTED', 'BOOT_FAIL', 'DEADLINE'})
# squeue %t codes.
SHORT = {'PENDING': 'PD', 'RUNNING': 'R', 'REQUEUED': 'RQ', 'SUSPENDED': 'S',
         'COMPLETING': 'CG', 'COMPLETED': 'CD', 'FAILED': 'F', 'CANCELLED': 'CA',
         'NODE_FAIL': 'NF', 'TIMEOUT': 'TO', 'OUT_OF_MEMORY': 'OOM',
         'PREEMPTED': 'PR', 'BOOT_FAIL': 'BF', 'DEADLINE': 'DL'}

SBATCH_SHIM = r'''#!/bin/bash
dir="$(dirname "$0")"
for a in "$@"; do
  case "$a" in --dependency=*)
    for id in $(echo "${a#--dependency=}" | tr ',:' '  '); do
      if [[ "$id" =~ ^[0-9]+$ ]] && grep -qx "$id" "$dir/forgotten" 2>/dev/null; then
        echo "sbatch: error: Batch job submission failed: Job dependency problem" >&2
        exit 1
      fi
    done ;;
  esac
done
n=$(cat "$dir/jobid_counter" 2>/dev/null || echo 1000)
n=$((n + 1))
echo $n > "$dir/jobid_counter"
mkdir -p "$dir/scripts"
cp "${@: -1}" "$dir/scripts/$n.sh"
printf '%s' "$n" >> "$dir/sbatch.log"
printf ' %q' "$@" >> "$dir/sbatch.log"
echo >> "$dir/sbatch.log"
echo $n
'''

# squeue: prints the rendered file; fails when told to.
QUERY_SHIM = r'''#!/bin/bash
dir="$(dirname "$0")"
name="$(basename "$0")"
echo "$name $*" >> "$dir/calls.log"
if [ -e "$dir/$name.fail" ]; then
  echo "$name: error: Socket timed out on send/recv operation" >&2
  exit 1
fi
cat "$dir/$name.out" 2>/dev/null
exit 0
'''

# sacct: prints only the jobs asked for with -j (one rendered file per job),
# and refuses a --format other than the one the simulator renders, so a
# remake that asks for the wrong ids or columns fails here, not on JASMIN.
SACCT_SHIM = r'''#!/bin/bash
dir="$(dirname "$0")"
echo "sacct $*" >> "$dir/calls.log"
if [ -e "$dir/sacct.fail" ]; then
  echo "sacct: error: Problem talking to the database: Connection refused" >&2
  exit 1
fi
ids=""; fmt=""
while [ $# -gt 0 ]; do
  case "$1" in
    -j) ids="$2"; shift ;;
    --format=*) fmt="${1#--format=}" ;;
  esac
  shift
done
if [ "$fmt" != "$(cat "$dir/sacct.format")" ]; then
  echo "fake sacct: unexpected --format=$fmt" >&2
  exit 2
fi
for id in $(echo "$ids" | tr ',' ' '); do
  cat "$dir/sacct.d/$id.psv" 2>/dev/null
done
exit 0
'''

SCANCEL_SHIM = r'''#!/bin/bash
dir="$(dirname "$0")"
echo "$*" >> "$dir/scancel.log"
'''


@dataclass
class Element:
    state: str = 'PENDING'
    start: int | None = None
    end: int | None = None
    node: str | None = None
    exit_code: str = '0:0'
    runs: int = 0  # times started (requeues)
    # How a never-started element was cancelled: 'single' (an aftercorr
    # dependency or `scancel J_i`: no sacct row at all) or 'whole' (the
    # array's pending remainder: one collapsed row). See
    # tests/slurm/fixtures/jasmin_probe_2026-11-23/README.md.
    cancelled: str | None = None
    by_user: bool = False


@dataclass
class Job:
    jobid: str
    name: str
    script: Path
    indices: list
    deps: list = field(default_factory=list)  # [(kind, jobid)]
    kill_on_invalid_dep: bool = False
    output: str = 'slurm-%A_%a.out'
    error: str = 'slurm-%A_%a.err'
    elements: dict = field(default_factory=dict)
    submitted: int = 0
    is_array: bool = True

    def eid(self, index):
        return f'{self.jobid}_{index}' if self.is_array else self.jobid


class FakeCluster:
    """Controller for the simulated cluster rooted at `root` (a project
    directory). Call `install(env)` (or use the `cluster` fixture) to put the
    shims on PATH."""

    def __init__(self, root, *, min_job_age=0, nodes=('host1', 'host2')):
        self.root = Path(root)
        self.bin = self.root / '_fakeslurm'
        self.bin.mkdir(parents=True, exist_ok=True)
        for name, text in (('sbatch', SBATCH_SHIM), ('squeue', QUERY_SHIM),
                           ('sacct', SACCT_SHIM), ('scancel', SCANCEL_SHIM)):
            path = self.bin / name
            path.write_text(text)
            path.chmod(0o755)
        from remake.executors.slurm_accounting import SACCT_FIELDS

        (self.bin / 'sacct.format').write_text(','.join(SACCT_FIELDS) + '\n')
        (self.bin / 'sacct.d').mkdir(exist_ok=True)
        # The logical clock counts seconds from the real time at
        # construction, so it compares with `date +%s` in submit.sh.
        self.t0 = int(time.time())
        self.clock = 0
        self.jobs = {}  # jobid -> Job
        self.min_job_age = min_job_age
        self.nodes = list(nodes)
        # Lags (in ticks): an ended element still RUNNING in accounting; a
        # started element not yet split out of the pending remainder; a new
        # job not yet listed by squeue.
        self.sacct_lag = 0
        self.sacct_start_lag = 0
        self.squeue_lag = 0
        self.hide_out_files = False  # .out files not (yet) visible here
        self.purged = set()
        self._log_pos = 0
        self._scancel_pos = 0
        self.render()

    # --- setup ---

    def path_env(self, env=None):
        env = dict(os.environ if env is None else env)
        env['PATH'] = f'{self.bin}{os.pathsep}{env.get("PATH", "")}'
        return env

    # --- faults ---

    def fail(self, tool, on=True):
        flag = self.bin / f'{tool}.fail'
        if on:
            flag.write_text('')
        else:
            flag.unlink(missing_ok=True)

    def forget(self, jobid):
        """The controller forgets a job: sbatch rejects dependencies on it."""
        with open(self.bin / 'forgotten', 'a') as f:
            f.write(f'{jobid}\n')

    def purge(self, jobid):
        """sacct has no rows for the job any more."""
        self.purged.add(str(jobid))
        self.render()

    def calls(self, tool=None):
        log = self.bin / 'calls.log'
        lines = log.read_text().splitlines() if log.exists() else []
        return [ln for ln in lines if tool is None or ln.split()[0] == tool]

    # --- submission ---

    def sync(self):
        """Pick up jobs submitted since the last call (sbatch.log) and
        scancel requests."""
        log = self.bin / 'sbatch.log'
        if log.exists():
            lines = log.read_text().splitlines()
            for line in lines[self._log_pos:]:
                jobid, *args = shlex.split(line)
                self._add_job(jobid, args)
            self._log_pos = len(lines)
        clog = self.bin / 'scancel.log'
        if clog.exists():
            lines = clog.read_text().splitlines()
            for line in lines[self._scancel_pos:]:
                for target in line.split():
                    if not target.startswith('-'):
                        self.cancel(*target.split('_'))
            self._scancel_pos = len(lines)
        self._settle()
        self.render()

    def _add_job(self, jobid, args):
        script = self.bin / 'scripts' / f'{jobid}.sh'
        opts = {}
        for line in script.read_text().splitlines():
            if line.startswith('#SBATCH '):
                opt = line[len('#SBATCH '):].strip()
                k, _, v = opt.partition('=') if opt.startswith('--') else opt.partition(' ')
                opts[k] = v.strip()
        for arg in args[:-1]:
            if arg.startswith('--'):
                k, _, v = arg.partition('=')
                opts[k] = v
        name = opts.get('--job-name', script.stem)
        is_array = '--array' in opts
        array = opts.get('--array', '0-0').split('%')[0]
        indices = []
        for part in array.split(','):
            a, _, b = part.partition('-')
            indices += list(range(int(a), int(b or a) + 1))
        deps = []
        for clause in filter(None, opts.get('--dependency', '').split(',')):
            kind, *ids = clause.split(':')
            deps += [(kind, i) for i in ids]
        job = Job(jobid, name, script, indices, deps,
                  opts.get('--kill-on-invalid-dep') == 'yes',
                  opts.get('-o', opts.get('--output', 'slurm-%A_%a.out')),
                  opts.get('-e', opts.get('--error', 'slurm-%A_%a.err')),
                  {i: Element() for i in indices}, self.clock)
        job.is_array = is_array
        self.jobs[jobid] = job

    # --- dependencies ---

    def _dep_status(self, job, index):
        """'ok', 'wait' or 'never' for one element's dependencies."""
        status = 'ok'
        for kind, dep_id in job.deps:
            dep = self.jobs.get(dep_id)
            if dep is None:
                continue  # left the controller long ago: treated as satisfied
            if kind == 'aftercorr':
                els = [dep.elements[index]] if index in dep.elements else []
            else:
                els = list(dep.elements.values())
            states = [e.state for e in els]
            if kind == 'afterany':
                if any(s not in TERMINAL for s in states):
                    status = 'wait'
            elif any(s in TERMINAL and s != 'COMPLETED' for s in states):
                return 'never'
            elif any(s != 'COMPLETED' for s in states):
                status = 'wait'
        return status

    def _settle(self):
        """Cancel elements whose dependency can never be met
        (kill-on-invalid-dep), repeatedly, as SLURM does."""
        changed = True
        while changed:
            changed = False
            for job in self.jobs.values():
                for i, el in job.elements.items():
                    if (el.state == 'PENDING' and job.kill_on_invalid_dep
                            and self._dep_status(job, i) == 'never'):
                        el.state, el.end = 'CANCELLED', self.clock
                        el.cancelled = ('single' if any(k == 'aftercorr' for k, _ in job.deps)
                                        else 'whole')
                        changed = True

    def eligible(self):
        """[(jobid, index)] of pending elements whose dependencies are met."""
        self.sync()
        return [(job.jobid, i) for job in self.jobs.values()
                for i, el in job.elements.items()
                if el.state == 'PENDING' and self._dep_status(job, i) == 'ok']

    # --- element life cycle ---

    def _el(self, jobid, index):
        return self.jobs[str(jobid)].elements[int(index)]

    def _out_path(self, job, index, pattern):
        path = pattern.replace('%A', job.jobid).replace('%a', str(index))
        return self.root / path

    def start(self, jobid, index, node=None):
        self.sync()
        job, el = self.jobs[str(jobid)], self._el(jobid, index)
        assert el.state == 'PENDING', (jobid, index, el.state)
        el.state, el.start, el.end = 'RUNNING', self.clock, None
        el.node = node or self.nodes[int(index) % len(self.nodes)]
        el.runs += 1
        for pattern in (job.output, job.error):
            path = self._out_path(job, int(index), pattern)
            if self.hide_out_files:
                path = path.with_name(path.name + '.hidden')
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        self.render()

    def show_out_files(self):
        """The .out/.err files written while hidden become visible."""
        self.hide_out_files = False
        for path in self.root.rglob('*.hidden'):
            path.rename(path.with_name(path.name.removesuffix('.hidden')))

    def finish(self, jobid, index):
        """Run the element's payload (the `remake ...` command in its
        script) in-process; COMPLETED or FAILED by its exit status."""
        job, el = self.jobs[str(jobid)], self._el(jobid, index)
        if el.state == 'PENDING':
            self.start(jobid, index)
        assert el.state == 'RUNNING', (jobid, index, el.state)
        code = self._run_payload(job, int(index))
        el.state = 'COMPLETED' if code == 0 else 'FAILED'
        el.exit_code = f'{code}:0'  # FAILED 3:0 in the probe
        el.end = self.clock
        self._settle()
        self.render()
        return code

    def _run_payload(self, job, index, extra_env=None):
        from remake.remake_cmd import remake_cmd

        env = {'SLURM_JOB_ID': job.jobid, **(extra_env or {})}
        if job.is_array:
            env.update(SLURM_ARRAY_JOB_ID=job.jobid, SLURM_ARRAY_TASK_ID=str(index))
        commands = []
        for line in job.script.read_text().splitlines():
            line = line.strip()
            if line.startswith('export '):
                k, _, v = line[len('export '):].partition('=')
                env[k] = v
            elif line.startswith('remake '):
                line = line.replace('$SLURM_ARRAY_TASK_ID', str(index))
                commands.append(shlex.split(line))
        code = 0
        out = self._out_path(job, index, job.output)
        err = self._out_path(job, index, job.error)
        out.parent.mkdir(parents=True, exist_ok=True)
        with (_environ(env), contextlib.chdir(self.root), open(out, 'a') as fo,
              open(err, 'a') as fe, contextlib.redirect_stdout(fo),
              contextlib.redirect_stderr(fe)):
            for argv in commands:
                try:
                    rc = remake_cmd(argv)
                except SystemExit as e:
                    rc = e.code if isinstance(e.code, int) else 1
                except Exception as e:
                    print(f'{type(e).__name__}: {e}', file=fe)
                    rc = 1
                code = rc or 0
                if code:
                    break
        return code

    # ExitCode as the live probe saw it.
    EXIT_CODES = {'NODE_FAIL': '1:0', 'TIMEOUT': '0:0', 'OUT_OF_MEMORY': '0:125',
                  'CANCELLED': '0:0', 'PREEMPTED': '0:0', 'BOOT_FAIL': '0:0',
                  'DEADLINE': '0:0', 'FAILED': '1:0'}

    def kill(self, jobid, index, state='NODE_FAIL', partial=False):
        """End an element without its payload finishing: no sidecar. A
        pending element is started first unless `state` is one a pending
        element can get (DEADLINE, BOOT_FAIL; cancel() for CANCELLED).
        partial=True: killed after writing its outputs but before
        reporting (the payload runs; its result file is dropped)."""
        assert state in TERMINAL and state != 'COMPLETED', state
        self.sync()
        job, el = self.jobs[str(jobid)], self._el(jobid, index)
        if el.state == 'PENDING' and state not in ('DEADLINE', 'BOOT_FAIL'):
            self.start(jobid, index)
        if partial:
            # Killed after writing its outputs, before reporting: run the
            # payload, then drop the result file it wrote.
            results = self.root / '.remake'
            before = set(results.rglob('results/**/*.json'))
            self._run_payload(job, int(index))
            for path in set(results.rglob('results/**/*.json')) - before:
                path.unlink()
        el.state, el.end = state, self.clock
        el.exit_code = self.EXIT_CODES[state] if el.start is not None else '0:0'
        self._settle()
        self.render()

    def cancel(self, jobid, index=None):
        """scancel: a pending element never starts; a running one is killed."""
        job = self.jobs[str(jobid)]
        targets = [int(index)] if index is not None else list(job.elements)
        for i in targets:
            el = job.elements[i]
            if el.state in TERMINAL:
                continue
            el.state, el.end = 'CANCELLED', self.clock
            el.exit_code = '0:0'  # scancel of a running element: 0:0 (probe)
            el.by_user = True
            if el.start is None:
                el.cancelled = 'whole' if index is None else 'single'
        self._settle()
        self.render()

    def requeue(self, jobid, index):
        """A running element goes back to pending (its start is kept in
        nothing but the .out file; sacct shows the latest incarnation)."""
        el = self._el(jobid, index)
        assert el.state == 'RUNNING'
        el.state, el.start, el.end, el.node = 'PENDING', None, None, None
        self.render()

    def tick(self, n=1):
        self.clock += n
        self.render()

    def drain(self, max_steps=10_000):
        """Run everything eligible to completion."""
        for _ in range(max_steps):
            ready = self.eligible()
            if not ready:
                return
            for jobid, index in ready:
                self.finish(jobid, index)
            self.tick()
        raise RuntimeError('drain did not finish')

    # --- rendering ---

    def render(self):
        squeue = []
        sacct_dir = self.bin / 'sacct.d'
        for job in self.jobs.values():
            listed = self.clock - job.submitted >= self.squeue_lag
            for i, el in sorted(job.elements.items()):
                ended_long_ago = (el.state in TERMINAL and el.end is not None
                                  and self.clock - el.end >= self.min_job_age)
                if listed and not ended_long_ago:
                    reason = 'Dependency' if el.state == 'PENDING' and job.deps else 'None'
                    squeue.append(f'{job.eid(i)} {SHORT[el.state]} {reason}')
            rows = [] if job.jobid in self.purged else self._sacct_rows(job)
            (sacct_dir / f'{job.jobid}.psv').write_text(''.join(f'{ln}\n' for ln in rows))
        (self.bin / 'squeue.out').write_text(''.join(f'{ln}\n' for ln in squeue))

    def sacct_text(self):
        """Everything sacct would print for all jobs (for tests)."""
        return ''.join(p.read_text() for p in sorted((self.bin / 'sacct.d').glob('*.psv')))

    def _sacct_rows(self, job):
        """sacct -X -P rows: JobID|State|ExitCode|Start|End|Elapsed|NodeList,
        times as epoch seconds (remake sets SLURM_TIME_FORMAT=%s)."""
        rows, pending, whole, never = [], [], [], []
        for i, el in sorted(job.elements.items()):
            not_split = (el.start is not None and el.state == 'RUNNING'
                         and self.clock - el.start < self.sacct_start_lag)
            if el.start is None or not_split:
                # Never started (or started too recently for accounting):
                # part of the unsplit remainder. A whole-array cancel keeps
                # the remainder's row; an element cancelled on its own has
                # none (live probe).
                never.append((i, el))
                if el.state in ('PENDING', 'RUNNING'):
                    pending.append(i)
                elif el.cancelled == 'whole':
                    whole.append((i, el))
                continue
            lagging = (el.state in TERMINAL and el.end is not None
                       and self.clock - el.end < self.sacct_lag)
            rows.append(self._row(job.eid(i), job.name,
                                  'RUNNING' if lagging else el.state, el))
        if not job.is_array:
            if never:
                i, el = never[0]
                state = 'PENDING' if el.state in ('PENDING', 'RUNNING') else el.state
                rows.append(self._row(job.jobid, job.name, state, Element(
                    by_user=el.by_user, end=None if state == 'PENDING' else el.end)))
            return rows
        if any(k == 'aftercorr' for k, _ in job.deps):
            # An aftercorr array's unsplit remainder is one row held by the
            # array's highest index, unbracketed: PENDING while any element
            # is pending, CANCELLED once none is. When the holder itself
            # starts, its row becomes its own and the remainder has none.
            # (Live probe: B_5 stood for 0, 2, 3, 5, then ran; soak
            # archive: 6133150_16 never started and kept a CANCELLED row.)
            holder = max(job.elements)
            if any(i == holder for i, _ in never):
                state = 'PENDING' if pending else 'CANCELLED'
                end = None if pending else max(el.end or 0 for _, el in never) or self.clock
                rows.append(self._row(f'{job.jobid}_{holder}', job.name, state,
                                      Element(end=end)))
            return rows
        if pending:
            rows.append(self._row(f'{job.jobid}_[{_ranges(pending)}]', job.name, 'PENDING',
                                  Element()))
        if whole:
            el = whole[-1][1]
            rows.append(self._row(f'{job.jobid}_[{_ranges([i for i, _ in whole])}]',
                                  job.name, 'CANCELLED', el))
        return rows

    def _row(self, jobid, name, state, el):
        if state == 'CANCELLED' and el.by_user:
            state = 'CANCELLED by 12345'

        def t(v):
            return str(self.t0 + v) if v is not None else 'Unknown'
        start = t(el.start) if el.start is not None else (
            'Unknown' if state == 'PENDING' else 'None')
        end = t(el.end) if el.end is not None and state.split()[0] not in ACTIVE else 'Unknown'
        elapsed = (el.end if el.end is not None else self.clock) - el.start \
            if el.start is not None else 0
        node = el.node or 'None assigned'
        # Exactly the fields remake asks for (slurm_accounting.SACCT_FIELDS).
        return '|'.join([jobid, state, el.exit_code, start, end, _hms(elapsed), node])


def _ranges(indices):
    out, run = [], [indices[0]]
    for i in indices[1:]:
        if i == run[-1] + 1:
            run.append(i)
        else:
            out.append(run)
            run = [i]
    out.append(run)
    return ','.join(f'{r[0]}-{r[-1]}' if len(r) > 1 else str(r[0]) for r in out)


def _hms(seconds):
    return f'{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}'


@contextlib.contextmanager
def _environ(values):
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
