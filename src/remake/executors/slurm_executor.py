"""SLURM executor.

Writes per-rule JSON job specs, per-rule sbatch scripts and a master
.remake/submit.sh, then executes it (unless dry_run). See
design_docs/remake3_design.md (SLURM executor) for the full behaviour.

File layout (all relative to the working directory, like the metadata DB):
    .remake/jobs/<rule>.<run_seq>.json  array of {task_key, rule, kwargs,
                                     run_seq}; SLURM array index = position.
                                     Immutable: written once per submission,
                                     never rewritten — the sbatch script pins
                                     this exact path (--specs), so replans and
                                     dry runs (which write a new file under
                                     their own run_seq) can never corrupt the
                                     indices a queued array reads. Pruned
                                     after a week (each rule's last-submitted
                                     spec is kept at any age).
    .remake/jobs/<rule>.jobids.json  sidecar written at submission; records
                                     the job id(s) and the submission's
                                     run_seq (pinning jobids to their spec
                                     file)
    .remake/slurm/<rule>.sbatch      per-rule script (safe to rewrite: SLURM
                                     copies it at submission)
    .remake/slurm/output/<rule>/     job stdout/stderr
    .remake/submit.sh                master script; `remake resubmit`
                                     re-executes it without replanning

Task kwargs must be JSON-serialisable (they round-trip through the job
specs). Already-queued detection is per rule: if any element of a rule's
previous submission is still pending/running, the whole rule is skipped
this run to avoid submitting duplicates of the in-flight tasks. Skipped
tasks are picked up by a later run.
"""
import getpass
import json
import re
import shlex
import subprocess as sp
import time
from pathlib import Path

from loguru import logger

from ..core.exceptions import RemakeError
from .executor import Executor

DEFAULT_SLURM_CONFIG = {
    'partition': 'standard',
    'qos': 'standard',
    'time': '4:00:00',
    'mem': '4G',
}

# Every rule is submitted as one array job, even for a single task
# (--array=0-0): one submission mode means one sbatch template, one submit
# line shape and one sidecar encoding (review finding C2).
ARRAY_SBATCH_TPL = """#!/bin/bash
#SBATCH --job-name={rule_name}
#SBATCH --array=0-{max_index}{array_throttle}
#SBATCH -o {output_dir}/%a.out
#SBATCH -e {output_dir}/%a.err
#SBATCH --kill-on-invalid-dep=yes
{opts}
echo "SLURM RUNNING {rule_name} $SLURM_ARRAY_TASK_ID"
remake run-array-task {remakefile} {rule_name} $SLURM_ARRAY_TASK_ID --specs {specs}
rc=$?
echo "SLURM COMPLETED {rule_name} $SLURM_ARRAY_TASK_ID (rc=$rc)"
exit $rc
"""

CONTINUATION_SBATCH_TPL = """#!/bin/bash
#SBATCH --job-name=remake_continue
#SBATCH -o {output_dir}/continuation.out
#SBATCH -e {output_dir}/continuation.err
#SBATCH --kill-on-invalid-dep=yes
{opts}
remake run {remakefile} --executor slurm
"""


def _sbatch_opts(config):
    return '\n'.join(f'#SBATCH --{k}={v}' for k, v in config.items() if v)


class SqueueError(RemakeError):
    """squeue could not be run, so the queue state is unknown. Distinct from
    an empty queue: treating "unknown" as "empty" would green-light
    resubmitting jobs that are still in flight
    (design_docs/slurm_already_running.md)."""


SQUEUE_TIMEOUT = 60

# squeue %t codes for jobs that are finished or will never run again. Any
# other state — PD/R/CF, but also suspended (S/ST), held/requeued (RH/RD/RQ/
# SE) and completing (CG) — can still (re-)read its job specs and write its
# outputs, so it counts as active. Inverted (inactive, not active) so an
# unrecognised state fails safe: treated as active, the rule is deferred to
# a later run rather than double-submitted.
INACTIVE_STATES = frozenset(('BF', 'CA', 'CD', 'DL', 'F', 'NF', 'OOM', 'TO'))


def squeue_snapshot():
    """{base_jobid: [(element_id, state, reason)]} for this user's queued/
    running jobs, one squeue call. {} means squeue ran and the queue is
    empty; raises SqueueError when squeue is missing, fails or hangs —
    callers must fail safe rather than assume an empty queue."""
    try:
        result = sp.run(
            ['squeue', '-h', '-r', '-u', getpass.getuser(), '-o', '%i %t %r'],
            capture_output=True, text=True, check=True, timeout=SQUEUE_TIMEOUT,
        )
    except FileNotFoundError as e:
        raise SqueueError('squeue not found: cannot check the job queue') from e
    except sp.TimeoutExpired as e:
        raise SqueueError(
            f'squeue gave no response within {SQUEUE_TIMEOUT}s: queue state unknown'
        ) from e
    except sp.CalledProcessError as e:
        detail = (e.stderr or '').strip() or f'exit {e.returncode}'
        raise SqueueError(f'squeue failed ({detail}): queue state unknown') from e
    snapshot = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split(maxsplit=2)
        elem_id, state = parts[0], parts[1]
        reason = parts[2] if len(parts) > 2 else ''
        snapshot.setdefault(elem_id.split('_')[0], []).append((elem_id, state, reason))
    return snapshot


JOBS_DIR = Path('.remake/jobs')


def spec_path(rule_name, run_seq=None):
    """Path of a rule's job-spec file: the immutable per-submission file for
    run_seq, or the legacy unversioned path when run_seq is None (pre-0.9
    submissions, whose sidecars carry no run_seq)."""
    if run_seq is None:
        return JOBS_DIR / f'{rule_name}.json'
    return JOBS_DIR / f'{rule_name}.{run_seq}.json'


def submitted_spec_path(rule_name):
    """The spec file the rule's last recorded submission actually ran
    against: resolved through the jobids sidecar's run_seq (the legacy
    unversioned path for pre-0.9 sidecars), so dry runs — which write spec
    files but never sidecars — can't skew it. Falls back to the newest spec
    file on disk when nothing was ever submitted (dry-run debugging), None
    if there are no specs at all."""
    recorded = _read_sidecar(JOBS_DIR / f'{rule_name}.jobids.json')
    if recorded is not None:
        return spec_path(rule_name, recorded.get('run_seq'))
    return latest_spec_path(rule_name)


def latest_spec_path(rule_name):
    """The rule's most recent job-spec file (highest run_seq, falling back to
    the legacy unversioned file), or None if none exists. Newest-on-disk
    includes unsubmitted dry-run plans — prefer submitted_spec_path for
    anything that must match a real submission."""
    versioned = []
    for path in JOBS_DIR.glob(f'{rule_name}.*.json'):
        seq = path.name.removeprefix(f'{rule_name}.').removesuffix('.json')
        if seq.isdigit():
            versioned.append((int(seq), path))
    if versioned:
        return max(versioned)[1]
    legacy = spec_path(rule_name)
    return legacy if legacy.exists() else None


def _read_sidecar(sidecar):
    """Parse a jobids sidecar; None if missing or unreadable. Sidecars are
    written by a shell redirect in submit.sh, so a killed script or full
    disk can leave a truncated file — callers treat that as 'no recorded
    submission' (fail safe) rather than crashing every SLURM command."""
    try:
        return json.loads(sidecar.read_text())
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f'Unreadable jobids sidecar {sidecar} ({e}); treating as absent')
        return None


SPEC_MAX_AGE_DAYS = 7


def prune_spec_files(max_age_days=SPEC_MAX_AGE_DAYS):
    """Delete job-spec files older than max_age_days, keeping each rule's
    sidecar-referenced (last-submitted) spec at any age. Age-based rather
    than provably-unreferenced: walltime limits (~2 days) mean no running
    job reads a week-old spec, and the sidecar exception covers the one
    case that outlives the window — the last submission pending for a long
    time. (Full design + accepted residual risk:
    design_docs/slurm_already_running.md, 2026-07-10 decisions.)"""
    if not JOBS_DIR.exists():
        return
    keep = set()
    skip_rules = set()
    for sidecar in JOBS_DIR.glob('*.jobids.json'):
        rule_name = sidecar.name.removesuffix('.jobids.json')
        recorded = _read_sidecar(sidecar)
        if recorded is None:
            # Can't tell which spec the submission pins: keep all of them.
            skip_rules.add(rule_name)
            continue
        keep.add(spec_path(rule_name, recorded.get('run_seq')))
    cutoff = time.time() - max_age_days * 86400
    npruned = 0
    for path in JOBS_DIR.glob('*.json'):
        if path.name.endswith('.jobids.json') or path in keep:
            continue
        if path.name.split('.')[0] in skip_rules:
            continue
        if path.stat().st_mtime < cutoff:
            path.unlink()
            npruned += 1
    if npruned:
        logger.info(f'Pruned {npruned} job-spec file(s) older than {max_age_days} day(s)')


def recorded_jobids(rule_name):
    """Job id(s) from the rule's last-submission sidecar, [] if never
    submitted. One base id per rule since arrays-everywhere; 'slurm_job_ids'
    (one id per task) is read for sidecars written by pre-arrays-only
    versions, whose jobs may still be queued."""
    recorded = _read_sidecar(JOBS_DIR / f'{rule_name}.jobids.json')
    if recorded is None:
        return []
    if 'slurm_array_job_id' in recorded:
        return [recorded['slurm_array_job_id']]
    return recorded.get('slurm_job_ids', [])


def active_jobids():
    """Base job ids of this user's jobs with any non-terminal element —
    pending/running, but also suspended/held/requeued ('1234_5' array
    elements map to base id '1234'). Raises SqueueError when the queue
    state is unknown."""
    return {
        base
        for base, elems in squeue_snapshot().items()
        if any(state not in INACTIVE_STATES for _, state, _ in elems)
    }


def check_resubmit_safe(submit_path):
    """Guard for `remake resubmit`: raise RemakeError when re-executing
    submit.sh would duplicate still-queued jobs, or would abort halfway on
    dependency ids baked in as literals whose jobs have left the queue.
    submit.sh is executed verbatim (no replanning), so the only safe course
    on any doubt — including an unreachable squeue — is to refuse and point
    at `remake run`, which replans and skips queued rules correctly."""
    text = submit_path.read_text()
    try:
        active = active_jobids()
    except SqueueError as e:
        raise RemakeError(
            f'{e}. Cannot verify nothing in {submit_path} is still queued: '
            'refusing to resubmit. Retry when squeue works, or replan with: '
            'remake run <remakefile> --executor slurm'
        ) from e

    # One sidecar write per rule the script submits.
    submitted_rules = re.findall(r'> \S*/([^/\s]+)\.jobids\.json', text)
    queued = {
        rule: ids for rule in submitted_rules
        if (ids := [j for j in recorded_jobids(rule) if j in active])
    }
    if queued:
        detail = ', '.join(f'{rule} (job {",".join(ids)})' for rule, ids in queued.items())
        raise RemakeError(
            f'Still queued from the last submission: {detail}. Resubmitting '
            f'would run duplicates racing on the same outputs. Wait, or '
            'replan with: remake run <remakefile> --executor slurm '
            '(skips queued rules and submits the rest)'
        )

    # Rules already queued at generation time were skipped, wiring their
    # downstream --dependency flags to literal job ids. Once those jobs
    # leave the queue the ids go stale: sbatch rejects the dependency and
    # `set -e` aborts the script halfway (jobs submitted, sidecars not
    # written). Only a replan re-derives the wiring.
    literal_ids = set()
    for dep in re.findall(r'--dependency=(\S+)', text):
        for clause in dep.split(','):  # 'afterok:1001:$JOB_proc'
            literal_ids.update(t for t in clause.split(':')[1:] if t.isdigit())
    stale = sorted(jobid for jobid in literal_ids if jobid not in active)
    if stale:
        raise RemakeError(
            f'{submit_path} depends on job id(s) {", ".join(stale)} that '
            'have left the queue (baked in when those rules were skipped as '
            'already queued). sbatch would reject the dependency mid-script. '
            'Replan with: remake run <remakefile> --executor slurm'
        )


def last_submission(rule_name, task_key=None):
    """(jobids, array_index) from the last submission's sidecar/job spec,
    (None, None) if the rule was never submitted. `array_index` is the task's
    position in the job spec of the submission the sidecar records (pinned by
    its run_seq, so later replans don't skew it); None unless task_key is
    given."""
    recorded = _read_sidecar(JOBS_DIR / f'{rule_name}.jobids.json')
    if recorded is None:
        return None, None
    jobids = recorded_jobids(rule_name)
    index = None
    specs_path = spec_path(rule_name, recorded.get('run_seq'))
    if task_key is not None and specs_path.exists():
        specs = json.loads(specs_path.read_text())
        index = next((i for i, s in enumerate(specs) if s['task_key'] == task_key), None)
    return jobids, index


def _elementwise(upstream_tasks, tasks):
    """True iff element i of `tasks` reads, among all the upstream outputs,
    only those produced by upstream element i — the condition for SLURM's
    aftercorr (element N starts when upstream element N finishes). Equal
    kwargs lists are NOT sufficient: a stencil rule (task t reads upstream
    t-1, t, t+1) has an identical matrix, yet aftercorr would start element
    t while its neighbours' inputs are unwritten — silent partial data
    (review finding 7). Derived from resolved task inputs/outputs, plain
    paths available at generation time."""
    if len(upstream_tasks) != len(tasks):
        return False
    up_outputs = [{str(p) for p in t.outputs.values()} for t in upstream_tasks]
    all_up = set().union(*up_outputs)
    if sum(len(outs) for outs in up_outputs) != len(all_up):
        # Elements share an output (e.g. one zarr store region-written by
        # all): "element i's file" is every element's file, so the subset
        # test below would pass vacuously while element i's data is still
        # being written by its siblings.
        return False
    reads = [{str(p) for p in task.inputs.values()} & all_up for task in tasks]
    # Every element must actually read from its counterpart (an empty
    # intersection — ordering-only depends_on — proves nothing).
    return all(read and read <= up_outputs[i] for i, read in enumerate(reads))


class _SubmittedRule:
    """How submit.sh refers to one rule's job(s)."""

    def __init__(self, rule, tasks, jobid_refs):
        self.rule = rule
        self.tasks = tasks
        # Shell var ('$JOB_extract') for rules submitted this run, or
        # literal job ids for already-queued rules.
        self.jobid_refs = jobid_refs


class SlurmExecutor(Executor):
    handles_deferred = True
    supports_dry_run = True

    def __init__(self, rmk, dry_run=False):
        super().__init__(rmk)
        self.dry_run = dry_run
        self.remakefile = rmk.remakefile
        if self.remakefile is None:
            raise RemakeError(
                'SlurmExecutor needs the remakefile path to generate scripts: '
                'run via the remake CLI, or set rmk.remakefile'
            )
        config = {**DEFAULT_SLURM_CONFIG, **rmk.config.get('slurm', {})}
        # Obsolete since arrays-everywhere; popped so it doesn't leak into
        # #SBATCH opts for configs that still set it.
        config.pop('array_threshold', None)
        self.slurm_config = config
        self.jobs_dir = JOBS_DIR
        self.slurm_dir = Path('.remake/slurm')
        self.output_dir = self.slurm_dir / 'output'
        self.submit_path = Path('.remake/submit.sh')

    # --- generation ---

    def run_tasks(self, tasks, deferred_rules=()):
        prune_spec_files()
        rule_tasks = {}  # rule -> [task], in plan (topological) order
        for task in tasks:
            rule_tasks.setdefault(task.rule, []).append(task)

        try:
            active_jobids = self._active_jobids()
        except SqueueError as e:
            # Unknown queue state. With previous submissions on record for
            # the planned rules we cannot tell whether they are still in
            # flight, and submitting blind risks duplicate arrays racing on
            # the same outputs — refuse. On a fresh dir (no sidecars) nothing
            # we submitted can be queued, so proceed as if the queue is empty
            # (keeps dry runs working off-cluster).
            recorded = [
                rule.name for rule in rule_tasks
                if (self.jobs_dir / f'{rule.name}.jobids.json').exists()
            ]
            if recorded:
                raise RemakeError(
                    f'{e}. Previous submissions are recorded for '
                    f'{", ".join(recorded)} and may still be queued/running: '
                    'refusing to submit possible duplicates. Retry when '
                    'squeue works; if you are sure nothing is queued, delete '
                    '.remake/jobs/<rule>.jobids.json and re-run.'
                ) from e
            logger.warning(f'{e}; no previous submissions recorded, proceeding')
            active_jobids = set()
        # One run_seq for this whole submission, allocated here on the submit
        # node: it versions the immutable job-spec files and is stamped into
        # each spec so downstream propagation survives the submit→compute
        # boundary.
        run_seq = self.rmk.metadata.current_run_seq()
        submitted = {}  # rule -> _SubmittedRule
        lines = ['#!/bin/bash', '# Generated by remake — re-run to resubmit without replanning.',
                 'set -e', '']
        nsubmit = 0
        for rule, tasks_for_rule in rule_tasks.items():
            queued_ids = self._queued_jobids(rule, active_jobids)
            if queued_ids:
                logger.info(f'{rule.name}: already queued (job {",".join(queued_ids)}), skipping')
                # Downstream rules depend on the queued jobs by literal id.
                submitted[rule] = _SubmittedRule(rule, None, queued_ids)
                continue
            self._write_job_specs(rule, tasks_for_rule, run_seq)
            logger.info(f'{rule.name}: submitting {len(tasks_for_rule)} task(s)')
            for task in tasks_for_rule:
                logger.trace('  {}: {} {}', rule.name, task.key, task.kwargs)
            self._write_sbatch(rule, tasks_for_rule, run_seq)
            dependency = self._dependency(rule, tasks_for_rule, submitted)
            lines.extend(self._submit_lines(rule, dependency, run_seq))
            lines.append('')
            nsubmit += len(tasks_for_rule)
            submitted[rule] = _SubmittedRule(rule, tasks_for_rule, [f'$JOB_{rule.name}'])

        if deferred_rules:
            names = ', '.join(rule.name for rule in deferred_rules)
            all_refs = [ref for sub in submitted.values() for ref in sub.jobid_refs]
            if all_refs:
                self._write_continuation()
                lines.append(f'# Continuation: replans and submits deferred rules ({names}).')
                lines.append(
                    f'sbatch --parsable --dependency=afterok:{":".join(all_refs)} '
                    f'{self.slurm_dir}/continuation.sbatch'
                )
                lines.append('')
            else:
                # Nothing submitted or queued to wait on: a continuation would
                # replan the exact same state and submit another continuation,
                # forever (review finding 8). Whatever the deferred matrices
                # are waiting for, this run cannot produce it.
                logger.warning(
                    f'Deferred rule(s) {names} are waiting on data no submitted '
                    'job will produce; not submitting a continuation. '
                    'Fix the missing inputs and re-run.'
                )

        self.submit_path.parent.mkdir(parents=True, exist_ok=True)
        self.submit_path.write_text('\n'.join(lines))
        self.submit_path.chmod(0o755)
        logger.info(f'Wrote {self.submit_path} ({nsubmit} task(s) in {len(submitted)} rule(s))')

        if self.dry_run:
            logger.info('Dry run: not submitting')
            return
        self.submit()

    def _write_job_specs(self, rule, tasks, run_seq):
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        specs = [
            {'task_key': task.key, 'rule': rule.name, 'kwargs': task.kwargs,
             'run_seq': run_seq}
            for task in tasks
        ]
        path = spec_path(rule.name, run_seq)
        if path.exists():
            # Queued arrays pin this exact file (--specs): rewriting it is
            # the index corruption per-submission specs exist to prevent.
            # Reaching this means run_seq was reused — run_tasks called
            # twice without a new invocation (metadata.begin_invocation).
            raise RemakeError(f'{path} already exists — job specs are write-once')
        path.write_text(json.dumps(specs, indent=1))

    def _write_sbatch(self, rule, tasks, run_seq):
        config = {**self.slurm_config, **rule.config.get('slurm', {})}
        config.pop('array_threshold', None)
        throttle = config.pop('array_throttle', None)
        output_dir = self.output_dir / rule.name
        output_dir.mkdir(parents=True, exist_ok=True)
        script = ARRAY_SBATCH_TPL.format(
            rule_name=rule.name,
            max_index=len(tasks) - 1,
            array_throttle=f'%{throttle}' if throttle else '',
            output_dir=output_dir,
            opts=_sbatch_opts(config),
            # The remakefile is a user-typed path (spaces in home/group dirs
            # word-split on the compute node); other interpolations are
            # rule-name-derived .remake/ paths, safe unquoted.
            remakefile=shlex.quote(str(self.remakefile)),
            specs=shlex.quote(str(spec_path(rule.name, run_seq))),
        )
        self.slurm_dir.mkdir(parents=True, exist_ok=True)
        (self.slurm_dir / f'{rule.name}.sbatch').write_text(script)

    def _dependency(self, rule, tasks, submitted):
        """--dependency=... for this rule, or '' if no upstream jobs."""
        parts = []
        for dep in rule.depends_on:
            sub = submitted.get(dep)
            if sub is None:
                continue  # upstream rule has no jobs this run (complete)
            # aftercorr only when provably element-wise (see _elementwise);
            # otherwise — including rules queued from a previous submission
            # (sub.tasks is None), whose element order is unknowable here —
            # wait for the whole upstream job.
            if sub.tasks is not None and _elementwise(sub.tasks, tasks):
                parts.append(f'aftercorr:{":".join(sub.jobid_refs)}')
            else:
                parts.append(f'afterok:{":".join(sub.jobid_refs)}')
        return f'--dependency={",".join(parts)} ' if parts else ''

    def _submit_lines(self, rule, dependency, run_seq):
        sbatch_path = self.slurm_dir / f'{rule.name}.sbatch'
        sidecar = self.jobs_dir / f'{rule.name}.jobids.json'
        var = f'JOB_{rule.name}'
        return [
            f'{var}=$(sbatch --parsable {dependency}{sbatch_path})',
            f'echo "{{\\"slurm_array_job_id\\": \\"${var}\\", '
            f'\\"run_seq\\": {run_seq}}}" > {sidecar}',
        ]

    def _write_continuation(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Replanning only, regardless of what per-rule mem/time would say.
        config = dict(self.slurm_config)
        config.pop('array_throttle', None)
        config['time'] = '00:10:00'
        config['mem'] = '1G'
        script = CONTINUATION_SBATCH_TPL.format(
            output_dir=self.output_dir,
            opts=_sbatch_opts(config),
            remakefile=shlex.quote(str(self.remakefile)),
        )
        (self.slurm_dir / 'continuation.sbatch').write_text(script)

    # --- submission / already-queued detection ---

    def submit(self):
        logger.info(f'Executing {self.submit_path}')
        result = sp.run(['bash', str(self.submit_path)], capture_output=True, text=True)
        if result.stdout.strip():
            logger.info(result.stdout.strip())
        if result.returncode != 0:
            logger.error(result.stderr.strip())
            raise RemakeError(f'{self.submit_path} failed (exit {result.returncode})')

    def _active_jobids(self):
        return active_jobids()

    def _queued_jobids(self, rule, active_jobids):
        """This rule's previously-submitted job ids that are still active."""
        return [jobid for jobid in recorded_jobids(rule.name) if jobid in active_jobids]
