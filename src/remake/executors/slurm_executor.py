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
                                     indices a queued array reads.
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
import subprocess as sp
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
ARRAY_THRESHOLD = 10

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

# Individual jobs share one script per rule; the task index is passed as a
# script argument by submit.sh, and -o/-e are set per job on the sbatch line.
INDIVIDUAL_SBATCH_TPL = """#!/bin/bash
#SBATCH --job-name={rule_name}
#SBATCH --kill-on-invalid-dep=yes
{opts}
echo "SLURM RUNNING {rule_name} $1"
remake run-array-task {remakefile} {rule_name} $1 --specs {specs}
rc=$?
echo "SLURM COMPLETED {rule_name} $1 (rc=$rc)"
exit $rc
"""

CONTINUATION_SBATCH_TPL = """#!/bin/bash
#SBATCH --job-name=remake_continue
#SBATCH -o {output_dir}/continuation.out
#SBATCH -e {output_dir}/continuation.err
{opts}
remake run {remakefile} --executor slurm
"""


def _sbatch_opts(config):
    return '\n'.join(f'#SBATCH --{k}={v}' for k, v in config.items() if v)


def squeue_snapshot():
    """{base_jobid: [(element_id, state, reason)]} for this user's queued/
    running jobs, one squeue call. Empty when squeue is unavailable."""
    try:
        result = sp.run(
            ['squeue', '-h', '-r', '-u', getpass.getuser(), '-o', '%i %t %r'],
            capture_output=True, text=True, check=True,
        )
    except (FileNotFoundError, sp.CalledProcessError) as e:
        logger.debug(f'squeue unavailable ({e!r})')
        return {}
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
    sidecar = JOBS_DIR / f'{rule_name}.jobids.json'
    if sidecar.exists():
        recorded = json.loads(sidecar.read_text())
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


def last_submission(rule_name, task_key=None):
    """(jobids, array_index) from the last submission's sidecar/job spec,
    (None, None) if the rule was never submitted. `array_index` is the task's
    position in the job spec of the submission the sidecar records (pinned by
    its run_seq, so later replans don't skew it); None unless task_key is
    given."""
    sidecar = JOBS_DIR / f'{rule_name}.jobids.json'
    if not sidecar.exists():
        return None, None
    recorded = json.loads(sidecar.read_text())
    jobids = recorded.get('slurm_job_ids', [])
    if 'slurm_array_job_id' in recorded:
        jobids = [recorded['slurm_array_job_id']]
    index = None
    specs_path = spec_path(rule_name, recorded.get('run_seq'))
    if task_key is not None and specs_path.exists():
        specs = json.loads(specs_path.read_text())
        index = next((i for i, s in enumerate(specs) if s['task_key'] == task_key), None)
    return jobids, index


class _SubmittedRule:
    """How submit.sh refers to one rule's job(s)."""

    def __init__(self, rule, tasks, is_array, jobid_refs):
        self.rule = rule
        self.tasks = tasks
        self.is_array = is_array
        # Shell vars ('$JOB_extract') for rules submitted this run, or
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
        self.array_threshold = int(config.pop('array_threshold', ARRAY_THRESHOLD))
        self.slurm_config = config
        self.jobs_dir = JOBS_DIR
        self.slurm_dir = Path('.remake/slurm')
        self.output_dir = self.slurm_dir / 'output'
        self.submit_path = Path('.remake/submit.sh')

    # --- generation ---

    def run_tasks(self, tasks, deferred_rules=()):
        rule_tasks = {}  # rule -> [task], in plan (topological) order
        for task in tasks:
            rule_tasks.setdefault(task.rule, []).append(task)

        active_jobids = self._active_jobids()
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
                submitted[rule] = _SubmittedRule(rule, None, False, queued_ids)
                continue
            self._write_job_specs(rule, tasks_for_rule, run_seq)
            is_array = len(tasks_for_rule) >= self.array_threshold
            kind = 'array' if is_array else 'individual'
            logger.info(f'{rule.name}: submitting {len(tasks_for_rule)} task(s) ({kind})')
            for task in tasks_for_rule:
                logger.trace('  {}: {} {}', rule.name, task.key, task.kwargs)
            self._write_sbatch(rule, tasks_for_rule, is_array, run_seq)
            dependency = self._dependency(rule, tasks_for_rule, is_array, submitted)
            lines.extend(self._submit_lines(rule, tasks_for_rule, is_array, dependency, run_seq))
            lines.append('')
            nsubmit += len(tasks_for_rule)
            refs = (
                [f'$JOB_{rule.name}'] if is_array
                else [f'$JOB_{rule.name}_{i}' for i in range(len(tasks_for_rule))]
            )
            submitted[rule] = _SubmittedRule(rule, tasks_for_rule, is_array, refs)

        if deferred_rules:
            names = ', '.join(rule.name for rule in deferred_rules)
            self._write_continuation()
            all_refs = [ref for sub in submitted.values() for ref in sub.jobid_refs]
            lines.append(f'# Continuation: replans and submits deferred rules ({names}).')
            dep = f'--dependency=afterok:{":".join(all_refs)} ' if all_refs else ''
            lines.append(
                f'sbatch --parsable {dep}{self.slurm_dir}/continuation.sbatch'
            )
            lines.append('')

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

    def _write_sbatch(self, rule, tasks, is_array, run_seq):
        config = {**self.slurm_config, **rule.config.get('slurm', {})}
        config.pop('array_threshold', None)
        throttle = config.pop('array_throttle', None)
        output_dir = self.output_dir / rule.name
        output_dir.mkdir(parents=True, exist_ok=True)
        tpl = ARRAY_SBATCH_TPL if is_array else INDIVIDUAL_SBATCH_TPL
        script = tpl.format(
            rule_name=rule.name,
            max_index=len(tasks) - 1,
            array_throttle=f'%{throttle}' if throttle else '',
            output_dir=output_dir,
            opts=_sbatch_opts(config),
            remakefile=self.remakefile,
            specs=spec_path(rule.name, run_seq),
        )
        self.slurm_dir.mkdir(parents=True, exist_ok=True)
        (self.slurm_dir / f'{rule.name}.sbatch').write_text(script)

    def _dependency(self, rule, tasks, is_array, submitted):
        """--dependency=... for this rule, or '' if no upstream jobs."""
        parts = []
        for dep in rule.depends_on:
            sub = submitted.get(dep)
            if sub is None:
                continue  # upstream rule has no jobs this run (complete)
            # aftercorr (element N waits on element N) is only valid when
            # both are arrays whose task lists correspond element-wise.
            if (
                is_array
                and sub.is_array
                and sub.tasks is not None
                and [t.kwargs for t in sub.tasks] == [t.kwargs for t in tasks]
            ):
                parts.append(f'aftercorr:{":".join(sub.jobid_refs)}')
            else:
                parts.append(f'afterok:{":".join(sub.jobid_refs)}')
        return f'--dependency={",".join(parts)} ' if parts else ''

    def _submit_lines(self, rule, tasks, is_array, dependency, run_seq):
        sbatch_path = self.slurm_dir / f'{rule.name}.sbatch'
        sidecar = self.jobs_dir / f'{rule.name}.jobids.json'
        if is_array:
            var = f'JOB_{rule.name}'
            return [
                f'{var}=$(sbatch --parsable {dependency}{sbatch_path})',
                f'echo "{{\\"slurm_array_job_id\\": \\"${var}\\", '
                f'\\"run_seq\\": {run_seq}}}" > {sidecar}',
            ]
        lines = []
        output_dir = self.output_dir / rule.name
        for i in range(len(tasks)):
            var = f'JOB_{rule.name}_{i}'
            lines.append(
                f'{var}=$(sbatch --parsable {dependency}'
                f'-o {output_dir}/{i}.out -e {output_dir}/{i}.err '
                f'{sbatch_path} {i})'
            )
        ids = ', '.join(f'\\"$JOB_{rule.name}_{i}\\"' for i in range(len(tasks)))
        lines.append(
            f'echo "{{\\"slurm_job_ids\\": [{ids}], '
            f'\\"run_seq\\": {run_seq}}}" > {sidecar}'
        )
        return lines

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
            remakefile=self.remakefile,
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
        """Base job ids of this user's pending/running jobs ('1234_5' array
        elements map to base id '1234')."""
        return {
            base
            for base, elems in squeue_snapshot().items()
            if any(state in ('PD', 'R', 'CF') for _, state, _ in elems)
        }

    def _queued_jobids(self, rule, active_jobids):
        """This rule's previously-submitted job ids that are still active."""
        sidecar = self.jobs_dir / f'{rule.name}.jobids.json'
        if not sidecar.exists():
            return []
        recorded = json.loads(sidecar.read_text())
        ids = recorded.get('slurm_job_ids', [])
        if 'slurm_array_job_id' in recorded:
            ids = [recorded['slurm_array_job_id']]
        return [jobid for jobid in ids if jobid in active_jobids]
