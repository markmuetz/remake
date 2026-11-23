# Live sacct/squeue probe, JASMIN, 2026-11-23

Plain sbatch jobs (no remake), run by `drive.sh` to see what `squeue -r`
and `sacct` report for each outcome (design_docs/designs/slurm_reconcile/,
plan step 0). The uid is replaced by 12345 and the account by ACCOUNT.

Jobs (ids.txt): A = `elem.sh`, array 0-5: 0 exits 0; 1 exits 3; 2 times out
(1 min); 3 is scancelled while running; 4 runs out of memory (500M); 5
sleeps 20 s. B = `dep.sh` array 0-5 `aftercorr:A`; C = `dep.sh` array 0-3
`afterok:A`; D = `dep.sh` array 0-3 `--begin=now+1h`, D_1 scancelled while
pending, then all of D; E = `slow.sh` array 0-1 (a requeue attempt: refused,
"Requested operation is presently disabled"). N = `sacct_on_node.sh`: sacct
from a compute node. Snapshots at t0, t15, t60, t150, t240 s and final.

Findings:

- Never-started elements often have **no sacct row of their own**: the
  aftercorr dependents of A's failures (B_1-4) and the individually
  cancelled pending D_1 vanish from squeue and sacct.
- While an aftercorr array waits, sacct shows its unsplit pending elements
  as **one row labelled with a single index, no brackets** (`6178694_5`
  PENDING stood for elements 0, 2, 3, 5 at t15). Other arrays show a
  bracketed range (`6178695_[0-3]`, `6178696_[0,2-3]`).
- A whole pending array that is cancelled keeps its collapsed row:
  `CANCELLED`, Start `None`, NodeList `None assigned`.
- `CANCELLED by <uid>` for user cancels; Start `Unknown` while pending,
  `None` once cancelled before starting; End `Unknown` while running.
- ExitCode: FAILED `3:0`, OUT_OF_MEMORY `0:125`, TIMEOUT `0:0`.
- `SLURM_TIME_FORMAT=%s` gives epoch seconds for Start/End.
- sacct works on a compute node (`/usr/bin/sacct`).
- Finished elements leave `squeue -r` within seconds (no MinJobAge lag
  seen); `%A_%a.out` files exist only for elements that started.
