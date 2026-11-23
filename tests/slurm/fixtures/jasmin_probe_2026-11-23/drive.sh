#!/bin/bash
# Live sacct/squeue probe for remake's SLURM reconciliation (step 0).
set -u
cd "$(dirname "$0")"; mkdir -p out res
O="--account=ACCOUNT --partition=standard --qos=short"
F=JobID,JobName,State,ExitCode,Start,End,Elapsed,NodeList
snap() {  # $1 = label
  squeue -r -u $USER -h -o '%i %T %r' > res/squeue_$1.txt 2>&1
  for ids in "$ALL"; do
    sacct -X -n -P -j $ids --format=$F > res/sacct_$1.psv 2>&1
    SLURM_TIME_FORMAT=%s sacct -X -n -P -j $ids --format=$F > res/sacct_epoch_$1.psv 2>&1; echo "rc=$?" >> res/sacct_epoch_$1.psv
    sacct -n -P -j $ids --format=$F > res/sacct_steps_$1.psv 2>&1
  done
}
A=$(sbatch --parsable $O elem.sh)
B=$(sbatch --parsable $O --array=0-5 --dependency=aftercorr:$A dep.sh)
C=$(sbatch --parsable $O --array=0-3 --dependency=afterok:$A dep.sh)
D=$(sbatch --parsable $O --array=0-3 --begin=now+3600 dep.sh)
E=$(sbatch --parsable $O --array=0-1 slow.sh)
ALL=$A,$B,$C,$D,$E
echo "A=$A B=$B C=$C D=$D E=$E" > res/ids.txt
date -u +%s > res/t_submit.txt
snap t0
sleep 15; snap t15
scancel ${A}_3; scancel ${D}_1          # running cancel; pending cancel
for i in $(seq 1 12); do
  st=$(squeue -h -r -j ${E}_0 -o %T 2>/dev/null)
  [ "$st" = RUNNING ] && { scontrol requeue ${E}_0 > res/requeue.txt 2>&1; echo "requeued at $(date -u +%s) rc=$?" >> res/requeue.txt; break; }
  sleep 10
done
sleep 20; snap t60
sleep 70; snap t150
scancel $D                               # whole pending array
N=$(sbatch --parsable $O sacct_on_node.sh $A)
echo "N=$N" >> res/ids.txt
sleep 60; snap t240
for i in $(seq 1 30); do
  squeue -h -u $USER -j $ALL,$N 2>/dev/null | grep -q . || break
  sleep 20
done
sleep 30; snap final
date -u +%s > res/t_done.txt
echo done > res/DONE
