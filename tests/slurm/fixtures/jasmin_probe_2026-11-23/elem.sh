#!/bin/bash
#SBATCH --job-name=probe_a
#SBATCH --array=0-5
#SBATCH -o out/%A_%a.out
#SBATCH -e out/%A_%a.err
#SBATCH --time=00:01:00
#SBATCH --mem=500M
#SBATCH --kill-on-invalid-dep=yes
echo "start $SLURM_ARRAY_JOB_ID $SLURM_ARRAY_TASK_ID $(hostname)"
case $SLURM_ARRAY_TASK_ID in
  0) exit 0 ;;
  1) exit 3 ;;
  2) sleep 300 ;;                       # TIMEOUT
  3) sleep 300 ;;                       # scancel while running
  4) python3 -c "a = bytearray(2*1024**3); import time; time.sleep(30)" ;;  # OOM
  5) sleep 20; exit 0 ;;
esac
