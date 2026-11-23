#!/bin/bash
#SBATCH --job-name=probe_dep
#SBATCH -o out/%A_%a.out
#SBATCH -e out/%A_%a.err
#SBATCH --time=00:01:00
#SBATCH --mem=100M
#SBATCH --kill-on-invalid-dep=yes
echo "dep start $SLURM_ARRAY_JOB_ID $SLURM_ARRAY_TASK_ID"
