#!/bin/bash
#SBATCH --job-name=probe_slow
#SBATCH -o out/%A_%a.out
#SBATCH -e out/%A_%a.err
#SBATCH --time=00:03:00
#SBATCH --mem=100M
echo "slow start $SLURM_ARRAY_JOB_ID $SLURM_ARRAY_TASK_ID $(hostname)"
sleep 60
