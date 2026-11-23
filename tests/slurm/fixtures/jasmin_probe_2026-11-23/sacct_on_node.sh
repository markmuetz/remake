#!/bin/bash
#SBATCH --job-name=probe_sacct
#SBATCH -o out/sacct_on_node.out
#SBATCH --time=00:02:00
#SBATCH --mem=200M
which sacct; SLURM_TIME_FORMAT=%s sacct -X -n -P -j $1 --format=JobID,State,Start,End; echo "rc=$?"
