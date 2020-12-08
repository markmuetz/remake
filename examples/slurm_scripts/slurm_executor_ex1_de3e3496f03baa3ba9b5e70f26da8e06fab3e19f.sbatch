#!/bin/bash
#SBATCH --job-name=/home/mark
#SBATCH -p short-serial
#SBATCH -o slurm_output/slurm_executor_ex1_rescan_%j.out
#SBATCH -e slurm_output/slurm_executor_ex1_rescan_%j.err
#SBATCH --time=04:00:00
#SBATCH --mem=16000


python /home/markmuetz/projects/remake/remake/executor/slurm_executor.py /home/markmuetz/projects/remake/examples/ex1.py 7ac8e3c9d423a8c77022c87e0294a13774289441 /home/markmuetz/projects/remake/examples/data/inputs/in1.txt
