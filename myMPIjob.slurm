#!/bin/bash
#SBATCH --job-name=JSTOR_ANALYSIS
#SBATCH --ntasks=20
#SBATCH --mem-per-cpu=1G
#SBATCH --time=00:30:00
#SBATCH --signal=INT@100
#SBATCH --mail-user=jcm10@rice.edu
#SBATCH --mail-type=ALL
module load iccifort
module load iccifort OpenMPI
module load Python/2.7.9

srun -n 20 python shakespeare_everything_blocking.py
