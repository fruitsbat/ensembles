#!/bin/bash
	
#SBATCH --time=20
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=5
#SBATCH --ntasks=10
#SBATCH --partition=west
#SBATCH --output=job.out
#SBATCH --error=job.err
export ENSEMBLES_MPIEXEC_PATH=mpiexec
export ENSEMBLES_NUMIO_PATH=/home/oosting/IO-partdiff/numio-posix
mpiexec python src/main.py