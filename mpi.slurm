#!/bin/bash
	
#SBATCH --time=2
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=10
#SBATCH --ntasks=20
#SBATCH --partition=west
#SBATCH --output=job.out
#SBATCH --error=job.err
export ENSEMBLES_MPIEXEC_PATH=mpiexec
export ENSEMBLES_NUMIO_PATH=/home/oosting/IO-partdiff/numio-posix
export ENSEMBLES_IDLE_ONLY=False
export ENSEMBLES_IDLE_ONLY_TIME=12
mpiexec python src/main.py