#!/bin/bash
	
#SBATCH --time=2
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=3
#SBATCH --ntasks=3
#SBATCH --partition=west
#SBATCH --output=job.out
#SBATCH --error=job.err
export ENSEMBLES_MPIEXEC_PATH=mpiexec
export ENSEMBLES_NUMIO_PATH=/home/oosting/IO-partdiff/numio-mpiasync
export ENSEMBLES_IDLE_ONLY=False
export ENSEMBLES_IDLE_ONLY_TIME=12
export ENSEMBLES_BACKGROUND_PROCESS_LIST='["cpu", "network"]'
mpiexec python src/main.py