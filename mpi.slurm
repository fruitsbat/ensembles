#!/bin/bash

#SBATCH --time=120
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=3
#SBATCH --partition=west
#SBATCH --output=job.out
#SBATCH --error=job.err

export ENSEMBLES_MPIEXEC_PATH=mpiexec
export ENSEMBLES_NUMIO_PATH=/home/oosting/IO-partdiff/numio-mpiasync
export ENSEMBLES_FIND_PATH=find
export ENSEMBLES_FIND_SEARCH_PATH="/"
export ENSEMBLES_ITERATIONS=6000 # count of numio iterations
export ENSEMBLES_MATRIX_SIZE=500 # how large the numio matrix should be
export ENSEMBLES_PERT=2 # 1 if no perturbation function, 2 if one should be used
export ENSEMBLES_RW_FREQUENCY=64 # which frequency to read and write with (read is frequency + 1 since numio will break otherwise)
export ENSEMBLES_RW_PATH=matrix.out # what path to write the matrix to
export ENSEMBLES_FAKE_COM_SIZE=200 # how much data to send during fake communication
export ENSEMBLES_FAKE_COM_FREQ=64 # how often to write data using fake communication
export ENSEMBLES_IDLE_ONLY=False # just sleep instead of running numio
export ENSEMBLES_IDLE_ONLY_TIME=12 # when sleeping instead of running numio wait for x seconds
export ENSEMBLES_GRAPH_OUT_PATH=plots # where to store the generated graphs (make sure the folder exists)
export ENSEBLES_DATA_OUT_PATH=data # where to store the generated log data (make sure the folder exists)
export ENSEMBLES_GRAPH_FILETYPE=pdf # supported file extension for matplotlib
export ENSEMBLES_BACKGROUND_PROCESS_LIST='["find", "idle"]' # json list of background processes to run
export ENSEMBLES_LOG_EVERY_X_SECONDS=3

mpiexec python src/main.py # run the script