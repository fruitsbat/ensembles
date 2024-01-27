import os
import psutil
from mpi4py import MPI


def allocated_cpu_count() -> int:
    try:
        return int(os.environ["SLURM_CPUS_PER_TASK"])
    except:
        return psutil.cpu_count()


def slurm_localid() -> int:
    return int(os.environ["SLURM_LOCALID"])


def work_done() -> None:
    comm = MPI.COMM_WORLD
    data = comm.recv(source=0, tag=200)
    print(data)
