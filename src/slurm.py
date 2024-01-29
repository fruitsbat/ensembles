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
    print(f"listening for done signal on {MPI.COMM_WORLD.Get_rank()}")
    comm = MPI.COMM_WORLD
    data = comm.recv(source=0, tag=200)
    print(f"received {data} on node {MPI.COMM_WORLD.Get_rank()} ending process...")
