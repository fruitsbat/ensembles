import os
import psutil
from mpi4py import MPI


# check how many cpus we have for threading
def allocated_cpu_count() -> int:
    try:
        return int(os.environ["SLURM_CPUS_PER_TASK"])
    except:
        return psutil.cpu_count()


def slurm_localid() -> int:
    return int(os.environ["SLURM_LOCALID"])


def work_done() -> None:
    print(f"listening for done signal on {MPI.COMM_WORLD.Get_rank()}")
    data = MPI.COMM_WORLD.irecv(source=0, tag=200)
    data.wait()
    print(f"received done signal on node {MPI.COMM_WORLD.Get_rank()} ending process...")
