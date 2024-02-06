import slurm
import threading
from mpi4py import MPI
from enum import Enum
import json
import os
from time import sleep
import typing

done: bool = False

# runs a function on all available cores using threading
def all_cores(f: typing.Callable[..., None]) -> None:
    cpu_count: int = slurm.allocated_cpu_count()
    for _ in range(0, cpu_count):
        runner = threading.Thread(target=f)
        runner.start()

def idle_daemon() -> None:
    while not done:
        sleep(5)

# doing a bunch of calculations to stress the cpu
def cpu_daemon() -> None:
    while not done:
        _ = 4 / 64

class DaemonType(Enum):
    CPU = "cpu"
    IDLE = "idle"

    # run an appropriate calculation for the selected daemon
    # this function must listen to the done variable above
    # in order to stop when numio is terminated
    def run_function(self) -> None:
        match self:
            case DaemonType.CPU:
                all_cores(cpu_daemon)
            case DaemonType.IDLE:
                idle_daemon()



# what type of daemon is this node?
def get_daemon_node_type() -> DaemonType:
    noderank: int = MPI.COMM_WORLD.Get_rank()
    daemon_list: list[DaemonType] = json.loads(
        os.environ["ENSEMBLES_BACKGROUND_PROCESS_LIST"]
    )
    return daemon_list[(noderank - 1) % len(daemon_list)]



def start() -> None:
    print(f"starting background daemon on node {MPI.COMM_WORLD.Get_rank()}")
    daemon_task = threading.Thread(target=get_daemon_node_type().run_function)
    daemon_task.start()

    slurm.work_done()
    print(f"done with running background daemon on node {MPI.COMM_WORLD.Get_rank()}")
