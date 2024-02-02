import slurm
import threading
from mpi4py import MPI
from enum import Enum
import json
import os


class DaemonType(Enum):
    CPU = "cpu"
    IDLE = "idle"


# what type of daemon is this node?
def get_daemon_node_type() -> DaemonType:
    noderank: int = MPI.COMM_WORLD.Get_rank()
    daemon_list: list[DaemonType] = json.loads(
        os.environ["ENSEMBLES_BACKGROUND_PROCESS_LIST"]
    )
    return daemon_list[(noderank - 1) % len(daemon_list)]


# run an appropriate calculation for the selected daemon
def run_calculation_step():
    pass


def start() -> None:
    print(f"starting background daemon on node {MPI.COMM_WORLD.Get_rank()}")
    # wait for done signal
    wait_for_done = threading.Thread(target=slurm.work_done)
    wait_for_done.start()
    while wait_for_done.is_alive():
        run_calculation_step()
    print(f"done with running background daemon on node {MPI.COMM_WORLD.Get_rank()}")
