import slurm
import threading
from mpi4py import MPI
from enum import Enum
import json
import os
from time import sleep
import typing
from datetime import datetime
import psutil


# runs a function on all available cores using threading
def all_cores(f: typing.Callable[..., None]) -> None:
    cpu_count: int = slurm.allocated_cpu_count()
    print(f"running {f} on {cpu_count}/{psutil.cpu_count()} cores")
    for _ in range(0, cpu_count):
        runner = threading.Thread(target=f)
        runner.start()


class DaemonType(Enum):
    CPU = "cpu"
    IDLE = "idle"


class Daemon:
    daemon: DaemonType
    done: bool

    def __init__(self) -> None:
        self.daemon = get_daemon_node_type()
        self.done = False
        print(f"got {self.daemon} on node {MPI.COMM_WORLD.Get_rank()}")

    def cpu_daemon(self):
        while not self.done:
            print("cpu attack")
            _ = 64 / 128

    def idle_daemon(self):
        while not self.done:
            print("awawawa")
            sleep(1)

    # run appropritate function for the current daemon
    def run_function(self) -> None:
        match self.daemon:
            case DaemonType.CPU:
                all_cores(self.cpu_daemon)
            case DaemonType.IDLE:
                self.idle_daemon()

    def run(self) -> None:
        print(f"{self.daemon} on node {MPI.COMM_WORLD} started running")
        thread = threading.Thread(target=self.run_function)
        thread.start()
        # wait for the main node to be done
        slurm.work_done()
        # then exit
        self.done = True
        print("daemon finishing run")


# what type of daemon is this node?
def get_daemon_node_type() -> DaemonType:
    noderank: int = MPI.COMM_WORLD.Get_rank()
    daemon_list: list[DaemonType] = json.loads(
        os.environ["ENSEMBLES_BACKGROUND_PROCESS_LIST"]
    )
    return daemon_list[(noderank - 1) % len(daemon_list)]


def start() -> None:
    print(
        f"starting background daemon on node {MPI.COMM_WORLD.Get_rank()} at {datetime.now()}"
    )
    daemon = Daemon()
    daemon.run()
    print(f"done with running background daemon on node {MPI.COMM_WORLD.Get_rank()}")
