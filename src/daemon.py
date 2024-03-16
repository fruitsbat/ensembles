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

done: bool = False


# runs a function on all available cores using threading
def all_cores(f: typing.Callable[..., None]) -> None:
    cpu_count: int = slurm.allocated_cpu_count()
    print(f"running {f} on {cpu_count}/{psutil.cpu_count()} cores")
    for n in range(0, cpu_count):
        runner = threading.Thread(target=f)
        runner.start()
        print(f"started on core {n}")


class DaemonType(Enum):
    CPU = "cpu"
    IDLE = "idle"


# partial function for the cpu daemon
def cpu_step() -> None:
    global done
    while not done:
        _ = 18783.83983 / 89374837.73


# cpu daemon
def cpu() -> None:
    all_cores(cpu_step)


# idle daemon
def idle() -> None:
    global done
    while not done:
        print("zzzzzzz")
        sleep(1)


# what type of daemon is this node?
def get_daemon_node_type() -> DaemonType:
    noderank: int = MPI.COMM_WORLD.Get_rank()
    daemon_list: list[str] = json.loads(
        os.environ["ENSEMBLES_BACKGROUND_PROCESS_LIST"]
    )
    s = daemon_list[(noderank - 1) % len(daemon_list)]
    if s == "cpu": return DaemonType.CPU
    else: return DaemonType.IDLE


# what function to run for the selected daemon
def function_for_daemon(daemon_type: DaemonType) -> typing.Callable[..., None]:
    print(f"selecting function for {daemon_type.value}")
    match daemon_type:
        case DaemonType.CPU:
            print("selected cpu")
            return cpu
        case DaemonType.IDLE:
            print("selected idle")
            return idle


# run a function for the daemon type
def run(daemon_type: DaemonType) -> None:
    # start daemon thread
    print("starting daemon threads")
    f: typing.Callable[..., None] = function_for_daemon(daemon_type)
    print(f)
    thread = threading.Thread(target=f)
    thread.start()
    # block until thread is done
    slurm.work_done()
    print("stopping daemon threads")
    global done
    done = True


def start() -> None:
    print(
        f"starting background daemon on node {MPI.COMM_WORLD.Get_rank()} at {datetime.now()}"
    )
    daemon_type: DaemonType = get_daemon_node_type()
    run(daemon_type)
    print(f"done with running background daemon on node {MPI.COMM_WORLD.Get_rank()}")
