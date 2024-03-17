# import random
import random
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
import multiprocessing
import subprocess

done: bool = False


# run a process on all cores
# this also kills processes once they are done
def all_cores(f: typing.Callable[..., None]) -> None:
    cpu_count: int = slurm.allocated_cpu_count()
    print(f"running {f} on {cpu_count}/{psutil.cpu_count()} cores")
    jobs = []
    for _ in range(0, cpu_count):
        process = multiprocessing.Process(target=f, daemon=True)
        jobs.append(process)
        process.start()

    global done
    while not done:
        sleep(1)

    print("finished running on all cores")


class DaemonType(Enum):
    CPU = "cpu"
    IDLE = "idle"
    RAM = "ram"
    FIND = "find"


# partial function for the cpu daemon
def cpu_step() -> None:
    while True:
        for _ in range(0, 100):
            _ = 4950495.304 / 938949384.32


# cpu daemon
def cpu() -> None:
    all_cores(cpu_step)


# simulates grepping for a file by walking the filesystem
def find() -> None:
    global done

    while not done:
        find_process = subprocess.run(
            [
                os.environ["ENSEMBLES_FIND_PATH"],
                os.environ["ENSEMBLES_FIND_SEARCH_PATH"],
            ],
            check=False,
            text=True,
        )
        find_result = find_process.stdout

        print(f"find found: {find_result}")


# idle daemon
def idle() -> None:
    global done
    while not done:
        sleep(1)


def ram() -> None:
    global done

    used_ram: int = psutil.virtual_memory().used
    total_ram: int = psutil.virtual_memory().total

    bytes = b""

    while not done:
        if used_ram / total_ram >= 0.95:
            random_bytes = random.randbytes(1000000)
            bytes = bytes + random_bytes
            used_ram = psutil.virtual_memory().used


# what type of daemon is this node?
def get_daemon_node_type() -> DaemonType:
    noderank: int = MPI.COMM_WORLD.Get_rank()
    daemon_list: list[str] = json.loads(os.environ["ENSEMBLES_BACKGROUND_PROCESS_LIST"])
    name = daemon_list[(noderank - 1) % len(daemon_list)]
    return DaemonType(name)


# what function to run for the selected daemon
def function_for_daemon(daemon_type: DaemonType) -> typing.Callable[..., None]:
    print(f"selecting function for {daemon_type.value}")
    match daemon_type:
        case DaemonType.CPU:
            return cpu
        case DaemonType.IDLE:
            return idle
        case DaemonType.RAM:
            return ram
        case DaemonType.FIND:
            return find


# run a function for the daemon type
def run(daemon_type: DaemonType) -> None:
    # start daemon thread
    print("starting daemon threads")
    f: typing.Callable[..., None] = function_for_daemon(daemon_type)
    print(f)
    thread = threading.Thread(target=f, daemon=True)
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
