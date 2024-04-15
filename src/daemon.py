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
    MPI_SEND = "mpi_send"
    MPI_RECEIVE = "mpi_receive"
    READ = "read"
    WRITE = "write"


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

    # do it again if it's done
    while not done:
        # start a process with find
        find_process = subprocess.run(
            [
                os.environ["ENSEMBLES_FIND_PATH"],
                os.environ["ENSEMBLES_FIND_SEARCH_PATH"],
            ],
            check=False,
            text=False,
        )
        # wait for process to be done
        _ = find_process.stdout


# reads from disk
def read() -> None:
    global done

    filepath: str = os.environ["ENSEMBLES_READ_PATH"]

    with open(filepath, "w") as file:
        # write 1 mb
        random_bytes = random.randbytes(100000000)
        file.write(str(random_bytes))

    # load and read the file
    while not done:
        with open(filepath) as file:
            _ = file.read()


def write() -> None:
    global done
    filepath: str = os.environ["ENSEMBLES_WRITE_PATH"]

    while not done:
        with open(filepath, "w") as file:
            random_bytes = random.randbytes(100000000)
            file.write(str(random_bytes))


# idle daemon
def idle() -> None:
    global done
    while not done:
        sleep(1)


def get_receiver_node_id() -> int:
    daemon_list: list[str] = json.loads(os.environ["ENSEMBLES_BACKGROUND_PROCESS_LIST"])
    for index, item in enumerate(daemon_list):
        if item == "mpi_receive":
            return index + 1
    raise Exception(
        "No receiver node found! One is needed for the network send daemon."
    )


def network_send() -> None:
    # find rank of the receive node
    receiver_id = get_receiver_node_id()
    print(f"receiver id: {receiver_id}")
    # generate random data
    print("generating random data")
    bytes = b""
    bytes = bytes + random.randbytes(100000000)
    print("done generating")

    req = MPI.COMM_WORLD.isend("bytes", dest=0, tag=3493943948)
    req.wait()
    print("data sent")

    global done
    while not done:
        # req = MPI.COMM_WORLD.isend(data, dest=receiver_id, tag=424242)
        # req.wait()
        sleep(5)
        print("sending")


def network_receive() -> None:
    # it's ok to stop listening when the daemon exits
    global done
    while not done:
        # req = MPI.COMM_WORLD.irecv(tag=424242)
        # req.wait()
        sleep(5)
        print(f"received some data")


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
        case DaemonType.READ:
            return read
        case DaemonType.WRITE:
            return write
        case DaemonType.MPI_SEND:
            return network_receive
        case DaemonType.MPI_RECEIVE:
            return network_send


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
