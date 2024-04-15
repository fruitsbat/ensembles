import datetime
import os
from time import sleep
from mpi4py import MPI
import subprocess


def send_done_signals():
    comm = MPI.COMM_WORLD
    world_size = comm.Get_size()

    # tell all the background daemons to stop
    for node in range(1, world_size):
        print(f"sending stop signal to node: {node}")
        req = comm.isend(obj="done", dest=node, tag=200)
        req.wait()
    print("sent all done signals")


def numio_write_args() -> str:
    args = f"freq={os.environ['ENSEMBLES_NUMIO_RW_FREQUENCY']},path={os.environ['ENSEMBLES_NUMIO_RW_PATH']}"
    if bool(os.environ["ENSEMBLES_NUMIO_W_IMMEDIATE"]):
        args = args + ",imm"
    elif bool(os.environ["ENSEMBLES_NUMIO_W_NOFILESYNC"]):
        args = args + ",nofilesync"
    return args


def numio_read_args() -> str:
    args = f"freq={int(os.environ['ENSEMBLES_NUMIO_RW_FREQUENCY']) + 1},path={os.environ['ENSEMBLES_NUMIO_RW_PATH']}"
    return args


def numio_collective_comms_args() -> str:
    args = f"freq={os.environ['ENSEMBLES_NUMIO_COLLECTIVE_COM_FREQ']},size={os.environ['ENSEMBLES_NUMIO_COLLECTIVE_COM_FREQ']}"
    return args


def numio_args() -> list[str]:
    args = [mpiexec_path(), "-n", f"{os.environ['ENSEMBLES_MPIEXEC_NODES']}"]
    args.append(numio_path())

    if bool(os.environ["ENSEMBLES_NUMIO_WRITE"]):
        args.append("--write")
        args.append(numio_write_args())

    if bool(os.environ["ENSEMBLES_NUMIO_READ"]):
        args.append("--read")
        args.append(numio_read_args())

    if bool(os.environ["ENSEMBLES_NUMIO_COLLECTIVE_COMMS"]):
        args.append("--collective-comms")
        args.append(numio_collective_comms_args())

    if bool(os.environ["ENSEMBLES_NUMIO_FPISIN"]):
        args.append("--func-fpisin")

    args.append(os.environ["ENSEMBLES_LINES"])
    args.append(os.environ["ENSEMBLES_ITERATIONS"])

    return args


# this is the main job that controls the numio benchmark
def start() -> None:
    start_time = datetime.datetime.now()
    print(f"starting numio at ${start_time}")
    run_numio()
    end_time = datetime.datetime.now()
    print(f"numio run finished at: {end_time}")


def mpiexec_path() -> str:
    return os.environ["ENSEMBLES_MPIEXEC_PATH"]


def numio_path() -> str:
    return os.environ["ENSEMBLES_NUMIO_PATH"]


def run_numio() -> None:
    if os.environ["ENSEMBLES_IDLE_ONLY"] == "True":
        print("idling instead of running numio")
        sleep(int(os.environ["ENSEMBLES_IDLE_ONLY_TIME"]))
        return
    print(numio_args())
    output = subprocess.run(
        numio_args(),
        capture_output=True,
        text=True,
    )
    print(f"numio run finished, output is: ${output.stdout}")
