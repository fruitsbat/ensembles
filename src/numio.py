import datetime
import os
from time import sleep
from mpi4py import MPI
import subprocess

def send_done_signals():
    comm = MPI.COMM_WORLD
    world_size = comm.Get_size()

    # tell all the background daemons to stop
    for node in range(1, world_size - 1):
        print(f"sending stop signal to node: ${node}")
        comm.send(obj="done", dest=node, tag=200)
        


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
    print("numio run!")
    if bool(os.environ["ENSEMBLES_IDLE_ONLY"]):
        print("idling instead of running numio")
        sleep(int(os.environ["ENSEMBLES_IDLE_ONLY_TIME"]))
        return
    
    output = subprocess.run(
        [
            mpiexec_path(),
            numio_path(),
            "-m",
            "iter=1,size=9,pert=1",
        ],
        capture_output=True,
        text=True,
    )
    print(f"numio run finished, output is: ${output.stdout}")
