import daemon
import slurm
import multiprocessing
import time


def start() -> None:
    pool = multiprocessing.Pool(processes=slurm.allocated_cpu_count())
    for _ in range(slurm.allocated_cpu_count()):
        pool.apply_async(calculations)
    while not daemon.STOPPED:
        time.sleep(1)
    pool.close()


def calculations() -> None:
    while not daemon.STOPPED:
        _ = 4 / 64
