from time import sleep
from log import Log
import cpu_load
import schedule
import threading
import slurm
import numio
from mpi4py import MPI


def main():
    # start logging data about the current node
    log = Log()
    log.schedule()

    # check if this is the main job or a background daemon
    if MPI.COMM_WORLD.Get_rank() == 0:
        print("found main node")
        # this is the main job
        numio_run = threading.Thread(target=numio.start)
        numio_run.start()
        while numio_run.is_alive():
            schedule.run_pending()
            sleep(1)
        print("main node finished, sending done signals...")
        numio.send_done_signals()
    else:
        wait_for_done = threading.Thread(target=slurm.work_done)
        wait_for_done.start()
        background_noise = threading.Thread(target=cpu_load.start)
        background_noise.start()
        while wait_for_done.is_alive():
            schedule.run_pending()
            sleep(1)
    
    log.write_histogram()


if __name__ == "__main__":
    main()
