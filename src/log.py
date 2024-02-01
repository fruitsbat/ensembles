import psutil
from time import time
import schedule
import slurm
import matplotlib.pyplot as plt
from datetime import datetime
import platform
from typing import NamedTuple
import math
from mpi4py import MPI


# takes a list of floats and shows the difference for each
# entry to the last one
# used for logging io
def to_differences(l: list[float]) -> list[float]:
    new_list: list[float] = []
    for index, item in enumerate(l):
        if index == 0:
            new_list.append(0)
        else:
            new_item: float = item - l[index - 1]
            new_list.append(new_item)
    return new_list


class LogDataLists(NamedTuple):
    cpu: list[float] = []
    disk: list[float] = []
    memory: list[float] = []
    disk_read: list[float] = []
    disk_write: list[float] = []
    timestamps: list[float] = []
    times: list[datetime] = []
    time_labels: list[str] = []
    net_up: list[float] = []
    net_down: list[float] = []


class LogEntry:
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_used: float
    fan_speed: float
    disk_write: float
    disk_read: float
    net_up: float
    net_down: float

    def __init__(self) -> None:
        self.timestamp = time()
        self.cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        self.memory_used = memory.used
        self.memory_percent = memory.percent
        diskio = psutil.disk_io_counters()
        netio = psutil.net_io_counters()
        try:
            self.disk_read = diskio.read_bytes
            self.disk_write = diskio.write_bytes
        except:
            self.disk_read = math.nan
            self.disk_write = math.nan
        try:
            self.net_down = netio.bytes_recv
            self.net_up = netio.bytes_sent
        except:
            self.net_up = math.nan
            self.net_down = math.nan


class Log:
    entries: list[LogEntry] = []
    beginning_timestamp: float

    def __init__(self) -> None:
        self.beginning_timestamp = time()
        self.log_current()

    def value_lists(self) -> LogDataLists:
        lists = LogDataLists()
        for entry in self.entries:
            lists.cpu.append(entry.cpu_percent)
            lists.memory.append(entry.memory_percent)
            lists.timestamps.append(entry.timestamp)
            dt = datetime.utcfromtimestamp(entry.timestamp)
            lists.times.append(dt)
            lists.time_labels.append(f"{dt.hour}:{dt.minute}:{dt.second}")
            lists.disk_write.append(entry.disk_write * 0.000001)
            lists.disk_read.append(entry.disk_read * 0.000001)
            lists.net_down.append(entry.net_down * 0.000001)
            lists.net_up.append(entry.net_up * 0.000001)
        return lists

    def log_current(self) -> None:
        self.entries.append(LogEntry())

    def schedule(self, seconds_interval: int = 5) -> None:
        schedule.every(seconds_interval).seconds.do(self.log_current)

    def write_histogram(self) -> None:
        fig, (percentage_plot, disk_bytes_plot, network_bytes_plot) = plt.subplots(
            nrows=3, sharex=True
        )
        value_lists = self.value_lists()
        percentage_plot.plot(value_lists.timestamps, value_lists.cpu, label="cpu")
        percentage_plot.plot(value_lists.timestamps, value_lists.memory, label="memory")
        percentage_plot.set_ylim([0, 100])
        percentage_plot.yaxis.set(
            ticks=[
                0,
                10,
                20,
                30,
                40,
                50,
                60,
                70,
                80,
                90,
                100,
            ]
        )
        percentage_plot.grid(axis="y")
        percentage_plot.set_ylabel("percentage used")
        percentage_plot.set_xticks(value_lists.timestamps)
        percentage_plot.set_xticklabels(value_lists.time_labels)
        percentage_plot.legend()

        disk_bytes_plot.plot(
            value_lists.timestamps,
            to_differences(value_lists.disk_read),
            label="disk read",
        )
        disk_bytes_plot.plot(
            value_lists.timestamps,
            to_differences(value_lists.disk_write),
            label="disk write",
        )
        disk_bytes_plot.set_ylabel("megabytes")
        disk_bytes_plot.legend()

        network_bytes_plot.plot(
            value_lists.timestamps,
            to_differences(value_lists.net_down),
            label="net down",
        )

        network_bytes_plot.plot(
            value_lists.timestamps, to_differences(value_lists.net_up), label="net up"
        )
        network_bytes_plot.set_ylabel("megabytes")
        network_bytes_plot.legend()

        fig.autofmt_xdate()
        plt.title(
            f"usage over time for: job {MPI.COMM_WORLD.Get_rank()} on {platform.node()}"
        )
        plt.xlabel("time")
        plt.savefig(f"plots/{datetime.now().isoformat()}-id{slurm.slurm_localid()}.pdf")
