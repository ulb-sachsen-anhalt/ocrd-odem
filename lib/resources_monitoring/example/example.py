import subprocess
from enum import Enum
from typing import List, Any, Optional

from lib.resources_monitoring import (
    ResourceMonitor,
    RmResourceData,
    RmConfig,
    RmProcessFilter,
    TheProcess, RmException,
)

MAX_RAM_PERCENTAGE: float = 70
MAX_DISK_USAGE_PERCENTAGE: float = 20


class Reason(Enum):
    RAM_EXCEEDED = 1,
    DISK_SPACE_EXCEEDED = 2


def long_process():
    print('process start')
    completed_process: subprocess.CompletedProcess = subprocess.run(
        ['python', './lib/resources_monitoring/example/increase_vmem_process.py'])
    print('process stop')


def kill(processes: List[TheProcess], reason: Reason) -> None:
    print('kill!!!', reason._name_)
    # print('Processes', processes)
    for process in processes:
        process.process.kill()
        pass


def callback(resource_data: RmResourceData) -> None:
    # print('callback', resource_data)
    ram_percentage: float = resource_data.virtual_memory.percent
    print('ram_percentage', ram_percentage)
    disk_usage_percentage: float = resource_data.disk_usage.memory.percent
    print('disk_usage_percentage', disk_usage_percentage)
    if ram_percentage > MAX_RAM_PERCENTAGE or disk_usage_percentage > MAX_DISK_USAGE_PERCENTAGE:
        reason: Optional[Reason] = None
        if ram_percentage > MAX_RAM_PERCENTAGE:
            reason = Reason.RAM_EXCEEDED
        elif disk_usage_percentage > MAX_DISK_USAGE_PERCENTAGE:
            reason = Reason.DISK_SPACE_EXCEEDED
        processes_not_me: List[TheProcess] = list(filter(lambda p: p.pid != resource_data.pid, resource_data.processes))
        kill(processes_not_me, reason)


if __name__ == "__main__":
    INTERVAL: int = 1
    PROC_NAME_PATT: List[str] = [r'^python[\d\.]?$', r'^ocrd-[a-zA-Z0-9_-]+$']
    DU_PATH: str = '/home/anitsche'
    PROC_FILTER: RmProcessFilter = RmProcessFilter(
        name_patterns=PROC_NAME_PATT,
    )
    resource_monitor_config: RmConfig = RmConfig(
        interval=INTERVAL,
        callback=callback,
        disk_usage_path=DU_PATH,
        process_filter=PROC_FILTER
    )
    resource_monitor: ResourceMonitor = ResourceMonitor(resource_monitor_config)
    resource_monitor.start()
    long_process()
    resource_monitor.stop()
