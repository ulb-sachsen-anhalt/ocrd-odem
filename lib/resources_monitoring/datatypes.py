from typing import Callable, NamedTuple, List, Optional

import psutil


class RmMemory(NamedTuple):
    total: int
    used: int
    free: int
    percent: float


class RmProcessMemory(NamedTuple):
    vms: int
    percent: float


class RmProcess(NamedTuple):
    process: psutil.Process
    memory: RmProcessMemory
    pid: int
    ppid: int
    name: str
    exe: str
    cmdline: List[str]


class RmDiskUsage(NamedTuple):
    absolute_path: str
    path: str
    memory: RmMemory


class RmResourceData(NamedTuple):
    pid: int
    processes: List[RmProcess]
    virtual_memory: RmMemory
    swap_memory: RmMemory
    disk_usage: RmDiskUsage


RmResourceDataCallback = Callable[[RmResourceData], None]


class RmProcessFilter(NamedTuple):
    name_patterns: List[str] = None
    pids: List[int] = None


class RmConfig(NamedTuple):
    callback: RmResourceDataCallback
    interval: Optional[float] = 1
    disk_usage_path: str = '/'
    process_filter: Optional[RmProcessFilter] = None


class ProcessResourceMonitorConfig(NamedTuple):
    enable_resource_monitoring: bool
    polling_interval: float
    path_disk_usage: str
    factor_free_disk_space_needed: float
    max_vmem_percentage: Optional[float] = None
    max_vmem_bytes: Optional[int] = None
