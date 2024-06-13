
import configparser
import psutil
from typing import Callable, NamedTuple, List, Optional, TypeVar

Result = TypeVar('Result')


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


def from_configuration(config: configparser.ConfigParser) -> ProcessResourceMonitorConfig:
    """Encapsulate transformation from configuration options into
    process monitor input config"""
    
    cfg_enabled_monitoring = config.getboolean('resource-monitoring', 'enable', fallback=False)
    cfg_polling_interval = config.getfloat('resource-monitoring', 'polling_interval', fallback=1)
    cfg_path_usage = config.get('resource-monitoring', 'path_disk_usage', fallback='/home/ocr')
    cfg_space_needed = config.getfloat(
        'resource-monitoring',
        'factor_free_disk_space_needed',
        fallback=3.0
    )
    cfg_vmem_percentage = config.getfloat('resource-monitoring', 'max_vmem_percentage', fallback=None)
    cfg_vmem_bytes = config.getint('resource-monitoring', 'max_vmem_bytes', fallback=None)
    return ProcessResourceMonitorConfig(
        enable_resource_monitoring=cfg_enabled_monitoring,
        polling_interval=cfg_polling_interval,
        path_disk_usage=cfg_path_usage,
        factor_free_disk_space_needed=cfg_space_needed,
        max_vmem_percentage=cfg_vmem_percentage,
        max_vmem_bytes=cfg_vmem_bytes,
    )
