"""API datatypes related to monitoring"""

import configparser
import multiprocessing
import typing

import humanize
import psutil

Result = typing.TypeVar('Result')


class RmException(Exception):
    pass


class NotEnoughDiskSpaceException(RmException):
    def __init__(self, path: str, bytes_needed: int, bytes_free: int, bytes_total: int):
        needed: str = humanize.naturalsize(bytes_needed, binary=True)
        free: str = humanize.naturalsize(bytes_free, binary=True)
        total: str = humanize.naturalsize(bytes_total, binary=True)
        msg: str = f'Not enough space in "{path}": {needed} are needed, {free} are free of total {total}'
        super().__init__(msg)


class VirtualMemoryExceededException(RmException):
    def __init__(self, bytes_used: int, bytes_free: int, bytes_total: int, percent: float):
        used: str = humanize.naturalsize(bytes_used, binary=True)
        free: str = humanize.naturalsize(bytes_free, binary=True)
        total: str = humanize.naturalsize(bytes_total, binary=True)
        msg: str = f'Virtual memory limit exceeded: {used} are used ({percent} %). {free} are free of total {total}'
        super().__init__(msg)


class RmMemory(typing.NamedTuple):
    total: int
    used: int
    free: int
    percent: float


class RmProcessMemory(typing.NamedTuple):
    vms: int
    percent: float


class RmProcess(typing.NamedTuple):
    process: psutil.Process
    memory: RmProcessMemory
    pid: int
    ppid: int
    name: str
    exe: str
    cmdline: typing.List[str]


class RmDiskUsage(typing.NamedTuple):
    absolute_path: str
    path: str
    memory: RmMemory


class RmResourceData(typing.NamedTuple):
    pid: int
    processes: typing.List[RmProcess]
    virtual_memory: RmMemory
    swap_memory: RmMemory
    disk_usage: RmDiskUsage


RmResourceDataCallback = typing.Callable[[RmResourceData], None]


class RmProcessFilter(typing.NamedTuple):
    name_patterns: typing.List[str] = None
    pids: typing.List[int] = None


class RmConfig(typing.NamedTuple):
    callback: RmResourceDataCallback
    interval: typing.Optional[float] = 1
    disk_usage_path: str = '/'
    process_filter: typing.Optional[RmProcessFilter] = None


class ProcessResourceMonitorConfig(typing.NamedTuple):
    enable_resource_monitoring: bool
    polling_interval: float
    path_disk_usage: str
    factor_free_disk_space_needed: float
    max_vmem_percentage: typing.Optional[float] = None
    max_vmem_bytes: typing.Optional[int] = None


class TheProcess(multiprocessing.Process):
    def __init__(self, run: typing.Callable, queue: multiprocessing.Queue):
        multiprocessing.Process.__init__(self)
        self.__run: typing.Callable = run
        self.__result_queue: multiprocessing.Queue = queue
        self.__exception: Exception | None = None

    @property
    def exception(self) -> Exception:
        return self.__exception

    def kill_with_exception(self, exception: Exception) -> None:
        self.__exception = exception
        self.terminate()

    def run(self):
        try:
            result: Result = self.__run()
            self.__result_queue.put(result)
        except Exception as exception:
            self.__exception = exception
