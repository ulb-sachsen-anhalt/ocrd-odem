"""define resource snapshots, monitor settings, and workflow IPC transport.

Memory and disk sizes use bytes; percentages use the 0-100 scale. Host memory
snapshots describe the system, not an individual workflow's allocation budget.
"""

import multiprocessing
import typing

from multiprocessing.connection import Connection

import humanize
import psutil

Result = typing.TypeVar('Result')


class RmException(Exception):
    """identify resource-check failures separately from OCR execution failures."""


class NotEnoughDiskSpaceException(RmException):
    """report insufficient free space at the configured monitoring path."""

    def __init__(self, path: str, bytes_needed: int, bytes_free: int, bytes_total: int):
        """format the required, free, and total disk bytes for an error message.

        The byte values describe the failed check and are rendered in binary
        units; this exception does not reserve space or undo downloaded files.
        """
        needed: str = humanize.naturalsize(bytes_needed, binary=True)
        free: str = humanize.naturalsize(bytes_free, binary=True)
        total: str = humanize.naturalsize(bytes_total, binary=True)
        msg: str = f'Not enough space in "{path}": {needed} are needed, {free} are free of total {total}'
        super().__init__(msg)


class VirtualMemoryExceededException(RmException):
    """report a host RAM threshold breach, not a per-process VMS limit."""

    def __init__(self, bytes_used: int, bytes_free: int, bytes_total: int, percent: float):
        """format sampled host memory values for the resource failure message.

        Sizes are bytes and percent is on the 0-100 scale. The exception records
        the condition in its message; it does not terminate a process itself.
        """
        used: str = humanize.naturalsize(bytes_used, binary=True)
        free: str = humanize.naturalsize(bytes_free, binary=True)
        total: str = humanize.naturalsize(bytes_total, binary=True)
        msg: str = f'Virtual memory limit exceeded: {used} are used ({percent} %). {free} are free of total {total}'
        super().__init__(msg)


class RmMemory(typing.NamedTuple):
    """hold a host RAM, swap, or disk snapshot in bytes and usage percent.

    The collector determines the meaning of used and free; RAM free is the
    psutil free value, not its estimate of memory available for allocation.
    """

    total: int
    used: int
    free: int
    percent: float


class RmProcessMemory(typing.NamedTuple):
    """hold process virtual address-space bytes and resident-memory percentage.

    vms is not resident RAM usage. percent comes from psutil's default
    memory_percent() calculation and measures RSS relative to system RAM.
    """

    vms: int
    percent: float


class RmProcess(typing.NamedTuple):
    """pair a live psutil handle with sampled identity and memory metadata.

    pid and ppid identify the process and its parent; name, exe, and cmdline
    describe its invocation. The handle can outlive the represented process,
    and the sampled fields are not continuously refreshed.
    """

    process: psutil.Process
    memory: RmProcessMemory
    pid: int
    ppid: int
    name: str
    exe: str
    cmdline: typing.List[str]


class RmDiskUsage(typing.NamedTuple):
    """associate filesystem usage with the requested and resolved path.

    absolute_path is the collector's absolute form of path. memory contains
    usage for the containing filesystem, not the size of the directory itself.
    """

    absolute_path: str
    path: str
    memory: RmMemory


class RmResourceData(typing.NamedTuple):
    """bundle one sampling pass over host resources and selected processes.

    pid identifies the sampling process, not an OCR worker. RAM, swap, disk,
    and process values are collected sequentially, not as an atomic snapshot.
    """

    pid: int
    processes: typing.List[RmProcess]
    virtual_memory: RmMemory
    swap_memory: RmMemory
    disk_usage: RmDiskUsage


RmResourceDataCallback = typing.Callable[[RmResourceData], None]


class RmProcessFilter(typing.NamedTuple):
    """select processes by command-line regexes and explicit process IDs.

    name_patterns matches the joined command line, despite its name. Each
    non-None group must match; alternatives within a group are ORed. None
    disables that restriction, whereas an empty list matches no processes.
    """

    name_patterns: typing.List[str] = None
    pids: typing.List[int] = None


class RmConfig(typing.NamedTuple):
    """configure background sampling and its synchronous snapshot callback.

    interval is in seconds, disk_usage_path selects a filesystem, and an absent
    process_filter includes all processes. callback executes on the sampling
    thread. Although interval permits None in its annotation, the sampler's
    numeric comparison requires a numeric value.
    """

    callback: RmResourceDataCallback
    interval: typing.Optional[float] = 1
    disk_usage_path: str = '/'
    process_filter: typing.Optional[RmProcessFilter] = None


class ProcessResourceMonitorConfig(typing.NamedTuple):
    """configure workflow checks and optional host-memory supervision.

    enable_resource_monitoring gates checks and subprocess supervision.
    polling_interval is in seconds; path_disk_usage selects the filesystem
    used to compare remaining space with load growth multiplied by
    factor_free_disk_space_needed. Live monitoring trips when either configured
    RAM threshold is exceeded: max_vmem_percentage on the 0-100 scale or
    max_vmem_bytes in bytes. None disables that live threshold, but the current
    percentage preflight requires max_vmem_percentage to be numeric.

    The CLI's separate live flag is not part of this configuration type.
    """

    enable_resource_monitoring: bool
    polling_interval: float
    path_disk_usage: str
    factor_free_disk_space_needed: float
    max_vmem_percentage: typing.Optional[float] = None
    max_vmem_bytes: typing.Optional[int] = None


class TheProcess(multiprocessing.Process):
    """execute a workflow in a child and send its outcome over a one-way pipe.

    Outcomes are (result, None) or (None, exception). The parent must read before
    joining so large synchronous sends can finish. Results and exceptions must
    be picklable; workflow object mutations remain local to the child.
    """

    def __init__(self, run: typing.Callable, connection: Connection):
        """retain a no-argument workflow callable and the pipe's sending endpoint.

        Construction does not start the child. The supervising parent owns
        process startup, result reception, joining, and its own endpoint copies.
        """
        multiprocessing.Process.__init__(self)
        self.__run: typing.Callable = run
        self.__result_connection: Connection = connection
        self.__exception: Exception | None = None

    @property
    def exception(self) -> Exception:
        """return the termination reason recorded in this process object's memory.

        No reason is represented by None despite the return annotation.
        Workflow exceptions are delivered through the pipe, not this property;
        attributes assigned in the child would not update the parent's copy.
        """
        return self.__exception

    def kill_with_exception(self, exception: Exception) -> None:
        """record a supervisor-side failure and request termination of this child.

        This does not join the child, stop descendants or containers, or wait
        for cleanup. Call it in the parent so the parent can recover the reason.
        """
        self.__exception = exception
        self.terminate()

    def run(self):
        """send the workflow result or an ordinary exception, then close the pipe.

        Sending is synchronous and can block until the parent reads. A result
        serialization failure is also sent as an exception when possible.
        BaseException subclasses and failures sending the exception itself are
        not converted into an outcome; the supervisor must handle missing data.
        """
        try:
            result: Result = self.__run()
            self.__result_connection.send((result, None))
        except Exception as exception:
            self.__result_connection.send((None, exception))
        finally:
            self.__result_connection.close()
