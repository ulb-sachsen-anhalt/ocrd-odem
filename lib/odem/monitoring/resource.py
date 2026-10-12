"""sample system resources and optionally supervise an OCR workflow child.

Preflight checks and live subprocess supervision are separate operations.
Memory thresholds apply to host RAM, not per-workflow quotas; polling and
child termination do not provide hard limits or process-tree cleanup.
"""

from __future__ import annotations # for python 3.8 return variants

import configparser
import math
import multiprocessing
import os
import re
import time
import threading
import typing

import psutil

import lib.odem as odem
import lib.odem.monitoring.datatypes as odem_mdt


class ResourceMonitor:
    """collect periodic resource snapshots on a daemon thread.

    The configured callback runs synchronously on that thread. Sampling or
    callback exceptions end the thread and are not forwarded to its caller.
    Lifecycle flags describe requested state rather than thread health.
    """

    __POLL_INTERVAL = 0.1  # 100 ms

    def __init__(self, config: odem_mdt.RmConfig):
        """retain sampling settings without collecting data or starting a thread."""
        self.__config: odem_mdt.RmConfig = config
        self.__is_running = False
        self.__enable_thread = False
        self.__thread: threading.Thread = None

    @property
    def is_running(self):
        """return the lifecycle flag, which can remain true after a thread failure."""
        return self.__is_running

    def start(self) -> bool:
        """start background sampling, returning false if already marked running.

        A successful start returns true. The flag does not establish that later
        sampling or callback execution succeeds, and startup errors propagate.
        """
        if self.__is_running:
            return False
        self.__is_running = True
        self.__enable_thread = True
        self.__thread = threading.Thread(target=self.__thread_fn, daemon=True, args=())
        self.__thread.start()
        return True

    def stop(self) -> bool:
        """request sampler shutdown and wait for its thread to finish.

        Return false when not marked running, otherwise true after joining.
        Call from outside the sampling thread. There is no join timeout, so an
        in-progress collection or blocked callback can delay shutdown.
        """
        if not self.__is_running:
            return False
        self.__enable_thread = False
        self.__thread.join()
        self.__is_running = False
        return True

    def __thread_fn(self) -> None:
        """collect and deliver snapshots with the configured interval after callbacks.

        Short sleeps keep the polling loop from busy-waiting. Exceptions escape
        in this thread rather than becoming workflow failures in the parent.
        """
        last_time: float = 0
        try:
            while self.__enable_thread:
                time_now: float = time.time()
                time_diff: float = time_now - last_time
                if time_diff >= self.__config.interval:
                    data: odem_mdt.RmResourceData = ResourceMonitor.get_resource_data(
                        self.__config.process_filter,
                        self.__config.disk_usage_path
                    )
                    self.__config.callback(data)
                    last_time = time.time()
                time.sleep(ResourceMonitor.__POLL_INTERVAL)  # needed for keeping cpu usage low
        except Exception as e:
            raise e

    @staticmethod
    def get_processes_raw(cmd_patterns: typing.List = None, pids: typing.List = None) -> typing.List[psutil.Process]:
        """return psutil handles matching each supplied filter group.

        Regexes match the joined command line. A process must match a listed PID
        and a regex when both groups are supplied; alternatives within a group
        are ORed. None leaves a group unrestricted, but an empty list excludes
        all processes. Enumeration and command-line access errors propagate.
        """

        def filter_processes(process: psutil.Process) -> bool:
            """require a match in every non-None PID and command-pattern group."""
            pid_matches: bool = False
            if pids is not None:
                with process.oneshot():
                    for pid in pids:
                        if process.pid == pid:
                            pid_matches = True
                            break
            name_matches: bool = False
            if cmd_patterns is not None:
                with process.oneshot():
                    for cmd_pattern in cmd_patterns:
                        cmd_str: str = ' '.join(process.cmdline())
                        if re.search(cmd_pattern, cmd_str):
                            name_matches = True
                            break
            return (pid_matches or pids is None) and (name_matches or cmd_patterns is None)

        processes_all: typing.Iterator[psutil.Process] = psutil.process_iter()
        return list(filter(filter_processes, processes_all))

    @staticmethod
    def get_processes(cmd_patterns: typing.List = None, pids: typing.List = None) -> typing.List[odem_mdt.RmProcess]:
        """sample identity, virtual address size, and RSS percentage for matching processes.

        Skip processes that disappear or deny access while collecting fields.
        Errors during the preceding raw filtering phase are not caught here.
        Returned metadata is a snapshot, even though each record retains a live
        psutil handle.
        """

        def map_processes(process: psutil.Process) -> typing.Optional[odem_mdt.RmProcess]:
            """build a process snapshot, or return None if collection loses access."""
            try:
                with process.oneshot():
                    memory_info = process.memory_full_info()
                    memory = odem_mdt.RmProcessMemory(
                        vms=memory_info.vms,
                        percent=process.memory_percent()
                    )
                    return odem_mdt.RmProcess(
                        process=process,
                        memory=memory,
                        pid=process.pid,
                        ppid=process.ppid(),
                        name=process.name(),
                        exe=process.exe(),
                        cmdline=process.cmdline(),
                    )
            except psutil.AccessDenied:
                pass
            except psutil.NoSuchProcess:
                pass
            return None

        processes_filtered: typing.List[psutil.Process] = ResourceMonitor.get_processes_raw(cmd_patterns, pids)
        processes_mapped: typing.List[odem_mdt.RmProcess] = list(map(map_processes, processes_filtered))
        processes_not_none: typing.List[odem_mdt.RmProcess] = list(filter(lambda p: p is not None, processes_mapped))
        return processes_not_none

    @staticmethod
    def get_virtual_memory() -> odem_mdt.RmMemory:
        """sample host RAM bytes and usage percent, not process virtual memory.

        free preserves psutil's free field; it is not available memory, which
        also accounts for reclaimable resources.
        """
        svmem = psutil.virtual_memory()
        return odem_mdt.RmMemory(
            total=svmem.total,
            used=svmem.used,
            free=svmem.free,
            percent=svmem.percent,
        )

    @staticmethod
    def get_swap_memory() -> odem_mdt.RmMemory:
        """sample system-wide swap capacity, usage, free bytes, and usage percent."""
        smem = psutil.swap_memory()
        return odem_mdt.RmMemory(
            total=smem.total,
            used=smem.used,
            free=smem.free,
            percent=smem.percent,
        )

    @staticmethod
    def get_disk_usage(path: str) -> odem_mdt.RmDiskUsage:
        """sample the containing filesystem's usage and retain both path forms.

        Relative paths are resolved against the current working directory.
        This measures filesystem capacity, not directory size; invalid or
        inaccessible paths raise the underlying psutil or OS error.
        """
        abs_path = path if os.path.isabs(path) else os.path.abspath(path)
        du = psutil.disk_usage(abs_path)
        return odem_mdt.RmDiskUsage(
            absolute_path=abs_path,
            path=path,
            memory=odem_mdt.RmMemory(
                total=du.total,
                used=du.used,
                free=du.free,
                percent=du.percent,
            )
        )

    @staticmethod
    def get_resource_data(process_filter: odem_mdt.RmProcessFilter = None, disk_usage_path: str = '/') -> odem_mdt.RmResourceData:
        """collect RAM, swap, filesystem, and selected process snapshots sequentially.

        An absent filter selects all processes. The returned pid identifies
        the sampler, not a workflow child. Values are not an atomic system
        snapshot, and unhandled collector errors propagate to the caller.
        """
        virtual_memory: odem_mdt.RmMemory = ResourceMonitor.get_virtual_memory()
        swap_memory: odem_mdt.RmMemory = ResourceMonitor.get_swap_memory()
        disk_usage: odem_mdt.RmDiskUsage = ResourceMonitor.get_disk_usage(disk_usage_path)
        if process_filter is None:
            process_filter = odem_mdt.RmProcessFilter()
        processes: typing.List[odem_mdt.RmProcess] = ResourceMonitor.get_processes(process_filter.name_patterns, process_filter.pids)
        data: odem_mdt.RmResourceData = odem_mdt.RmResourceData(
            pid=os.getpid(),
            disk_usage=disk_usage,
            processes=processes,
            virtual_memory=virtual_memory,
            swap_memory=swap_memory,
        )
        return data


class ProcessResourceMonitor(typing.Generic[odem_mdt.Result]):
    """provide load-space checks, RAM preflight, and optional workflow supervision.

    Live execution adds a child process and a sampling thread. Thresholds are
    host-wide and termination targets only the wrapper child. The CLI chooses
    whether to invoke live execution using its separate monitoring.live flag.
    Instances hold one workflow process at a time and are not intended for
    concurrent monit_vmem calls.
    """

    def __init__(
            self,
            config: odem_mdt.ProcessResourceMonitorConfig,
            fct_logger_error: typing.Callable = None,
            fct_client_update: typing.Callable = None,
            fct_notify: typing.Callable = None,
            process_identifier: str = None,
            rec_ident: str = None,
    ):
        """prepare the sampler and optional failure callbacks without starting work.

        fct_logger_error accepts a logging format string and arguments.
        fct_client_update receives status, urn, and info keyword arguments;
        fct_notify receives a subject and message. process_identifier labels
        logs, while rec_ident identifies the record for updates and notices.
        Callbacks run on the sampling thread and must return for termination
        to proceed; prefer keyword arguments to avoid positional misbinding.
        """
        self.__config: odem_mdt.ProcessResourceMonitorConfig = config
        self.__resource_monitor: ResourceMonitor = ResourceMonitor(
            odem_mdt.RmConfig(
                interval=self.__config.polling_interval,
                callback=self.__resource_monitor_callback,
                disk_usage_path=self.__config.path_disk_usage,
            )
        )
        self.__fct_logger_error: typing.Callable = fct_logger_error
        self.__fct_client_update: typing.Callable = fct_client_update
        self.__fct_notify: typing.Callable = fct_notify
        self.__process_identifier: str = process_identifier
        self.__rec_ident: str = rec_ident
        self.__process: typing.Optional[odem_mdt.TheProcess] = None

    def monit_disk_space(self, fct_process_load: typing.Callable):
        """run loading and check remaining space against its measured disk growth.

        With monitoring enabled, compare filesystem usage before and after
        fct_process_load(), then require free space of at least the observed
        growth times factor_free_disk_space_needed, rounded up to bytes.
        Raise NotEnoughDiskSpaceException if insufficient space remains.

        This is a post-load estimate, not a reservation or continuous check;
        other filesystem activity affects the measured growth. Disabled
        monitoring simply invokes loading. Its return value is not forwarded,
        and failures do not roll back files.
        """
        if self.__config.enable_resource_monitoring:
            # check disk space before load
            memory_used_before_load: int = self.__resource_monitor.get_disk_usage(
                self.__config.path_disk_usage).memory.used
            fct_process_load()
            # check disk after load
            memory_after_load: odem_mdt.RmMemory = self.__resource_monitor.get_disk_usage(self.__config.path_disk_usage).memory
            memory_free_after_load: int = memory_after_load.free
            memory_used_after_load: int = memory_after_load.used
            memory_used_images_size: int = memory_used_after_load - memory_used_before_load
            memory_free_needed: int = math.ceil(memory_used_images_size * self.__config.factor_free_disk_space_needed)
            if memory_free_after_load < memory_free_needed:
                raise odem_mdt.NotEnoughDiskSpaceException(
                    path=self.__config.path_disk_usage,
                    bytes_needed=memory_free_needed,
                    bytes_free=memory_free_after_load,
                    bytes_total=memory_after_load.total
                )
        else:
            fct_process_load()

    def monit_vmem(self, fct_process_run: typing.Callable[[], odem_mdt.Result]) -> odem_mdt.Result | None:
        """execute a workflow directly or in a child supervised for host RAM usage.

        Disabled monitoring returns fct_process_run() without creating IPC.
        Otherwise start the child before the sampler and read its pipe outcome
        before joining, allowing large results to transfer without the former
        queue-flush deadlock. Results and ordinary exceptions must serialize.
        A workflow result of None is valid.

        Raise the monitor's recorded termination reason in preference to a
        received error; EOF without an outcome becomes RuntimeError. Cleanup
        attempts to stop the child and sampler and close the pipe endpoints.
        Receives and joins have no overall timeout, and descendants or containers
        are not reliably terminated. Workflow object mutations remain child-local.
        The CLI's live flag is not evaluated by this method.
        """
        if not self.__config.enable_resource_monitoring:
            return fct_process_run()

        result_connection, child_connection = multiprocessing.Pipe(duplex=False)
        self.__process = odem_mdt.TheProcess(fct_process_run, child_connection)
        try:
            self.__process.start()
            child_connection.close()
            self.__resource_monitor.start()
            while not result_connection.poll(0.1):
                if not self.__process.is_alive():
                    break
            try:
                result, exception = result_connection.recv()
            except EOFError:
                result = None
                exception = RuntimeError('Monitored workflow exited without returning a result')
            self.__process.join()
            self.__resource_monitor.stop()
            if self.__process.exception is not None:
                raise self.__process.exception
            if exception is not None:
                raise exception
            return result
        finally:
            if self.__process.pid is not None:
                if self.__process.is_alive():
                    self.__process.terminate()
                self.__process.join()
            self.__resource_monitor.stop()
            result_connection.close()
            child_connection.close()

    def check_vmem(self):
        """reject an enabled workflow preflight above the host RAM percentage limit.

        Raise VirtualMemoryExceededException on a strict percentage breach;
        do nothing when monitoring is disabled. This check ignores the bytes
        threshold and currently requires a numeric max_vmem_percentage: None
        causes TypeError rather than disabling the preflight threshold.
        """
        if self.__config.enable_resource_monitoring:
            vmem: odem_mdt.RmMemory = self.__resource_monitor.get_virtual_memory()
            memory_percentage: float = vmem.percent
            if memory_percentage > self.__config.max_vmem_percentage:
                raise odem_mdt.VirtualMemoryExceededException(
                    bytes_used=vmem.used,
                    bytes_free=vmem.free,
                    bytes_total=vmem.total,
                    percent=memory_percentage
                )

    def __resource_monitor_callback(self, resource_data: odem_mdt.RmResourceData) -> None:
        """report a strict breach of either configured host RAM threshold and terminate.

        None disables the corresponding live threshold. Logging, record updates,
        and notifications precede termination and run on the sampling thread;
        an exception or hang in a callback can prevent the termination request.
        """
        vmem: odem_mdt.RmMemory = resource_data.virtual_memory
        ram_percentage: float = vmem.percent
        ram_used: int = vmem.used
        vmem_exceeds_percentage: bool = (self.__config.max_vmem_percentage is not None) and \
                                        (ram_percentage > self.__config.max_vmem_percentage)
        vmem_exceeds_bytes: bool = (self.__config.max_vmem_bytes is not None) and \
                                   (ram_used > self.__config.max_vmem_bytes)
        if vmem_exceeds_percentage or vmem_exceeds_bytes:
            exception: odem_mdt.VirtualMemoryExceededException = odem_mdt.VirtualMemoryExceededException(
                bytes_used=vmem.used,
                bytes_free=vmem.free,
                bytes_total=vmem.total,
                percent=vmem.percent
            )
            err_args = str(exception)
            name = type(exception).__name__
            if self.__fct_logger_error is not None:
                self.__fct_logger_error(
                    "[%s] odem fails with %s: '%s'", self.__process_identifier, name, err_args
                )
            if self.__fct_client_update is not None:
                self.__fct_client_update(status=odem.MARK_OCR_FAIL, urn=self.__rec_ident, info=err_args)
            if self.__fct_notify is not None:
                self.__fct_notify(f'[OCR-D-ODEM] Failure for {self.__rec_ident}', err_args)
            self.__kill_with_exception(exception)

    def __kill_with_exception(self, exc: odem_mdt.RmException):
        """record the failure in the parent and terminate the initialized workflow child."""
        self.__process.kill_with_exception(exc)


def from_configuration(config: configparser.ConfigParser) -> odem_mdt.ProcessResourceMonitorConfig:
    """read monitoring settings with typed conversions and implementation defaults.

    Defaults are disabled monitoring, a one-second polling interval, disk path
    /home/ocr, a disk-growth factor of 3.0, and unset RAM thresholds. Invalid
    configured values raise ConfigParser conversion errors. The live flag is
    deliberately left to CLI execution selection and is not read here.
    """

    cfg_enabled_monitoring = config.getboolean(odem.CFG_SEC_MONITOR, 'enable', fallback=False)
    cfg_polling_interval = config.getfloat(odem.CFG_SEC_MONITOR, 'polling_interval', fallback=1)
    cfg_path_usage = config.get(odem.CFG_SEC_MONITOR, 'path_disk_usage', fallback='/home/ocr')
    cfg_space_needed = config.getfloat(
        odem.CFG_SEC_MONITOR,
        'factor_free_disk_space_needed',
        fallback=3.0
    )
    cfg_vmem_percentage = config.getfloat(odem.CFG_SEC_MONITOR, 'max_vmem_percentage', fallback=None)
    cfg_vmem_bytes = config.getint(odem.CFG_SEC_MONITOR, 'max_vmem_bytes', fallback=None)
    return odem_mdt.ProcessResourceMonitorConfig(
        enable_resource_monitoring=cfg_enabled_monitoring,
        polling_interval=cfg_polling_interval,
        path_disk_usage=cfg_path_usage,
        factor_free_disk_space_needed=cfg_space_needed,
        max_vmem_percentage=cfg_vmem_percentage,
        max_vmem_bytes=cfg_vmem_bytes,
    )
