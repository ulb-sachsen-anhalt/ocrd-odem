"""API for process ressource monitoring"""

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
    __POLL_INTERVAL = 0.001  # 1 ms

    def __init__(self, config: odem_mdt.RmConfig):
        self.__config: odem_mdt.RmConfig = config
        self.__is_running = False
        self.__enable_thread = False
        self.__thread: threading.Thread = None

    @property
    def is_running(self):
        return self.__is_running

    def start(self) -> bool:
        if self.__is_running:
            return False
        self.__is_running = True
        self.__enable_thread = True
        self.__thread = threading.Thread(target=self.__thread_fn, daemon=True, args=())
        self.__thread.start()
        return True

    def stop(self) -> bool:
        if not self.__is_running:
            return False
        self.__enable_thread = False
        self.__thread.join()
        self.__is_running = False
        return True

    def __thread_fn(self) -> None:
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

        def filter_processes(process: psutil.Process) -> bool:
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

        def map_processes(process: psutil.Process) -> typing.Optional[odem_mdt.RmProcess]:
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
        svmem = psutil.virtual_memory()
        return odem_mdt.RmMemory(
            total=svmem.total,
            used=svmem.used,
            free=svmem.free,
            percent=svmem.percent,
        )

    @staticmethod
    def get_swap_memory() -> odem_mdt.RmMemory:
        smem = psutil.swap_memory()
        return odem_mdt.RmMemory(
            total=smem.total,
            used=smem.used,
            free=smem.free,
            percent=smem.percent,
        )

    @staticmethod
    def get_disk_usage(path: str) -> odem_mdt.RmDiskUsage:
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

    def __init__(
            self,
            config: odem_mdt.ProcessResourceMonitorConfig,
            fct_logger_error: typing.Callable = None,
            fct_client_update: typing.Callable = None,
            fct_notify: typing.Callable = None,
            process_identifier: str = None,
            rec_ident: str = None,
    ):
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
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        if not self.__config.enable_resource_monitoring:
            return fct_process_run()
        else:
            self.__resource_monitor.start()
            self.__process = odem_mdt.TheProcess(fct_process_run, result_queue)
            self.__process.start()
            self.__process.join()
            self.__resource_monitor.stop()
            if self.__process.exception is not None:
                raise self.__process.exception
            if not result_queue.empty():
                return result_queue.get()

    def check_vmem(self):
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
        self.__process.kill_with_exception(exc)


def from_configuration(config: configparser.ConfigParser) -> odem_mdt.ProcessResourceMonitorConfig:
    """Encapsulate transformation from configuration options into
    process monitor input config"""

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
