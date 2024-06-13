import os
import re
import time
from os.path import abspath, isabs
from threading import Thread
from typing import Final, Optional
from typing import Iterator, List

import psutil

import lib.odem.monitoring as odem_m
# from lib.ocrd3_odem.monitoring import (RmConfig, RmDiskUsage, RmMemory, RmProcess, RmProcessFilter, RmProcessMemory,
                                    #   RmResourceData)


class ResourceMonitor:
    __POLL_INTERVAL: Final[float] = 0.001  # 1 ms

    def __init__(self, config: odem_m.RmConfig):
        self.__config: odem_m.RmConfig = config
        self.__is_running = False
        self.__enable_thread = False
        self.__thread: Optional[Thread] = None

    @property
    def is_running(self):
        return self.__is_running

    def start(self) -> bool:
        if self.__is_running:
            return False
        self.__is_running = True
        self.__enable_thread = True
        self.__thread = Thread(target=self.__thread_fn, daemon=True, args=())
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
                    data: odem_m.RmResourceData = ResourceMonitor.get_resource_data(
                        self.__config.process_filter,
                        self.__config.disk_usage_path
                    )
                    self.__config.callback(data)
                    last_time = time.time()
                time.sleep(ResourceMonitor.__POLL_INTERVAL)  # needed for keeping cpu usage low
        except Exception as e:
            raise e

    @staticmethod
    def get_processes_raw(cmd_patterns: List[str] = None, pids: List[int] = None) -> List[psutil.Process]:

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

        processes_all: Iterator[psutil.Process] = psutil.process_iter()
        return list(filter(filter_processes, processes_all))

    @staticmethod
    def get_processes(cmd_patterns: List[str] = None, pids: List[int] = None) -> List[odem_m.RmProcess]:

        def map_processes(process: psutil.Process) -> Optional[odem_m.RmProcess]:
            try:
                with process.oneshot():
                    memory_info = process.memory_full_info()
                    memory = odem_m.RmProcessMemory(
                        vms=memory_info.vms,
                        percent=process.memory_percent()
                    )
                    return odem_m.RmProcess(
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

        processes_filtered: List[psutil.Process] = ResourceMonitor.get_processes_raw(cmd_patterns, pids)
        processes_mapped: List[odem_m.RmProcess] = list(map(map_processes, processes_filtered))
        processes_not_none: List[odem_m.RmProcess] = list(filter(lambda p: p is not None, processes_mapped))
        return processes_not_none

    @staticmethod
    def get_virtual_memory() -> odem_m.RmMemory:
        svmem = psutil.virtual_memory()
        return odem_m.RmMemory(
            total=svmem.total,
            used=svmem.used,
            free=svmem.free,
            percent=svmem.percent,
        )

    @staticmethod
    def get_swap_memory() -> odem_m.RmMemory:
        smem = psutil.swap_memory()
        return odem_m.RmMemory(
            total=smem.total,
            used=smem.used,
            free=smem.free,
            percent=smem.percent,
        )

    @staticmethod
    def get_disk_usage(path: str) -> odem_m.RmDiskUsage:
        abs_path = path if isabs(path) else abspath(path)
        du = psutil.disk_usage(abs_path)
        return odem_m.RmDiskUsage(
            absolute_path=abs_path,
            path=path,
            memory=odem_m.RmMemory(
                total=du.total,
                used=du.used,
                free=du.free,
                percent=du.percent,
            )
        )

    @staticmethod
    def get_resource_data(process_filter: odem_m.RmProcessFilter = None, disk_usage_path: str = '/') -> odem_m.RmResourceData:
        virtual_memory: odem_m.RmMemory = ResourceMonitor.get_virtual_memory()
        swap_memory: odem_m.RmMemory = ResourceMonitor.get_swap_memory()
        disk_usage: odem_m.RmDiskUsage = ResourceMonitor.get_disk_usage(disk_usage_path)
        if process_filter is None:
            process_filter = odem_m.RmProcessFilter()
        processes: List[odem_m.RmProcess] = ResourceMonitor.get_processes(process_filter.name_patterns, process_filter.pids)
        data: odem_m.RmResourceData = odem_m.RmResourceData(
            pid=os.getpid(),
            disk_usage=disk_usage,
            processes=processes,
            virtual_memory=virtual_memory,
            swap_memory=swap_memory,
        )
        return data
