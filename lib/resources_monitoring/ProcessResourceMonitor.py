from __future__ import annotations

import math
import multiprocessing
from typing import Callable, Optional, Generic

from lib.ocrd3_odem import MARK_OCR_FAIL
from lib.resources_monitoring import (
    NotEnoughDiskSpaceException, ProcessResourceMonitorConfig, ResourceMonitor,
    RmConfig, RmException, RmMemory, RmResourceData,
    VirtualMemoryExceededException, Result
)
from lib.resources_monitoring.TheProcess import TheProcess


class ProcessResourceMonitor(Generic[Result]):

    def __init__(
            self,
            config: ProcessResourceMonitorConfig,
            fct_logger_error: Callable = None,
            fct_client_update: Callable = None,
            fct_notify: Callable = None,
            process_identifier: str = None,
            rec_ident: str = None,
    ):

        self.__config: ProcessResourceMonitorConfig = config
        self.__resource_monitor: ResourceMonitor = ResourceMonitor(
            RmConfig(
                interval=self.__config.polling_interval,
                callback=self.__resource_monitor_callback,
                disk_usage_path=self.__config.path_disk_usage,
            )
        )
        self.__fct_logger_error: Callable = fct_logger_error
        self.__fct_client_update: Callable = fct_client_update
        self.__fct_notify: Callable = fct_notify
        self.__process_identifier: str = process_identifier
        self.__rec_ident: str = rec_ident

        self.__process: Optional[TheProcess] = None

    def monit_disk_space(self, fct_process_load: Callable):
        if self.__config.enable_resource_monitoring:
            # check disk space before load
            memory_used_before_load: int = self.__resource_monitor.get_disk_usage(
                self.__config.path_disk_usage).memory.used
            fct_process_load()
            # check disk after load
            memory_after_load: RmMemory = self.__resource_monitor.get_disk_usage(self.__config.path_disk_usage).memory
            memory_free_after_load: int = memory_after_load.free
            memory_used_after_load: int = memory_after_load.used
            memory_used_images_size: int = memory_used_after_load - memory_used_before_load
            memory_free_needed: int = math.ceil(memory_used_images_size * self.__config.factor_free_disk_space_needed)
            if memory_free_after_load < memory_free_needed:
                raise NotEnoughDiskSpaceException(
                    path=self.__config.path_disk_usage,
                    bytes_needed=memory_free_needed,
                    bytes_free=memory_free_after_load,
                    bytes_total=memory_after_load.total
                )
        else:
            fct_process_load()

    def monit_vmem(self, fct_process_run: Callable[[], Result]) -> Result | None:
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        if not self.__config.enable_resource_monitoring:
            return fct_process_run()
        else:
            self.__resource_monitor.start()
            self.__process = TheProcess(fct_process_run, result_queue)
            self.__process.start()
            self.__process.join()
            self.__resource_monitor.stop()
            if self.__process.exception is not None:
                raise self.__process.exception
            if not result_queue.empty():
                return result_queue.get()

    def check_vmem(self):
        if self.__config.enable_resource_monitoring:
            vmem: RmMemory = self.__resource_monitor.get_virtual_memory()
            memory_percentage: float = vmem.percent
            if memory_percentage > self.__config.max_vmem_percentage:
                raise VirtualMemoryExceededException(
                    bytes_used=vmem.used,
                    bytes_free=vmem.free,
                    bytes_total=vmem.total,
                    percent=memory_percentage
                )

    def __resource_monitor_callback(self, resource_data: RmResourceData) -> None:
        vmem: RmMemory = resource_data.virtual_memory
        ram_percentage: float = vmem.percent
        ram_used: int = vmem.used
        vmem_exceeds_percentage: bool = (self.__config.max_vmem_percentage is not None) and \
                                        (ram_percentage > self.__config.max_vmem_percentage)
        vmem_exceeds_bytes: bool = (self.__config.max_vmem_bytes is not None) and \
                                   (ram_used > self.__config.max_vmem_bytes)

        # print(ram_percentage, self.__config.max_vmem_percentage, vmem_exceeds_percentage, vmem_exceeds_bytes)

        if vmem_exceeds_percentage or vmem_exceeds_bytes:
            exception: VirtualMemoryExceededException = VirtualMemoryExceededException(
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
                self.__fct_client_update(status=MARK_OCR_FAIL, urn=self.__rec_ident, info=err_args)

            if self.__fct_notify is not None:
                self.__fct_notify(f'[OCR-D-ODEM] Failure for {self.__rec_ident}', err_args)

            self.__kill_with_exception(exception)

    def __kill_with_exception(self, exc: RmException):
        self.__process.kill_with_exception(exc)
