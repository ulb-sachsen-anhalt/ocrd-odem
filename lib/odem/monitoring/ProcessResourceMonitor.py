from __future__ import annotations

import math
import multiprocessing
import typing

import lib.odem as odem
import lib.odem.monitoring as odem_m


class ProcessResourceMonitor(typing.Generic[odem_m.Result]):

    def __init__(
            self,
            config: odem_m.ProcessResourceMonitorConfig,
            fct_logger_error: typing.Callable = None,
            fct_client_update: typing.Callable = None,
            fct_notify: typing.Callable = None,
            process_identifier: str = None,
            rec_ident: str = None,
    ):
        self.__config: odem_m.ProcessResourceMonitorConfig = config
        self.__resource_monitor: odem_m.ResourceMonitor = odem_m.ResourceMonitor(
            odem_m.RmConfig(
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
        self.__process: typing.Optional[odem_m.TheProcess] = None

    def monit_disk_space(self, fct_process_load: typing.Callable):
        if self.__config.enable_resource_monitoring:
            # check disk space before load
            memory_used_before_load: int = self.__resource_monitor.get_disk_usage(
                self.__config.path_disk_usage).memory.used
            fct_process_load()
            # check disk after load
            memory_after_load: odem_m.RmMemory = self.__resource_monitor.get_disk_usage(self.__config.path_disk_usage).memory
            memory_free_after_load: int = memory_after_load.free
            memory_used_after_load: int = memory_after_load.used
            memory_used_images_size: int = memory_used_after_load - memory_used_before_load
            memory_free_needed: int = math.ceil(memory_used_images_size * self.__config.factor_free_disk_space_needed)
            if memory_free_after_load < memory_free_needed:
                raise odem_m.NotEnoughDiskSpaceException(
                    path=self.__config.path_disk_usage,
                    bytes_needed=memory_free_needed,
                    bytes_free=memory_free_after_load,
                    bytes_total=memory_after_load.total
                )
        else:
            fct_process_load()

    def monit_vmem(self, fct_process_run: typing.Callable[[], odem_m.Result]) -> odem_m.Result | None:
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        if not self.__config.enable_resource_monitoring:
            return fct_process_run()
        else:
            self.__resource_monitor.start()
            self.__process = odem_m.TheProcess(fct_process_run, result_queue)
            self.__process.start()
            self.__process.join()
            self.__resource_monitor.stop()
            if self.__process.exception is not None:
                raise self.__process.exception
            if not result_queue.empty():
                return result_queue.get()

    def check_vmem(self):
        if self.__config.enable_resource_monitoring:
            vmem: odem_m.RmMemory = self.__resource_monitor.get_virtual_memory()
            memory_percentage: float = vmem.percent
            if memory_percentage > self.__config.max_vmem_percentage:
                raise odem_m.VirtualMemoryExceededException(
                    bytes_used=vmem.used,
                    bytes_free=vmem.free,
                    bytes_total=vmem.total,
                    percent=memory_percentage
                )

    def __resource_monitor_callback(self, resource_data: odem_m.RmResourceData) -> None:
        vmem: odem_m.RmMemory = resource_data.virtual_memory
        ram_percentage: float = vmem.percent
        ram_used: int = vmem.used
        vmem_exceeds_percentage: bool = (self.__config.max_vmem_percentage is not None) and \
                                        (ram_percentage > self.__config.max_vmem_percentage)
        vmem_exceeds_bytes: bool = (self.__config.max_vmem_bytes is not None) and \
                                   (ram_used > self.__config.max_vmem_bytes)
        if vmem_exceeds_percentage or vmem_exceeds_bytes:
            exception: odem_m.VirtualMemoryExceededException = odem_m.VirtualMemoryExceededException(
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

    def __kill_with_exception(self, exc: odem_m.RmException):
        self.__process.kill_with_exception(exc)
