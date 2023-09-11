from __future__ import annotations

import multiprocessing
from multiprocessing import Queue
from typing import Callable, Final

from lib.resources_monitoring import Result


class TheProcess(multiprocessing.Process):
    def __init__(self, run: Callable, queue: Queue):
        multiprocessing.Process.__init__(self)
        self.__run: Final[Callable] = run
        self.__result_queue: Final[Queue] = queue
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
