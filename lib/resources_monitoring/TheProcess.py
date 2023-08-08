import multiprocessing
from typing import Callable, Optional


class TheProcess(multiprocessing.Process):
    def __init__(self, run: Callable):
        multiprocessing.Process.__init__(self)
        self.__run: Callable = run
        self.__exception: Optional[Exception] = None

    @property
    def exception(self) -> Exception:
        return self.__exception

    def kill_with_exception(self, exception: Exception) -> None:
        self.__exception = exception
        self.terminate()

    def run(self):
        try:
            self.__run()
        except Exception as exception:
            self.__exception = exception
