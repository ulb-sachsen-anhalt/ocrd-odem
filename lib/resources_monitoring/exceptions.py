import humanize


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
