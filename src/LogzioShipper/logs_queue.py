import queue

from typing import Optional


class LogsQueue:

    END_LOG = 'END'

    def __init__(self) -> None:
        self._queue = queue.Queue()

    def get_log_from_queue(self) -> Optional[str]:
        log = self._queue.get()

        if log == LogsQueue.END_LOG:
            return None

        return log

    def put_log_into_queue(self, log: str) -> None:
        self._queue.put(log)

    def put_end_log_into_queue(self) -> None:
        self._queue.put(LogsQueue.END_LOG)
