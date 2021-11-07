import queue

from typing import Optional


class ConsumerProducerQueues:

    END_LOG = 'END'

    def __init__(self) -> None:
        self._logs_queue = queue.Queue()
        self._info_queue = queue.Queue()
        self._errors_queue = queue.Queue()

    def get_log_from_queue(self) -> Optional[str]:
        log = self._logs_queue.get()

        if log == ConsumerProducerQueues.END_LOG:
            return None

        return log

    def put_log_into_queue(self, log: str) -> None:
        self._logs_queue.put(log)

    def put_end_log_into_queue(self) -> None:
        self._logs_queue.put(ConsumerProducerQueues.END_LOG)

    def get_info_from_queue(self) -> Optional[str]:
        if self._info_queue.empty():
            return None

        try:
            info_message = self._info_queue.get(timeout=1)
        except queue.Empty:
            return None

        return info_message

    def put_info_into_queue(self, info_message: str) -> None:
        self._info_queue.put(info_message)

    def get_error_from_queue(self) -> Optional[str]:
        if self._errors_queue.empty():
            return None

        try:
            error_message = self._errors_queue.get(timeout=1)
        except queue.Empty:
            return None

        return error_message

    def put_error_into_queue(self, error_message: str) -> None:
        self._errors_queue.put(error_message)
