import logging
import json

from abc import ABC, abstractmethod
from typing import Generator, Optional
from io import BufferedIOBase
from jsonpath_ng import parse
from datetime import datetime


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileParser(ABC):

    def __init__(self, file_stream: BufferedIOBase, datetime_finder: Optional[str],
                 datetime_format: Optional[str]) -> None:
        self._file_stream = file_stream
        self._datetime_finder = datetime_finder
        self._datetime_format = datetime_format
        self._are_all_logs_parsed = True

    @property
    def are_all_logs_parsed(self) -> bool:
        return self._are_all_logs_parsed

    @abstractmethod
    def parse_file(self) -> Generator[str, None, None]:
        pass

    def get_log_datetime(self, log: str) -> Optional[datetime]:
        if self._datetime_finder is not None and self._datetime_format is not None:
            json_log = json.loads(log)
            match = parse(self._datetime_finder).find(json_log)

            if not match:
                return None

            try:
                log_datetime = datetime.strptime(match[0].value, self._datetime_format)
            except ValueError:
                logger.error("datetime in log {0} does not match datetime format {1}".format(log, self._datetime_format))
                return None

            return log_datetime

        return None
