import logging
import json

from abc import ABC, abstractmethod
from typing import Generator, Optional, Any
from io import BufferedIOBase
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

    @abstractmethod
    def get_log_datetime(self, log: str) -> Optional[datetime]:
        pass

    def _get_log_datetime(self, log: str, json_path_parser: Optional[Any]) -> Optional[datetime]:
        if self._datetime_finder is None or self._datetime_format is None:
            return None

        if json_path_parser is None:
            return None

        json_log = json.loads(log)
        match = json_path_parser.find(json_log)

        if not match or match[0].value is None:
            logger.error("No match has been found with datetime finder json path {0} for log - {1}".format(
                self._datetime_finder, log))
            return None

        try:
            log_datetime = datetime.strptime(match[0].value, self._datetime_format)
        except ValueError:
            logger.error("datetime in log {0} does not match datetime format {1}".format(log, self._datetime_format))
            return None

        return log_datetime
