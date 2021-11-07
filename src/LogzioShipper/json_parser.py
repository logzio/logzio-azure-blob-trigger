import logging
import json

from typing import Generator, Optional
from io import BytesIO
from jsonpath_ng import parse, JSONPathError
from datetime import datetime
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JsonParser(FileParser):

    def __init__(self, file_stream: BytesIO, datetime_finder: Optional[str] = None,
                 datetime_format: Optional[str] = None) -> None:
        super().__init__(file_stream, datetime_finder, datetime_format)

        if self._datetime_finder is not None:
            try:
                self._json_path_parser = parse(self._datetime_finder)
            except JSONPathError as e:
                logger.error(
                    "Something is wrong with the datetime finder json path {0} - {1}".format(self._datetime_finder, e))
                self._json_path_parser = None

    def parse_file(self) -> Generator[str, None, None]:
        while True:
            log = self._file_stream.readline().decode("utf-8").rstrip()

            if log == '':
                break

            try:
                json.loads(log)
                yield log
            except ValueError:
                logger.error("The following json is not valid: {}".format(log))
                self._are_all_logs_parsed = False

    def get_log_datetime(self, log: str) -> Optional[datetime]:
        return self._get_log_datetime(log, self._json_path_parser)
