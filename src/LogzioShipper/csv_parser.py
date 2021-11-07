import logging
import io
import csv
import json

from typing import Generator, Optional
from io import BytesIO
from jsonpath_ng import parse, JSONPathError
from datetime import datetime
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CsvParser(FileParser):
    
    def __init__(self, file_stream: BytesIO, delimiter: str, datetime_finder: Optional[str] = None,
                 datetime_format: Optional[str] = None) -> None:
        super().__init__(file_stream, datetime_finder, datetime_format)
        self._delimiter = delimiter

        if self._datetime_finder is not None:
            try:
                self._json_path_parser = parse(self._datetime_finder)
            except JSONPathError as e:
                logger.error(
                    "Something is wrong with the datetime finder json path {0} - {1}".format(self._datetime_finder, e))
                self._json_path_parser = None

    def parse_file(self) -> Generator[str, None, None]:
        logs = [self._file_stream.readline().decode("utf-8").rstrip(), '']

        while True:
            logs[1] = self._file_stream.readline().decode("utf-8").rstrip()

            if logs[1] == '':
                break

            reader = csv.DictReader(io.StringIO('\n'.join(logs)), delimiter=self._delimiter)
            log = next(reader)

            yield json.dumps(log)

    def get_log_datetime(self, log: str) -> Optional[datetime]:
        return self._get_log_datetime(log, self._json_path_parser)
