import logging
import io
import csv
import json

from typing import Generator
from io import BytesIO
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CsvParser(FileParser):
    
    def __init__(self, file_stream: BytesIO, delimiter: str) -> None:
        super().__init__(file_stream)
        self._delimiter = delimiter

    def parse_file(self) -> Generator[str, None, None]:
        logs = [self._file_stream.readline().decode("utf-8").rstrip(), '']

        while True:
            logs[1] = self._file_stream.readline().decode("utf-8").rstrip()

            if logs[1] == '':
                break

            reader = csv.DictReader(io.StringIO('\n'.join(logs)), delimiter=self._delimiter)
            log = next(reader)

            yield json.dumps(log)
