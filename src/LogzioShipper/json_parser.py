import logging
import json

from typing import Generator
from io import BytesIO
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JsonParser(FileParser):

    def __init__(self, file_stream: BytesIO) -> None:
        super().__init__(file_stream)

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
