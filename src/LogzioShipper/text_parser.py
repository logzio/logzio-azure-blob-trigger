import logging
import re

from typing import Generator, Optional
from io import BytesIO
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TextParser(FileParser):

    NO_REGEX_VALUE = 'NO_REGEX'

    def __init__(self, file_stream: BytesIO, multiline_regex: str) -> None:
        super().__init__(file_stream)
        self._multiline_regex = multiline_regex

    def parse_file(self) -> Generator:
        if self._multiline_regex != TextParser.NO_REGEX_VALUE:
            while True:
                log = self._file_stream.readline().decode("utf-8")

                if log == '':
                    break

                multiline_log = self._get_multiline_log(log)

                if multiline_log is None:
                    logger.error("There is no match using the regex {}".format(repr(self._multiline_regex)))
                    self._are_all_logs_parsed = False
                    break

                yield multiline_log
        else:
            while True:
                log = self._file_stream.readline().decode("utf-8").rstrip()

                if log == '':
                    break

                log = log.replace('"', '\\"')

                yield "{{\"message\": \"{}\"}}".format(log)

    def _get_multiline_log(self, multiline_log: str) -> Optional[str]:
        while True:
            if re.fullmatch(self._multiline_regex, multiline_log) is not None:
                multiline_log = multiline_log.replace('\n', ' ')
                multiline_log = multiline_log.replace('"', '\\"')

                return "{{\"message\": \"{}\"}}".format(multiline_log)
            elif re.fullmatch(self._multiline_regex, multiline_log.rstrip()) is not None:
                multiline_log = multiline_log.replace('\n', ' ')
                multiline_log = multiline_log.replace('"', '\\"')

                return "{{\"message\": \"{}\"}}".format(multiline_log.rstrip().replace('\n', ' '))

            line = self._file_stream.readline().decode("utf-8")

            if line == '':
                return None

            multiline_log += line
