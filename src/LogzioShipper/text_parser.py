import logging
import re

from typing import Generator, Optional
from io import BytesIO
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TextParser(FileParser):

    def __init__(self, file_stream: BytesIO, multiline_regex: str) -> None:
        super().__init__(file_stream)
        self.multiline_regex = multiline_regex

    def parse_file(self) -> Generator:
        if self.multiline_regex != '':
            while True:
                log = self.file_stream.readline().decode("utf-8")

                if log == '':
                    break

                multiline_log = self.__get_multiline_log(log)

                if multiline_log is None:
                    logger.error("There is no match using the regex {}".format(self.multiline_regex))
                    break

                yield multiline_log
        else:
            while True:
                log = self.file_stream.readline().decode("utf-8").rstrip()

                if log == '':
                    break

                yield "{{\"message\": \"{}\"}}".format(log)

    def __get_multiline_log(self, multiline_log: str) -> Optional[str]:
        while True:
            if re.fullmatch(self.multiline_regex, multiline_log) is not None:
                return "{{\"message\": \"{}\"}}".format(multiline_log.replace('\n', ' '))
            elif re.fullmatch(self.multiline_regex, multiline_log.rstrip()) is not None:
                return "{{\"message\": \"{}\"}}".format(multiline_log.rstrip().replace('\n', ' '))

            line = self.file_stream.readline().decode("utf-8")

            if line == '':
                return None

            multiline_log += line
