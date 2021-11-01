import logging
import re
import json

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

    def parse_file(self) -> Generator[str, None, None]:
        if self._multiline_regex != TextParser.NO_REGEX_VALUE:
            while True:
                log = self._file_stream.readline().decode("utf-8")

                if log == '':
                    break
                try:
                    multiline_log = self._get_multiline_log(log)
                except Exception:
                    self._are_all_logs_parsed = False
                    break

                if multiline_log is None:
                    logger.error("There is no match using the regex {}".format(repr(self._multiline_regex)))
                    self._are_all_logs_parsed = False
                    break

                yield self._get_json_log(multiline_log)
        else:
            while True:
                log = self._file_stream.readline().decode("utf-8").rstrip()

                if log == '':
                    break

                yield self._get_json_log(log)

    def _get_multiline_log(self, multiline_log: str) -> Optional[str]:
        while True:
            try:
                match = re.fullmatch(self._multiline_regex, multiline_log)
            except Exception as e:
                logger.error("Something is wrong with the multiline regex {0} - {1}".format(repr(self._multiline_regex),
                                                                                            e))
                raise

            if match is not None:
                return multiline_log

            line = self._file_stream.readline().decode("utf-8")

            if line == '':
                return None

            multiline_log += line

    def _get_json_log(self, log: str) -> str:
        json_log = '{"message": ""}'
        json_log_data = json.loads(json_log)

        json_log_data['message'] = log

        return json.dumps(json_log_data)
