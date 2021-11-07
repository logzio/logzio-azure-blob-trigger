import logging
import re
import json

from typing import Generator, Optional
from io import BytesIO
from datetime import datetime
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TextParser(FileParser):

    def __init__(self, file_stream: BytesIO, multiline_regex: Optional[str] = None,
                 datetime_finder: Optional[str] = None, datetime_format: Optional[str] = None) -> None:
        super().__init__(file_stream, datetime_finder, datetime_format)
        self._multiline_regex = multiline_regex

    def parse_file(self) -> Generator[str, None, None]:
        if self._multiline_regex is not None:
            while True:
                log = self._file_stream.readline().decode("utf-8")

                if log == '':
                    break
                try:
                    multiline_log = self._get_multiline_log(log)
                except re.error:
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

    def get_log_datetime(self, log: str) -> Optional[datetime]:
        if self._datetime_finder is None or self._datetime_format is None:
            return None

        try:
            matches = re.findall(self._datetime_finder, log)
        except re.error as e:
            logger.error(
                "Something is wrong with the datetime finder regex {0} - {1}".format(repr(self._datetime_finder), e))
            return None

        if not matches:
            return None

        try:
            log_datetime = datetime.strptime(matches[0], self._datetime_format)
        except ValueError:
            logger.error("datetime in log {0} does not match datetime format {1}".format(log, self._datetime_format))
            return None

        return log_datetime

    def _get_multiline_log(self, multiline_log: str) -> Optional[str]:
        while True:
            try:
                if re.fullmatch(self._multiline_regex, multiline_log) is not None:
                    return multiline_log
                elif re.fullmatch(self._multiline_regex, multiline_log.rstrip()) is not None:
                    return multiline_log.rstrip()
            except re.error as e:
                logger.error("Something is wrong with the multiline regex {0} - {1}".format(repr(self._multiline_regex),
                                                                                            e))
                raise

            line = self._file_stream.readline().decode("utf-8")

            if line == '':
                return None

            multiline_log += line

    def _get_json_log(self, log: str) -> str:
        json_log = '{"message": ""}'
        json_log_data = json.loads(json_log)

        json_log_data['message'] = log

        return json.dumps(json_log_data)
