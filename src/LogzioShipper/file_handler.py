import logging
import os
import csv
import zlib
import json

from typing import Optional, Generator
from io import BytesIO, IOBase
from jsonpath_ng import parse
from dateutil import parser
from .file_parser import FileParser
from .json_parser import JsonParser
from .csv_parser import CsvParser
from .text_parser import TextParser
from .logzio_shipper import LogzioShipper
from .custom_field import CustomField


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileHandler:

    JSON_STARTING_CHAR = '{'
    GZ_MAGIC_NUMBER = b'\x1f\x8b'
    CSV_DELIMITERS: Optional[str] = [',', ';', '|']

    LOGZIO_URL_ENVIRON_NAME = 'LogzioURL'
    LOGZIO_TOKEN_ENVIRON_NAME = 'LogzioToken'
    MULTILINE_REGEX_ENVIRON_NAME = 'MultilineRegex'
    FILTER_DATE_ENVIRON_NAME = 'FilterDate'
    FILTER_DATE_JSON_PATH_ENVIRON_NAME = 'FilterDateJsonPath'

    NO_FILTER_DATE_VALUE = 'NO_FILTER_DATE'
    NO_FILTER_DATE_JSON_PATH_VALUE = 'NO_FILTER_DATE_JSON_PATH'

    def __init__(self, file_name: str, file_stream: IOBase, file_size: int) -> None:
        self._file_name = file_name
        self._file_stream = self._get_seekable_file_stream(file_stream)
        self._file_size = file_size
        self._filter_date = self._get_filter_date()
        self._filter_date_json_path = self._get_filter_date_json_path()
        self._file_parser = self._get_file_parser()
        self._logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                             os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])
        self._custom_fields = [CustomField(field_key='file', field_value=self._file_name)]

        self._add_custom_fields_to_logzio_shipper()

    @property
    def file_parser(self) -> FileParser:
        return self._file_parser

    def get_custom_fields(self) -> Generator:
        for custom_field in self._custom_fields:
            yield custom_field

    class FailedToSendLogsError(Exception):
        pass

    def handle_file(self) -> None:
        if self._file_size == 0:
            logger.info("The file {} is empty.".format(self._file_name))
            return

        logging.info("Starts processing file - {}".format(self._file_name))

        try:
            self._send_logs_to_logzio()
        except Exception:
            raise self.FailedToSendLogsError()

        logger.info("Successfully finished processing file - {}".format(self._file_name))

    def _get_seekable_file_stream(self, file_stream: IOBase) -> BytesIO:
        seekable_file_stream = BytesIO()

        for line in file_stream:
            seekable_file_stream.write(line)

        seekable_file_stream.seek(0)

        if not self._is_gz_file(seekable_file_stream):
            return seekable_file_stream

        return self._get_seekable_decompressed_gz_file_stream(seekable_file_stream)

    def _is_gz_file(self, seekable_file_stream: BytesIO) -> bool:
        is_gz_file = False

        if seekable_file_stream.read(2) == FileHandler.GZ_MAGIC_NUMBER:
            is_gz_file = True

        seekable_file_stream.seek(0)

        return is_gz_file

    def _get_seekable_decompressed_gz_file_stream(self, seekable_file_stream: BytesIO) -> BytesIO:
        seekable_decompressed_gz_file_stream = BytesIO()
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

        for line in seekable_file_stream:
            decompressed_lines = decompressor.decompress(line).decode("utf-8")
            new_lines_num = decompressed_lines.count('\n')

            for decompressed_line in decompressed_lines.splitlines():
                if new_lines_num > 0:
                    seekable_decompressed_gz_file_stream.write(str.encode(decompressed_line + '\n'))
                    new_lines_num -= 1
                else:
                    seekable_decompressed_gz_file_stream.write(str.encode(decompressed_line))

        seekable_decompressed_gz_file_stream.seek(0)

        return seekable_decompressed_gz_file_stream

    def _get_filter_date(self) -> Optional[str]:
        filter_date = os.environ[FileHandler.FILTER_DATE_ENVIRON_NAME]

        if filter_date == FileHandler.NO_FILTER_DATE_VALUE:
            return None

        return filter_date

    def _get_filter_date_json_path(self) -> Optional[str]:
        filter_date_json_path = os.environ[FileHandler.FILTER_DATE_JSON_PATH_ENVIRON_NAME]

        if filter_date_json_path == FileHandler.NO_FILTER_DATE_JSON_PATH_VALUE:
            return None

        return filter_date_json_path

    def _get_file_parser(self) -> FileParser:
        logs_sample = [self._file_stream.readline().decode("utf-8").rstrip(),
                       self._file_stream.readline().decode("utf-8").rstrip()]

        self._file_stream.seek(0)

        if logs_sample[0].startswith(FileHandler.JSON_STARTING_CHAR):
            return JsonParser(self._file_stream)

        delimiter = self._is_file_csv(logs_sample)

        if delimiter is not None:
            return CsvParser(self._file_stream, delimiter)

        return TextParser(self._file_stream, os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME])

    def _add_custom_fields_to_logzio_shipper(self) -> None:
        for custom_field in self._custom_fields:
            self._logzio_shipper.add_custom_field_to_list(custom_field)

    def _is_file_csv(self, logs_sample: list) -> Optional[str]:
        sample = '\n'.join(logs_sample)

        try:
            dialect = csv.Sniffer().sniff(sample, FileHandler.CSV_DELIMITERS)

            return str(dialect.delimiter)
        except csv.Error:
            return None

    def _send_logs_to_logzio(self) -> None:
        is_log_added_to_send = False

        for log in self._file_parser.parse_file():
            if not self._is_log_date_greater_or_equal_filter_date(log):
                logger.info("Log was not sent to Logzio because of filter date - {}".format(log))
                continue

            try:
                self._logzio_shipper.add_log_to_send(log)
                is_log_added_to_send = True
            except Exception:
                logger.error("Failed to send logs to Logz.io for {}".format(self._file_name))
                raise self.FailedToSendLogsError()

        if not is_log_added_to_send:
            logger.error("Failed to send logs to Logz.io for {}".format(self._file_name))
            raise self.FailedToSendLogsError()

        try:
            self._logzio_shipper.send_to_logzio()
        except Exception:
            logger.error("Failed to send logs to Logz.io for {}".format(self._file_name))
            raise self.FailedToSendLogsError()

    def _is_log_date_greater_or_equal_filter_date(self, log: str) -> bool:
        if self._filter_date is not None and self._filter_date_json_path is not None:
            json_log = json.loads(log)
            match = parse(self._filter_date_json_path).find(json_log)

            if not match:
                return True

            log_date = parser.parse(match[0].value)
            filter_date = parser.parse(self._filter_date)

            if log_date < filter_date:
                return False

        return True
