import logging
import os
import csv
import zlib
import threading

from typing import Optional, Generator, List
from io import BytesIO, IOBase
from datetime import datetime
from .file_parser import FileParser
from .json_parser import JsonParser
from .csv_parser import CsvParser
from .text_parser import TextParser
from .logs_queue import LogsQueue
from .logzio_shipper import LogzioShipper
from .custom_field import CustomField


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileHandler:

    GZ_MAGIC_NUMBER = b'\x1f\x8b'
    JSON_CHAR = '{'

    FORMAT_ENVIRON_NAME = 'Format'
    LOGZIO_URL_ENVIRON_NAME = 'LogzioURL'
    LOGZIO_TOKEN_ENVIRON_NAME = 'LogzioToken'
    MULTILINE_REGEX_ENVIRON_NAME = 'MultilineRegex'
    DATETIME_FILTER_ENVIRON_NAME = 'DatetimeFilter'
    DATETIME_FINDER_ENVIRON_NAME = 'DatetimeFinder'
    DATETIME_FORMAT_ENVIRON_NAME = 'DatetimeFormat'

    JSON_FORMAT_VALUE = 'JSON'
    CSV_FORMAT_VALUE = 'CSV'
    TEXT_FORMAT_VALUE = 'TEXT'
    NO_MULTILINE_REGEX_VALUE = 'NO_REGEX'
    NO_DATETIME_FILTER_VALUE = 'NO_DATETIME_FILTER'
    NO_DATETIME_FINDER_VALUE = 'NO_DATETIME_FINDER'
    NO_DATETIME_FORMAT_VALUE = 'NO_DATETIME_FORMAT'

    VERSION = '1.0.3'

    def __init__(self, file_name: str, file_stream: IOBase, file_size: int) -> None:
        self._file_name = file_name
        self._file_stream = self._get_seekable_file_stream(file_stream)
        self._file_size = file_size
        self._datetime_format = self._get_datetime_format()
        self._file_parser = self._get_file_parser()
        self._logs_queue = LogsQueue()
        self._logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                             os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME],
                                             self._logs_queue,
                                             FileHandler.VERSION)
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
            raise

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

    def _get_multiline_regex(self) -> Optional[str]:
        multiline_regex = os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME]

        if multiline_regex == FileHandler.NO_MULTILINE_REGEX_VALUE:
            return None

        return multiline_regex

    def _get_datetime_filter(self) -> Optional[str]:
        datetime_filter = os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME]

        if datetime_filter == FileHandler.NO_DATETIME_FILTER_VALUE:
            return None

        return datetime_filter

    def _get_datetime_finder(self) -> Optional[str]:
        datetime_finder = os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME]

        if datetime_finder == FileHandler.NO_DATETIME_FINDER_VALUE:
            return None

        return datetime_finder

    def _get_datetime_format(self) -> Optional[str]:
        datetime_format = os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME]

        if datetime_format == FileHandler.NO_DATETIME_FORMAT_VALUE:
            return None

        return datetime_format

    def _get_file_parser(self) -> FileParser:
        file_format = os.environ[FileHandler.FORMAT_ENVIRON_NAME]
        datetime_finder = self._get_datetime_finder()
        multiline_regex = self._get_multiline_regex()

        logs_sample = [self._file_stream.readline().decode("utf-8").rstrip(),
                       self._file_stream.readline().decode("utf-8").rstrip()]

        self._file_stream.seek(0)

        if file_format == FileHandler.JSON_FORMAT_VALUE:
            if logs_sample[0].startswith(FileHandler.JSON_CHAR):
                return JsonParser(self._file_stream, datetime_finder, self._datetime_format)

            return TextParser(self._file_stream, multiline_regex)

        if file_format == FileHandler.CSV_FORMAT_VALUE:
            delimiter = self._get_csv_delimiter(logs_sample)

            if delimiter is not None:
                return CsvParser(self._file_stream, delimiter, datetime_finder, self._datetime_format)

            return TextParser(self._file_stream, multiline_regex)

        return TextParser(self._file_stream, multiline_regex, datetime_finder, self._datetime_format)

    def _add_custom_fields_to_logzio_shipper(self) -> None:
        for custom_field in self._custom_fields:
            self._logzio_shipper.add_custom_field_to_list(custom_field)

    def _get_csv_delimiter(self, logs_sample: List[str]) -> Optional[str]:
        try:
            dialect = csv.Sniffer().sniff('\n'.join(logs_sample))
            return str(dialect.delimiter)
        except csv.Error:
            logger.error(
                "Could not determine delimiter for the csv file - {} . Using text format instead.".format(
                    self._file_name))

        return None

    def _send_logs_to_logzio(self) -> None:
        datetime_filter = self._get_datetime_filter()

        logzio_shipper_thread = threading.Thread(target=self._logzio_shipper.run_logzio_shipper)
        logzio_shipper_thread.start()

        for log in self._file_parser.parse_file():
            if not self._is_log_datetime_greater_or_equal_datetime_filter(datetime_filter, log):
                logger.info("Log was not sent to Logz.io because of date filter - {}".format(log))
                continue

            self._logs_queue.put_log_into_queue(log)

            if self._logzio_shipper.is_exception_occurred:
                logzio_shipper_thread.join()
                raise self.FailedToSendLogsError("Failed to send logs to Logz.io for {}".format(self._file_name))

        self._logs_queue.put_end_log_into_queue()
        logzio_shipper_thread.join()

        if self._logzio_shipper.is_exception_occurred:
            raise self.FailedToSendLogsError("Failed to send logs to Logz.io for {}".format(self._file_name))

        if not self._file_parser.are_all_logs_parsed or self._logzio_shipper.was_any_log_invalid:
            raise self.FailedToSendLogsError("Some/All logs did not send to Logz.io in {}".format(self._file_name))

    def _is_log_datetime_greater_or_equal_datetime_filter(self, datetime_filter: str, log: str) -> bool:
        if datetime_filter is not None:
            log_datetime = self._file_parser.get_log_datetime(log)
            filter_datetime = datetime.strptime(datetime_filter, self._datetime_format)

            if log_datetime < filter_datetime:
                return False

        return True
