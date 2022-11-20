import logging
import os
import csv
import zlib
import concurrent.futures

from typing import Optional, Generator, List
from io import BytesIO, IOBase
from datetime import datetime
from .file_parser import FileParser
from .json_parser import JsonParser
from .csv_parser import CsvParser
from .text_parser import TextParser
from .consumer_producer_queues import ConsumerProducerQueues
from .logzio_shipper import LogzioShipper
from .custom_field import CustomField


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileHandler:

    GZ_MAGIC_NUMBER = b'\x1f\x8b'

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
        self._datetime_filter = self._get_datetime_filter()
        self._datetime_finder = self._get_datetime_finder()
        self._datetime_format = self._get_datetime_format()
        self._is_default_file_parser = False
        self._file_format = os.environ[FileHandler.FORMAT_ENVIRON_NAME]
        self._file_parser = self._get_file_parser()
        self._consumer_producer_queues = ConsumerProducerQueues()
        self._logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                             os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME],
                                             self._consumer_producer_queues,
                                             FileHandler.VERSION)
        self._custom_fields = [CustomField(field_key='file', field_value=self._file_name)]

        self._add_custom_fields_to_logzio_shipper()

    @property
    def file_parser(self) -> FileParser:
        return self._file_parser

    def get_custom_fields(self) -> Generator:
        for custom_field in self._custom_fields:
            yield custom_field

    class DefaultParserError(Exception):
        pass

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
            if line.decode("utf-8").rstrip() == '':
                continue

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
        logs_sample = [self._file_stream.readline().decode("utf-8").rstrip(),
                       self._file_stream.readline().decode("utf-8").rstrip()]

        self._file_stream.seek(0)

        if self._file_format == FileHandler.JSON_FORMAT_VALUE:
            self._write_is_datetime_filter_enabled()
            return JsonParser(self._file_stream, self._datetime_finder, self._datetime_format)

        if self._file_format == FileHandler.CSV_FORMAT_VALUE:
            delimiter = self._get_csv_delimiter(logs_sample)

            if delimiter is not None:
                self._write_is_datetime_filter_enabled()
                return CsvParser(self._file_stream, delimiter, self._datetime_finder, self._datetime_format)

            logger.info('Datetime filter is disabled.')
            self._is_default_file_parser = True
            return TextParser(self._file_stream)

        multiline_regex = self._get_multiline_regex()
        self._write_is_datetime_filter_enabled()

        return TextParser(self._file_stream, multiline_regex, self._datetime_finder, self._datetime_format)

    def _write_is_datetime_filter_enabled(self) -> None:
        if self._datetime_filter is None or self._datetime_finder is None or self._datetime_format is None:
            logger.info('Datetime filter is disabled.')
        else:
            logger.info('Datetime filter is enabled.')

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
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        executor.submit(self._logzio_shipper.run_logzio_shipper)

        for log in self._file_parser.parse_file():
            if not self._is_log_datetime_greater_or_equal_datetime_filter(self._datetime_filter, log):
                logger.info("Log was not sent to Logz.io because of datetime filter - {}".format(log))
                continue

            self._consumer_producer_queues.put_log_into_queue(log)

            if self._logzio_shipper.exception is not None:
                executor.shutdown()
                self._write_info_logs()
                self._write_error_logs()
                raise self.FailedToSendLogsError("Failed to send logs to Logz.io for {}".format(self._file_name))

        self._consumer_producer_queues.put_end_log_into_queue()
        executor.shutdown()
        self._write_info_logs()
        self._write_error_logs()

        if self._logzio_shipper.exception is not None:
            raise self.FailedToSendLogsError("Failed to send logs to Logz.io for {}".format(self._file_name))

        if not self._file_parser.are_all_logs_parsed or self._logzio_shipper.is_any_log_invalid:
            raise self.FailedToSendLogsError("Some/All logs did not send to Logz.io in {}".format(self._file_name))

        if self._is_default_file_parser:
            raise self.DefaultParserError("The file {0} is not in {1} format. Used text format instead.".format(
                self._file_name, self._file_format))

    def _is_log_datetime_greater_or_equal_datetime_filter(self, datetime_filter: str, log: str) -> bool:
        if datetime_filter is None or self._datetime_format is None:
            return True

        log_datetime = self._file_parser.get_log_datetime(log)

        if log_datetime is None:
            return True

        try:
            filter_datetime = datetime.strptime(datetime_filter, self._datetime_format)
        except ValueError:
            logger.error(
                "datetime filter {0} does not match datetime format {1}".format(datetime_filter, self._datetime_format))
            return True

        if log_datetime < filter_datetime:
            return False

        return True

    def _write_info_logs(self) -> None:
        while True:
            info_message = self._consumer_producer_queues.get_info_from_queue()

            if info_message is None:
                return

            logger.info(info_message)

    def _write_error_logs(self) -> None:
        while True:
            error_message = self._consumer_producer_queues.get_error_from_queue()

            if error_message is None:
                return

            logger.error(error_message)
