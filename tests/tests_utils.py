import logging
import requests_mock
import gzip
import os
import json

from typing import Tuple
from logging.config import fileConfig
from io import BytesIO
from src.LogzioShipper.file_parser import FileParser
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.logs_queue import LogsQueue


fileConfig('tests/logging_config.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class TestsUtils:

    LOGZIO_URL = 'https://listener.logz.io:8071'
    LOGZIO_TOKEN = '123456789a'

    @staticmethod
    def set_up(file_format: str) -> None:
        os.environ[FileHandler.FORMAT_ENVIRON_NAME] = file_format
        os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME] = TestsUtils.LOGZIO_URL
        os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME] = TestsUtils.LOGZIO_TOKEN
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = FileHandler.NO_MULTILINE_REGEX_VALUE
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FILTER_VALUE
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FINDER_VALUE
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = FileHandler.NO_DATETIME_FORMAT_VALUE

    @staticmethod
    def get_file_stream_and_size(file_path: str) -> Tuple[BytesIO, int]:
        with open(file_path, 'r') as json_file:
            file_data = str.encode(json_file.read())

        return BytesIO(file_data), len(file_data)

    @staticmethod
    def get_gz_file_stream(file_stream: BytesIO) -> BytesIO:
        file_stream.seek(0)
        compressed_data = gzip.compress(file_stream.read())

        return BytesIO(compressed_data)

    def get_parsed_logs_num(self, file_parser: FileParser, file_stream: BytesIO) -> int:
        parsed_logs_num = 0

        for _ in file_parser.parse_file():
            parsed_logs_num += 1

        file_stream.seek(0)

        return parsed_logs_num

    def get_parsed_logs_bytes(self, file_parser: FileParser, file_stream: BytesIO) -> int:
        parsed_logs_bytes = 0

        for log in file_parser.parse_file():
            parsed_logs_bytes += len(log)

        file_stream.seek(0)

        return parsed_logs_bytes

    def get_file_stream_logs_num(self, file_stream: BytesIO) -> int:
        logs_num = 0

        for _ in file_stream:
            logs_num += 1

        file_stream.seek(0)

        return logs_num

    def get_sending_file_results(self, file_handler: FileHandler) -> Tuple[int, int, int]:
        requests_num = 0
        sent_logs_num = 0
        sent_bytes = 0

        with requests_mock.Mocker() as mocker:
            mocker.register_uri(method='POST', url=os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status_code=200)

            try:
                file_handler.handle_file()
            except file_handler.FailedToSendLogsError:
                pass

        for request in mocker.request_history:
            requests_num += 1

            for log in gzip.decompress(request.body).splitlines():
                sent_logs_num += 1
                sent_bytes += len(log)

        return requests_num, sent_logs_num, sent_bytes

    def add_first_log_to_logzio_shipper(self, file_parser: FileParser, logs_queue: LogsQueue) -> None:
        for log in file_parser.parse_file():
            logs_queue.put_log_into_queue(log)
            break

        logs_queue.put_end_log_into_queue()

    def get_file_custom_fields_bytes(self, file_handler: FileHandler) -> int:
        custom_fields: dict = {}

        for custom_field in file_handler.get_custom_fields():
            custom_fields[custom_field.key] = custom_field.value

        return len(json.dumps(custom_fields))

    def create_file_handler(self, file_name: str, file_stream: BytesIO, file_size: int) -> FileHandler:
        file_handler = FileHandler(file_name, file_stream, file_size)

        file_stream.seek(0)

        return file_handler
