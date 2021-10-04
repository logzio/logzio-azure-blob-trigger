import gzip
import yaml
import os
import json

from typing import Tuple, List
from io import BytesIO
from src.LogzioShipper.file_parser import FileParser
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.logzio_shipper import LogzioShipper


class TestsUtils:

    CONFIGURATION_FILE = 'tests/config.yaml'

    @staticmethod
    def set_up() -> None:
        with open(TestsUtils.CONFIGURATION_FILE, 'r') as yaml_file:
            config = yaml.load(yaml_file, Loader=yaml.FullLoader)

            os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME] = config['logzio']['url']
            os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME] = config['logzio']['token']
            os.environ[FileHandler.FILTER_DATE_ENVIRON_NAME] = FileHandler.NO_FILTER_DATE_VALUE
            os.environ[FileHandler.FILTER_DATE_JSON_PATH_ENVIRON_NAME] = FileHandler.NO_FILTER_DATE_JSON_PATH_VALUE

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

    def get_parsed_logs_num(self, file_parser: FileParser) -> int:
        parsed_logs_num = 0

        for _ in file_parser.parse_file():
            parsed_logs_num += 1

        return parsed_logs_num

    def get_parsed_logs_bytes(self, file_parser: FileParser) -> int:
        parsed_logs_bytes = 0

        for log in file_parser.parse_file():
            parsed_logs_bytes += len(log)

        return parsed_logs_bytes

    def get_file_stream_logs_num(self, file_stream: BytesIO) -> int:
        logs_num = 0

        for _ in file_stream:
            logs_num += 1

        return logs_num

    def get_sending_file_results(self, file_handler: FileHandler, latest_requests: list) -> Tuple[int, int, int]:
        requests_num = 0
        sent_logs_num = 0
        sent_bytes = 0

        try:
            file_handler.handle_file()
        except file_handler.FailedToSendLogsError:
            pass

        for request in latest_requests:
            requests_num += 1

            for log in gzip.decompress(request.parsed_body).splitlines():
                sent_logs_num += 1
                sent_bytes += len(log)

        return int(requests_num / 2), int(sent_logs_num / 2), int(sent_bytes / 2)

    def add_first_log_to_logzio_shipper(self, file_parser: FileParser, logzio_shipper: LogzioShipper) -> None:
        for log in file_parser.parse_file():
            logzio_shipper.add_log_to_send(log)
            break

    def reset_file_streams_position(self, file_streams: List[BytesIO]) -> None:
        for file_stream in file_streams:
            file_stream.seek(0)

    def get_file_custom_fields_bytes(self, file_handler: FileHandler) -> int:
        custom_fields: dict = {}

        for custom_field in file_handler.get_custom_fields():
            custom_fields[custom_field.key] = custom_field.value

        return len(json.dumps(custom_fields))
