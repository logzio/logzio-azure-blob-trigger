import gzip
import yaml
import os

from io import BytesIO
from src.LogzioShipper.file_parser import FileParser
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.logzio_shipper import LogzioShipper


class TestsUtils:

    CONFIGURATION_FILE = 'tests/config.yaml'

    def __init__(self):
        with open(TestsUtils.CONFIGURATION_FILE, 'r') as yaml_file:
            config = yaml.load(yaml_file, Loader=yaml.FullLoader)

            os.environ['LogzioURL'] = config['logzio']['url']
            os.environ['LogzioToken'] = config['logzio']['token']

    def get_file_stream_and_size(self, file_path: str) -> dict:
        with open(file_path, 'r') as json_file:
            file_data = str.encode(json_file.read())

        file_stream = BytesIO(file_data)

        return {
            "file_stream": file_stream,
            "file_size": len(file_data)
        }

    def get_parsed_logs_num(self, file_parser: FileParser) -> int:
        parsed_logs_num = 0

        for _ in file_parser.parse_file():
            parsed_logs_num += 1

        return parsed_logs_num

    def get_stream_logs_num(self, file_stream: BytesIO) -> int:
        logs_num = 0

        for _ in file_stream:
            logs_num += 1

        return logs_num

    def get_sending_file_results(self, file_handler: FileHandler, latest_requests: list) -> dict:
        requests_num = 0
        sent_logs_num = 0
        sent_bytes = 0

        file_handler.handle_file()

        for request in latest_requests:
            requests_num += 1

            for log in gzip.decompress(request.parsed_body).splitlines():
                sent_logs_num += 1
                sent_bytes += len(log)

        return {
            "requests_num": requests_num / 2,
            "sent_logs_num": sent_logs_num / 2,
            "sent_bytes": sent_bytes / 2
        }

    def get_parsed_logs_bytes(self, file_parser: FileParser):
        parsed_logs_bytes = 0

        for log in file_parser.parse_file():
            parsed_logs_bytes += len(log)

        return parsed_logs_bytes

    def add_first_log_to_logzio_shipper(self, file_parser: FileParser, logzio_shipper: LogzioShipper):
        for log in file_parser.parse_file():
            logzio_shipper.add_log_to_send(log)
            break
