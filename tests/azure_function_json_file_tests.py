import unittest
import logging
import os
import httpretty
import math

from logging.config import fileConfig
from io import BytesIO
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.json_parser import JsonParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


fileConfig('tests/logging_config.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class TestAzureFunctionJsonFile(unittest.TestCase):

    JSON_LOG_FILE = 'tests/logs/json'
    JSON_WITH_BAD_LINES_LOG_FILE = 'tests/logs/json_bad_lines'

    tests_utils = None
    json_stream: BytesIO = None
    json_size = None
    json_bad_logs_stream: BytesIO = None
    json_bad_logs_size = None

    @classmethod
    def setUpClass(cls) -> None:
        TestAzureFunctionJsonFile.tests_utils = TestsUtils()

        results = TestAzureFunctionJsonFile.tests_utils.get_file_stream_and_size(TestAzureFunctionJsonFile.JSON_LOG_FILE)
        TestAzureFunctionJsonFile.json_stream = results['file_stream']
        TestAzureFunctionJsonFile.json_size = results['file_size']

        results = TestAzureFunctionJsonFile.tests_utils.get_file_stream_and_size(TestAzureFunctionJsonFile.JSON_WITH_BAD_LINES_LOG_FILE)
        TestAzureFunctionJsonFile.json_bad_lines_stream = results['file_stream']
        TestAzureFunctionJsonFile.json_bad_lines_size = results['file_size']

    def setUp(self) -> None:
        TestAzureFunctionJsonFile.json_stream.seek(0)
        TestAzureFunctionJsonFile.json_bad_logs_stream.seek(0)

    def test_identify_json_file(self) -> None:
        file_handler = FileHandler(TestAzureFunctionJsonFile.JSON_LOG_FILE,
                                   TestAzureFunctionJsonFile.json_stream,
                                   TestAzureFunctionJsonFile.json_size)

        self.assertEqual(JsonParser, type(file_handler.file_parser))

    def test_parse_json(self) -> None:
        file_parser = JsonParser(TestAzureFunctionJsonFile.json_stream)
        parsed_logs_num = TestAzureFunctionJsonFile.tests_utils.get_parsed_logs_num(file_parser)

        TestAzureFunctionJsonFile.json_stream.seek(0)
        stream_logs_num = TestAzureFunctionJsonFile.tests_utils.get_stream_logs_num(TestAzureFunctionJsonFile.json_stream)

        self.assertEqual(stream_logs_num, parsed_logs_num)

    @httpretty.activate
    def test_send_json_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunctionJsonFile.JSON_LOG_FILE, TestAzureFunctionJsonFile.json_stream,
                                   TestAzureFunctionJsonFile.json_size)

        results = TestAzureFunctionJsonFile.tests_utils.get_sending_file_results(file_handler,
                                                                                 httpretty.latest_requests())

        TestAzureFunctionJsonFile.json_stream.seek(0)
        stream_logs_num = TestAzureFunctionJsonFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionJsonFile.json_stream)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(stream_logs_num, results['sent_logs_num'])
        self.assertEqual(TestAzureFunctionJsonFile.json_size - stream_logs_num + 1, results['sent_bytes'])

    @httpretty.activate
    def test_send_json_data_with_bad_logs(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunctionJsonFile.JSON_WITH_BAD_LINES_LOG_FILE,
                                   TestAzureFunctionJsonFile.json_bad_logs_stream,
                                   TestAzureFunctionJsonFile.json_bad_logs_size)

        results = TestAzureFunctionJsonFile.tests_utils.get_sending_file_results(file_handler,
                                                                                 httpretty.latest_requests())

        TestAzureFunctionJsonFile.json_bad_logs_stream.seek(0)
        stream_logs_num = TestAzureFunctionJsonFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionJsonFile.json_bad_logs_stream)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertNotEqual(stream_logs_num, results['sent_logs_num'])
        self.assertNotEqual(TestAzureFunctionJsonFile.json_bad_logs_size - stream_logs_num + 1, results['sent_bytes'])


if __name__ == '__main__':
    unittest.main()
