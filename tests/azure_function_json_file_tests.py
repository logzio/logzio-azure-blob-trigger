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
    JSON_GZ_LOG_FILE = "{}.gz".format(JSON_LOG_FILE)

    json_stream: BytesIO = None
    json_size = 0
    json_bad_logs_stream: BytesIO = None
    json_bad_logs_size = 0
    json_gz_stream: BytesIO = None

    @classmethod
    def setUpClass(cls) -> None:
        TestsUtils.set_up()

        cls.json_stream, cls.json_size = TestsUtils.get_file_stream_and_size(cls.JSON_LOG_FILE)
        cls.json_bad_logs_stream, cls.json_bad_logs_size = TestsUtils.get_file_stream_and_size(
            cls.JSON_WITH_BAD_LINES_LOG_FILE)
        cls.json_gz_stream = TestsUtils.get_gz_file_stream(cls.json_stream)

    def setUp(self) -> None:
        self.tests_utils = TestsUtils()

        self.tests_utils.reset_file_streams_position([TestAzureFunctionJsonFile.json_stream,
                                                      TestAzureFunctionJsonFile.json_bad_logs_stream,
                                                      TestAzureFunctionJsonFile.json_gz_stream])

        self.json_file_handler = FileHandler(TestAzureFunctionJsonFile.JSON_LOG_FILE,
                                             TestAzureFunctionJsonFile.json_stream,
                                             TestAzureFunctionJsonFile.json_size)
        self.json_bad_logs_file_handler = FileHandler(TestAzureFunctionJsonFile.JSON_WITH_BAD_LINES_LOG_FILE,
                                                      TestAzureFunctionJsonFile.json_bad_logs_stream,
                                                      TestAzureFunctionJsonFile.json_bad_logs_size)
        self.json_gz_file_handler = FileHandler(TestAzureFunctionJsonFile.JSON_GZ_LOG_FILE,
                                                TestAzureFunctionJsonFile.json_gz_stream,
                                                TestAzureFunctionJsonFile.json_size)
        self.json_parser = JsonParser(TestAzureFunctionJsonFile.json_stream)

        self.tests_utils.reset_file_streams_position([TestAzureFunctionJsonFile.json_stream,
                                                      TestAzureFunctionJsonFile.json_bad_logs_stream,
                                                      TestAzureFunctionJsonFile.json_gz_stream])

    def test_identify_json_file(self) -> None:
        self.assertEqual(JsonParser, type(self.json_file_handler.file_parser))
        self.assertEqual(JsonParser, type(self.json_gz_file_handler.file_parser))

    def test_parse_json(self) -> None:
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(self.json_parser)

        TestAzureFunctionJsonFile.json_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_stream)

        self.assertEqual(stream_logs_num, parsed_logs_num)

    @httpretty.activate
    def test_send_json_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(self.json_file_handler,
                                                                                            httpretty.latest_requests())

        TestAzureFunctionJsonFile.json_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_stream)

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(TestAzureFunctionJsonFile.json_size - stream_logs_num + 1, sent_bytes)

    @httpretty.activate
    def test_send_json_data_with_bad_logs(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(
            self.json_bad_logs_file_handler, httpretty.latest_requests())

        TestAzureFunctionJsonFile.json_bad_logs_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_bad_logs_stream)

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertNotEqual(stream_logs_num, sent_logs_num)
        self.assertNotEqual(TestAzureFunctionJsonFile.json_bad_logs_size - stream_logs_num + 1, sent_bytes)

    @httpretty.activate
    def test_send_json_gz_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        gz_requests_num, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(
            self.json_gz_file_handler, httpretty.latest_requests())

        httpretty.latest_requests().clear()
        regular_requests_num, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(
            self.json_file_handler, httpretty.latest_requests())

        self.assertEqual(regular_requests_num, gz_requests_num)
        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)


if __name__ == '__main__':
    unittest.main()
