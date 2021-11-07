import unittest
import logging
import os
import math

from io import BytesIO
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.json_parser import JsonParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


logger = logging.getLogger(__name__)


class TestAzureFunctionJsonFile(unittest.TestCase):

    JSON_LOG_FILE = 'tests/logs/json'
    JSON_WITH_BAD_LINES_LOG_FILE = 'tests/logs/json_bad_lines'
    JSON_GZ_LOG_FILE = "{}.gz".format(JSON_LOG_FILE)
    DATETIME_FILTER = '2021-11-01T10:10:10'
    DATETIME_FINDER = 'metadata.datetime'
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
    BAD_DATETIME_FINDER = 'metadata.date'
    BAD_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    json_stream: BytesIO = None
    json_size = 0
    json_bad_logs_stream: BytesIO = None
    json_bad_logs_size = 0
    json_gz_stream: BytesIO = None

    @classmethod
    def setUpClass(cls) -> None:
        TestsUtils.set_up(FileHandler.JSON_FORMAT_VALUE)

        cls.json_stream, cls.json_size = TestsUtils.get_file_stream_and_size(cls.JSON_LOG_FILE)
        cls.json_bad_logs_stream, cls.json_bad_logs_size = TestsUtils.get_file_stream_and_size(
            cls.JSON_WITH_BAD_LINES_LOG_FILE)
        cls.json_gz_stream = TestsUtils.get_gz_file_stream(cls.json_stream)

    def setUp(self) -> None:
        self.tests_utils = TestsUtils()

        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FILTER_VALUE
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FINDER_VALUE
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = FileHandler.NO_DATETIME_FORMAT_VALUE

        TestAzureFunctionJsonFile.json_stream.seek(0)
        TestAzureFunctionJsonFile.json_bad_logs_stream.seek(0)
        TestAzureFunctionJsonFile.json_gz_stream.seek(0)

    def test_parse_json(self) -> None:
        json_parser = JsonParser(TestAzureFunctionJsonFile.json_stream)
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(json_parser, TestAzureFunctionJsonFile.json_stream)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_stream)

        self.assertEqual(stream_logs_num, parsed_logs_num)

    def test_send_json_data(self) -> None:
        json_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionJsonFile.JSON_LOG_FILE,
                                                                 TestAzureFunctionJsonFile.json_stream,
                                                                 TestAzureFunctionJsonFile.json_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(json_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_stream)
        json_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(json_file_handler)
        stream_size = TestAzureFunctionJsonFile.json_size - stream_logs_num + 1
        stream_size += stream_logs_num * json_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(stream_size, sent_bytes)

    def test_send_json_data_with_bad_logs(self) -> None:
        json_bad_logs_file_handler = FileHandler(TestAzureFunctionJsonFile.JSON_WITH_BAD_LINES_LOG_FILE,
                                                 TestAzureFunctionJsonFile.json_bad_logs_stream,
                                                 TestAzureFunctionJsonFile.json_bad_logs_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(json_bad_logs_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_bad_logs_stream)
        json_bad_logs_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(json_bad_logs_file_handler)
        stream_size = TestAzureFunctionJsonFile.json_bad_logs_size - stream_logs_num + 1
        stream_size += stream_logs_num * json_bad_logs_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertNotEqual(stream_logs_num, sent_logs_num)
        self.assertNotEqual(TestAzureFunctionJsonFile.json_bad_logs_size - stream_logs_num + 1, sent_bytes)

    def test_send_json_gz_data(self) -> None:
        json_gz_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionJsonFile.JSON_GZ_LOG_FILE,
                                                                    TestAzureFunctionJsonFile.json_gz_stream,
                                                                    TestAzureFunctionJsonFile.json_size)
        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(json_gz_file_handler)
        json_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(json_gz_file_handler)
        gz_sent_bytes -= gz_sent_logs_num * json_gz_file_custom_fields_bytes

        json_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionJsonFile.JSON_LOG_FILE,
                                                                 TestAzureFunctionJsonFile.json_stream,
                                                                 TestAzureFunctionJsonFile.json_size)
        _, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(json_file_handler)
        json_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(json_file_handler)
        regular_sent_bytes -= regular_sent_logs_num * json_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)

    def test_datetime_filter(self) -> None:
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionJsonFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionJsonFile.DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionJsonFile.DATETIME_FORMAT

        json_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionJsonFile.JSON_LOG_FILE,
                                                                 TestAzureFunctionJsonFile.json_stream,
                                                                 TestAzureFunctionJsonFile.json_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(json_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_stream)
        json_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(json_file_handler)
        stream_size = TestAzureFunctionJsonFile.json_size - stream_logs_num + 1
        stream_size += stream_logs_num * json_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 5, sent_logs_num)
        self.assertNotEqual(stream_size, sent_bytes)

    def test_bad_datetime_finder(self) -> None:
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionJsonFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionJsonFile.BAD_DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionJsonFile.DATETIME_FORMAT

        json_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionJsonFile.JSON_LOG_FILE,
                                                                 TestAzureFunctionJsonFile.json_stream,
                                                                 TestAzureFunctionJsonFile.json_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(json_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_stream)
        json_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(json_file_handler)
        stream_size = TestAzureFunctionJsonFile.json_size - stream_logs_num + 1
        stream_size += stream_logs_num * json_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(stream_size, sent_bytes)

    def test_bad_datetime_format(self) -> None:
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionJsonFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionJsonFile.DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionJsonFile.BAD_DATETIME_FORMAT

        json_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionJsonFile.JSON_LOG_FILE,
                                                                 TestAzureFunctionJsonFile.json_stream,
                                                                 TestAzureFunctionJsonFile.json_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(json_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionJsonFile.json_stream)
        json_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(json_file_handler)
        stream_size = TestAzureFunctionJsonFile.json_size - stream_logs_num + 1
        stream_size += stream_logs_num * json_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(stream_size, sent_bytes)


if __name__ == '__main__':
    unittest.main()
