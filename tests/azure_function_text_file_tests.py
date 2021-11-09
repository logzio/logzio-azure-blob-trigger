import unittest
import logging
import os
import math

from io import BytesIO
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.text_parser import TextParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


logger = logging.getLogger(__name__)


class TestAzureFunctionTextFile(unittest.TestCase):

    TEXT_LOG_FILE = 'tests/logs/text'
    TEXT_MULTILINE_LOG_FILE = 'tests/logs/text_multiline'
    TEXT_GZ_LOG_FILE = "{}.gz".format(TEXT_LOG_FILE)
    TEXT_MULTILINE_GZ_LOG_FILE = "{}.gz".format(TEXT_MULTILINE_LOG_FILE)
    MULTILINE_REGEX = '(ERROR|INFO):\n[a-zA-Z. ]+'
    BAD_MULTILINE_REGEX = 'WARNING:\n[a-zA-Z. ]+'
    DATETIME_FILTER = '2021-11-01T10:10:10'
    DATETIME_FINDER = '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
    BAD_DATETIME_FINDER = 'date'
    BAD_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    text_stream: BytesIO = None
    text_size = 0
    text_multiline_stream: BytesIO = None
    text_multiline_size = 0
    text_gz_stream: BytesIO = None
    text_multiline_gz_stream: BytesIO = None

    @classmethod
    def setUpClass(cls) -> None:
        TestsUtils.set_up(FileHandler.TEXT_FORMAT_VALUE)

        cls.text_stream, cls.text_size = TestsUtils.get_file_stream_and_size(cls.TEXT_LOG_FILE)
        cls.text_multiline_stream, cls.text_multiline_size = TestsUtils.get_file_stream_and_size(
            cls.TEXT_MULTILINE_LOG_FILE)
        cls.text_gz_stream = TestsUtils.get_gz_file_stream(cls.text_stream)
        cls.text_multiline_gz_stream = TestsUtils.get_gz_file_stream(cls.text_multiline_stream)

    def setUp(self) -> None:
        self.tests_utils = TestsUtils()

        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FILTER_VALUE
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FINDER_VALUE
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = FileHandler.NO_DATETIME_FORMAT_VALUE

        TestAzureFunctionTextFile.text_stream.seek(0)
        TestAzureFunctionTextFile.text_multiline_stream.seek(0)
        TestAzureFunctionTextFile.text_gz_stream.seek(0)
        TestAzureFunctionTextFile.text_multiline_gz_stream.seek(0)

    def test_parse_text_file(self) -> None:
        text_parser = TextParser(TestAzureFunctionTextFile.text_stream, multiline_regex=None)
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(text_parser, TestAzureFunctionTextFile.text_stream)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_stream)

        self.assertEqual(stream_logs_num, parsed_logs_num)

    def test_parse_text_multiline_file(self) -> None:
        text_multiline_parser = TextParser(TestAzureFunctionTextFile.text_multiline_stream,
                                           TestAzureFunctionTextFile.MULTILINE_REGEX)
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(text_multiline_parser,
                                                               TestAzureFunctionTextFile.text_multiline_stream)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_multiline_stream) / 2

        self.assertEqual(stream_logs_num, parsed_logs_num)

    def test_send_text_data(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = FileHandler.NO_MULTILINE_REGEX_VALUE

        text_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionTextFile.TEXT_LOG_FILE,
                                                                 TestAzureFunctionTextFile.text_stream,
                                                                 TestAzureFunctionTextFile.text_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(text_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_stream)
        text_parser = TextParser(TestAzureFunctionTextFile.text_stream, multiline_regex=None)
        text_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_file_handler)
        text_bytes = self.tests_utils.get_parsed_logs_bytes(text_parser, TestAzureFunctionTextFile.text_stream)
        text_bytes += stream_logs_num * text_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(text_bytes, sent_bytes)

    def test_send_text_multiline_data(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = TestAzureFunctionTextFile.MULTILINE_REGEX

        text_multiline_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE,
            TestAzureFunctionTextFile.text_multiline_stream,
            TestAzureFunctionTextFile.text_multiline_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(text_multiline_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_multiline_stream) / 2
        text_multiline_parser = TextParser(TestAzureFunctionTextFile.text_multiline_stream,
                                           TestAzureFunctionTextFile.MULTILINE_REGEX)
        text_multiline_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_multiline_file_handler)
        text_bytes = self.tests_utils.get_parsed_logs_bytes(text_multiline_parser,
                                                            TestAzureFunctionTextFile.text_multiline_stream)
        text_bytes += stream_logs_num * text_multiline_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(text_bytes, sent_bytes)

    def test_send_text_bad_multiline_data(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = TestAzureFunctionTextFile.BAD_MULTILINE_REGEX

        text_multiline_bad_regex_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE,
            TestAzureFunctionTextFile.text_multiline_stream,
            TestAzureFunctionTextFile.text_multiline_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(text_multiline_bad_regex_file_handler)

        self.assertEqual(0, requests_num)
        self.assertEqual(0, sent_logs_num)
        self.assertEqual(0, sent_bytes)

    def test_send_text_gz_data(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = FileHandler.NO_MULTILINE_REGEX_VALUE

        text_gz_file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_GZ_LOG_FILE,
                                           TestAzureFunctionTextFile.text_gz_stream,
                                           TestAzureFunctionTextFile.text_size)
        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(text_gz_file_handler)
        text_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_gz_file_handler)
        gz_sent_bytes -= gz_sent_logs_num * text_gz_file_custom_fields_bytes

        text_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionTextFile.TEXT_LOG_FILE,
                                                                 TestAzureFunctionTextFile.text_stream,
                                                                 TestAzureFunctionTextFile.text_size)
        _, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(text_file_handler)
        text_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_file_handler)
        regular_sent_bytes -= regular_sent_logs_num * text_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)

    def test_send_text_multiline_gz_data(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = TestAzureFunctionTextFile.MULTILINE_REGEX

        text_multiline_gz_file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_MULTILINE_GZ_LOG_FILE,
                                                     TestAzureFunctionTextFile.text_multiline_gz_stream,
                                                     TestAzureFunctionTextFile.text_multiline_size)
        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(text_multiline_gz_file_handler)
        text_multiline_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_multiline_gz_file_handler)
        gz_sent_bytes -= gz_sent_logs_num * text_multiline_gz_file_custom_fields_bytes

        text_multiline_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE,
            TestAzureFunctionTextFile.text_multiline_stream,
            TestAzureFunctionTextFile.text_multiline_size)
        regular_requests_num, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(
            text_multiline_file_handler)
        text_multiline_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_multiline_file_handler)
        regular_sent_bytes -= regular_sent_logs_num * text_multiline_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)

    def test_datetime_filter(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = FileHandler.NO_MULTILINE_REGEX_VALUE
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionTextFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionTextFile.DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionTextFile.DATETIME_FORMAT

        text_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionTextFile.TEXT_LOG_FILE,
                                                                 TestAzureFunctionTextFile.text_stream,
                                                                 TestAzureFunctionTextFile.text_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(text_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_stream)
        text_parser = TextParser(TestAzureFunctionTextFile.text_stream, multiline_regex=None)
        text_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_file_handler)
        text_bytes = self.tests_utils.get_parsed_logs_bytes(text_parser, TestAzureFunctionTextFile.text_stream)
        text_bytes += stream_logs_num * text_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 5, sent_logs_num)
        self.assertNotEqual(text_bytes, sent_bytes)

    def test_bad_datetime_finder(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = FileHandler.NO_MULTILINE_REGEX_VALUE
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionTextFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionTextFile.BAD_DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionTextFile.DATETIME_FORMAT

        text_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionTextFile.TEXT_LOG_FILE,
                                                                 TestAzureFunctionTextFile.text_stream,
                                                                 TestAzureFunctionTextFile.text_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(text_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_stream)
        text_parser = TextParser(TestAzureFunctionTextFile.text_stream, multiline_regex=None)
        text_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_file_handler)
        text_bytes = self.tests_utils.get_parsed_logs_bytes(text_parser, TestAzureFunctionTextFile.text_stream)
        text_bytes += stream_logs_num * text_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(text_bytes, sent_bytes)

    def test_bad_datetime_format(self) -> None:
        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = FileHandler.NO_MULTILINE_REGEX_VALUE
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionTextFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionTextFile.DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionTextFile.BAD_DATETIME_FORMAT

        text_file_handler = self.tests_utils.create_file_handler(TestAzureFunctionTextFile.TEXT_LOG_FILE,
                                                                 TestAzureFunctionTextFile.text_stream,
                                                                 TestAzureFunctionTextFile.text_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(text_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_stream)
        text_parser = TextParser(TestAzureFunctionTextFile.text_stream, multiline_regex=None)
        text_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(text_file_handler)
        text_bytes = self.tests_utils.get_parsed_logs_bytes(text_parser, TestAzureFunctionTextFile.text_stream)
        text_bytes += stream_logs_num * text_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(text_bytes, sent_bytes)


if __name__ == '__main__':
    unittest.main()
