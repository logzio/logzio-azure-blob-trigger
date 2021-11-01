import unittest
import logging
import os
import httpretty
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

        self.tests_utils.reset_file_streams_position([TestAzureFunctionTextFile.text_stream,
                                                      TestAzureFunctionTextFile.text_multiline_stream,
                                                      TestAzureFunctionTextFile.text_gz_stream,
                                                      TestAzureFunctionTextFile.text_multiline_gz_stream])

        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = TextParser.NO_REGEX_VALUE
        self.text_file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_LOG_FILE,
                                             TestAzureFunctionTextFile.text_stream,
                                             TestAzureFunctionTextFile.text_size)
        self.text_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(self.text_file_handler)
        self.text_gz_file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_GZ_LOG_FILE,
                                                TestAzureFunctionTextFile.text_gz_stream,
                                                TestAzureFunctionTextFile.text_size)
        self.text_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(self.text_gz_file_handler)

        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = TestAzureFunctionTextFile.MULTILINE_REGEX
        self.text_multiline_file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE,
                                                       TestAzureFunctionTextFile.text_multiline_stream,
                                                       TestAzureFunctionTextFile.text_multiline_size)
        self.text_multiline_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(
            self.text_multiline_file_handler)
        self.text_multiline_gz_file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_MULTILINE_GZ_LOG_FILE,
                                                          TestAzureFunctionTextFile.text_multiline_gz_stream,
                                                          TestAzureFunctionTextFile.text_multiline_size)
        self.text_multiline_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(
            self.text_multiline_gz_file_handler)

        os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME] = TestAzureFunctionTextFile.BAD_MULTILINE_REGEX
        self.text_multiline_bad_regex_file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE,
                                                                 TestAzureFunctionTextFile.text_multiline_stream,
                                                                 TestAzureFunctionTextFile.text_multiline_size)
        self.text_parser = TextParser(TestAzureFunctionTextFile.text_stream, TextParser.NO_REGEX_VALUE)
        self.text_multiline_parser = TextParser(TestAzureFunctionTextFile.text_multiline_stream,
                                                TestAzureFunctionTextFile.MULTILINE_REGEX)

        self.tests_utils.reset_file_streams_position([TestAzureFunctionTextFile.text_stream,
                                                      TestAzureFunctionTextFile.text_multiline_stream,
                                                      TestAzureFunctionTextFile.text_gz_stream,
                                                      TestAzureFunctionTextFile.text_multiline_gz_stream])

    def test_identify_text_file(self) -> None:
        self.assertEqual(TextParser, type(self.text_file_handler.file_parser))
        self.assertEqual(TextParser, type(self.text_gz_file_handler.file_parser))

    def test_identify_text_multiline_file(self) -> None:
        self.assertEqual(TextParser, type(self.text_multiline_file_handler.file_parser))
        self.assertEqual(TextParser, type(self.text_multiline_gz_file_handler.file_parser))

    def test_parse_text_file(self) -> None:
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(self.text_parser)

        TestAzureFunctionTextFile.text_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_stream)

        self.assertEqual(stream_logs_num, parsed_logs_num)

    def test_parse_text_multiline_file(self) -> None:
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(self.text_multiline_parser)

        TestAzureFunctionTextFile.text_multiline_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_multiline_stream) / 2

        self.assertEqual(stream_logs_num, parsed_logs_num)

    @httpretty.activate
    def test_send_text_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(self.text_file_handler,
                                                                                            httpretty.latest_requests())

        TestAzureFunctionTextFile.text_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_stream)

        TestAzureFunctionTextFile.text_stream.seek(0)
        text_bytes = self.tests_utils.get_parsed_logs_bytes(self.text_parser)
        text_bytes += stream_logs_num * self.text_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(text_bytes, sent_bytes)

    @httpretty.activate
    def test_send_text_multiline_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(
            self.text_multiline_file_handler, httpretty.latest_requests())

        TestAzureFunctionTextFile.text_multiline_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionTextFile.text_multiline_stream) / 2

        TestAzureFunctionTextFile.text_multiline_stream.seek(0)
        text_bytes = self.tests_utils.get_parsed_logs_bytes(self.text_multiline_parser)
        text_bytes += stream_logs_num * self.text_multiline_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(text_bytes, sent_bytes)

    @httpretty.activate
    def test_send_text_bad_multiline_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(
            self.text_multiline_bad_regex_file_handler, httpretty.latest_requests())

        self.assertEqual(0, requests_num)
        self.assertEqual(0, sent_logs_num)
        self.assertEqual(0, sent_bytes)

    @httpretty.activate
    def test_send_text_gz_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(
            self.text_gz_file_handler, httpretty.latest_requests())
        gz_sent_bytes -= gz_sent_logs_num * self.text_gz_file_custom_fields_bytes

        httpretty.latest_requests().clear()
        regular_requests_num, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(
            self.text_file_handler, httpretty.latest_requests())
        regular_sent_bytes -= regular_sent_logs_num * self.text_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)

    @httpretty.activate
    def test_send_text_multiline_gz_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(
            self.text_multiline_gz_file_handler, httpretty.latest_requests())
        gz_sent_bytes -= gz_sent_logs_num * self.text_multiline_gz_file_custom_fields_bytes

        httpretty.latest_requests().clear()
        regular_requests_num, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(
            self.text_multiline_file_handler, httpretty.latest_requests())
        regular_sent_bytes -= regular_sent_logs_num * self.text_multiline_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)


if __name__ == '__main__':
    unittest.main()
