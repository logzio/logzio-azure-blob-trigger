import unittest
import logging
import os
import httpretty
import math

from logging.config import fileConfig
from io import BytesIO
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.text_parser import TextParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


fileConfig('tests/logging_config.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class TestAzureFunctionTextFile(unittest.TestCase):

    TEXT_LOG_FILE = 'tests/logs/text'
    TEXT_MULTILINE_LOG_FILE = 'tests/logs/text_multiline'
    MULTILINE_REGEX = '(ERROR|INFO):\n[a-zA-Z. ]+'
    BAD_MULTILINE_REGEX = 'WARNING:\n[a-zA-Z. ]+'

    tests_utils: TestsUtils = None
    text_stream: BytesIO = None
    text_size: int = 0
    text_multiline_stream: BytesIO = None
    text_multiline_size: int = 0

    @classmethod
    def setUpClass(cls) -> None:
        TestAzureFunctionTextFile.tests_utils = TestsUtils()

        results = TestAzureFunctionTextFile.tests_utils.get_file_stream_and_size(
            TestAzureFunctionTextFile.TEXT_LOG_FILE)
        TestAzureFunctionTextFile.text_stream = results['file_stream']
        TestAzureFunctionTextFile.text_size = results['file_size']

        results = TestAzureFunctionTextFile.tests_utils.get_file_stream_and_size(
            TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE)
        TestAzureFunctionTextFile.text_multiline_stream = results['file_stream']
        TestAzureFunctionTextFile.text_multiline_size = results['file_size']

    def setUp(self) -> None:
        TestAzureFunctionTextFile.text_stream.seek(0)
        TestAzureFunctionTextFile.text_multiline_stream.seek(0)
        os.environ['MultilineRegex'] = ''

    def test_identify_text_file(self) -> None:
        file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_LOG_FILE,
                                   TestAzureFunctionTextFile.text_stream,
                                   TestAzureFunctionTextFile.text_size)

        self.assertEqual(TextParser, type(file_handler.file_parser))

    def test_parse_text_file(self) -> None:
        file_parser = TextParser(TestAzureFunctionTextFile.text_stream)
        parsed_logs_num = TestAzureFunctionTextFile.tests_utils.get_parsed_logs_num(file_parser)

        TestAzureFunctionTextFile.text_stream.seek(0)
        stream_logs_num = TestAzureFunctionTextFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionTextFile.text_stream)

        self.assertEqual(stream_logs_num, parsed_logs_num)

    @httpretty.activate
    def test_send_text_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_LOG_FILE, TestAzureFunctionTextFile.text_stream,
                                   TestAzureFunctionTextFile.text_size)

        results = TestAzureFunctionTextFile.tests_utils.get_sending_file_results(file_handler,
                                                                                 httpretty.latest_requests())

        TestAzureFunctionTextFile.text_stream.seek(0)
        stream_logs_num = TestAzureFunctionTextFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionTextFile.text_stream)

        TestAzureFunctionTextFile.text_stream.seek(0)
        file_parser = TextParser(TestAzureFunctionTextFile.text_stream)
        text_bytes = TestAzureFunctionTextFile.tests_utils.get_parsed_logs_bytes(file_parser)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(stream_logs_num, results['sent_logs_num'])
        self.assertEqual(text_bytes, results['sent_bytes'])

    @httpretty.activate
    def test_send_text_multiline_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        os.environ['MultilineRegex'] = TestAzureFunctionTextFile.MULTILINE_REGEX

        file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE, TestAzureFunctionTextFile.text_multiline_stream,
                                   TestAzureFunctionTextFile.text_multiline_size)

        results = TestAzureFunctionTextFile.tests_utils.get_sending_file_results(file_handler,
                                                                                 httpretty.latest_requests())

        TestAzureFunctionTextFile.text_multiline_stream.seek(0)
        stream_logs_num = TestAzureFunctionTextFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionTextFile.text_multiline_stream)

        TestAzureFunctionTextFile.text_multiline_stream.seek(0)
        file_parser = TextParser(TestAzureFunctionTextFile.text_multiline_stream, os.environ['MultilineRegex'])
        text_bytes = TestAzureFunctionTextFile.tests_utils.get_parsed_logs_bytes(file_parser)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(stream_logs_num, results['sent_logs_num'])
        self.assertEqual(text_bytes, results['sent_bytes'] + stream_logs_num / (os.environ['MultilineRegex'].count('\n') + 1))

    @httpretty.activate
    def test_send_text_bad_multiline_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        os.environ['MultilineRegex'] = TestAzureFunctionTextFile.BAD_MULTILINE_REGEX

        file_handler = FileHandler(TestAzureFunctionTextFile.TEXT_MULTILINE_LOG_FILE,
                                   TestAzureFunctionTextFile.text_multiline_stream,
                                   TestAzureFunctionTextFile.text_multiline_size)

        results = TestAzureFunctionTextFile.tests_utils.get_sending_file_results(file_handler,
                                                                                 httpretty.latest_requests())

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(0, results['sent_logs_num'])
        self.assertEqual(0, results['sent_bytes'])


if __name__ == '__main__':
    unittest.main()
