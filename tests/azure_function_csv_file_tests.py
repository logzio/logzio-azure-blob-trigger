import unittest
import logging
import os
import httpretty
import math

from logging.config import fileConfig
from io import BytesIO
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.csv_parser import CsvParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


fileConfig('tests/logging_config.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class TestAzureFunctionCsvFile(unittest.TestCase):

    CSV_COMMA_DELIMITER_LOG_FILE = 'tests/logs/csv_comma_delimiter'
    CSV_SEMICOLON_DELIMITER_FILE = 'tests/logs/csv_semicolon_delimiter'
    CSV_COMMA_DELIMITER = ','
    CSV_SEMICOLON_DELIMITER = ';'

    tests_utils = None
    csv_comma_delimiter_stream: BytesIO = None
    csv_comma_delimiter_size = None
    csv_semicolon_delimiter_stream: BytesIO = None
    csv_semicolon_delimiter_size = None

    @classmethod
    def setUpClass(cls) -> None:
        TestAzureFunctionCsvFile.tests_utils = TestsUtils()

        results = TestAzureFunctionCsvFile.tests_utils.get_file_stream_and_size(
            TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE)
        TestAzureFunctionCsvFile.csv_comma_delimiter_stream = results['file_stream']
        TestAzureFunctionCsvFile.csv_comma_delimiter_size = results['file_size']

        results = TestAzureFunctionCsvFile.tests_utils.get_file_stream_and_size(
            TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER_FILE)
        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream = results['file_stream']
        TestAzureFunctionCsvFile.csv_semicolon_delimiter_size = results['file_size']

    def setUp(self) -> None:
        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)

    def test_identify_csv_comma_delimiter_file(self) -> None:
        file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
                                   TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                   TestAzureFunctionCsvFile.csv_comma_delimiter_size)

        self.assertEqual(CsvParser, type(file_handler.file_parser))

    def test_identify_csv_semicolon_delimiter_file(self) -> None:
        file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER_FILE,
                                   TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                   TestAzureFunctionCsvFile.csv_semicolon_delimiter_size)

        self.assertEqual(CsvParser, type(file_handler.file_parser))

    def test_parse_csv_comma_delimiter_file(self) -> None:
        file_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER)
        parsed_logs_num = TestAzureFunctionCsvFile.tests_utils.get_parsed_logs_num(file_parser)

        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        stream_logs_num = TestAzureFunctionCsvFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream)

        self.assertEqual(stream_logs_num - 1, parsed_logs_num)

    def test_parse_csv_semicolon_delimiter_file(self) -> None:
        file_parser = CsvParser(TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER)
        parsed_logs_num = TestAzureFunctionCsvFile.tests_utils.get_parsed_logs_num(file_parser)

        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)
        stream_logs_num = TestAzureFunctionCsvFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)

        self.assertEqual(stream_logs_num - 1, parsed_logs_num)

    @httpretty.activate
    def test_send_csv_comma_delimiter_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
                                   TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                   TestAzureFunctionCsvFile.csv_comma_delimiter_size)

        results = TestAzureFunctionCsvFile.tests_utils.get_sending_file_results(file_handler,
                                                                                httpretty.latest_requests())

        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        stream_logs_num = TestAzureFunctionCsvFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream)

        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        file_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER)
        csv_bytes = TestAzureFunctionCsvFile.tests_utils.get_parsed_logs_bytes(file_parser)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(stream_logs_num - 1, results['sent_logs_num'])
        self.assertEqual(csv_bytes, results['sent_bytes'])

    @httpretty.activate
    def test_send_csv_semicolon_delimiter_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER_FILE,
                                   TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                   TestAzureFunctionCsvFile.csv_semicolon_delimiter_size)

        results = TestAzureFunctionCsvFile.tests_utils.get_sending_file_results(file_handler,
                                                                                httpretty.latest_requests())

        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)
        stream_logs_num = TestAzureFunctionCsvFile.tests_utils.get_stream_logs_num(
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)

        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)
        file_parser = CsvParser(TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER)
        csv_bytes = TestAzureFunctionCsvFile.tests_utils.get_parsed_logs_bytes(file_parser)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(stream_logs_num - 1, results['sent_logs_num'])
        self.assertEqual(csv_bytes, results['sent_bytes'])


if __name__ == '__main__':
    unittest.main()
