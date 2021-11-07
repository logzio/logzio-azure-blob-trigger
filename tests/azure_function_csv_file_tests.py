import unittest
import logging
import os
import math

from io import BytesIO
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.csv_parser import CsvParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


logger = logging.getLogger(__name__)


class TestAzureFunctionCsvFile(unittest.TestCase):

    CSV_COMMA_DELIMITER_LOG_FILE = 'tests/logs/csv_comma_delimiter'
    CSV_SEMICOLON_DELIMITER_FILE = 'tests/logs/csv_semicolon_delimiter'
    CSV_COMMA_DELIMITER_GZ_LOG_FILE = "{}.gz".format(CSV_COMMA_DELIMITER_LOG_FILE)
    CSV_SEMICOLON_GZ_LOG_FILE = "{}.gz".format(CSV_SEMICOLON_DELIMITER_FILE)
    CSV_COMMA_DELIMITER = ','
    CSV_SEMICOLON_DELIMITER = ';'
    DATETIME_FILTER = '2021-11-01T10:10:10'
    DATETIME_FINDER = 'datetime'
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
    BAD_DATETIME_FINDER = 'date'
    BAD_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    csv_comma_delimiter_stream: BytesIO = None
    csv_comma_delimiter_size = 0
    csv_semicolon_delimiter_stream: BytesIO = None
    csv_semicolon_delimiter_size = 0
    csv_comma_delimiter_gz_stream: BytesIO = None
    csv_semicolon_delimiter_gz_stream: BytesIO = None

    @classmethod
    def setUpClass(cls) -> None:
        TestsUtils.set_up(FileHandler.CSV_FORMAT_VALUE)

        cls.csv_comma_delimiter_stream, cls.csv_comma_delimiter_size = TestsUtils.get_file_stream_and_size(
            cls.CSV_COMMA_DELIMITER_LOG_FILE)
        cls.csv_semicolon_delimiter_stream, cls.csv_semicolon_delimiter_size = TestsUtils.get_file_stream_and_size(
            cls.CSV_SEMICOLON_DELIMITER_FILE)
        cls.csv_comma_delimiter_gz_stream = TestsUtils.get_gz_file_stream(cls.csv_comma_delimiter_stream)
        cls.csv_semicolon_delimiter_gz_stream = TestsUtils.get_gz_file_stream(cls.csv_semicolon_delimiter_stream)

    def setUp(self) -> None:
        self.tests_utils = TestsUtils()

        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FILTER_VALUE
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = FileHandler.NO_DATETIME_FINDER_VALUE
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = FileHandler.NO_DATETIME_FORMAT_VALUE

        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)
        TestAzureFunctionCsvFile.csv_comma_delimiter_gz_stream.seek(0)
        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)

    def test_parse_csv_comma_delimiter_file(self) -> None:
        csv_comma_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                     TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER)
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(csv_comma_parser,
                                                               TestAzureFunctionCsvFile.csv_comma_delimiter_stream)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream)

        self.assertEqual(stream_logs_num - 1, parsed_logs_num)

    def test_parse_csv_semicolon_delimiter_file(self) -> None:
        csv_semicolon_parser = CsvParser(TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                         TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER)
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(csv_semicolon_parser,
                                                               TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)

        self.assertEqual(stream_logs_num - 1, parsed_logs_num)

    def test_send_csv_comma_delimiter_data(self) -> None:
        csv_comma_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
            TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(csv_comma_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_comma_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                     TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER)
        csv_comma_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_comma_file_handler)
        csv_bytes = self.tests_utils.get_parsed_logs_bytes(csv_comma_parser,
                                                           TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_bytes += (stream_logs_num - 1) * csv_comma_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 1, sent_logs_num)
        self.assertEqual(csv_bytes, sent_bytes)

    def test_send_csv_semicolon_delimiter_data(self) -> None:
        csv_semicolon_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER_FILE,
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(csv_semicolon_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)
        csv_semicolon_parser = CsvParser(TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                         TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER)
        csv_semicolon_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_semicolon_file_handler)
        csv_bytes = self.tests_utils.get_parsed_logs_bytes(csv_semicolon_parser,
                                                           TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)
        csv_bytes += (stream_logs_num - 1) * csv_semicolon_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 1, sent_logs_num)
        self.assertEqual(csv_bytes, sent_bytes)

    def test_send_csv_comma_delimiter_gz_data(self) -> None:
        csv_comma_gz_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_GZ_LOG_FILE,
            TestAzureFunctionCsvFile.csv_comma_delimiter_gz_stream,
            TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        csv_comma_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_comma_gz_file_handler)
        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(csv_comma_gz_file_handler)
        gz_sent_bytes -= gz_sent_logs_num * csv_comma_gz_file_custom_fields_bytes

        csv_comma_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
            TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        csv_comma_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_comma_file_handler)
        _, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(csv_comma_file_handler)
        regular_sent_bytes -= regular_sent_logs_num * csv_comma_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)

    def test_send_csv_semicolon_delimiter_gz_data(self) -> None:
        csv_semicolon_gz_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_SEMICOLON_GZ_LOG_FILE,
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_gz_stream,
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_size)
        csv_semicolon_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_semicolon_gz_file_handler)
        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(csv_semicolon_gz_file_handler)
        gz_sent_bytes -= gz_sent_logs_num * csv_semicolon_gz_file_custom_fields_bytes

        csv_semicolon_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER_FILE,
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_size)
        csv_semicolon_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_semicolon_file_handler)
        _, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(csv_semicolon_file_handler)
        regular_sent_bytes -= regular_sent_logs_num * csv_semicolon_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)

    def test_datetime_filter(self) -> None:
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionCsvFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionCsvFile.DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionCsvFile.DATETIME_FORMAT

        csv_comma_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
            TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(csv_comma_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_comma_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                     TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER,
                                     TestAzureFunctionCsvFile.DATETIME_FINDER,
                                     TestAzureFunctionCsvFile.DATETIME_FORMAT)
        csv_comma_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_comma_file_handler)
        csv_bytes = self.tests_utils.get_parsed_logs_bytes(csv_comma_parser,
                                                           TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_bytes += (stream_logs_num - 1) * csv_comma_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 1 - 5, sent_logs_num)
        self.assertNotEqual(csv_bytes, sent_bytes)

    def test_bad_datetime_finder(self) -> None:
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionCsvFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionCsvFile.BAD_DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionCsvFile.DATETIME_FORMAT

        csv_comma_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
            TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(csv_comma_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_comma_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                     TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER,
                                     TestAzureFunctionCsvFile.DATETIME_FINDER,
                                     TestAzureFunctionCsvFile.DATETIME_FORMAT)
        csv_comma_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_comma_file_handler)
        csv_bytes = self.tests_utils.get_parsed_logs_bytes(csv_comma_parser,
                                                           TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_bytes += (stream_logs_num - 1) * csv_comma_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 1, sent_logs_num)
        self.assertEqual(csv_bytes, sent_bytes)

    def test_bad_datetime_format(self) -> None:
        os.environ[FileHandler.DATETIME_FILTER_ENVIRON_NAME] = TestAzureFunctionCsvFile.DATETIME_FILTER
        os.environ[FileHandler.DATETIME_FINDER_ENVIRON_NAME] = TestAzureFunctionCsvFile.DATETIME_FINDER
        os.environ[FileHandler.DATETIME_FORMAT_ENVIRON_NAME] = TestAzureFunctionCsvFile.BAD_DATETIME_FORMAT

        csv_comma_file_handler = self.tests_utils.create_file_handler(
            TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
            TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(csv_comma_file_handler)

        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_comma_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                     TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER,
                                     TestAzureFunctionCsvFile.DATETIME_FINDER,
                                     TestAzureFunctionCsvFile.DATETIME_FORMAT)
        csv_comma_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(csv_comma_file_handler)
        csv_bytes = self.tests_utils.get_parsed_logs_bytes(csv_comma_parser,
                                                           TestAzureFunctionCsvFile.csv_comma_delimiter_stream)
        csv_bytes += (stream_logs_num - 1) * csv_comma_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 1, sent_logs_num)
        self.assertEqual(csv_bytes, sent_bytes)


if __name__ == '__main__':
    unittest.main()
