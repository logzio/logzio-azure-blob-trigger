import unittest
import logging
import os
import httpretty
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

        self.tests_utils.reset_file_streams_position([TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                                      TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                                      TestAzureFunctionCsvFile.csv_comma_delimiter_gz_stream,
                                                      TestAzureFunctionCsvFile.csv_semicolon_delimiter_gz_stream])

        self.csv_comma_file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_LOG_FILE,
                                                  TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                                  TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        self.csv_comma_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(
            self.csv_comma_file_handler)
        self.csv_semicolon_file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER_FILE,
                                                      TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                                      TestAzureFunctionCsvFile.csv_semicolon_delimiter_size)
        self.csv_semicolon_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(
            self.csv_semicolon_file_handler)
        self.csv_comma_gz_file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER_GZ_LOG_FILE,
                                                     TestAzureFunctionCsvFile.csv_comma_delimiter_gz_stream,
                                                     TestAzureFunctionCsvFile.csv_comma_delimiter_size)
        self.csv_comma_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(
            self.csv_comma_gz_file_handler)
        self.csv_semicolon_gz_file_handler = FileHandler(TestAzureFunctionCsvFile.CSV_SEMICOLON_GZ_LOG_FILE,
                                                         TestAzureFunctionCsvFile.csv_semicolon_delimiter_gz_stream,
                                                         TestAzureFunctionCsvFile.csv_semicolon_delimiter_size)
        self.csv_semicolon_gz_file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(
            self.csv_semicolon_gz_file_handler)
        self.csv_comma_parser = CsvParser(TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                          TestAzureFunctionCsvFile.CSV_COMMA_DELIMITER)
        self.csv_semicolon_parser = CsvParser(TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                              TestAzureFunctionCsvFile.CSV_SEMICOLON_DELIMITER)

        self.tests_utils.reset_file_streams_position([TestAzureFunctionCsvFile.csv_comma_delimiter_stream,
                                                      TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream,
                                                      TestAzureFunctionCsvFile.csv_comma_delimiter_gz_stream,
                                                      TestAzureFunctionCsvFile.csv_semicolon_delimiter_gz_stream])

    def test_identify_csv_comma_delimiter_file(self) -> None:
        self.assertEqual(CsvParser, type(self.csv_comma_file_handler.file_parser))
        self.assertEqual(CsvParser, type(self.csv_comma_gz_file_handler.file_parser))

    def test_identify_csv_semicolon_delimiter_file(self) -> None:
        self.assertEqual(CsvParser, type(self.csv_semicolon_file_handler.file_parser))
        self.assertEqual(CsvParser, type(self.csv_semicolon_gz_file_handler.file_parser))

    def test_parse_csv_comma_delimiter_file(self) -> None:
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(self.csv_comma_parser)

        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(
            TestAzureFunctionCsvFile.csv_comma_delimiter_stream)

        self.assertEqual(stream_logs_num - 1, parsed_logs_num)

    def test_parse_csv_semicolon_delimiter_file(self) -> None:
        parsed_logs_num = self.tests_utils.get_parsed_logs_num(self.csv_semicolon_parser)

        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(
            TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)

        self.assertEqual(stream_logs_num - 1, parsed_logs_num)

    @httpretty.activate
    def test_send_csv_comma_delimiter_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(
            self.csv_comma_file_handler, httpretty.latest_requests())

        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionCsvFile.csv_comma_delimiter_stream)

        TestAzureFunctionCsvFile.csv_comma_delimiter_stream.seek(0)
        csv_bytes = self.tests_utils.get_parsed_logs_bytes(self.csv_comma_parser)
        csv_bytes += (stream_logs_num - 1) * self.csv_comma_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 1, sent_logs_num)
        self.assertEqual(csv_bytes, sent_bytes)

    @httpretty.activate
    def test_send_csv_semicolon_delimiter_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(
            self.csv_semicolon_file_handler, httpretty.latest_requests())

        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream)

        TestAzureFunctionCsvFile.csv_semicolon_delimiter_stream.seek(0)
        csv_bytes = self.tests_utils.get_parsed_logs_bytes(self.csv_semicolon_parser)
        csv_bytes += (stream_logs_num - 1) * self.csv_semicolon_file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num - 1, sent_logs_num)
        self.assertEqual(csv_bytes, sent_bytes)

    @httpretty.activate
    def test_send_csv_comma_delimiter_gz_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(
            self.csv_comma_gz_file_handler, httpretty.latest_requests())
        gz_sent_bytes -= gz_sent_logs_num * self.csv_comma_gz_file_custom_fields_bytes

        httpretty.latest_requests().clear()
        regular_requests_num, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(
            self.csv_comma_file_handler, httpretty.latest_requests())
        regular_sent_bytes -= regular_sent_logs_num * self.csv_comma_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)

    @httpretty.activate
    def test_send_csv_semicolon_delimiter_gz_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        _, gz_sent_logs_num, gz_sent_bytes = self.tests_utils.get_sending_file_results(
            self.csv_semicolon_gz_file_handler, httpretty.latest_requests())
        gz_sent_bytes -= gz_sent_logs_num * self.csv_semicolon_gz_file_custom_fields_bytes

        httpretty.latest_requests().clear()
        regular_requests_num, regular_sent_logs_num, regular_sent_bytes = self.tests_utils.get_sending_file_results(
            self.csv_semicolon_file_handler, httpretty.latest_requests())
        regular_sent_bytes -= regular_sent_logs_num * self.csv_semicolon_file_custom_fields_bytes

        self.assertEqual(regular_sent_logs_num, gz_sent_logs_num)
        self.assertEqual(regular_sent_bytes, gz_sent_bytes)


if __name__ == '__main__':
    unittest.main()
