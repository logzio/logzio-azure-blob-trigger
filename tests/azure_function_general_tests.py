import unittest
import logging
import requests
import os
import httpretty
import math

from io import BytesIO
from requests.sessions import InvalidSchema
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.json_parser import JsonParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


logger = logging.getLogger(__name__)


class TestAzureFunctionGeneral(unittest.TestCase):

    JSON_DATETIME_LOG_FILE = 'tests/logs/json_datetime'
    BAD_LOGZIO_URL = 'https://bad.endpoint:1234'
    BAD_URI = 'https:/bad.uri:1234'
    BAD_CONNECTION_ADAPTER_URL = 'bad://connection.adapter:1234'
    FILTER_DATE = '2021-09-20T10:10:10'
    FILTER_DATE_JSON_PATH = 'datetime'
    BAD_FILTER_DATE_JSON_PATH = 'date'

    json_datetime_stream: BytesIO = None
    json_datetime_size = 0

    @classmethod
    def setUpClass(cls) -> None:
        TestsUtils.set_up(FileHandler.JSON_FORMAT_VALUE)

        os.environ[FileHandler.FILTER_DATE_ENVIRON_NAME] = cls.FILTER_DATE
        cls.json_datetime_stream, cls.json_datetime_size = TestsUtils.get_file_stream_and_size(
            cls.JSON_DATETIME_LOG_FILE)

    def setUp(self) -> None:
        self.tests_utils = TestsUtils()

        self.tests_utils.reset_file_streams_position([TestAzureFunctionGeneral.json_datetime_stream])

        os.environ[FileHandler.FILTER_DATE_JSON_PATH_ENVIRON_NAME] = TestAzureFunctionGeneral.FILTER_DATE_JSON_PATH
        self.file_handler = FileHandler(TestAzureFunctionGeneral.JSON_DATETIME_LOG_FILE,
                                        TestAzureFunctionGeneral.json_datetime_stream,
                                        TestAzureFunctionGeneral.json_datetime_size)
        self.file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(self.file_handler)
        self.file_parser = JsonParser(TestAzureFunctionGeneral.json_datetime_stream)

        TestAzureFunctionGeneral.json_datetime_stream.seek(0)

        os.environ[FileHandler.FILTER_DATE_JSON_PATH_ENVIRON_NAME] = TestAzureFunctionGeneral.BAD_FILTER_DATE_JSON_PATH
        self.file_bad_filter_handler = FileHandler(TestAzureFunctionGeneral.JSON_DATETIME_LOG_FILE,
                                                   TestAzureFunctionGeneral.json_datetime_stream,
                                                   TestAzureFunctionGeneral.json_datetime_size)

        self.tests_utils.reset_file_streams_position([TestAzureFunctionGeneral.json_datetime_stream])

    @httpretty.activate
    def test_send_retry_status_500(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=500)

        try:
            self.file_handler.handle_file()
        except self.file_handler.FailedToSendLogsError:
            pass

        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_502(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=502)

        try:
            self.file_handler.handle_file()
        except self.file_handler.FailedToSendLogsError:
            pass

        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_503(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=503)

        try:
            self.file_handler.handle_file()
        except self.file_handler.FailedToSendLogsError:
            pass

        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_504(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=504)

        try:
            self.file_handler.handle_file()
        except self.file_handler.FailedToSendLogsError:
            pass
        
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_bad_format(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=400)

        logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_logzio_url(self) -> None:
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_LOGZIO_URL,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.ConnectionError, logzio_shipper.send_to_logzio)

    @httpretty.activate
    def test_sending_bad_logzio_token(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=401)

        logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_uri(self) -> None:
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_URI,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.exceptions.InvalidURL, logzio_shipper.send_to_logzio)

    def test_sending_bad_connection_adapter(self) -> None:
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_CONNECTION_ADAPTER_URL,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(InvalidSchema, logzio_shipper.send_to_logzio)

    @httpretty.activate
    def test_filter_date(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(self.file_handler,
                                                                                            httpretty.latest_requests())

        TestAzureFunctionGeneral.json_datetime_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionGeneral.json_datetime_stream)
        stream_size = TestAzureFunctionGeneral.json_datetime_size - stream_logs_num + 1
        stream_size += stream_logs_num * self.file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num / 2, sent_logs_num)
        self.assertNotEqual(stream_size, sent_bytes)

    @httpretty.activate
    def test_bad_filter_date(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=200)

        requests_num, sent_logs_num, sent_bytes = self.tests_utils.get_sending_file_results(
            self.file_bad_filter_handler, httpretty.latest_requests())

        TestAzureFunctionGeneral.json_datetime_stream.seek(0)
        stream_logs_num = self.tests_utils.get_file_stream_logs_num(TestAzureFunctionGeneral.json_datetime_stream)
        stream_size = TestAzureFunctionGeneral.json_datetime_size - stream_logs_num + 1
        stream_size += stream_logs_num * self.file_custom_fields_bytes

        self.assertEqual(math.ceil(sent_bytes / LogzioShipper.MAX_BULK_SIZE_BYTES), requests_num)
        self.assertEqual(stream_logs_num, sent_logs_num)
        self.assertEqual(stream_size, sent_bytes)


if __name__ == '__main__':
    unittest.main()
