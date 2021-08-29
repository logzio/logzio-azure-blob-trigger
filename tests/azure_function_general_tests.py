import unittest
import logging
import requests
import os
import httpretty

from logging.config import fileConfig
from io import BytesIO
from requests.sessions import InvalidSchema
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.json_parser import JsonParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


fileConfig('tests/logging_config.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class TestAzureFunctionGeneral(unittest.TestCase):

    JSON_LOG_FILE = 'tests/logs/json'
    BAD_LOGZIO_URL = 'https://bad.endpoint:1234'
    BAD_LOGZIO_TOKEN = '123456789'
    BAD_URI = 'https:/bad.uri:1234'
    BAD_CONNECTION_ADAPTER_URL = 'bad://connection.adapter:1234'

    json_stream: BytesIO = None
    json_size = 0

    @classmethod
    def setUpClass(cls) -> None:
        TestsUtils.set_up()

        cls.json_stream, cls.json_size = TestsUtils.get_file_stream_and_size(cls.JSON_LOG_FILE)

    def setUp(self) -> None:
        self.tests_utils = TestsUtils()

        self.tests_utils.reset_file_streams_position([TestAzureFunctionGeneral.json_stream])

        self.file_handler = FileHandler(TestAzureFunctionGeneral.JSON_LOG_FILE, TestAzureFunctionGeneral.json_stream,
                                        TestAzureFunctionGeneral.json_size)
        self.file_parser = JsonParser(TestAzureFunctionGeneral.json_stream)

        self.tests_utils.reset_file_streams_position([TestAzureFunctionGeneral.json_stream])

    @httpretty.activate
    def test_send_retry_status_500(self):
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=500)

        self.file_handler.handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_502(self):
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=502)

        self.file_handler.handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_503(self):
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=503)

        self.file_handler.handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_504(self):
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=504)

        self.file_handler.handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_bad_format(self):
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=400)

        logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_logzio_url(self):
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_LOGZIO_URL,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.ConnectionError, logzio_shipper.send_to_logzio)

    def test_sending_bad_logzio_token(self):
        logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                       TestAzureFunctionGeneral.BAD_LOGZIO_TOKEN)

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_uri(self):
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_URI,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(requests.exceptions.InvalidURL, logzio_shipper.send_to_logzio)

    def test_sending_bad_connection_adapter(self):
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_CONNECTION_ADAPTER_URL,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, logzio_shipper)
        self.assertRaises(InvalidSchema, logzio_shipper.send_to_logzio)


if __name__ == '__main__':
    unittest.main()
