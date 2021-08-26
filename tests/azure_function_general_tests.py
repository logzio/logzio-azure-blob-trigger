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

    tests_utils: TestsUtils = None
    json_stream: BytesIO = None
    json_size = None

    @classmethod
    def setUpClass(cls) -> None:
        TestAzureFunctionGeneral.tests_utils = TestsUtils()

        results = TestAzureFunctionGeneral.tests_utils.get_file_stream_and_size(
            TestAzureFunctionGeneral.JSON_LOG_FILE)
        TestAzureFunctionGeneral.json_stream = results['file_stream']
        TestAzureFunctionGeneral.json_size = results['file_size']

    def setUp(self) -> None:
        TestAzureFunctionGeneral.json_stream.seek(0)

    @httpretty.activate
    def test_send_retry_status_500(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=500)

        FileHandler(TestAzureFunctionGeneral.JSON_LOG_FILE, TestAzureFunctionGeneral.json_stream,
                    TestAzureFunctionGeneral.json_size).handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_502(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=502)

        FileHandler(TestAzureFunctionGeneral.JSON_LOG_FILE, TestAzureFunctionGeneral.json_stream,
                    TestAzureFunctionGeneral.json_size).handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_503(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=503)

        FileHandler(TestAzureFunctionGeneral.JSON_LOG_FILE, TestAzureFunctionGeneral.json_stream,
                    TestAzureFunctionGeneral.json_size).handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_504(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=504)

        FileHandler(TestAzureFunctionGeneral.JSON_LOG_FILE, TestAzureFunctionGeneral.json_stream,
                    TestAzureFunctionGeneral.json_size).handle_file()
        requests_num = len(httpretty.latest_requests()) / 2

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_bad_format(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=400)

        file_parser = JsonParser(TestAzureFunctionGeneral.json_stream)
        logzio_shipper = LogzioShipper(os.environ['LogzioURL'], os.environ['LogzioToken'])

        TestAzureFunctionGeneral.tests_utils.add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_logzio_url(self):
        file_parser = JsonParser(TestAzureFunctionGeneral.json_stream)
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_LOGZIO_URL, os.environ['LogzioToken'])

        TestAzureFunctionGeneral.tests_utils.add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.ConnectionError, logzio_shipper.send_to_logzio)

    def test_sending_bad_logzio_token(self):
        file_parser = JsonParser(TestAzureFunctionGeneral.json_stream)
        logzio_shipper = LogzioShipper(os.environ['LogzioURL'], TestAzureFunctionGeneral.BAD_LOGZIO_TOKEN)

        TestAzureFunctionGeneral.tests_utils.add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_uri(self):
        file_parser = JsonParser(TestAzureFunctionGeneral.json_stream)
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_URI, os.environ['LogzioToken'])

        TestAzureFunctionGeneral.tests_utils.add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.exceptions.InvalidURL, logzio_shipper.send_to_logzio)

    def test_sending_bad_connection_adapter(self):
        file_parser = JsonParser(TestAzureFunctionGeneral.json_stream)
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_CONNECTION_ADAPTER_URL, os.environ['LogzioToken'])

        TestAzureFunctionGeneral.tests_utils.add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(InvalidSchema, logzio_shipper.send_to_logzio)


if __name__ == '__main__':
    unittest.main()
