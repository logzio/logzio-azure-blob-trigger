import unittest
import logging
import requests
import os
import httpretty

from io import BytesIO
from requests.sessions import InvalidSchema
from .tests_utils import TestsUtils
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.json_parser import JsonParser
from src.LogzioShipper.consumer_producer_queues import ConsumerProducerQueues
from src.LogzioShipper.logzio_shipper import LogzioShipper


logger = logging.getLogger(__name__)


class TestAzureFunctionGeneral(unittest.TestCase):

    JSON_DATETIME_LOG_FILE = 'tests/logs/json_general'
    BAD_LOGZIO_URL = 'https://bad.endpoint:1234'
    BAD_URI = 'https:/bad.uri:1234'
    BAD_CONNECTION_ADAPTER_URL = 'bad://connection.adapter:1234'

    json_stream: BytesIO = None
    json_size = 0

    @classmethod
    def setUpClass(cls) -> None:
        TestsUtils.set_up(FileHandler.JSON_FORMAT_VALUE)

        cls.json_stream, cls.json_size = TestsUtils.get_file_stream_and_size(
            cls.JSON_DATETIME_LOG_FILE)

    def setUp(self) -> None:
        self.tests_utils = TestsUtils()

        TestAzureFunctionGeneral.json_stream.seek(0)

        self.file_handler = self.tests_utils.create_file_handler(TestAzureFunctionGeneral.JSON_DATETIME_LOG_FILE,
                                                                 TestAzureFunctionGeneral.json_stream,
                                                                 TestAzureFunctionGeneral.json_size)
        self.file_custom_fields_bytes = self.tests_utils.get_file_custom_fields_bytes(self.file_handler)
        self.file_parser = JsonParser(TestAzureFunctionGeneral.json_stream)

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

        consumer_producer_queues = ConsumerProducerQueues()
        logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME],
                                       consumer_producer_queues,
                                       FileHandler.VERSION)

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, consumer_producer_queues)
        logzio_shipper.run_logzio_shipper()

        self.assertEqual(requests.HTTPError, type(logzio_shipper.exception))

    def test_sending_bad_logzio_url(self) -> None:
        consumer_producer_queues = ConsumerProducerQueues()
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_LOGZIO_URL,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME],
                                       consumer_producer_queues,
                                       FileHandler.VERSION)

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, consumer_producer_queues)
        logzio_shipper.run_logzio_shipper()

        self.assertEqual(requests.ConnectionError, type(logzio_shipper.exception))

    @httpretty.activate
    def test_sending_bad_logzio_token(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME], status=401)

        consumer_producer_queues = ConsumerProducerQueues()
        logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME],
                                       consumer_producer_queues,
                                       FileHandler.VERSION)

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, consumer_producer_queues)
        logzio_shipper.run_logzio_shipper()

        self.assertEqual(requests.HTTPError, type(logzio_shipper.exception))

    def test_sending_bad_uri(self) -> None:
        consumer_producer_queues = ConsumerProducerQueues()
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_URI,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME],
                                       consumer_producer_queues,
                                       FileHandler.VERSION)

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, consumer_producer_queues)
        logzio_shipper.run_logzio_shipper()

        self.assertEqual(requests.exceptions.InvalidURL, type(logzio_shipper.exception))

    def test_sending_bad_connection_adapter(self) -> None:
        consumer_producer_queues = ConsumerProducerQueues()
        logzio_shipper = LogzioShipper(TestAzureFunctionGeneral.BAD_CONNECTION_ADAPTER_URL,
                                       os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME],
                                       consumer_producer_queues,
                                       FileHandler.VERSION)

        self.tests_utils.add_first_log_to_logzio_shipper(self.file_parser, consumer_producer_queues)
        logzio_shipper.run_logzio_shipper()

        self.assertEqual(InvalidSchema, type(logzio_shipper.exception))


if __name__ == '__main__':
    unittest.main()
