import unittest
import logging
import requests
import yaml
import os
import httpretty
import math

from logging.config import fileConfig
from requests.sessions import InvalidSchema
from src.LogzioShipper.file_handler import FileHandler
from src.LogzioShipper.file_parser import FileParser
from src.LogzioShipper.json_parser import JsonParser
from src.LogzioShipper.csv_parser import CsvParser
from src.LogzioShipper.text_parser import TextParser
from src.LogzioShipper.logzio_shipper import LogzioShipper


fileConfig('logging_config.ini')
logger = logging.getLogger(__name__)


class TestAzureFunction(unittest.TestCase):

    CONFIGURATION_FILE = 'config.yaml'
    JSON_LOG_FILE = 'logs/json'
    JSON_WITH_BAD_LINES_LOG_FILE = 'logs/json_bad_lines'
    CSV_COMMA_DELIMITER_LOG_FILE = 'logs/csv_comma_delimiter'
    CSV_SEMICOLON_DELIMITER_FILE = 'logs/csv_semicolon_delimiter'
    TEXT_LOG_FILE = 'logs/text'

    CSV_COMMA_DELIMITER = ','
    CSV_SEMICOLON_DELIMITER = ';'

    BAD_LOGZIO_URL = 'https://bad.endpoint:1234'
    BAD_LOGZIO_TOKEN = '123456789'
    BAD_URI = 'https:/bad.uri:1234'
    BAD_CONNECTION_ADAPTER_URL = 'bad://connection.adapter:1234'

    json_data = None
    json_bad_lines_data = None
    csv_comma_delimiter_data = None
    csv_semicolon_delimiter_data = None
    text_data = None

    @classmethod
    def setUpClass(cls) -> None:
        with open(TestAzureFunction.CONFIGURATION_FILE, 'r') as yaml_file:
            config = yaml.load(yaml_file, Loader=yaml.FullLoader)

            os.environ['LogzioURL'] = config['logzio']['url']
            os.environ['LogzioToken'] = config['logzio']['token']
        
        with open(TestAzureFunction.JSON_LOG_FILE, 'r') as json_file:
            TestAzureFunction.json_data = json_file.read()

        with open(TestAzureFunction.JSON_WITH_BAD_LINES_LOG_FILE, 'r') as json_file:
            TestAzureFunction.json_bad_lines_data = json_file.read()

        with open(TestAzureFunction.CSV_COMMA_DELIMITER_LOG_FILE, 'r') as csv_file:
            TestAzureFunction.csv_comma_delimiter_data = csv_file.read()

        with open(TestAzureFunction.CSV_SEMICOLON_DELIMITER_FILE, 'r') as csv_file:
            TestAzureFunction.csv_semicolon_delimiter_data = csv_file.read()

        with open(TestAzureFunction.TEXT_LOG_FILE, 'r') as text_file:
            TestAzureFunction.text_data = text_file.read()

    def test_identify_json_file(self) -> None:
        file_handler = FileHandler(TestAzureFunction.JSON_LOG_FILE, TestAzureFunction.json_data)

        self.assertEqual(JsonParser, type(file_handler.file_parser))

    def test_identify_csv_comma_delimiter_file(self) -> None:
        file_handler = FileHandler(TestAzureFunction.CSV_COMMA_DELIMITER_LOG_FILE, TestAzureFunction.csv_comma_delimiter_data)

        self.assertEqual(CsvParser, type(file_handler.file_parser))

    def test_identify_csv_semicolon_delimiter_file(self) -> None:
        file_handler = FileHandler(TestAzureFunction.CSV_SEMICOLON_DELIMITER_FILE, TestAzureFunction.csv_semicolon_delimiter_data)

        self.assertEqual(CsvParser, type(file_handler.file_parser))

    def test_identify_text_file(self) -> None:
        file_handler = FileHandler(TestAzureFunction.TEXT_LOG_FILE, TestAzureFunction.text_data)

        self.assertEqual(TextParser, type(file_handler.file_parser))

    def test_parse_json(self) -> None:
        file_parser = JsonParser(TestAzureFunction.json_data)
        parsed_lines = self.__get_parsed_lines(file_parser)

        self.assertEqual(TestAzureFunction.json_data.count('\n') + 1, parsed_lines)

    def test_parse_csv_comma_delimiter_file(self) -> None:
        file_parser = CsvParser(TestAzureFunction.csv_comma_delimiter_data, TestAzureFunction.CSV_COMMA_DELIMITER)
        parsed_lines = self.__get_parsed_lines(file_parser)

        self.assertEqual(TestAzureFunction.csv_comma_delimiter_data.count('\n'), parsed_lines)

    def test_parse_csv_semicolon_delimiter_file(self) -> None:
        file_parser = CsvParser(TestAzureFunction.csv_semicolon_delimiter_data, TestAzureFunction.CSV_SEMICOLON_DELIMITER)
        parsed_lines = self.__get_parsed_lines(file_parser)

        self.assertEqual(TestAzureFunction.csv_semicolon_delimiter_data.count('\n'), parsed_lines)

    def test_parse_text_file(self) -> None:
        file_parser = TextParser(TestAzureFunction.text_data)
        parsed_lines = self.__get_parsed_lines(file_parser)

        self.assertEqual(TestAzureFunction.text_data.count('\n') + 1, parsed_lines)

    @httpretty.activate
    def test_send_json_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunction.JSON_LOG_FILE, TestAzureFunction.json_data)
        results = self.__get_sending_file_results(file_handler, httpretty.latest_requests())
        file_lines_num = TestAzureFunction.json_data.count('\n')

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(file_lines_num + 1, results['sent_lines_num'])
        self.assertEqual(len(TestAzureFunction.json_data) - file_lines_num, results['sent_bytes'])

    @httpretty.activate
    def test_send_json_data_with_bad_lines(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunction.JSON_WITH_BAD_LINES_LOG_FILE, TestAzureFunction.json_bad_lines_data)
        results = self.__get_sending_file_results(file_handler, httpretty.latest_requests())
        file_lines_num = TestAzureFunction.json_bad_lines_data.count('\n')

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertNotEqual(file_lines_num + 1, results['sent_lines_num'])
        self.assertNotEqual(len(TestAzureFunction.json_bad_lines_data) - file_lines_num, results['sent_bytes'])

    @httpretty.activate
    def test_send_csv_comma_delimiter_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunction.CSV_COMMA_DELIMITER_LOG_FILE, TestAzureFunction.csv_comma_delimiter_data)
        results = self.__get_sending_file_results(file_handler, httpretty.latest_requests())
        file_lines_num = TestAzureFunction.csv_comma_delimiter_data.count('\n')
        file_parser = CsvParser(TestAzureFunction.csv_comma_delimiter_data, TestAzureFunction.CSV_COMMA_DELIMITER)
        csv_bytes = 0

        for log in file_parser.parse_file():
            csv_bytes += len(log)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(file_lines_num, results['sent_lines_num'])
        self.assertEqual(csv_bytes, results['sent_bytes'])

    @httpretty.activate
    def test_send_csv_semicolon_delimiter_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunction.CSV_SEMICOLON_DELIMITER_FILE, TestAzureFunction.csv_semicolon_delimiter_data)
        results = self.__get_sending_file_results(file_handler, httpretty.latest_requests())
        file_lines_num = TestAzureFunction.csv_semicolon_delimiter_data.count('\n')
        file_parser = CsvParser(TestAzureFunction.csv_semicolon_delimiter_data, TestAzureFunction.CSV_SEMICOLON_DELIMITER)
        csv_bytes = 0

        for log in file_parser.parse_file():
            csv_bytes += len(log)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(file_lines_num, results['sent_lines_num'])
        self.assertEqual(csv_bytes, results['sent_bytes'])

    @httpretty.activate
    def test_send_text_data(self) -> None:
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=200)

        file_handler = FileHandler(TestAzureFunction.TEXT_LOG_FILE, TestAzureFunction.text_data)
        results = self.__get_sending_file_results(file_handler, httpretty.latest_requests())
        file_lines_num = TestAzureFunction.text_data.count('\n')
        file_parser = TextParser(TestAzureFunction.text_data)
        text_bytes = 0

        for log in file_parser.parse_file():
            text_bytes += len(log)

        self.assertEqual(math.ceil(results['sent_bytes'] / LogzioShipper.MAX_BULK_SIZE_BYTES), results['requests_num'])
        self.assertEqual(file_lines_num + 1, results['sent_lines_num'])
        self.assertEqual(text_bytes, results['sent_bytes'])

    @httpretty.activate
    def test_send_retry_status_500(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=500)

        file_handler = FileHandler(TestAzureFunction.JSON_LOG_FILE, TestAzureFunction.json_data)
        requests_num = self.__get_requests_num(file_handler)

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_502(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=502)

        file_handler = FileHandler(TestAzureFunction.JSON_LOG_FILE, TestAzureFunction.json_data)
        requests_num = self.__get_requests_num(file_handler)

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_503(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=503)

        file_handler = FileHandler(TestAzureFunction.JSON_LOG_FILE, TestAzureFunction.json_data)
        requests_num = self.__get_requests_num(file_handler)

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_retry_status_504(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=504)

        file_handler = FileHandler(TestAzureFunction.JSON_LOG_FILE, TestAzureFunction.json_data)
        requests_num = self.__get_requests_num(file_handler)

        self.assertEqual(LogzioShipper.MAX_RETRIES + 1, requests_num)

    @httpretty.activate
    def test_send_bad_format(self):
        httpretty.register_uri(httpretty.POST, os.environ['LogzioURL'], status=400)

        file_parser = JsonParser(TestAzureFunction.json_data)
        logzio_shipper = LogzioShipper(os.environ['LogzioURL'], os.environ['LogzioToken'])

        self.__add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_logzio_url(self):
        file_parser = JsonParser(TestAzureFunction.json_data)
        logzio_shipper = LogzioShipper(TestAzureFunction.BAD_LOGZIO_URL, os.environ['LogzioToken'])

        self.__add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.ConnectionError, logzio_shipper.send_to_logzio)

    def test_sending_bad_logzio_token(self):
        file_parser = JsonParser(TestAzureFunction.json_data)
        logzio_shipper = LogzioShipper(os.environ['LogzioURL'], TestAzureFunction.BAD_LOGZIO_TOKEN)

        self.__add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.HTTPError, logzio_shipper.send_to_logzio)

    def test_sending_bad_uri(self):
        file_parser = JsonParser(TestAzureFunction.json_data)
        logzio_shipper = LogzioShipper(TestAzureFunction.BAD_URI, os.environ['LogzioToken'])

        self.__add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(requests.exceptions.InvalidURL, logzio_shipper.send_to_logzio)

    def test_sending_bad_connection_adapter(self):
        file_parser = JsonParser(TestAzureFunction.json_data)
        logzio_shipper = LogzioShipper(TestAzureFunction.BAD_CONNECTION_ADAPTER_URL, os.environ['LogzioToken'])

        self.__add_first_log_to_logzio_shipper(file_parser, logzio_shipper)
        self.assertRaises(InvalidSchema, logzio_shipper.send_to_logzio)

    def __get_parsed_lines(self, file_parser: FileParser) -> int:
        parsed_lines = 0

        for _ in file_parser.parse_file():
            parsed_lines += 1

        return parsed_lines

    def __get_sending_file_results(self, file_handler: FileHandler, latest_requests: list) -> dict:
        results = {
            "requests_num": 0,
            "sent_lines_num": 0,
            "sent_bytes": 0
        }

        file_handler.handle_file()

        for request in latest_requests:
            results['requests_num'] += 1

            for line in request.parsed_body.splitlines():
                results['sent_lines_num'] += 1
                results['sent_bytes'] += len(line)

        results['requests_num'] /= 2
        results['sent_lines_num'] /= 2
        results['sent_bytes'] /= 2

        return results

    def __get_requests_num(self, file_handler: FileHandler) -> int:
        requests_num = 0

        file_handler.handle_file()

        for _ in httpretty.latest_requests():
            requests_num += 1

        requests_num /= 2

        return requests_num

    def __add_first_log_to_logzio_shipper(self, file_parser: FileParser, logzio_shipper: LogzioShipper):
        for log in file_parser.parse_file():
            logzio_shipper.add_log_to_send(log)
            break


if __name__ == '__main__':
    unittest.main()
