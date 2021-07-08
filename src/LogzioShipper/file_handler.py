import logging
import os
import azure.functions as func

from .file_parser import FileParser
from .json_parser import JsonParser
from .csv_parser import CsvParser
from .text_parser import TextParser
from .logzio_shipper import LogzioShipper


class FileHandler:

    JSON_STARTING_CHAR = '{'
    CSV_SEPARATOR = ','

    '''
    os.environ['URL'], os.environ['TOKEN']
    '''

    def __init__(self, blob_file: func.InputStream):
        self.file_name = blob_file.name
        self.file_data = blob_file.read().decode("utf-8")
        self.file_size = blob_file.length
        self.file_parser = None
        self.logzio_shipper = LogzioShipper("https://listener.logz.io:8071", "McvJQAtOrFUZQRFMrvSqnKSEJhjjFZHz")

    def handle_file(self) -> None:
        logging.info("Starts processing file - {}".format(self.file_name))

        self.file_parser = self.__get_file_parser()

        for log in self.file_parser.parse_file():
            try:
                self.logzio_shipper.add_log_to_send(log)
            except Exception:
                logging.error("Failed to send logs to Logz.io")
                return

        try:    
            self.logzio_shipper.send_to_logzio()
        except Exception:
            logging.error("Failed to send logs to Logz.io")
            return

        logging.info("Successfully finished processing file - {}".format(self.file_name))

    def __get_file_parser(self) -> FileParser:
        if self.file_data.startswith(FileHandler.JSON_STARTING_CHAR):
            return JsonParser(self.file_data)

        if self.__is_file_csv():
            return CsvParser(self.file_data)

        return TextParser(self.file_data)

    def __is_file_csv(self) -> bool:
        first_line = self.file_data.split('\n')[0]
        vars = first_line.split(FileHandler.CSV_SEPARATOR)
        separator_count = first_line.count(FileHandler.CSV_SEPARATOR)

        if len(vars) == separator_count + 1:
            return True

        return False




