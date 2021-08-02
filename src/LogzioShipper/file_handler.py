import logging
import os
import csv

from typing import Optional
from .file_parser import FileParser
from .json_parser import JsonParser
from .csv_parser import CsvParser
from .text_parser import TextParser
from .logzio_shipper import LogzioShipper


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileHandler:

    JSON_STARTING_CHAR = '{'
    CSV_DELIMITERS: Optional[str] = [',', ';', '|']

    def __init__(self, file_name: str, file_data: str) -> None:
        self.file_name = file_name
        self.file_data = file_data
        self.file_parser = self.__get_file_parser()
        self.logzio_shipper = LogzioShipper(os.environ['LogzioURL'], os.environ['LogzioToken'])

    def handle_file(self) -> None:
        logging.info("Starts processing file - {}".format(self.file_name))

        for log in self.file_parser.parse_file():
            try:
                self.logzio_shipper.add_log_to_send(log)
            except Exception:
                logger.error("Failed to send logs to Logz.io for {}".format(self.file_name))
                return

        try:    
            self.logzio_shipper.send_to_logzio()
        except Exception:
            logger.error("Failed to send logs to Logz.io for {}".format(self.file_name))
            return

        logger.info("Successfully finished processing file - {}".format(self.file_name))

    def __get_file_parser(self) -> FileParser:
        if self.file_data.startswith(FileHandler.JSON_STARTING_CHAR):
            return JsonParser(self.file_data)

        delimiter = self.__is_file_csv()

        if delimiter is not None:
            return CsvParser(self.file_data, delimiter)

        return TextParser(self.file_data)

    def __is_file_csv(self) -> Optional[str]:
        lines = self.file_data.split('\n', 2)
        sample = '\n'.join(lines[:2])

        try:
            dialect = csv.Sniffer().sniff(sample, FileHandler.CSV_DELIMITERS)

            return str(dialect.delimiter)
        except csv.Error:
            return None
