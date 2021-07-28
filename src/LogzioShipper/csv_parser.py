import logging
import io
import csv
import json

from typing import Generator
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CsvParser(FileParser):
    
    def __init__(self, file_data: str, delimiter: str) -> None:
        super().__init__(file_data)
        self.delimiter = delimiter

    def parse_file(self) -> Generator:
        try:
            reader = csv.DictReader(io.StringIO(self.file_data), delimiter=self.delimiter)
            
            for row in reader:
                yield json.dumps(row)
        except TypeError:
            logger.error("One of the csv_comma_delimiter lines was unable to be serialized to json.")
