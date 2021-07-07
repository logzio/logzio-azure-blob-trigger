import logging
import io
import csv
import json

from typing import Generator
from .file_parser import FileParser


class CsvParser(FileParser):
    
    def __init__(self, file_data: bytes) -> None:
        super().__init__(file_data)

    def parse_file(self) -> Generator:
        try:
            reader = csv.DictReader(io.StringIO(self.file_data))
            
            for row in reader:
                yield json.dumps(row)
        except TypeError:
            logging.error("One of the csv lines was unable to be serialized to json.")
