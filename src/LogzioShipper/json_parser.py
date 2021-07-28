import logging
import json

from typing import Generator
from .file_parser import FileParser


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JsonParser(FileParser):

    def __init__(self, file_data: str) -> None:
        super().__init__(file_data)

    def parse_file(self) -> Generator:
        jsons = self.file_data.split('\n')

        for json_data in jsons:
            try:
                json.loads(json_data)
                yield json_data
            except ValueError:
                logger.error("One of the jsons is not valid.")
