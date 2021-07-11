from typing import Generator
from .file_parser import FileParser

class TextParser(FileParser):

    def __init__(self, file_data: str) -> None:
        super().__init__(file_data)

    def parse_file(self) -> Generator:
        text_lines = self.file_data.split('\n')

        for text_line in text_lines:
            json_data = "{{\"message\": \"{}\"}}".format(text_line)
            yield json_data
