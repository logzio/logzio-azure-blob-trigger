from abc import abstractmethod
from typing import Generator


class FileParser:

    def __init__(self, file_data: bytes) -> None:
        self.file_data = file_data

    @abstractmethod
    def parse_file(self) -> Generator:
        pass
