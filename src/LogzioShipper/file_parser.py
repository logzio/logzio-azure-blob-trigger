from abc import ABC, abstractmethod
from typing import Generator
from io import BufferedIOBase


class FileParser(ABC):

    def __init__(self, file_stream:BufferedIOBase) -> None:
        self.file_stream = file_stream

    @abstractmethod
    def parse_file(self) -> Generator:
        pass
