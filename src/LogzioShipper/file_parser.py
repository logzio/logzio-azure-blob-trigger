from abc import ABC, abstractmethod
from typing import Generator
from io import BufferedIOBase


class FileParser(ABC):

    def __init__(self, file_stream:BufferedIOBase) -> None:
        self._file_stream = file_stream
        self._are_all_logs_parsed = True

    @property
    def are_all_logs_parsed(self) -> bool:
        return self._are_all_logs_parsed

    @abstractmethod
    def parse_file(self) -> Generator[str, None, None]:
        pass
