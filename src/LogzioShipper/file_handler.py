import logging
import os
import csv
import zlib

from typing import Optional
from io import BytesIO, IOBase
from .file_parser import FileParser
from .json_parser import JsonParser
from .csv_parser import CsvParser
from .text_parser import TextParser
from .logzio_shipper import LogzioShipper


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileHandler:

    JSON_STARTING_CHAR = '{'
    GZ_FILE_SUFFIX = '.gz'
    CSV_DELIMITERS: Optional[str] = [',', ';', '|']

    LOGZIO_URL_ENVIRON_NAME = 'LogzioURL'
    LOGZIO_TOKEN_ENVIRON_NAME = 'LogzioToken'
    MULTILINE_REGEX_ENVIRON_NAME = 'MultilineRegex'

    def __init__(self, file_name: str, file_stream: IOBase, file_size: int) -> None:
        self.file_name = file_name
        self.file_stream = self.__get_seekable_file_stream(file_stream)
        self.file_size = file_size
        self.file_parser = self.__get_file_parser()
        self.logzio_shipper = LogzioShipper(os.environ[FileHandler.LOGZIO_URL_ENVIRON_NAME],
                                            os.environ[FileHandler.LOGZIO_TOKEN_ENVIRON_NAME])

    class FailedToSendLogsError(Exception):
        pass

    def handle_file(self) -> None:
        if self.file_size == 0:
            logger.info("The file {} is empty.".format(self.file_name))
            return

        logging.info("Starts processing file - {}".format(self.file_name))

        is_log_added_to_send = False

        for log in self.file_parser.parse_file():
            try:
                self.logzio_shipper.add_log_to_send(log)
                is_log_added_to_send = True
            except Exception:
                logger.error("Failed to send logs to Logz.io for {}".format(self.file_name))
                raise self.FailedToSendLogsError()

        if not is_log_added_to_send:
            logger.error("Failed to send logs to Logz.io for {}".format(self.file_name))
            raise self.FailedToSendLogsError()

        try:    
            self.logzio_shipper.send_to_logzio()
        except Exception:
            logger.error("Failed to send logs to Logz.io for {}".format(self.file_name))
            raise self.FailedToSendLogsError()

        logger.info("Successfully finished processing file - {}".format(self.file_name))

    def __get_seekable_file_stream(self, file_stream: IOBase) -> BytesIO:
        seekable_file_stream = BytesIO()

        if self.file_name.endswith(FileHandler.GZ_FILE_SUFFIX):
            decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

            for line in file_stream:
                decompressed_lines = decompressor.decompress(line).decode("utf-8")
                new_lines_num = decompressed_lines.count('\n')

                for decompressed_line in decompressed_lines.splitlines():
                    if new_lines_num > 0:
                        seekable_file_stream.write(str.encode(decompressed_line + '\n'))
                        new_lines_num -= 1
                    else:
                        seekable_file_stream.write(str.encode(decompressed_line))
        else:
            for line in file_stream:
                seekable_file_stream.write(line)
        
        seekable_file_stream.seek(0)

        return seekable_file_stream

    def __get_file_parser(self) -> FileParser:
        logs_sample = [self.file_stream.readline().decode("utf-8").rstrip(),
                       self.file_stream.readline().decode("utf-8").rstrip()]

        self.file_stream.seek(0)

        if logs_sample[0].startswith(FileHandler.JSON_STARTING_CHAR):
            return JsonParser(self.file_stream)

        delimiter = self.__is_file_csv(logs_sample)

        if delimiter is not None:
            return CsvParser(self.file_stream, delimiter)

        return TextParser(self.file_stream, os.environ[FileHandler.MULTILINE_REGEX_ENVIRON_NAME])

    def __is_file_csv(self, logs_sample: list) -> Optional[str]:
        sample = '\n'.join(logs_sample)

        try:
            dialect = csv.Sniffer().sniff(sample, FileHandler.CSV_DELIMITERS)

            return str(dialect.delimiter)
        except csv.Error:
            return None
