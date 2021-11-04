import logging
import requests
import threading
import gzip
import json

from typing import List
from requests.adapters import HTTPAdapter, RetryError
from requests.sessions import InvalidSchema, Session
from urllib3.util.retry import Retry
from .logs_queue import LogsQueue
from .custom_field import CustomField


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LogzioShipper:

    MAX_BODY_SIZE_BYTES = 10 * 1024 * 1024              # 10 MB
    MAX_BULK_SIZE_BYTES = MAX_BODY_SIZE_BYTES / 10      # 1 MB
    MAX_LOG_SIZE_BYTES = 500 * 1000                     # 500 KB

    MAX_RETRIES = 3
    BACKOFF_FACTOR = 1
    STATUS_FORCELIST = [500, 502, 503, 504]
    CONNECTION_TIMEOUT_SECONDS = 5

    def __init__(self, logzio_url: str, token: str, logs_queue: LogsQueue, version: str) -> None:
        self._logzio_url = "{0}/?token={1}&type=azure_blob_trigger".format(logzio_url, token)
        self._logs_queue = logs_queue
        self._version = version
        self._session = self._get_request_retry_session()
        self._threads: List[threading.Thread] = []
        self._is_exception_occurred = False
        self._was_any_log_invalid = False
        self._logs: List[str] = []
        self._bulk_size = 0
        self._custom_fields: List[CustomField] = []

    @property
    def is_exception_occurred(self) -> bool:
        return self._is_exception_occurred

    @property
    def was_any_log_invalid(self) -> bool:
        return self._was_any_log_invalid

    def run_logzio_shipper(self) -> None:
        while True:
            if self._is_exception_occurred:
                break

            log = self._logs_queue.get_log_from_queue()

            if log is None:
                self._create_send_to_logzio_thread()
                break

            enriched_log = self._add_custom_fields_to_log(log)
            enriched_log_size = len(enriched_log)

            if not self._is_log_valid_to_be_sent(enriched_log, enriched_log_size):
                self._was_any_log_invalid = True
                continue

            if not self._bulk_size + enriched_log_size > LogzioShipper.MAX_BULK_SIZE_BYTES:
                self._logs.append(enriched_log)
                self._bulk_size += enriched_log_size
                continue

            self._create_send_to_logzio_thread()
            self._reset_logs()
            self._logs.append(enriched_log)
            self._bulk_size = enriched_log_size

        for thread in self._threads:
            thread.join()

        self._reset_logs()
        self._session.close()

    async def send_to_logzio(self, logs: List[str], bulk_size: int) -> None:
        if len(logs) == 0:
            return

        try:
            headers = {"Content-Type": "application/json",
                       "Content-Encoding": "gzip",
                       "Logzio-Shipper": "logzio-azure-blob-trigger/v{0}/0/0.".format(self._version)}
            compressed_data = gzip.compress(str.encode('\n'.join(logs)))
            response = self._session.post(url=self._logzio_url,
                                          data=compressed_data,
                                          headers=headers,
                                          timeout=LogzioShipper.CONNECTION_TIMEOUT_SECONDS)
            response.raise_for_status()
            logger.info("Successfully sent bulk of {} bytes to Logz.io.".format(bulk_size))
        except requests.ConnectionError as e:
            logger.error(
                "Can't establish connection to {0} url. Please make sure your url is a Logz.io valid url. Max retries of {1} has reached. response: {2}".format(
                    self._logzio_url, LogzioShipper.MAX_RETRIES, e))
            self._is_exception_occurred = True
            return
        except RetryError as e:
            logger.error(
                "Something went wrong. Max retries of {0} has reached. response: {1}".format(LogzioShipper.MAX_RETRIES,
                                                                                             e))
            self._is_exception_occurred = True
            return
        except requests.exceptions.InvalidURL:
            logger.error("Invalid url. Make sure your url is a valid url.")
            self._is_exception_occurred = True
            return
        except InvalidSchema:
            logger.error(
                "No connection adapters were found for {}. Make sure your url starts with http:// or https://".format(
                    self._logzio_url))
            self._is_exception_occurred = True
            return
        except requests.HTTPError as e:
            status_code = e.response.status_code
            self._is_exception_occurred = True

            if status_code == 400:
                logger.error("The logs are bad formatted. response: {}".format(e))
                return

            if status_code == 401:
                logger.error("The token is missing or not valid. Make sure you’re using the right account token.")
                return

            logger.error("Somthing went wrong. response: {}".format(e))
            return
        except Exception as e:
            logger.error("Something went wrong. response: {}".format(e))
            self._is_exception_occurred = True
            return

    def add_custom_field_to_list(self, custom_field: CustomField) -> None:
        self._custom_fields.append(custom_field)

    def _create_send_to_logzio_thread(self) -> None:
        thread = threading.Thread(target=self.send_to_logzio, args=(self._logs, self._bulk_size,))

        self._threads.append(thread)
        thread.start()

    def _is_log_valid_to_be_sent(self, log: str, log_size: int) -> bool:
        if log_size > LogzioShipper.MAX_LOG_SIZE_BYTES:
            logger.error(
                "The following log's size is greater than the max log size - {0} bytes, that can be sent to Logz.io: {1}".format(
                    LogzioShipper.MAX_LOG_SIZE_BYTES, log))

            return False

        return True

    def _add_custom_fields_to_log(self, log: str) -> str:
        json_log = json.loads(log)

        for custom_field in self._custom_fields:
            json_log[custom_field.key] = custom_field.value

        return json.dumps(json_log)

    def _get_request_retry_session(
            self,
            retries=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=STATUS_FORCELIST
    ) -> Session:
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            status=retries,
            backoff_factor=backoff_factor,
            allowed_methods=frozenset(['GET', 'POST']),
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)

        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({"Content-Type": "application/json"})

        return session

    def _reset_logs(self) -> None:
        self._logs = []
        self._bulk_size = 0
