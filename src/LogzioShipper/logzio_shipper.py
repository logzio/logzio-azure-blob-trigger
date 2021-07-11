import logging
import requests

from typing import Any
from requests.adapters import HTTPAdapter, RetryError
from urllib3.util import Retry
from requests.sessions import InvalidSchema, Session
from urllib3.util.retry import Retry


class LogzioShipper:

    MAX_BODY_SIZE_BYTES = 10 * 1024 * 1024                    # 10 MB
    MAX_BULK_SIZE_BYTES = MAX_BODY_SIZE_BYTES / 20            # 0.5 MB
    MAX_LOG_SIZE_BYTES = 500 * 1000                           # 500 KB

    MAX_RETRIES = 3
    BACKOFF_FACTOR = 1
    STATUS_FORCELIST = [500, 502, 503, 504]
    CONNECTION_TIMEOUT_SECONDS = 5

    def __init__(self, logzio_url: str, token: str) -> None:
        self.logzio_url = "{0}/?token={1}&type=azure".format(logzio_url, token)
        self.logs = []
        self.bulk_size = 0

    def add_log_to_send(self, log: Any) -> None:
        log_size = len(log)

        if not self.__is_log_valid_to_be_sent(log_size):
            return

        if not self.bulk_size + log_size > LogzioShipper.MAX_BULK_SIZE_BYTES:
            self.logs.append(log)
            self.bulk_size += log_size
            return

        try:
            self.send_to_logzio()
        except Exception:
            raise

        self.__reset_after_send(log, log_size)

    def send_to_logzio(self):
        if self.logs is None:
            return

        try:
            response = self.__get_request_retry_session().post(url=self.logzio_url, data=str.encode('\n'.join(self.logs)), timeout=LogzioShipper.CONNECTION_TIMEOUT_SECONDS)
            response.raise_for_status()
            logging.info("Successfully sent bulk of {} bytes to Logz.io.".format(self.bulk_size))
        except requests.ConnectionError as e:
            logging.error(
                "Can't establish connection to {0} url. Please make sure your url is a Logz.io valid url. Max retries of {1} has reached. response: {2}"
                .format(self.logzio_url, LogzioShipper.MaxRetries, e))
            raise
        except RetryError as e:
            logging.error("Something went wrong. Max retries of {0} has reached. response: {1}".format(LogzioShipper.MaxRetries, e))
            raise
        except InvalidSchema as e:
            logging.error("No connection adapters were found for {}. Make sure your url starts with http:// or https://".format(self.logzio_url))
            raise
        except requests.HTTPError as e:
            status_code = response.status_code

            if status_code == 400:
                logging.error("The logs are bad formatted. response: {}".format(e))
                raise

            if status_code == 401:
                logging.error("The token is missing or not valid. Make sure you’re using the right account token.")
                raise

            logging.error("Somthing went wrong. response: {}".format(e))
            raise
        except Exception as e:
            logging.error("Something went wrong. response: {}".format(e))
            raise

    def __is_log_valid_to_be_sent(self, log_size: int) -> bool:
        if log_size > LogzioShipper.MAX_LOG_SIZE_BYTES:
            logging.error(
                "One of the log's size is greater than the max log size - {} bytes, that can be sent to Logz.io".format(LogzioShipper.MAX_LOG_SIZE_BYTES))

            return False

        return True

    def __get_request_retry_session(
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

    def __reset_after_send(self, log: Any, log_size: int) -> None:
        self.logs.clear()
        self.logs.append(log)
        self.bulk_size = log_size
