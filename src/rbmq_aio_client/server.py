import time
import socket
import traceback
from typing import Optional, Callable
from structlog import get_logger


logger = get_logger(__name__)

DEFAULT_BIND_ADDRESS = "0.0.0.0"
DEFAULT_BIND_PORT = 8000


class HealthCheckServer(object):
    def __init__(
        self,
        ip: str = DEFAULT_BIND_ADDRESS,
        port: int = DEFAULT_BIND_PORT,
        handle_method: "Optional[Callable]" = None,
        log=False,
        retry_count=5,
    ):

        super(HealthCheckServer, self).__init__()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__ip = ip
        self.__port = port
        self.log = log
        self.retry_count = retry_count
        self.current_try_count = 0

        self.handle_method = handle_method or self.__default_handle_method

    def __default_handle_method(self):
        """Simple handle method that accepts the connection then closes."""
        while True:
            if self.log:
                logger.info("Waiting for a connection")

            connection, client_address = self.sock.accept()
            try:
                if self.log:
                    logger.info("Client connected: {0}".format(client_address))
                connection.sendall("TEST_COMPLETE".encode("utf-8"))
            finally:
                connection.shutdown(1)

    def __run_socket_server(self):
        server_address = (self.__ip, self.__port)

        if self.log:
            logger.info(
                "Starting health server on {0} port {1}".format(self.__ip, self.__port)
            )

        if self.current_try_count > self.retry_count:
            if self.log:
                logger.info(
                    "Unable to start health server on {0} port {1}".format(
                        self.__ip, self.__port
                    )
                )
            return

        try:
            self.current_try_count = self.current_try_count + 1
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(server_address)
            self.sock.listen(1)
            self.__default_handle_method()
        except socket.error as e:
            if self.log:
                logger.debug(traceback.format_exc())
                logger.error(e)
                logger.error("Unable to start health server...retrying in 5s.")
            time.sleep(5)
            self.start()
        except Exception as e:
            logger.error(
                "Unable to start health server due to unknown exception {0}".format(e)
            )

    def start(self):
        try:
            self.__run_socket_server()
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            self.__run_socket_server()
