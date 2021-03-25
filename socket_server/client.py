
import time
import logging
import socket

from socket_server.message import Message, MessageParser, CloseMessage
from socket_server.server import get_socket_server_family, get_socket_server_type, SOCKET_SERVER_TCP


class SocketClient:
    def __init__(self, address, socket_type: str = SOCKET_SERVER_TCP):
        self.address = address
        self.socket_type = socket_type

        self.message_parser = MessageParser()
        self.socket = None
        self._listen_socket = True

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def start(self):
        self.socket = socket.socket(get_socket_server_family(self.address), get_socket_server_type(self.socket_type))
        self.socket.connect(self.address)
        self.socket.settimeout(0.1)  # Avoid busy waiting
        self.socket.setblocking(True)
        logging.info(f'Connected to server {self.address}')

    def close(self, terminate=False):
        if not self.socket:
            return

        logging.info(f'Closing connection to client {self.address}')
        if not terminate:
            try:
                self.send_message(CloseMessage())
            except:
                pass

        self._listen_socket = False
        self.socket.close()
        self.socket = None

    def receive_messages(self):
        while self._listen_socket:
            try:
                new_data = self.socket.recv(65536)  # Read 64KB at time 65536
                self.message_parser.received_data(new_data)

                for message in self.message_parser.parse_messages():
                    if isinstance(message, CloseMessage):
                        self.close(terminate=True)
                        break
                    else:
                        yield message

            except socket.timeout:
                continue
            except socket.error as exc:
                self.close(terminate=True)
                raise exc

            if len(new_data) == 0:
                logging.debug('Waiting 0.1s')
                time.sleep(0.1)

    def send_message(self, message: Message):
        self.socket.sendall(message.encode())
