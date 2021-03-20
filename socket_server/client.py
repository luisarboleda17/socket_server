
import time
import logging
import socket

from socket_server.message import Message, MessageParser
from socket_server.server import get_socket_server_family, get_socket_server_type, SOCKET_SERVER_TCP


class SocketClient:
    def __init__(self, address, socket_type: str = SOCKET_SERVER_TCP):
        self.address = address
        self.socket_type = socket_type

        self.message_parser = MessageParser()
        self.socket = None

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

    def close(self):
        self.socket.close()

    def receive_messages(self):
        while True:
            try:
                new_data = self.socket.recv(65536)  # Read 64KB at time 65536
                self.message_parser.received_data(new_data)

                for message in self.message_parser.parse_messages():
                    yield message

            except socket.timeout:
                continue

            if len(new_data) == 0:
                logging.debug('Waiting 0.1s')
                time.sleep(0.1)

    def send_message(self, message: Message):
        self.socket.sendall(message.encode())
