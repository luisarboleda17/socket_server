
import os
import sys
import time
import logging
import socket
import inspect
import signal
import threading
import multiprocessing

from socket_server.message import MessageParser, TextMessage, JSONMessage, EventMessage, Message, CloseMessage


_DEFAULT_HOST = '127.0.0.1'
_DEFAULT_PORT = 5564

# Socket server types
SOCKET_SERVER_UDP = 'udp'
SOCKET_SERVER_TCP = 'tcp'


def _is_windows():
    return os.name == 'nt'


def get_socket_server_type(socket_type):
    if socket_type == SOCKET_SERVER_TCP:
        return socket.SOCK_STREAM
    else:
        return socket.SOCK_DGRAM


def get_socket_server_family(address):
    if isinstance(address, tuple):
        return socket.AF_INET
    else:
        return socket.AF_UNIX


class SocketServer:
    def __init__(
        self,
        address,
        socket_type=SOCKET_SERVER_TCP,
        workers_quantity: int = 1
    ):
        self.address = address
        self.socket_type = socket_type
        self.workers_quantity = workers_quantity

        self._handlers = {
            'text': None,
            'json': None,
            'event': {},
        }
        self._workers = [None] * workers_quantity  # Create workers list

    def json_handler(self, handler):
        """Set the handler for JSON messages. If an event handler is set, this handler will be ignored for defined
        events.

        :param handler:
        :return:
        """
        self._handlers['json'] = handler
        return handler

    def text_handler(self, handler):
        """Set the handler for text messages.

        :param handler:
        :return:
        """
        self._handlers['text'] = handler
        return handler

    def event_handler(self, event_name):
        """Set the handler for event name. JSON handler will be ignored for given events.

        :param event_name:
        :return:
        """
        def decorator(handler):
            self._handlers['event'][event_name] = handler
            return handler
        return decorator

    def _start_worker_process(self, worker_id: int):
        if self._workers[worker_id]:
            self._workers[worker_id].terminate()

        worker_process = _SocketWorkerProcess(
            self.address,
            self.socket_type,
            self._handlers,
            name=f'SocketServerWorker-{worker_id}',
        )
        worker_process.daemon = True
        worker_process.start()
        self._workers[worker_id] = worker_process

    def start(self):
        if isinstance(self.address, str):
            try:
                os.remove(self.address)
            except OSError:
                pass

        signal.signal(signal.SIGINT, self._handle_terminate_signal)
        signal.signal(signal.SIGTERM, self._handle_terminate_signal)

        # Workers monitoring
        while True:
            for worker_id, worker in enumerate(self._workers):
                if not worker or not worker.is_alive():
                    self._start_worker_process(worker_id)
            time.sleep(1)

    def close(self):
        for worker in self._workers:
            if worker:
                worker.terminate()
                worker.join(timeout=1.0)

    def _handle_terminate_signal(self, *args):
        logging.info('Shutting down server...')
        self.close()
        sys.exit(1)


class _SocketWorkerProcess(multiprocessing.Process):
    def __init__(
        self,
        address,
        socket_type: str,
        handlers: dict,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.address = address
        self.socket_type = socket_type
        self.handlers = handlers

        self._kill_event = threading.Event()

    def run(self):
        signal.signal(signal.SIGINT, self._handle_terminate_signal)
        signal.signal(signal.SIGTERM, self._handle_terminate_signal)

        with socket.socket(get_socket_server_family(self.address), get_socket_server_type(self.socket_type)) as sock:

            # Check if can use SO_REUSEPORT, only in Unix (Linux)
            if not _is_windows():
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

            sock.bind(self.address)
            sock.listen()
            sock.settimeout(0.1)  # Avoid busy waiting
            sock.setblocking(True)

            logging.info(f'[{self.name}]: Listening connections in {self.address}')

            while True:
                try:
                    client_sock, address = sock.accept()
                    connection_thread = _SocketConnectionHandlerThread(
                        self.name,
                        client_sock,
                        address,
                        self._kill_event,
                        self.handlers,
                    )
                    connection_thread.start()
                except socket.timeout:
                    continue

    def _handle_terminate_signal(self, *args):
        logging.info(f'[{self.name}]: Shutting down worker...')
        sys.exit(1)


class _SocketConnectionHandlerThread(threading.Thread):
    def __init__(self, worker_name, client_socket, address, kill_event, handlers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_name = worker_name
        self.client_socket = client_socket
        self.address = address
        self.kill_event = kill_event
        self.handlers = handlers

        self.message_parser = MessageParser()

    def close(self):
        self.client_socket.close()

    @staticmethod
    def _map_result_to_message(result):
        if not result:
            return

        if isinstance(result, Message):
            return result
        if isinstance(result, dict):
            return JSONMessage(result)
        else:
            return TextMessage(str(result))

    def send(self, message):
        if not message:
            return
        self.client_socket.sendall(message.encode())

    def _handle_handler_response(self, message, handler):
        handler_result = handler(message)

        if not handler_result:
            return

        if inspect.isgenerator(handler_result):
            for result in handler_result:
                self.send(self._map_result_to_message(result))
        else:
            self.send(self._map_result_to_message(handler_result))

    def run(self):
        client_host, client_port = self.address
        logging.info(f'[{self.worker_name}]: Client {client_host}:{client_port} connected')

        self.client_socket.settimeout(0.1)

        while not self.kill_event.is_set():
            try:
                new_data = self.client_socket.recv(65536)  # Read 64KB at time 65536
                self.message_parser.received_data(new_data)

                for message in self.message_parser.parse_messages():
                    # First check custom messages, then raw messages
                    if isinstance(message, CloseMessage):
                        self.close()
                    elif isinstance(message, EventMessage) and message.event_name in self.handlers['event']:
                        self._handle_handler_response(message, self.handlers['event'][message.event_name])
                    elif isinstance(message, TextMessage) and self.handlers['text']:
                        self._handle_handler_response(message, self.handlers['text'])
                    elif isinstance(message, JSONMessage) and self.handlers['json']:
                        self._handle_handler_response(message, self.handlers['json'])
                    else:
                        logging.warning(f'[{self.worker_name}]: No handler registered for message type {message}')
            except socket.timeout:
                continue

            if len(new_data) == 0:
                logging.debug(f'[{self.worker_name}]: Waiting 0.1s')
                time.sleep(0.1)

        self.client_socket.close()
