
import sys
import logging
import argparse
from socket_server import SocketClient, SOCKET_SERVER_TCP, EventMessage


_DEFAULT_HOST = '127.0.0.1'
_DEFAULT_PORT = 5564


def _parse_arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default=_DEFAULT_HOST, help='Listen host')
    parser.add_argument('--port', type=int, default=_DEFAULT_PORT, help='Listen port')
    return parser.parse_known_args(argv)


def run(argv=None):
    known_args, _ = _parse_arguments(argv)
    host = known_args.host
    port = known_args.port

    with SocketClient((host, port), SOCKET_SERVER_TCP) as client:

        client.send_message(EventMessage('test_event', 'Hello World!'))

        for message in client.receive_messages():
            logging.info(f'Message received {message}')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run(sys.argv)
