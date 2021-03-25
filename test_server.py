
import sys
import logging
import argparse

from socket_server.server import SocketServer, EventMessage, SOCKET_SERVER_TCP, CloseMessage


_DEFAULT_HOST = '127.0.0.1'
_DEFAULT_PORT = 5564
_DEFAULT_WORKERS = 1


def _parse_arguments(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default=_DEFAULT_HOST, help='Listen host')
    parser.add_argument('--port', type=int, default=_DEFAULT_PORT, help='Listen port')
    parser.add_argument('--workers', type=int, default=_DEFAULT_WORKERS, help='Workers quantity')
    return parser.parse_known_args(argv)


def _evaluate_parameter(argument, config_value):
    return argument if argument else config_value


def run(argv=None):
    known_args, _ = _parse_arguments(argv)
    host = known_args.host
    port = known_args.port
    workers = known_args.workers

    socket_server = SocketServer((host, port), SOCKET_SERVER_TCP, workers)

    @socket_server.event_handler('test_event')
    def event_handler(message: EventMessage):
        logging.info(f'Message received {message}')
        logging.info('Returning close message')

        return CloseMessage()

    socket_server.start()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run(sys.argv)
