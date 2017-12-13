import os
import sys
import signal
import logging
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer

from broker import Broker


log = logging.getLogger(__name__)
DEFAULT_BROKER = os.environ.get('BROKER_URL', 'redis://redis:6379/0')
DEFAULT_QUEUES = ['celery']
DEFAULT_ADDR = os.environ.get('DEFAULT_ADDR', '0.0.0.0:8888')
LOG_FORMAT = '[%(asctime)s] %(name)s:%(levelname)s: %(message)s'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker', default=DEFAULT_BROKER,
                        help="URL to the Celery broker. Defaults to {}".format(DEFAULT_BROKER))
    parser.add_argument('--queues', default=DEFAULT_QUEUES, type=lambda x: x.split(','),
                        help="URL to the Celery broker. Defaults to {}".format(','.join(DEFAULT_QUEUES)))
    parser.add_argument('--addr', default=DEFAULT_ADDR,
                        help="Address to listen on. Defaults to {}".format(DEFAULT_ADDR))
    parser.add_argument('--verbose', action='store_true', default=False,
                        help="Enable verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    else:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    broker = Broker(args.broker)

    def get_queues():
        queues = broker.queues(args.queues)
        return {q['name']: q['messages'] for q in queues}

    log.info('Current queues: %s', get_queues())

    log.info('Listening on %s', args.addr)
    addr, port = args.addr.split(':')
    httpd = HTTPServer((addr, int(port)), get_handler_class(get_queues))
    httpd.serve_forever()


def get_handler_class(get_queues):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
            self.end_headers()

            for q, length in get_queues().items():
                self.wfile.write(b'celery_queue_length{queue="%b"} %d\n' % (q.encode('utf-8'), length))

        def log_message(self, format, *args):
            return

    return Handler


def shutdown(signum, frame):
    """
    Shutdown is called if the process receives a TERM signal. This way
    we try to prevent an ugly stacktrace being rendered to the user on
    a normal shutdown.
    """
    log.info("Shutting down")
    sys.exit(0)


if __name__ == '__main__':
    main()
