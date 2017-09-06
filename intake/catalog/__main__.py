import sys
import argparse

import tornado.ioloop
import tornado.web

from . import load_catalog
from .browser import get_browser_handlers
from .server import get_server_handlers


def make_app(local_catalog):
    handlers = get_browser_handlers(local_catalog) + get_server_handlers(local_catalog)
    return tornado.web.Application(handlers)


def main(argv=None):
    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser(description='Intake Catalog Server')
    parser.add_argument('-p', '--port', type=int, default=5000,
                    help='port number for server to listen on')
    parser.add_argument('catalog_file', metavar='FILE', type=str,
                    help='Name of catalog YAML file')
    args = parser.parse_args(argv[1:])

    catalog_yaml = args.catalog_file
    print('Loading %s' % catalog_yaml)

    catalog = load_catalog(catalog_yaml)
    print('Entries:', ','.join(catalog.list()))

    print('Listening on port %d' % args.port)

    app = make_app(catalog)

    app.listen(args.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    sys.exit(main(sys.argv))