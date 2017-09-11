import sys
import argparse
import time
import traceback
import os.path

import tornado.ioloop
import tornado.web

from .local import LocalCatalogHandle
from .remote import RemoteCatalog
from .browser import get_browser_handlers
from .server import get_server_handlers


def load_catalog(uri):
    if uri.startswith('http://') or uri.startswith('https://'):
        return RemoteCatalog(uri)
    else:
        return LocalCatalogHandle(uri) # This can trigger reload


def make_app(local_catalog):
    handlers = get_browser_handlers(local_catalog) + get_server_handlers(local_catalog)
    return tornado.web.Application(handlers)


def make_file_watcher(filename, local_catalog, interval_ms):
    full_path = os.path.abspath(filename)
    last_load = os.path.getmtime(full_path)

    def callback():
        nonlocal last_load
        mtime = os.path.getmtime(full_path)
        if mtime > last_load:
            try:
                print('Autodetecting change to %s.  Reloading...' % filename)
                local_catalog.reload()
                print('Catalog entries:', ', '.join(local_catalog.list()))
                last_load = mtime
            except Exception as e:
                print('Unable to reload %s' % filename)
                traceback.print_exc()

    callback = tornado.ioloop.PeriodicCallback(callback, interval_ms,
                                               io_loop=tornado.ioloop.IOLoop.current())
    return callback

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
    watcher = make_file_watcher(catalog_yaml, catalog, 1000) # poll every second
    watcher.start()

    app.listen(args.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    sys.exit(main(sys.argv))