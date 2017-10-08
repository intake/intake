import sys
import argparse
import time
import traceback
import os.path
import signal

import tornado.ioloop
import tornado.web

from .local import LocalCatalog, UnionCatalog, ReloadableCatalog
from .remote import RemoteCatalog
from .browser import get_browser_handlers
from .server import IntakeServer


def call_exit_on_sigterm(signal, frame):
    sys.exit(0)


def catalog_args_func_factory(args):
    def build_catalog_func():
        catalogs = []
        for arg in args:
            if arg.startswith('http://') or arg.startswith('https://'):
                return RemoteCatalog(arg)
            else:
                if os.path.isdir(arg):
                    for fname in os.listdir(arg):
                        fullname = os.path.join(arg, fname)
                        if fullname.endswith('.yml') or fullname.endswith('.yaml'):
                            catalogs.append(LocalCatalog(fullname))
                else:
                    catalogs.append(LocalCatalog(arg))

        if len(catalogs) > 1:
            return UnionCatalog(catalogs)
        else:
            return catalogs[0]

    def catalog_mtime_func():
        last_mtime = 0
        for arg in args:
            if arg.startswith('http://') or arg.startswith('https://'):
                continue  # FIXME: How do we get mtime of remote catalog?
            else:
                if os.path.isdir(arg):
                    last_mtime = max(last_mtime, os.path.getmtime(arg))
                    for fname in os.listdir(arg):
                        fullname = os.path.join(arg, fname)
                        if fullname.endswith('.yml') or fullname.endswith('.yaml'):
                            last_mtime = max(last_mtime, os.path.getmtime(fullname))
                else:
                    last_mtime = max(last_mtime, os.path.getmtime(arg))

        return last_mtime

    return build_catalog_func, catalog_mtime_func


def make_app(catalog, server):
    handlers = get_browser_handlers(catalog) + server.get_handlers()
    return tornado.web.Application(handlers)


def make_file_watcher(catalog_mtime_func, catalog, interval_ms):
    last_load = catalog_mtime_func()

    def callback():
        nonlocal last_load
        mtime = catalog_mtime_func()
        if mtime > last_load:
            try:
                print('Autodetecting change to catalog.  Reloading...')
                catalog.reload()
                print('Catalog entries:', ', '.join(catalog.list()))
                last_load = mtime
            except Exception:
                print('Unable to reload.  Catalog left in previous state.')
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
    parser.add_argument('--sys-exit-on-sigterm', action='store_true',
                    help='internal flag used during unit testsing to ensure .coverage file is written')
    parser.add_argument('catalog_args', metavar='FILE', type=str, nargs='+',
                    help='Name of catalog YAML file')
    args = parser.parse_args(argv[1:])

    if args.sys_exit_on_sigterm:
        signal.signal(signal.SIGTERM, call_exit_on_sigterm)

    print('Creating catalog from:')
    for arg in args.catalog_args:
        print('  - %s' % arg)

    build_catalog_func, catalog_mtime_func = catalog_args_func_factory(args.catalog_args)
    catalog = ReloadableCatalog(build_catalog_func)

    print('Entries:', ','.join(catalog.list()))

    print('Listening on port %d' % args.port)

    server = IntakeServer(catalog)
    app = make_app(catalog, server)
    watcher = make_file_watcher(catalog_mtime_func, catalog, 1000) # poll every second
    watcher.start()

    app.listen(args.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
