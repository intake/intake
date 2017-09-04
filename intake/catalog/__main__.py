import sys

import tornado.ioloop
import tornado.web

from .local import load_catalog
from .browser import get_browser_handlers
from .server import get_server_handlers


def make_app(local_catalog):
    handlers = get_browser_handlers(local_catalog) + get_server_handlers(local_catalog)
    return tornado.web.Application(handlers)


if __name__ == "__main__":
    catalog_yaml = sys.argv[1]
    print('Loading %s' % catalog_yaml)

    catalog = load_catalog(catalog_yaml)
    print('Entries:', ','.join(catalog.list()))

    app = make_app(catalog)

    app.listen(5000)
    tornado.ioloop.IOLoop.current().start()