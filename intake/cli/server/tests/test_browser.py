import os.path

import tornado.web
from tornado.testing import AsyncHTTPTestCase

from intake.cli.server.browser import get_browser_handlers
from intake.catalog.local import LocalCatalog


class TestBrowser(AsyncHTTPTestCase):
    def get_app(self):
        catalog_file = os.path.join(os.path.dirname(__file__), 'catalog1.yml')
        local_catalog = LocalCatalog(catalog_file)
        handlers = get_browser_handlers(local_catalog)
        return tornado.web.Application(handlers)

    def test_main_page(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        self.assertIn(b'entry1 full', response.body)
        self.assertIn(b'entry1 part', response.body)
