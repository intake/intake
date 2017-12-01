import jinja2
import tornado.web


env = jinja2.Environment(loader=jinja2.PackageLoader('intake.cli.server'),
                         autoescape=jinja2.select_autoescape(['html', 'xml']))


def get_browser_handlers(local_catalog):
    return [
        (r"/", BrowserHandler, dict(local_catalog=local_catalog)),
    ]


class BrowserHandler(tornado.web.RequestHandler):
    def initialize(self, local_catalog):
        self.local_catalog = local_catalog

    def get(self):
        try:
            template = env.get_template('index.html')

            sources = []
            for _, name, source in self.local_catalog.walk():
                description = source.describe()
                sources.append(dict(name=name, description=description))

            self.write(template.render(dict(sources=sources)))
        except:
            pass
