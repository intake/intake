import os

import intake
import panel as pn

from .base import Base

class FileSelector(Base):
    """
    Panel interface for picking files

    The current path is stored in .path and the current selection is stored in
    .url. Currently does not support windows style paths
    """
    def __init__(self, filters=['yaml', 'yml']):
        self.filters = filters
        self.setup()
        self.panel = pn.Column(*self.children, name='Local')

    def setup(self):
        self.watchers = []
        self.path = os.getcwd() + '/'
        self.main = pn.widgets.MultiSelect(size=15)

        self.up = pn.widgets.Button(name='‹', width=30, height=30)
        self.watchers.append(
            self.up.param.watch(self.move_up, 'clicks'))

        self.path_pane = pn.pane.Markdown(self.path)
        self.make_options()

        self.watchers.append(
            self.main.param.watch(self.move_down, ['value']))
        self.children = [
            pn.Row(self.up, self.path_pane),
            self.main]

    @property
    def url(self):
        return os.path.join(self.path, self.main.value[0])

    def move_up(self, arg=None):
        self.path = os.path.dirname(self.path.rstrip('/')).rstrip('/') + '/'
        self.make_options()

    def make_options(self):
        self.path_pane.object = self.path
        out = []

        for f in sorted(os.listdir(self.path)):
            if f.startswith('.'):
                continue
            elif os.path.isdir(self.path + f):
                out.append(f + '/')
            elif any(f.endswith(ext) for ext in self.filters):
                out.append(f)

        self.main.value = []
        self.main.options = dict(zip(out, out))

    def move_down(self, *events):
        for event in events:
            if event.name == 'value' and len(event.new) > 0:
                fn = event.new[0]
                if fn.endswith('/'):
                    self.path = self.path + fn
                    self.make_options()

class URLSelector(Base):
    """
    Panel interface for inputting a URL to a remote catalog

    The inputted URL is stored in .url.
    """
    def __init__(self):
        self.setup()
        self.panel = pn.Row(*self.children, name='Remote')

    def setup(self):
        self.watchers = []
        self.label = 'URL:'
        self.widget = pn.widgets.TextInput(
            placeholder="Full URL with protocol",
            width=600)
        self.children = ['URL:', self.widget]

    @property
    def url(self):
        return self.widget.value


class CatAdder(Base):
    """Sub-widget for adding new cats from file or remote"""
    cat = None

    def __init__(self, state='open', done_callback=None):
        self.panel = pn.Column()
        self.state = state
        self.done_callback = done_callback

    def setup(self):
        self.watchers = []
        self.fs = FileSelector()
        self.url = URLSelector()
        self.selectors = [self.fs, self.url]
        self.tabs = pn.Tabs(*map(lambda x: x.panel, self.selectors))
        self.widget = pn.widgets.Button(name='Add Catalog')
        self.watchers.append(
            self.widget.param.watch(self.add_cat, 'clicks'))
        self.children = [self.tabs, self.widget]

    def add_cat(self, arg=None):
        self.cat_url = self.selectors[self.tabs.active].url
        self.cat = intake.open_catalog(self.cat_url)
        if self.done_callback:
            self.done_callback(self.cat)
