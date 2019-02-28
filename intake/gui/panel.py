#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from collections import OrderedDict
from contextlib import contextmanager
import panel as pn
import os
import posixpath

import intake


here = os.path.abspath(os.path.dirname(__file__))
logo_file = os.path.join(here, 'logo.png')
logo = pn.Column(logo_file)


def cat_from_string_or_cat(cat):
    """Return a catalog instance when given a cat or a string"""
    if isinstance(cat, str):
        cat = intake.open_catalog(cat)
    return cat

def cats_from_string_list_or_cats(cats):
    if isinstance(cats, dict):
        return cats
    if isinstance(cats, list):
        cats = cats
    else:
        cats = [cats]
    cats = map(cat_from_string_or_cat, cats)
    return OrderedDict([(cat.name, cat) for cat in cats])


class Base(object):  # pragma: no cover
    widget = None
    selected = None
    options = None
    watchers = []  # list of all the watchers that we might want to disconnect

    def __repr__(self):
        return ("Intake GUI instance: to get widget to display, you must "
                "install panel. If running from within jupyterlab, "
                "install the jlab extension.")

    def unwatch(self):
        for watcher in self.watchers:
            self.widget.param.unwatch(watcher)

    def callback(self, *events):
        print(events)
        for event in events:
            if event.name == 'options' and event.new != self.options:
                self.add(event.new)
            elif event.name == 'value' and event.new != self.selected:
                self.select(event.new)

    def update_selected(self):
        if self.widget.value != self.selected:
            self.widget.value = self.selected

    def update_options(self):
        if self.widget.options != self.options:
            self.widget.options = self.options.copy()

    def add(self, new):
        raise NotImplementedError

    def select(self, new):
        raise NotImplementedError

    def panel(self):
        return pn.Column(self.widget)


class CatalogBrowser(Base):  # pragma: no cover
    """Intake Catalog browser

    Examples
    --------

    Display GUI in a notebook thus:

    >>> gui = intake.gui.widgets.CatalogBrowser()
    >>> gui.panel()

    Optionally you can specify some Catalog instances to begin with. If none
    are given, will use the default one, ``intake.cat``.

    After interacting, the value of ``gui.item`` will be the entry selected.
    """
    selected = []  # list because it is a multi-select

    def __init__(self, cats=None):
        if cats is None:
            cats = [intake.cat]
        self.options = cats_from_string_list_or_cats(cats)
        self.widget = pn.widgets.MultiSelect(size=9)
        self.update_options()
        self.watchers.append(
            self.widget.param.watch(self.callback, ['options', 'value']))

    def select(self, cat):
        """Select one cat by name"""
        if isinstance(cat, str):
            cat = [self.options[cat]]
        self.selected = cat
        self.update_selected()

    def unselect(self):
        cats = self.selected
        self.selected = []
        self.update_selected()
        return cats

    def add(self, cat):
        options = cats_from_string_list_or_cats(cat)
        self.options.update(options)
        self.update_options()
        self.select(list(options.values()))

    def del_selected(self, *args):
        """Delete the selected catalog - allow the passing of arbitrary
        args so that buttons work"""
        cats = self.unselect()
        for cat in cats:
            self.options.pop(cat.name)
        self.update_options()


class DataSourceBrowser(Base):
    selected = []  # list because it is a multi-select
    options = {}

    def __init__(self, sources=None):
        if sources is not None:
            sources = sources if isinstance(sources, list) else [sources]
            self.options = OrderedDict([(s.name, s) for s in sources])
        self.widget = pn.widgets.MultiSelect(size=9)
        self.update_options()
        self.watchers.append(
            self.widget.param.watch(self.callback, ['options', 'value']))

    def select(self, new):
        """Select one datasource by name"""
        if isinstance(new, str):
            new = [self.options[new]]
        self.selected = new
        self.update_selected()

    def unselect(self, old=None):
        old = old or self.selected
        self.selected = [s for s in self.selected if s != old]
        self.update_selected()
        return old

    def add(self, sources):
        options = OrderedDict([(s.name, s) for s in sources])
        self.options.update(options)
        self.update_options()
        self.select(list(options.values()))


class Description(object):
    def __init__(self, source=None):
        self._source = source
        self.pane = pn.pane.Markdown(self.contents)

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        """When the source gets updated, update the pane object"""
        if source != self._source:
            self._source = source
            self.pane.object = self.contents
        return self._source

    @property
    def contents(self):
        return str(self._source.describe()) if self._source else " "

    def panel(self):
        return pn.Column(self.pane)


class DataBrowser(Base):
    def __init__(self, cats=None):
        self.cat = CatalogBrowser(cats)

        self.source_browser = DataSourceBrowser()
        self.update_sources_from_cats(self.cats)
        self.watchers.append(
            self.cat.widget.param.watch(self.cats_to_sources, ['value']))

        self.description = Description()
        self.update_description_from_sources(self.sources)
        self.watchers.append(
            self.source_browser.widget.param.watch(self.sources_to_description, ['value']))

    def update_sources_from_cats(self, cats):
        source = self.source_browser

        if source.selected:
            for s in source.selected:
                if s._catalog not in cats:
                    source.unselect(s)

        options = {}
        for cat in cats:
            options.update(OrderedDict([(s, cat[s]) for s in cat]))
        source.options = options
        source.update_options()

        if not source.selected and list(options.values()):
            source.select([list(options.values())[0]])

    def update_description_from_sources(self, sources):
        if len(sources) > 0:
            source = sources[0]
            self.description.source = source

    def cats_to_sources(self, *events):
        for event in events:
            if event.name == 'value':
                self.update_sources_from_cats(event.new)

    def sources_to_description(self, *events):
        for event in events:
            if event.name == 'value':
                self.update_description_from_sources(event.new)

    @property
    def cats(self):
        return self.cat.selected

    @property
    def sources(self):
        return self.source_browser.selected

    def panel(self):
        return pn.Row(
            self.cat.panel(),
            self.source_browser.panel(),
            self.description.panel(),
        )


class FileSelector(Base):
    """
    panel interface for picking files

    The current path is stored in .path and the current selection is stored in
    .url.
    """
    def __init__(self, filters=['yaml', 'yml']):
        self.filters = filters
        self.path = os.getcwd() + '/'
        self.main = pn.widgets.MultiSelect(size=15)

        self.up = pn.widgets.Button(name='‹', width=40)
        self.watchers.append(
            self.up.param.watch(self.move_up, 'clicks'))

        self.path_pane = pn.pane.Markdown(self.path)
        self.make_options()

        self.watchers.append(
            self.main.param.watch(self.move_down, ['value']))

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

    def panel(self):
        return pn.Column(
            pn.Row(self.up, self.path_pane),
            self.main,
            name='Local'
        )

class URLSelector(Base):
    def __init__(self):
        self.label = 'URL:'
        self.widget = pn.widgets.TextInput(
            placeholder="Full URL with protocol",
            width=600)

    @property
    def url(self):
        return self.widget.value

    def panel(self):
        return pn.Row(self.label, self.widget, name='Remote')


class CatSelector(Base):
    """Sub-widget for adding new cats from file or remote"""
    cat = None

    def __init__(self):
        self.fs = FileSelector()
        self.url = URLSelector()
        self.tabs = [self.fs, self.url]
        self.widget = pn.Tabs(*map(lambda x: x.panel(), self.tabs))
        self.add = pn.widgets.Button(name='Add Catalog')
        self.watchers.append(
            self.add.param.watch(self.add_cat, 'clicks'))

    def add_cat(self, arg=None):
        self.cat_url = self.tabs[self.widget.active].url
        self.cat = intake.open_catalog(self.cat_url)

    def panel(self):
        return pn.Column(self.widget, self.add, pn.Spacer(height=400))


class GUI(Base):
    def __init__(self):
        self.search = pn.widgets.Button(name='🔍', width=40)
        self.open = pn.widgets.Button(name='+', width=40)
        self.remove = pn.widgets.Button(name='-', width=40)
        self.close = pn.widgets.Button(name='x', width=40)

        self.browser = DataBrowser()
        self.watchers.append(
            self.remove.param.watch(self.browser.cat.del_selected, 'clicks'))

        self.selector = CatSelector()
        self.add_cat_from_selector(self.selector.cat)
        self.watchers.append(
            self.selector.add.param.watch(self.add_cat_from_selector, 'clicks'))

        self.control = pn.Column(logo_file, self.search, self.open, self.remove)
        self.select_panel = pn.Row(self.selector.panel(), self.close)
        self.panel = pn.Column(
            pn.Row(self.control, *self.browser.panel()),
            self.select_panel
        )
        self.watchers.append(
            self.open.param.watch(self.open_selector, 'clicks'))
        self.watchers.append(
            self.close.param.watch(self.close_selector, 'clicks'))

    def add_cat_from_selector(self, arg=None):
        cat = self.selector.cat
        if cat is not None:
            self.browser.cat.add(cat)

    def close_selector(self, arg=None):
        if self.select_panel in self.panel:
            self.panel.pop(self.select_panel)

    def open_selector(self, arg=None):
        if self.select_panel not in self.panel:
            self.panel.append(self.select_panel)
