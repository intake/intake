#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os

import intake
import panel as pn

from .base import Base
from .catalog_add import CatAdder
from .source_select import CatSelector, SourceSelector
from .source_view import Description


here = os.path.abspath(os.path.dirname(__file__))
logo_file = os.path.join(here, 'logo.png')
logo = pn.Column(logo_file)


class DataBrowser(Base):
    def __init__(self, cats=None):
        self.cat = CatSelector(cats)

        self.source_selector = SourceSelector()
        self.update_sources_from_cats(self.cats)
        self.watchers.append(
            self.cat.widget.param.watch(self.cats_to_sources, ['value']))

        self.description = Description()
        self.update_description_from_sources(self.sources)
        self.watchers.append(
            self.source_selector.widget.param.watch(self.sources_to_description, ['value']))

    def update_sources_from_cats(self, cats):
        """
        When selected cat changes:
          - remove all sources
          - replace with ones from selected catalogs
          - Select first item
        """
        self.source_selector.remove(list(self.source_selector.options.values()))

        sources = []
        for cat in cats:
            sources.extend(list(cat._entries.values()))

        self.source_selector.add(sources, autoselect=False)
        if len(sources) > 0:
            self.source_selector.select([sources[0]])

    def update_description_from_sources(self, sources):
        if len(sources) > 0:
            source = sources[0]
            self.description.source = source
        else:
            self.description.source = None

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
        return self.source_selector.selected

    def panel(self):
        return pn.Row(
            self.cat.panel(),
            self.source_selector.panel(),
            self.description.panel(),
        )

class GUI(Base):
    def __init__(self):
        self.search = pn.widgets.Button(name='🔍', width=30)
        self.open = pn.widgets.Button(name='+', width=30)
        self.remove = pn.widgets.Button(name='-', width=30)
        self.close = pn.widgets.Button(name='x', max_width=30, max_height=30)

        self.browser = DataBrowser()
        self.watchers.append(
            self.remove.param.watch(self.browser.cat.remove_selected, 'clicks'))

        self.setup_selector()

        self.control = pn.Column(logo_file, self.search, self.open, self.remove)
        self.panel = pn.Column(
            pn.Row(self.control, *self.browser.panel()),
            self.select_panel
        )
        self.watchers.append(
            self.open.param.watch(self.open_selector, 'clicks'))
        self.watchers.append(
            self.close.param.watch(self.close_selector, 'clicks'))

    def setup_selector(self):
        self.selector = CatAdder()
        self.add_cat_from_selector(self.selector.cat)
        self.selector.watchers.append(
            self.selector.add.param.watch(self.add_cat_from_selector, 'clicks'))

        self.select_panel = pn.Row(self.selector.panel(), self.close)

    def add_cat_from_selector(self, arg=None):
        cat = self.selector.cat
        if cat is not None:
            self.browser.cat.add(cat)

    def close_selector(self, arg=None):
        self.selector.unwatch()
        if self.select_panel in self.panel:
            self.panel.pop(self.select_panel)
            self.select_panel = None

    def open_selector(self, arg=None):
        if not self.select_panel:
            self.setup_selector()
            self.panel.append(self.select_panel)
