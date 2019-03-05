#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
from collections import OrderedDict

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
        self.source_selector.from_cats(self.cats)
        self.watchers.append(
            self.cat.widget.param.watch(self.cats_to_sources, ['value']))

        self.description = Description()
        self.description.from_sources(self.sources)
        self.watchers.append(
            self.source_selector.widget.param.watch(self.sources_to_description, ['value']))

    def cats_to_sources(self, *events):
        for event in events:
            if event.name == 'value':
                self.source_selector.from_cats(event.new)

    def sources_to_description(self, *events):
        for event in events:
            if event.name == 'value':
                self.description.from_sources(event.new)

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
    select_panel = None

    def __init__(self):
        self.search = pn.widgets.Button(name='🔍', width=30)
        self.open = pn.widgets.Button(name='+', width=30)
        self.remove = pn.widgets.Button(name='-', width=30)
        self.close = pn.widgets.Button(name='x', max_width=30, max_height=30)

        self.browser = DataBrowser()
        self.watchers.append(
            self.remove.param.watch(self.browser.cat.remove_selected, 'clicks'))

        self.control = pn.Column(logo_file, self.search, self.open, self.remove)
        self.panel = pn.Column(
            pn.Row(self.control, *self.browser.panel()),
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

    @property
    def item(self):
        if len(self.browser.sources) == 0:
            return None
        return self.browser.sources[0]
