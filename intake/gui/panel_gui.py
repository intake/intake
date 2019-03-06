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
from .catalog_search import Search


here = os.path.abspath(os.path.dirname(__file__))
logo_file = os.path.join(here, 'logo.png')
logo = pn.Column(logo_file)


class DataBrowser(Base):
    def __init__(self, cats=None):
        self._cats = cats
        self.setup()
        self.panel = pn.Row(*self.children)

    def setup(self):
        self.cat = CatSelector(self._cats)

        self.source = SourceSelector()
        self.source.from_cats(self.cats)
        self.cat.watchers.append(
            self.cat.widget.param.watch(self.cats_to_sources, ['value']))

        self.description = Description()
        self.description.from_sources(self.sources)
        self.source.watchers.append(
            self.source.widget.param.watch(self.sources_to_description, ['value']))

        self.children = [
            self.cat.panel,
            self.source.panel,
            self.description.panel,
        ]

    def cats_to_sources(self, *events):
        for event in events:
            if event.name == 'value':
                self.source.from_cats(event.new)

    def sources_to_description(self, *events):
        for event in events:
            if event.name == 'value':
                self.description.from_sources(event.new)

    @property
    def cats(self):
        return self.cat.selected

    @property
    def sources(self):
        return self.source.selected


class GUI(object):
    def __init__(self):
        self.remove = pn.widgets.Button(name='-', width=30)
        self.search = pn.widgets.RadioButtonGroup(
            options={'🔍': 'open', 'x': 'shut'},
            value='shut',
            width=50)

        self.cat_add = pn.widgets.RadioButtonGroup(
            options={'+': 'open', 'x': 'shut'},
            value='shut',
            width=50)

        self.plot = pn.widgets.RadioButtonGroup(
            options={'📊': 'open', 'x': 'shut'}, width=50,
            disabled=True)

        self.browser = DataBrowser()
        self.selector = CatAdder(state=self.cat_add.value,
                                 done_callback=self.browser.cat.add)
        self.searcher = Search(cats=self.browser.cats,
                               state=self.search.value,
                               done_callback=self.browser.cat.add)

        self.remove.param.watch(self.browser.cat.remove_selected, 'clicks')
        self.cat_add.link(self.selector, value='state')
        self.search.link(self.searcher, value='state')
        self.browser.cat.widget.link(self.searcher, value='cats')

        self.panel = pn.Column(
            pn.Row(
                pn.Column(
                    logo_file,
                    self.remove,
                    self.search,
                    self.cat_add,
                    self.plot),
                self.browser.panel),
            self.searcher.panel,
            self.selector.panel,
        )

    @property
    def item(self):
        if len(self.browser.sources) == 0:
            return None
        return self.browser.sources[0]
