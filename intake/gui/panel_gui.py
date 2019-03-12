#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
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
    def __init__(self, cats=None, visible=True):
        self._cats = cats
        self.panel = pn.Row(name='Data Browser')
        self.visible = visible

    def setup(self):
        self.cat = CatSelector(self._cats)
        self.source = SourceSelector(cats=self.cats)
        self.description = Description(source=self.sources)

        self.watchers = [
            self.cat.widget.link(self.source, value='cats'),
            self.source.widget.link(self.description, value='source')
        ]
        self.children = [
            self.cat.panel,
            self.source.panel,
            self.description.panel,
        ]

    @property
    def cats(self):
        return self.cat.selected

    @property
    def sources(self):
        return self.source.selected


class GUI(Base):
    def __init__(self, visible=True):
        self.panel = pn.Column(name='GUI')
        self.visible = visible

    def setup(self):
        self.search = pn.widgets.RadioButtonGroup(
            options={'🔍': True, 'x': False},
            value=False,
            width=80)

        self.cat_add = pn.widgets.RadioButtonGroup(
            options={'＋': True, 'x': False},
            value=False,
            width=80)

        self.plot = pn.widgets.RadioButtonGroup(
            options={'📊': True, 'x': False},
            width=80,
            disabled=True)

        self.browser = DataBrowser()
        self.selector = CatAdder(visible=self.cat_add.value,
                                 done_callback=self.browser.cat.add)
        self.searcher = Search(cats=self.browser.cats,
                               visible=self.search.value,
                               done_callback=self.browser.cat.add)

        self.watchers = [
            self.cat_add.link(self.selector, value='visible'),
            self.search.link(self.searcher, value='visible'),
            self.browser.cat.widget.link(self.searcher, value='cats')
        ]

        self.children = [
            pn.Row(
                pn.Column(
                    logo_file,
                    self.search,
                    self.cat_add,
                    self.plot),
                self.browser.panel),
            self.searcher.panel,
            self.selector.panel,
        ]

    @property
    def item(self):
        if len(self.browser.sources) == 0:
            return None
        return self.browser.sources[0]
