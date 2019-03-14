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
from .source_view import Description, DefinedPlots
from .catalog_search import Search


here = os.path.abspath(os.path.dirname(__file__))
logo_file = os.path.join(here, 'logo.png')
logo = pn.Column(logo_file)


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
            value=False,
            width=80)

        self.cat_browser = CatSelector()
        self.source_browser = SourceSelector(cats=self.cats)
        self.description = Description(source=self.sources)
        self.cat_adder = CatAdder(visible=self.cat_add.value,
                                  done_callback=self.cat_browser.add)
        self.searcher = Search(cats=self.cats,
                               visible=self.search.value,
                               done_callback=self.cat_browser.add)
        self.plotter = DefinedPlots(source=self.sources,
                            visible=self.plot.value)

        self.watchers = [
            self.cat_add.link(self.cat_adder, value='visible'),
            self.search.link(self.searcher, value='visible'),
            self.plot.link(self.plotter, value='visible'),
            self.cat_browser.widget.link(self.searcher, value='cats'),
            self.cat_browser.widget.link(self.source_browser, value='cats'),
            self.source_browser.widget.link(self.description, value='source'),
            self.source_browser.widget.link(self.plotter, value='source'),
        ]

        self.children = [
            pn.Row(
                pn.Column(
                    logo_file,
                    self.search,
                    self.cat_add,
                    self.plot),
                self.cat_browser.panel,
                self.source_browser.panel,
                self.description.panel,
                background='#eeeeee'),
            self.plotter.panel,
            self.searcher.panel,
            self.cat_adder.panel,
        ]

    @property
    def cats(self):
        return self.cat_browser.selected

    @property
    def sources(self):
        return self.source_browser.selected

    @property
    def item(self):
        if len(self.sources) == 0:
            return None
        return self.sources[0]
