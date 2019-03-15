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
    def __init__(self, cats=None, **kwargs):
        self._cats = cats
        self.panel = pn.Column(name='GUI')
        super().__init__(**kwargs)

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

        self.cat_browser = CatSelector(cats=self._cats)
        self.source_browser = SourceSelector(cats=self.cats)
        self.description = Description(source=self.sources)
        self.cat_adder = CatAdder(visible=self.cat_add.value,
                                  done_callback=self.cat_browser.add)
        self.searcher = Search(cats=self.cats,
                               done_callback=self.cat_browser.add,
                               control_widget=self.search)
        self.plotter = DefinedPlots(source=self.sources,
                                    control_widget=self.plot)

        self.watchers = [
            self.cat_add.link(self.cat_adder, value='visible'),
            self.search.param.watch(self.on_click_search, 'value'),
            self.plot.param.watch(self.on_click_plot, 'value'),
            self.cat_browser.widget.link(self.source_browser, value='cats'),
            self.source_browser.widget.link(self.description, value='source'),
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
                background='#eeeeee',
                sizing_mode='stretch_width'),
            self.searcher.panel,
            self.cat_adder.panel,
            self.plotter.panel,
        ]

    def on_click_plot(self, event):
        """ When the plot control is toggled, set visibility and hand down source"""
        self.plotter.source = self.sources
        self.plotter.visible = event.new
        if self.plotter.visible:
            self.plotter.watchers.append(
                self.source_browser.widget.link(self.plotter, value='source'))

    def on_click_search(self, event):
        """ When the search control is toggled, set visibility and hand down cats"""
        self.searcher.cats = self.cats
        self.searcher.visible = event.new
        if self.searcher.visible:
            self.searcher.watchers.append(
                self.cat_browser.widget.link(self.searcher, value='cats'))

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
