#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
from collections import OrderedDict
from functools import partial

import intake
import panel as pn

from .base import Base, MAX_WIDTH, BACKGROUND, enable_widget
from .catalog_add import CatAdder
from .source_select import CatSelector, SourceSelector
from .source_view import Description, DefinedPlots
from .catalog_search import Search


here = os.path.abspath(os.path.dirname(__file__))
logo_file = os.path.join(here, 'logo.png')


class GUI(Base):
    """
    Top level GUI panel that contains controls and all visible sub-panels

    This class is responsible for coordinating the inputs and outputs
    of various sup-panels and their effects on each other.

    Parameters
    ----------
    cats: list of catalogs
        catalogs used to initalize the cat_browser

    Attributes
    ----------
    children: list of panel objects
        children that will be used to populate the panel when visible
    panel: panel layout object
        instance of a panel layout (row or column) that contains children
        when visible
    watchers: list of param watchers
        watchers that are set on children - cleaned up when visible
        is set to false.
    """
    def __init__(self, cats=None, **kwargs):
        self._cats = cats
        self.panel = pn.Column(name='GUI', width_policy='max', max_width=MAX_WIDTH)
        super().__init__(**kwargs)

    def setup(self):
        self.search = pn.widgets.Toggle(
            name='🔍',
            value=False,
            disabled=True,
            width=50)

        self.cat_add = pn.widgets.Toggle(
            name='＋',
            value=False,
            disabled=False,
            width=50)

        self.plot = pn.widgets.Toggle(
            name='📊',
            value=False,
            disabled=True,
            width=50)

        self.cat_browser = CatSelector(cats=self._cats,
                                       enable_dependent=partial(enable_widget, self.search))
        self.source_browser = SourceSelector(cats=self.cats,
                                             enable_dependent=partial(enable_widget, self.plot))
        self.description = Description(source=self.sources)
        self.cat_adder = CatAdder(done_callback=self.cat_browser.add,
                                  visible=self.cat_add.value,
                                  visible_callback=partial(setattr, self.cat_add, 'value'))
        self.searcher = Search(cats=self.cats,
                               done_callback=self.cat_browser.add,
                               visible=self.search.value,
                               visible_callback=partial(setattr, self.search, 'value'))
        self.plotter = DefinedPlots(source=self.sources,
                                    visible=self.plot.value,
                                    visible_callback=partial(setattr, self.plot, 'value'))

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
                    self.cat_add,
                    self.search,
                    self.plot
                ),
                self.cat_browser.panel,
                self.source_browser.panel,
                self.description.panel,
                background=BACKGROUND,
                width_policy='max',
                max_width=MAX_WIDTH,
            ),
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
        """Cats that have been selected from the cat_browser"""
        return self.cat_browser.selected

    @property
    def sources(self):
        """Sources that have been selected from the source_browser"""
        return self.source_browser.selected

    @property
    def item(self):
        """Item that is selected"""
        if len(self.sources) == 0:
            return None
        return self.sources[0]
