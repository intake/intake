#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from functools import partial

import intake
import panel as pn

from .base import Base, MAX_WIDTH, BACKGROUND, enable_widget, logo
from .catalog import CatAdder, CatSelector, Search
from .source import SourceSelector, DefinedPlots, Description


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
        self.search_toggle = pn.widgets.Toggle(
            name='🔍',
            value=False,
            disabled=True,
            width=50)

        self.cat_add_toggle = pn.widgets.Toggle(
            name='＋',
            value=False,
            disabled=False,
            width=50)

        self.plot_toggle = pn.widgets.Toggle(
            name='📊',
            value=False,
            disabled=True,
            width=50)

        self.cat_browser = CatSelector(cats=self._cats,
                                       done_callback=self.enable_search_toggle)
        self.source_browser = SourceSelector(cats=self.cats,
                                             done_callback=self.enable_plot_toggle)
        self.description = Description(source=self.sources)
        self.cat_add = CatAdder(done_callback=self.cat_browser.add,
                                  visible=self.cat_add_toggle.value,
                                  visible_callback=partial(setattr, self.cat_add_toggle, 'value'))
        self.search = Search(cats=self.cats,
                             done_callback=self.cat_browser.add,
                             visible=self.search_toggle.value,
                             visible_callback=partial(setattr, self.search_toggle, 'value'))
        self.plot = DefinedPlots(source=self.sources,
                                 visible=self.plot_toggle.value,
                                 visible_callback=partial(setattr, self.plot_toggle, 'value'))

        self.watchers = [
            self.cat_add_toggle.link(self.cat_add, value='visible'),
            self.search_toggle.param.watch(self.on_click_search_toggle, 'value'),
            self.plot_toggle.param.watch(self.on_click_plot_toggle, 'value'),
            self.cat_browser.widget.link(self.source_browser, value='cats'),
            self.source_browser.widget.link(self.description, value='source'),
        ]

        self.children = [
            pn.Row(
                pn.Column(
                    logo,
                    self.cat_add_toggle,
                    self.search_toggle,
                    self.plot_toggle,
                ),
                self.cat_browser.panel,
                self.source_browser.panel,
                self.description.panel,
                background=BACKGROUND,
                width_policy='max',
                max_width=MAX_WIDTH,
                margin=0
            ),
            self.search.panel,
            self.cat_add.panel,
            self.plot.panel,
        ]

    def enable_plot_toggle(self, enable):
        if not enable:
            self.plot_toggle.value = False
        return enable_widget(self.plot_toggle, enable)

    def on_click_plot_toggle(self, event):
        """ When the plot control is toggled, set visibility and hand down source"""
        self.plot.source = self.sources
        self.plot.visible = event.new
        if self.plot.visible:
            self.plot.watchers.append(
                self.source_browser.widget.link(self.plot, value='source'))

    def enable_search_toggle(self, enable):
        if not enable:
            self.search_toggle.value = False
        return enable_widget(self.search_toggle, enable)

    def on_click_search_toggle(self, event):
        """ When the search control is toggled, set visibility and hand down cats"""
        self.search.cats = self.cats
        self.search.visible = event.new
        if self.search.visible:
            self.search.watchers.append(
                self.cat_browser.widget.link(self.search, value='cats'))

    @property
    def cats(self):
        """Cats that have been selected from the cat_browser"""
        return self.cat_browser.selected

    def add(self, *args, **kwargs):
        """Add to list of cats TODO """
        return self.cat_browser.add(*args, **kwargs)

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
