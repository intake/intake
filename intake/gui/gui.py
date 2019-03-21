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

from .base import Base, MAX_WIDTH, BACKGROUND
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
        self.panel = pn.Tabs(name='GUI', width_policy='max')
        super().__init__(**kwargs)

    def setup(self):
        self.cat_browser = CatSelector(cats=self._cats)
        self.source_browser = SourceSelector(cats=self.cats)
        self.description = Description(source=self.sources)
        self.cat_adder = CatAdder(done_callback=self.done_callback)
        self.searcher = Search(cats=self.cats,
                               done_callback=self.cat_browser.add)
        self.searcher.watchers.append(
                self.cat_browser.widget.link(self.searcher, value='cats'))

        self.plotter = DefinedPlots(source=self.sources)
        self.plotter.watchers.append(
            self.source_browser.widget.link(self.plotter, value='source'))

        self.watchers = [
            self.cat_browser.widget.link(self.source_browser, value='cats'),
            self.source_browser.widget.link(self.description, value='source'),
        ]

        self.children = [
            self.cat_adder.panel,
            pn.Column(
                pn.Row(
                    self.cat_browser.panel,
                    self.source_browser.panel,
                    self.description.panel,
                ),
                self.searcher.panel,
                name='Select Entry'
            ),
            self.plotter.panel,
        ]

    def done_callback(self, cats=None):
        self.panel.active += 1
        if cats:
            self.cat_browser.add(cats)

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
