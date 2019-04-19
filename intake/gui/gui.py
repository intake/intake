#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import intake
import panel as pn

from .base import Base, MAX_WIDTH, logo_panel
from .catalog.gui import CatGUI
from .source.gui import SourceGUI


class GUI(Base):
    """
    Top level GUI panel that contains controls and all visible sub-panels

    This class is responsible for coordinating the inputs and outputs
    of various sup-panels and their effects on each other.

    Parameters
    ----------
    cats: list of catalogs
        catalogs used to initalize the cat panel

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
    def __init__(self, cats=None):
        self.source = SourceGUI()
        self.cat = CatGUI(cats=cats, done_callback=self.done_callback)
        self.panel = pn.Row(name='GUI')
        self.visible = True

    def setup(self):
        self.children = [
            logo_panel,
            pn.Column(
                pn.Row(
                    pn.Column(
                        self.cat.select.panel,
                        self.cat.control_panel,
                        margin=0,
                    ),
                    pn.Column(
                        self.source.select.panel,
                        self.source.control_panel,
                        margin=0
                    ),
                    self.source.description.panel,
                    margin=0,
                ),
                self.cat.search.panel,
                self.cat.add.panel,
                self.source.plot.panel,
                width_policy='max',
                max_width=MAX_WIDTH,
            ),
        ]

    def done_callback(self, cats):
        self.source.select.cats = cats

    @property
    def cats(self):
        """Cats that have been selected from the cat_browser"""
        return self.cat.cats

    def add(self, *args, **kwargs):
        """Add to list of cats"""
        return self.cat.select.add(*args, **kwargs)

    @property
    def sources(self):
        """Sources that have been selected from the source_browser"""
        return self.source.sources

    @property
    def item(self):
        """Item that is selected"""
        if len(self.sources) == 0:
            return None
        return self.sources[0]
