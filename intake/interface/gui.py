#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import intake
import panel as pn

from .base import Base, MAX_WIDTH, ICONS
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
        self.panel = pn.Column(
            pn.Row(
                pn.panel(ICONS['logo']),
                pn.Column(
                    self.cat.select.panel,
                    self.cat.control_panel,
                    margin=0, width_policy='max'),
                pn.Column(
                    self.source.select.panel,
                    self.source.control_panel,
                    margin=0, width_policy='max'
                ),
                self.source.description.panel,
                margin=0, width_policy='max'
            ),
            pn.Row(self.cat.search.panel, self.cat.add.panel,
                   self.source.plot.panel, width_policy='max'),
            width_policy='max'
        )
        super(GUI, self).__init__()

    def done_callback(self, cats):
        self.source.select.cats = cats

    @property
    def cats(self):
        """Cats that have been selected from the cat sub-panel"""
        return self.cat.cats

    def add(self, *args, **kwargs):
        """Add to list of cats"""
        return self.cat.select.add(*args, **kwargs)

    @property
    def sources(self):
        """Sources that have been selected from the source sub-panel"""
        return self.source.sources

    @property
    def source_instance(self):
        """DataSource instance for the current selection and any parameters"""
        return self.source.source_instance

    @property
    def item(self):
        """Item that is selected"""
        if len(self.sources) == 0:
            return None
        return self.sources[0]

    def __getstate__(self):
        """Serialize the current state of the object"""
        return {
            'visible': self.visible,
            'cat': self.cat.__getstate__(),
            'source': self.source.__getstate__(),
        }

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        self.visible = state.get('visible', True)
        self.cat.__setstate__(state['cat'])
        self.source.__setstate__(state['source'])
        return self

    @classmethod
    def from_state(cls, state):
        """Create a new object from a serialized exising object.

        Example
        -------
        original = GUI()
        copy = GUI.from_state(original.__getstate__())
        """
        return cls(cats=[]).__setstate__(state)
