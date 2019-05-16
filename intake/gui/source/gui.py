#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from functools import partial
import panel as pn
from intake.utils import remake_instance

from ..base import Base, enable_widget, MAX_WIDTH
from .select import SourceSelector
from .defined_plots import DefinedPlots
from .description import Description


class SourceGUI(Base):
    """
    Top level GUI panel that contains controls and all visible sub-panels

    This class is responsible for coordinating the inputs and outputs
    of various sup-panels and their effects on each other.

    Parameters
    ----------
    cats: list of catalogs, opt
        catalogs used to initalize, provided as objects.
    sources: list of sources, opt
        sources used to initalize, provided as objects.
    done_callback: func, opt
        called when the object's main job has completed. In this case,
        selecting source(s).

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
    def __init__(self, cats=None, sources=None, done_callback=None, **kwargs):
        self._cats = cats
        self._sources = sources
        self.panel = pn.Column(name='Entries', width_policy='max', max_width=MAX_WIDTH)
        self.done_callback = done_callback

        self.plot_widget = pn.widgets.Toggle(
            name='📊',
            value=False,
            disabled=True,
            width=50)
        self.controls = [self.plot_widget]
        self.control_panel = pn.Row(name='Controls', margin=0)

        self.select = SourceSelector(cats=self._cats,
                                     sources=self._sources,
                                     done_callback=self.callback)
        self.description = Description(source=self.sources)

        self.plot = DefinedPlots(source=self.sources,
                                 visible=self.plot_widget.value,
                                 visible_callback=partial(setattr, self.plot_widget, 'value'))

        super().__init__(**kwargs)

    def setup(self):
        self.watchers = [
            self.plot_widget.param.watch(self.on_click_plot_widget, 'value'),
            self.select.widget.link(self.description, value='source'),
        ]

        self.children = [
            pn.Row(
                pn.Column(
                    self.select.panel,
                    self.control_panel,
                    margin=0
                ),
                self.description.panel,
                margin=0
            ),
            self.plot.panel,
        ]

    @Base.visible.setter
    def visible(self, visible):
        """When visible changed, do setup or unwatch and call visible_callback"""
        self._visible = visible

        if visible and len(self._panel.objects) == 0:
            self.setup()
            self.select.visible = True
            self.description.visible = True
            if len(self.control_panel.objects) == 0:
                self.control_panel.extend(self.controls)
            self._panel.extend(self.children)
        elif not visible and len(self._panel.objects) > 0:
            self.unwatch()
            # do children
            self.select.visible = False
            self.control_panel.clear()
            self.description.visible = False
            self.plot.visible = False
            self._panel.clear()
        if self.visible_callback:
            self.visible_callback(visible)

    def callback(self, sources):
        """When a source is selected, enable widgets that depend on that condition
        and do done_callback"""
        enable = bool(sources)
        if not enable:
            self.plot_widget.value = False
        enable_widget(self.plot_widget, enable)

        if self.done_callback:
            self.done_callback(sources)

    def on_click_plot_widget(self, event):
        """ When the plot control is toggled, set visibility and hand down source"""
        self.plot.source = self.sources
        self.plot.visible = event.new
        if self.plot.visible:
            self.plot.watchers.append(
                self.select.widget.link(self.plot, value='source'))

    @property
    def sources(self):
        """Sources that have been selected from the source GUI"""
        return self.select.selected

    def __getstate__(self):
        """Serialize the current state of the object"""
        return {
            'visible': self.visible,
            'select': self.select.__getstate__(),
            'description': self.description.__getstate__(include_source=False),
            'plot':  self.plot.__getstate__(include_source=False),
        }

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        self.visible = state.get('visible', True)
        if self.visible:
            self.select.__setstate__(state['select'])
            self.description.__setstate__(state['description'])
            self.plot.__setstate__(state['plot'])
        return self

    @classmethod
    def from_state(cls, state):
        """Create a new object from a serialized exising object.

        Example
        -------
        original = SourceGUI()
        copy = SourceGUI.from_state(original.__getstate__())
        """
        return cls(cats=[], sources=[]).__setstate__(state)
