#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from copy import deepcopy

import panel as pn
from ..base import Base, pretty_describe


class DefinedPlots(Base):
    """
    Panel for displaying pre-defined plots from catalog.

    Parameters
    ----------
    source: intake catalog entry, or list of same
        source to describe in this object

    Attributes
    ----------
    plot: holoviews object
        plot object displayed in plot_pane
    has_plots: bool
        whether the source has plots defined
    instructions_contents: str
        instructions to put on the plot selector label
    options: list
        plots options defined on the source
    selected: str
        name of selected plot
    children: list of panel objects
        children that will be used to populate the panel when visible
    panel: panel layout object
        instance of a panel layout (row or column) that contains children
        when visible
    watchers: list of param watchers
        watchers that are set on children - cleaned up when visible
        is set to false.
    """
    select = None

    def __init__(self, source=None, **kwargs):
        self.source = source
        self.panel = pn.Column(name='Plot', margin=0)
        super().__init__(**kwargs)

    def setup(self):
        self.instructions = pn.pane.Markdown(self.instructions_contents)
        self.select = pn.widgets.Select(options=self.options)
        self.desc = pn.pane.Str()
        self.pane = pn.pane.HoloViews(self._plot_object(self.selected))
        self.show_desc = pn.widgets.Checkbox(value=False, width_policy='min')

        self.watchers = [
            self.select.param.watch(self.callback, ['options','value']),
            self.show_desc.param.watch(self._toggle_desc, 'value')
        ]

        self.children = [
            self.instructions,
            pn.Row(
                self.select,
                self.show_desc,
                "show yaml",
            ),
            self.desc,
            self.pane,
        ]

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        """When the source gets updated, update the select widget"""
        if isinstance(source, list):
            # if source is a list, get first item or None
            source = source[0] if len(source) > 0 else None
        self._source = source
        if self.select:
            self.select.options = self.options

    @property
    def has_plots(self):
        """Whether the source has plots defined"""
        return self.source is not None and len(self._source.plots) > 0

    @property
    def instructions_contents(self):
        """Instructions to put on the plot selector label"""
        if self.has_plots:
            return '**Select from the predefined plots:**'
        return '*No predefined plots found - declare these in the catalog*'

    @property
    def options(self):
        """Plots options defined on the source"""
        return self.source.plots if self.source is not None else []

    @property
    def selected(self):
        """Name of selected plot"""
        return self.select.value if self.select is not None else None

    @selected.setter
    def selected(self, selected):
        """When plot is selected set, make sure widget stays uptodate"""
        self.select.value = selected

    def callback(self, *events):
        for event in events:
            if event.name == 'value':
                if self.show_desc.value:
                    self.desc.object = self._desc_contents(event.new)
                self.pane.object = self._plot_object(event.new)
            if event.name == 'options':
                self.instructions.object = self.instructions_contents

    @property
    def plot(self):
        """Holoviews plot object displayed in plot_pane"""
        return self.plot_pane.object

    def _plot_object(self, selected):
        if selected:
            plot_method = getattr(self.source.plot, selected)
            if plot_method:
                return plot_method()

    def _desc_contents(self, selected):
        if selected:
            contents = self.source.metadata['plots'][selected]
            return pretty_describe(contents)

    def _toggle_desc(self, event):
        if event.new:
            self.desc.object = self._desc_contents(self.selected)
        else:
            self.desc.object = None
