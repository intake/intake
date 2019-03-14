#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from copy import deepcopy

import panel as pn
from .base import Base


def pretty_describe(object, nestedness=0, indent=2):
    """Maintain dict ordering - but make string version prettier"""
    if not isinstance(object, dict):
        return str(object)
    sep = f'\n{" " * nestedness * indent}'
    return sep.join((f'{k}: {pretty_describe(v, nestedness + 1)}' for k, v in object.items()))


class Description(Base):
    def __init__(self, source=None, visible=True):
        self._source = source
        self.panel = pn.Column(name='Description')
        self.visible = visible

    def setup(self):
        self.pane = pn.pane.Str(self.contents, sizing_mode='stretch_width')
        self.children = [self.pane]

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        """When the source gets updated, update the pane object"""
        if isinstance(source, list):
            # if source is a list, get first item or None
            source = source[0] if len(source) > 0 else None
        if source != self._source:
            self._source = source
            self.pane.object = self.contents
        return self._source

    @property
    def contents(self):
        if not self._source:
            return ''
        contents = deepcopy(self.source.describe())
        try:
            extra = deepcopy(self.source.describe_open())
            contents.update(extra)
            if 'plots' in contents['metadata']:
                contents['metadata'].pop('plots')
            if 'plots' in contents['args'].get('metadata', {}):
                contents['args']['metadata'].pop('plots')
            return pretty_describe(contents)
        except ValueError:
            warning = f'Need additional plugin to use {self.source._driver} driver'
            return pretty_describe(contents) + '\n' + warning


class DefinedPlots(Base):
    plot = None
    select = None

    def __init__(self, source=None, visible=True):
        self.source = source
        self.panel = pn.Row(name='Plot')
        self.visible = visible
        if not visible:
            self.setup()

    def setup(self):
        self.select = pn.widgets.Select(options=self.options)
        self.plot_desc = pn.pane.Str(self.plot_desc_contents)
        self.update_plot()

        self.watchers = [
            self.select.param.watch(self.callback, 'value')
        ]

        self.children = [
            pn.Column(
                self.instructions,
                self.select,
                self.plot_desc),
            self.plot
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
            self.select.value = None

    @property
    def has_plots(self):
        return self.source is not None and len(self._source.plots) > 0

    @property
    def instructions(self):
        if self.has_plots:
            return '**Select from the predefined plots:**'
        return '*No predefined plots found - declare these in the catalog*'

    @property
    def options(self):
        return self.source.plots if self.source is not None else []

    def callback(self, event):
        print('EVENT:', event)
        self.plot_desc.object = self.plot_desc_contents
        self.update_plot()
        if self.children[-1] != self.plot:
            self.children[-1] = self.plot
            if self.visible:
                self.panel.objects = self.children

    def update_plot(self):
        if isinstance(self.plot, pn.pane.HoloViews):
            self.plot.object = self.plot_object
        elif self.select.value:
            self.plot = pn.pane.HoloViews(self.plot_object)
        else:
            self.plot = pn.pane.Pane('')

    @property
    def plot_desc_contents(self):
        if not self.select.value:
            return ''
        if self.select.value:
            contents = self.source.metadata['plots'][self.select.value]
            return pretty_describe(contents)

    @property
    def plot_object(self):
        if self.select.value:
            plot_method = getattr(self.source.plot, self.select.value)
            if plot_method:
                return plot_method()

