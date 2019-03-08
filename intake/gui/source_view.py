#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

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
        contents = self.source.describe().copy()
        try:
            contents.update(self.source.describe_open())
            return pretty_describe(contents)
        except ValueError:
            warning = f'Need additional plugin to use {self.source._driver} driver'
            return pretty_describe(contents) + '\n' + warning
