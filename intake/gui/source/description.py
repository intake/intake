#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import panel as pn
from ..base import BaseView
from ...utils import pretty_describe


class Description(BaseView):
    """
    Class for displaying a textual description of a data source.

    Parameters
    ----------
    source: intake catalog entry, or list of same
        source to describe in this object

    Attributes
    ----------
    contents: str
        string representation of the source's description
    label: str
        label to display at top of panel - contains name of source
    children: list of panel objects
        children that will be used to populate the panel when visible
    panel: panel layout object
        instance of a panel layout (row or column) that contains children
        when visible
    watchers: list of param watchers
        watchers that are set on children - cleaned up when visible
        is set to false.
    """
    main_pane = None

    def __init__(self, source=None, **kwargs):
        self.source = source
        self.panel = pn.Column(name='Description', width_policy='max',
                               margin=0, height=240, sizing_mode='stretch_width',
                               scroll=True)
        super().__init__(**kwargs)

    def setup(self):
        self.main_pane = pn.pane.Markdown(self.contents)
        self.children = [self.main_pane]

    @BaseView.source.setter
    def source(self, source):
        """When the source gets updated, update the pane object"""
        BaseView.source.fset(self, source)
        if self.main_pane:
            self.main_pane.object = """```yaml\n{}\n```""".format(self.contents)

    @property
    def contents(self):
        """String representation of the source's description"""
        if not self._source:
            return 'name: ' + "⠀" * 30
        contents = self.source.describe()
        return pretty_describe(contents)
