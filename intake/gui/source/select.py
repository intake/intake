#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from collections import OrderedDict

import intake
from intake.utils import remake_instance
import panel as pn

from ..base import BaseSelector, coerce_to_list


class SourceSelector(BaseSelector):
    """
    The source selector takes a variety of inputs such as cats or sources
    and uses those to populate a select widget containing all the sources.

    Once the source selector is populated with these options, the user can
    select which source(s) are of interest. These sources are stored on
    the ``selected`` property of the class.

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
    selected: list of sources
        list of selected sources
    items: list of sources
        list of all the source values represented in widget
    labels: list of str
        list of labels for all the sources represented in widget
    options: dict
        dict of widget labels and values (same as `dict(zip(self.labels, self.values))`)
    children: list of panel objects
        children that will be used to populate the panel when visible
    panel: panel layout object
        instance of a panel layout (row or column) that contains children
        when visible
    watchers: list of param watchers
        watchers that are set on children - cleaned up when visible
        is set to false.
    """
    preprocess = None
    children = []

    def __init__(self, sources=None, cats=None, done_callback=None, **kwargs):
        """Set sources or cats to initialize the class - sources trumps cats.

        The order of the calls in this method matters and is different
        from the order in other panel init methods because the top level
        gui class needs to be able to watch these widgets.
        """
        self.panel = pn.Column(name='Select Data Source', margin=0)
        self.widget = pn.widgets.MultiSelect(size=9, min_width=200, width_policy='min')
        self.done_callback = done_callback
        super().__init__(**kwargs)

        if sources is not None:
            self.items = sources
        elif cats is not None:
            self.cats = cats

    def setup(self):
        label = pn.pane.Markdown('#### Sources', max_height=40)
        self.watchers = [
            self.widget.param.watch(self.callback, 'value'),
        ]
        self.children = [label, self.widget]

    @property
    def cats(self):
        """Cats represented in the sources options"""
        return set(source._catalog for source in self.items)

    @cats.setter
    def cats(self, cats):
        """Set sources from a list of cats"""
        sources = []
        for cat in coerce_to_list(cats):
            sources.extend([entry for k, entry in cat.items()
                            if entry.describe()['container'] != 'catalog'])
        self.items = sources

    def callback(self, event):
        if self.done_callback:
            self.done_callback(event.new)

    def __getstate__(self):
        """Serialize the current state of the object"""
        return {
            'visible': self.visible,
            'labels': self.labels,
            'sources': [source.__getstate__() for source in self.items],
            'selected': [k for k, v in self.options.items() if v in self.selected],
        }

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        sources = state['sources']
        labels = state['labels']
        self.widget.options = {l: remake_instance(s) for l, s in zip(labels, sources)}
        self.selected = state.get('selected', [])
        self.visible = state.get('visible', True)
        return self
