#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import panel as pn

from ..base import Base, MAX_WIDTH, BACKGROUND


class SearchInputs(Base):
    """Input areas to control search parameters

    Attributes
    ----------
    text: str
        text to use in search. Displayed in text_widget
    depth: int or 'All'
        depth of nestedness to use in search. Displayed in depth_widget.
    children: list of panel objects
        children that will be used to populate the panel when visible
    panel: panel layout object
        instance of a panel layout (row or column) that contains children
        when visible
    watchers: list of param watchers
        watchers that are set on children - cleaned up when visible
        is set to false.
    """
    def __init__(self, **kwargs):
        self.panel = pn.Row(name='Search Inputs', height_policy='min', margin=0)
        super().__init__(**kwargs)

    def setup(self):
        text_label = pn.pane.Markdown('Search Text:', align='center')
        depth_label = pn.pane.Markdown('Depth:', align='center')
        self.text_widget = pn.widgets.TextInput(
            placeholder="Set of words",
            width_policy='max',
            align='center')
        self.depth_widget = pn.widgets.Select(
            options=['1', '2', '3', '4', '5', 'All'],
            width=80, height=30,
            align='center')

        self.children = [text_label, self.text_widget,
                         depth_label, self.depth_widget]

    @property
    def text(self):
        """Text to use in search. Displayed in text_widget"""
        return self.text_widget.value

    @text.setter
    def text(self, text):
        self.text_widget.value = text

    @property
    def depth(self):
        """Depth of nestedness to use in search. Displayed in depth_widget"""
        return int(self.depth_widget.value) if self.depth_widget.value != 'All' else 99

    @depth.setter
    def depth(self, depth):
        self.depth_widget.value = depth


class Search(Base):
    """Search panel for searching a list of catalogs

    Parameters
    ----------
    cats: list of catalogs
        catalogs that will be searched
    done_callback: function with cats as input
        function that is called when new cats have been generated
        via the search functionality

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
    def __init__(self, cats, done_callback=None, **kwargs):
        self.cats = cats
        self.done_callback = done_callback
        self.panel = pn.Row(name='Search', background=BACKGROUND,
                            height_policy='min',
                            width_policy='max', max_width=MAX_WIDTH, margin=0)
        super().__init__(**kwargs)

    def setup(self):
        self.inputs = SearchInputs()
        self.widget = pn.widgets.Button(name='🔍', width=50, align='center')

        self.watchers = [
            self.widget.param.watch(self.do_search, 'clicks')
        ]

        self.children = [self.inputs.panel, self.widget]

    def do_search(self, arg=None):
        """Do search and close panel"""
        new_cats = []
        for cat in self.cats:
            new_cat = cat.search(self.inputs.text,
                                 depth=self.inputs.depth)
            if len(list(new_cat)) > 0:
                new_cats.append(new_cat)
        if len(new_cats) > 0:
            self.done_callback(new_cats)
            self.visible = False
