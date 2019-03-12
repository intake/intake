#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import panel as pn

from .base import Base


class SearchInputs(Base):
    """Input areas to control search parameters"""
    def __init__(self, visible=True):
        self.panel = pn.Row(name='Search Inputs')
        self.visible = visible

    def setup(self):
        self.text_input = pn.widgets.TextInput(
            placeholder="Set of words",
            width=400)
        self.depth_input = pn.widgets.Select(
            options=['1', '2', '3', '4', '5', 'All'],
            width=80)

        self.children = ['Search Text:', self.text,
                         'Depth:', self.depth]

    @property
    def text(self):
        return self.text_input.value

    @property
    def depth(self):
        return int(self.depth_input.value) if self.depth_input.value != 'All' else 99


class Search(Base):
    """Input is a list of catalogs and output is a list of new search catalogs"""
    def __init__(self, cats, visible=True, done_callback=None):
        self.cats = cats
        self.done_callback = done_callback
        self.panel = pn.Row(name='Search')
        self.visible = visible

    def setup(self):
        self.search_inputs = SearchInputs(visible=self.visible)
        self.widget = pn.widgets.Button(name='🔍', width=30)

        self.watchers = [
            self.widget.param.watch(self.do_search, 'clicks')
        ]

        self.children = [self.search_inputs.panel, self.widget]

    def do_search(self, arg=None):
        new_cats = []
        for cat in self.cats:
            new_cat = cat.search(self.search_inputs.text,
                                 depth=self.search_inputs.depth)
            if len(list(new_cat)) > 0:
                new_cats.append(new_cat)
        if len(new_cats) > 0:
            self.done_callback(new_cats)
