#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from functools import partial

import panel as pn

from ..base import Base, logo, enable_widget, MAX_WIDTH
from .select import CatSelector
from .add import CatAdder
from .search import Search


class CatGUI(Base):
    """
    Top level GUI panel that contains controls and all visible sub-panels

    This class is responsible for coordinating the inputs and outputs
    of various sup-panels and their effects on each other.

    Parameters
    ----------
    cats: list of catalogs
        catalogs used to initalize the select
    done_callback: func, opt
        called when the object's main job has completed. In this case,
        selecting catalog(s).

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
    def __init__(self, cats=None, done_callback=None, **kwargs):
        self._cats = cats
        self.panel = pn.Column(name='Catalogs', width_policy='max', max_width=MAX_WIDTH)
        self.done_callback = done_callback or (lambda x: x)
        super().__init__(**kwargs)

    def setup(self):
        self.add_widget = pn.widgets.Toggle(
            name='＋',
            value=False,
            disabled=False,
            width=50)

        self.remove_widget = pn.widgets.Button(
            name='―',
            disabled=True,
            width=50)

        self.search_widget = pn.widgets.Toggle(
            name='🔍',
            value=False,
            disabled=True,
            width=50)

        self.select = CatSelector(cats=self._cats,
                                  done_callback=self.callback)
        self.add = CatAdder(done_callback=self.select.add,
                            visible=self.add_widget.value,
                            visible_callback=partial(setattr, self.add_widget, 'value'))
        self.search = Search(cats=self.cats,
                             done_callback=self.select.add,
                             visible=self.search_widget.value,
                             visible_callback=partial(setattr, self.search_widget, 'value'))

        self.watchers = [
            self.add_widget.link(self.add, value='visible'),
            self.search_widget.param.watch(self.on_click_search_widget, 'value'),
            self.remove_widget.param.watch(self.select.remove_selected, 'clicks'),
        ]
        self.control_panel = pn.Row(
            self.add_widget,
            self.remove_widget,
            self.search_widget,
            margin=0
        )

        self.children = [
            self.select.panel,
            self.control_panel,
            self.search.panel,
            self.add.panel,
        ]

    def callback(self, cats):
        """When a catalog is selected, enable widgets that depend on that condition
        and do done_callback"""
        enable = bool(cats)
        if not enable:
            # close search if it is visible
            self.search.visible = False
        enable_widget(self.search_widget, enable)
        enable_widget(self.remove_widget, enable)

        if self.done_callback:
            self.done_callback(cats)

    def on_click_search_widget(self, event):
        """ When the search control is toggled, set visibility and hand down cats"""
        self.search.cats = self.cats
        self.search.visible = event.new
        if self.search.visible:
            self.search.watchers.append(
                self.select.widget.link(self.search, value='cats'))

    @property
    def cats(self):
        """Cats that have been selected from the select"""
        return self.select.selected
