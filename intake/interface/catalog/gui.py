# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
from functools import partial

import panel as pn

from ..base import MAX_WIDTH, Base, enable_widget
from .add import CatAdder
from .search import Search
from .select import CatSelector


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
        self.panel = pn.Column(name="Catalogs", width_policy="max", max_width=MAX_WIDTH)
        self.done_callback = done_callback or (lambda x: x)

        self.add_widget = pn.widgets.Toggle(name="＋", value=False, disabled=False, width=50)

        self.remove_widget = pn.widgets.Button(name="―", disabled=True, width=50)

        self.search_widget = pn.widgets.Toggle(name="🔍", value=False, disabled=True, width=50)

        self.controls = [
            self.add_widget,
            self.remove_widget,
            self.search_widget,
        ]
        self.control_panel = pn.Row(name="Controls", margin=0)

        self.select = CatSelector(cats=self._cats, done_callback=self.callback)
        self.add = CatAdder(done_callback=self.select.add, visible=self.add_widget.value, visible_callback=partial(setattr, self.add_widget, "value"))
        self.search = Search(
            cats=self.cats, done_callback=self.select.add, visible=self.search_widget.value, visible_callback=partial(setattr, self.search_widget, "value")
        )

        self.children = [
            self.select.panel,
            self.control_panel,
            self.search.panel,
            self.add.panel,
        ]

        super().__init__(**kwargs)

    def setup(self):
        self.watchers = [
            self.add_widget.link(self.add, value="visible"),
            self.search_widget.param.watch(self.on_click_search_widget, "value"),
            self.remove_widget.param.watch(self.select.remove_selected, "clicks"),
        ]

    @Base.visible.setter
    def visible(self, visible):
        """When visible changed, do setup or unwatch and call visible_callback"""
        self._visible = visible

        if visible and len(self._panel.objects) == 0:
            self.setup()
            self.select.visible = True
            self.control_panel.extend(self.controls)
            self._panel.extend(self.children)
        elif not visible and len(self._panel.objects) > 0:
            self.unwatch()
            # do children
            self.select.visible = False
            self.control_panel.clear()
            self.search.visible = False
            self.add.visible = False
            self._panel.clear()
        if self.visible_callback:
            self.visible_callback(visible)

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
        """When the search control is toggled, set visibility and hand down cats"""
        self.search.cats = self.cats
        self.search.visible = event.new
        if self.search.visible:
            self.search.watchers.append(self.select.widget.link(self.search, value="cats"))

    @property
    def cats(self):
        """Cats that have been selected from the select"""
        return self.select.selected

    def __getstate__(self):
        """Serialize the current state of the object"""
        return {
            "visible": self.visible,
            "select": self.select.__getstate__(),
            "add": self.add.__getstate__(),
            "search": self.search.__getstate__(include_cats=False),
        }

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        self.visible = state.get("visible", True)
        if self.visible:
            self.select.__setstate__(state["select"])
            self.add.__setstate__(state["add"])
            self.search.__setstate__(state["search"])
        return self

    @classmethod
    def from_state(cls, state):
        """Create a new object from a serialized verion of another one.

        Example
        -------
        original = CatGUI()
        copy = CatGUI.from_state(original.__getstate__())
        """
        return cls(cats=[]).__setstate__(state)
