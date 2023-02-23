# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import panel as pn

import intake
from intake.utils import remake_instance

from ..base import BaseSelector


class CatSelector(BaseSelector):
    """
    The cat selector takes a variety of inputs such as a catalog instance,
    a path to a catalog, or a list of either of those.

    Once the cat selector is populated with these options, the user can
    select which catalog(s) are of interest. These catalogs are stored on
    the ``selected`` property of the class.

    Parameters
    ----------
    cats: list of catalogs, opt
        catalogs used to initalize, can be provided as objects or
        urls pointing to local or remote catalogs.
    done_callback: func, opt
        called when the object's main job has completed. In this case,
        selecting catalog(s).

    Attributes
    ----------
    selected: list of cats
        list of selected cats
    items: list of cats
        list of all the catalog values represented in widget
    labels: list of str
        list of labels for all the catalog represented in widget
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

    children = []

    @classmethod
    def preprocess(cls, cat):
        """Function to run on each cat input"""
        if isinstance(cat, str):
            cat = intake.open_catalog(cat)
        return cat

    def __init__(self, cats=None, done_callback=None, **kwargs):
        """Set cats to initialize the class.

        The order of the calls in this method matters and is different
        from the order in other panel init methods because the top level
        gui class needs to be able to watch these widgets.
        """
        self.panel = pn.Column(name="Select Catalog", margin=0)
        self.widget = pn.widgets.MultiSelect(size=9, min_width=200, width_policy="min")
        self.done_callback = done_callback
        super().__init__(**kwargs)

        self.items = cats if cats is not None else [intake.cat]

    def setup(self):
        label = pn.pane.Markdown("#### Catalogs", max_height=40)
        self.watchers = [
            self.widget.param.watch(self.callback, "value"),
        ]

        self.children = [label, self.widget]

    def callback(self, event):
        self.expand_nested(event.new)
        if self.done_callback:
            self.done_callback(event.new)

    def expand_nested(self, cats):
        """Populate widget with nested catalogs"""
        down = "│"
        right = "└──"

        def get_children(parent):
            out = []
            for k, e in parent._entries.items():
                try:
                    if e.describe()["container"] == "catalog":
                        out.append(e())
                except IOError:
                    # may fail to load
                    pass
            return out

        if len(cats) == 0:
            return

        cat = cats[0]
        old = list(self.options.items())
        name = next(k for k, v in old if v == cat)
        index = next(i for i, (k, v) in enumerate(old) if v == cat)
        if right in name:
            prefix = f"{name.split(right)[0]}{down} {right}"
        else:
            prefix = right

        children = get_children(cat)
        for i, child in enumerate(children):
            old.insert(index + i + 1, (f"{prefix} {child.name}", child))
        self.widget.options = dict(old)

    def collapse_nested(self, cats, max_nestedness=10):
        """
        Collapse any items that are nested under cats.
        `max_nestedness` acts as a fail-safe to prevent infinite looping.
        """
        children = []
        removed = set()
        nestedness = max_nestedness

        old = list(self.widget.options.values())
        nested = [cat for cat in old if getattr(cat, "cat") is not None]
        parents = {cat.cat for cat in nested}
        parents_to_remove = cats
        while len(parents_to_remove) > 0 and nestedness > 0:
            for cat in nested:
                if cat.cat in parents_to_remove:
                    children.append(cat)
            removed = removed.union(parents_to_remove)
            nested = [cat for cat in nested if cat not in children]
            parents_to_remove = {c for c in children if c in parents - removed}
            nestedness -= 1
        self.remove(children)

    def remove_selected(self, *args):
        """Remove the selected catalog - allow the passing of arbitrary
        args so that buttons work. Also remove any nested catalogs."""
        self.collapse_nested(self.selected)
        self.remove(self.selected)

    def __getstate__(self):
        """Serialize the current state of the object"""
        return {
            "visible": self.visible,
            "labels": self.labels,
            "cats": [cat.__getstate__() for cat in self.items],
            "selected": [k for k, v in self.options.items() if v in self.selected],
        }

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        cats, labels = state["cats"], state["labels"]
        self.widget.options = {label: remake_instance(cat) for label, cat in zip(labels, cats)}
        self.selected = state.get("selected", [])
        self.visible = state.get("visible", True)
        return self
