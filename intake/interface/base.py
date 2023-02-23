# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
import os
from collections import OrderedDict

import panel as pn

MAX_WIDTH = 5000

here = os.path.abspath(os.path.dirname(__file__))
ICONS = {
    "logo": os.path.join(here, "icons", "logo.png"),
    "error": os.path.join(here, "icons", "baseline-error-24px.svg"),
}


def enable_widget(widget, enable=True):
    """Set disabled on widget"""
    widget.disabled = not enable


def coerce_to_list(items, preprocess=None):
    """Given an instance or list, coerce to list.

    With optional preprocessing.
    """
    if not isinstance(items, list):
        items = [items]
    if preprocess:
        items = list(map(preprocess, items))
    return items


class Base(object):
    """
    Base class for composable panel objects that make up the GUI.

    Parameters
    ----------
    children: list of panel objects
        children that will be used to populate the panel when visible
    panel: panel layout object
        instance of a panel layout (row or column) that contains children
        when visible
    watchers: list of param watchers
        watchers that are set on children - cleaned up when visible
        is set to false.
    visible: bool
        whether or not the instance should be visible. When not visible
        ``panel`` is empty.
    logo : bool, opt
        whether to show the intake logo in a panel to the left of the main
        panel. Default is False
    """

    children = None
    panel = None
    watchers = None
    visible_callback = None
    logo_panel = pn.Column(pn.pane.PNG(ICONS["logo"], align="center"), margin=(25, 0, 0, 0), width=50)
    logo = False

    def __init__(self, visible=True, visible_callback=None, logo=False):
        self.visible = visible
        self.visible_callback = visible_callback
        self.logo = logo

    @property
    def panel(self):
        if not self.logo:
            return self._panel
        return pn.Row(self.logo_panel, self._panel, margin=0)

    @panel.setter
    def panel(self, panel):
        self._panel = panel

    def servable(self, *args, **kwargs):
        return self.panel.servable(*args, **kwargs)

    def show(self, *args, **kwargs):
        return self.panel.show(*args, **kwargs)

    def __repr__(self):
        """Print output"""
        return self.panel.__repr__()

    def _repr_mimebundle_(self, *args, **kwargs):
        """Display in a notebook or a server"""
        try:
            return self.panel._repr_mimebundle_(*args, **kwargs)
        except Exception:
            raise NotImplementedError("Panel does not seem to be set " "up properly")

    def setup(self):
        """Should instantiate widgets, set ``children``, and set watchers"""
        raise NotImplementedError

    @property
    def visible(self):
        """Whether or not the instance should be visible."""
        return self._visible

    @visible.setter
    def visible(self, visible):
        """When visible changed, do setup or unwatch and call visible_callback"""
        self._visible = visible
        pan = getattr(self._panel, "_layout", self._panel)
        if visible and len(pan.objects) == 0:
            self.setup()
            self._panel.extend(self.children)
        elif not visible and len(pan.objects) > 0:
            self.unwatch()
            pan.clear()
        if self.visible_callback:
            self.visible_callback(visible)

    def unwatch(self):
        """Get rid of any lingering watchers and remove from list"""
        if self.watchers is not None:
            unwatched = []
            for watcher in self.watchers:
                watcher.inst.param.unwatch(watcher)
                unwatched.append(watcher)
            self.watchers = [w for w in self.watchers if w not in unwatched]

    def __getstate__(self):
        """Serialize the current state of the object"""
        return {"visible": self.visible}

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        self.visible = state.get("visible", True)
        return self

    @classmethod
    def from_state(cls, state):
        """Create a new object from a serialized exising object.

        Example
        -------
        original = cls()
        copy = cls.from_state(original.__getstate__())
        """
        return cls().__setstate__(state)


class BaseSelector(Base):
    """Base class for capturing selector logic.

    Parameters
    ----------
    preprocess: function
        run on every input value when creating options
    widget: panel widget
        selector widget which this class keeps uptodate with class properties
    """

    preprocess = None
    widget = None

    @property
    def labels(self):
        """Labels of items in widget"""
        return self.widget.labels

    @property
    def items(self):
        """Available items to select from"""
        return self.widget.values

    @items.setter
    def items(self, items):
        """When setting items make sure widget options are uptodate"""
        if items is not None:
            self.options = items

    def _create_options(self, items):
        """Helper method to create options from list, or instance.

        Applies preprocess method if available to create a uniform
        output
        """
        return OrderedDict(map(lambda x: (x.name, x), coerce_to_list(items, self.preprocess)))

    @property
    def options(self):
        """Options available on the widget"""
        return self.widget.options

    @options.setter
    def options(self, new):
        """Set options from list, or instance of named item

        Over-writes old options
        """
        options = self._create_options(new)
        if self.widget.value:
            self.widget.set_param(options=options, value=list(options.values())[:1])
        else:
            self.widget.options = options
            self.widget.value = list(options.values())[:1]

    def add(self, items):
        """Add items to options"""
        options = self._create_options(items)
        for k, v in options.items():
            if k in self.labels and v not in self.items:
                options.pop(k)
                count = 0
                while f"{k}_{count}" in self.labels:
                    count += 1
                options[f"{k}_{count}"] = v
        self.widget.options.update(options)
        self.widget.param.trigger("options")
        self.widget.value = list(options.values())[:1]

    def remove(self, items):
        """Remove items from options"""
        items = coerce_to_list(items)
        new_options = {k: v for k, v in self.options.items() if v not in items}
        self.widget.options = new_options
        self.widget.param.trigger("options")

    @property
    def selected(self):
        """Value selected on the widget"""
        return self.widget.value

    @selected.setter
    def selected(self, new):
        """Set selected from list or instance of object or name.

        Over-writes existing selection
        """

        def preprocess(item):
            if isinstance(item, str):
                return self.options[item]
            return item

        items = coerce_to_list(new, preprocess)
        self.widget.value = items


class BaseView(Base):
    def __getstate__(self, include_source=True):
        """Serialize the current state of the object. Set include_source
        to False when using with another panel that will include source."""
        if include_source:
            return {
                "visible": self.visible,
                "label": self.source._name,
                "source": self.source.__getstate__(),
            }
        else:
            return {"visible": self.visible}

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        if "source" in state:
            self.source = state["source"]
        self.visible = state.get("visible", True)

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
