#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import panel as pn
from param.parameterized import Event

MAX_WIDTH = 1200
BACKGROUND = '#eeeeee'


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
    dependent_widgets: list of widgets
        widgets that should be enabled when the instance's action completes
    control_widget: widget
        widget tied to visible property of class. When value changes on widget
        visible value changes as well.
    visible: bool
        whether or not the instance should be visible. When not visible
        ``panel`` is empty.
    """
    children = None
    panel = None
    watchers  = None
    dependent_widgets = None
    control_widget = None

    def __init__(self, visible=True, control_widget=None, dependent_widgets=None):
        self.dependent_widgets = dependent_widgets or []
        self.control_widget = control_widget
        self.visible = visible if control_widget is None else control_widget.value

    def __repr__(self):
        """Print output"""
        try:
            return self.panel.__repr__()
        except:
            raise RuntimeError("Panel does not seem to be set up properly")


    def _repr_mimebundle_(self, *args, **kwargs):
        """Display in a notebook or a server"""
        try:
            return self.panel._repr_mimebundle_(*args, **kwargs)
        except:
            raise RuntimeError("Panel does not seem to be set up properly")

    def setup(self):
        """Should instantiate widgets, set ``children``, and set watchers"""
        raise NotImplementedError

    @property
    def visible(self):
        """Whether or not the instance should be visible."""
        return self._visible

    @visible.setter
    def visible(self, visible):
        """When visible changed, do setup or unwatch and teardown"""
        self._visible = visible
        if visible and len(self.panel.objects) == 0:
            self.setup()
            self.panel.extend(self.children)
        elif not visible and len(self.panel.objects) > 0:
            self.unwatch()
            self.teardown()
            self.panel.clear()
        if self.control_widget:
            self.control_widget.value = visible

    def enable_dependents(self, enable=True):
        """Set disabled on all dependent widgets"""
        if self.dependent_widgets:
            if isinstance(enable, Event):
                enable = bool(enable.new)
            for widget in self.dependent_widgets:
                widget.disabled = not enable

    def teardown(self):
        """Should clean up any dependent widgets that setup appends"""
        pass

    def unwatch(self):
        """Get rid of any lingering watchers and remove from list"""
        if self.watchers is not None:
            unwatched = []
            for watcher in self.watchers:
                watcher.inst.param.unwatch(watcher)
                unwatched.append(watcher)
            self.watchers = [w for w in self.watchers if w not in unwatched]
