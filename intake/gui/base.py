#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import panel as pn
from param.parameterized import Event

MAX_WIDTH = 1600
BACKGROUND = '#ffffff'


def enable_widget(widget, enable=True):
    """Set disabled on widget"""
    widget.disabled = not enable


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
    """
    children = None
    panel = None
    watchers = None
    visible_callback = None

    def __init__(self, visible=True, visible_callback=None):
        self.visible = visible
        self.visible_callback = visible_callback

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
        """When visible changed, do setup or unwatch and call visible_callback"""
        self._visible = visible
        if visible and len(self.panel.objects) == 0:
            self.setup()
            self.panel.extend(self.children)
        elif not visible and len(self.panel.objects) > 0:
            self.unwatch()
            self.panel.clear()
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
