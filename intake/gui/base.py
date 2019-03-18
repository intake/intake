#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import panel as pn
from param.parameterized import Event

class Base(object):  # pragma: no cover
    children = None
    widget = None
    panel = None
    watchers  = None
    dependent_widgets = None

    def __init__(self, visible=True, control_widget=None, dependent_widgets=None):
        self.dependent_widgets = dependent_widgets or []
        self.control_widget = control_widget
        self.visible = visible if control_widget is None else control_widget.value

    def __repr__(self):
        try:
            return self.panel.__repr__()
        except:
            raise RuntimeError("Panel does not seem to be set up properly")


    def _repr_mimebundle_(self, *args, **kwargs):
        try:
            return self.panel._repr_mimebundle_(*args, **kwargs)
        except:
            raise RuntimeError("Panel does not seem to be set up properly")

    def setup(self):
        """This method should set self.children"""
        raise NotImplementedError

    @property
    def visible(self):
        return self._visible

    @visible.setter
    def visible(self, visible):
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
        if self.dependent_widgets:
            if isinstance(enable, Event):
                enable = bool(enable.new)
            for widget in self.dependent_widgets:
                widget.disabled = not enable

    def teardown(self):
        """This method should clean up any dependent widgets that setup appends"""
        pass

    def unwatch(self):
        """This method should get rid of any lingering watchers"""
        if self.watchers is not None:
            unwatched = []
            for watcher in self.watchers:
                watcher.inst.param.unwatch(watcher)
                unwatched.append(watcher)
            self.watchers = [w for w in self.watchers if w not in unwatched]
